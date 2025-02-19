package data

/*
 Copyright 2019 - 2024 Crunchy Data Solutions, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 Date     : October 2022
 Authors  : Benoit De Mezzo (benoit dot de dot mezzo at oslandia dot com)
        	Amaury Zarzelli (amaury dot zarzelli at ign dot fr)
			Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
			Nicolas Revelant (nicolas dot revelant at ign dot fr)
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/conf"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/paulmach/orb/geojson"
	log "github.com/sirupsen/logrus"
)

type catalogDB struct {
	dbconn        *pgxpool.Pool
	tableIncludes map[string]string
	tableExcludes map[string]string
	tables        []*api.Table
	tableMap      map[string]*api.Table
	functions     []*api.Function
	functionMap   map[string]*api.Function
	cache         Cacher
	listener      *listenerDB
}

var isStartup bool
var isFunctionsLoaded bool
var instanceDB *catalogDB

const fmtQueryStats = "Database query result: %v rows in %v"

// first function called when accessing this file
func init() {
	isStartup = true
	instanceDB = nil
}

// CatDBInstance tbd
func CatDBInstance() Catalog {
	if instanceDB == nil {
		instanceDB = newCatalogDB()
	}
	return instanceDB
}

func newCatalogDB() *catalogDB {
	conn := dbConnect()
	cache := makeCache()

	var listener = newListenerDB(conn, cache)

	cat := &catalogDB{
		dbconn:   conn,
		cache:    cache,
		listener: listener,
	}

	return cat
}

// etags cache
func makeCache() Cacher {
	if conf.Configuration.Cache.Type == "Naive" {
		cache_size := conf.Configuration.Cache.Naive.MapSize
		return &CacheNaive{make(map[string]interface{}, cache_size)}
	} else if conf.Configuration.Cache.Type == "Redis" {
		cache := CacheRedis{}
		err := cache.Init(conf.Configuration.Cache.Redis.Url, conf.Configuration.Cache.Redis.Password)
		if err != nil {
			log.Fatalf("Error in CacheRedis init: %v", err)
		}
		return &cache
	} else if conf.Configuration.Cache.Type == "Disabled" || conf.Configuration.Cache.Type == "" {
		return &CacheDisabled{}
	} else {
		log.Fatal(fmt.Errorf("Invalid cache type: Disabled, Naive and Redis are supported. %v defined", conf.Configuration.Cache.Type))
		return &CacheDisabled{}
	}
}

func dbConnect() *pgxpool.Pool {
	dbconfig := dbConfig()

	db, err := pgxpool.ConnectConfig(context.Background(), dbconfig)
	if err != nil {
		log.Fatal(err)
	}
	dbName := dbconfig.ConnConfig.Config.Database
	dbUser := dbconfig.ConnConfig.Config.User
	dbHost := dbconfig.ConnConfig.Config.Host
	log.Infof("Connected as %s to %s @ %s", dbUser, dbName, dbHost)
	return db
}

func dbConfig() *pgxpool.Config {
	dbconf := conf.Configuration.Database.DbConnection
	// disallow blank config for safety
	if dbconf == "" {
		log.Fatal("Blank DbConnection is disallowed for security reasons")
	}

	dbconfig, err := pgxpool.ParseConfig(conf.Configuration.Database.DbConnection)
	if err != nil {
		log.Fatal(err)
	}
	// Read and parse connection lifetime
	dbPoolMaxLifeTime, errt := time.ParseDuration(conf.Configuration.Database.DbPoolMaxConnLifeTime)
	if errt != nil {
		log.Fatal(errt)
	}
	dbconfig.MaxConnLifetime = dbPoolMaxLifeTime

	// Read and parse max connections
	dbPoolMaxConns := conf.Configuration.Database.DbPoolMaxConns
	if dbPoolMaxConns > 0 {
		dbconfig.MaxConns = int32(dbPoolMaxConns)
	}

	// Read current log level and use one less-fine level
	dbconfig.ConnConfig.Logger = logrusadapter.NewLogger(log.New())
	levelString, _ := (log.GetLevel() - 1).MarshalText()
	pgxLevel, _ := pgx.LogLevelFromString(string(levelString))
	dbconfig.ConnConfig.LogLevel = pgxLevel

	return dbconfig
}

func (cat *catalogDB) Initialize(includeList []string, excludeList []string) {
	//-- include schemas / tables
	cat.tableIncludes = make(map[string]string)
	for _, name := range includeList {
		nameLow := strings.ToLower(name)
		cat.tableIncludes[nameLow] = nameLow
	}
	//-- excluded schemas / tables
	cat.tableExcludes = make(map[string]string)
	for _, name := range excludeList {
		nameLow := strings.ToLower(name)
		cat.tableExcludes[nameLow] = nameLow
	}

	// Init the listener
	cat.listener.Initialize(cat.tableIncludes, cat.tableExcludes)
}

func (cat *catalogDB) Close() {
	cat.listener.Close()
	cat.dbconn.Close()
}

func (cat *catalogDB) GetCache() Cacher {
	return cat.cache
}

func (cat *catalogDB) Tables() ([]*api.Table, error) {
	cat.refreshTables(true)
	return cat.tables, nil
}

func (cat *catalogDB) TableReload(name string) {
	tbl, err := cat.TableByName(name)
	if err != nil {
		return
	}
	// load extent (which may change over time
	sqlExtentEst := sqlExtentEstimated(tbl)
	isExtentLoaded := cat.loadExtent(sqlExtentEst, tbl)
	if !isExtentLoaded {
		log.Debugf("Can't get estimated extent for %s", name)
		sqlExtentExact := sqlExtentExact(tbl)
		cat.loadExtent(sqlExtentExact, tbl)
	}
}

func (cat *catalogDB) loadExtent(sql string, tbl *api.Table) bool {
	var (
		xmin pgtype.Float8
		xmax pgtype.Float8
		ymin pgtype.Float8
		ymax pgtype.Float8
	)
	log.Debug("Extent query: " + sql)
	err := cat.dbconn.QueryRow(context.Background(), sql).Scan(&xmin, &ymin, &xmax, &ymax)
	if err != nil {
		log.Debugf("Error querying Extent for %s: %v", tbl.ID, err)
	}
	// no extent was read (perhaps a view...)
	if xmin.Status == pgtype.Null {
		return false
	}
	tbl.Extent.Minx = xmin.Float
	tbl.Extent.Miny = ymin.Float
	tbl.Extent.Maxx = xmax.Float
	tbl.Extent.Maxy = ymax.Float
	return true
}

func (cat *catalogDB) TableByName(name string) (*api.Table, error) {
	cat.refreshTables(false)
	var tbl *api.Table
	tbl, ok := cat.tableMap[name]
	if !ok {
		tbl, ok = cat.tableMap["public."+name]
		if !ok {
			return nil, fmt.Errorf("Unknown table '%v'", name)
		}
	}
	return tbl, nil
}

func (cat *catalogDB) TableFeatures(ctx context.Context, name string, param *QueryParam) ([]*api.GeojsonFeatureData, error) {
	tbl, err := cat.TableByName(name)
	if err != nil || tbl == nil {
		return nil, err
	}
	cols := param.Columns
	sql, argValues := sqlFeatures(tbl, param)
	log.Debug("Features query: " + sql)
	idColIndex := indexOfName(cols, tbl.IDColumn)
	features, err := readFeaturesWithArgs(ctx, cat.dbconn, sql, argValues, name, idColIndex, cols, cat.cache)
	return features, err
}

func (cat *catalogDB) TableFeature(ctx context.Context, name string, id string, param *QueryParam) (*api.GeojsonFeatureData, error) {
	tbl, err := cat.TableByName(name)
	if err != nil {
		return nil, err
	}
	cols := param.Columns
	sql := sqlFeature(tbl, param)
	log.Debug("Feature query: " + sql)

	idColIndex := indexOfName(cols, tbl.IDColumn)

	//--- Add a SQL arg for the feature ID
	argValues := make([]interface{}, 0)
	argValues = append(argValues, id)
	features, err := readFeaturesWithArgs(ctx, cat.dbconn, sql, argValues, name, idColIndex, cols, cat.cache)

	if len(features) == 0 {
		return nil, err
	}

	return features[0], nil
}

func (cat *catalogDB) AddTableFeature(ctx context.Context, tableName string, jsonData []byte, crs string) (int64, error) {
	var schemaObject api.GeojsonFeatureData
	err := json.Unmarshal(jsonData, &schemaObject)
	if err != nil {
		return -9999, err
	}
	var columnStr []string
	var placementStr []string
	var values []interface{}

	tbl, err := cat.TableByName(tableName)
	if err != nil {
		return -9999, err
	}
	var i = 0
	var maxCol = 0
	for colName := range tbl.DbTypes {
		if schemaObject.Props[colName] != nil { // ignore non required and missing columns
			maxCol++
		}
	}

	for colName, col := range tbl.DbTypes {
		if (colName == tbl.IDColumn && tbl.IDColHasDefault) || (colName != tbl.IDColumn && schemaObject.Props[colName] == nil) {
			continue // ignore id column if it has a default value
		}

		i++
		columnStr = append(columnStr, colName)
		placementStr = append(placementStr, fmt.Sprintf("$%d", i))

		var convVal interface{}
		var errConv error

		if colName == tbl.IDColumn {
			convVal, errConv = schemaObject.ID, nil
		} else {
			convVal, errConv = col.Type.ParseJSONInterface(schemaObject.Props[colName])
		}

		if errConv != nil {
			return -9999, errConv
		}
		values = append(values, convVal)
	}

	i++
	columnStr = append(columnStr, tbl.GeometryColumn)
	geomStr := fmt.Sprintf("ST_GeomFromGeoJSON($%d)", i)
	if crs != "" {
		geomStr = fmt.Sprintf("ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($%d), %s), %v)", i, crs, tbl.Srid)
	}
	placementStr = append(placementStr, geomStr)
	geomJson, _ := schemaObject.Geom.MarshalJSON()
	values = append(values, geomJson)
	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
		RETURNING %s`,
		tbl.ID, strings.Join(columnStr, ", "), strings.Join(placementStr, ", "), tbl.IDColumn)

	var id int64 = -1
	err = cat.dbconn.QueryRow(ctx, sqlStatement, values...).Scan(&id)
	if err != nil {
		return -9999, err
	}

	return id, nil
}

func (cat *catalogDB) PartialUpdateTableFeature(ctx context.Context, tableName string, id string, jsonData []byte, crs string) error {

	idx, errInt := strconv.ParseInt(id, 10, 64)
	if errInt != nil {
		return errInt
	}

	tbl, errTbl := cat.TableByName(tableName)
	if errTbl != nil {
		return errTbl
	}

	var schemaObject api.GeojsonFeatureData
	errJson := json.Unmarshal(jsonData, &schemaObject)
	if errJson != nil {
		return errJson
	}

	var columnStr []string
	var placementStr []string
	var values []interface{}

	var i = 0
	for colName, col := range tbl.DbTypes {
		if colName == tbl.IDColumn {
			continue // ignore id column
		}
		if schemaObject.Props[colName] == nil {
			continue // ignore empty data
		}

		i++

		columnStr = append(columnStr, colName)
		placementStr = append(placementStr, fmt.Sprintf("$%d", i))

		convVal, errConv := col.Type.ParseJSONInterface(schemaObject.Props[colName])
		if errConv != nil {
			return errConv
		}
		values = append(values, convVal)
	}

	if schemaObject.Geom != nil {
		i++
		columnStr = append(columnStr, tbl.GeometryColumn)
		geomStr := fmt.Sprintf("ST_GeomFromGeoJSON($%d)", i)
		if crs != "" {
			geomStr = fmt.Sprintf("ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($%d), %s), %v)", i, crs, tbl.Srid)
		}
		placementStr = append(placementStr, geomStr)
		geomJson, _ := schemaObject.Geom.MarshalJSON()
		values = append(values, geomJson)
	}

	var setStr string
	if len(columnStr) == 1 {
		setStr = fmt.Sprintf("%s = %s", strings.Join(columnStr, ", "), strings.Join(placementStr, ", "))
	} else {
		setStr = fmt.Sprintf("( %s ) = ( %s )", strings.Join(columnStr, ", "), strings.Join(placementStr, ", "))
	}

	sqlStatement := fmt.Sprintf(`
		UPDATE %s
		SET    %s
		WHERE  %s=%s
		RETURNING %s
	`, tbl.ID, setStr, tbl.IDColumn, id, tbl.IDColumn)

	row := cat.dbconn.QueryRow(ctx, sqlStatement, values...)

	errQuery := row.Scan(&idx)
	if errQuery != nil {
		return errQuery
	}

	return nil
}

func (cat *catalogDB) ReplaceTableFeature(ctx context.Context, tableName string, id string, jsonData []byte, crs string) error {

	idx, errInt := strconv.ParseInt(id, 10, 64)
	if errInt != nil {
		return errInt
	}

	var schemaObject api.GeojsonFeatureData
	err := json.Unmarshal(jsonData, &schemaObject)
	if err != nil {
		return err
	}
	var colValueStr []string
	var values []interface{}

	tbl, err := cat.TableByName(tableName)
	if err != nil {
		return err
	}
	var i = 0
	for colName, col := range tbl.DbTypes {
		if colName == tbl.IDColumn {
			continue // ignore id column
		}

		i++
		colValueStr = append(colValueStr, fmt.Sprintf("%s=$%d", colName, i))
		if col.IsRequired || schemaObject.Props[colName] != nil {
			convVal, errConv := col.Type.ParseJSONInterface(schemaObject.Props[colName])
			if errConv != nil {
				return errConv
			}
			values = append(values, convVal)
		} else {
			values = append(values, nil)
		}
	}

	i++
	geomStr := fmt.Sprintf("%s=ST_GeomFromGeoJSON($%d)", tbl.GeometryColumn, i)
	if crs != "" {
		geomStr = fmt.Sprintf("%s=ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON($%d), %s), %v)", tbl.GeometryColumn, i, crs, tbl.Srid)
	}
	colValueStr = append(colValueStr, geomStr)
	geomJson, _ := schemaObject.Geom.MarshalJSON()
	values = append(values, geomJson)

	sqlStatement := fmt.Sprintf(`
		UPDATE %s AS t
		SET %s
		WHERE %s=%s
		RETURNING %s
		`, tbl.ID, strings.Join(colValueStr, ", "), tbl.IDColumn, id, tbl.IDColumn)

	err = cat.dbconn.QueryRow(ctx, sqlStatement, values...).Scan(&idx)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	return nil
}

func (cat *catalogDB) DeleteTableFeature(ctx context.Context, tableName string, fid string) error {
	tbl, err := cat.TableByName(tableName)
	if err != nil {
		return err
	}

	sqlStatement := fmt.Sprintf(`
		DELETE FROM %s
		WHERE %s=%s`,
		tableName, tbl.IDColumn, fid)

	var id int64 = -1
	err = cat.dbconn.QueryRow(ctx, sqlStatement).Scan(&id)

	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	return nil
}

func (cat *catalogDB) refreshTables(force bool) {
	// TODO: refresh on timed basis?
	if force || isStartup {
		cat.loadTables()
		isStartup = false
	}
}

func (cat *catalogDB) loadTables() {
	cat.tableMap = cat.readTables(cat.dbconn)
	cat.tables = tablesSorted(cat.tableMap)
}

func tablesSorted(tableMap map[string]*api.Table) []*api.Table {
	// TODO: use database order instead of sorting here
	var lsort []*api.Table
	for key := range tableMap {
		lsort = append(lsort, tableMap[key])
	}
	sort.SliceStable(lsort, func(i, j int) bool {
		return lsort[i].Title < lsort[j].Title
	})
	return lsort
}

func (cat *catalogDB) readTables(db *pgxpool.Pool) map[string]*api.Table {
	log.Debugf("Load table catalog:\n%v", sqlTables)
	rows, err := db.Query(context.Background(), sqlTables)
	if err != nil {
		log.Fatal(err)
	}
	tables := make(map[string]*api.Table)
	for rows.Next() {
		tbl := scanTable(rows)
		if isIncluded(tbl, cat.tableIncludes, cat.tableExcludes) {
			tables[tbl.ID] = tbl
		}
	}
	// Check for errors from iterating over rows.
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	rows.Close()
	return tables
}

func isIncluded(tbl *api.Table, tableIncludes map[string]string, tableExcludes map[string]string) bool {
	//--- if no includes defined, always include
	isIncluded := true
	if len(tableIncludes) > 0 {
		isIncluded = isMatchSchemaTable(tbl, tableIncludes)
	}
	isExcluded := false
	if len(tableExcludes) > 0 {
		isExcluded = isMatchSchemaTable(tbl, tableExcludes)
	}
	return isIncluded && !isExcluded
}

func isMatchSchemaTable(tbl *api.Table, list map[string]string) bool {
	schemaLow := strings.ToLower(tbl.Schema)
	if _, ok := list[schemaLow]; ok {
		return true
	}
	idLow := strings.ToLower(tbl.ID)
	if _, ok := list[idLow]; ok {
		return true
	}
	return false
}

func scanTable(rows pgx.Rows) *api.Table {
	var (
		id, schema, table, description, geometryCol string
		srid                                        int
		geometryType, idColumn                      string
		idColHasDefault                             bool
		props                                       pgtype.TextArray
	)

	err := rows.Scan(&id, &schema, &table, &description, &geometryCol,
		&srid, &geometryType, &idColumn, &idColHasDefault, &props)
	if err != nil {
		log.Fatal(err)
	}

	// Use https://godoc.org/github.com/jackc/pgtype#TextArray
	// here to scan the text[][] map of attribute name/type
	// created in the query. It gets a little ugly demapping the
	// pgx TextArray type, but it is at least native handling of
	// the array. It's complex because of PgSQL ARRAY generality
	// really, no fault of pgx

	arrLen := 0
	arrStart := 0
	elmLen := 0
	if props.Status != pgtype.Null {
		arrLen = int(props.Dimensions[0].Length)
		arrStart = int(props.Dimensions[0].LowerBound - 1)
		elmLen = int(props.Dimensions[1].Length)
	}

	// TODO: query columns in table-defined order

	// Since Go map order is random, list columns in array
	columns := make([]string, arrLen)
	jsontypes := make([]api.JSONType, arrLen)
	datatypes := make(map[string]api.Column)
	colDesc := make([]string, arrLen)

	for i := arrStart; i < arrLen; i++ {
		elmPos := i * elmLen
		name := props.Elements[elmPos].String
		datatype := api.PGType(props.Elements[elmPos+1].String)
		columns[i] = name
		notNull, _ := strconv.ParseBool(props.Elements[elmPos+4].String)
		datatypes[name] = api.Column{Index: i, Type: datatype, IsRequired: notNull}
		jsontypes[i] = datatype.ToJSONType()
		colDesc[i] = props.Elements[elmPos+2].String
	}

	// Synthesize a title for now
	title := id
	// synthesize a description if none provided
	if description == "" {
		description = fmt.Sprintf("Data for table %v", id)
	}

	return &api.Table{
		ID:              id,
		Schema:          schema,
		Table:           table,
		Title:           title,
		Description:     description,
		GeometryColumn:  geometryCol,
		Srid:            srid,
		GeometryType:    geometryType,
		IDColumn:        idColumn,
		Columns:         columns,
		DbTypes:         datatypes,
		JSONTypes:       jsontypes,
		ColDesc:         colDesc,
		IDColHasDefault: idColHasDefault,
	}
}

//=================================================

//nolint:unused
func readFeatures(ctx context.Context, db *pgxpool.Pool, sql string, tableName string, idColIndex int, propCols []string, cache Cacher) ([]*api.GeojsonFeatureData, error) {
	return readFeaturesWithArgs(ctx, db, sql, nil, tableName, idColIndex, propCols, cache)
}

func readFeaturesWithArgs(ctx context.Context, db *pgxpool.Pool, sql string, args []interface{}, tableName string, idColIndex int, propCols []string, cache Cacher) ([]*api.GeojsonFeatureData, error) {
	start := time.Now()
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		log.Warnf("Error running 'Features' (query: '%v'): %v", sql, err)
		return nil, err
	}
	defer rows.Close()
	data, err := scanFeatures(ctx, rows, tableName, idColIndex, propCols, cache)
	if err != nil {
		return data, err
	}
	log.Debugf(fmtQueryStats, len(data), time.Since(start))
	return data, nil
}

func scanFeatures(ctx context.Context, rows pgx.Rows, tableName string, idColIndex int, propCols []string, cache Cacher) ([]*api.GeojsonFeatureData, error) {
	// init features array to empty (not nil)
	var features []*api.GeojsonFeatureData = []*api.GeojsonFeatureData{}
	for rows.Next() {
		feature, err := scanFeature(rows, tableName, idColIndex, propCols, cache)
		if err != nil {
			return nil, err
		}
		features = append(features, feature)
	}
	// context check done outside rows loop,
	// because a long-running function might not produce any rows before timeout
	if err := ctx.Err(); err != nil {
		//log.Debugf("Context error scanning Features: %v", err)
		return features, err
	}
	// Check for errors from scanning rows.
	if err := rows.Err(); err != nil {
		log.Warnf("Error scanning rows for Features: %v", err)
		// TODO: return nil here ?
		return features, err
	}
	return features, nil
}

func scanFeature(rows pgx.Rows, tableName string, idColIndex int, propNames []string, cache Cacher) (*api.GeojsonFeatureData, error) {
	var id string

	vals, err := rows.Values()
	if err != nil {
		log.Warnf("Error scanning row for Feature: %v", err)
		return nil, err
	}

	weakEtagStr := fmt.Sprint(vals[1]) // Weak etag value

	httpDateString := api.GetCurrentHttpDate() // Last modified value

	// val[0] = geometry column
	// val[1] = etag
	// -> properties columns start at 3rd index
	propOffset := 2
	if idColIndex >= 0 {
		id = fmt.Sprintf("%v", vals[idColIndex+propOffset])
	}

	props := extractProperties(vals, idColIndex, propOffset, propNames)

	//--- geom value is expected to be a GeoJSON string or geojson object
	//--- convert NULL to an empty string
	var out *api.GeojsonFeatureData
	if vals[0] != nil {
		if "string" == reflect.TypeOf(vals[0]).String() {
			var g geojson.Geometry
			err := g.UnmarshalJSON([]byte(vals[0].(string)))
			if err != nil {
				return nil, err
			}
			out = api.MakeGeojsonFeature(tableName, id, g, props, weakEtagStr, httpDateString)

		} else {
			out = api.MakeGeojsonFeature(tableName, id, vals[0].(geojson.Geometry), props, weakEtagStr, httpDateString)
		}

	} else {
		var g geojson.Geometry
		out = api.MakeGeojsonFeature(tableName, id, g, props, weakEtagStr, httpDateString)
	}

	// Check the etag presence into the cache, and add it if necessary
	weakEtagStr = out.WeakEtag.String()
	present, err := IsOneEtagInCache(cache, []string{weakEtagStr})
	if err != nil {
		log.Warnf(api.ErrMsgMalformedEtag+". Error: %v", weakEtagStr, err)
	}
	if !present {
		// ===== DOUBLE ADD!!
		//nolint:errcheck
		cache.AddWeakEtag(out.WeakEtag.CacheKey(), out.WeakEtag)
		//nolint:errcheck
		cache.AddWeakEtag(out.WeakEtag.AlternateCacheKey(), out.WeakEtag)
	}

	return out, nil
}

func extractProperties(vals []interface{}, idColIndex int, propOffset int, propNames []string) map[string]interface{} {
	props := make(map[string]interface{})
	for i, name := range propNames {
		if i == idColIndex {
			continue
		}
		// offset vals index by 2 to skip geom, id
		// val := vals[i+propOffset]
		props[name] = toJSONValue(vals[i+propOffset])
		// fmt.Printf("%v: %v\n", name, val)
	}
	return props
}

// toJSONValue convert PG types to JSON values
func toJSONValue(value interface{}) interface{} {
	//fmt.Printf("toJSONValue: %v\n", reflect.TypeOf(value))
	switch v := value.(type) {
	case *pgtype.Numeric:
		var num float64
		// TODO: handle error
		v.AssignTo(&num) //nolint:errcheck
		return num
	case *pgtype.JSON:
		var jsonval string
		v.AssignTo(&jsonval) //nolint:errcheck
		return json.RawMessage(jsonval)
	case *pgtype.TextArray:
		var strarr []string
		v.AssignTo(&strarr) //nolint:errcheck
		return strarr
	case *pgtype.BoolArray:
		var valarr []bool
		v.AssignTo(&valarr) //nolint:errcheck
		return valarr
	case *pgtype.Int2Array:
		var numarr []int16
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
	case *pgtype.Int4Array:
		var numarr []int32
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
	case *pgtype.Int8Array:
		var numarr []int64
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
	case *pgtype.Float4Array:
		var numarr []float64
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
	case *pgtype.Float8Array:
		var numarr []float64
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
	case *pgtype.NumericArray:
		var numarr []float64
		v.AssignTo(&numarr) //nolint:errcheck
		return numarr
		// TODO: handle other conversions?
	}
	// for now all other values are returned  as is
	// this is only safe if the values are text!
	return value
}

// indexOfName finds the index of a name in an array of names
// It returns the index or -1 if not found
func indexOfName(names []string, name string) int {
	for i, nm := range names {
		if nm == name {
			return i
		}
	}
	return -1
}
