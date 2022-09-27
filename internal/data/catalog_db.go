package data

/*
 Copyright 2022 Crunchy Data Solutions, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

*/

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	cache         Cache
}

var isStartup bool
var isFunctionsLoaded bool
var instanceDB catalogDB

const fmtQueryStats = "Database query result: %v rows in %v"

func init() {
	isStartup = true
}

// CatDBInstance tbd
func CatDBInstance() Catalog {
	// TODO: make a singleton
	instanceDB = newCatalogDB()
	return &instanceDB
}

func newCatalogDB() catalogDB {
	conn := dbConnect()
	cache := makeCache()
	cat := catalogDB{
		dbconn: conn,
		cache:  cache,
	}
	return cat
}

// -------------------------------------------------
// etags cache
func makeCache() Cache {
	var cache_size = 200000
	value, present := os.LookupEnv("PGFS_CACHE")
	if present && value == "1" {
		log.Infof("PGFS_CACHE var detected -> etag cache activated")
		return CacheActive{make(map[string]interface{}, cache_size)}
	} else {
		log.Infof("etag cache disabled")
		return CachePassive{make(map[string]interface{})}
	}
}

// -------------------------------------------------

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

func (cat *catalogDB) SetIncludeExclude(includeList []string, excludeList []string) {
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
}

func (cat *catalogDB) Close() {
	cat.dbconn.Close()
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
	tbl, ok := cat.tableMap[name]
	if !ok {
		tbl, ok := cat.tableMap["public."+name]
		if !ok {
			return nil, nil
		}
		return tbl, nil
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
	features, _, err := readFeaturesWithArgs(ctx, cat.dbconn, sql, argValues, idColIndex, cols, cat.cache)
	return features, err
}

func (cat *catalogDB) TableFeature(ctx context.Context, name string, id string, param *QueryParam) (*api.GeojsonFeatureData, string, error) {
	tbl, err := cat.TableByName(name)
	if err != nil {
		return nil, "", err
	}
	cols := param.Columns
	sql := sqlFeature(tbl, param)
	log.Debug("Feature query: " + sql)

	idColIndex := indexOfName(cols, tbl.IDColumn)

	//--- Add a SQL arg for the feature ID
	argValues := make([]interface{}, 0)
	argValues = append(argValues, id)
	features, etags, err := readFeaturesWithArgs(ctx, cat.dbconn, sql, argValues, idColIndex, cols, cat.cache)

	if len(features) == 0 {
		return nil, "", err
	}

	return features[0], etags[0], nil
}

func (cat *catalogDB) AddTableFeature(ctx context.Context, tableName string, jsonData []byte) (int64, error) {
	var schemaObject api.GeojsonFeatureData
	err := json.Unmarshal(jsonData, &schemaObject)
	if err != nil {
		return -9999, err
	}
	var columnStr string
	var placementStr string
	var values []interface{}

	tbl, err := cat.TableByName(tableName)
	if err != nil {
		return -9999, err
	}
	var i = 0
	for colName, col := range tbl.DbTypes {
		if colName == tbl.IDColumn {
			continue // ignore id column
		}

		i++
		columnStr += colName
		placementStr += fmt.Sprintf("$%d", i)

		convVal, errConv := col.Type.ParseJSONInterface(schemaObject.Props[colName])
		if errConv != nil {
			return -9999, errConv
		}
		values = append(values, convVal)

		if i < len(tbl.Columns)-1 {
			columnStr += ", "
			placementStr += ", "
		}

	}

	i++
	columnStr += ", " + tbl.GeometryColumn
	placementStr += fmt.Sprintf(", ST_GeomFromGeoJSON($%d)", i)
	geomJson, _ := schemaObject.Geom.MarshalJSON()
	values = append(values, geomJson)

	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s (%s)
		VALUES (%s)
		RETURNING %s`,
		tbl.ID, columnStr, placementStr, tbl.IDColumn)

	var id int64 = -1
	err = cat.dbconn.QueryRow(ctx, sqlStatement, values...).Scan(&id)
	if err != nil {
		return -9999, err
	}

	return id, nil
}

func (cat *catalogDB) PartialUpdateTableFeature(ctx context.Context, tableName string, id string, jsonData []byte) (int64, error) {

	idx, errInt := strconv.ParseInt(id, 10, 64)
	if errInt != nil {
		return -9999, errInt
	}

	tbl, errTbl := cat.TableByName(tableName)
	if errTbl != nil {
		return -9999, errTbl
	}

	var schemaObject api.GeojsonFeatureData
	errJson := json.Unmarshal(jsonData, &schemaObject)
	if errJson != nil {
		return -9999, errJson
	}

	var columnStr string
	var placementStr string
	var values []interface{}

	var i = 0
	for c, t := range tbl.DbTypes {
		if c == tbl.IDColumn {
			continue // ignore id column
		}
		if schemaObject.Props[c] == nil {
			continue // ignore empty data
		}

		i++

		columnStr += c
		columnStr += ", "
		placementStr += fmt.Sprintf("$%d", i)
		placementStr += ", "

		if t.Type == "int4" {
			values = append(values, int(schemaObject.Props[c].(float64)))
		} else {
			values = append(values, schemaObject.Props[c])
		}
	}

	columnStr = strings.TrimSuffix(columnStr, ", ")
	placementStr = strings.TrimSuffix(placementStr, ", ")

	if schemaObject.Geom != nil {
		i++
		columnStr += ", " + tbl.GeometryColumn
		placementStr += fmt.Sprintf(", ST_GeomFromGeoJSON($%d)", i)
		geomJson, _ := schemaObject.Geom.MarshalJSON()
		values = append(values, geomJson)
	}

	sqlStatement := fmt.Sprintf(`
		UPDATE %s
		SET ( %s ) = ( %s )
		WHERE id = %s
		RETURNING %s
	`, tbl.ID, columnStr, placementStr, id, tbl.IDColumn)

	row := cat.dbconn.QueryRow(ctx, sqlStatement, values...)

	errQuery := row.Scan(&idx)
	if errQuery != nil {
		return -9999, errQuery
	}

	return idx, nil
}

func (cat *catalogDB) ReplaceTableFeature(ctx context.Context, tableName string, id string, jsonData []byte) error {
	var schemaObject api.GeojsonFeatureData
	err := json.Unmarshal(jsonData, &schemaObject)
	if err != nil {
		return err
	}
	var colValueStr string
	var values []interface{}

	tbl, err := cat.TableByName(tableName)
	if err != nil {
		return err
	}
	var i = 0
	for c, t := range tbl.DbTypes {
		if c == tbl.IDColumn {
			continue // ignore id column
		}

		i++
		colValueStr += c
		colValueStr += "="
		colValueStr += fmt.Sprintf("$%d", i)
		if t.Type == "int4" {
			values = append(values, int(schemaObject.Props[c].(float64)))
		} else {
			values = append(values, schemaObject.Props[c])
		}

		if i < len(tbl.Columns)-1 {
			colValueStr += ", "
		}

	}

	i++
	colValueStr += ", " + tbl.GeometryColumn
	colValueStr += "="
	colValueStr += fmt.Sprintf("ST_GeomFromGeoJSON($%d)", i)
	geomJson, _ := schemaObject.Geom.MarshalJSON()
	values = append(values, geomJson)

	sqlStatement := fmt.Sprintf(`
		UPDATE %s AS t
		SET %s
		WHERE %s=%s`,
		tbl.ID, colValueStr, tbl.IDColumn, id)

	err = cat.dbconn.QueryRow(ctx, sqlStatement, values...).Scan()
	if err != nil && err != pgx.ErrNoRows {
		return err
	}

	return nil
}

func (cat *catalogDB) DeleteTableFeature(ctx context.Context, tableName string, fid string) error {

	sqlStatement := fmt.Sprintf(`
		DELETE FROM %s
		WHERE id = %s`,
		tableName, fid)

	var id int64 = -1
	err := cat.dbconn.QueryRow(ctx, sqlStatement).Scan(&id)

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
		if cat.isIncluded(tbl) {
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

func (cat *catalogDB) isIncluded(tbl *api.Table) bool {
	//--- if no includes defined, always include
	isIncluded := true
	if len(cat.tableIncludes) > 0 {
		isIncluded = isMatchSchemaTable(tbl, cat.tableIncludes)
	}
	isExcluded := false
	if len(cat.tableExcludes) > 0 {
		isExcluded = isMatchSchemaTable(tbl, cat.tableExcludes)
	}
	return isIncluded && !isExcluded
}

func (cat *catalogDB) CheckStrongEtags(etagsList []string) (bool, error) {
	for _, strongEtag := range etagsList {
		found, err := cat.cache.ContainsWeakEtag(strongEtag)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
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
		props                                       pgtype.TextArray
	)

	err := rows.Scan(&id, &schema, &table, &description, &geometryCol,
		&srid, &geometryType, &idColumn, &props)
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
		// TODO must find a way to compute IsRequired
		datatypes[name] = api.Column{Index: i, Type: datatype, IsRequired: true}
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
		ID:             id,
		Schema:         schema,
		Table:          table,
		Title:          title,
		Description:    description,
		GeometryColumn: geometryCol,
		Srid:           srid,
		GeometryType:   geometryType,
		IDColumn:       idColumn,
		Columns:        columns,
		DbTypes:        datatypes,
		JSONTypes:      jsontypes,
		ColDesc:        colDesc,
	}
}

//=================================================

//nolint:unused
func readFeatures(ctx context.Context, db *pgxpool.Pool, sql string, idColIndex int, propCols []string, cache Cache) ([]*api.GeojsonFeatureData, []string, error) {
	return readFeaturesWithArgs(ctx, db, sql, nil, idColIndex, propCols, cache)
}

func readFeaturesWithArgs(ctx context.Context, db *pgxpool.Pool, sql string, args []interface{}, idColIndex int, propCols []string, cache Cache) ([]*api.GeojsonFeatureData, []string, error) {
	start := time.Now()
	fmt.Printf("sql: %s", sql)
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		log.Warnf("Error running Features query: %v", err)
		return nil, nil, err
	}
	defer rows.Close()
	data, etags, err := scanFeatures(ctx, rows, idColIndex, propCols, cache)
	if err != nil {
		return data, nil, err
	}
	log.Debugf(fmtQueryStats, len(data), time.Since(start))
	return data, etags, nil
}

func scanFeatures(ctx context.Context, rows pgx.Rows, idColIndex int, propCols []string, cache Cache) ([]*api.GeojsonFeatureData, []string, error) {
	// init features array to empty (not nil)
	var features []*api.GeojsonFeatureData = []*api.GeojsonFeatureData{}
	var etags []string
	for rows.Next() {
		feature, etag, err := scanFeature(rows, idColIndex, propCols, cache)
		if err != nil {
			return nil, nil, err
		}
		features = append(features, feature)
		etags = append(etags, etag)
	}
	// context check done outside rows loop,
	// because a long-running function might not produce any rows before timeout
	if err := ctx.Err(); err != nil {
		//log.Debugf("Context error scanning Features: %v", err)
		return features, nil, err
	}
	// Check for errors from scanning rows.
	if err := rows.Err(); err != nil {
		log.Warnf("Error scanning rows for Features: %v", err)
		// TODO: return nil here ?
		return features, nil, err
	}
	return features, etags, nil
}

func scanFeature(rows pgx.Rows, idColIndex int, propNames []string, cache Cache) (*api.GeojsonFeatureData, string, error) {
	var id string

	vals, err := rows.Values()
	if err != nil {
		log.Warnf("Error scanning row for Feature: %v", err)
		return nil, "", err
	}

	weakEtag := fmt.Sprint(vals[1])

	cache.AddWeakEtag(weakEtag, map[string]interface{}{"lastModified": time.Now().String()})

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
	if vals[0] != nil {
		if "string" == reflect.TypeOf(vals[0]).String() {
			var g geojson.Geometry
			err := g.UnmarshalJSON([]byte(vals[0].(string)))
			if err != nil {
				return nil, "", err
			}
			return api.MakeGeojsonFeature(id, g, props), weakEtag, nil
		} else {
			return api.MakeGeojsonFeature(id, vals[0].(geojson.Geometry), props), weakEtag, nil
		}
	} else {
		var g geojson.Geometry
		return api.MakeGeojsonFeature(id, g, props), weakEtag, nil
	}
}

func extractProperties(vals []interface{}, idColIndex int, propOffset int, propNames []string) map[string]interface{} {
	props := make(map[string]interface{})
	for i, name := range propNames {
		if i == idColIndex {
			continue
		}
		// offset vals index by 2 to skip geom, id
		val := vals[i+propOffset]
		props[name] = toJSONValue(vals[i+propOffset])
		fmt.Printf("%v: %v\n", name, val)
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
