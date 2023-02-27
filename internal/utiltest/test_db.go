package utiltest

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

 Date     : September 2022
 Authors  : Benoit De Mezzo (benoit dot de dot mezzo at oslandia dot com)
*/

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/conf"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	log "github.com/sirupsen/logrus"
)

const SpecialSchemaStr = `😀.$^{schema}.👿.😱`
const SpecialTableStr = `😀.$^{table}.👿.😱`
const SpecialColumnStr = `😀.$^{column}.👿.😱`

func CreateTestDb() *pgxpool.Pool {
	dbURL := os.Getenv(conf.AppConfig.EnvDBURL)
	if dbURL == "" {
		dbURL = "postgresql://postgres@localhost/pg_featureserv"
		log.Warnf("No env var '%s' defined, using default value: %s", conf.AppConfig.EnvDBURL, dbURL)
	}
	conf.Configuration.Database.DbConnection = dbURL
	conf.Configuration.Database.DbPoolMaxConnLifeTime = "1h"

	ctx := context.Background()
	dbconfig, errConf := pgxpool.ParseConfig(conf.Configuration.Database.DbConnection)
	if errConf != nil {
		log.Fatal(errConf)
	}
	db, errConn := pgxpool.ConnectConfig(ctx, dbconfig)
	if errConn != nil {
		log.Fatal(errConn)
	}

	dbName := dbconfig.ConnConfig.Config.Database
	dbUser := dbconfig.ConnConfig.Config.User
	dbHost := dbconfig.ConnConfig.Config.Host
	log.Debugf("Connected as %s to %s @ %s", dbUser, dbName, dbHost)

	CreateSchema(db, "complex")
	CreateSchema(db, SpecialSchemaStr)
	InsertSimpleDataset(db, "public")
	InsertSuperSimpleDataset(db, "public", "mock_ssimple")
	InsertComplexDataset(db, "complex")
	InsertSuperSimpleDataset(db, SpecialSchemaStr, SpecialTableStr)

	log.Debugf("Sample data injected")

	return db
}

func CreateSchema(db *pgxpool.Pool, schema string) {
	ctx := context.Background()
	cleanedSchema := pgx.Identifier{schema}.Sanitize()
	_, errExec := db.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s;`, cleanedSchema))
	if errExec != nil {
		CloseTestDb(db)
		log.Fatal(errExec)
	}
}

func InsertSimpleDataset(db *pgxpool.Pool, schema string) {
	ctx := context.Background()

	// collections tables
	type tableContent struct {
		geometryType api.GeometryType
		extent       api.Extent
		nx           int
		ny           int
	}
	tablesAndExtents := map[string]tableContent{
		"mock_a":    {api.GeometryTypePoint, api.Extent{Minx: -120, Miny: 40, Maxx: -74, Maxy: 50}, 3, 3},
		"mock_b":    {api.GeometryTypePoint, api.Extent{Minx: -75, Miny: 45, Maxx: -74, Maxy: 46}, 10, 10},
		"mock_c":    {api.GeometryTypePoint, api.Extent{Minx: -120, Miny: 40, Maxx: -74, Maxy: 60}, 100, 100},
		"mock_poly": {api.GeometryTypePolygon, api.Extent{}, 0, 0},
	}

	createBytes := []byte(`
		DROP TABLE IF EXISTS %s CASCADE;
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			geometry public.geometry(%s, 4326) NOT NULL,
			prop_a text NOT NULL,
			prop_b int NOT NULL,
			prop_c text,
			prop_d int
		);
		CREATE INDEX %s_geometry_idx ON %s USING GIST (geometry);
	`)

	for tableName, tableElements := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, tableName}.Sanitize()
		createStatement := fmt.Sprintf(string(createBytes), tableNameWithSchema, tableNameWithSchema, tableElements.geometryType, tableName, tableNameWithSchema)

		_, errExec := db.Exec(ctx, createStatement)
		if errExec != nil {
			CloseTestDb(db)
			log.Fatal(errExec)
		}
	}

	// ================================================================================

	// inserting features into mock collections
	b := &pgx.Batch{}

	insertBytes := []byte(`
		INSERT INTO %s (geometry, prop_a, prop_b, prop_c, prop_d)
		VALUES (ST_GeomFromGeoJSON($1), $2, $3, $4, $5)
	`)
	for tableName, tableElements := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, tableName}.Sanitize()
		insertStatement := fmt.Sprintf(string(insertBytes), tableNameWithSchema)

		switch tableElements.geometryType {
		case api.GeometryTypePoint:
			for _, feat := range data.MakeFeaturesMockPoint(tableName, tableElements.extent, tableElements.nx, tableElements.ny) {
				geomStr, _ := feat.Geom.MarshalJSON()
				b.Queue(insertStatement, geomStr, feat.Props["prop_a"], feat.Props["prop_b"], feat.Props["prop_c"], feat.Props["prop_d"])
			}
		case api.GeometryTypePolygon:
			for _, feat := range data.MakeFeaturesMockPolygon(tableNameWithSchema) {
				geomStr, _ := feat.Geom.MarshalJSON()
				b.Queue(insertStatement, geomStr, feat.Props["prop_a"], feat.Props["prop_b"], feat.Props["prop_c"], feat.Props["prop_d"])
			}
		}
		res := db.SendBatch(ctx, b)
		if res == nil {
			CloseTestDb(db)
			log.Fatal("Injection failed")
		}
		resClose := res.Close()
		if resClose != nil {
			CloseTestDb(db)
			log.Fatal(fmt.Sprintf("Injection failed: %v", resClose.Error()))
		}

	}

}

func InsertSuperSimpleDataset(db *pgxpool.Pool, schema string, tablename string) {
	ctx := context.Background()
	// collections tables
	// tables := []string{"mock_a", "mock_b", "mock_c"}
	type tableContent struct {
		extent api.Extent
		nx     int
		ny     int
	}
	tablesAndExtents := map[string]tableContent{
		tablename: {api.Extent{Minx: -120, Miny: 40, Maxx: -74, Maxy: 50}, 3, 3},
	}

	cleanedColumn := pgx.Identifier{SpecialColumnStr}.Sanitize()

	createBytes := []byte(`
		DROP TABLE IF EXISTS %s CASCADE;
		CREATE TABLE IF NOT EXISTS %s (
			id int PRIMARY KEY,
			geometry public.geometry(Point, 4326) NOT NULL,
			%s text
		);
		CREATE INDEX geometry_idx ON %s USING GIST (geometry);
	`)
	for s := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, s}.Sanitize()
		createStatement := fmt.Sprintf(string(createBytes), tableNameWithSchema, tableNameWithSchema, cleanedColumn, tableNameWithSchema)

		_, errExec := db.Exec(ctx, createStatement)
		if errExec != nil {
			CloseTestDb(db)
			log.Fatal(errExec)
		}
	}

	// collections features/table records
	b := &pgx.Batch{}

	insertBytes := []byte(`
		INSERT INTO %s (id, geometry)
		VALUES ($2, ST_GeomFromGeoJSON($1))
	`)
	for tableName, tableElements := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, tableName}.Sanitize()
		insertStatement := fmt.Sprintf(string(insertBytes), tableNameWithSchema)
		featuresMock := data.MakeFeaturesMockPoint(tableName, tableElements.extent, tableElements.nx, tableElements.ny)

		for i, f := range featuresMock {
			geomStr, _ := f.Geom.MarshalJSON()
			b.Queue(insertStatement, geomStr, i)
		}
		res := db.SendBatch(ctx, b)
		if res == nil {
			CloseTestDb(db)
			log.Fatal("Injection failed")
		}
		resClose := res.Close()
		if resClose != nil {
			CloseTestDb(db)
			log.Fatal(fmt.Sprintf("Injection failed: %v", resClose.Error()))
		}
	}
}

func MakeGeojsonFeatureMockPoint(id int, x float64, y float64) *api.GeojsonFeatureData {

	geom := geojson.NewGeometry(orb.Point{x, y})
	idstr := strconv.Itoa(id)
	props := make(map[string]interface{})
	props["prop_t"] = idstr
	props["prop_i"] = id
	props["prop_l"] = int64(id)
	props["prop_f"] = float64(id)
	props["prop_r"] = float32(id)
	props["prop_b"] = []bool{id%2 == 0, id%2 == 1}
	props["prop_d"] = time.Now()
	props["prop_j"] = api.Sorting{Name: idstr, IsDesc: id%2 == 1}
	props["prop_v"] = idstr

	feat := api.GeojsonFeatureData{Type: "Feature", ID: idstr, Geom: geom, Props: props}

	return &feat
}

func InsertComplexDataset(db *pgxpool.Pool, schema string) {
	ctx := context.Background()
	cleanedSchema := pgx.Identifier{schema}.Sanitize()

	// NOT same as featureMock
	// TODO: mark all props as required with NOT NULL contraint?
	_, errExec := db.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s.mock_multi CASCADE;
		CREATE TABLE IF NOT EXISTS %s.mock_multi (
			geometry public.geometry(Point, 4326) NOT NULL,
			fid SERIAL PRIMARY KEY,
			prop_t text NOT NULL,
			prop_i int NOT NULL,
			prop_l bigint NOT NULL,
			prop_f float8 NOT NULL,
			prop_r real NOT NULL,
			prop_b bool[] NOT NULL,
			prop_d date NOT NULL,
			prop_j json NOT NULL,
			prop_v varchar NOT NULL
		);
		CREATE INDEX mock_multi_geometry_idx ON %s.mock_multi USING GIST (geometry);
		`, cleanedSchema, cleanedSchema, cleanedSchema))
	if errExec != nil {
		CloseTestDb(db)
		log.Fatal(errExec)
	}

	n := 5.0
	features := make([]*api.GeojsonFeatureData, int((n*2)*(n*2)))
	id := 1
	for ix := -n; ix < n; ix++ {
		for iy := -n; iy < n; iy++ {
			feat := MakeGeojsonFeatureMockPoint(id, ix, iy)
			features[id-1] = feat
			id++
		}
	}

	b := &pgx.Batch{}
	sqlStatement := fmt.Sprintf(`
		INSERT INTO %s.mock_multi (geometry, prop_t, prop_i, prop_l, prop_f, prop_r, prop_b, prop_d, prop_j, prop_v)
		VALUES (ST_GeomFromGeoJSON($1), $2, $3, $4, $5, $6, $7, $8, $9, $10)`, cleanedSchema)

	for _, f := range features {
		geomStr, _ := f.Geom.MarshalJSON()
		b.Queue(sqlStatement, geomStr, f.Props["prop_t"], f.Props["prop_i"], f.Props["prop_l"], f.Props["prop_f"], f.Props["prop_r"], f.Props["prop_b"], f.Props["prop_d"], f.Props["prop_j"], f.Props["prop_v"])
	}
	res := db.SendBatch(ctx, b)
	if res == nil {
		CloseTestDb(db)
		log.Fatal("Injection failed")
	}
	resClose := res.Close()
	if resClose != nil {
		CloseTestDb(db)
		log.Fatal(fmt.Sprintf("Injection failed: %v", resClose.Error()))
	}
}

func CloseTestDb(db *pgxpool.Pool) {
	log.Debugf("Sample dbs will be cleared...")
	var sql string
	cleanedTableNameWithSchema := pgx.Identifier{SpecialSchemaStr, SpecialTableStr}.Sanitize()
	for _, t := range []string{"public.mock_a", "public.mock_b", "public.mock_c", "complex.mock_multi",
		"public.mock_ssimple", cleanedTableNameWithSchema} {
		sql = fmt.Sprintf("%s DROP TABLE IF EXISTS %s CASCADE;", sql, t)
	}
	_, errExec := db.Exec(context.Background(), sql)
	if errExec != nil {
		log.Warnf("Failed to drop sample dbs! ")
		log.Warnf(errExec.Error())
	}
	db.Close()
	log.Debugf("Sample dbs cleared!")
}
