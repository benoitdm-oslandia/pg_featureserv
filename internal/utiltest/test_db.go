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
	// tables := []string{"mock_a", "mock_b", "mock_c"}
	type tableContent struct {
		extent api.Extent
		nx     int
		ny     int
	}
	tablesAndExtents := map[string]tableContent{
		"mock_a": {api.Extent{Minx: -120, Miny: 40, Maxx: -74, Maxy: 50}, 3, 3},
		"mock_b": {api.Extent{Minx: -75, Miny: 45, Maxx: -74, Maxy: 46}, 10, 10},
		"mock_c": {api.Extent{Minx: -120, Miny: 40, Maxx: -74, Maxy: 60}, 100, 100},
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

	for s := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, s}.Sanitize()
		createStatement := fmt.Sprintf(string(createBytes), tableNameWithSchema, tableNameWithSchema, "Point", s, tableNameWithSchema)

		_, errExec := db.Exec(ctx, createStatement)
		if errExec != nil {
			CloseTestDb(db)
			log.Fatal(errExec)
		}
	}

	// Table/collection dedicated to polygons
	tableNameWithSchema := fmt.Sprintf("%s.%s", schema, "mock_poly")
	createStatement := fmt.Sprintf(string(createBytes), tableNameWithSchema, tableNameWithSchema, "Polygon")
	_, errExec := db.Exec(ctx, createStatement)
	if errExec != nil {
		CloseTestDb(db)
		log.Fatal(errExec)
	}

	// ================================================================================

	// inserting point features into mock collections
	b := &pgx.Batch{}

	insertBytes := []byte(`
		INSERT INTO %s (geometry, prop_a, prop_b, prop_c, prop_d)
		VALUES (ST_GeomFromGeoJSON($1), $2, $3, $4, $5)
	`)
	for tableName, tableElements := range tablesAndExtents {
		tableNameWithSchema := pgx.Identifier{schema, tableName}.Sanitize()
		insertStatement := fmt.Sprintf(string(insertBytes), tableNameWithSchema)
		featuresMock := data.MakeFeaturesMockPoint(tableName, tableElements.extent, tableElements.nx, tableElements.ny)

		for _, f := range featuresMock {
			geomStr, _ := f.Geom.MarshalJSON()
			b.Queue(insertStatement, geomStr, f.Props["prop_a"], f.Props["prop_b"], f.Props["prop_c"], f.Props["prop_d"])
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

	// inserting polygon features into mock_poly collection/table
	polygons := make([]orb.Ring, 3)
	polygons[0] = (orb.Ring{{-0.024590485281003, 49.2918461864342}, {-0.02824214022877, 49.2902093052715}, {-0.032731597583892, 49.2940548086905}, {-0.037105514267367, 49.2982628947696}, {-0.035096222035489, 49.2991273714187}, {-0.038500457450357, 49.3032655348948}, {-0.034417965728768, 49.3047607558599}, {-0.034611922456059, 49.304982637632}, {-0.028287271276391, 49.3073904622151}, {-0.022094153540685, 49.3097046833446}, {-0.022020905508067, 49.3096240670749}, {-0.019932810088915, 49.3103884833526}, {-0.013617304476105, 49.3129751788625}, {-0.010317714854534, 49.3091925467367}, {-0.006352474569531, 49.3110873002743}, {-0.001853050940172, 49.3070612288807}, {0.002381370562776, 49.3028484930665}, {-0.000840217324783, 49.3013882187799}, {-0.00068928216257, 49.3012429006019}, {-0.003864625123604, 49.3000173218511}, {-0.003918013833785, 49.2999931219338}, {-0.010095065847337, 49.2974103246769}, {-0.010150643294152, 49.2974622610823}, {-0.013587537856462, 49.2959737733625}, {-0.01384030494609, 49.2962233671643}, {-0.017222409797967, 49.294623513139}, {-0.017308576106142, 49.2947057553981}, {-0.020709238582055, 49.2930969232562}, {-0.021034503634088, 49.2933909821512}, {-0.024481057600533, 49.2917430023163}, {-0.024590485281003, 49.2918461864342}})
	polygons[1] = (orb.Ring{{0.012754827133148, 49.3067879156925}, {0.008855271114669, 49.3050781328888}, {0.004494239224312, 49.3091080209745}, {-0.000152707581678, 49.3133105602284}, {0.005720060734669, 49.3160862415579}, {0.005012790172897, 49.3167672210029}, {0.000766997696737, 49.3211596408574}, {0.007624129875227, 49.3239385018443}, {0.008367761372595, 49.3242455690107}, {0.008290411160612, 49.3243148348313}, {0.014857908580632, 49.327355944666}, {0.021563621634322, 49.330400077634}, {0.021666104647453, 49.3302974189836}, {0.024971410363691, 49.3317809883673}, {0.02492195583839, 49.3318321743075}, {0.029104098429698, 49.3336152412767}, {0.028646253682028, 49.3340827604102}, {0.035511767129074, 49.3367701742839}, {0.04198105053544, 49.3391776115466}, {0.046199095420336, 49.3352329627991}, {0.047069675744848, 49.3344290720305}, {0.048144047016136, 49.334920703514}, {0.048423560249958, 49.3346968337392}, {0.051915791431139, 49.3363621210079}, {0.056947292176151, 49.3326168697662}, {0.061993411180365, 49.3286019089077}, {0.055850651601917, 49.3253039337471}, {0.049713813923233, 49.3219158062857}, {0.049393633537099, 49.3221688494924}, {0.047471649153311, 49.3213066024438}, {0.04755106595679, 49.3212332612062}, {0.040845011450398, 49.3181905415208}, {0.040150920245632, 49.31787904142}, {0.039962885130089, 49.317782152465}, {0.04034174516319, 49.3173686114171}, {0.033626289449895, 49.3145051363955}, {0.032740557919845, 49.3141516109565}, {0.031347338613429, 49.313459605015}, {0.031235682243362, 49.3135509641281}, {0.029314267528688, 49.3127840624681}, {0.024083333873085, 49.3105820713374}, {0.02383988821816, 49.3108046457384}, {0.022989404102509, 49.3104651415232}, {0.016397609318679, 49.3078735624598}, {0.016236244414416, 49.3080276777805}, {0.013035870818624, 49.3065310213615}, {0.012754827133148, 49.3067879156925}})
	polygons[2] = (orb.Ring{{0.019797816099279, 49.325229088603}, {0.013235498621243, 49.3220984135413}, {0.006679188663454, 49.3188775447307}, {0.001751478001915, 49.3231631269776}, {0.00030826510927, 49.3244180023312}, {0.000034521402383, 49.3242899085418}, {-0.004894257776504, 49.3285751953461}, {-0.009823855515987, 49.332860261738}, {-0.003845879462176, 49.3357402000546}, {-0.004376904724334, 49.336234279179}, {0.00019267127677, 49.3382699850882}, {0.00003896662097, 49.3384130063648}, {0.006882712504834, 49.3414613328914}, {0.013584586312611, 49.3445956881043}, {0.013835900545075, 49.3443662391223}, {0.018429968444473, 49.3465144456831}, {0.019007858697842, 49.3459970497808}, {0.022212104736706, 49.3477771230593}, {0.028477356337026, 49.3513495867644}, {0.033807665316216, 49.347252820989}, {0.038724697445692, 49.3431456923271}, {0.034812389120157, 49.3408267818312}, {0.036339781995501, 49.3391292768443}, {0.040721479048813, 49.3347390581568}, {0.036808655724018, 49.3329836158413}, {0.037123735821512, 49.3326718720873}, {0.030269026676719, 49.3298048842398}, {0.023282829964216, 49.3268442840858}, {0.023162342964376, 49.3269672904862}, {0.021527329925941, 49.3262612666818}, {0.019602511201379, 49.3254039935278}, {0.019797816099279, 49.325229088603}})

	tableNameWithSchema = fmt.Sprintf("%s.%s", schema, "mock_poly")
	insertPolyStatement := fmt.Sprintf(string(insertBytes), tableNameWithSchema)
	propVal := 100
	features := data.MakeFeaturesMockPolygon(tableNameWithSchema, propVal, polygons)

	for _, feat := range features {
		geomStr, _ := feat.Geom.MarshalJSON()
		b.Queue(insertPolyStatement, geomStr, feat.Props["prop_a"], feat.Props["prop_b"], feat.Props["prop_c"], feat.Props["prop_d"])
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
