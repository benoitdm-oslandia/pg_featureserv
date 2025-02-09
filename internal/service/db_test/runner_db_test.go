package db_test

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

 Date     : October 2022
 Authors  : Benoit De Mezzo (benoit dot de dot mezzo at oslandia dot com)
*/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/conf"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/CrunchyData/pg_featureserv/internal/service"
	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
	"github.com/jackc/pgx/v4/pgxpool"
	log "github.com/sirupsen/logrus"
)

// ...
var hTest util.HttpTesting
var db *pgxpool.Pool
var cat data.Catalog

// base struct for all test
type DbTests struct {
	Test *testing.T
}

// Test entrypoint
func TestMain(m *testing.M) {
	conf.InitConfig("", false) // getting default configuration
	conf.Configuration.Database.AllowWrite = true

	log.Debug("init : Db/Service")
	db = util.CreateTestDb()

	cat = data.CatDBInstance()
	service.SetCatalogInstance(cat)

	hTest = util.MakeHttpTesting("http://test", "/pg_featureserv", "../../../assets", service.InitRouter("/pg_featureserv"))
	service.Initialize()

	os.Exit(m.Run())
}

// Describes all test groups
func TestRunnerHandlerDb(t *testing.T) {
	// tests intialization
	beforeRun()

	t.Run("Init", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestProperDbInit()
		afterEachRun()
	})
	t.Run("GET", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestPropertiesAllFromDbSimpleTable()
		test.TestPropertiesAllFromDbComplexTable()
		test.TestGetAllForAnyGeometryTable()
		test.TestGetFormatHandlingSuffix()
		test.TestGetCrs()
		test.TestGetWrongCrs()
		afterEachRun()
	})
	t.Run("DELETE", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestDeleteFeatureDb()
		afterEachRun()
	})
	t.Run("PUT", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestSimpleReplaceFeatureSuccessDb()
		test.TestGetComplexCollectionReplaceSchema()
		test.TestReplaceAnyGeometryFeatureDb()
		test.TestReplaceComplexFeatureDb()
		test.TestReplaceComplexFeatureDbCrs()
		test.TestReplaceComplexFeatureDbWrongCrs()
		afterEachRun()
	})
	t.Run("POST", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestCreateSimpleFeatureWithBadGeojsonInputDb()
		test.TestCreateSimpleFeatureDb()
		test.TestCreateSuperSimpleFeatureDb()
		test.TestCreateComplexFeatureDb()
		test.TestCreateAnyGeometryFeatureDb()
		test.TestGetComplexCollectionCreateSchema()
		test.TestCreateFeatureCrsDb()
		test.TestCreateFeatureWrongCrsDb()
		afterEachRun()
	})
	t.Run("UPDATE", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestGetComplexCollectionUpdateSchema()
		test.TestUpdateComplexFeatureDb()
		test.TestUpdateSimpleFeatureDb()
		test.TestUpdateSimpleFeatureNoPropDb()
		test.TestUpdateComplexFeatureDbCrs()
		test.TestUpdateComplexFeatureDbWrongCrs()
		test.TestUpdateAnyGeometryFeatureDb()
		afterEachRun()
	})
	t.Run("CACHE-ETAGS", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestCacheActivationDb()
		test.TestLastModifiedDb()
		test.TestEtagDb()
		test.TestWeakEtagStableOnRequestsDb()
		test.TestEtagHeaderIfNonMatchMalformedEtagDb()
		test.TestEtagHeaderIfNonMatchVariousEtagsDb()
		test.TestEtagHeaderIfNonMatchWeakEtagDb()
		test.TestEtagHeaderIfMatchDb()
		test.TestEtagReplaceFeatureDb()
		afterEachRun()
	})
	t.Run("Listen", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		// Only starting to listen now because beforeEachRun and afterEachRun break the cache
		// (too many INSERTs and DELETEs at the same time)
		cat.Initialize(nil, nil)
		test.TestCacheSizeIncreaseAfterCreate()
		test.TestCacheSizeIncreaseAfterCreateComplex()
		test.TestCacheSizeDecreaseAfterDelete()
		test.TestCacheModifiedAfterUpdate()
		test.TestMultipleNotificationAfterCreate()
		afterEachRun()
	})
	t.Run("HEADER-IF-NON-MATCH", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestGetFeatureIfNoneMatchStaValueWithEtagPresentInCacheDb()
		test.TestGetFeatureIfNoneMatchStaValueWithNoEtagDb()
		test.TestGetFeatureIfNonMatchAfterReplaceDb()
		test.TestUpdateFeatureIfNoneMatchWithEtagPresentInCacheDb()
		test.TestUpdateFeatureIfNoneMatchWithEtagNotDetectedDb()
		test.TestUpdateFeatureIfNoneMatchStarValueWithExistingRepresentationInCacheDb()
		test.TestUpdateFeatureIfNoneMatchStarValueWithNoRepresentationInCacheDb()
		test.TestReplaceFeatureIfNoneMatchWithEtagPresentInCacheDb()
		test.TestReplaceFeatureIfNoneMatchWithEtagNotDetectedDb()
		test.TestReplaceFeatureIfNoneMatchStarValueWithNoRepresentationInCacheDb()
		test.TestReplaceFeatureIfNoneMatchStarValueWithExistingRepresentationInCacheDb()
		afterEachRun()
	})
	t.Run("LOD", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestGeometrySimplificationSingleFeature()
		test.TestGeometrySimplificationSeveralFeatures()
		test.TestGeometrySimplificationNegativeValue()
		test.TestGeometrySimplificationWrongFloatSeparatorValue()
		test.TestGeometrySimplificationVariousSimplificationValues()
		afterEachRun()
	})

	t.Run("SPECIAL_SCHEMA_TABLE_COLUMN", func(t *testing.T) {
		beforeEachRun()
		test := DbTests{Test: t}
		test.TestSpecialSchemaTableColumnName()
		afterEachRun()
	})

	// after tests cleaning
	afterRun()
}

// Run before all tests
func beforeRun() {
	log.Debug("beforeRun")
	// some stuff...
}

// Run after all tests
func afterRun() {
	log.Debug("afterRun")
	cat.Close()
	// close Db
	util.CloseTestDb(db)
}

// Run before each test
func beforeEachRun() {
	log.Debug("beforeEachRun")
	// drop and create table
	util.InsertSimpleDataset(db, "public")
	util.InsertSuperSimpleDataset(db, "public", "mock_ssimple")
	util.InsertComplexDataset(db, "complex")
	util.InsertSuperSimpleDataset(db, util.SpecialSchemaStr, util.SpecialTableStr)

}

// Run after each test
func afterEachRun() {
	log.Debug("afterEachRun")
	// some stuff...
}

// Check if item is available and is not empty
// (copy from service/handler_test.go)
func checkItem(t *testing.T, tableName string, id int) []byte {
	return checkItemWithGeom(t, tableName, id, "")
}

// Check if item is available and is not empty
// (copy from service/handler_test.go)
func checkItemWithGeom(t *testing.T, tableName string, id int, geomType string) []byte {
	path := fmt.Sprintf("/collections/%v/items/%d", url.QueryEscape(tableName), id)
	resp := hTest.DoRequest(t, path)
	body, _ := ioutil.ReadAll(resp.Body)

	var v api.GeojsonFeatureData
	errUnMarsh := json.Unmarshal(body, &v)
	util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

	util.Equals(t, "Feature", v.Type, "feature type")
	if len(geomType) > 0 {
		util.Equals(t, geomType, v.Geom.Type, "geometry type")
	}

	actId, _ := strconv.Atoi(v.ID)
	util.Equals(t, id, actId, "feature id")

	tbl, _ := service.CatalogInstance().TableByName(tableName)
	util.Equals(t, len(tbl.Columns)-1, len(v.Props), "# feature props")

	return body
}
