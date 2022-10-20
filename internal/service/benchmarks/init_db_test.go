package db_bench

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

 Date     : Octobre 2022
 Authors  : Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
*/

import (
	"os"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/CrunchyData/pg_featureserv/internal/service"
	"github.com/CrunchyData/pg_featureserv/internal/util"
	"github.com/jackc/pgx/v4/pgxpool"
)

var hTest util.HttpTesting
var db *pgxpool.Pool
var cat data.Catalog

func TestMain(m *testing.M) {
	db = util.CreateTestDb()
	defer util.CloseTestDb(db)

	cat = data.CatDBInstance()
	service.SetCatalogInstance(cat)

	hTest = util.MakeHttpTesting("http://test", "/pg_featureserv", "../../../assets", service.InitRouter("/pg_featureserv"))
	service.Initialize()

	os.Exit(m.Run())
}
