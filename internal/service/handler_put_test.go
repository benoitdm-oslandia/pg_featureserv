package service

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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/getkin/kin-openapi/openapi3"
)

// checks swagger api contains put operation from collection schema
func TestApiContainsCollectionSchemas(t *testing.T) {
	resp := doRequest(t, "/api")
	body, _ := ioutil.ReadAll(resp.Body)

	var v openapi3.Swagger
	json.Unmarshal(body, &v)

	equals(t, 11, len(v.Paths), "# api paths")
	equals(t, "Provides access to data representation (schema) for any features in specified collection", v.Paths.Find("/collections/{collectionId}/schema").Description, "schema path present")
	equals(t, "putCollectionSchema", v.Paths.Find("/collections/{collectionId}/schema").Put.OperationID, "schema path put present")
}

func TestReplaceFeature(t *testing.T) {

}
