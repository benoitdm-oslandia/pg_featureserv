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
	"github.com/getkin/kin-openapi/openapi3"
)

// checks swagger api contains method PATCH for updating a feaure from a specified collection
func TestApiContainsMethodPatchFeature(t *testing.T) {
	resp := doRequest(t, "/api")
	body, _ := ioutil.ReadAll(resp.Body)

	var v openapi3.Swagger
	json.Unmarshal(body, &v)

	equals(t, 11, len(v.Paths), "# api paths")
	equals(t, "Provides access to a single feature identitfied by {featureId} from the specified collection", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Description, "path present")
	equals(t, "updateCollectionFeature", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Patch.OperationID, "method PATCH present")
}

// TODO
func TestPatchMethodUpdateFeature(t *testing.T) {
	path := "/collections/mock_a/items/0"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaPatchJSON)

	resp := doRequestMethodStatus(t, "PATCH", path, nil, header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(body))

	var fis openapi3.Schema
	err := fis.UnmarshalJSON(body)
	if err != nil {
		t.Fatal(err)
	}

	equals(t, "https://geojson.org/schema/Point.json", fis.Properties["geometry"].Value.Items.Ref, "feature geometry")
	equals(t, "prop_a", fis.Properties["properties"].Value.Required[0], "feature required a")
	equals(t, "prop_b", fis.Properties["properties"].Value.Required[1], "feature required b")
	equals(t, "Feature", fis.Properties["type"].Value.Default, "feature required b")
}
