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
	err := json.Unmarshal(body, &v)
	assert(t, err == nil, fmt.Sprintf("%v", err))

	equals(t, 11, len(v.Paths), "# api paths")
	equals(t, "Provides access to a single feature identitfied by {featureId} from the specified collection", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Description, "path present")
	equals(t, "updateCollectionFeature", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Patch.OperationID, "method PATCH present")
}

func TestSuccessAllUpdateFeature(t *testing.T) {
	path := "/collections/mock_a/items/1"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaPatchJSON)

	jsonStr := `{
		"type": "Feature",
		"id": "1",
		"geometry": {
			"type": "Point",
			"coordinates": [
			-120,
			40
			]
		},
		"properties": {
			"prop_a": "propA...",
			"prop_b": 1,
			"prop_c": "propC...",
			"prop_d": 1
		}
	}`

	resp := doRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(body))

	var jsonData map[string]interface{}
	err := json.Unmarshal(body, &jsonData)
	assert(t, err == nil, fmt.Sprintf("%v", err))

	equals(t, "1", jsonData["ID"].(string), "feature ID")
	equals(t, "propA...", jsonData["prop_a"].(string), "feature value a")
	equals(t, 1, int(jsonData["prop_b"].(float64)), "feature value b")
	equals(t, "propC...", jsonData["prop_c"].(string), "feature value c")
	equals(t, 1, int(jsonData["prop_d"].(float64)), "feature value d")
	geom := jsonData["geometry"].(map[string]interface{})
	equals(t, "Point", geom["type"].(string), "feature Type")
	// TODO : coordinates !

}

func TestSuccessPartialUpdateFeature(t *testing.T) {
	path := "/collections/mock_a/items/2"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaPatchJSON)

	jsonStr := `{
		"type": "Feature",
		"id": "2",
		"geometry": {
			"type": "Point",
			"coordinates": [
			-120,
			40
			]
		},
		"properties": {
			"prop_a": "propA...",
			"prop_b": 2
		}
	}`

	resp := doRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(body))

	var jsonData map[string]interface{}
	err := json.Unmarshal(body, &jsonData)
	assert(t, err == nil, fmt.Sprintf("%v", err))

	equals(t, "2", jsonData["ID"].(string), "feature ID")
	equals(t, "propA...", jsonData["prop_a"].(string), "feature value a")
	equals(t, 2, int(jsonData["prop_b"].(float64)), "feature value b")
	equals(t, "propC", jsonData["prop_c"].(string), "feature value c")
	equals(t, 2, int(jsonData["prop_d"].(float64)), "feature value d")
	geom := jsonData["geometry"].(map[string]interface{})
	equals(t, "Point", geom["type"].(string), "feature Type")
	// TODO : coordinates !

}

func TestSuccessdOnlyPropUpdateFeature(t *testing.T) {
	path := "/collections/mock_a/items/3"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaPatchJSON)

	jsonStr := `{
		"type": "Feature",
		"id": "3",
		"properties": {
			"prop_a": "propA..."
		}
	}`

	resp := doRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(body))

	var jsonData map[string]interface{}
	err := json.Unmarshal(body, &jsonData)
	assert(t, err == nil, fmt.Sprintf("%v", err))

	equals(t, "3", jsonData["ID"].(string), "feature ID")
	equals(t, "propA...", jsonData["prop_a"].(string), "feature value a")
	equals(t, 3, int(jsonData["prop_b"].(float64)), "feature value b")
	equals(t, "propC", jsonData["prop_c"].(string), "feature value c")
	equals(t, 3, int(jsonData["prop_d"].(float64)), "feature value d")
}

func TestSuccessdOnlyGeomUpdateFeature(t *testing.T) {
	path := "/collections/mock_a/items/4"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaPatchJSON)

	jsonStr := `{
		"type": "Feature",
		"id": "4",
		"geometry": {
			"type": "Point",
			"coordinates": [
			-120,
			40
			]
		}
	}`

	resp := doRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	fmt.Println(string(body))

	var jsonData map[string]interface{}
	err := json.Unmarshal(body, &jsonData)
	assert(t, err == nil, fmt.Sprintf("%v", err))

	equals(t, "4", jsonData["ID"].(string), "feature ID")
	equals(t, "propA", jsonData["prop_a"].(string), "feature value a")
	equals(t, 4, int(jsonData["prop_b"].(float64)), "feature value b")
	equals(t, "propC", jsonData["prop_c"].(string), "feature value c")
	equals(t, 4, int(jsonData["prop_d"].(float64)), "feature value d")
	geom := jsonData["geometry"].(map[string]interface{})
	equals(t, "Point", geom["type"].(string), "feature Type")
	// TODO : coordinates !
}
