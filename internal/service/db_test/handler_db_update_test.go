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
 Authors  : Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
*/

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/jackc/pgx/v4"
)

func (t *DbTests) TestUpdateSimpleFeatureDb() {
	t.Test.Run("TestUpdateSimpleFeatureDb", func(t *testing.T) {
		path := "/collections/mock_a/items/2"
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)

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
				"prop_a": "propJ...",
				"prop_b": 656
			}
		}`

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusNoContent)

		// check if it can be read
		feature := checkItem(t, "mock_a", 2)
		var jsonData map[string]interface{}
		errUnMarsh := json.Unmarshal(feature, &jsonData)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		util.Equals(t, "2", jsonData["id"].(string), "feature ID")
		util.Equals(t, "Feature", jsonData["type"].(string), "feature Type")
		props := jsonData["properties"].(map[string]interface{})
		util.Equals(t, "propJ...", props["prop_a"].(string), "feature value a")
		util.Equals(t, 656, int(props["prop_b"].(float64)), "feature value b")
		util.Equals(t, "propC", props["prop_c"].(string), "feature value c")
		util.Equals(t, 2, int(props["prop_d"].(float64)), "feature value d")
		geom := jsonData["geometry"].(map[string]interface{})
		util.Equals(t, "Point", geom["type"].(string), "feature Type")
		coordinate := geom["coordinates"].([]interface{})
		util.Equals(t, -120, int(coordinate[0].(float64)), "feature latitude")
		util.Equals(t, 40, int(coordinate[1].(float64)), "feature longitude")
	})
}

func (t *DbTests) TestUpdateSimpleFeatureNoPropDb() {
	t.Test.Run("TestUpdateSimpleFeatureNoPropDb", func(t *testing.T) {
		path := "/collections/mock_a/items/3"
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)

		jsonStr := `{
			"type": "Feature",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			}
		}`

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusNoContent)

		// check if it can be read
		feature := checkItem(t, "mock_a", 3)
		var jsonData map[string]interface{}
		errUnMarsh := json.Unmarshal(feature, &jsonData)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		util.Equals(t, "3", jsonData["id"].(string), "feature ID")
		util.Equals(t, "Feature", jsonData["type"].(string), "feature Type")
		props := jsonData["properties"].(map[string]interface{})
		util.Equals(t, "propA", props["prop_a"].(string), "feature value a")
		util.Equals(t, 3, int(props["prop_b"].(float64)), "feature value b")
		util.Equals(t, "propC", props["prop_c"].(string), "feature value c")
		util.Equals(t, 3, int(props["prop_d"].(float64)), "feature value d")
		geom := jsonData["geometry"].(map[string]interface{})
		util.Equals(t, "Point", geom["type"].(string), "feature Type")
		coordinate := geom["coordinates"].([]interface{})
		util.Equals(t, -120, int(coordinate[0].(float64)), "feature latitude")
		util.Equals(t, 40, int(coordinate[1].(float64)), "feature longitude")
	})
}

func (t *DbTests) TestGetComplexCollectionUpdateSchema() {
	t.Test.Run("TestGetComplexCollectionUpdateSchema", func(t *testing.T) {
		path := "/collections/complex.mock_multi/schema?type=update"
		var header = make(http.Header)
		header.Add("Accept", api.ContentTypeSchemaJSON)

		resp := hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusOK)
		body, _ := ioutil.ReadAll(resp.Body)

		var fis openapi3.Schema
		errUnMarsh := json.Unmarshal(body, &fis)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		util.Equals(t, "Data for table complex.mock_multi", fis.Description, "feature description")
		util.Equals(t, "GeoJSON Point", fis.Properties["geometry"].Value.Title, "feature geometry")

		util.Equals(t, "Feature", fis.Properties["type"].Value.Default, "feature type is feature")

		val := fis.Properties["properties"].Value
		util.Equals(t, 0, len(fis.Required), "no required field")
		util.Equals(t, "array", val.Properties["prop_b"].Value.Type, "feature type bool")
		util.Equals(t, "boolean", val.Properties["prop_b"].Value.Items.Value.Type, "feature array type bool")
		util.Equals(t, "string", val.Properties["prop_d"].Value.Type, "feature type date")
		util.Equals(t, "number", val.Properties["prop_f"].Value.Type, "feature type float64")
		util.Equals(t, "integer", val.Properties["prop_i"].Value.Type, "feature type int")
		util.Equals(t, "object", val.Properties["prop_j"].Value.Type, "feature type json")
		util.Equals(t, "integer", val.Properties["prop_l"].Value.Type, "feature type long")
		util.Equals(t, "number", val.Properties["prop_r"].Value.Type, "feature type real")
		util.Equals(t, "string", val.Properties["prop_t"].Value.Type, "feature type text")
		util.Equals(t, "string", val.Properties["prop_v"].Value.Type, "feature type varchar")
	})
}

func (t *DbTests) TestUpdateComplexFeatureDb() {
	t.Test.Run("TestUpdateComplexFeatureDb", func(t *testing.T) {
		path := "/collections/complex.mock_multi/items/100"
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)

		feat := data.MakeApiFeatureWithPointForMulti("complex.mock_multi", 99999, -50, 35)
		json, err := json.Marshal(feat)
		util.Assert(t, err == nil, fmt.Sprintf("Error marshalling feature into JSON: %v", err))

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, json, header, http.StatusNoContent)

		// check if it can be read
		checkItem(t, "complex.mock_multi", 100)
	})
}

func (t *DbTests) TestUpdateAnyGeometryFeatureDb() {
	t.Test.Run("TestUpdateAnyGeometryFeatureDb", func(t *testing.T) {
		path := "/collections/public.mock_geom/items/10"

		// check if it can be read
		checkItemWithGeom(t, "public.mock_geom", 10, "Polygon")

		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)

		// change geometry type only
		jsonStr := `{
			"type": "Feature",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			}
		}`
		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusNoContent)

		// check if it can be read
		checkItemWithGeom(t, "public.mock_geom", 10, "Point")
	})
}

func (t *DbTests) TestUpdateComplexFeatureDbCrs() {
	t.Test.Run("TestUpdateComplexFeatureDbCrs", func(t *testing.T) {
		path := "/collections/complex.mock_multi/items/100"
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)
		header.Add("Content-Crs", "2154")

		feat := data.MakeApiFeatureWithPointForMulti("complex.mock_multi", 99999, 657775, 6860705)
		json, err := json.Marshal(feat)
		util.Assert(t, err == nil, fmt.Sprintf("Error marshalling feature into JSON: %v", err))

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, json, header, http.StatusNoContent)

		// check if it can be read
		checkItem(t, "complex.mock_multi", 100)
	})
}

func (t *DbTests) TestUpdateComplexFeatureDbWrongCrs() {
	t.Test.Run("TestUpdateComplexFeatureDbWrongCrs", func(t *testing.T) {
		path := "/collections/complex.mock_multi/items/100"
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)
		header.Add("Content-Crs", "3")

		feat := data.MakeApiFeatureWithPointForMulti("complex.mock_multi", 99999, 657775, 6860705)
		json, err := json.Marshal(feat)
		util.Assert(t, err == nil, fmt.Sprintf("Error marshalling feature into JSON: %v", err))

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, json, header, http.StatusBadRequest)

		// check if it can be read
		checkItem(t, "complex.mock_multi", 100)
	})
}

func (t *DbTests) TestSpecialSchemaTableColumnName() {
	t.Test.Run("TestSpecialSchemaTableColumnName", func(t *testing.T) {
		cleanedTableNameWithSchema := pgx.Identifier{util.SpecialSchemaStr, util.SpecialTableStr}.Sanitize()
		path := url.QueryEscape(fmt.Sprintf(`/collections/%s/items/3`, cleanedTableNameWithSchema))
		var header = make(http.Header)
		header.Add("Content-Type", api.ContentTypeSchemaPatchJSON)

		jsonStr := `{
			"type": "Feature",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			}
		}`

		_ = hTest.DoRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusNoContent)

		// check if it can be read
		feature := checkItem(t, cleanedTableNameWithSchema, 3)
		var jsonData map[string]interface{}
		errUnMarsh := json.Unmarshal(feature, &jsonData)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		util.Equals(t, "3", jsonData["id"].(string), "feature ID")
		util.Equals(t, "Feature", jsonData["type"].(string), "feature Type")
		geom := jsonData["geometry"].(map[string]interface{})
		util.Equals(t, "Point", geom["type"].(string), "feature Type")
		coordinate := geom["coordinates"].([]interface{})
		util.Equals(t, -120, int(coordinate[0].(float64)), "feature latitude")
		util.Equals(t, 40, int(coordinate[1].(float64)), "feature longitude")

		props := jsonData["properties"].(map[string]interface{})
		util.Equals(t, nil, props[util.SpecialColumnStr], fmt.Sprintf("feature value %s", util.SpecialColumnStr))
	})
}
