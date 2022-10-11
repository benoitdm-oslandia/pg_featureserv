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

 Date     : September 2022
 Authors  : Benoit De Mezzo (benoit dot de dot mezzo at oslandia dot com)
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/CrunchyData/pg_featureserv/internal/util"
	"github.com/getkin/kin-openapi/openapi3"
)

func TestCreateSimpleFeatureWithBadGeojsonInputDb(t *testing.T) {
	var header = make(http.Header)
	header.Add("Content-Type", "application/geo+json")

	jsonStr := `[{
		"id": 101,
		"name": "Test",
		"email": "test@test.com"
	      }, {
		"id": 102,
		"name": "Sample",
		"email": "sample@test.com"
	    }]`

	rr := hTest.DoRequestMethodStatus(t, "POST", "/collections/mock_a/items", []byte(jsonStr), header, http.StatusInternalServerError)

	util.Equals(t, http.StatusInternalServerError, rr.Code, "Should have failed")
	util.Assert(t, strings.Index(rr.Body.String(), fmt.Sprintf(api.ErrMsgCreateFeatureNotConform+"\n", "mock_a")) == 0, "Should have failed with not conform")
}

func TestCreateSimpleFeatureDb(t *testing.T) {
	var header = make(http.Header)
	header.Add("Content-Type", "application/geo+json")

	//--- retrieve max feature id before insert
	var features []*api.GeojsonFeatureData
	params := data.QueryParam{Limit: 100000, Offset: 0, Crs: 4326}
	features, _ = cat.TableFeatures(context.Background(), "mock_a", &params)
	maxIdBefore := len(features)

	//--- generate json from new object
	tables, _ := cat.Tables()
	var cols []string
	for _, t := range tables {
		if t.ID == "public.mock_a" {
			for _, c := range t.Columns {
				if c != "id" {
					cols = append(cols, c)
				}
			}
			break
		}
	}
	jsonStr := data.MakeFeatureMockPointAsJSON(99, 12, 34, cols)
	fmt.Println(jsonStr)

	// -- do the request call but we have to force the catalogInstance to db during this operation
	rr := hTest.DoPostRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)

	loc := rr.Header().Get("Location")

	//--- retrieve max feature id after insert
	features, _ = cat.TableFeatures(context.Background(), "mock_a", &params)
	maxIdAfter := len(features)

	util.Assert(t, maxIdAfter > maxIdBefore, "# feature in db")
	util.Assert(t, len(loc) > 1, "Header location must not be empty")
	util.Equals(t, fmt.Sprintf("http://test/collections/mock_a/items/%d", maxIdAfter), loc,
		"Header location must contain valid data")

	// check if it can be read
	checkItem(t, "mock_a", maxIdAfter)
}

// checks collection schema contains valid data description
func TestGetComplexCollectionCreateSchema(t *testing.T) {
	path := "/collections/mock_multi/schema?type=create"
	var header = make(http.Header)
	header.Add("Accept", api.ContentTypeSchemaJSON)

	resp := hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusOK)
	body, _ := ioutil.ReadAll(resp.Body)

	var fis openapi3.Schema
	errUnMarsh := json.Unmarshal(body, &fis)
	util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

	util.Equals(t, "Data for table public.mock_multi", fis.Description, "feature description")
	util.Equals(t, "https://geojson.org/schema/Point.json", fis.Properties["geometry"].Value.Items.Ref, "feature geometry")

	util.Equals(t, "Feature", fis.Properties["type"].Value.Default, "feature type is feature")

	val := fis.Properties["properties"].Value
	util.Equals(t, "prop_b", val.Required[0], "feature required bool")
	util.Equals(t, "array", val.Properties["prop_b"].Value.Type, "feature type bool")
	util.Equals(t, "boolean", val.Properties["prop_b"].Value.Items.Value.Type, "feature array type bool")

	util.Equals(t, "prop_d", val.Required[1], "feature required date")
	util.Equals(t, "string", val.Properties["prop_d"].Value.Type, "feature type date")

	util.Equals(t, "prop_f", val.Required[2], "feature required float64")
	util.Equals(t, "number", val.Properties["prop_f"].Value.Type, "feature type float64")

	util.Equals(t, "prop_i", val.Required[3], "feature required int")
	util.Equals(t, "integer", val.Properties["prop_i"].Value.Type, "feature type int")

	util.Equals(t, "prop_j", val.Required[4], "feature required json")
	util.Equals(t, "object", val.Properties["prop_j"].Value.Type, "feature type json")

	util.Equals(t, "prop_l", val.Required[5], "feature required long")
	util.Equals(t, "integer", val.Properties["prop_l"].Value.Type, "feature type long")

	util.Equals(t, "prop_r", val.Required[6], "feature required real")
	util.Equals(t, "number", val.Properties["prop_r"].Value.Type, "feature type real")

	util.Equals(t, "prop_t", val.Required[7], "feature required text")
	util.Equals(t, "string", val.Properties["prop_t"].Value.Type, "feature type text")
}

func TestCreateComplexFeatureDb(t *testing.T) {
	var header = make(http.Header)
	header.Add("Content-Type", "application/geo+json")

	//--- retrieve max feature id before insert
	var features []*api.GeojsonFeatureData
	params := data.QueryParam{Limit: 100000, Offset: 0, Crs: 4326}
	features, _ = cat.TableFeatures(context.Background(), "mock_multi", &params)
	maxIdBefore := len(features)

	//--- generate json from new object
	feat := util.MakeGeojsonFeatureMockPoint(99999, -50, 35)
	json, err := json.Marshal(feat)
	util.Assert(t, err == nil, fmt.Sprintf("Error marshalling feature into JSON: %v", err))
	jsonStr := string(json)

	// -- do the request call but we have to force the catalogInstance to db during this operation
	rr := hTest.DoPostRequest(t, "/collections/mock_multi/items", []byte(jsonStr), header)

	loc := rr.Header().Get("Location")

	//--- retrieve max feature id after insert
	features, _ = cat.TableFeatures(context.Background(), "mock_multi", &params)
	maxIdAfter := len(features)

	util.Assert(t, maxIdAfter > maxIdBefore, "# feature in db")
	util.Assert(t, len(loc) > 1, "Header location must not be empty")
	util.Equals(t, fmt.Sprintf("http://test/collections/mock_multi/items/%d", maxIdAfter), loc,
		"Header location must contain valid data")

	// check if it can be read
	checkItem(t, "mock_multi", maxIdAfter)
}
