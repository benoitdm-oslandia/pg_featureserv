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
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/CrunchyData/pg_featureserv/util"
	"github.com/getkin/kin-openapi/openapi3"
)

// checks swagger api contains put operation from collection schema
func TestApiContainsMethodPut(t *testing.T) {
	resp := hTest.DoRequest(t, "/api")
	body, _ := ioutil.ReadAll(resp.Body)

	var v openapi3.Swagger
	json.Unmarshal(body, &v)

	util.Equals(t, 11, len(v.Paths), "# api paths")
	util.Equals(t, "Provides access to a single feature identitfied by {featureId} from the specified collection", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Description, "feature path present")
	util.Equals(t, "replaceCollectionFeature", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Put.OperationID, "method PUT present")
}

func TestReplaceFeature(t *testing.T) {

	var header = make(http.Header)
	header.Add("Content-Type", "application/geo+json")

	params := data.QueryParam{Limit: 100, Offset: 0}
	features, _ := catalogMock.TableFeatures(context.Background(), "mock_a", &params)
	maxId := len(features)

	featureUrl := fmt.Sprintln("/collections/mock_a/items/%d", maxId)

	{
		jsonStr := `{
			"id": 100,
			"name": "Sample",
			"email": "sample@test.com"
		}`
		rr := hTest.DoRequestMethodStatus(t, "PUT", featureUrl, []byte(jsonStr), header, http.StatusInternalServerError)
		util.Equals(t, http.StatusInternalServerError, rr.Code, "Should have failed")
		util.Assert(t, strings.Index(rr.Body.String(), fmt.Sprintf(api.ErrMsgCreateFeatureNotConform+"\n", "mock_a")) == 0, "Should have failed with not conform")
	}

	{
		var cols []string
		for _, t := range catalogMock.TableDefs {
			if t.ID == "mock_a" {
				cols = t.Columns
				break
			}
		}
		// create and put replacement point
		jsonStr := data.MakeFeatureMockPointAsJSON(100, 12, 34, cols)
		fmt.Println(jsonStr)
		hTest.DoRequestMethodStatus(t, "PUT", featureUrl, []byte(jsonStr), header, http.StatusOK)

		// check if item available and that point has been replaced
		checkItemEquals(t, maxId, jsonStr)
	}
}
