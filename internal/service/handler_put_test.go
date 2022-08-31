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
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/data"
	"github.com/getkin/kin-openapi/openapi3"
)

// checks swagger api contains put operation from collection schema
func TestApiContainsMethodPut(t *testing.T) {
	resp := doRequest(t, "/api")
	body, _ := ioutil.ReadAll(resp.Body)

	var v openapi3.Swagger
	json.Unmarshal(body, &v)

	equals(t, 11, len(v.Paths), "# api paths")
	equals(t, "Provides access to a single feature identitfied by {featureId} from the specified collection", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Description, "feature path present")
	equals(t, "replaceCollectionFeature", v.Paths.Find("/collections/{collectionId}/items/{featureId}").Put.OperationID, "method PUT present")
}

func TestReplaceFeature(t *testing.T) {
	//--- retrieve max feature id
	params := data.QueryParam{
		Limit:  100,
		Offset: 0,
	}
	// create mock
	features, _ := catalogMock.TableFeatures(context.Background(), "mock_a", &params)
	maxId := len(features)

	var header = make(http.Header)
	header.Add("Content-Type", "application/geo+json")
	// create and put replacement point
	jsonStr := catalogMock.MakeFeatureMockPointAsJSON(maxId, 12, 34)
	fmt.Println(jsonStr)
	rr := doPutRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)

	// check response code 204

	// check if point can be read

	// check that point has been replaced

}
