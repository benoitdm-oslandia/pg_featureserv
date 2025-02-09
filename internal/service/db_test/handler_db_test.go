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
        	Amaury Zarzelli (amaury dot zarzelli at ign dot fr)
			Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
			Nicolas Revelant (nicolas dot revelant at ign dot fr)
*/

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/data"
	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
	"github.com/paulmach/orb"
)

func (t *DbTests) TestProperDbInit() {
	t.Test.Skip() // FIXME system tables not filtred correctly !?
	tables, _ := cat.Tables()
	util.Equals(t.Test, 6, len(tables), "# tables in DB")
}

func (t *DbTests) TestPropertiesAllFromDbSimpleTable() {
	t.Test.Run("TestPropertiesAllFromDbSimpleTable", func(t *testing.T) {
		rr := hTest.DoRequest(t, "/collections/mock_a/items?limit=2")

		var v api.FeatureCollection
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		// Note that JSON numbers are read as float64
		util.Equals(t, 2, len(v.Features), "# features")
		util.Equals(t, 4, len(v.Features[0].Props), "feature 1 # properties")

		util.Equals(t, "propA", v.Features[0].Props["prop_a"], "feature 1 # property A")
		util.Equals(t, 1.0, v.Features[0].Props["prop_b"], "feature 1 # property B")
		util.Equals(t, "propC", v.Features[0].Props["prop_c"], "feature 1 # property C")
		util.Equals(t, 1.0, v.Features[0].Props["prop_d"], "feature 1 # property D")
	})
}

func (t *DbTests) TestGetAllForAnyGeometryTable() {
	t.Test.Run("TestGetAllAnyGeometryTable", func(t *testing.T) {
		limit := 10
		rr := hTest.DoRequest(t, fmt.Sprintf("/collections/mock_geom/items?limit=%d", limit))

		var v api.FeatureCollection
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		// Note that JSON numbers are read as float64
		util.Equals(t, limit, len(v.Features), "# features")

		for i := 0; i < limit; i++ {
			// first 9 features are Point
			if i < 9 {
				util.Equals(t, 4, len(v.Features[i].Props), fmt.Sprintf("feature #%d properties", i))

				util.Equals(t, "propA", v.Features[i].Props["prop_a"], fmt.Sprintf("feature #%d property A", i))

				util.Equals(t, "float64", reflect.TypeOf(v.Features[i].Props["prop_b"]).String(), fmt.Sprintf("feature #%d type of property B", i))
				util.Equals(t, float64(i+1), v.Features[i].Props["prop_b"], fmt.Sprintf("feature #%d property B", i))

				util.Equals(t, "propC", v.Features[i].Props["prop_c"], fmt.Sprintf("feature #%d property C", i))

				util.Equals(t, "float64", reflect.TypeOf(v.Features[i].Props["prop_d"]).String(), fmt.Sprintf("feature #%d type of property B", i))
				util.Equals(t, float64(i+1), v.Features[i].Props["prop_d"], fmt.Sprintf("feature #%d property D", i))

				util.Equals(t, "Point", v.Features[i].Geom.Type, fmt.Sprintf("feature #%d geomtry type", i))
			} else {
				// then are the polygons starting with id 100
				util.Equals(t, "propA", v.Features[i].Props["prop_a"], fmt.Sprintf("feature #%d property A", i))

				util.Equals(t, "float64", reflect.TypeOf(v.Features[i].Props["prop_b"]).String(), fmt.Sprintf("feature #%d type of property B", i))
				util.Equals(t, float64(i+91), v.Features[i].Props["prop_b"], fmt.Sprintf("feature #%d property B", i))

				util.Equals(t, "propC", v.Features[i].Props["prop_c"], fmt.Sprintf("feature #%d property C", i))

				util.Equals(t, "float64", reflect.TypeOf(v.Features[i].Props["prop_d"]).String(), fmt.Sprintf("feature #%d type of property B", i))
				util.Equals(t, float64((i+91)%10), v.Features[i].Props["prop_d"], fmt.Sprintf("feature #%d property D", i))

				util.Equals(t, "Polygon", v.Features[i].Geom.Type, fmt.Sprintf("feature #%d geomtry type", i))
			}
		}
	})
}

func (t *DbTests) TestPropertiesAllFromDbComplexTable() {
	t.Test.Run("TestPropertiesAllFromDbComplexTable", func(t *testing.T) {
		rr := hTest.DoRequest(t, "/collections/complex.mock_multi/items?limit=5")

		var v api.FeatureCollection
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		// Note that JSON numbers are read as float64
		util.Equals(t, 5, len(v.Features), "# features")
		util.Equals(t, 9, len(v.Features[0].Props), "feature 1 # properties")

		util.Equals(t, "1", v.Features[0].Props["prop_t"].(string), "feature 1 # property text")

		tbl, _ := cat.TableByName("complex.mock_multi")
		params := data.QueryParam{Limit: 100000, Offset: 0, Crs: 4326, Columns: tbl.Columns}
		features, _ := cat.TableFeatures(context.Background(), "complex.mock_multi", &params)

		util.Equals(t, "1", features[0].Props["prop_t"].(string), "feature 1 # property text")
		util.Equals(t, int32(1), features[0].Props["prop_i"].(int32), "feature 1 # property int")
		util.Equals(t, int64(1), features[0].Props["prop_l"].(int64), "feature 1 # property long")
		util.Equals(t, float64(1.0), features[0].Props["prop_f"].(float64), "feature 1 # property float64")
		util.Equals(t, float32(1.0), features[0].Props["prop_r"].(float32), "feature 1 # property float32")
		util.Equals(t, []bool{false, true}, features[0].Props["prop_b"].([]bool), "feature 1 # property bool")
		util.Assert(t, time.Now().After(features[0].Props["prop_d"].(time.Time)), "feature 1 # property date")
		util.Equals(t, "1", features[0].Props["prop_v"].(string), "feature 1 # property varchar")

		expectJson := map[string]interface{}{
			"Name":   features[0].Props["prop_t"].(string),
			"IsDesc": features[0].Props["prop_i"].(int32)%2 == 1}
		util.Equals(t, expectJson, features[0].Props["prop_j"], "feature 1 # property json")
	})
}

func (t *DbTests) TestGetFormatHandlingSuffix() {
	t.Test.Run("TestGetFormatHandlingSuffix", func(t *testing.T) {

		// checking supported suffixes HTML and JSON, and missing suffix
		checkRouteResponseFormat(t, "/collections/public.mock_a", api.ContentTypeJSON)
		checkRouteResponseFormat(t, "/collections/public.mock_a.html", api.ContentTypeHTML)
		checkRouteResponseFormat(t, "/collections/public.mock_a.json", api.ContentTypeJSON)
		checkRouteResponseFormat(t, "/collections/mock_a/items/2?limit=100", api.ContentTypeGeoJSON)
		checkRouteResponseFormat(t, "/collections/mock_a/items/2.html?limit=100", api.ContentTypeHTML)
		checkRouteResponseFormat(t, "/collections/mock_a/items/2.json?limit=100", api.ContentTypeGeoJSON)
	})
}

func (t *DbTests) TestGetCrs() {
	t.Test.Run("TestGetCrs", func(t *testing.T) {
		rr := hTest.DoRequest(t, "/collections/mock_a/items?limit=2&crs=2154")

		var v api.FeatureCollection
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))

		util.Equals(t, 2, len(v.Features), "# features")
		util.Equals(t, 4, len(v.Features[0].Props), "feature 1 # properties")
		util.Assert(t, v.Features[0].Geom.Geometry().(orb.Point).X() < -1e+5, "feature 1 # coordinate X")
		util.Assert(t, v.Features[0].Geom.Geometry().(orb.Point).Y() > 1e+5, "feature 1 # coordinate Y")
	})
}

func (t *DbTests) TestGetWrongCrs() {
	t.Test.Run("TestGetWrongCrs", func(t *testing.T) {
		hTest.DoRequestStatus(t, "/collections/mock_a/items?limit=2&crs=3", http.StatusBadRequest)
	})
}

// sends a GET request and checks the expected format (Content-Type header) from the response
func checkRouteResponseFormat(t *testing.T, url string, expectedContentType string) {
	resp := hTest.DoRequestStatus(t, url, http.StatusOK)
	respContentType := resp.Result().Header["Content-Type"][0]
	util.Equals(t, expectedContentType, respContentType, fmt.Sprintf("wrong Content-Type: %s", respContentType))
}
