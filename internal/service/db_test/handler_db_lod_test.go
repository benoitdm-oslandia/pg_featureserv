package db_test

/*
 Copyright 2023 Crunchy Data Solutions, Inc.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 Date     : February 2023
 Authors  : Nicolas Revelant (nicolas dot revelant at ign dot fr)
*/

import (
	"encoding/json"
	"fmt"
	"net/http"

	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// Simple unit test case ensuring that simplification is working on a single feature
func (t *DbTests) TestGeometrySimplificationSingleFeature() {
	t.Test.Run("TestGeometrySimplificationSingleFeature", func(t *testing.T) {
		rr := hTest.DoRequest(t, "/collections/mock_poly/items/6?max-allowable-offset=0.01")
		var v api.GeojsonFeatureData
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))
		util.Equals(t, 5, len(v.Geom.Geometry().(orb.Polygon)[0]), "")

	})
}

// Simple unit test case ensuring that simplification is working on several features
func (t *DbTests) TestGeometrySimplificationSeveralFeatures() {
	t.Test.Run("TestGeometrySimplificationSeveralFeatures", func(t *testing.T) {
		rr := hTest.DoRequest(t, "/collections/mock_poly/items?max-allowable-offset=0.01")
		// Feature collection
		var v api.FeatureCollection
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &v)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))
		util.Equals(t, 6, len(v.Features), "wrong number of features")
		feature := v.Features[0]
		util.Equals(t, 4, len(feature.Geom.Geometry().(orb.Polygon)[0]), "wrong number of simplified coordinates")

	})
}

// Test case with negative value as simplification factor
func (t *DbTests) TestGeometrySimplificationNegativeValue() {
	t.Test.Run("TestGeometrySimplificationNegativeValue", func(t *testing.T) {
		path := "/collections/mock_poly/items/6?max-allowable-offset=-0.01"
		// If lower thant minVal, then minValue (0) is considered
		hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusOK)
	})
}

// Test case with wrong float separator for the simplification factor
func (t *DbTests) TestGeometrySimplificationWrongFloatSeparatorValue() {
	t.Test.Run("TestGeometrySimplificationWrongFloatSeparatorValue", func(t *testing.T) {
		path := "/collections/mock_poly/items?max-allowable-offset=0,01"
		hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusBadRequest)
	})
}

// Test case with various values as simplification factor
func (t *DbTests) TestGeometrySimplificationVariousSimplificationValues() {
	t.Test.Run("TestGeometrySimplificationVariousSimplificationValues", func(t *testing.T) {
		path := "/collections/mock_poly/items/4?max-allowable-offset=0.01"
		rr := hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusOK)
		// Feature collection
		var feat api.GeojsonFeatureData
		errUnMarsh := json.Unmarshal(hTest.ReadBody(rr), &feat)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))
		util.Equals(t, 4, len(feat.Geom.Geometry().(orb.Polygon)[0]), "wrong number of simplified coordinates")

		path = "/collections/mock_poly/items/4?max-allowable-offset=0.001"
		rr = hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusOK)
		errUnMarsh = json.Unmarshal(hTest.ReadBody(rr), &feat)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))
		util.Equals(t, 10, len(feat.Geom.Geometry().(orb.Polygon)[0]), "wrong number of simplified coordinates")

		path = "/collections/mock_poly/items/4?max-allowable-offset=1"
		rr = hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusOK)
		errUnMarsh = json.Unmarshal(hTest.ReadBody(rr), &feat)
		util.Assert(t, errUnMarsh == nil, fmt.Sprintf("%v", errUnMarsh))
		util.Equals(t, (*geojson.Geometry)(nil), feat.Geom, "simplified geometry still present")

	})
}
