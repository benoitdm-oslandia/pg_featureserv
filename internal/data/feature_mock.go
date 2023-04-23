package data

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
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	orb "github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

// mock object used in catalog_mock
type featureMock struct {
	api.GeojsonFeatureData
}

func (fm *featureMock) toJSON(propNames []string) string {
	props := fm.Props
	if propNames != nil {
		props = fm.extractProperties(propNames)
	}
	return api.MakeGeojsonFeatureJSON("", fm.ID, *fm.Geom, props, fm.WeakEtag.Etag, fm.WeakEtag.LastModified)
}

func (fm *featureMock) extractProperties(propNames []string) map[string]interface{} {
	props := make(map[string]interface{})
	for _, name := range propNames {
		val, err := fm.getProperty(name)
		if err != nil {
			// panic to avoid having to return error
			panic(fmt.Errorf("Unknown property: %v", name))
		}
		props[name] = val
	}
	return props
}

func (fm *featureMock) getProperty(name string) (interface{}, error) {
	if name == "prop_a" || name == "prop_b" || name == "prop_c" || name == "prop_d" {
		return fm.Props[name], nil
	}
	return nil, fmt.Errorf("Unknown property: %v", name)
}

func (fm *featureMock) newPropsFilteredFeature(props []string) *api.GeojsonFeatureData {
	f := api.GeojsonFeatureData{
		Type:  fm.Type,
		ID:    fm.ID,
		Geom:  fm.Geom,
		Props: map[string]interface{}{},
	}

	for _, p := range props {
		f.Props[p] = fm.Props[p]
	}

	return &f
}

func (fm *featureMock) isFilterMatches(filter []*PropertyFilter) bool {
	for _, cond := range filter {
		val, _ := fm.getProperty(cond.Name)
		valStr := fmt.Sprintf("%v", val)
		if cond.Value != valStr {
			return false
		}
	}
	return true
}

func doFilter(features []*featureMock, filter []*PropertyFilter) []*featureMock {
	var result []*featureMock
	for _, feat := range features {
		if feat.isFilterMatches(filter) {
			result = append(result, feat)
		}
	}
	return result
}

func doLimit(features []*featureMock, limit int, offset int) []*featureMock {
	start := 0
	end := len(features)
	// handle limit/offset (offset is only respected if limit present)
	if limit < len(features) {
		start = offset
		end = offset + limit
		if end >= len(features) {
			end = len(features)
		}
	}
	return features[start:end]
}

func MakeMockFromApiFeature(geojsonFeat *api.GeojsonFeatureData) *featureMock {
	feat := featureMock{
		GeojsonFeatureData: *geojsonFeat,
	}
	return &feat
}

// make point feature for any table
func MakeMock(tableName string, id int, geom *geojson.Geometry, cols map[string]interface{}) *featureMock {
	sum := fnv.New32a()
	encodedContent, _ := json.Marshal(geom)
	sum.Write(encodedContent)

	httpDateString := api.GetCurrentHttpDate() // Last modified value

	idstr := strconv.Itoa(id)

	feat := featureMock{
		GeojsonFeatureData: *api.MakeGeojsonFeature(
			tableName,
			idstr,
			*geom,
			cols,
			fmt.Sprint(sum.Sum32()),
			httpDateString),
	}
	return &feat
}

// make point feature for any table
func MakeMockWithPoint(tableName string, id int, x float64, y float64, cols map[string]interface{}) *featureMock {
	return MakeMock(tableName, id, geojson.NewGeometry(orb.Point{x, y}), cols)
}

// make polygon feature for any table
func MakeMockWithPolygon(tableName string, id int, coords orb.Ring, cols map[string]interface{}) *featureMock {
	return MakeMock(tableName, id, geojson.NewGeometry(orb.Polygon{coords}), cols)
}

func makeMocksWithPointFor(tableType string, tableName string, extent api.Extent, nx int, ny int) []*featureMock {
	basex := extent.Minx
	basey := extent.Miny
	dx := (extent.Maxx - extent.Minx) / float64(nx)
	dy := (extent.Maxy - extent.Miny) / float64(ny)

	n := nx * ny
	features := make([]*featureMock, n)
	index := 0
	for ix := 0; ix < nx; ix++ {
		for iy := 0; iy < ny; iy++ {
			id := index + 1
			x := basex + dx*float64(ix)
			y := basey + dy*float64(iy)

			var feat *api.GeojsonFeatureData
			if tableType == "simple" {
				feat = MakeApiFeatureWithPointForSimple(tableName, id, x, y)
			} else {
				feat = MakeApiFeatureWithPointForMulti(tableName, id, x, y)
			}
			features[index] = MakeMockFromApiFeature(feat)

			index++
		}
	}
	return features
}

// Generates and returns a slice of Point typed features
// -> which coordinates are generated inside the provided extent
// -> which quantity depends on the nx and ny values provided as arguments (nx*ny)
func MakeMocksWithPointForSimple(tableName string, extent api.Extent, nx int, ny int) []*featureMock {
	return makeMocksWithPointFor("simple", tableName, extent, nx, ny)
}

// Returns a slice of Polygon typed featureMocks
func MakeMocksWithPolygonForSimple(tableName string) []*featureMock {

	polygons := make([]orb.Ring, 0)
	polygons = append(polygons, (orb.Ring{{-0.024590485281003, 49.2918461864342}, {-0.02824214022877, 49.2902093052715}, {-0.032731597583892, 49.2940548086905}, {-0.037105514267367, 49.2982628947696}, {-0.035096222035489, 49.2991273714187}, {-0.038500457450357, 49.3032655348948}, {-0.034417965728768, 49.3047607558599}, {-0.034611922456059, 49.304982637632}, {-0.028287271276391, 49.3073904622151}, {-0.022094153540685, 49.3097046833446}, {-0.022020905508067, 49.3096240670749}, {-0.019932810088915, 49.3103884833526}, {-0.013617304476105, 49.3129751788625}, {-0.010317714854534, 49.3091925467367}, {-0.006352474569531, 49.3110873002743}, {-0.001853050940172, 49.3070612288807}, {0.002381370562776, 49.3028484930665}, {-0.000840217324783, 49.3013882187799}, {-0.00068928216257, 49.3012429006019}, {-0.003864625123604, 49.3000173218511}, {-0.003918013833785, 49.2999931219338}, {-0.010095065847337, 49.2974103246769}, {-0.010150643294152, 49.2974622610823}, {-0.013587537856462, 49.2959737733625}, {-0.01384030494609, 49.2962233671643}, {-0.017222409797967, 49.294623513139}, {-0.017308576106142, 49.2947057553981}, {-0.020709238582055, 49.2930969232562}, {-0.021034503634088, 49.2933909821512}, {-0.024481057600533, 49.2917430023163}, {-0.024590485281003, 49.2918461864342}}))
	polygons = append(polygons, (orb.Ring{{0.012754827133148, 49.3067879156925}, {0.008855271114669, 49.3050781328888}, {0.004494239224312, 49.3091080209745}, {-0.000152707581678, 49.3133105602284}, {0.005720060734669, 49.3160862415579}, {0.005012790172897, 49.3167672210029}, {0.000766997696737, 49.3211596408574}, {0.007624129875227, 49.3239385018443}, {0.008367761372595, 49.3242455690107}, {0.008290411160612, 49.3243148348313}, {0.014857908580632, 49.327355944666}, {0.021563621634322, 49.330400077634}, {0.021666104647453, 49.3302974189836}, {0.024971410363691, 49.3317809883673}, {0.02492195583839, 49.3318321743075}, {0.029104098429698, 49.3336152412767}, {0.028646253682028, 49.3340827604102}, {0.035511767129074, 49.3367701742839}, {0.04198105053544, 49.3391776115466}, {0.046199095420336, 49.3352329627991}, {0.047069675744848, 49.3344290720305}, {0.048144047016136, 49.334920703514}, {0.048423560249958, 49.3346968337392}, {0.051915791431139, 49.3363621210079}, {0.056947292176151, 49.3326168697662}, {0.061993411180365, 49.3286019089077}, {0.055850651601917, 49.3253039337471}, {0.049713813923233, 49.3219158062857}, {0.049393633537099, 49.3221688494924}, {0.047471649153311, 49.3213066024438}, {0.04755106595679, 49.3212332612062}, {0.040845011450398, 49.3181905415208}, {0.040150920245632, 49.31787904142}, {0.039962885130089, 49.317782152465}, {0.04034174516319, 49.3173686114171}, {0.033626289449895, 49.3145051363955}, {0.032740557919845, 49.3141516109565}, {0.031347338613429, 49.313459605015}, {0.031235682243362, 49.3135509641281}, {0.029314267528688, 49.3127840624681}, {0.024083333873085, 49.3105820713374}, {0.02383988821816, 49.3108046457384}, {0.022989404102509, 49.3104651415232}, {0.016397609318679, 49.3078735624598}, {0.016236244414416, 49.3080276777805}, {0.013035870818624, 49.3065310213615}, {0.012754827133148, 49.3067879156925}}))
	polygons = append(polygons, (orb.Ring{{0.019797816099279, 49.325229088603}, {0.013235498621243, 49.3220984135413}, {0.006679188663454, 49.3188775447307}, {0.001751478001915, 49.3231631269776}, {0.00030826510927, 49.3244180023312}, {0.000034521402383, 49.3242899085418}, {-0.004894257776504, 49.3285751953461}, {-0.009823855515987, 49.332860261738}, {-0.003845879462176, 49.3357402000546}, {-0.004376904724334, 49.336234279179}, {0.00019267127677, 49.3382699850882}, {0.00003896662097, 49.3384130063648}, {0.006882712504834, 49.3414613328914}, {0.013584586312611, 49.3445956881043}, {0.013835900545075, 49.3443662391223}, {0.018429968444473, 49.3465144456831}, {0.019007858697842, 49.3459970497808}, {0.022212104736706, 49.3477771230593}, {0.028477356337026, 49.3513495867644}, {0.033807665316216, 49.347252820989}, {0.038724697445692, 49.3431456923271}, {0.034812389120157, 49.3408267818312}, {0.036339781995501, 49.3391292768443}, {0.040721479048813, 49.3347390581568}, {0.036808655724018, 49.3329836158413}, {0.037123735821512, 49.3326718720873}, {0.030269026676719, 49.3298048842398}, {0.023282829964216, 49.3268442840858}, {0.023162342964376, 49.3269672904862}, {0.021527329925941, 49.3262612666818}, {0.019602511201379, 49.3254039935278}, {0.019797816099279, 49.325229088603}}))

	id := 100 // arbitrary value used to populate the feature properties

	features := make([]*featureMock, 0)
	for _, coords := range polygons {
		feature := MakeApiFeatureWithPolygonForSimple(tableName, id, coords)
		features = append(features, MakeMockFromApiFeature(feature))
		id++
	}
	return features
}

// Generates and returns a slice of Point typed features
// -> which coordinates are generated inside the provided extent
// -> which quantity depends on the nx and ny values provided as arguments (nx*ny)
func MakeMocksWithPointForMulti(tableName string, extent api.Extent, nx int, ny int) []*featureMock {
	return makeMocksWithPointFor("multi", tableName, extent, nx, ny)
}

// make feature for mock_multi table
func MakeApiFeatureWithPointForMulti(tableName string, id int, x float64, y float64) *api.GeojsonFeatureData {
	idstr := strconv.Itoa(id)
	props := make(map[string]interface{})
	props["prop_t"] = idstr
	props["prop_i"] = id
	props["prop_l"] = int64(id)
	props["prop_f"] = float64(id)
	props["prop_r"] = float32(id)
	props["prop_b"] = []bool{id%2 == 0, id%2 == 1}
	props["prop_d"] = time.Now()
	props["prop_j"] = api.Sorting{Name: idstr, IsDesc: id%2 == 1}
	props["prop_v"] = idstr

	feat := MakeMockWithPoint(tableName, id, x, y, props)

	return &feat.GeojsonFeatureData
}

// make feature for mock simple tables
func MakeApiFeatureWithPointForSimple(tableName string, id int, x float64, y float64) *api.GeojsonFeatureData {
	props := make(map[string]interface{})
	props["prop_a"] = "propA"
	props["prop_b"] = id
	props["prop_c"] = "propC"
	props["prop_d"] = id % 10

	feat := MakeMockWithPoint(tableName, id, x, y, props)

	return &feat.GeojsonFeatureData
}

// make feature for mock simple tables
func MakeApiFeatureWithPolygonForSimple(tableName string, id int, coords orb.Ring) *api.GeojsonFeatureData {
	props := make(map[string]interface{})
	props["prop_a"] = "propA"
	props["prop_b"] = id
	props["prop_c"] = "propC"
	props["prop_d"] = id % 10

	feat := MakeMockWithPolygon(tableName, id, coords, props)

	return &feat.GeojsonFeatureData
}

// Returns a JSON representation of a Point typed feature
func MakeJSONWithPointForSimple(tableName string, id int, x float64, y float64) string {
	feat := MakeApiFeatureWithPointForSimple(tableName, id, x, y)
	return MakeMockFromApiFeature(feat).toJSON(nil)
}

// Returns a JSON representation of a Polygon typed feature
func MakeJSONWithPolygonForSimple(tableName string, id int, coords orb.Ring) string {
	feat := MakeApiFeatureWithPolygonForSimple(tableName, id, coords)
	return MakeMockFromApiFeature(feat).toJSON(nil)
}

// Returns a JSON representation of a Point typed feature
func MakeJSONWithPointForMulti(tableName string, id int, x float64, y float64) string {
	feat := MakeApiFeatureWithPointForMulti(tableName, id, x, y)
	return MakeMockFromApiFeature(feat).toJSON(nil)
}
