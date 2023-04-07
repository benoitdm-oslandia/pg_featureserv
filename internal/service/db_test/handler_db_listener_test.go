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
 Authors  : Amaury Zarzelli (amaury dot zarzelli at ign dot fr)

*/

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CrunchyData/pg_featureserv/internal/data"
	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
)

func (t *DbTests) TestCacheSizeIncreaseAfterCreate() {
	t.Test.Run("TestCacheSizeIncreaseAfterCreate", func(t *testing.T) {
		var header = make(http.Header)
		header.Add("Content-Type", "application/geo+json")

		//--- retrieve cache size before insert
		var sizeBefore = cat.GetCache().Size()

		//--- generate json from new object
		tableName := "public.mock_a"
		tables, _ := cat.Tables()
		var cols []string
		for _, tbl := range tables {
			if tbl.ID == tableName {
				for _, c := range tbl.Columns {
					if c != tbl.IDColumn {
						cols = append(cols, c)
					}
				}
				break
			}
		}
		jsonStr := data.MakeFeatureMockPointAsJSON(tableName, 99, 12, 34, cols)
		// -- do the request call but we have to force the catalogInstance to db during this operation
		_ = hTest.DoPostRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size after insert
		var sizeAfter = cat.GetCache().Size()

		util.Assert(t, sizeAfter > sizeBefore, "cache size augmented after one insert")
	})
}

func (t *DbTests) TestCacheSizeIncreaseAfterCreateComplex() {
	t.Test.Run("TestCacheSizeIncreaseAfterCreate", func(t *testing.T) {
		var header = make(http.Header)
		header.Add("Content-Type", "application/geo+json")

		//--- retrieve cache size before insert
		var sizeBefore = cat.GetCache().Size()

		//--- generate json from new object
		feat := util.MakeGeojsonFeatureMockPoint(99, 12, 34)
		jsonBytes, erMarsh := json.Marshal(feat)
		util.Assert(t, erMarsh == nil, fmt.Sprintf("%v", erMarsh))

		// -- do the request call but we have to force the catalogInstance to db during this operation
		_ = hTest.DoPostRequest(t, "/collections/complex.mock_multi/items", jsonBytes, header)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size after insert
		var sizeAfter = cat.GetCache().Size()

		util.Assert(t, sizeAfter > sizeBefore, "cache size augmented after one insert")
	})
}
func (t *DbTests) TestCacheSizeDecreaseAfterDelete() {
	t.Test.Run("TestCacheSizeDecreaseAfterDelete", func(t *testing.T) {
		var header = make(http.Header)
		header.Add("Content-Type", "application/geo+json")

		//--- generate json from new object
		tableName := "public.mock_a"
		tables, _ := cat.Tables()
		var cols []string
		for _, tbl := range tables {
			if tbl.ID == tableName {
				for _, c := range tbl.Columns {
					if c != tbl.IDColumn {
						cols = append(cols, c)
					}
				}
				break
			}
		}
		jsonStr := data.MakeFeatureMockPointAsJSON(tableName, 101, 12, 34, cols)
		// -- do the request call but we have to force the catalogInstance to db during this operation
		_ = hTest.DoPostRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)
		rr := hTest.DoPostRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)

		loc := rr.Header().Get("Location")
		var splittedLoc = strings.Split(loc, "/")
		var firstId, _ = strconv.Atoi(splittedLoc[len(splittedLoc)-1])

		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size before delete
		var sizeBefore = cat.GetCache().Size()

		hTest.DoDeleteRequestStatus(t, fmt.Sprintf("/collections/mock_a/items/%v", firstId), http.StatusNoContent)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size after delete
		var sizeAfter = cat.GetCache().Size()

		util.Assert(t, sizeAfter < sizeBefore, "cache size decreased after one delete")
	})
}

func (t *DbTests) TestCacheModifiedAfterUpdate() {
	t.Test.Run("TestCacheModifiedAfterUpdate", func(t *testing.T) {
		var header = make(http.Header)
		header.Add("Content-Type", "application/geo+json")

		//--- generate json from new object
		jsonStr := `{
			"type": "Feature",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			},
			"properties": {
				"prop_a": "POST",
				"prop_b": 1,
				"prop_c": "propC"
			}
		}`
		// -- do the request call but we have to force the catalogInstance to db during this operation
		rr := hTest.DoPostRequest(t, "/collections/mock_a/items", []byte(jsonStr), header)

		loc := rr.Header().Get("Location")
		var splittedLoc = strings.Split(loc, "/")
		var firstId, _ = strconv.Atoi(splittedLoc[len(splittedLoc)-1])
		time.Sleep(100 * time.Millisecond)
		//--- retrieve cache size before update
		var sizeBefore = cat.GetCache().Size()

		jsonStr = fmt.Sprintf(`{
			"type": "Feature",
			"id": "%v",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			},
			"properties": {
				"prop_a": "PUT",
				"prop_b": 1,
				"prop_c": "propC"
			}
		}`, firstId)

		path := fmt.Sprintf("/collections/mock_a/items/%v", firstId)
		hTest.DoRequestMethodStatus(t, "PUT", path, []byte(jsonStr), header, http.StatusNoContent)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)
		//--- retrieve cache size after update
		var sizeAfter1 = cat.GetCache().Size()

		util.Equals(t, sizeBefore, sizeAfter1, "cache size do not changed after update")

		jsonStr = fmt.Sprintf(`{
			"type": "Feature",
			"id": "%v",
			"geometry": {
				"type": "Point",
				"coordinates": [
				-120,
				40
				]
			},
			"properties": {
				"prop_a": "PATCH",
				"prop_b": 1,
				"prop_c": "propC"
			}
		}`, firstId)

		hTest.DoRequestMethodStatus(t, "PATCH", path, []byte(jsonStr), header, http.StatusNoContent)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size after other update
		var sizeAfter2 = cat.GetCache().Size()
		util.Equals(t, sizeBefore, sizeAfter2, "cache size do not changed after update")
	})
}

func (t *DbTests) TestMultipleNotificationAfterCreate() {
	t.Test.Run("TestMultipleNotificationAfterCreate", func(t *testing.T) {
		var header = make(http.Header)
		header.Add("Content-Type", "application/geo+json")

		//--- retrieve cache size before insert
		var sizeBefore = cat.GetCache().Size()

		//--- generate json from new object
		feats := data.MakeFeaturesMockPolygon("public.mock_poly")
		coords := orb.Ring{
			{-0.024590485281003, 49.2918461864342}, {-0.02824214022877, 49.2902093052715},
			{-0.032731597583892, 49.2940548086905}, {-0.037105514267367, 49.2982628947696},
			{-0.035096222035489, 49.2991273714187}, {-0.038500457450357, 49.3032655348948},
			{-0.034417965728768, 49.3047607558599}, {-0.034611922456059, 49.304982637632},
			{-0.028287271276391, 49.3073904622151}, {-0.022094153540685, 49.3097046833446},
			{-0.022020905508067, 49.3096240670749}, {-0.019932810088915, 49.3103884833526},
			{-0.013617304476105, 49.3129751788625}, {-0.010317714854534, 49.3091925467367},
			{-0.006352474569531, 49.3110873002743}, {-0.001853050940172, 49.3070612288807},
			{0.002381370562776, 49.3028484930665}, {-0.000840217324783, 49.3013882187799},
			{-0.00068928216257, 49.3012429006019}, {-0.003864625123604, 49.3000173218511},
			{-0.003918013833785, 49.2999931219338}, {-0.010095065847337, 49.2974103246769},
			{-0.010150643294152, 49.2974622610823}, {-0.013587537856462, 49.2959737733625},
			{-0.01384030494609, 49.2962233671643}, {-0.017222409797967, 49.294623513139},
			{-0.017308576106142, 49.2947057553981}, {-0.020709238582055, 49.2930969232562},
			{-0.021034503634088, 49.2933909821512}, {-0.024481057600533, 49.2917430023163},
			{0.012754827133148, 49.3067879156925}, {0.008855271114669, 49.3050781328888},
			{0.004494239224312, 49.3091080209745}, {-0.000152707581678, 49.3133105602284},
			{0.005720060734669, 49.3160862415579}, {0.005012790172897, 49.3167672210029},
			{0.000766997696737, 49.3211596408574}, {0.007624129875227, 49.3239385018443},
			{0.008367761372595, 49.3242455690107}, {0.008290411160612, 49.3243148348313},
			{0.014857908580632, 49.327355944666}, {0.021563621634322, 49.330400077634},
			{0.021666104647453, 49.3302974189836}, {0.024971410363691, 49.3317809883673},
			{0.02492195583839, 49.3318321743075}, {0.029104098429698, 49.3336152412767},
			{0.028646253682028, 49.3340827604102}, {0.035511767129074, 49.3367701742839},
			{0.04198105053544, 49.3391776115466}, {0.046199095420336, 49.3352329627991},
			{0.047069675744848, 49.3344290720305}, {0.048144047016136, 49.334920703514},
			{0.048423560249958, 49.3346968337392}, {0.051915791431139, 49.3363621210079},
			{0.056947292176151, 49.3326168697662}, {0.061993411180365, 49.3286019089077},
			{0.055850651601917, 49.3253039337471}, {0.049713813923233, 49.3219158062857},
			{0.049393633537099, 49.3221688494924}, {0.047471649153311, 49.3213066024438},
			{0.04755106595679, 49.3212332612062}, {0.040845011450398, 49.3181905415208},
			{0.040150920245632, 49.31787904142}, {0.039962885130089, 49.317782152465},
			{0.04034174516319, 49.3173686114171}, {0.033626289449895, 49.3145051363955},
			{0.032740557919845, 49.3141516109565}, {0.031347338613429, 49.313459605015},
			{0.031235682243362, 49.3135509641281}, {0.029314267528688, 49.3127840624681},
			{0.024083333873085, 49.3105820713374}, {0.02383988821816, 49.3108046457384},
			{0.022989404102509, 49.3104651415232}, {0.016397609318679, 49.3078735624598},
			{0.016236244414416, 49.3080276777805}, {0.013035870818624, 49.3065310213615},
			{0.019797816099279, 49.325229088603}, {0.013235498621243, 49.3220984135413},
			{0.006679188663454, 49.3188775447307}, {0.001751478001915, 49.3231631269776},
			{0.00030826510927, 49.3244180023312}, {0.000034521402383, 49.3242899085418},
			{-0.004894257776504, 49.3285751953461}, {-0.009823855515987, 49.332860261738},
			{-0.003845879462176, 49.3357402000546}, {-0.004376904724334, 49.336234279179},
			{0.00019267127677, 49.3382699850882}, {0.00003896662097, 49.3384130063648},
			{0.006882712504834, 49.3414613328914}, {0.013584586312611, 49.3445956881043},
			{0.013835900545075, 49.3443662391223}, {0.018429968444473, 49.3465144456831},
			{0.019007858697842, 49.3459970497808}, {0.022212104736706, 49.3477771230593},
			{0.028477356337026, 49.3513495867644}, {0.033807665316216, 49.347252820989},
			{0.038724697445692, 49.3431456923271}, {0.034812389120157, 49.3408267818312},
			{0.036339781995501, 49.3391292768443}, {0.040721479048813, 49.3347390581568},
			{0.036808655724018, 49.3329836158413}, {0.037123735821512, 49.3326718720873},
			{0.030269026676719, 49.3298048842398}, {0.023282829964216, 49.3268442840858},
			{0.023162342964376, 49.3269672904862}, {0.021527329925941, 49.3262612666818},
			{0.019602511201379, 49.3254039935278},
			{-0.024590485281003, 49.2918461864342},
		}

		feats[0].Geom = geojson.NewGeometry(orb.Polygon{coords, coords})
		jsonStr, err := json.Marshal(feats[0])
		util.Assert(t, err == nil, "Error marshalling feature into JSON: %v", err)

		// -- do the request call but we have to force the catalogInstance to db during this operation
		_ = hTest.DoPostRequest(t, "/collections/public.mock_poly/items", []byte(jsonStr), header)

		// Sleep in order to wait for the cache to update (parallel goroutine)
		time.Sleep(100 * time.Millisecond)

		//--- retrieve cache size after insert
		var sizeAfter = cat.GetCache().Size()

		util.Assert(t, sizeAfter > sizeBefore, fmt.Sprintf("cache size augmented after one insert: %d should > %d", sizeAfter, sizeBefore))
	})
}
