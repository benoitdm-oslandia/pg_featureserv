package db_bench

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

 Date     : Octobre 2022
 Authors  : Jean-philippe Bazonnais (jean-philippe dot bazonnais at ign dot fr)
*/

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

const path = "/collections/mock_a/items/1"

var etag string

func getCollectionItemFirst() error {
	req, err := http.NewRequest("GET", hTest.BasePath+path, nil)
	if err != nil {
		return err
	}
	rr := httptest.NewRecorder()
	hTest.Router.ServeHTTP(rr, req)

	status := rr.Code
	if status != 200 {
		return fmt.Errorf("Bad status : %d !", rr.Result().StatusCode)
	}

	etag = rr.Header().Get("ETag")

	return nil
}

func getCollectionItem(b *testing.B) {
	// TODO
	// on peut aussi tester la fonction handler pour avoir des metriques plus précis
	// cf. https://blog.questionable.services/article/testing-http-handlers-go/

	for i := 0; i < b.N; i++ {
		b.StartTimer()
		req, err := http.NewRequest("GET", hTest.BasePath+path, nil)
		if err != nil {
			b.Fatal(err)
		}

		// header
		if etag != "" {
			req.Header.Add("If-None-Match", etag)
		}

		rr := httptest.NewRecorder()
		// handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 	err := service.HandleItem(w, r)
		// 	if err != nil {
		// 		b.Fatal(err)
		// 	}
		// })
		// handler.ServeHTTP(rr, req)
		hTest.Router.ServeHTTP(rr, req)

		status := rr.Code
		if status != 200 && status != 304 {
			b.Fatalf("HTTP is not 200 : %d !", rr.Result().StatusCode)
		}
		b.StopTimer()
	}
}

func BenchmarkGetCollectionItem(b *testing.B) {
	err := getCollectionItemFirst()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	// benchmark à executer en positionnant la variable d'environnement :
	// 'export PGFS_CACHE=1|0'
	// 'export PGFS_CACHESIZE=1000'
	getCollectionItem(b)
}
