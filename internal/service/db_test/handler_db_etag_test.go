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
 Authors  : Nicolas Revelant (nicolas dot revelant at ign dot fr)
*/

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/api"
	"github.com/CrunchyData/pg_featureserv/internal/util"
)

func TestEtagDb(t *testing.T) {
	path := "/collections/mock_a/items/1"

	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)
	etagFromServer := resp.Header().Get("ETag")

	// Verifying the format and content of the strong etag received from server

	base64FormRegex := "([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)$"
	isEtagBase64, _ := regexp.MatchString(base64FormRegex, etagFromServer)
	util.Assert(t, isEtagBase64, "strong etag has to be Base64 encoded")

	decodedStrongEtag, _ := base64.StdEncoding.DecodeString(etagFromServer)
	decodedString := string(decodedStrongEtag)
	decodedString = strings.Replace(decodedString, "\"", "", -1)

	elementsNumber := len(strings.Split(decodedString, "-"))
	util.Assert(t, elementsNumber == 4, "strong etag composed of a wrong number of elements")

	s := strings.Split(decodedString, "-")
	collectionName, sridValue, format, weakEtag := s[0], s[1], s[2], s[3]
	util.Assert(t, collectionName == "mock_a", "wrong collection name")
	util.Assert(t, sridValue == "4326", "wrong SRID value")
	util.Assert(t, format == "json", "wrong format")
	weakEtagValue, _ := strconv.Atoi(weakEtag)
	util.Assert(t, reflect.TypeOf(weakEtagValue).String() == "int", "weak etag is not an integer value")
	// mock_a-4326-json-812

}

func TestWeakEtagStableOnRequestsDb(t *testing.T) {

	path := "/collections/mock_b/items/1"
	var headerJson = make(http.Header)
	headerJson.Add("Accept", api.ContentTypeJSON)

	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)
	strongEtag1 := resp.Header().Get("Etag")

	decodedStrongEtag1, _ := base64.StdEncoding.DecodeString(strongEtag1)
	decodedString1 := string(decodedStrongEtag1)
	decodedString1 = strings.Replace(decodedString1, "\"", "", -1)
	weakEtag1 := strings.Split(decodedString1, "-")[3]

	resp2 := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)
	strongEtagGml := resp2.Header().Get("Etag")

	decodedStrongEtag2, _ := base64.StdEncoding.DecodeString(strongEtagGml)
	decodedString2 := string(decodedStrongEtag2)
	decodedString2 = strings.Replace(decodedString2, "\"", "", -1)
	weakEtag2 := strings.Split(decodedString2, "-")[3]
	util.Assert(t, weakEtag1 == weakEtag2, "weak etag values are different for the same feature!")

}

func TestWeakEtagFromDifferentRepresentationsDb(t *testing.T) {

	path := "/collections/mock_b/items/1"
	var headerJson = make(http.Header)
	headerJson.Add("Accept", api.ContentTypeJSON)

	// JSON representation
	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), headerJson, http.StatusOK)
	strongEtagJson := resp.Header().Get("Etag")

	decodedStrongEtag, _ := base64.StdEncoding.DecodeString(strongEtagJson)
	decodedString := string(decodedStrongEtag)
	decodedString = strings.Replace(decodedString, "\"", "", -1)
	weakEtagJson := strings.Split(decodedString, "-")[3]

	// GML representation
	var headerGml = make(http.Header)
	// TODO
	// headerGml.Add("Accept", api.ContentTypeGML)

	path2 := "/collections/mock_b/items/1"
	resp2 := hTest.DoRequestMethodStatus(t, "GET", path2, []byte(""), headerGml, http.StatusOK)
	strongEtagGml := resp2.Header().Get("Etag")

	decodedStrongEtag2, _ := base64.StdEncoding.DecodeString(strongEtagGml)
	decodedString2 := string(decodedStrongEtag2)
	decodedString2 = strings.Replace(decodedString2, "\"", "", -1)
	weakEtagGml := strings.Split(decodedString2, "-")[3]

	// TODO -> need implementation for other formats than JSON
	// util.Assert(t, strongEtagJson != strongEtagGml, "same strong etags for different representations !")
	util.Assert(t, weakEtagJson == weakEtagGml, "weak etag values are different for the same feature!")

	// TODO path3 := "/collections/mock_b/items/1.html"
	// TODO path4 := "/collections/mock_b/items/1.json"
	// ...

}

func TestEtagHeaderIfNonMatchDb(t *testing.T) {
	path := "/collections/mock_a/items/1"

	// first GET prefetches the etag into the server cache
	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)

	strongEtagFromServer := resp.Header().Get("ETag")

	var header = make(http.Header)
	header.Add("If-None-Match", strongEtagFromServer)
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusNotModified)

}

func TestEtagHeaderIfNonMatchAfterReplaceDb(t *testing.T) {

	path := "/collections/mock_a/items/1"
	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)
	strongEtagFromServer := resp.Header().Get("ETag")

	// If-None-Match before replace
	var header = make(http.Header)
	header.Add("If-None-Match", strongEtagFromServer)
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusNotModified)

	// Replace
	var headerPut = make(http.Header)
	headerPut.Add("Accept", api.ContentTypeSchemaPatchJSON)
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
	hTest.DoRequestMethodStatus(t, "PUT", path, []byte(jsonStr), headerPut, http.StatusNoContent)

	// TODO : test will be OK once merge done with trigger

	// If-None-Match after replace
	// resp2 := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusOK)
	// strongEtagAfterPut := resp2.Header().Get("Etag")

	// util.Assert(t, strongEtagFromServer != strongEtagAfterPut, "strong etag value is still the same after replace!")

}

func TestEtagHeaderIfNonMatchMalformedEtagDb(t *testing.T) {

	path := "/collections/mock_a/items/1"

	var header = make(http.Header)
	header.Add("If-None-Match", "\"unknown_etag\"")
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusBadRequest)

	var header2 = make(http.Header)
	header2.Add("If-None-Match", "\"mock_a-4326-json-812\"")
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header2, http.StatusBadRequest)

	var header3 = make(http.Header)
	header3.Add("If-None-Match", "mock_a-4326-json-812")
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header3, http.StatusBadRequest)

}

func TestEtagHeaderIfNonMatchVariousEtagsDb(t *testing.T) {
	path := "/collections/mock_a/items/1"
	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), nil, http.StatusOK)
	encodedValidEtagFromServer := resp.Header().Get("ETag")

	wrongEtag := "mock_a-4326-json-99999"
	wrongEtag2 := "collection2-4326-html-99999"

	encodedWrongEtag1 := base64.StdEncoding.EncodeToString([]byte(wrongEtag))
	encodedWrongEtag2 := base64.StdEncoding.EncodeToString([]byte(wrongEtag2))

	// If-None-Match before replace
	var header = make(http.Header)
	etags := []string{encodedWrongEtag1, encodedWrongEtag2, encodedValidEtagFromServer}
	headerValue := strings.Join(etags, ",")
	header.Add("If-None-Match", headerValue)
	fmt.Printf("header: " + header.Get("If-None-Match"))
	hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusNotModified)
}

func TestEtagHeaderIfNonMatchWeakEtagDb(t *testing.T) {
	// TODO
}

func TestEtagHeaderIfMatchDb(t *testing.T) {
	// TODO
}

func TestEtagReplaceFeatureDb(t *testing.T) {
	path := "/collections/mock_b/items/1"
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

	resp := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusOK)
	strongEtagBeforePut := resp.Header().Get("Etag")

	decodedStrongEtag, _ := base64.StdEncoding.DecodeString(strongEtagBeforePut)
	decodedString := string(decodedStrongEtag)
	decodedString = strings.Replace(decodedString, "\"", "", -1)
	weakEtagBeforePut := strings.Split(decodedString, "-")[3]

	// Replace
	hTest.DoRequestMethodStatus(t, "PUT", path, []byte(jsonStr), header, http.StatusNoContent)

	resp2 := hTest.DoRequestMethodStatus(t, "GET", path, []byte(""), header, http.StatusOK)
	strongEtagAfterPut := resp2.Header().Get("Etag")

	decodedStrongEtag2, _ := base64.StdEncoding.DecodeString(strongEtagAfterPut)
	decodedString2 := string(decodedStrongEtag2)
	decodedString2 = strings.Replace(decodedString2, "\"", "", -1)
	weakEtagAfterPut := strings.Split(decodedString2, "-")[3]

	util.Assert(t, strongEtagBeforePut != strongEtagAfterPut, "strong etag value is still the same after replace!")
	util.Assert(t, weakEtagBeforePut != weakEtagAfterPut, "weak etag value is still the same after replace!")

}
