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

 Date     : September 2022
 Authors  : Nicolas Revelant (nicolas dot revelant at ign dot fr)
*/

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/CrunchyData/pg_featureserv/internal/util"
)

// checks swagger api contains delete feature operation from collection
func TestGetFeatureHeaderIfNoneMatchUsingWeakEtag(t *testing.T) {

	// - GET
	path := "/collections/mock_b/items/1"
	var header = make(http.Header)
	header.Add("If-None-Match", "W/\"mock_b-4326-json-123456789\"")
	hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusNotImplemented)

	header.Add("If-None-Match", "W/\"123456789\"")
	hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusNotImplemented)

	header.Add("If-None-Match", "W/\"123456789\", \"xrgnjxorthj\", \"mock_b-4326-json-123456789\"")
	hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusNotImplemented)

}

func TestGetFeatureNoHeaderCheckEtag(t *testing.T) {
	// - GET
	path := "/collections/mock_b/items/1"
	resp := hTest.DoRequestStatus(t, path, http.StatusOK)

	// - Read Strong eTag
	body := resp.Result()
	fmt.Println("------")
	fmt.Print(body)
	fmt.Print("\n")
	fmt.Println("------")
	// Check strong ETag validity
	strongEtag := resp.Result().Header.Values("ETag")[0]
	decodedString, _ := base64.StdEncoding.DecodeString(strongEtag)
	decodedStrongEtag := string(decodedString)
	decodedStrongEtag = strings.Replace(decodedStrongEtag, "\"", "", -1)
	etagElements := strings.Split(decodedStrongEtag, "-")
	util.Equals(t, 4, len(etagElements), "strong ETag has to contain 4 values")
	util.Equals(t, "mock_b-4326-json-3574564743", decodedStrongEtag, "wrong strong ETag value")

	// - Extract weak eTag from Strong eTag
	weakEtag := etagElements[3]
	util.Equals(t, 10, len(weakEtag), "wrong weak ETag string size")
	util.Equals(t, "3574564743", weakEtag, "wrong weak ETag string")

}

func TestGetFeatureHeaderIfNoneMatchMalformedEtag(t *testing.T) {

	// - GET
	path := "/collections/mock_b/items/1"
	var header = make(http.Header)
	header.Add("If-None-Match", "\"aa-mock_b-4326-json-3574564743\"")
	hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusBadRequest)

}

func TestGetFeatureHeaderIfNoneMatchWithETagInCache(t *testing.T) {

	// - GET
	path := "/collections/mock_b/items/1"
	// First GET to prefetch weak etag
	hTest.DoRequestMethodStatus(t, "GET", path, nil, nil, http.StatusOK)

	var header = make(http.Header)
	clearEtag := "\"mock_b-4326-json-3574564743\""
	encodedEtag := base64.StdEncoding.EncodeToString([]byte(clearEtag))
	header.Add("If-None-Match", encodedEtag)
	hTest.DoRequestMethodStatus(t, "GET", path, nil, header, http.StatusNotModified)

}

func TestGetFeatureHeaderIfNoneMatchWithETagNotInCache(t *testing.T) {

	// Not yet implemented

}

func TestGetFeatureHeaderIfNoneMatchWithSeveralETagWithOneInCache(t *testing.T) {

	// Not yet implemented

}

func TestPutFeatureEtag(t *testing.T) {

	// Not yet implemented
}

func TestPatchFeatureEtag(t *testing.T) {

	// Not yet implemented
}
