package data

import (
	"bytes"
	"encoding/base64"
	"fmt"
)

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

type CachePassive struct {
	entries map[string]interface{}
}

func (cache CachePassive) ContainsWeakEtag(strongEtag string) (bool, error) {
	// _, present := cache.entries[weakETag]
	// return present
	// _, dbEtag, err := catalogInstance.TableFeature(ctx, tableName, fid, param)
	// if err != nil {
	// 	return false
	// }
	// if weakETag == dbEtag {
	// 	return true
	// }
	return false, nil
}

func (cache CachePassive) AddWeakEtag(weakEtag string, etag interface{}) bool {
	// if !cache.ContainsWeak(weakETag) {
	// 	cache.entries[weakETag] = []string{}
	// 	return true
	// }
	fmt.Print("PASSIVE")
	return false
}

func (cache CachePassive) ToString() string {
	b := new(bytes.Buffer)
	for key, value := range cache.entries {
		fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
	}
	return b.String()
}

// func (cache CachePassive) CheckStrongEtags(ctx context.Context, tableName string, fid string, param *data.QueryParam, w http.ResponseWriter, etags []string) (bool, appError) {

// for _, strongETag := range etags {

// 	strongETag := strings.ReplaceAll(strongETag, "\"", "")
// 	decomposedEtag := strings.Split(strongETag, "-")
// 	if len(decomposedEtag) != 4 {
// 		w.WriteHeader(http.StatusBadRequest)
// 		return nil, nil
// 	}

// 	weakEtagValue := decomposedEtag[3]

// 	tbl, err1 := catalogInstance.TableByName(name)
// 	if err1 != nil {
// 		return nil, appErrorInternalFmt(err1, api.ErrMsgCollectionAccess, name)
// 	}
// 	if tbl == nil {
// 		return nil, appErrorNotFoundFmt(err1, api.ErrMsgCollectionNotFound, name)
// 	}
// 	param, errQuery := createQueryParams(&reqParam, tbl.Columns, tbl.Srid)

// 	if errQuery == nil {

// 		if cache.ContainsWeak(contexte, name, fid, param, weakEtagValue) {
// 			w.WriteHeader(http.StatusNotModified)
// 			return nil
// 		}

// 	} else {
// 		return nil, appErrorInternalFmt(errQuery, api.ErrMsgInvalidQuery)
// 	}

// }
// }

func (cache CachePassive) EncodeBase64(text string) string {
	return base64.StdEncoding.EncodeToString([]byte(text))
}

func (cache CachePassive) DecodeBase64(text string) string {
	decodedString, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return ""
	}
	return string(decodedString)
}
