package data

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
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

type CacheActive struct {
	entries map[string]interface{}
}

func (cache CacheActive) ContainsWeakEtag(strongEtag string) (bool, error) {

	log.Infof("strongEtag before: " + strongEtag)
	strongEtag = strings.ReplaceAll(strongEtag, "\"", "")
	log.Infof("strongEtag after replaceall: " + strongEtag)
	strongEtag = cache.DecodeBase64(strongEtag)
	log.Infof("strongEtag after decode64: " + strongEtag)
	strongEtag = strings.ReplaceAll(strongEtag, "\"", "")
	log.Infof("strongEtag after replaceall 2: " + strongEtag)
	if strongEtag == "" {
		return false, errors.New("malformed etag")
	}
	decomposedEtag := strings.Split(strongEtag, "-")
	if len(decomposedEtag) != 4 {
		return false, errors.New("malformed etag")
	}
	weakEtagValue := decomposedEtag[3]
	log.Infof("weakEtagValue: " + weakEtagValue)

	_, present := cache.entries[weakEtagValue]
	log.Infof("present ? : " + strconv.FormatBool(present))
	log.Infof(cache.ToString())
	return present, nil
}

func (cache CacheActive) AddWeakEtag(weakEtag string, etag interface{}) bool {
	cache.entries[weakEtag] = etag
	return true
}

func (cache CacheActive) ToString() string {
	b := new(bytes.Buffer)
	for key, value := range cache.entries {
		fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
	}
	return b.String()
}

func (cache CacheActive) EncodeBase64(text string) string {
	return base64.StdEncoding.EncodeToString([]byte(text))
}

func (cache CacheActive) DecodeBase64(text string) string {
	decodedString, err := base64.StdEncoding.DecodeString(text)
	if err != nil {
		return ""
	}
	return string(decodedString)
}
