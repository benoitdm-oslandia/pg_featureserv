package service

/*
 Copyright 2019 - 2024 Crunchy Data Solutions, Inc.
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
	"fmt"
	"testing"

	util "github.com/CrunchyData/pg_featureserv/internal/utiltest"
)

func TestRootEmptyBasePath(t *testing.T) {
	hTestBadPath := util.MakeHttpTesting("http://test", "", InitRouter(""))
	Initialize()

	testCases := []string{
		"/",
		"/index.html",
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s route works with empty base path", tc), func(t *testing.T) {
			resp := hTestBadPath.DoRequest(t, tc)
			util.Assert(t, resp.Code == 200, "Status must be 200")
		})
	}

}
