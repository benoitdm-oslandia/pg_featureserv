package api

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
	"net/http"
	"net/url"
	"strings"
)

const (
	// ContentTypeJSON
	ContentTypeJSON = "application/json"

	// ContentTypeGeoJSON
	ContentTypeGeoJSON = "application/geo+json"

	// ContentTypeGML
	ContentTypeGML = "application/gml+xml"

	// ContentTypeSchemaJSON
	ContentTypeSchemaJSON = "application/schema+json"

	// ContentTypeSchemaPatchJSON
	ContentTypeSchemaPatchJSON = "application/merge-patch+json"

	// ContentTypeHTML
	ContentTypeHTML = "text/html"

	// ContentTypeText
	ContentTypeText = "text/plain"

	// ContentTypeSVG
	ContentTypeSVG = "image/svg+xml"

	// ContentTypeHTML
	ContentTypeOpenAPI = "application/vnd.oai.openapi+json;version=3.0"

	// FormatJSON code and extension for JSON
	FormatJSON = "json"

	// FormatHTML code and extension for HTML
	FormatHTML = "html"

	// FormatText code and extension for Text
	FormatText = "text"

	// FormatText code and extension for Text
	FormatSVG = "svg"

	// FormatJSON code and extension for JSON
	FormatSchemaJSON = "schema+json"

	// FormatXML code and extension for XML/GML
	FormatXML = "xml"
)

// RequestedFormat gets the format for a request from extension or headers
func RequestedFormat(r *http.Request) string {
	// "/collections/mock_a/items/1.dummyformat"
	// first check explicit path
	path := r.URL.EscapedPath()
	splittedPath := strings.Split(path, "/")
	pathEnd := splittedPath[len(splittedPath)-1]
	extension := ""
	if strings.Contains(pathEnd, ".") {
		splittedEnd := strings.Split(pathEnd, ".")
		splitSize := len(splittedEnd)
		if splitSize == 2 {
			extension = splittedEnd[1]
		}
	}
	if extension != "" {
		switch extension {
		case ".json":
			return FormatJSON
		case ".html":
			return FormatHTML
		case ".txt":
			return FormatText
		case ".svg":
			return FormatSVG
		default:
			return extension
		}
	}
	// Use Accept header if present
	hdrAcceptValue := r.Header.Get("Accept")
	//fmt.Println("Accept:" + hdrAccept)
	if hdrAcceptValue != "" {
		switch hdrAcceptValue {
		case ContentTypeJSON:
			return FormatJSON
		case ContentTypeSchemaJSON, ContentTypeSchemaPatchJSON:
			return FormatSchemaJSON
		case ContentTypeHTML:
			return FormatHTML
		case ContentTypeText:
			return FormatText
		case ContentTypeSVG:
			return FormatSVG
		default:
			return hdrAcceptValue
		}
	}
	return FormatJSON
}

// RequestedFormat gets the format for a request from extension or headers
func SentDataFormat(r *http.Request) string {
	// Use ContentType header if present
	hdrContentType := r.Header.Get("Content-Type")
	if strings.Contains(hdrContentType, ContentTypeGeoJSON) {
		return FormatJSON
	}
	if strings.Contains(hdrContentType, ContentTypeGML) {
		return FormatXML
	}
	return FormatJSON
}

// PathStripFormat removes a format extension from a path
func PathStripFormat(path string) string {
	if strings.HasSuffix(path, ".html") || strings.HasSuffix(path, ".json") {
		return path[0 : len(path)-5]
	}
	return path
}

// URLQuery gets the query part of a URL
func URLQuery(url *url.URL) string {
	uri := url.RequestURI()
	qloc := strings.Index(uri, "?")
	if qloc < 0 {
		return ""
	}
	query := uri[qloc+1:]
	return query
}
