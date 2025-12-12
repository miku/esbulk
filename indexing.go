// Copyright 2021 by Leipzig University Library, http://ub.uni-leipzig.de
//                   The Finc Authors, http://finc.info
//                   Martin Czygan, <martin.czygan@uni-leipzig.de>
//
// This file is part of some open source application.
//
// Some open source application is free software: you can redistribute
// it and/or modify it under the terms of the GNU General Public
// License as published by the Free Software Foundation, either
// version 3 of the License, or (at your option) any later version.
//
// Some open source application is distributed in the hope that it will
// be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Foobar.  If not, see <http://www.gnu.org/licenses/>.
//
// @license GPL-3.0+ <http://spdx.org/licenses/GPL-3.0+>

package esbulk

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/encoding/json"
	"github.com/sethgrid/pester"
)

var (
	errParseCannotServerAddr = errors.New("cannot parse server address")

	// Worker errors
	ErrWorkerCopyFailed = errors.New("worker failed to copy document batch")
	ErrWorkerBulkIndex   = errors.New("worker bulk index operation failed")
)

// Options represents bulk indexing options.
type Options struct {
	Servers            []string
	Index              string
	OpType             string
	DocType            string
	BatchSize          int
	Verbose            bool
	IDField            string
	Scheme             string // http or https; deprecated, use: Servers.
	Username           string
	Password           string
	Pipeline           string
	IncludeTypeName    bool // https://www.elastic.co/blog/moving-from-types-to-typeless-apis-in-elasticsearch-7-0
	InsecureSkipVerify bool
}

// CreateHTTPClient creates a pester client with optional TLS configuration.
func CreateHTTPClient(insecureSkipVerify bool) *pester.Client {
	client := pester.New()

	if insecureSkipVerify {
		transport := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
		customClient := &http.Client{Transport: transport}
		client.EmbedHTTPClient(customClient)
	}

	return client
}

// CreateHTTPRequest builds an HTTP request with authentication and headers.
// It handles basic authentication, content-type headers, and returns a ready-to-use request.
func CreateHTTPRequest(method, url string, body io.Reader, options Options) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	// Set basic authentication if credentials are provided
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}

	// Set content type header (for requests with body)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return req, nil
}

// Item represents a bulk action.
type Item struct {
	IndexAction struct {
		Index  string `json:"_index"`
		Type   string `json:"_type"`
		ID     string `json:"_id"`
		Status int    `json:"status"`
		Error  struct {
			Type      string `json:"type"`
			Reason    string `json:"reason"`
			IndexUUID string `json:"index_uuid"`
			Shard     string `json:"shard"`
			Index     string `json:"index"`
		} `json:"error"`
	} `json:"index"`
}

// BulkResponse is a response to a bulk request.
type BulkResponse struct {
	Took      int    `json:"took"`
	HasErrors bool   `json:"errors"`
	Items     []Item `json:"items"`
}

// nestedStr handles the nested JSON values.
func nestedStr(tokstr []string, docmap map[string]interface{}, currentID string) interface{} {
	thistok := tokstr[0]
	tempStr2, ok := docmap[thistok].(map[string]interface{})
	if !ok {
		return nil
	}
	var TokenVal interface{}
	var ok1 bool
	TokenVal = tempStr2
	for count3 := 1; count3 < len(tokstr); count3++ {
		thistok = tokstr[count3]
		TokenVal, ok1 = tempStr2[thistok]
		if !ok1 {
			return nil
		}
		if count3 < len(tokstr)-1 {
			tempStr2 = TokenVal.(map[string]interface{})
		}
	}
	return TokenVal

}

// extractDocumentID extracts an ID from a JSON document based on the given ID field specification.
// The ID field can contain multiple fields separated by commas or spaces, and supports
// nested field access using dot notation (e.g., "user.id,name").
// Returns: extracted ID string, updated document (if needed), error
func extractDocumentID(doc string, idField string) (string, string, error) {
	var docmap map[string]interface{}
	dec := json.NewDecoder(strings.NewReader(doc))
	dec.UseNumber()
	if err := dec.Decode(&docmap); err != nil {
		return "", "", fmt.Errorf("failed to json decode doc: %v", err)
	}

	// Split ID field specification into individual fields
	id := strings.FieldsFunc(idField, func(r rune) bool { return r == ',' || r == ' ' })

	// Extract and concatenate values from each field
	var idstr string
	for counter := range id {
		currentID := id[counter]
		tokstr := strings.Split(currentID, ".")
		var TokenVal interface{}

		if len(tokstr) > 1 {
			// Handle nested field access
			TokenVal = nestedStr(tokstr, docmap, currentID)
			if TokenVal == nil {
				return "", "", fmt.Errorf("document has no ID field (%s): %s", currentID, doc)
			}
		} else {
			// Handle simple field access
			var ok2 bool
			TokenVal, ok2 = docmap[currentID]
			if !ok2 {
				return "", "", fmt.Errorf("document has no ID field (%s): %s", currentID, doc)
			}
		}

		// Convert value to string representation
		switch tempStr1 := interface{}(TokenVal).(type) {
		case string:
			idstr = idstr + tempStr1
		case fmt.Stringer:
			idstr = idstr + tempStr1.String()
		case json.Number:
			idstr = idstr + tempStr1.String()
		default:
			return "", "", fmt.Errorf("cannot convert id value to string")
		}
	}

	// Check if any of the fields was named '_id' (special case)
	var containsUnderscoreID bool
	for count := range id {
		if id[count] == "_id" {
			containsUnderscoreID = true
			break
		}
	}

	// Remove '_id' field from document if it was used for ID extraction
	var updatedDoc string
	if containsUnderscoreID {
		delete(docmap, "_id")
		// Marshal the updated document back to string
		marshaledDoc, err := json.Marshal(docmap)
		if err != nil {
			return "", "", err
		}
		updatedDoc = string(marshaledDoc)
	}

	return idstr, updatedDoc, nil
}

// BulkIndex takes a set of documents as strings and indexes them into elasticsearch.
func BulkIndex(docs []string, options Options) error {
	if len(docs) == 0 {
		return nil
	}

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]

	link := fmt.Sprintf("%s/_bulk", server)

	if options.Pipeline != "" {
		link = fmt.Sprintf("%s/_bulk?pipeline=%s", server, options.Pipeline)
	}

	var lines []string
	for _, doc := range docs {
		if len(strings.TrimSpace(doc)) == 0 {
			continue
		}
		var header string
		if options.DocType == "" {
			header = fmt.Sprintf(`{"%s": {"_index": "%s"}}`, options.OpType, options.Index)
		} else {
			header = fmt.Sprintf(`{"%s": {"_index": "%s", "_type": "%s"}}`, options.OpType, options.Index, options.DocType)
		}

		// If an "-id" is given, peek into the document to extract the ID and
		// use it in the header.
		if options.IDField != "" {
			idStr, updatedDoc, err := extractDocumentID(doc, options.IDField)
			if err != nil {
				return err
			}
			if updatedDoc != "" {
				doc = updatedDoc
			}

			if options.DocType == "" {
				header = fmt.Sprintf(`{"%s": {"_index": "%s", "_id": %q}}`, options.OpType, options.Index, idStr)
			} else {
				header = fmt.Sprintf(`{"%s": {"_index": "%s", "_type": "%s", "_id": %q}}`,
					options.OpType, options.Index, options.DocType, idStr)
			}
		}

		if options.OpType == "update" {
			doc = fmt.Sprintf(`{"doc": %s, "doc_as_upsert" : true}`, doc)
		}

		lines = append(lines, header, doc)
	}

	body := fmt.Sprintf("%s\n", strings.Join(lines, "\n"))
	if options.Verbose {
		log.Printf("message content-length will be %d", len(body))
	}

	// There are multiple ways indexing can fail, e.g. connection errors or
	// bad requests. Finally, if we have a HTTP 200, the bulk request could
	// still have failed: for that we need to decode the elasticsearch
	// response.
	req, err := CreateHTTPRequest("POST", link, strings.NewReader(body), options)
	if err != nil {
		return err
	}

	client := CreateHTTPClient(options.InsecureSkipVerify)

	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode >= 400 {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, response.Body); err != nil {
			return err
		}
		return fmt.Errorf("indexing failed with %d %s: %s",
			response.StatusCode, http.StatusText(response.StatusCode), buf.String())
	}

	var br BulkResponse
	if err := json.NewDecoder(response.Body).Decode(&br); err != nil {
		return err
	}
	if br.HasErrors {
		if options.Verbose {
			log.Println("error details: ")
			for _, v := range br.Items {
				log.Printf("  %q\n", v.IndexAction.Error)
			}
		}
		log.Printf("request body: %s", body)
		return fmt.Errorf("error during bulk operation, check error details; maybe try fewer workers (-w) or increase thread_pool.bulk.queue_size in your nodes")
	}
	return nil
}

// Worker will batch index documents that come in on the lines channel.
// Errors are sent to the provided error channel; the function always returns nil
// to satisfy the WaitGroup contract.
func Worker(id string, options Options, lines chan string, wg *sync.WaitGroup, errChan chan<- error) error {
	defer wg.Done()
	var docs []string
	counter := 0

	for s := range lines {
		docs = append(docs, s)
		counter++
		if counter%options.BatchSize == 0 {
			msg := make([]string, len(docs))
			if n := copy(msg, docs); n != len(docs) {
				errChan <- fmt.Errorf("worker %s: %w: expected %d, but got %d", id, ErrWorkerCopyFailed, len(docs), n)
				continue
			}

			if err := BulkIndex(msg, options); err != nil {
				errChan <- fmt.Errorf("worker %s: %w: %w", id, ErrWorkerBulkIndex, err)
				continue
			}
			if options.Verbose {
				log.Printf("[%s] @%d\n", id, counter)
			}
			docs = nil
		}
	}

	if len(docs) == 0 {
		return nil
	}

	msg := make([]string, len(docs))
	if n := copy(msg, docs); n != len(docs) {
		errChan <- fmt.Errorf("worker %s: %w: expected %d, but got %d", id, ErrWorkerCopyFailed, len(docs), n)
		return nil
	}

	if err := BulkIndex(msg, options); err != nil {
		errChan <- fmt.Errorf("worker %s: %w: %w", id, ErrWorkerBulkIndex, err)
		return nil
	}
	if options.Verbose {
		log.Printf("[%s] @%d\n", id, counter)
	}

	return nil
}

// PutMapping applies a mapping from a reader.
func PutMapping(options Options, body io.Reader) error {

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	var link string
	if options.DocType == "" {
		link = fmt.Sprintf("%s/%s/_mapping", server, options.Index)
	} else {
		if options.IncludeTypeName {
			// https://www.elastic.co/blog/moving-from-types-to-typeless-apis-in-elasticsearch-7-0
			link = fmt.Sprintf("%s/%s/_mapping/%s?include_type_name=true", server, options.Index, options.DocType)
		} else {
			link = fmt.Sprintf("%s/%s/_mapping/%s", server, options.Index, options.DocType)
		}
	}

	if options.Verbose {
		log.Printf("applying mapping: %s", link)
	}
	req, err := CreateHTTPRequest("PUT", link, body, options)
	if err != nil {
		return err
	}
	client := CreateHTTPClient(options.InsecureSkipVerify)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return err
		}
		return fmt.Errorf("failed to apply mapping with %s: %s", resp.Status, buf.String())
	}
	if options.Verbose {
		log.Printf("applied mapping: %s", resp.Status)
	}
	return resp.Body.Close()
}

// CreateIndex creates a new index.
func CreateIndex(options Options, body io.Reader) error {
	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s", server, options.Index)

	req, err := CreateHTTPRequest("GET", link, nil, options)
	if err != nil {
		return err
	}
	client := CreateHTTPClient(options.InsecureSkipVerify)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Index already exists, return.
	if resp.StatusCode == 200 {
		return nil
	}

	req, err = CreateHTTPRequest("PUT", fmt.Sprintf("%s/%s/", server, options.Index), body, options)
	if err != nil {
		return err
	}
	resp, err = client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	// Elasticsearch backwards compat.
	if resp.StatusCode == 400 {
		var errResponse struct {
			Error  string `json:"error"`
			Status int    `json:"status"`
		}
		var buf bytes.Buffer
		rdr := io.TeeReader(resp.Body, &buf)
		// Might return a 400 on "No handler found for uri" ...
		if err := json.NewDecoder(rdr).Decode(&errResponse); err == nil {
			if strings.Contains(errResponse.Error, "IndexAlreadyExistsException") {
				return nil
			}
		}
		log.Printf("elasticsearch response was: %s", buf.String())
	}
	if resp.StatusCode >= 400 {
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, resp.Body); err != nil {
			return err
		}
		return errors.New(buf.String())
	}
	if options.Verbose {
		log.Printf("created index: %s\n", resp.Status)
	}
	return nil
}

// DeleteIndex removes an index.
func DeleteIndex(options Options) error {
	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s", server, options.Index)

	req, err := CreateHTTPRequest("DELETE", link, nil, options)
	if err != nil {
		return err
	}
	client := CreateHTTPClient(options.InsecureSkipVerify)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if options.Verbose {
		log.Printf("purged index: %s", resp.Status)
	}
	return resp.Body.Close()
}
