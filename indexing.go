package esbulk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sethgrid/pester"
)

var errParseCannotServerAddr = errors.New("cannot parse server address")

// Options represents bulk indexing options.
type Options struct {
	Servers   []string
	Index     string
	DocType   string
	BatchSize int
	Verbose   bool
	IDField   string
	Scheme    string // http or https; deprecated, use: Servers.
	Username  string
	Password  string
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

// BulkIndex takes a set of documents as strings and indexes them into elasticsearch.
func BulkIndex(docs []string, options Options) error {
	if len(docs) == 0 {
		return nil
	}

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/_bulk", server)

	var lines []string
	for _, doc := range docs {
		if len(strings.TrimSpace(doc)) == 0 {
			continue
		}

		header := fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s"}}`, options.Index, options.DocType)

		// If an "-id" is given, peek into the document to extract the ID and
		// use it in the header.
		if options.IDField != "" {
			var docmap map[string]interface{}
			dec := json.NewDecoder(strings.NewReader(doc))
			dec.UseNumber()
			if err := dec.Decode(&docmap); err != nil {
				return fmt.Errorf("failed to json decode doc: %v", err)
			}

			idstring := options.IDField // A delimiter separates string with all the fields to be used as ID.
			id := strings.FieldsFunc(idstring, func(r rune) bool { return r == ',' || r == ' ' })
			// ID can be any type at this point, try to find a string
			// representation or bail out.
			var idstr string
			var currentID string
			for counter := range id {
				currentID = id[counter]
				tokstr := strings.Split(currentID, ".")
				var TokenVal interface{}
				if len(tokstr) > 1 {
					TokenVal = nestedStr(tokstr, docmap, currentID)
					if TokenVal == nil {
						return fmt.Errorf("document has no ID field (%s): %s", currentID, doc)
					}
				} else {
					var ok2 bool
					TokenVal, ok2 = docmap[currentID]
					if !ok2 {
						return fmt.Errorf("document has no ID field (%s): %s", currentID, doc)
					}
				}
				switch tempStr1 := interface{}(TokenVal).(type) {
				case string:
					idstr = idstr + tempStr1
				case fmt.Stringer:
					idstr = idstr + tempStr1.String()
				case json.Number:
					idstr = idstr + tempStr1.String()
				default:
					return fmt.Errorf("cannot convert id value to string")
				}
			}

			header = fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s", "_id": %q}}`,
				options.Index, options.DocType, idstr)

			// Remove the IDField if it is accidentally named '_id', since
			// Field [_id] is a metadata field and cannot be added inside a
			// document.
			var flag int
			for count := range id {
				if id[count] == "_id" {
					flag = 1 // Check if any of the id fields to be concatenated is named '_id'.
				}
			}

			if flag == 1 {
				delete(docmap, "_id")
				b, err := json.Marshal(docmap)
				if err != nil {
					return err
				}
				doc = string(b)
			}
		}
		lines = append(lines, header)
		lines = append(lines, doc)
	}

	body := fmt.Sprintf("%s\n", strings.Join(lines, "\n"))

	// There are multiple ways indexing can fail, e.g. connection errors or
	// bad requests. Finally, if we have a HTTP 200, the bulk request could
	// still have failed: for that we need to decode the elasticsearch
	// response.
	req, err := http.NewRequest("POST", link, strings.NewReader(body))
	if err != nil {
		return err
	}

	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")

	client := pester.New()
	client.Concurrency = 1
	client.MaxRetries = 9
	client.Backoff = pester.ExponentialBackoff
	client.KeepLog = true
	response, err := client.Do(req)
	if err != nil {
		return err
	}

	defer response.Body.Close()

	if response.StatusCode >= 400 && response.StatusCode != 504 {
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
			log.Println("Error details: ")
			for _, v := range br.Items {
				log.Printf("  %q\n", v.IndexAction.Error)
			}
		}
		return fmt.Errorf("error during bulk operation, check error details, try less workers (lower -w value) or  increase thread_pool.bulk.queue_size in your nodes")
	}
	return nil
}

// Worker will batch index documents that come in on the lines channel.
func Worker(id string, options Options, lines chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var docs []string
	counter := 0
	for s := range lines {
		docs = append(docs, s)
		counter++
		if counter%options.BatchSize == 0 {
			msg := make([]string, len(docs))
			if n := copy(msg, docs); n != len(docs) {
				log.Fatalf("expected %d, but got %d", len(docs), n)
			}

			if err := BulkIndex(msg, options); err != nil {
				log.Fatal(err)
			}
			if options.Verbose {
				log.Printf("[%s] @%d\n", id, counter)
			}
			docs = nil
		}
	}
	if len(docs) == 0 {
		return
	}
	msg := make([]string, len(docs))
	if n := copy(msg, docs); n != len(docs) {
		log.Fatalf("expected %d, but got %d", len(docs), n)
	}

	if err := BulkIndex(msg, options); err != nil {
		log.Fatal(err)
	}
	if options.Verbose {
		log.Printf("[%s] @%d\n", id, counter)
	}
}

// PutMapping applies a mapping from a reader.
func PutMapping(options Options, body io.Reader) error {

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s/_mapping/%s", server, options.Index, options.DocType)

	if options.Verbose {
		log.Printf("applying mapping: %s", link)
	}
	req, err := http.NewRequest("PUT", link, body)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
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
func CreateIndex(options Options) error {
	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s", server, options.Index)

	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return err
	}

	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Index already exists, return.
	if resp.StatusCode == 200 {
		return nil
	}

	req, err = http.NewRequest("PUT", fmt.Sprintf("%s/%s/", server, options.Index), nil)

	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)

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
		log.Printf("es response was: %s", buf.String())
	}

	if err != nil {
		return err
	}
	defer resp.Body.Close()
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

	req, err := http.NewRequest("DELETE", link, nil)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if options.Verbose {
		log.Printf("purged index: %s", resp.Status)
	}
	return resp.Body.Close()
}
