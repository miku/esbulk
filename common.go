package esbulk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// Application Version
const Version = "0.4.0"

var ErrParseCannotServerAddr = errors.New("cannot parse server address")

// Options represents bulk indexing options
type Options struct {
	Host      string
	Port      int
	Index     string
	DocType   string
	BatchSize int
	Verbose   bool
	IDField   string
	// http or https
	Scheme string
}

// SetServer parses out host and port for a string and sets the option values.
func (o *Options) SetServer(s string) error {
	locator, err := url.Parse(s)
	if err != nil {
		return err
	}
	o.Scheme = locator.Scheme
	parts := strings.Split(locator.Host, ":")
	switch len(parts) {
	case 1:
		log.Println(s, locator.Host, parts)
		// assume port, like https://:9200
		port, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}
		o.Port = port
	case 2:
		o.Host = parts[0]
		port, err := strconv.Atoi(parts[1])
		if err != nil {
			return err
		}
		o.Port = port
	default:
		return ErrParseCannotServerAddr
	}
	return nil
}

// BulkIndex takes a set of documents as strings and indexes them into elasticsearch
func BulkIndex(docs []string, options Options) error {
	if len(docs) == 0 {
		return nil
	}
	link := fmt.Sprintf("%s://%s:%d/%s/%s/_bulk", options.Scheme, options.Host, options.Port, options.Index, options.DocType)
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
				return err
			}

			// Find ID in the document.
			id, ok := docmap[options.IDField]
			if !ok {
				return fmt.Errorf("document has no ID field (%s): %s", options.IDField, doc)
			}

			// ID can be any type at this point, try to find a string representation or bail out.
			var idstr string
			switch t := id.(type) {
			case string:
				idstr = t
			case fmt.Stringer:
				idstr = t.String()
			case json.Number:
				idstr = t.String()
			default:
				return fmt.Errorf("cannot convert %T id value to string: %v", id, id)
			}

			header = fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}`,
				options.Index, options.DocType, idstr)

			// Remove the IDField if it is accidentally named '_id', since
			// Field [_id] is a metadata field and cannot be added inside a
			// document.
			if options.IDField == "_id" {
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
	response, err := http.Post(link, "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return fmt.Errorf("indexing failed with %d %s", response.StatusCode, http.StatusText(response.StatusCode))
	}
	return response.Body.Close()
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
			docs = docs[:0]
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

// PutMapping reads and applies a mapping from a reader.
func PutMapping(options Options, body io.Reader) error {
	link := fmt.Sprintf("%s://%s:%d/%s/_mapping/%s", options.Scheme, options.Host, options.Port, options.Index, options.DocType)
	req, err := http.NewRequest("PUT", link, body)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if options.Verbose {
		log.Printf("applied mapping: %s", resp.Status)
	}
	return resp.Body.Close()
}

// CreateIndex creates a new index.
func CreateIndex(options Options) error {
	resp, err := http.Get(fmt.Sprintf("%s://%s:%d/%s", options.Scheme, options.Host, options.Port, options.Index))
	if err != nil {
		return err
	}
	if resp.StatusCode == 200 {
		return nil
	}
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/%s/", options.Scheme, options.Host, options.Port, options.Index), nil)
	if err != nil {
		return err
	}
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 400 {
		msg, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return errors.New(string(msg))
	}
	if options.Verbose {
		log.Printf("created index: %s\n", resp.Status)
	}
	return nil
}

// DeleteIndex removes an index.
func DeleteIndex(options Options) error {
	link := fmt.Sprintf("%s://%s:%d/%s", options.Scheme, options.Host, options.Port, options.Index)
	req, err := http.NewRequest("DELETE", link, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if options.Verbose {
		log.Printf("purged index: %s", resp.Status)
	}
	return resp.Body.Close()
}
