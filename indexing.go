package esbulk

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

var ErrParseCannotServerAddr = errors.New("cannot parse server address")

// Options represents bulk indexing options.
type Options struct {
	Host      string
	Port      int
	Index     string
	DocType   string
	BatchSize int
	Verbose   bool
	IDField   string
	Scheme    string // http or https
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
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"error"`
	}
}

// BulkResponse is a response to a bulk request.
type BulkResponse struct {
	Took      int    `json:"took"`
	HasErrors bool   `json:"errors"`
	Items     []Item `json:"items"`
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

// BulkIndex takes a set of documents as strings and indexes them into elasticsearch.
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

			idstring := options.IDField                            //A delimiter separates string with all the fields to be used as ID
			id:=strings.FieldsFunc(idstring, func (r rune) bool { return r == ',' || r == ' '})			
		//	idtemp := strings.Split(idstring, ",")
		//	id := strings.Split(idtemp, " ")
			// ID can be any type at this point, try to find a string
			// representation or bail out.
			var idstr string
			var currrentId string
			for counter:= range id{
				currrentId = id[counter]
				tokstr := strings.Split(currrentId,".")
				if len(tokstr)>1{
					thistok := tokstr[0]				
					tempStr2, ok := docmap[thistok].(map[string]interface{})
					if !ok {
						return fmt.Errorf("document has no ID field (%s): %s", currrentId, doc)
					}
					var TokenVal interface{}
					var ok1 bool
					TokenVal = tempStr2
					for count3:=1;count3<len(tokstr);count3++{
						thistok = tokstr[count3]
						TokenVal, ok1 = tempStr2[thistok]
 						if !ok1 {
							return fmt.Errorf("document has no ID field (%s): %s", currrentId, doc)
						}
						if(count3<len(tokstr)-1){
							tempStr2 = TokenVal.(map[string]interface{})
						}
					}
					switch tempStr1:= interface{}(TokenVal).(type){
						case string:
							idstr=idstr + tempStr1
						case fmt.Stringer:
							idstr = idstr + tempStr1.String()					
						case json.Number:
							idstr = idstr + tempStr1.String()
						default:
							return fmt.Errorf("cannot convert id value to string")
					}
				} else {
					var TokenVal interface{}
					var ok2 bool
					TokenVal,ok2 = docmap[currrentId]
					if !ok2 {
						return fmt.Errorf("document has no ID field here (%s): %s", currrentId, doc)
					}
					switch tempStr1:= interface{}(TokenVal).(type){
						case string:
							idstr=idstr + tempStr1
						case fmt.Stringer:
							idstr = idstr + tempStr1.String()					
						case json.Number:
							idstr = idstr + tempStr1.String()
						default:
							return fmt.Errorf("cannot convert id value to string")
				
					}
				}
			}
                                                                                                //enigma end
			header = fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s", "_id": "%s"}}`,
				options.Index, options.DocType, idstr)

			// Remove the IDField if it is accidentally named '_id', since
			// Field [_id] is a metadata field and cannot be added inside a
			// document.
			var flag int = 0
			for count:= range id{
				if id[count]=="_id"{
					flag = 1                          //check if any of the id fields to be concatenated is named '_id'
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

	response, err := http.DefaultClient.Do(req)
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
		return fmt.Errorf("error during bulk operation, try less workers (lower -w value) or increase thread_pool.bulk.queue_size in your nodes")
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
	link := fmt.Sprintf("%s://%s:%d/%s/_mapping/%s", options.Scheme, options.Host, options.Port, options.Index, options.DocType)
	req, err := http.NewRequest("PUT", link, body)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
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
	link := fmt.Sprintf("%s://%s:%d/%s", options.Scheme, options.Host, options.Port, options.Index)
	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return err
	}

	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// index already exists, return
	if resp.StatusCode == 200 {
		return nil
	}

	req, err = http.NewRequest("PUT", fmt.Sprintf("%s://%s:%d/%s/", options.Scheme, options.Host, options.Port, options.Index), nil)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	resp, err = http.DefaultClient.Do(req)
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
	link := fmt.Sprintf("%s://%s:%d/%s", options.Scheme, options.Host, options.Port, options.Index)
	req, err := http.NewRequest("DELETE", link, nil)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
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
