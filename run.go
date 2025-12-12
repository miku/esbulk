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
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"os"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	gzip "github.com/klauspost/pgzip"
	"github.com/segmentio/encoding/json"
)

var (
	// Version of application.
	Version = "0.7.26"

	ErrIndexNameRequired = errors.New("index name required")
	ErrNoWorkers         = errors.New("no workers configured")
	ErrInvalidBatchSize  = errors.New("cannot use zero batch size")
)

// Runner bundles various options. Factored out of a former main func and
// should be further split up (TODO).
type Runner struct {
	BatchSize          int
	Config             string
	CpuProfile         string
	OpType             string
	DocType            string
	File               *os.File
	FileGzipped        bool
	IdentifierField    string
	IndexName          string
	Mapping            string
	MemProfile         string
	NumWorkers         int
	Password           string
	Pipeline           string
	Purge              bool
	PurgePause         time.Duration
	RefreshInterval    string
	Scheme             string
	Servers            []string
	Settings           string
	ShowVersion        bool
	SkipBroken         bool
	Username           string
	Verbose            bool
	InsecureSkipVerify bool
	ZeroReplica        bool
}

// Run starts indexing documents from file into a given index.
func (r *Runner) Run() (err error) {
	if r.ShowVersion {
		fmt.Println(Version)
		return nil
	}
	if r.NumWorkers == 0 {
		return ErrNoWorkers
	}
	if r.BatchSize == 0 {
		return ErrInvalidBatchSize
	}
	if r.CpuProfile != "" {
		f, err := os.Create(r.CpuProfile)
		if err != nil {
			return err
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if r.IndexName == "" {
		return ErrIndexNameRequired
	}
	if r.OpType == "" {
		r.OpType = "index"
	}
	if len(r.Servers) == 0 {
		r.Servers = append(r.Servers, "http://localhost:9200")
	}
	r.Servers = mapString(prependSchema, r.Servers)
	if r.Verbose {
		log.Printf("using %d server(s)", len(r.Servers))
	}
	options := Options{
		Servers:            r.Servers,
		Index:              r.IndexName,
		OpType:             r.OpType,
		DocType:            r.DocType,
		BatchSize:          r.BatchSize,
		Verbose:            r.Verbose,
		Scheme:             "http", // deprecated
		IDField:            r.IdentifierField,
		Username:           r.Username,
		Password:           r.Password,
		Pipeline:           r.Pipeline,
		InsecureSkipVerify: r.InsecureSkipVerify,
	}
	if r.Verbose {
		log.Println(options)
	}
	if r.Purge {
		if err := DeleteIndex(options); err != nil {
			return err
		}
		time.Sleep(r.PurgePause)
	}
	var createIndexBody io.Reader
	if r.Config != "" {
		if _, err := os.Stat(r.Config); os.IsNotExist(err) {
			createIndexBody = strings.NewReader(r.Config)
		} else {
			file, err := os.Open(r.Config)
			if err != nil {
				return err
			}
			defer file.Close()
			createIndexBody = bufio.NewReader(file)
		}
	}
	if err := CreateIndex(options, createIndexBody); err != nil {
		return err
	}
	if r.Mapping != "" {
		var reader io.Reader
		if _, err := os.Stat(r.Mapping); os.IsNotExist(err) {
			reader = strings.NewReader(r.Mapping)
		} else {
			file, err := os.Open(r.Mapping)
			if err != nil {
				return err
			}
			defer file.Close()
			reader = bufio.NewReader(file)
		}
		err := PutMapping(options, reader)
		if err != nil {
			return err
		}
	}
	var (
		queue = make(chan string)
		wg    sync.WaitGroup
	)
	wg.Add(r.NumWorkers)
	for i := 0; i < r.NumWorkers; i++ {
		name := fmt.Sprintf("worker-%d", i)
		go Worker(name, options, queue, &wg)
	}
	if r.Verbose {
		log.Printf("started %d workers", r.NumWorkers)
	}
	for i, _ := range options.Servers {
		// Store number_of_replicas settings for restoration later.
		doc, err := GetSettings(i, options)
		if err != nil {
			return err
		}
		// TODO(miku): Rework this.
		numberOfReplicas := doc[options.Index].(map[string]interface{})["settings"].(map[string]interface{})["index"].(map[string]interface{})["number_of_replicas"]
		if r.Verbose {
			log.Printf("on shutdown, number_of_replicas will be set back to %s", numberOfReplicas)
		}
		if r.Verbose {
			log.Printf("on shutdown, refresh_interval will be set back to %s", r.RefreshInterval)
		}
		// Shutdown procedure. TODO(miku): Handle signals, too.
		defer func() {
			// Realtime search.
			if _, err = indexSettingsRequest(fmt.Sprintf(`{"index": {"refresh_interval": "%s"}}`, r.RefreshInterval), options); err != nil {
				return
			}
			// Reset number of replicas.
			if _, err = indexSettingsRequest(fmt.Sprintf(`{"index": {"number_of_replicas": %q}}`, numberOfReplicas), options); err != nil {
				return
			}
			// Persist documents.
			err = FlushIndex(i, options)
		}()
		// Realtime search.
		resp, err := indexSettingsRequest(`{"index": {"refresh_interval": "-1"}}`, options)
		if err != nil {
			return err
		}
		if resp.StatusCode >= 400 {
			b, err := httputil.DumpResponse(resp, true)
			if err != nil {
				return err
			}
			return fmt.Errorf("got %v: %v", resp.StatusCode, string(b))
		}
		if r.ZeroReplica {
			// Reset number of replicas.
			if _, err := indexSettingsRequest(`{"index": {"number_of_replicas": 0}}`, options); err != nil {
				return err
			}
		}
	}
	var (
		reader  = bufio.NewReader(r.File)
		counter = 0
		start   = time.Now()
	)
	if r.FileGzipped {
		zreader, err := gzip.NewReader(r.File)
		if err != nil {
			log.Fatal(err)
		}
		reader = bufio.NewReader(zreader)
	}
	if r.Verbose && r.File != nil {
		log.Printf("start reading from %v", r.File.Name())
	}
	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if line = strings.TrimSpace(line); len(line) == 0 {
			continue
		}
		if r.SkipBroken {
			if !(isJSON(line)) {
				if r.Verbose {
					fmt.Printf("skipped line [%s]\n", line)
				}
				continue
			}
		}
		queue <- line
		counter++
	}
	close(queue)
	wg.Wait()
	elapsed := time.Since(start)
	if r.MemProfile != "" {
		f, err := os.Create(r.MemProfile)
		if err != nil {
			return err
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
	if r.Verbose {
		elapsed := elapsed.Seconds()
		if elapsed < 0.1 {
			elapsed = 0.1
		}
		rate := float64(counter) / elapsed
		log.Printf("%d docs in %0.2fs at %0.3f docs/s with %d workers\n", counter, elapsed, rate, r.NumWorkers)
	}
	return nil
}

// indexSettingsRequest runs updates an index setting, given a body and
// options. Body consist of the JSON document, e.g. `{"index":
// {"refresh_interval": "1s"}}`.
func indexSettingsRequest(body string, options Options) (*http.Response, error) {
	r := strings.NewReader(body)

	rand.Seed(time.Now().Unix())
	server := options.Servers[rand.Intn(len(options.Servers))]
	link := fmt.Sprintf("%s/%s/_settings", server, options.Index)

	req, err := http.NewRequest("PUT", link, r)
	if err != nil {
		return nil, err
	}
	// Auth handling.
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")

	// Create custom HTTP client if InsecureSkipVerify is true
	client := CreateHTTPClient(options.InsecureSkipVerify)

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if options.Verbose {
		log.Printf("applied setting: %s with status %s\n", body, resp.Status)
	}
	return resp, nil
}

// isJSON checks if a string is valid json.
func isJSON(str string) bool {
	var js json.RawMessage
	return json.Unmarshal([]byte(str), &js) == nil
}

func prependSchema(s string) string {
	if !strings.HasPrefix(s, "http") {
		return fmt.Sprintf("http://%s", s)
	}
	return s
}

func mapString(f func(string) string, vs []string) (result []string) {
	for _, v := range vs {
		result = append(result, f(v))
	}
	return
}
