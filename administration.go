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
	"fmt"
	"log"

	"github.com/segmentio/encoding/json"
)

// FlushIndex flushes index.
func FlushIndex(idx int, options Options) error {
	server := options.Servers[idx]
	link := fmt.Sprintf("%s/%s/_flush", server, options.Index)
	req, err := CreateHTTPRequest("POST", link, nil, options)
	if err != nil {
		return err
	}
	client := CreateHTTPClient(options.InsecureSkipVerify, 0) // Using default timeout
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if options.Verbose {
		log.Printf("index flushed: %s\n", resp.Status)
	}
	return nil
}

// GetSettings fetches the settings of the index.
func GetSettings(idx int, options Options) (map[string]interface{}, error) {
	server := options.Servers[idx]
	link := fmt.Sprintf("%s/%s/_settings", server, options.Index)

	req, err := CreateHTTPRequest("GET", link, nil, options)
	if err != nil {
		return nil, err
	}
	client := CreateHTTPClient(options.InsecureSkipVerify, 0) // Using default timeout
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("could not get settings: %s", link)
	}

	doc := make(map[string]interface{})
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("failed to decode settings: %v", err)
	}
	// Example response.
	// {
	// 	"ai": {
	// 	  "settings": {
	// 		"index": {
	// 		  "refresh_interval": "1s",
	// 		  "number_of_shards": "5",
	// 		  "provided_name": "ai",
	// 		  "creation_date": "1523372145102",
	// 		  "number_of_replicas": "1",
	// 		  "uuid": "5k-id0OZTKKU4A7DeeUNdQ",
	// 		  "version": {
	// 			"created": "6020399"
	// 		  }
	// 		}
	// 	  }
	// 	}
	// }

	return doc, nil
}
