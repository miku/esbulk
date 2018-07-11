package esbulk

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/sethgrid/pester"
)

// FlushIndex flushes index.
func FlushIndex(idx int, options Options) error {
	server := options.Servers[idx]
	link := fmt.Sprintf("%s/%s/_flush", server, options.Index)
	req, err := http.NewRequest("POST", link, nil)
	if err != nil {
		return err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := pester.Do(req)
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

	req, err := http.NewRequest("GET", link, nil)
	if err != nil {
		return nil, err
	}
	if options.Username != "" && options.Password != "" {
		req.SetBasicAuth(options.Username, options.Password)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := pester.Do(req)
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
