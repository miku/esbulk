package esbulk

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIncompleteConfig(t *testing.T) {
	var cases = []struct {
		help string
		r    Runner
		err  error
	}{
		{help: "no default index name", r: Runner{}, err: ErrNoWorkers},
		{help: "no such host", r: Runner{
			IndexName:  "abc",
			BatchSize:  10,
			NumWorkers: 1,
			Servers:    []string{"http://broken.server:9200"},
		}, err: &url.Error{Op: "Get", URL: "http://broken.server:9200/abc"}},
	}
	for _, c := range cases {
		err := c.r.Run()
		switch err.(type) {
		case nil:
			if c.err != nil {
				t.Fatalf("got: %#v, %T, want: %v [%s]", err, err, c.err, c.help)
			}
		case *url.Error:
			// For now, only check whether we expect an error.
			if c.err == nil {
				t.Fatalf("got: %#v, %T, want: %v [%s]", err, err, c.err, c.help)
			}
		default:
			if err != c.err {
				t.Fatalf("got: %#v, %T, want: %v [%s]", err, err, c.err, c.help)
			}
		}
	}
}

// startServer starts an elasticsearch server from image, exposing the http port.
func startServer(image string, httpPort int) (testcontainers.Container, error) {
	var (
		ctx   = context.Background()
		hp    = fmt.Sprintf("%d:9200/tcp", httpPort)
		parts = strings.Split(image, ":")
		tag   string
	)
	if len(parts) == 2 {
		tag = parts[1]
	} else {
		tag = "latest"
	}
	var (
		name = fmt.Sprintf("esbulk-test-es-%s", tag)
		req  = testcontainers.ContainerRequest{
			Image: image,
			Name:  name,
			Env: map[string]string{
				"discovery.type": "single-node",
			},
			ExposedPorts: []string{hp},
			WaitingFor:   wait.ForLog("started"),
		}
	)
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	// XXX: log from container to some file, logs/tc-1616683025.log ...
}

func LogReader(t *testing.T, r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("read failed: %s", err)
		return nil
	}
	t.Logf("%s", string(b))
	return b
}

func TestMinimalConfig(t *testing.T) {
	ctx := context.Background()

	var imageConf = []struct {
		Image    string
		HttpPort int
	}{
		{"elasticsearch:7.11.2", 39200},
		{"elasticsearch:6.8.14", 39200},
		{"elasticsearch:5.6.16", 39200},
	}

	for _, conf := range imageConf {
		c, err := startServer(conf.Image, conf.HttpPort)
		if err != nil {
			t.Fatalf("could not start test container: %v", err)
		}
		base := fmt.Sprintf("http://localhost:%d", conf.HttpPort)
		resp, err := http.Get(base)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()
		LogReader(t, resp.Body)
		t.Logf("server should be up at %s", base)

		var cases = []struct {
			filename  string
			indexName string
			numDocs   int64
			err       error
		}{
			{"fixtures/v10k.jsonl", "abc", 10000, nil},
		}
		for _, c := range cases {
			f, err := os.Open(c.filename)
			if err != nil {
				t.Errorf("could not open fixture: %s", c.filename)
			}
			defer f.Close()
			r := Runner{
				Servers:         []string{"http://localhost:39200"},
				BatchSize:       5000,
				NumWorkers:      1,
				RefreshInterval: "1s",
				IndexName:       "abc",
				DocType:         "x",
				File:            f,
				Verbose:         true,
			}
			err = r.Run()
			if err != c.err {
				t.Fatalf("got %v, want %v", err, c.err)
			}
			searchURL := fmt.Sprintf("%s/%s/_search", base, r.IndexName)
			resp, err = http.Get(searchURL)
			if err != nil {
				t.Errorf("could not query es: %v", err)
			}
			defer resp.Body.Close()
			b := LogReader(t, resp.Body)
			var (
				sr7 SearchResponse7
				sr6 SearchResponse6
			)
			if err = json.Unmarshal(b, &sr7); err != nil {
				if err = json.Unmarshal(b, &sr6); err != nil {
					t.Errorf("could not parse json response (6, 7): %v", err)
				} else {
					t.Log("es6 detected")
				}
			}
			if sr7.Hits.Total.Value != c.numDocs && sr6.Hits.Total != c.numDocs {
				t.Errorf("expected %d docs", c.numDocs)
			}
		}
		if err := c.Terminate(ctx); err != nil {
			t.Errorf("could not kill container: %v", err)
		}
	}
}

type SearchResponse6 struct {
	Hits struct {
		Hits []struct {
			Id     string  `json:"_id"`
			Index  string  `json:"_index"`
			Score  float64 `json:"_score"`
			Source struct {
				V string `json:"v"`
			} `json:"_source"`
			Type string `json:"_type"`
		} `json:"hits"`
		MaxScore float64 `json:"max_score"`
		Total    int64   `json:"total"`
	} `json:"hits"`
	Shards struct {
		Failed     int64 `json:"failed"`
		Skipped    int64 `json:"skipped"`
		Successful int64 `json:"successful"`
		Total      int64 `json:"total"`
	} `json:"_shards"`
	TimedOut bool  `json:"timed_out"`
	Took     int64 `json:"took"`
}

type SearchResponse7 struct {
	Hits struct {
		Hits []struct {
			Id     string  `json:"_id"`
			Index  string  `json:"_index"`
			Score  float64 `json:"_score"`
			Source struct {
				V string `json:"v"`
			} `json:"_source"`
			Type string `json:"_type"`
		} `json:"hits"`
		MaxScore float64 `json:"max_score"`
		Total    struct {
			Relation string `json:"relation"`
			Value    int64  `json:"value"`
		} `json:"total"`
	} `json:"hits"`
	Shards struct {
		Failed     int64 `json:"failed"`
		Skipped    int64 `json:"skipped"`
		Successful int64 `json:"successful"`
		Total      int64 `json:"total"`
	} `json:"_shards"`
	TimedOut bool  `json:"timed_out"`
	Took     int64 `json:"took"`
}