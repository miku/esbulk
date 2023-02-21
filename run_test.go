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
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"strings"
	"testing"
	"time"

	"github.com/segmentio/encoding/json"
	"github.com/sethgrid/pester"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIncompleteConfig(t *testing.T) {
	skipNoDocker(t)
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

// startServer starts an elasticsearch server from image, exposing the http
// port. Note that the Java heap required may be 2GB or more.
func startServer(ctx context.Context, image string, httpPort int) (testcontainers.Container, error) {
	var (
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
		name = fmt.Sprintf("esbulk-test-es-%s-%d", tag, time.Now().UnixNano())
		req  = testcontainers.ContainerRequest{
			Image: image,
			Name:  name,
			Env: map[string]string{
				"discovery.type": "single-node",
				// If you’re starting a single-node Elasticsearch cluster in a
				// Docker container, security will be automatically enabled and
				// configured for you. -- https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html#docker-cli-run-dev-mode
				"xpack.security.enabled": "false",
				"ES_JAVA_OPTS":           "-Xms4g -Xmx4g",
			},
			ExposedPorts: []string{hp},
			WaitingFor:   wait.ForLog("started"),
		}
	)
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ProviderType:     testcontainers.ProviderPodman,
		ContainerRequest: req,
		Started:          true,
	})
}

// logReader reads data from reader and bot logs it and returns it. Fails, if
// reading fails.
func logReader(t *testing.T, r io.Reader) []byte {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("read failed: %s", err)
		return nil
	}
	t.Logf("%s", string(b))
	return b
}

// skipNoDocker skips a test, if docker is not running. Also support podman.
func skipNoDocker(t *testing.T) {
	noDocker := false
	cmd := exec.Command("systemctl", "is-active", "docker")
	b, err := cmd.CombinedOutput()
	if err != nil {
		noDocker = true
	}
	if strings.TrimSpace(string(b)) != "active" {
		noDocker = true
	}
	if !noDocker {
		// We found some docker.
		return
	}
	// Otherwise, try podman.
	_, err = exec.LookPath("podman")
	if err == nil {
		t.Logf("podman detected")
		// DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock
		usr, err := user.Current()
		if err != nil {
			t.Logf("cannot get UID, set DOCKER_HOST manually")
		} else {
			sckt := fmt.Sprintf("unix:///run/user/%v/podman/podman.sock", usr.Uid)
			os.Setenv("DOCKER_HOST", sckt)
			t.Logf("set DOCKER_HOST to %v", sckt)
		}
		noDocker = false
	}
	if noDocker {
		t.Skipf("docker not installed or not running")
	}
}

func TestMinimalConfig(t *testing.T) {
	skipNoDocker(t)
	ctx := context.Background()
	var imageConf = []struct {
		ElasticsearchMajorVersion int
		Image                     string
		HttpPort                  int
	}{
		{2, "elasticsearch:2.3.4", 39200},
		{5, "elasticsearch:5.6.16", 39200},
		{6, "elasticsearch:6.8.14", 39200},
		{7, "elasticsearch:7.17.0", 39200}, // https://is.gd/MPwhaM, https://is.gd/RJ4LOZ, ...
		{8, "elasticsearch:8.6.0", 39200},
	}
	log.Printf("testing %d versions: %v", len(imageConf), imageConf)
	for _, conf := range imageConf {
		wrapper := func() error {
			c, err := startServer(ctx, conf.Image, conf.HttpPort)
			if err != nil {
				t.Fatalf("could not start test container for %v: %v", conf.Image, err)
			}
			defer func() {
				if err := c.Terminate(ctx); err != nil {
					t.Errorf("could not kill container: %v", err)
				}
			}()
			base := fmt.Sprintf("http://localhost:%d", conf.HttpPort)
			resp, err := pester.Get(base)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			logReader(t, resp.Body)
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
					return fmt.Errorf("could not open fixture: %s", c.filename)
				}
				defer f.Close()
				r := Runner{
					Servers:         []string{"http://localhost:39200"},
					BatchSize:       5000,
					NumWorkers:      1,
					RefreshInterval: "1s",
					IndexName:       "abc",
					File:            f,
					Verbose:         true,
				}
				if conf.ElasticsearchMajorVersion < 7 {
					r.DocType = "any" // deprecated with ES7, fails with ES8
				}
				err = r.Run()
				if err != c.err {
					return fmt.Errorf("got %v, want %v", err, c.err)
				}
				searchURL := fmt.Sprintf("%s/%s/_search", base, r.IndexName)
				resp, err = pester.Get(searchURL)
				if err != nil {
					return fmt.Errorf("could not query es: %v", err)
				}
				defer resp.Body.Close()
				b := logReader(t, resp.Body)
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
			// Save logs.
			rc, err := c.Logs(ctx)
			if err != nil {
				log.Printf("logs not available: %v", err)
			}
			if err := os.MkdirAll("logs", 0755); err != nil {
				if !os.IsExist(err) {
					log.Printf("create dir failed: %v", err)
				}
			}
			cname, err := c.Name(ctx)
			if err != nil {
				t.Logf("failed to get container name: %v", err)
			}
			fn := fmt.Sprintf("logs/%s-%s.log", time.Now().Format("20060102150405"), strings.TrimLeft(cname, "/"))
			f, err := os.Create(fn)
			if err != nil {
				log.Printf("failed to create log file: %v", err)
			}
			defer f.Close()
			log.Printf("logging to %s", fn)
			if _, err := io.Copy(f, rc); err != nil {
				log.Printf("log failed: %v", err)
			}
			return nil
		}
		if err := wrapper(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGH32(t *testing.T) {
	skipNoDocker(t)
	ctx := context.Background()
	c, err := startServer(ctx, "elasticsearch:7.17.0", 39200)
	if err != nil {
		t.Fatalf("could not start test container: %v", err)
	}
	defer func() {
		if err := c.Terminate(ctx); err != nil {
			t.Errorf("could not kill container: %v", err)
		}
	}()
	base := fmt.Sprintf("http://localhost:%d", 39200)
	resp, err := pester.Get(base)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	logReader(t, resp.Body)
	t.Logf("server should be up at %s", base)

	f, err := os.Open("fixtures/v10k.jsonl")
	if err != nil {
		t.Errorf("could not open fixture: %v", err)
	}
	defer f.Close()
	r := Runner{
		Servers:         []string{"http://localhost:39200"},
		BatchSize:       5000,
		NumWorkers:      1,
		RefreshInterval: "1s",
		Mapping:         `{}`,
		IndexName:       "abc",
		DocType:         "any", // deprecated with ES7
		File:            f,
		Verbose:         true,
	}
	// this should fail with #32
	err = r.Run()
	if err != nil {
		t.Logf("expected err: %v", err)
	} else {
		t.Fatalf("expected fail, see #32")
	}
	// w/o doctype, we should be good
	r = Runner{
		Servers:         []string{"http://localhost:39200"},
		BatchSize:       5000,
		NumWorkers:      1,
		RefreshInterval: "1s",
		Mapping:         `{}`,
		IndexName:       "abc",
		File:            f,
		Verbose:         true,
	}
	err = r.Run()
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
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
