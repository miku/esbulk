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
	"log"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
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
//
// The container asks for a 4g heap (see ES_JAVA_OPTS below), so the container
// runtime must have enough memory. On macOS (Apple Silicon) the default podman
// machine only has 2GiB; increase it first, e.g.:
//
//	podman machine stop && podman machine set --memory 6144 --cpus 4 && podman machine start
func startServer(ctx context.Context, image string, httpPort int) (testcontainers.Container, error) {
	var (
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
			// testcontainers-go v0.43 only accepts a bare container port
			// (or range) here; the host-side mapping is set via PortBindings
			// in the HostConfigModifier below.
			ExposedPorts: []string{"9200/tcp"},
			WaitingFor:   wait.ForLog("started"),
			// $ docker run --platform linux/amd64 m 8g -e ES_JAVA_OPTS="-Xms512m -Xmx512m" elasticsearch:2.3.4
			// library initialization failed - unable to allocate file descriptor table - out of memory#
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.CgroupnsMode = container.CgroupnsModeHost
				// Bind container 9200 to the fixed host port httpPort, so the
				// Runner can reach it at http://localhost:<httpPort>.
				hc.PortBindings = network.PortMap{
					network.MustParsePort("9200/tcp"): []network.PortBinding{{
						HostIP:   netip.MustParseAddr("0.0.0.0"),
						HostPort: strconv.Itoa(httpPort),
					}},
				}
				hc.Resources = container.Resources{
					Ulimits: []*container.Ulimit{{
						Name: "nofile",
						Hard: 65536,
						Soft: 65536,
					}},
				}
			},
		}
	)
	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ProviderType:     testcontainers.ProviderDefault,
		ContainerRequest: req,
		Started:          true,
	})
}

// logReader reads data from reader and bot logs it and returns it. Fails, if
// reading fails.
func logReader(t *testing.T, r io.Reader) []byte {
	b, err := io.ReadAll(r)
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
		// Ryuk (the testcontainers resource reaper) fails to start under
		// podman because it tries to attach to a network named "bridge",
		// which podman does not provide by default ("network not found").
		// The tests already Terminate their containers explicitly, so it is
		// safe to disable the reaper here. Respect an explicit override.
		if _, set := os.LookupEnv("TESTCONTAINERS_RYUK_DISABLED"); !set {
			os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
			t.Logf("disabled testcontainers ryuk reaper (podman)")
		}
		// The rootless podman socket lives at a Linux-specific path; on
		// macOS (and other non-Linux hosts) the socket is elsewhere (e.g.
		// under the podman machine dir), so let testcontainers auto-detect
		// it via the default docker socket instead of pointing DOCKER_HOST
		// at a path that does not exist here.
		if runtime.GOOS == "linux" {
			// DOCKER_HOST=unix:///run/user/$UID/podman/podman.sock
			usr, err := user.Current()
			if err != nil {
				t.Logf("cannot get UID, set DOCKER_HOST manually")
			} else {
				sckt := fmt.Sprintf("unix:///run/user/%v/podman/podman.sock", usr.Uid)
				os.Setenv("DOCKER_HOST", sckt)
				t.Logf("set DOCKER_HOST to %v", sckt)
			}
		} else {
			t.Logf("non-linux host (%s): leaving DOCKER_HOST to testcontainers auto-detection", runtime.GOOS)
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

		{8, "elasticsearch:8.19.23", 39200}, // https://is.gd/MPwhaM, https://is.gd/RJ4LOZ, ...
		{9, "elasticsearch:9.5.5", 39200},
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
					sr8 SearchResponse
					sr9 SearchResponse
				)
				if err = json.Unmarshal(b, &sr9); err != nil {
					if err = json.Unmarshal(b, &sr8); err != nil {
						t.Errorf("could not parse json response (8, 9): %v", err)
					} else {
						t.Log("deprecated es detected")
					}
				}
				if sr8.Hits.Total.Value != c.numDocs && sr9.Hits.Total.Value != c.numDocs {
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
	// 7.17.7 bundles a Java version, that has improved cgroups2 support
	c, err := startServer(ctx, "elasticsearch:7.17.7", 39200)
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

type SearchResponse struct {
	Hits struct {
		Hits []struct {
			ID     string  `json:"_id"`
			Index  string  `json:"_index"`
			Score  float64 `json:"_score"`
			Source struct {
				V string `json:"v"`
			} `json:"_source"`
			Type string `json:"_type,omitempty"`
		} `json:"hits"`
		MaxScore *float64 `json:"max_score"`
		Total    struct {
			Value    int64  `json:"value"`
			Relation string `json:"relation"`
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

func TestContextCancellation(t *testing.T) {
	tests := []struct {
		name        string
		cancelDelay time.Duration
		expectError bool
	}{
		{"early cancellation", 10 * time.Millisecond, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			if tt.cancelDelay > 0 {
				time.AfterFunc(tt.cancelDelay, cancel)
			}
			options := Options{
				BatchSize:      10,
				Verbose:        false, // Reduce noise in test output
				Servers:        []string{"http://localhost:9200"},
				Index:          "test-index",
				RequestTimeout: 5 * time.Second,
			}
			var (
				lines   = make(chan string, 100)
				errChan = make(chan error, 1)
				wg      sync.WaitGroup
			)
			wg.Add(1)
			for i := 0; i < 5; i++ {
				lines <- `{"test": "data"}`
			}
			close(lines)
			err := Worker(ctx, "test-worker", options, lines, &wg, errChan)
			if tt.expectError {
				select {
				case <-ctx.Done():
					t.Logf("Context cancelled as expected: %v", ctx.Err())
				case err := <-errChan:
					// May get connection errors, which is ok when context is cancelled
					t.Logf("worker error (expected): %v", err)
				case <-time.After(200 * time.Millisecond):
					t.Error("expected context cancellation but worker didn't finish")
				}
			} else {
				if err != nil {
					t.Errorf("worker failed unexpectedly: %v", err)
				}
			}
		})
	}
}

func TestWorkerRespectsImmediateCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	options := Options{
		BatchSize:      10,
		Verbose:        false,
		Servers:        []string{"http://localhost:9200"},
		Index:          "test-index",
		RequestTimeout: 30 * time.Second,
	}
	var (
		lines   = make(chan string, 100)
		errChan = make(chan error, 1)
	)
	var wg sync.WaitGroup
	wg.Add(1)
	var (
		start = time.Now()
		err   = Worker(ctx, "test-worker", options, lines, &wg, errChan)
	)
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Errorf("worker should return immediately when context is cancelled, but took %v", elapsed)
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCreateHTTPRequestWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := Options{
		Username: "testuser",
		Password: "testpass",
	}
	req, err := CreateHTTPRequestWithContext(ctx, "GET", "http://example.com", nil, options)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if req.Method != "GET" {
		t.Errorf("expected method GET, got %s", req.Method)
	}
	if req.URL.String() != "http://example.com" {
		t.Errorf("expected URL http://example.com, got %s", req.URL.String())
	}
	username, password, ok := req.BasicAuth()
	if !ok {
		t.Error("expected basic auth to be set")
	}

	if username != "testuser" || password != "testpass" {
		t.Errorf("expected basic auth user:testuser pass:testpass, got %s:%s", username, password)
	}
}

func TestCreateHTTPClientWithTimeout(t *testing.T) {
	tests := []struct {
		name            string
		insecureSkip    bool
		timeout         time.Duration
		expectedTimeout time.Duration
	}{
		{"default timeout", false, 0, 30 * time.Second},
		{"custom timeout", false, 45 * time.Second, 45 * time.Second},
		{"insecure with timeout", true, 60 * time.Second, 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := CreateHTTPClient(tt.insecureSkip, tt.timeout)
			if client.Timeout != tt.expectedTimeout {
				t.Errorf("expected timeout %v, got %v", tt.expectedTimeout, client.Timeout)
			}
		})
	}
}
