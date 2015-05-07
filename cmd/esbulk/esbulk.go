package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/miku/esbulk"
)

func main() {

	version := flag.Bool("v", false, "prints current program version")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write heap profile to file")
	indexName := flag.String("index", "", "index name")
	docType := flag.String("type", "default", "elasticsearch doc type")
	host := flag.String("host", "localhost", "elasticsearch host")
	port := flag.Int("port", 9200, "elasticsearch port")
	batchSize := flag.Int("size", 1000, "bulk batch size")
	numWorkers := flag.Int("w", runtime.NumCPU(), "number of workers to use")
	verbose := flag.Bool("verbose", false, "output basic progress")
	gzipped := flag.Bool("z", false, "unzip gz'd file on the fly")

	var PrintUsage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [OPTIONS] JSON\n", os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *version {
		fmt.Println(esbulk.Version)
		os.Exit(0)
	}

	if flag.NArg() < 1 {
		PrintUsage()
		os.Exit(1)
	}

	if *indexName == "" {
		log.Fatal("index name required")
	}

	file, err := os.Open(flag.Args()[0])
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	runtime.GOMAXPROCS(*numWorkers)

	options := esbulk.Options{
		Host:      *host,
		Port:      *port,
		Index:     *indexName,
		DocType:   *docType,
		BatchSize: *batchSize,
		Verbose:   *verbose,
	}

	queue := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go esbulk.Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	client := &http.Client{}

	// shutdown procedure
	// TODO(miku): maybe handle signals, too
	defer func() {
		r := strings.NewReader(`{"index": {"refresh_interval": "1s"}}`)
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/_settings", *host, *port, *indexName), r)
		if err != nil {
			log.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("setting index.refresh_interval to 1s: %s\n", resp.Status)
		resp, err = http.Post(fmt.Sprintf("http://%s:%d/%s/_flush", *host, *port, *indexName), "", nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("index flush: %s\n", resp.Status)
	}()

	// create index if not exists
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s:%d/%s/_status", *host, *port, *indexName), nil)
	if err != nil {
		log.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode == 404 {
		log.Printf("creating index: %s\n", resp.Status)
		req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/", *host, *port, *indexName), nil)
		if err != nil {
			log.Fatal(err)
		}
		resp, err = client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode == 400 {
			msg, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Fatal(err)
			}
			log.Fatal(string(msg))
		}
	}

	// set refresh interval to -1
	log.Printf("setting index.refresh_interval to -1: %s\n", resp.Status)
	r := strings.NewReader(`{"index": {"refresh_interval": "-1"}}`)
	req, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/_settings", *host, *port, *indexName), r)
	if err != nil {
		log.Fatal(err)
	}
	resp, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		log.Fatal(resp)
	}

	reader := bufio.NewReader(file)
	if *gzipped {
		zreader, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}
		reader = bufio.NewReader(zreader)
	}

	counter := 0
	start := time.Now()

	for {
		line, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		line = strings.TrimSpace(line)
		queue <- line
		counter += 1
	}

	close(queue)
	wg.Wait()
	elapsed := time.Since(start)

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}

	if *verbose {
		rate := float64(counter) / elapsed.Seconds()
		log.Printf("%d docs in %s at %0.3f docs/s with %d workers\n", counter, elapsed, rate, *numWorkers)
	}
}
