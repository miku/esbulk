package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

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
	quiet := flag.Bool("q", false, "do not produce any output")
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
		Quiet:     *quiet,
	}

	queue := make(chan string)
	var wg sync.WaitGroup

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go esbulk.Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	// set refresh inteval to -1
	_, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/_settings", *host, *port, *indexName), strings.NewReader(`{"index": {"refresh_interval": "-1"}}`))
	if err != nil {
		log.Fatal(err)
	}

	reader := bufio.NewReader(file)
	if *gzipped {
		zreader, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}
		reader = bufio.NewReader(zreader)
	}

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
	}

	close(queue)
	wg.Wait()

	defer func() {
		_, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/_settings", *host, *port, *indexName), strings.NewReader(`{"index": {"refresh_interval": "1s"}}`))
		if err != nil {
			log.Fatal(err)
		}
		_, err = http.Post(fmt.Sprintf("http://%s:%d/%s/_flush", *host, *port, *indexName), "", nil)
		if err != nil {
			log.Fatal(err)
		}
	}()

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}
