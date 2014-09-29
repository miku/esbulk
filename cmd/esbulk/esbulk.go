package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"

	"github.com/miku/esbulk"
)

// Options represents bulk indexing options
type Options struct {
	Host      string
	Port      int
	Index     string
	DocType   string
	BatchSize int
	Quiet     bool
}

// BulkIndex takes a list of documents as strings and indexes them into elasticsearch
func BulkIndex(docs []string, options Options) error {
	url := fmt.Sprintf("http://%s:%d/%s/%s/_bulk", options.Host, options.Port, options.Index, options.DocType)
	header := fmt.Sprintf(`{"index": {"_index": "%s", "_type": "%s"}}`, options.Index, options.DocType)
	var lines []string
	for _, doc := range docs {
		if len(strings.TrimSpace(doc)) == 0 {
			continue
		}
		lines = append(lines, header)
		lines = append(lines, doc)
	}
	body := fmt.Sprintf("%s\n", strings.Join(lines, "\n"))
	response, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		return err
	}
	// > Caller should close resp.Body when done reading from it.
	// Results in a resource leak otherwise.
	response.Body.Close()
	return nil
}

// Worker will batch index documents that come in on the lines channel
func Worker(id string, options Options, lines chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var docs []string
	counter := 0
	for s := range lines {
		docs = append(docs, s)
		counter++
		if counter%options.BatchSize == 0 {
			err := BulkIndex(docs, options)
			if err != nil {
				log.Fatal(err)
			}
			if !options.Quiet {
				fmt.Fprintf(os.Stderr, "[%s] @%d\n", id, counter)
			}
			docs = docs[:0]
		}
	}
	err := BulkIndex(docs, options)
	if err != nil {
		log.Fatal(err)
	}
	if !options.Quiet {
		fmt.Fprintf(os.Stderr, "[%s] @%d\n", id, counter)
	}
}

func main() {

	version := flag.Bool("v", false, "prints current program version")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write heap profile to file")
	indexName := flag.String("index", "", "index name")
	docType := flag.String("type", "default", "type")
	host := flag.String("host", "localhost", "elasticsearch host")
	port := flag.Int("port", 9200, "elasticsearch port")
	batchSize := flag.Int("size", 1000, "bulk batch size")
	numWorkers := flag.Int("w", runtime.NumCPU(), "number of workers to use")
	quiet := flag.Bool("q", false, "do not produce any output")

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

	options := Options{
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
		go Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	// set refresh inteval to -1
	_, err = http.NewRequest("PUT", fmt.Sprintf("http://%s:%d/%s/_settings", *host, *port, *indexName), strings.NewReader(`{"index": {"refresh_interval": "-1"}}`))
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := scanner.Text()
		queue <- s
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
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
