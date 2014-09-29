package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
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
}

// BulkIndex takes a list of documents as strings and indexes them into elasticsearch
func BulkIndex(docs []string, host string, port int, index, docType string) error {
	url := fmt.Sprintf("http://%s:%d/%s/%s/_bulk", host, port, index, docType)
	var buf bytes.Buffer
	for _, doc := range docs {
		if len(doc) == 0 {
			continue
		}
		buf.WriteString(`{"index": {}}`)
		buf.WriteString("\n")
		buf.WriteString(doc)
		buf.WriteString("\n")
	}
	buf.WriteString("\n")
	_, err := http.Post(url, "application/json", bytes.NewReader(buf.Bytes()))
	if err != nil {
		return err
	}
	return nil
}

// Worker will batch index documents that come in on the lines channel
func Worker(id string, options Options, lines chan *string, wg *sync.WaitGroup) {
	defer wg.Done()
	var docs []string
	counter := 0
	for s := range lines {
		docs = append(docs, *s)
		counter += 1
		if counter%options.BatchSize == 0 {
			err := BulkIndex(docs, options.Host, options.Port, options.Index, options.DocType)
			if err != nil {
				log.Fatal(err)
			}
			docs = docs[:0]
		}
	}
	err := BulkIndex(docs, options.Host, options.Port, options.Index, options.DocType)
	if err != nil {
		log.Fatal(err)
	}
	docs = docs[:0]
}

func main() {

	version := flag.Bool("v", false, "prints current program version")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	indexName := flag.String("index", "", "index name")
	docType := flag.String("type", "default", "type")
	host := flag.String("host", "localhost", "elasticsearch host")
	port := flag.Int("port", 9200, "elasticsearch port")
	batchSize := flag.Int("size", 1000, "bulk batch size")
	numWorkers := flag.Int("w", runtime.NumCPU(), "number of workers to use")

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
	}

	queue := make(chan *string)
	var wg sync.WaitGroup

	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		s := scanner.Text()
		queue <- &s
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(queue)
	wg.Wait()
}
