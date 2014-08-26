package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	"github.com/belogik/goes"
	"github.com/miku/esmlt"
	"github.com/miku/stardust"
)

type Options struct {
	Host      string
	Port      string
	Index     string
	DocType   string
	BatchSize int
}

func Worker(id string, options Options, lines chan *[]byte, wg *sync.WaitGroup) {
	defer wg.Done()
	conn := goes.NewConnection(options.Host, options.Port)
	counter := 0
	var source map[string]interface{}
	var documents []goes.Document
	for b := range lines {
		json.Unmarshal(*b, &source)
		doc := goes.Document{
			Id:          esmlt.Value("content.001", source),
			Index:       options.Index,
			Type:        options.DocType,
			BulkCommand: goes.BULK_COMMAND_INDEX,
			Fields:      source,
		}
		documents = append(documents, doc)
		counter++
		if counter%options.BatchSize == 0 {
			fmt.Printf("[%s] Bulk indexing batch... %d\n", id, counter)
			_, err := conn.BulkSend(documents)
			fmt.Printf("[%s] Bulk sent.\n", id)
			if err != nil {
				log.Fatal(err)
			}
			documents = documents[:0]
		}
	}
}

func main() {
	version := flag.Bool("v", false, "prints current program version")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	indexName := flag.String("index", "", "index name")
	docType := flag.String("type", "default", "type")
	esHost := flag.String("host", "localhost", "elasticsearch host")
	esPort := flag.String("port", "9200", "elasticsearch port")
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
		fmt.Println(stardust.Version)
		os.Exit(0)
	}

	if flag.NArg() < 1 {
		PrintUsage()
		os.Exit(1)
	}

	filename := flag.Args()[0]

	var file *os.File
	var err error
	if filename == "-" {
		file = os.Stdin
	} else {
		file, err = os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatalln(err)
		}
	}

	runtime.GOMAXPROCS(*numWorkers)

	options := Options{
		Host:      *esHost,
		Port:      *esPort,
		Index:     *indexName,
		DocType:   *docType,
		BatchSize: *batchSize,
	}

	queue := make(chan *[]byte)
	var wg sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		wg.Add(1)
		go Worker(fmt.Sprintf("worker-%d", i), options, queue, &wg)
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		b := scanner.Bytes()
		queue <- &b
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	close(queue)
	wg.Wait()
	return
}
