esbulk
======

Fast parallel bulk loading utility for elasticsearch. [Asciicast](https://asciinema.org/a/5id2uazhw5faykujavgzns3mo).

Installation
------------

    $ go get github.com/miku/esbulk/cmd/esbulk

For `deb` or `rpm` packages, see: https://github.com/miku/esbulk/releases

Usage
-----

    $ esbulk -h
    Usage: esbulk [OPTIONS] JSON
      -cpuprofile="": write cpu profile to file
      -host="localhost": elasticsearch host
      -index="": index name
      -memprofile="": write heap profile to file
      -port=9200: elasticsearch port
      -size=1000: bulk batch size
      -type="default": elasticsearch doc type
      -v=false: prints current program version
      -verbose=false: output basic progress
      -w=4: number of workers to use
      -z=false: unzip gz'd file on the fly

To index a JSON file, that contains one document per line, just run:

    $ esbulk -index example file.ldj

Where `file.ldj` is line delimited JSON, like:

    {"name": "esbulk", "version": "0.2.4"}
    {"name": "estab", "version": "0.1.3"}
    ...

By default `esbulk` will use as many parallel workers, as there are cores.
To tweak the indexing process, adjust the `-size` and `-w` parameters.

You can index from gzipped files as well, using the `-z` flag:

    $ esbulk -z -index example file.ldj.gz

----

A similar project has been started for solr, called [solrbulk](https://github.com/miku/solrbulk).
