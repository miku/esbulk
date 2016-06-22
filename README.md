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
      -cpuprofile string
          write cpu profile to file
      -host string
          elasticsearch host (default "localhost")
      -index string
          index name
      -mapping string
          mapping string or filename to apply before indexing
      -memprofile string
          write heap profile to file
      -port int
          elasticsearch port (default 9200)
      -purge
          purge any existing index before indexing
      -server string
          elasticsearch server, this works with https as well (default "http://localhost:9200")
      -size int
          bulk batch size (default 1000)
      -type string
          elasticsearch doc type (default "default")
      -v  prints current program version
      -verbose
          output basic progress
      -w int
          number of workers to use (default 4)
      -z  unzip gz'd file on the fly

To index a JSON file, that contains one document
per line, just run:

    $ esbulk -index example file.ldj

Where `file.ldj` is line delimited JSON, like:

    {"name": "esbulk", "version": "0.2.4"}
    {"name": "estab", "version": "0.1.3"}
    ...

By default `esbulk` will use as many parallel
workers, as there are cores. To tweak the indexing
process, adjust the `-size` and `-w` parameters.

You can index from gzipped files as well, using
the `-z` flag:

    $ esbulk -z -index example file.ldj.gz

Starting with 0.3.7 the preferred method to set a
non-default server hostport is via `-server`, e.g.

    $ esbulk -server https://0.0.0.0:9201

This way, you can use https as well, which was not
possible before. Options `-host` and `-port` are
kept for backwards compatibility.

----

A similar project has been started for solr, called [solrbulk](https://github.com/miku/solrbulk).
