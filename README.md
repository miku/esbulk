esbulk
======

Fast parallel bulk loading utility for elasticsearch.

[![Build Status](http://img.shields.io/travis/miku/esbulk.svg?style=flat)](https://travis-ci.org/miku/esbulk)

Installation
------------

    $ go get github.com/miku/esbulk/cmd/esbulk

Usage
-----

    $ esbulk -h
      -cpuprofile="": write cpu profile to file
      -host="localhost": elasticsearch host
      -index="": index name
      -memprofile="": write heap profile to file
      -port=9200: elasticsearch port
      -q=false: do not produce any output
      -size=1000: bulk batch size
      -type="default": type
      -v=false: prints current program version
      -w=4: number of workers to use

To index a JSON file, that contains one document per line, just run:

    $ esbulk -index my-index file.ldj

This will use as many parallel workers, as there are cores. To optimize
the indexing process, adjust the `size` and `w` parameters.
