package main

import (
	"flag"
	"log"
	"os"

	. "bind.ch/blkidx"
)

var (
	flagRoot *string
)

func init() {
	wd, err := os.Getwd()
	if err != nil {
		wd = ""
	}
	flagRoot = flag.String("root", wd, "root folder to index")
}

func main() {
	flag.Parse()

	if *flagRoot == "" {
		flag.Usage()
		os.Exit(1)
	}

	var index Index = new(MemoryIndex)
	l := log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)
	var indexer = &Indexer{
		Index: index,
		Log:   l,
	}

	c := WalkFiles(*flagRoot)
	indexer.IndexAllFiles(c)

	c = WalkFiles(*flagRoot)
	indexer.IndexAllFiles(c)
}
