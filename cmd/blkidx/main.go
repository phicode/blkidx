package main

import (
	"database/sql"
	"flag"
	"io"
	"log"
	"os"
	"os/user"

	. "bind.ch/blkidx"

	_ "github.com/mattn/go-sqlite3"
)

var (
	flagRoot        *string
	flagDb          *string
	flagConcurrency = flag.Int("c", 1, "concurrency")
	logger          = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)
)

func init() {
	wd, err := os.Getwd()
	if err != nil {
		wd = ""
	}
	flagRoot = flag.String("root", wd, "root folder to index")

	var db string
	user, err := user.Current()
	if err == nil {
		db = user.HomeDir + string(os.PathSeparator) + ".blkidx.sqlite3"
	}
	flagDb = flag.String("db", db, "sqlite database file to store")
}

func main() {
	flag.Parse()
	if *flagRoot == "" || *flagDb == "" {
		flag.Usage()
		os.Exit(1)
	}

	index, dbCloser, err := openDbIndex()

	if err != nil {
		logger.Printf("failed to open the sqlite3 database: %v", err)
		os.Exit(2)
	}
	defer dbCloser.Close()

	var indexer = &Indexer{
		Index:       index,
		Log:         logger,
		Concurrency: *flagConcurrency,
	}

	c := WalkFiles(*flagRoot)
	indexer.IndexAllFiles(c)

	// TODO: remove double walking
	c = WalkFiles(*flagRoot)
	indexer.IndexAllFiles(c)
}

func openDbIndex() (Index, io.Closer, error) {
	// TODO: doesn'nt work, see comment below
	//dbUrl := "file:" + *flagDb + "?cache=shared&mode=rwc"
	dbUrl := *flagDb
	db, err := sql.Open("sqlite3", dbUrl)
	if err != nil {
		return nil, nil, err
	}
	idx, err := NewSqlIndex(db)
	if err != nil {
		db.Close()
		return nil, nil, err
	}
	// TODO: get sqlite locking/serialization right so that we do not have to
	// wrap access the database through a single thread
	return &LockedIndex{Backend: idx}, db, nil
}
