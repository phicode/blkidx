package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"runtime"

	. "bind.ch/blkidx"

	_ "github.com/mattn/go-sqlite3"
)

var (
	flagDb          *string
	flagConcurrency = flag.Int("c", 1, "concurrency")
	logger          = log.New(os.Stderr, "", log.Ldate|log.Ltime|log.Lmicroseconds)
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	var db string
	user, err := user.Current()
	if err == nil {
		db = user.HomeDir + string(os.PathSeparator) + ".blkidx.sqlite3"
	}
	flagDb = flag.String("db", db, "sqlite database file to store")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
%s [options] command <path>\n

commands:

  index [path...]      add (or update) a path to the index.
                     an empty path mean.

  dups               show files with duplicate checksums


options:
`, os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()
	args := flag.Args()
	if *flagDb == "" || len(args) == 0 {
		errUsage()
	}

	idx, dbCloser, err := openDbIndex(*flagDb)
	if err != nil {
		logger.Printf("failed to open the sqlite3 database: %v", err)
		os.Exit(2)
	}
	defer dbCloser.Close()

	switch args[0] {
	case "index":
		if len(args) == 1 {
			index(idx, []string{"."})
		} else {
			index(idx, args[1:])
		}
	//case "dups":
	//	dups(idx)
	default:
		flag.Usage()
		os.Exit(1)
	}
}

func errUsage() {
	flag.Usage()
	os.Exit(1)
}

func openDbIndex(dbUrl string) (Index, io.Closer, error) {
	// TODO: doesn'nt work, see comment below
	//dbUrl := "file:" + *flagDb + "?cache=shared&mode=rwc"
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

func index(index Index, paths []string) {
	var indexer = &Indexer{
		Index:       index,
		Log:         logger,
		Concurrency: *flagConcurrency,
	}

	for _, p := range paths {
		c := WalkFiles(p)
		indexer.IndexAll(p, c)
	}
}

func dups(index Index) {
	namess, err := index.FindEqualHashes()
	if err != nil {
		fmt.Fprintln(os.Stderr, "find duplicates failed:", err)
		os.Exit(1)
	}
	if len(namess) == 0 {
		fmt.Println("no duplicates found")
		return
	}
	for _, names := range namess {
		fmt.Println("==============================================================")
		for _, name := range names {
			fmt.Println("\t", name)
		}
	}
}
