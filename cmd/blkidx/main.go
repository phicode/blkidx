package main

import (
	"bind.ch/blkidx/fs"
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"

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
	if user, err := user.Current(); err == nil {
		db = user.HomeDir + string(os.PathSeparator) + ".blkidx.sqlite3"
	}
	flagDb = flag.String("db", db, "sqlite database file to store")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
%s [options] command <path>

commands:

  index [path...]            add or update files to the index.

  remove [path...]           remove files from the index.

  remove-missing [path...]   remove files from the index which
                             are also missing on the filesystem.

  list                       list all files that are currently in the index.

  list-missing [path...]     list only files that are in the index
                             but not on the filesystem.

  dups [path...]             show all files in the index which
                             have the same checksums.

  rm-dups [path...]          interactive duplicate removal.


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
	found, err := run(args, *flagDb)
	if !found {
		errUsage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func errUsage() {
	flag.Usage()
	os.Exit(1)
}

func run(args []string, dbUrl string) (found bool, err error) {
	idx, dbCloser, err := openDbIndex(dbUrl)
	if err != nil {
		return true, fmt.Errorf("failed to open the sqlite3 database: %v", err)
	}
	defer dbCloser.Close()

	var paths fs.Paths
	if len(args) == 1 {
		paths, err = fs.WorkingDirectory()
	} else {
		paths, err = fs.NewPaths(args[1:]...)
	}
	if err != nil {
		return true, err
	}

	switch args[0] {
	case "index":
		index(idx, paths)

	case "remove":
		err = remove(idx, paths, nil)

	case "remove-missing":
		removeMissing(idx, paths)

	case "list":
		err = list(idx)

	case "list-missing":
		err = listMissing(idx, paths)

	case "dups":
		err = dups(idx, paths, false)

	case "rm-dups":
		err = dups(idx, paths, true)

	default:
		return false, nil
	}
	return true, err
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

func index(idx Index, paths fs.Paths) {
	var indexer = &Indexer{
		Index:       idx,
		Log:         logger,
		Concurrency: *flagConcurrency,
	}

	indexer.IndexAll(fs.WalkFiles(paths))
}

//TODO: review
func remove(idx Index, paths fs.Paths, exclude fs.Paths) error {
	names, err := idx.AllNames()
	if err != nil {
		return err
	}

	for path, _ := range paths {
		var remove Names
		for _, name := range names {
			if strings.HasPrefix(name, path) {
				if _, found := exclude[name]; !found {
					remove = append(remove, name)
				}
			}
		}
		if len(remove) > 0 {
			if err := idx.Remove(remove); err != nil {
				return err
			}
		}
	}

	c, _ := idx.Count()
	fmt.Println("files removed:", len(names)-c, "remaining:", c)
	return nil
}

func removeMissing(idx Index, paths fs.Paths) {
	var exclude fs.Paths = findAllFiles(paths)
	remove(idx, paths, exclude)
}

func list(idx Index) error {
	names, err := idx.AllNames()
	if err != nil {
		return err
	}
	names.Sort()

	for _, name := range names {
		fmt.Println(name)
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "files listed:", len(names))
	return nil
}

// TODO: review
func listMissing(idx Index, paths fs.Paths) error {
	names, err := getMissing(idx, paths, findAllFiles(paths))
	if err != nil {
		return err
	}
	var ns Names
	for n, _ := range names {
		ns = append(ns, n)
	}
	ns.Sort()
	for _, name := range ns {
		fmt.Println(name)
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "files missing:", len(ns))
	return nil
}

// TODO: review
func getMissing(idx Index, paths fs.Paths, present fs.Paths) (fs.Paths, error) {
	names, err := idx.AllNames()
	if err != nil {
		return nil, err
	}

	missing := make(fs.Paths)
	for path, _ := range paths {
		for _, name := range names {
			if strings.HasPrefix(name, path) {
				if _, found := present[name]; !found {
					missing[name] = struct{}{}
				}
			}
		}
	}

	return missing, nil
}

func dups(idx Index, paths fs.Paths, rm bool) error {
	equalBlobs, err := idx.FindEqualHashes()
	if err != nil {
		return fmt.Errorf("find duplicates failed: %v", err)
	}
	equalBlobs = reduceEqualBlobs(equalBlobs, findAllFiles(paths))
	if len(equalBlobs) == 0 {
		fmt.Println("no duplicates found")
		return nil
	}

	var savings int64
	separator := strings.Repeat("-", 80)
	for _, equal := range equalBlobs {
		fmt.Println(separator)
		for i, name := range equal.Names {
			fmt.Printf("%d - %s\n", (i + 1), name)
		}

		if rm {
			n, err := askRemove(idx, equal)
			if err != nil {
				return err
			}
			savings += equal.Size * int64(n)
		} else {
			savings += (equal.Size * (int64(len(equal.Names) - 1)))
		}
	}
	fmt.Fprintln(os.Stderr)
	if rm {
		fmt.Fprintln(os.Stderr, "removed", sizePretty(savings))
	} else {
		fmt.Fprintln(os.Stderr, "removing all duplicates would save", sizePretty(savings))
	}
	return nil
}

func askRemove(idx Index, equal EqualBlobs) (int, error) {
	r := bufio.NewReader(os.Stdin)

	fmt.Println(`enter space-separated file indexes to delete or enter to delete-nothing
!!! this really deleted the file !!!`)
	indexes, err := readIntFieldsLine(r, -1)
	if err != nil {
		return 0, err
	}
	if len(indexes) == 0 {
		return 0, nil
	}
	for _, index := range indexes {
		if index >= 0 && index < len(equal.Names) {
			file := equal.Names[index]
			fmt.Println("deleting", file)
			if err := os.Remove(file); err != nil {
				return 0, err // TODO: still report how much was already saved
			}
			var deleted Names = Names{file}
			if err := idx.Remove(deleted); err != nil {
				return 0, fmt.Errorf("file removed but still in index due do: %v", err) // TODO: same as above
			}
		}
	}

	return len(indexes), nil
}

func readIntFieldsLine(r *bufio.Reader, offset int) ([]int, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(line) == "" {
		return nil, nil
	}
	parts := strings.Fields(line)
	var indexes []int
	for _, p := range parts {
		index, err := strconv.Atoi(p)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index+offset)
	}
	return indexes, nil
}

func findAllFiles(paths fs.Paths) fs.Paths {
	c := fs.WalkFiles(paths)
	return fs.AggregateLogErrors(c, logger)
}

func reduceEqualBlobs(ebs []EqualBlobs, filesInPaths fs.Paths) []EqualBlobs {
	var rv []EqualBlobs
	for _, eb := range ebs {
		if eb.ContainsAnyName(filesInPaths) {
			rv = append(rv, eb)
		}
	}
	return rv
}

func sizePretty(s int64) string {
	var o order = orders[0]
	if s < o.v {
		return fmt.Sprintf("%d bytes", s)
	}
	for _, x := range orders {
		if s < x.v {
			break
		}
		o = x
	}
	return fmt.Sprintf("%.3f %s", float64(s)/float64(o.v), o.s)
}

type order struct {
	v int64
	s string
}

var orders = []order{
	{1 << 10, "KiB"},
	{1 << 20, "MiB"},
	{1 << 30, "GiB"},
	{1 << 40, "TiB"},
	{1 << 50, "PiB"},
	{1 << 60, "EiB"},
}
