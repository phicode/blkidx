package blkidx

import (
	"bufio"
	"crypto"
	"io"
	"log"
	"os"
	"sync"
	"time"

	_ "crypto/sha256"
)

var (
	DefaultHashAlgorithm crypto.Hash = crypto.SHA256
	DefaultHashBlockSize int         = 64 << 20
)

func init() {
	if !DefaultHashAlgorithm.Available() {
		panic("default hash algorithm not available")
	}
}

func IndexFile(name string) (blob *Blob, err error) {
	var file *os.File
	file, err = os.Open(name)
	if err != nil {
		return
	}
	defer file.Close()

	var fileInfo os.FileInfo
	fileInfo, err = file.Stat()
	if err != nil {
		return
	}

	blob = new(Blob)
	blob.Name = name
	blob.IndexTime = time.Now().UTC()
	blob.ModTime = fileInfo.ModTime().UTC()
	blob.HashAlgorithm = DefaultHashAlgorithm
	blob.HashBlockSize = DefaultHashBlockSize

	blob.Hash, blob.HashedBlocks, blob.Size, err = HashAll(file, blob.HashAlgorithm, blob.HashBlockSize)
	return
}

func HashAll(r io.Reader, algorithm crypto.Hash, blockSize int) (all []byte, blocks [][]byte, n int64, err error) {
	var (
		bufrdr *bufio.Reader = bufio.NewReader(r)
		hasher Hasher        = NewHasher(algorithm, blockSize)
	)

	//TODO: progress if file is larger then a certain size
	n, err = io.Copy(hasher, bufrdr)
	if err != nil {
		return nil, nil, 0, err
	}
	all, blocks = hasher.Finish()
	return
}

type Indexer struct {
	Index Index

	Log *log.Logger

	Concurrency int

	wg sync.WaitGroup
}

func (i *Indexer) IndexAll(c <-chan *PathElem) {
	if i.Concurrency < 1 {
		i.Concurrency = 1
	}
	for x := 0; x < i.Concurrency; x++ {
		i.wg.Add(1)
		go i.indexWorker(c)
	}
	i.wg.Wait()
	i.logf("INFO: finished")
}

func (i *Indexer) indexWorker(c <-chan *PathElem) {
	for pe := range c {
		if pe.Err != nil {
			i.logf("ERROR: %v", pe.Err)
			continue
		}
		i.index(pe)
	}
	i.wg.Done()
}

func (i *Indexer) logf(format string, x ...interface{}) {
	if i.Log != nil {
		i.Log.Printf(format, x...)
	}
}

func (i *Indexer) index(pe *PathElem) {
	previous, err := i.Index.LookupByName(pe.Path)
	if err != nil {
		i.logf("ERROR: index lookup failed: %v", err)
		return
	}
	if previous != nil {
		var size int64 = pe.Info.Size()
		var mtime time.Time = pe.Info.ModTime()
		if !previous.HasChanged(size, mtime) {
			i.logf("INFO: index up to date for %q", pe.Path)
			return
		}
		i.logf("INFO: file in index but changed, reindexing %q - %d %d %v %v", pe.Path, size, previous.Size, mtime, previous.ModTime)
	}

	i.logf("INFO: indexing %q", pe.Path)

	indexed, err := IndexFile(pe.Path)
	if err != nil {
		i.logf("ERROR: file indexing failed: %v", err)
		return
	}
	if previous != nil {
		indexed.Version = previous.Version + 1
	}
	if err := i.Index.Store(indexed); err != nil {
		i.logf("ERROR: index store failed: %v", err)
	}
}
