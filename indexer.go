package blkidx

import (
	"bufio"
	"crypto"
	"io"
	"os"
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
	blob.IndexTime = time.Now()
	blob.ModTime = fileInfo.ModTime()
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

	n, err = io.Copy(hasher, bufrdr)
	if err != nil {
		return nil, nil, 0, err
	}
	all, blocks = hasher.Finish()
	return
}
