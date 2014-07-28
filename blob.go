package blkidx

import (
	"bytes"
	"crypto"
	"errors"
	"strings"
	"time"
)

type Blob struct {
	Name      string
	Version   uint64
	IndexTime time.Time

	Size    int64
	ModTime time.Time

	HashAlgorithm crypto.Hash

	// hash of the full blob
	Hash []byte

	// size of hashed blocks
	HashBlockSize int

	// hashes of individual blocks
	HashedBlocks [][]byte
}

func (b *Blob) HasChanged(size int64, mtime time.Time) bool {
	return b.Size != size ||
		b.ModTime.UTC() != mtime.UTC()
}

var (
	blobErrNil        = errors.New("invalid nil block")
	blobErrEmptyName  = errors.New("invalid empty name")
	blobErrSpaceName  = errors.New("invalid leading or trailing space in name")
	blobErrZeroIdx    = errors.New("invalid zero index time")
	blobErrZeroMod    = errors.New("invalid zero modify time")
	blobErrHashAlg    = errors.New("invalid hash algorithm")
	blobErrBlkSize    = errors.New("invalid hash block size")
	blobErrSize       = errors.New("invalid blob size")
	blobErrHashLen    = errors.New("invalid hash length")
	blobErrBlkHashLen = errors.New("invalid empty hashed blocks")
)

func (b *Blob) Validate() error {
	if b == nil {
		return blobErrNil
	}
	if b.Name == "" {
		return blobErrEmptyName
	}
	if strings.TrimSpace(b.Name) != b.Name {
		return blobErrSpaceName
	}
	if b.IndexTime.IsZero() {
		return blobErrZeroIdx
	}
	if b.ModTime.IsZero() {
		return blobErrZeroMod
	}
	if !b.HashAlgorithm.Available() {
		return blobErrHashAlg
	}
	if b.HashBlockSize <= 0 {
		return blobErrBlkSize
	}
	if b.Size < 0 {
		return blobErrSize
	}
	if b.Size > 0 {
		if len(b.Hash) != b.HashAlgorithm.Size() {
			return blobErrHashLen
		}
		if len(b.HashedBlocks) < 1 {
			return blobErrBlkHashLen
		}
	}
	return nil
}

func (b *Blob) CheckOptimisticLock(update *Blob) error {
	if update.Version != b.Version+1 {
		return &OptimisticLockingError{
			Name:          b.Name,
			FailedVersion: update.Version,
		}
	}
	return nil
}

func (b *Blob) EqualHash(other *Blob) bool {
	return bytes.Equal(b.Hash, other.Hash)
}

type EqualBlobs struct {
	Names Names
	Size  int64
}

func (e *EqualBlobs) Append(blob *Blob) {
	e.Names = append(e.Names, blob.Name)
	e.Size = blob.Size
}

func (e *EqualBlobs) AppendRaw(name string, size int64) {
	e.Names = append(e.Names, name)
	e.Size = size
}

func (e *EqualBlobs) ContainsAnyName(names map[string]struct{}) bool {
	for _, n := range e.Names {
		if _, found := names[n]; found {
			return true
		}
	}
	return false
}
