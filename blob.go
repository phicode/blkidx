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

func (b *Blob) Validate() error {
	if b == nil {
		return errors.New("invalid nil block")
	}
	if b.Name == "" {
		return errors.New("invalid empty name")
	}
	if strings.TrimSpace(b.Name) != b.Name {
		return errors.New("invalid leading or trailing space in name")
	}
	if b.IndexTime.IsZero() || b.ModTime.IsZero() {
		return errors.New("invalid zero index or modify time")
	}
	if !b.HashAlgorithm.Available() {
		return errors.New("invalid hash algorithm")
	}
	if b.HashBlockSize <= 0 {
		return errors.New("invalid hash block size")
	}
	if b.Size < 0 {
		return errors.New("invalid blob size")
	}
	if b.Size > 0 {
		if len(b.Hash) != b.HashAlgorithm.Size() {
			return errors.New("invalid hash length")
		}
		if len(b.HashedBlocks) < 1 {
			return errors.New("invalid empty hashed blocks")
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
