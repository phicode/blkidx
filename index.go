package blkidx

import (
	"crypto"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Index interface {
	// stores a blob by its name.
	// if the version field is zero and no such blob exists it will be stored.
	// existing blobs will be overwritten if the new version is excalty one higher than the existing one.
	// otherwise a OptimisticLockingError will be returned.
	Store(blob *Blob) error

	// blob lookup by name. if no blob by a certain name exists "nil, nil" is returned.
	// the error return value is indicative of problems with the underlying storage strategy.
	LookupByName(name string) (*Blob, error)
}

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
	// TODO: more checks
	return nil
}

func (b *Blob) CheckOptimisticLock(update *Blob) error {
	if update.Version != b.Version+1 {
		return &OptimisticLockingError{
			Name:          b.Name,
			IndexVersion:  b.Version,
			FailedVersion: update.Version,
		}
	}
	return nil
}

type OptimisticLockingError struct {
	Name          string
	IndexVersion  uint64
	FailedVersion uint64
}

var _ error = (*OptimisticLockingError)(nil)

func (o *OptimisticLockingError) Error() string {
	return fmt.Sprintf("optimistic locking error - index-version: %d - failed version: %d - blob: %q", //
		o.IndexVersion, o.FailedVersion, o.Name)
}
