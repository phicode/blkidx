package blkidx

import (
	"sync"

	"fmt"
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

type LockedIndex struct {
	Backend Index

	mu sync.Mutex
}

var _ Index = (*LockedIndex)(nil)

func (i *LockedIndex) Store(blob *Blob) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.Store(blob)
}

func (i *LockedIndex) LookupByName(name string) (*Blob, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.LookupByName(name)
}
