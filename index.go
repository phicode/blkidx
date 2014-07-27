package blkidx

import (
	"fmt"
	"sort"
	"sync"
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

	FindEqualHashes() ([]EqualsBlobs, error)

	AllNames() (Names, error)

	Remove(names Names) error

	Count() (int, error)
}

type Names []string

func (n Names) Sort() { sort.Strings([]string(n)) }

type EqualsBlobs struct {
	Names Names
	Size  int64
}

func (e *EqualsBlobs) Append(blob *Blob) {
	e.Names = append(e.Names, blob.Name)
	e.Size = blob.Size
}

func (e *EqualsBlobs) AppendRaw(name string, size int64) {
	e.Names = append(e.Names, name)
	e.Size = size
}

type OptimisticLockingError struct {
	Name          string
	FailedVersion uint64
}

var _ error = (*OptimisticLockingError)(nil)

func (o *OptimisticLockingError) Error() string {
	return fmt.Sprintf("optimistic locking error - version: %d already reached - blob: %q", //
		o.FailedVersion, o.Name)
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

func (i *LockedIndex) FindEqualHashes() ([]EqualsBlobs, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.FindEqualHashes()
}

func (i *LockedIndex) AllNames() (Names, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.AllNames()
}

func (i *LockedIndex) Remove(names Names) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.Remove(names)
}

func (i *LockedIndex) Count() (int, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.Backend.Count()
}
