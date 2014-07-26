package blkidx

import (
	"sync"
)

type memoryIndex struct {
	rwmu  sync.RWMutex
	blobs map[string]*Blob
}

var _ Index = (*memoryIndex)(nil)

func NewMemoryIndex() Index {
	return &memoryIndex{
		blobs: make(map[string]*Blob, 1024),
	}
}

func (m *memoryIndex) Store(blob *Blob) error {
	if err := blob.Validate(); err != nil {
		return err
	}
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	if b, found := m.blobs[blob.Name]; found {
		if err := b.CheckOptimisticLock(blob); err != nil {
			return err
		}
	}

	m.blobs[blob.Name] = blob
	return nil
}

func (m *memoryIndex) LookupByName(name string) (*Blob, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return m.blobs[name], nil
}
