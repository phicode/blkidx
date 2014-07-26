package blkidx

import (
	"sync"
)

type MemoryIndex struct {
	rwmu  sync.RWMutex
	blobs map[string]*Blob
}

var _ Index = (*MemoryIndex)(nil)

func (m *MemoryIndex) Store(blob *Blob) error {
	if err := blob.Validate(); err != nil {
		return err
	}
	m.init()

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

func (m *MemoryIndex) LookupByName(name string) (*Blob, error) {
	m.init()

	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return m.blobs[name], nil
}

func (m *MemoryIndex) init() {
	if m.blobs != nil {
		return
	}
	m.rwmu.Lock()
	defer m.rwmu.Unlock()
	if m.blobs != nil {
		return
	}
	m.blobs = make(map[string]*Blob)
}
