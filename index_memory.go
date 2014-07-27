package blkidx

import (
	"bytes"
	"sort"
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

func (m *memoryIndex) FindEqualHashes() (rv []EqualsBlobs, err error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if len(m.blobs) <= 1 {
		return nil, nil
	}

	// TODO: measure/profile and maybe implement a proper algorithm
	var all []*Blob = make([]*Blob, 0, len(m.blobs))

	for _, blob := range m.blobs {
		all = append(all, blob)
	}

	sort.Sort(byHash(all))

	i := 0
	for i+1 < len(all) {
		var equal EqualsBlobs
		if all[i].EqualHash(all[i+1]) {
			equal.Append(all[i])
			equal.Append(all[i+1])
			i++
			for i+1 < len(all) && all[i].EqualHash(all[i+1]) {
				equal.Append(all[i+1])
				i++
			}
			rv = append(rv, equal)
		}
	}
	return
}

func (m *memoryIndex) AllNames() (Names, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	var rv Names = make(Names, 0, len(m.blobs))
	for name, _ := range m.blobs {
		rv = append(rv, name)
	}
	return rv, nil
}

func (m *memoryIndex) Remove(names Names) error {
	m.rwmu.Lock()
	defer m.rwmu.Unlock()

	for _, name := range names {
		delete(m.blobs, name)
	}
	return nil
}

func (m *memoryIndex) Count() (int, error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	return len(m.blobs), nil
}

type byHash []*Blob

var _ sort.Interface = (*byHash)(nil)

func (s byHash) Len() int           { return len(s) }
func (s byHash) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byHash) Less(i, j int) bool { return bytes.Compare(s[i].Hash, s[j].Hash) <= 0 }
