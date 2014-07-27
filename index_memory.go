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

func (m *memoryIndex) FindEqualHashes() (rv []Names, err error) {
	m.rwmu.RLock()
	defer m.rwmu.RUnlock()

	if len(m.blobs) <= 1 {
		return nil, nil
	}

	// TODO: measure/profile and maybe implement a proper algorithm
	var all []hashToBlob = make([]hashToBlob, 0, len(m.blobs))

	for _, blob := range m.blobs {
		all = append(all, hashToBlob{hash: blob.Hash, name: blob.Name})
	}

	sort.Sort(compareByHash(all))

	i := 0
	for i+1 < len(all) {
		var equal Names
		if bytes.Equal(all[i].hash, all[i+1].hash) {
			equal = append(equal, all[i].name, all[i+1].name)
			i++
			for i+1 < len(all) && bytes.Equal(all[i].hash, all[i+1].hash) {
				equal = append(equal, all[i+1].name)
				i++
			}
			rv = append(rv, equal)
		}
	}

	return
}

type hashToBlob struct {
	hash []byte
	name string
}

type compareByHash []hashToBlob

var _ sort.Interface = (*compareByHash)(nil)

func (s compareByHash) Len() int           { return len(s) }
func (s compareByHash) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s compareByHash) Less(i, j int) bool { return bytes.Compare(s[i].hash, s[j].hash) <= 0 }
