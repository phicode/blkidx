package blkidx

//import (
//	"sync"
//)
//
//type writeBackCacheIndex struct {
//	backend Index
//
//	cache Index
//	mu    sync.Mutex
//
//	queries <-chan query
//}
//
//func NewCachedIndex(backend Index) Index {
//	c:= &cachedIndex{
//		backend: backend,
//		cache:     NewMemoryIndex(),
//	}
//	go c.worker()
//	return c
//}
//
//var _ Index = (*cachedIndex)(nil)
//
//func (i *cachedIndex) Store(blob *Blob) error {
//	i.mu.Lock()
//	defer i.mu.Unlock()
//
//	return i.Backend.Store(blob)
//}
//
//func (i *cachedIndex) LookupByName(name string) (*Blob, error) {
//	i.mu.Lock()
//	defer i.mu.Unlock()
//
//	return i.Backend.LookupByName(name)
//}
//
//func (c *cachedIndex) worker() {
//	for cmd:=range
//}
