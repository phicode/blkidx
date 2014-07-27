package blkidx

//import (
//	"sync"
//)
//
//type FsIndex struct {
//	mu     sync.Mutex
//	cond   *sync.Cond
//	root   string
//	active map[string]struct{}
//}
//
//var _ Index = (*FsIndex)(nil)
//
//func (fs *FsIndex) Store(blob *Blob) error {
//	if err := blob.Validate(); err != nil {
//		return err
//	}
//	fs.lock(blob.Name)
//	defer fs.unlock(blob.Name)
//
//	existing, err := fs.lockedLookupByName(blob.Name)
//	if err != nil {
//		return err
//	}
//	if existing != nil {
//		if err := existing.CheckOptimisticLock(blob); err != nil {
//			return err
//		}
//	}
//	//TODO
//	//return marshal(blob)
//	return nil
//}
//
//func (fs *FsIndex) LookupByName(name string) (*Blob, error) {
//	fs.lock(name)
//	defer fs.unlock(name)
//
//	return fs.lockedLookupByName(name)
//}
//
//func (fs *FsIndex) lockedLookupByName(name string) (*Blob, error) {
//	return nil, nil
//}
//
//func (fs *FsIndex) init() {
//	if fs.active != nil {
//		return
//	}
//	fs.mu.Lock()
//	defer fs.mu.Unlock()
//	if fs.active != nil {
//		return
//	}
//	fs.active = make(map[string]struct{})
//	fs.cond = sync.NewCond(&fs.mu)
//}
//
//func (fs *FsIndex) lock(name string) {
//	fs.init()
//	fs.mu.Lock()
//	defer fs.mu.Unlock()
//
//	for {
//		if _, found := fs.active[name]; found {
//			fs.cond.Wait()
//		} else {
//			break
//		}
//	}
//	fs.active[name] = struct{}{}
//}
//
//func (fs *FsIndex) unlock(name string) {
//	fs.init()
//	fs.mu.Lock()
//	defer fs.mu.Unlock()
//	delete(fs.active, name)
//	fs.cond.Broadcast()
//}
//
