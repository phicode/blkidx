package blkidx

import (
	"testing"
)

func TestBlobCheckOptimisticLock(t *testing.T) {
	var a, b Blob
	a.Version = 0
	b.Version = 1

	if err := a.CheckOptimisticLock(&b); err != nil {
		t.Errorf("want: nil error for valid versions, got: %v", err)
	}

	b.Version = 0
	checkIsOptimisticLockingError(t, a.CheckOptimisticLock(&b))

	b.Version = 2
	checkIsOptimisticLockingError(t, a.CheckOptimisticLock(&b))
}

func checkIsOptimisticLockingError(t *testing.T, err error) {
	if err == nil {
		t.Errorf("want: error for invalid versions, got: nil", err)
	}
	if _, ok := err.(*OptimisticLockingError); !ok {
		t.Errorf("want: *OptimisticLockingError, got: %#v", err)
	}
}
