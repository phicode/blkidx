package blkidx

import (
	"testing"
	"time"
)

func TestBlobValidate(t *testing.T) {
	var blob *Blob = nil
	verifyBlobError(t, blob.Validate(), blobErrNil)

	blob = new(Blob)
	verifyBlobError(t, blob.Validate(), blobErrEmptyName)

	blob.Name = " asdf "
	verifyBlobError(t, blob.Validate(), blobErrSpaceName)
	blob.Name = "asdf"

	verifyBlobError(t, blob.Validate(), blobErrZeroIdx)
	blob.IndexTime = time.Now()

	verifyBlobError(t, blob.Validate(), blobErrZeroMod)
	blob.ModTime = time.Now()

	verifyBlobError(t, blob.Validate(), blobErrHashAlg)
	blob.HashAlgorithm = DefaultHashAlgorithm

	verifyBlobError(t, blob.Validate(), blobErrBlkSize)
	blob.HashBlockSize = 1

	blob.Size = -1
	verifyBlobError(t, blob.Validate(), blobErrSize)

	blob.Size = 0
	if blob.Validate() != nil {
		t.Error("blob should be valid")
	}

	blob.Size = 1
	verifyBlobError(t, blob.Validate(), blobErrHashLen)
	blob.Hash = make([]byte, blob.HashAlgorithm.Size())

	verifyBlobError(t, blob.Validate(), blobErrBlkHashLen)
	blob.HashedBlocks = [][]byte{blob.Hash}

	if blob.Validate() != nil {
		t.Error("blob should be valid")
	}
}

func verifyBlobError(t *testing.T, err error, expected error) {
	if err == nil {
		t.Error("got nil, expected:", expected)
		return
	}
	if err != expected {
		t.Errorf("got %v, expected: %v", err, expected)
	}
}

func TestBlobHasChanged(t *testing.T) {
	var blob Blob
	if blob.HasChanged(0, time.Time{}) {
		t.Error("all zero block reports changed")
	}

	blob.Size = 1
	if !blob.HasChanged(0, time.Time{}) {
		t.Error("size changed blob does not report changed")
	}

	now := time.Now()
	blob.ModTime = now

	if !blob.HasChanged(1, now.Add(time.Nanosecond)) {
		t.Error("time changed blob does not report changed")
	}

	if blob.HasChanged(1, now) {
		t.Error("non zero block reports changed")
	}
}

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
