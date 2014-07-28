package blkidx

import (
	"strings"
	"testing"
	"time"
)

func TestBlobValidate(t *testing.T) {
	var blob *Blob = nil
	verifyPartialError(t, blob.Validate(), "nil")

	blob = new(Blob)
	verifyPartialError(t, blob.Validate(), "empty name")

	blob.Name = " asdf "
	verifyPartialError(t, blob.Validate(), "space")
	blob.Name = "asdf"

	verifyPartialError(t, blob.Validate(), "index time")
	blob.IndexTime = time.Now()

	verifyPartialError(t, blob.Validate(), "modify time")
	blob.ModTime = time.Now()

	verifyPartialError(t, blob.Validate(), "algorithm")
	blob.HashAlgorithm = DefaultHashAlgorithm

	verifyPartialError(t, blob.Validate(), "block size")
	blob.HashBlockSize = 1

	blob.Size = -1
	verifyPartialError(t, blob.Validate(), "blob size")

	blob.Size = 0
	if blob.Validate() != nil {
		t.Error("blob should be valid")
	}

	blob.Size = 1
	verifyPartialError(t, blob.Validate(), "invalid hash length")
	blob.Hash = make([]byte, blob.HashAlgorithm.Size())

	verifyPartialError(t, blob.Validate(), "empty hashed block")
	blob.HashedBlocks = [][]byte{blob.Hash}

	if blob.Validate() != nil {
		t.Error("blob should be valid")
	}
}

func verifyPartialError(t *testing.T, err error, part string) {
	if err == nil {
		t.Error("expected error that contains:", part)
		return
	}
	if !strings.Contains(err.Error(), part) {
		t.Error("error not found:", part, "in:", err.Error())
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
