package blkidx

import (
	"bytes"
	"crypto"
	"io"
	"testing"
)

func TestHasherSmallWrites(t *testing.T) {
	alg := crypto.SHA256
	var h Hasher = NewHasher(alg, 1)

	writeAllBytes(t, h)

	all, blocks := h.Finish()
	if len(all) != alg.Size() {
		t.Errorf("all size - want %d; got %d", alg.Size(), len(all))
	}
	if len(blocks) != 256 {
		t.Errorf("blocks - want 256; got %d", len(blocks))
	}
	for idx, block := range blocks {
		if len(block) != alg.Size() {
			t.Errorf("block size - want %d; got %d", alg.Size(), len(block))
		}
		for i := idx + 1; i < 256; i++ {
			if bytes.Equal(block, blocks[i]) {
				t.Errorf("blocks %d and %d are equal", idx, i)
			}
		}
	}
}

func TestHasherOneBlock(t *testing.T) {
	alg := crypto.SHA256
	var h Hasher = NewHasher(alg, 256)

	writeAllBytes(t, h)

	all, blocks := h.Finish()
	if len(all) != alg.Size() {
		t.Errorf("all size - want %d; got %d", alg.Size(), len(all))
	}
	if len(blocks) != 1 {
		t.Errorf("blocks - want 1; got %d", len(blocks))
	}
	block := blocks[0]
	if len(block) != alg.Size() {
		t.Errorf("block size - want %d; got %d", alg.Size(), len(block))
	}
	if !bytes.Equal(all, block) {
		t.Errorf("overall and one block differ: %v - %v", all, block)
	}
}

func writeAllBytes(t *testing.T, w io.Writer) {
	var b [1]byte
	var p []byte = b[:]
	for i := 0; i <= 255; i++ {
		b[0] = byte(i)
		if n, err := w.Write(p); n != 1 || err != nil {
			t.Errorf("want (1, nil) - got (%d, %#v)", n, err)
		}
	}
}
