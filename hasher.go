package blkidx

import (
	"crypto"
	"hash"
	"io"
)

type Hasher interface {
	io.Writer

	// the first return value is the hash of all data that has been written to this Hasher.
	// the second return value are the hashes of each block that has been written to this Hasher.
	Finish() ([]byte, [][]byte)
}

type hasher struct {
	all   hash.Hash
	block hash.Hash

	blockSize int
	blockRem  int
	blocks    [][]byte
}

var _ Hasher = (*hasher)(nil)

func NewHasher(algorithm crypto.Hash, blockSize int) Hasher {
	return &hasher{
		all:       algorithm.New(),
		block:     algorithm.New(),
		blockSize: blockSize,
		blockRem:  blockSize,
		blocks:    make([][]byte, 0, 8),
	}
}

func (h *hasher) Write(p []byte) (n int, err error) {
	n = len(p)
	h.all.Write(p)

	for {
		if h.blockRem == 0 {
			h.finishBlock()
		}

		if h.blockRem >= len(p) {
			h.block.Write(p)
			h.blockRem -= len(p)
			break
		}

		h.block.Write(p[:h.blockRem])
		p = p[h.blockRem:]
		h.finishBlock()
	}
	return
}

func (h *hasher) finishBlock() {
	if h.blockRem == h.blockSize {
		return
	}
	var blockHash []byte = h.block.Sum(nil)
	h.blocks = append(h.blocks, blockHash)
	h.block.Reset()
	h.blockRem = h.blockSize
}

func (h *hasher) Finish() ([]byte, [][]byte) {
	h.finishBlock()
	return h.all.Sum(nil), h.blocks
}
