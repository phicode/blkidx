package blkidx

import (
	"io"
	"os"
	"path/filepath"
)

type PathElem struct {
	Err  error
	Path string
	Info os.FileInfo
}

func WalkFiles(root string) <-chan *PathElem {
	c := make(chan *PathElem)
	fn := makeWalkFilesFunc(c)
	go func() {
		if err := filepath.Walk(root, fn); err != nil && err != io.EOF {
			c <- &PathElem{Err: err}
		}
		close(c)
	}()
	return c
}

func makeWalkFilesFunc(c chan *PathElem) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			c <- &PathElem{Err: err}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		c <- &PathElem{
			Path: path,
			Info: info,
		}
		return nil
	}
}
