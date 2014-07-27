package fs

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

type PathElem struct {
	Err  error
	Path string
	Info os.FileInfo
}

func WalkFiles(paths Paths) <-chan *PathElem {
	c := make(chan *PathElem)
	fn := makeWalkFilesFunc(c)
	go func() {
		for path, _ := range paths {
			if err := filepath.Walk(path, fn); err != nil && err != io.EOF {
				c <- &PathElem{Err: err}
			}
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

func AggregateLogErrors(c <-chan *PathElem, l *log.Logger) Paths {
	paths := make(Paths)

	for pe := range c {
		if pe.Err != nil {
			if l != nil {
				l.Printf("ERROR: %v", pe.Err)
			}
			continue
		}
		paths.Add(pe.Path)
	}

	return paths
}
