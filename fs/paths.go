package fs

import (
	"os"
	"path/filepath"
)

type Paths map[string]struct{}

func (p Paths) Add(path string) error {
	path, err := CleanAbsolute(path)
	if err != nil {
		return err
	}
	p[path] = struct{}{}
	return nil
}

func WorkingDirectory() (Paths, error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	return NewPaths(wd)
}

func NewPaths(paths ...string) (Paths, error) {
	rv := make(Paths, len(paths))

	for _, path := range paths {
		if err := rv.Add(path); err != nil {
			return nil, err
		}
	}

	return rv, nil
}

func CleanAbsolute(path string) (string, error) {
	if !filepath.IsAbs(path) {
		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			return "", err
		}
	}
	return filepath.Clean(path), nil
}
