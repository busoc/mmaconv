package walk

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func Walk(root string, fn filepath.WalkFunc) error {
	info, err := os.Lstat(root)
	if err != nil {
		err = fn(root, nil, err)
	} else {
		err = walk(root, info, fn)
	}
	if err == filepath.SkipDir {
		return nil
	}
	return err
}

func walk(path string, info os.FileInfo, walkFn filepath.WalkFunc) error {
	if !info.IsDir() {
		return walkFn(path, info, nil)
	}

	names, err := readDirNames(path)
	if err != nil {
		return err
	}
	if err := walkFn(path, info, err); err != nil {
		return err
	}

	for _, name := range names {
		file := filepath.Join(path, name)
		fi, err := os.Lstat(file)
		if err != nil {
			if err := walkFn(file, fi, err); err != nil && err != filepath.SkipDir {
				return err
			}
		}
		if ext := filepath.Ext(file); ext == ".bad" {
			continue
		}
		err = walk(file, fi, walkFn)
		if err != nil {
			if !fi.IsDir() || err != filepath.SkipDir {
				return err
			}
		}
	}
	return nil
}

func readDirNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	sort.Slice(names, func(i, j int) bool {
		iseq, iwhen := splitFile(filepath.Base(names[i]))
		jseq, jwhen := splitFile(filepath.Base(names[j]))

		if iwhen.IsZero() || jwhen.IsZero() {
			return names[i] < names[j]
		}

		if iwhen.After(jwhen) {
			return false
		}
		if iwhen.Before(jwhen) {
			return true
		}
		return iseq < jseq
	})
	return names, nil
}

const timePattern = "20060102_150405"

func splitFile(file string) (int, time.Time) {
	var (
		ps = strings.Split(file, "_")
		z  = len(ps)
	)
	if z <= 1 {
		return 0, time.Time{}
	}
	when, _ := time.Parse(timePattern, strings.Join(ps[z-3:z-1], "_"))
	seq, _ := strconv.ParseInt(ps[z-4], 10, 64)
	return int(seq), when
}
