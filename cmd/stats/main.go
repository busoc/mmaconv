package main

import (
	"flag"
	"fmt"
	"os"
  "path/filepath"
	"sort"
	"strings"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

const Pattern = "%s %4d: %6d (vmu-seq: %6d)"

func main() {
  withbad := flag.Bool("b", false, "keep bad files")
	flag.Parse()

	for _, a := range flag.Args() {
		var (
			prefix = fmt.Sprintf("doy %s:", splitFile(a))
			stat   = collect(a, *withbad)
		)
		for i, k := range stat.Keys {
			fmt.Printf(Pattern, prefix, k, stat.Stats[k].Count, stat.Stats[k].Seq)
			fmt.Println()
			if i == 0 {
				prefix = strings.Repeat(" ", len(prefix))
			}
		}
	}
}

func splitFile(file string) string {
  file = filepath.Clean(file)
  var (
    parts = strings.Split(file, "/")
    size = len(parts)
  )
  if size < 2 {
    return file
  }
  return strings.Join(parts[size-2:], "/")
}

type Count struct {
	Seq   uint32
	Count int
}

type Stat struct {
	Keys  []int
	Stats map[int]Count
}

func collect(dir string, bad bool) Stat {
	s := Stat{
		Stats: make(map[int]Count),
	}
	walk.Walk(dir, func(file string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
		}
    if ext := filepath.Ext(file); !bad && ext == ".bad" {
      return nil
    }
		rs, err := mmaconv.Convert(file, false)
		if err != nil || len(rs) == 0 {
			return err
		}
		count := len(rs) * mmaconv.MeasCount
		c, ok := s.Stats[count]
		if !ok {
			s.Keys = append(s.Keys, count)
			c.Seq = rs[0].Vid
		}
		c.Count++
		s.Stats[count] = c
		return nil
	})
	sort.Ints(s.Keys)
	return s
}
