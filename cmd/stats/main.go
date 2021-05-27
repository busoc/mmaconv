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
	flag.Parse()

	for _, a := range flag.Args() {
		var (
			prefix = fmt.Sprintf("doy %s:", filepath.Base(a))
			stat   = collect(a)
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

type Count struct {
	Seq   uint32
	Count int
}

type Stat struct {
	Keys  []int
	Stats map[int]Count
}

func collect(dir string) Stat {
	s := Stat{
		Stats: make(map[int]Count),
	}
	walk.Walk(dir, func(file string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
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
