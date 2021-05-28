package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/options"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

const Pattern = "%s %4d: %8d (vmu-seq: %6d)"

func main() {
	var (
		verbose   = flag.Bool("v", false, "verbose")
		summarize = flag.Bool("s", false, "produce a summary")
		exlist    options.Exclude
	)
	flag.Var(&exlist, "x", "list of directories to be exlude from stats")
	flag.Parse()

	stats := makeStat()
	for _, a := range flag.Args() {
		a = splitFile(a)
		if exlist.Has(a) {
			continue
		}
		var (
			prefix = fmt.Sprintf("doy %s:", a)
			stat   = collect(a)
		)
		if !*summarize || *verbose {
			printStat(stat, prefix)
		}
		for c, s := range stat.Stats {
			count, ok := stats.Stats[c]
			if !ok {
				stats.Keys = append(stats.Keys, c)
			}
			count.Count += s.Count
			stats.Stats[c] = count
		}
	}
	if *summarize {
		if *verbose {
			fmt.Println("---")
		}
		printStat(stats, "summary")
	}
}

func printStat(stat Stat, prefix string) {
	sort.Ints(stat.Keys)
	for i, k := range stat.Keys {
		fmt.Printf(Pattern, prefix, k, stat.Stats[k].Count, stat.Stats[k].Seq)
		fmt.Println()
		if i == 0 {
			prefix = strings.Repeat(" ", len(prefix))
		}
	}
}

func splitFile(file string) string {
	file = filepath.Clean(file)
	var (
		parts = strings.Split(file, "/")
		size  = len(parts)
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

func makeStat() Stat {
	return Stat{
		Stats: make(map[int]Count),
	}
}

func collect(dir string) Stat {
	s := makeStat()
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
	return s
}
