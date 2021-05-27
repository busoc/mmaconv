package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

func main() {
	quiet := flag.Bool("q", false, "quiet")
	flag.Parse()
	var (
		files   uint64
		records uint64
		min int = -1
		max int = -1
	)
	walk.Walk(flag.Arg(0), func(file string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
		}
		rs, err := mmaconv.Convert(file, false)
		if err != nil || len(rs) == 0 {
			return err
		}
		total := len(rs) * mmaconv.MeasCount
		if min < 0 || total < min {
			min = total
		}
		if max < 0 || total > max {
			max = total
		}
		if !*quiet {
			fmt.Printf("%s: %d", file, total)
			fmt.Println()
		}
		files++
		records += uint64(total)
		return nil
	})
	if files == 0 {
		return
	}
	avg := records/files
	fmt.Printf("files: %d, records: %d (avg: %d, min: %d, max: %d)", files, records, avg, min, max)
	fmt.Println()
}
