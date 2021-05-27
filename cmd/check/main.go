package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/busoc/mmaconv"
)

const (
	Diff    = time.Microsecond * 750
	Format  = "2006-01-02 15:04:05.000000"
	Pattern = "%d: %s - %s => diff: %10s (prev: %6d, curr: %6d, delta: %6d)"
)

func main() {
	mindur := flag.Duration("d", Diff, "check for difference greater than the given duration")
	flag.Parse()

	var r io.Reader = os.Stdin
	if flag.NArg() > 0 {
		f, err := os.Open(flag.Arg(0))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer f.Close()
		r = f
	}

	rs := csv.NewReader(r)
	rs.Comment = '#'
	rs.Comma = ','
	rs.Read()

	var (
		prev time.Time
		last uint16
	)
	for i := 1; ; i++ {
		row, err := rs.Read()
		if row == nil || err != nil {
			break
		}
		prev, last = check(row, i, prev, last, *mindur)
	}
}

func check(row []string, rid int, prev time.Time, last uint16, delta time.Duration) (time.Time, uint16) {
	var (
		now  = getTime(row[0])
		curr = getCount(row[2])
	)
	if !prev.IsZero() {
		var diff uint16
		if curr >= last {
			diff = curr - last
		} else {
			diff = curr + (mmaconv.MaxSequence - last) + 1
		}
		var (
			seqcheck = diff != 0 && diff != 9
			timcheck = now.Sub(prev) < 0 || now.Sub(prev) > delta
		)
		if timcheck || seqcheck {
			var (
				p = prev.UTC()
				n = now.UTC()
				d = now.Sub(prev)
			)
			fmt.Printf(Pattern, rid, p.Format(Format), n.Format(Format), d, last, curr, diff)
			fmt.Println()
		}
	}
	return now, curr
}

func getCount(field string) uint16 {
	x, _ := strconv.ParseUint(field, 0, 16)
	return uint16(x)
}

func getTime(field string) time.Time {
	w, _ := time.Parse("2006-01-02T15:04:05.000000", field)
	return w
}
