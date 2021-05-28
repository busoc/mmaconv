package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/dump"
	"github.com/busoc/mmaconv/cmd/internal/options"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

type Flag struct {
	Adjust  bool
	Iso     bool
	Flat    bool
	All     bool
	Recurse bool
	Quiet   bool
	Write   string
	Time    time.Duration
	RecPer  int
}

func (f Flag) DumpFlag() dump.Flag {
	return dump.Flag{
		Iso:  f.Iso,
		All:  f.All,
		Time: f.Time,
	}
}

const Threshold = 1512

func main() {
	var (
		set   Flag
		sched options.Schedule
		tbl   = mmaconv.DefaultTable
	)
	flag.BoolVar(&set.Adjust, "j", false, "adjust time")
	flag.BoolVar(&set.Iso, "i", false, "format time as RFC3339")
	flag.BoolVar(&set.Flat, "f", false, "keep values of same record")
	flag.BoolVar(&set.All, "a", false, "write all fields")
	flag.BoolVar(&set.Recurse, "r", false, "recurse")
	flag.DurationVar(&set.Time, "t", 0, "time interval between two records")
	flag.IntVar(&set.RecPer, "b", Threshold, "max number of records per input files to compute date of each")
	flag.StringVar(&set.Write, "d", "", "diretory where files should be written")
	flag.Var(&tbl, "c", "use parameters table")
	flag.Var(&sched, "x", "range of dates")
	flag.Parse()

	os.RemoveAll(set.Write)
	if err := process(tbl, flag.Arg(0), set, sched); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func process(tbl mmaconv.Table, dir string, set Flag, sched options.Schedule) error {
	var (
		headers     = dump.SplitHeaders
		writeRecord = dump.Split
		freq        float64
	)
	if set.Flat {
		writeRecord = dump.Flat
		headers = nil
	}
	if set.Adjust {
		freq = tbl.SampleFrequency()
	}
	walk.Walk(dir, func(file string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			if !set.Recurse {
				err = filepath.SkipDir
			}
			return err
		}
		ms, err := tbl.Calibrate(file)
		if err != nil || len(ms) == 0 {
			return nil
		}
		if !sched.Keep(ms[0].When) {
			return nil
		}

		var w io.Writer = os.Stdout
		if set.Write != "" {
			path := filepath.Join(set.Write, ms[0].When.Format("2006/002.csv"))
			if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
				return err
			}
			f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			defer f.Close()
			w = f
		}
		ws := csv.NewWriter(w)
		if len(headers) > 0 {
			ws.Write(headers)
			ws.Flush()
			headers = nil
		}
		defer ws.Flush()

		df := set.DumpFlag()
		if n := len(ms) * mmaconv.MeasCount; set.RecPer > 0 && n >= set.RecPer {
			df.Indatable = true
		}
		_, err = writeRecord(ws, ms, freq, df)
		return err
	})
	return nil
}
