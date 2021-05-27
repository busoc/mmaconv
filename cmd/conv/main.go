package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/dump"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

type File struct {
	io.WriteCloser
}

func (f *File) Set(file string) error {
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return err
	}
	w, err := os.Create(file)
	if err == nil {
		f.WriteCloser = w
	}
	return err
}

func (f *File) String() string {
	return "output file"
}

func (f *File) IsSet() bool {
	return f.WriteCloser != nil
}

type Flag struct {
	Adjust  bool
	Iso     bool
	Flat    bool
	All     bool
	Mini    bool
	Recurse bool
	Order   bool
	Quiet   bool
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
		out File
		set Flag
		tbl = mmaconv.DefaultTable
	)
	flag.BoolVar(&set.Adjust, "j", false, "adjust time")
	flag.BoolVar(&set.Iso, "i", false, "format time as RFC3339")
	flag.BoolVar(&set.Flat, "f", false, "keep values of same record")
	flag.BoolVar(&set.All, "a", false, "write all fields")
	flag.BoolVar(&set.Mini, "z", false, "compress output file")
	flag.BoolVar(&set.Recurse, "r", false, "recurse")
	flag.BoolVar(&set.Quiet, "q", false, "quiet")
	flag.BoolVar(&set.Order, "o", false, "order traversing by acqtime available in filename")
	flag.DurationVar(&set.Time, "t", 0, "time interval between two records")
	flag.IntVar(&set.RecPer, "b", Threshold, "max number of records per input files to compute date of each")
	flag.Var(&tbl, "c", "use parameters table")
	flag.Var(&out, "w", "output file")
	flag.Parse()

	var w io.Writer = ioutil.Discard
	if !set.Quiet {
		w = os.Stdout
		if out.IsSet() {
			defer out.Close()
			w = out

			if set.Mini {
				z, _ := gzip.NewWriterLevel(w, gzip.BestCompression)
				defer z.Close()
				w = z
			}
		}
	}
	if err := process(w, tbl, flag.Arg(0), set); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func process(w io.Writer, tbl mmaconv.Table, dir string, set Flag) error {
	var (
		headers     = dump.SplitHeaders
		writeRecord = dump.Split
	)
	if set.Flat {
		writeRecord = dump.Flat
		headers = nil
	}

	ws := csv.NewWriter(w)
	if !set.All && len(headers) > 0 {
		if err := ws.Write(headers); err != nil {
			return err
		}
		ws.Flush()
	}
	var walkfn = filepath.Walk
	if set.Order {
		walkfn = walk.Walk
	}
	walkfn(dir, func(file string, i os.FileInfo, err error) error {
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
		if len(ms) == 0 {
			return err
		}
		if err == nil {
			var freq float64
			if set.Adjust {
				freq = tbl.SampleFrequency()
			}
			df := set.DumpFlag()
			if n := len(ms) * mmaconv.MeasCount; set.RecPer > 0 && n >= set.RecPer {
				df.Indatable = true
			}
			_, err = writeRecord(ws, ms, freq, df)
		}
		return err
	})
	return ws.Error()
}
