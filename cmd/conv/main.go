package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/dump"
	"github.com/busoc/mmaconv/cmd/internal/options"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

type encoder struct {
	writer  io.WriteCloser
	encoder *csv.Writer
}

func (e *encoder) Close() error {
	e.encoder.Flush()
	return e.writer.Close()
}

type Cache struct {
	files   map[time.Time]*encoder
	headers []string
	dir     string
	mini    bool
}

func New(dir string, minify bool, headers []string) *Cache {
	return &Cache{
		files:   make(map[time.Time]*encoder),
		headers: headers,
		dir:     dir,
		mini:    minify,
	}
}

func (c *Cache) Close() error {
	for _, e := range c.files {
		e.Close()
	}
	return nil
}

func (c *Cache) Get(acq time.Time, doy string) (*csv.Writer, error) {
	acq = acq.Truncate(time.Hour * 24)
	e, ok := c.files[acq]
	if ok {
		return e.encoder, nil
	}

	file := acq.Format("2006/002")
	if doy != "" {
		file = fmt.Sprintf("%s.%s", file, doy)
	}
	wc, err := Create(filepath.Join(c.dir, file), c.mini, doy=="")
	if err != nil {
		return nil, err
	}

	ew := csv.NewWriter(wc)
	if len(c.headers) > 0 {
		ew.Write(c.headers)
		ew.Flush()
		if err := ew.Error(); err != nil {
			return nil, err
		}
	}
	c.files[acq] = &encoder{
		writer:  wc,
		encoder: ew,
	}
	return ew, nil
}

func glob(file string) int {
	files, _ := filepath.Glob(file)
	return len(files) + 1
}

type Writer struct {
	writer io.WriteCloser
	inner  io.WriteCloser
}

func Create(file string, mini, guess bool) (io.WriteCloser, error) {
	err := os.MkdirAll(filepath.Dir(file), 0755)
	if err != nil {
		return nil, err
	}
	var (
		ws  Writer
		ext = ".csv"
	)
	if mini {
		ext += ".gz"
	}
	if guess {
		count := glob(file + "*" + ext)
		file = fmt.Sprintf("%s.%d%s", file, count, ext)
	} else {
		file += ext
	}
	if ws.inner, err = os.Create(file); err != nil {
		return nil, err
	}
	if mini {
		z, _ := gzip.NewWriterLevel(ws.inner, gzip.BestCompression)
		ws.writer = ws.inner
		ws.inner = z
	}
	return &ws, nil
}

func (w *Writer) Write(b []byte) (int, error) {
	return w.inner.Write(b)
}

func (w *Writer) Close() error {
	w.inner.Close()
	if w.writer != nil {
		return w.writer.Close()
	}
	return nil
}

type Flag struct {
	Adjust  bool
	Iso     bool
	Flat    bool
	All     bool
	Recurse bool
	Quiet   bool
	Mini    bool
	Dir     string
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

const Threshold = 1521

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
	flag.BoolVar(&set.Mini, "z", false, "compress output file")
	flag.BoolVar(&set.Recurse, "r", false, "recurse")
	flag.DurationVar(&set.Time, "t", 0, "time interval between two records")
	flag.IntVar(&set.RecPer, "b", Threshold, "max number of records per input files to compute date of each")
	flag.StringVar(&set.Dir, "d", "", "diretory where files should be written")
	flag.Var(&tbl, "c", "parameters table to use")
	flag.Var(&sched, "x", "range of dates in config files when activities took place")
	flag.Parse()

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
	cache := New(set.Dir, set.Mini, headers)
	defer cache.Close()

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
		if err != nil || len(ms) == 0 || !sched.Keep(ms[0].When) {
			return nil
		}

		ws, err := cache.Get(ms[0].When, doy(file))
		if err != nil {
			return err
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

func doy(file string) string {
	var (
		parts = strings.Split(file, "/")
		size  = len(parts) - 4
	)
	if size < 0 {
		return ""
	}
	return parts[size]
}
