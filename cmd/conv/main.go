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
	"strconv"
	"time"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

const (
	timeFormat      = "2006.002.15.04.05.000000"
	isoFormat       = "2006-01-02T15:04:05.000000"
	splitFieldCount = 10
	flatFieldCount  = (3 * mmaconv.MeasCount) + 7
	allFieldDiff    = 9
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
}

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
		headers     = splitHeaders
		writeRecord = writeSplit
	)
	if set.Flat {
		writeRecord = writeFlat
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
			_, err = writeRecord(ws, ms, freq, set)
		}
		return err
	})
	return ws.Error()
}

var splitHeaders = []string{
	"time",
	"upi",
	"sequence",
	"vmu-sequence",
	"Tx [degC]",
	"Ty [degC]",
	"Tz [degC]",
	"Ax [microG]",
	"Ay [microG]",
	"Az [microG]",
}

func writeSplit(ws *csv.Writer, data []mmaconv.Measurement, freq float64, set Flag) (time.Time, error) {
	var now time.Time
	if len(data) == 0 {
		return now, nil
	}
	var (
		size = splitFieldCount
		tf   = timeFormat
	)
	if set.All {
		size += allFieldDiff
	}
	if set.Iso {
		tf = isoFormat
	}
	var (
		str     = make([]string, 0, size)
		delta   = time.Duration(freq*1_000_000_000) * time.Nanosecond
		prev    uint16
		elapsed time.Duration
	)
	if set.Time > 0 {
		delta = set.Time
	}
	for i, m := range data {
		curr := uint16(m.Seq)
		if d := sequenceDelta(i, curr, prev); d > 0 && d != mmaconv.MeasCount {
			elapsed += delta * time.Duration(d/mmaconv.MeasCount)
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			now = m.When.Add(elapsed)
			str = append(str, now.Format(tf))
			str = append(str, m.UPI)
			str = append(str, formatSequence(m.Seq))
			str = append(str, formatSequence2(m.Vid))
			str = append(str, formatFloat(m.DegX))
			str = append(str, formatFloat(m.DegY))
			str = append(str, formatFloat(m.DegZ))
			if set.All {
				str = appendFields(str, m)
			}
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
			if err := ws.Write(str); err != nil {
				return now, err
			}
			str = str[:0]
			elapsed += delta
		}
		prev = curr
	}
	ws.Flush()
	return now, ws.Error()
}

func writeFlat(ws *csv.Writer, data []mmaconv.Measurement, freq float64, set Flag) (time.Time, error) {
	var now time.Time
	if len(data) == 0 {
		return now, nil
	}
	var (
		size = flatFieldCount
		tf   = timeFormat
	)
	if set.All {
		size += allFieldDiff
	}
	if set.Iso {
		tf = isoFormat
	}
	var (
		str     = make([]string, 0, size)
		delta   = time.Duration(freq*1_000_000) * time.Microsecond
		prev    uint16
		elapsed time.Duration
	)
	if set.Time > 0 {
		delta = set.Time
	}
	for i, m := range data {
		curr := uint16(m.Seq)
		if d := sequenceDelta(i, curr, prev); d > 0 && d != mmaconv.MeasCount {
			elapsed += delta * time.Duration(d/mmaconv.MeasCount)
		}
		now = m.When.Add(elapsed)
		str = append(str, now.Format(tf))
		str = append(str, m.UPI)
		str = append(str, formatSequence(m.Seq))
		str = append(str, formatSequence2(m.Vid))
		str = append(str, formatFloat(m.DegX))
		str = append(str, formatFloat(m.DegY))
		str = append(str, formatFloat(m.DegZ))
		if set.All {
			str = appendFields(str, m)
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
		}
		if err := ws.Write(str); err != nil {
			return now, err
		}
		elapsed += mmaconv.MeasCount * delta
		prev = curr
		str = str[:0]
	}
	ws.Flush()
	return now, ws.Error()
}

func appendFields(str []string, m mmaconv.Measurement) []string {
	str = append(str, formatFloat(m.MicX))
	str = append(str, formatFloat(m.MicY))
	str = append(str, formatFloat(m.MicZ))
	str = append(str, formatFloat(m.ScaleX))
	str = append(str, formatFloat(m.OffsetX))
	str = append(str, formatFloat(m.ScaleY))
	str = append(str, formatFloat(m.OffsetY))
	str = append(str, formatFloat(m.ScaleZ))
	str = append(str, formatFloat(m.OffsetZ))
	return str
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatSequence2(v uint32) string {
	return strconv.FormatUint(uint64(v), 10)
}

func formatSequence(v uint16) string {
	return strconv.FormatUint(uint64(v), 10)
}

const MaxSequence = (1 << 16) - 1

func sequenceDelta(iter int, curr, prev uint16) uint16 {
	if iter <= 0 {
		return 0
	}
	if curr < prev {
		return curr + (MaxSequence - prev) + 1
	}
	return curr - prev
}
