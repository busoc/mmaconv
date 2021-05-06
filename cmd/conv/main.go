package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/busoc/mmaconv"
)

const (
	timeFormat      = "2006.002.15.04.05.000000"
	isoFormat       = "2006-01-02T15:04:05.000000"
	splitFieldCount = 8 + 1
	flatFieldCount  = (3 * mmaconv.MeasCount) + 5 + 1
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

func main() {
	var (
		tbl     = mmaconv.DefaultTable
		out     File
		iso     = flag.Bool("i", false, "format time as RFC3339")
		flat    = flag.Bool("f", false, "keep values of same record")
		all     = flag.Bool("a", false, "write all fields")
		mini    = flag.Bool("z", false, "compress output file")
		recurse = flag.Bool("r", false, "recurse")
	)
	flag.Var(&tbl, "c", "use parameters table")
	flag.Var(&out, "w", "output file")
	flag.Parse()

	var w io.Writer = os.Stdout
	if out.IsSet() {
		defer out.Close()
		w = out

		if *mini {
			z, _ := gzip.NewWriterLevel(w, gzip.BestCompression)
			defer z.Close()
			w = z
		}
	}
	if err := process(w, tbl, flag.Arg(0), *flat, *all, *recurse, *iso); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func process(w io.Writer, tbl mmaconv.Table, dir string, flat, all, recurse, iso bool) error {
	var (
		headers     = splitHeaders
		writeRecord = writeSplit
	)
	if flat {
		writeRecord = writeFlat
		headers = nil
	}

	ws := csv.NewWriter(w)
	if !all && len(headers) > 0 {
		if err := ws.Write(headers); err != nil {
			return err
		}
	}
	filepath.Walk(dir, func(file string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			if !recurse {
				err = filepath.SkipDir
			}
			return err
		}
		ms, err := tbl.Calibrate(file)
		if err == nil {
			err = writeRecord(ws, ms, all, iso)
		}
		return err
	})
	ws.Flush()
	return ws.Error()
}

func writeFlat(ws *csv.Writer, data []mmaconv.Measurement, all, iso bool) error {
	size := flatFieldCount
	if all {
		size += allFieldDiff
	}
	tf := timeFormat
	if iso {
		tf = isoFormat
	}
	str := make([]string, 0, size)
	for _, m := range data {
		str = append(str, m.When.Format(tf))
		str = append(str, m.UPI)
		// str = append(str, strconv.Itoa(m.Seq))
		str = append(str, formatSequence(m.Seq))
		str = append(str, formatFloat(m.DegX))
		str = append(str, formatFloat(m.DegY))
		str = append(str, formatFloat(m.DegZ))
		if all {
			str = appendFields(str, m)
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
		}
		if err := ws.Write(str); err != nil {
			return err
		}
		str = str[:0]
	}
	return nil
}

var splitHeaders = []string{
	"time",
	"upi",
	"sequence",
	"Tx [degC]",
	"Ty [degC]",
	"Tz [degC]",
	"Ax [microG]",
	"Ay [microG]",
	"Az [microG]",
}

func writeSplit(ws *csv.Writer, data []mmaconv.Measurement, all, iso bool) error {
	size := splitFieldCount
	if all {
		size += allFieldDiff
	}
	tf := timeFormat
	if iso {
		tf = isoFormat
	}
	str := make([]string, 0, size)
	for _, m := range data {
		for i := 0; i < mmaconv.MeasCount; i++ {
			str = append(str, m.When.Format(tf))
			str = append(str, m.UPI)
			// str = append(str, strconv.Itoa(m.Seq))
			str = append(str, formatSequence(m.Seq))
			str = append(str, formatFloat(m.DegX))
			str = append(str, formatFloat(m.DegY))
			str = append(str, formatFloat(m.DegZ))
			if all {
				str = appendFields(str, m)
			}
			str = append(str, formatFloat(m.AccX[i]))
			str = append(str, formatFloat(m.AccY[i]))
			str = append(str, formatFloat(m.AccZ[i]))
			if err := ws.Write(str); err != nil {
				return err
			}
			str = str[:0]
		}
	}
	return nil
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

func formatSequence(v int) string {
	x := uint16(v)
	return strconv.FormatUint(uint64(x), 10)
}
