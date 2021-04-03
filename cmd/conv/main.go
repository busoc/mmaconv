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

func main() {
	var (
		tbl     = mmaconv.DefaultTable
		flat    = flag.Bool("f", false, "keep values of same record")
		all     = flag.Bool("a", false, "write all fields")
		mini    = flag.Bool("z", false, "compress output file")
		file    = flag.String("w", "", "output file")
		recurse = flag.Bool("r", false, "recurse")
	)
	flag.Var(&tbl, "c", "use parameters table")
	flag.Parse()

	var w io.Writer = os.Stdout
	if *file != "" {
		if err := os.MkdirAll(filepath.Dir(*file), 0755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		if *mini && filepath.Ext(*file) != ".gz" {
			*file += ".gz"
		}
		f, err := os.Create(*file)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer f.Close()

		w = f
		if *mini {
			z, _ := gzip.NewWriterLevel(w, gzip.BestCompression)
			defer z.Close()

			w = z
		}
	}
	writeRecord := writeSplit
	if *flat {
		writeRecord = writeFlat
	}

	ws := csv.NewWriter(w)
	defer ws.Flush()
	filepath.Walk(flag.Arg(0), func(file string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if i.IsDir() {
			if !*recurse {
				err = filepath.SkipDir
			}
			return err
		}
		ms, err := tbl.Calibrate(file)
		if err == nil {
			err = writeRecord(ws, ms, *all)
		}
		return err
	})
}

const (
	timeFormat      = "2006.002.15.04.05.000000"
	splitFieldCount = 8
	flatFieldCount  = (3 * mmaconv.MeasCount) + 5
	allField        = 9
)

func writeFlat(ws *csv.Writer, data []mmaconv.Measurement, all bool) error {
	size := flatFieldCount
	if all {
		size += allField
	}
	str := make([]string, 0, size)
	for _, m := range data {
		str = append(str, m.When.Format(timeFormat))
		str = append(str, strconv.Itoa(m.Seq))
		str = append(str, formatFloat(m.DegX))
		str = append(str, formatFloat(m.DegY))
		str = append(str, formatFloat(m.DegZ))
		if all {
			str = append(str, formatFloat(m.MicX))
			str = append(str, formatFloat(m.MicY))
			str = append(str, formatFloat(m.MicZ))
		}
		for i := 0; i < mmaconv.MeasCount; i++ {
			if all {
				str = append(str, formatFloat(m.ScaleX))
				str = append(str, formatFloat(m.OffsetX))
			}
			str = append(str, formatFloat(m.AccX))
			if all {
				str = append(str, formatFloat(m.ScaleY))
				str = append(str, formatFloat(m.OffsetY))
			}
			str = append(str, formatFloat(m.AccY))
			if all {
				str = append(str, formatFloat(m.ScaleZ))
				str = append(str, formatFloat(m.OffsetZ))
			}
			str = append(str, formatFloat(m.AccZ))
		}
		if err := ws.Write(str); err != nil {
			return err
		}
		str = str[:0]
	}
	return nil
}

func writeSplit(ws *csv.Writer, data []mmaconv.Measurement, all bool) error {
	size := splitFieldCount
	if all {
		size += allField
	}
	str := make([]string, 0, size)
	for _, m := range data {
		for i := 0; i < mmaconv.MeasCount; i++ {
			str = append(str, m.When.Format(timeFormat))
			str = append(str, strconv.Itoa(m.Seq))
			str = append(str, formatFloat(m.DegX))
			str = append(str, formatFloat(m.DegY))
			str = append(str, formatFloat(m.DegZ))
			if all {
				str = append(str, formatFloat(m.MicX))
				str = append(str, formatFloat(m.MicY))
				str = append(str, formatFloat(m.MicZ))
				str = append(str, formatFloat(m.ScaleX))
				str = append(str, formatFloat(m.OffsetX))
				str = append(str, formatFloat(m.ScaleY))
				str = append(str, formatFloat(m.OffsetY))
				str = append(str, formatFloat(m.ScaleZ))
				str = append(str, formatFloat(m.OffsetZ))
			}
			str = append(str, formatFloat(m.AccX))
			str = append(str, formatFloat(m.AccY))
			str = append(str, formatFloat(m.AccZ))
			if err := ws.Write(str); err != nil {
				return err
			}
			str = str[:0]
		}
	}
	return nil
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
