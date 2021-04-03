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
	csvExt          = ".csv"
	gzExt           = ".gz"
	timeFormat      = "2006.002.15.04.05.000000"
	splitFieldCount = 8
	flatFieldCount  = (3 * mmaconv.MeasCount) + 5
	allFieldDiff    = 9
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

	var (
		w   io.Writer = os.Stdout
		err error
	)
	if *file != "" {
		*file, err = makeFile(*file, *mini)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
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
	if err := process(w, tbl, flag.Arg(0), *flat, *all, *recurse); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func process(w io.Writer, tbl mmaconv.Table, dir string, flat, all, recurse bool) error {
	writeRecord := writeSplit
	if flat {
		writeRecord = writeFlat
	}

	ws := csv.NewWriter(w)
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
			err = writeRecord(ws, ms, all)
		}
		return err
	})
	ws.Flush()
	return ws.Error()
}

func writeFlat(ws *csv.Writer, data []mmaconv.Measurement, all bool) error {
	size := flatFieldCount
	if all {
		size += allFieldDiff
	}
	str := make([]string, 0, size)
	for _, m := range data {
		str = append(str, m.When.Format(timeFormat))
		str = append(str, strconv.Itoa(m.Seq))
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

func writeSplit(ws *csv.Writer, data []mmaconv.Measurement, all bool) error {
	size := splitFieldCount
	if all {
		size += allFieldDiff
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

func makeFile(file string, minify bool) (string, error) {
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return "", err
	}
	if filepath.Ext(file) != csvExt {
		file += csvExt
	}
	if minify && filepath.Ext(file) != gzExt {
		file += gzExt
	}
	return file, nil
}
