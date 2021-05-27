package main

import (
	"compress/gzip"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/busoc/hadock"
	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/dump"
	"github.com/midbel/toml"
)

const Origin = "51"

type Option struct {
	Addr       string
	In         string `toml:"in-dir"`
	Out        string `toml:"out-dir"`
	KeepBad    bool   `toml:"keep-bad"`
	AdjustTime bool   `toml:"adjust-time"`
	IsoTime    bool   `toml:"iso-format"`
	Compress   bool
	Interval   string
}

func (o Option) DumpFlag() dump.Flag {
	dur, _ := time.ParseDuration(o.Interval)
	return dump.Flag{
		Iso:  o.IsoTime,
		All:  false,
		Time: dur,
	}
}

func main() {
	flag.Parse()

	var opt Option
	if err := toml.DecodeFile(flag.Arg(0), &opt); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	queue, err := Listen(opt.Addr, opt.KeepBad)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(3)
	}
	for m := range queue {
		if err := Process(m, opt); err != nil {
			fmt.Fprintf(os.Stderr, "fail to process file %s: %w", m.Reference, err)
      fmt.Fprintln(os.Stderr)
		}
	}
}

func Process(m hadock.Message, opt Option) error {
	var (
		in  = filepath.Join(opt.In, m.Reference)
		out = filepath.Join(opt.Out, m.Reference)
	)

	ms, err := mmaconv.DefaultTable.Calibrate(in)
	if err != nil {
		return err
	}

	var w io.Writer
	if f, err := os.Create(out); err != nil {
		return err
	} else {
		defer f.Close()
		w = f
	}

	if opt.Compress {
		z, _ := gzip.NewWriterLevel(w, gzip.BestCompression)
		defer z.Close()
		w = z
	}

	var freq float64
	if opt.AdjustTime {
		freq = mmaconv.DefaultTable.SampleFrequency()
	}

	ws := csv.NewWriter(w)
	_, err = dump.Split(ws, ms, freq, opt.DumpFlag())

	ws.Flush()
	return ws.Error()

}

func Listen(addr string, withbad bool) (<-chan hadock.Message, error) {
	a, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenMulticastUDP("udp", nil, a)
	if err != nil {
		return nil, err
	}
	queue := make(chan hadock.Message)
	go func() {
		defer func() {
			conn.Close()
			close(queue)
		}()
		for {
			m, err := hadock.DecodeMessage(conn)
			if err != nil {
				return
			}
			if ext := filepath.Ext(m.Reference); !withbad && ext == ".bad" {
				continue
			}
			if m.Origin != Origin {
				continue
			}
			queue <- m
		}
	}()
	return queue, nil
}
