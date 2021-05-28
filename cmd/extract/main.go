package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/busoc/mmaconv"
	"github.com/busoc/mmaconv/cmd/internal/walk"
)

func main() {
	var (
		quiet = flag.Bool("q", false, "quiet")
		nodup = flag.Bool("d", false, "remove duplicate")
	)
	flag.Parse()

	var out io.Writer = os.Stdout
	if *quiet {
		out = ioutil.Discard
	}
	ws := csv.NewWriter(out)
	defer ws.Flush()

	var (
		str  = make([]string, 2, 32)
		prev uint16
		msg  int
	)
	walk.Walk(flag.Arg(0), func(file string, i os.FileInfo, err error) error {
		if err != nil || i.IsDir() {
			return err
		}

		data, err := mmaconv.Convert(file, !*nodup)
		if err != nil || len(data) == 0 {
			return nil
		}
		for i, rec := range data {
			var diff uint16
			if i > 0 {
				diff = rec.Seq - prev
			}
			prev = rec.Seq
			str[0] = strconv.FormatUint(uint64(diff), 10)
			str[1] = strconv.FormatUint(uint64(rec.Seq), 10)
			for i := range rec.Raw[1:] {
				str = append(str, strconv.FormatInt(int64(rec.Raw[i+1]), 10))
			}
			if err := ws.Write(str); err != nil {
				return err
			}
			str = str[:2]
			msg++
		}
		return nil
	})
	ws.Flush()
	fmt.Printf("messages: %d", msg)
	fmt.Println()
}
