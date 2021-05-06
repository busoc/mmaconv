package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/busoc/mmaconv"
)

func main() {
	nodup := flag.Bool("d", false, "remove duplicate")
	flag.Parse()

	data, err := mmaconv.Convert(flag.Arg(0), !*nodup)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(data) == 0 {
		return
	}

	ws := csv.NewWriter(os.Stdout)
	defer ws.Flush()

	var (
		str  = make([]string, 2, 32)
		prev uint16
	)
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
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		str = str[:2]
	}
}
