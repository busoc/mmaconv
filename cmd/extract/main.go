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
	flag.Parse()

	data, _, err := mmaconv.Convert(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if len(data) == 0 {
		return
	}

	ws := csv.NewWriter(os.Stdout)
	defer ws.Flush()

	str := make([]string, len(data[0]))
	for _, row := range data {
		for i := range row {
			str[i] = strconv.FormatInt(int64(row[i]), 10)
		}
		if err := ws.Write(str); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
}
