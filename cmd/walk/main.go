package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/busoc/mmaconv/cmd/internal/walk"
)

func main() {
	flag.Parse()
	walk.Walk(flag.Arg(0), func(file string, i os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		fmt.Println(file)
		return nil
	})
}
