// Copyright 2020 QINIU. All rights reserved.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/someonegg/fcp-car/convert"
)

var inFile string
var outFile string

func init() {
	flag.StringVar(&inFile, "i", "", "input path")
	flag.StringVar(&outFile, "o", "", "output path")
}

func main() {
	flag.Parse()
	if inFile == "" || outFile == "" {
		os.Exit(1)
	}

	cid, carsz, err := convert.FileConvertToCAR(context.Background(), inFile, outFile)

	fmt.Println(cid, carsz, err)

	if err != nil {
		os.Exit(2)
	}
}
