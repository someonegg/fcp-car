// Copyright 2020 QINIU. All rights reserved.

package main

import (
	"flag"
	"io"
	"log"
	"os"
)

var rawFile string
var carFile string

func init() {
	flag.StringVar(&rawFile, "r", "", "the raw file")
	flag.StringVar(&carFile, "c", "", "the car file")
}

const blockSize int64 = 1 << 20
const uniqueSize int64 = 512

func main() {
	flag.Parse()
	if rawFile == "" || carFile == "" {
		os.Exit(1)
	}

	rawF, err := os.Open(rawFile)
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	defer rawF.Close() //nolint:errcheck

	rawS, err := rawF.Stat()
	if err != nil {
		log.Println(err)
		os.Exit(2)
	}
	if rawS.Size()%blockSize != 0 {
		log.Println("Wrong raw file size")
		os.Exit(2)
	}

	carF, err := os.Open(carFile)
	if err != nil {
		log.Println(err)
		os.Exit(3)
	}
	defer carF.Close() //nolint:errcheck

	carS, err := carF.Stat()
	if err != nil {
		log.Println(err)
		os.Exit(3)
	}
	if carS.Size() <= rawS.Size() {
		log.Println("Wrong car file size")
		os.Exit(3)
	}

	blocks := rawS.Size() / blockSize

	block := make([]byte, blockSize)
	unique := make([]byte, uniqueSize)

	carOffset := int64(0x3d) // HEADER

	for nb := int64(0); nb < blocks; nb++ {
		_, err := rawF.Seek(nb*blockSize, 0)
		if err != nil {
			log.Println(err)
			os.Exit(4)
		}

		_, err = io.ReadFull(rawF, unique)
		if err != nil {
			log.Println(err)
			os.Exit(4)
		}

		for {
			_, err := carF.Seek(carOffset, 0)
			if err != nil {
				log.Println(err)
				os.Exit(5)
			}

			_, err = io.ReadFull(carF, block)
			if err != nil {
				log.Println(err)
				os.Exit(5)
			}

			location, newoff := locate(carOffset, block, unique)
			carOffset = newoff

			if location != 0 {
				log.Printf("Block %v Location %v\n", nb, location)
				break
			}
		}
	}
}

func locate(offset int64, block []byte, unique []byte) (location int64, newoff int64) {
	bsz := len(block)
	usz := len(unique)

loop1:
	for i := 0; i < bsz-usz+1; i++ {
		for j := 0; j < usz; j++ {
			if block[i+j] != unique[j] {
				continue loop1
			}
		}
		location = offset + int64(i)
		break
	}

	if location != 0 {
		newoff = location + blockSize
		return
	}

	newoff = offset + int64(bsz-usz+1)
	return
}
