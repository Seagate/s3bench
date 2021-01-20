package main

import (
	"bufio"
	"flag"
	"os"
	"io"
	"fmt"
	"crypto/sha512"
	"encoding/base32"
)

func to_b32(dt []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(dt)
}

func main() {
	flag.Usage = func() {
	fmt.Fprintf(flag.CommandLine.Output(), "%s calculates file's hash sum the same way\n", os.Args[0])
	fmt.Fprintf(flag.CommandLine.Output(), "s3bench does to generate object name.\n")
	fmt.Fprintf(flag.CommandLine.Output(), "It could be helpfull for manual object verification.\n\n")
	fmt.Fprintf(flag.CommandLine.Output(), "Usage:\n")
	flag.PrintDefaults()
}

	file := flag.String("file", "file.ext", "file to read")
	flag.Parse()

	f, err := os.Open(*file)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err = f.Close(); err != nil {
			panic(err)
		}
	}()

	hasher := sha512.New()
	reader := bufio.NewReader(f)
	sz, err := io.Copy(hasher, reader)
	sum := hasher.Sum(nil)
	hash_base32 := to_b32(sum[:])

	fmt.Printf("File %v \nSize %v \nS3Bench Hash %v \n", *file, sz, hash_base32)

}
