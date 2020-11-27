package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pierrec/lz4"
)

// Compress the given data.
func compress(dataBuffer []byte) []byte {
	var compressedDataBuffer bytes.Buffer
	zw := lz4.NewWriter(&compressedDataBuffer)
	zw.WithConcurrency(4)
	r := bytes.NewReader(dataBuffer)
	_, err := io.Copy(zw, r)
	if err != nil {
		fmt.Println(err)
	}
	err = zw.Close()
	if err != nil {
		fmt.Println(err)
	}
	return compressedDataBuffer.Bytes()
}

func decompress(dataBuffer []byte) []byte {
	var deCompressedDataBuffer bytes.Buffer

	compressedDataBuffer := bytes.NewBuffer(dataBuffer)
	zr := lz4.NewReader(compressedDataBuffer)

	_, err := io.Copy(&deCompressedDataBuffer, zr)
	if err != nil {
		fmt.Println(err)
	}
	return deCompressedDataBuffer.Bytes()
}
