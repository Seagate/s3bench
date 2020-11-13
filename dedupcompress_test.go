package main

import (
	"bytes"
	"testing"
)

func TestDedup50Percent10BytesObject(t *testing.T) {
	var bufferBytes []byte
	var objectSize int64 = 10
	var dedupPercent int32 = 50
	var compressPercent int32 = 0
	var blockSize int64 = 5
	var bufferPattern []byte
	var zeroFill bool = false

	bufferBytes = make([]byte, objectSize, objectSize)

	// debugEnabled = true
	fillBuffer(bufferBytes, blockSize, dedupPercent, compressPercent,
		bufferPattern, zeroFill)

	if !bytes.Equal(bufferBytes[0:objectSize/2], bufferBytes[objectSize/2:]) {
		t.Errorf("Failed generating duplicate data.")
		t.Errorf("bufferBytes[] = %x", bufferBytes)
		t.Errorf("bufferBytes[0:%d] = %x", objectSize/2, bufferBytes[0:objectSize/2])
		t.Errorf("bufferBytes[%d:] = %x", objectSize/2, bufferBytes[objectSize/2:])
	}
}

func TestDedup20Percent100BytesObject(t *testing.T) {
	var bufferBytes []byte
	var objectSize int64 = 100
	var dedupPercent int32 = 20
	var compressPercent int32 = 0
	var blockSize int64 = 5
	var bufferPattern []byte
	var zeroFill bool = false

	bufferBytes = make([]byte, objectSize, objectSize)

	// debugEnabled = true
	fillBuffer(bufferBytes, blockSize, dedupPercent, compressPercent,
		bufferPattern, zeroFill)

	// Assert last 5 buffers as duplicate in reverse order
	currentOffset := objectSize - blockSize
	for i := 0; i < 5; i++ {
		firstChunkStart := currentOffset
		firstChunkEnd := currentOffset + blockSize
		secondChunkStart := firstChunkStart - blockSize
		secondChunkEnd := secondChunkStart + blockSize
		if !bytes.Equal(bufferBytes[firstChunkStart:firstChunkEnd], bufferBytes[secondChunkStart:secondChunkEnd]) {
			t.Errorf("Failed generating duplicate data.")
			t.Errorf("bufferBytes[] = %x", bufferBytes)
			t.Errorf("bufferBytes[%d:%d] = %x", firstChunkStart, firstChunkEnd, bufferBytes[firstChunkStart:firstChunkEnd])
			t.Errorf("bufferBytes[%d:%d] = %x", secondChunkStart, secondChunkEnd, bufferBytes[secondChunkStart:secondChunkEnd])
		}
		firstChunkStart = secondChunkStart
	}
}
