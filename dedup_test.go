package main

import (
	"bytes"
	"fmt"
	"testing"
)

func getPositiveDiff(val1 int32, val2 int32) int32 {
	if val1 > val2 {
		return val1 - val2
	} else {
		return val2 - val1
	}
}

func calcPercentage(val1 int, val2 int) float64 {
	diff := float64(0)
	if val1 > val2 {
		diff = ((float64(val1) - float64(val2)) / float64(val1)) * 100.0
	} else {
		diff = ((float64(val2) - float64(val1)) / float64(val2)) * 100.0
	}
	return diff
}

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

func TestDedupObject(t *testing.T) {

	type Params struct {
		objectSize      int64
		dedupPercent    int32
		compressPercent int32
		blockSize       int64
		bufferPattern   []byte
		zeroFill        bool
	}

	// Define various tests
	testCases := []struct {
		testName      string
		params        Params
		percentMargin int32 // expected error margin in calculated dedup percentage
	}{
		{
			testName: "Test1: 20% duplicate data for 100 bytes object",
			params: Params{
				objectSize:      100,
				dedupPercent:    20,
				compressPercent: 0,
				blockSize:       10,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 0,
		},
		{
			testName: "Test1: 25% duplicate data for 4K object",
			params: Params{
				objectSize:      4096,
				dedupPercent:    25,
				compressPercent: 0,
				blockSize:       512,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 64% duplicate data for 4K object",
			params: Params{
				objectSize:      4096,
				dedupPercent:    64,
				compressPercent: 0,
				blockSize:       512,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 25% duplicate data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    25,
				compressPercent: 0,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 11% duplicate data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    11,
				compressPercent: 0,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 0% duplicate data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    0,
				compressPercent: 0,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 0,
		},
		{
			testName: "Test1: 100% duplicate data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    100,
				compressPercent: 0,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 1,
		},
		{
			testName: "Test1: 22% duplicate data for 512K object with 4k block",
			params: Params{
				objectSize:      512 * 1024,
				dedupPercent:    22,
				compressPercent: 0,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 2,
		},
	}

	// Execute all the test cases
	for _, testCase := range testCases {
		t.Run(testCase.testName, func(t *testing.T) {
			var logMessageString string = "\n"
			originalDataBuffer := make([]byte, testCase.params.objectSize, testCase.params.objectSize)

			// Generate the duplicate data
			fillBuffer(originalDataBuffer, testCase.params.blockSize,
				testCase.params.dedupPercent, testCase.params.compressPercent,
				testCase.params.bufferPattern, testCase.params.zeroFill)
			originalDataBufferLength := len(originalDataBuffer)
			logMessageString += fmt.Sprintf("originalDataBufferLength(%d)\n", originalDataBufferLength)

			// Deduplicate the data
			dedupBuffer := dedup(originalDataBuffer, testCase.params.blockSize)
			dedupBufferLength := len(dedupBuffer)
			logMessageString += fmt.Sprintf("dedupBufferLength(%d)\n", dedupBufferLength)

			// After dedup len of data should be reduced by dedupPercent, with margin of 5%
			calculatedDedupPercent := int32(calcPercentage(len(originalDataBuffer),
				len(dedupBuffer)))
			logMessageString += fmt.Sprintf("calculatedDedupPercent(%d)\n", calculatedDedupPercent)
			logMessageString += fmt.Sprintf("Requested dedupPercent(%d)\n", testCase.params.dedupPercent)

			diffPercent := getPositiveDiff(testCase.params.dedupPercent, calculatedDedupPercent)
			logMessageString += fmt.Sprintf("diffPercent(%d)\n", diffPercent)

			// diffPercent should be less than testCase.percentMargin
			logMessageString += fmt.Sprintf("percentMargin(%d)\n", testCase.percentMargin)
			if diffPercent > testCase.percentMargin {
				t.Errorf("Invalid duplicate data generation .")
				t.Errorf(logMessageString)
			}
			t.Log(logMessageString)
		}) // go func
	} // t.Run

}
