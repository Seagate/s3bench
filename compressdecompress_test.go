package main

import (
	"fmt"
	"testing"
)

func TestCompressDecompressObject(t *testing.T) {

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
			testName: "Test1: 20% compressible data for 100 bytes object",
			params: Params{
				objectSize:      100,
				dedupPercent:    0,
				compressPercent: 20,
				blockSize:       10,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 0,
		},
		{
			testName: "Test1: 25% compressible data for 4K object",
			params: Params{
				objectSize:      4096,
				dedupPercent:    0,
				compressPercent: 25,
				blockSize:       512,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 64% compressible data for 4K object",
			params: Params{
				objectSize:      4096,
				dedupPercent:    0,
				compressPercent: 64,
				blockSize:       512,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 25% compressible data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    0,
				compressPercent: 25,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 11% compressible data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    0,
				compressPercent: 11,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 5,
		},
		{
			testName: "Test1: 0% compressible data for 1MB object with 4k block",
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
			testName: "Test1: 100% compressible data for 1MB object with 4k block",
			params: Params{
				objectSize:      1024 * 1024,
				dedupPercent:    0,
				compressPercent: 100,
				blockSize:       4096,
				bufferPattern:   nil,
				zeroFill:        false,
			},
			percentMargin: 1,
		},
		{
			testName: "Test1: 22% compressible data for 512K object with 4k block",
			params: Params{
				objectSize:      512 * 1024,
				dedupPercent:    0,
				compressPercent: 22,
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

			// Compress the data
			compressedBuffer := compress(originalDataBuffer)

			// After compression len of data should be reduced by compressPercent, with margin of 5%
			// Sometimes data may not be compressible and size can increase.
			compressedBufferLength := len(compressedBuffer)
			originalDataBufferLength := len(originalDataBuffer)
			logMessageString += fmt.Sprintf("compressedBufferLength(%d)\n", compressedBufferLength)
			logMessageString += fmt.Sprintf("originalDataBufferLength(%d)\n", originalDataBufferLength)
			if compressedBufferLength > originalDataBufferLength {
				// Ignore, data is not compressible.
				logMessageString += fmt.Sprintf("Data is not compressible\n")
			} else {
				calculatedCompressPercent := int32(calcPercentage(originalDataBufferLength,
					compressedBufferLength))
				logMessageString += fmt.Sprintf("calculatedCompressPercent(%d)\n", calculatedCompressPercent)
				logMessageString += fmt.Sprintf("Requested compressPercent(%d)\n", testCase.params.compressPercent)

				diffPercent := getPositiveDiff(testCase.params.compressPercent, calculatedCompressPercent)
				logMessageString += fmt.Sprintf("diffPercent(%d)\n", diffPercent)

				// diffPercent should be less than testCase.percentMargin
				logMessageString += fmt.Sprintf("percentMargin(%d)\n", testCase.percentMargin)
				if diffPercent > testCase.percentMargin {
					t.Errorf("Invalid compressible data generation .")
					t.Errorf(logMessageString)
				}
				t.Log(logMessageString)
			}
		}) // go func
	} // t.Run

}
