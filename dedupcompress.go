package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

var debugLogEnabled bool = false

func enableDebugLog() {
	debugLogEnabled = true
}

func printLog(msg string) {
	if debugLogEnabled {
		println(msg)
	}
}

// Method to fill up given buffer with data that can be compressed or deduplicated.
// param: bufToFill - Buffer to fill with generated data
// param: blockSize - base block size. When block size is 0 it generates units based on bufSize
// param: dedupPercent - Percentange of deplicate data within bufToFill expected on generated data.
// param: compressPercent - Percentange of compression expected on generated data.
// param: compressBufPattern - Pattern to be used as compressible data
// param: zeroFill - When compressBufPattern is not specified and this is true,
// 									 compressible data will be zero filled, else char 'A'
func fillBuffer(bufToFill []byte, blockSize int64, dedupPercent int32,
	compressPercent int32, compressBufPattern []byte, zeroFill bool) {

	var bufSize int64 = int64(len(bufToFill))
	var compressBufPatternSize int64 = int64(len(compressBufPattern))
	var compressibleBuffer []byte
	var compressibleBufferSize int64
	var totalUnits int64
	var unitSize int64 // typically = blocksize if specified

	timeGenData := time.Now()
	if dedupPercent > 100 || compressPercent > 100 {
		fmt.Printf("Unsupported values for dedupPercent(%d) or compressPercent(%d)", dedupPercent, compressPercent)
		os.Exit(1)
	}

	if dedupPercent == 0 {
		// No dedup expected
		unitSize = int64(len(bufToFill))
		totalUnits = 1 // Treat entire buffer as 1 unit for compression only
	} else {
		// dedupPercent specified.
		if blockSize > 0 {
			if blockSize >= bufSize || bufSize%blockSize != 0 {
				fmt.Printf("blockSize(%d), bufSize(%d): bufSize should be greater than and multiple of blockSize\n", blockSize, bufSize)
				os.Exit(1)
			}
			// blockSize <= bufSize * (dedupPercent/100)
			if !(blockSize <= int64(float64(bufSize)*(float64(dedupPercent)/100.0))) {
				fmt.Printf("blockSize(%d), bufSize(%d), dedupPercent(%d)\n", blockSize, bufSize, dedupPercent)
				fmt.Printf("Failed condition: ! (blockSize <= bufSize * (dedupPercent/100))")
				os.Exit(1)
			}
			// When blockSize is specified, bufSize should be atleast 100 * blockSize so that
			// Percentages can be applied realisticaly
			unitSize = blockSize
			totalUnits = bufSize / blockSize
		} else {
			// Break total buffer in 1% sized units of total bufSize
			// example bufSize=4096, 1% size = 4096 * 0.01 = 40.96 = ~40
			if bufSize < 100 {
				totalUnits = bufSize
			} else {
				totalUnits = int64(float64(bufSize) * (1.0 / 100.0))
			}
			// example bufSize=4096, totalUnits = 40, unitSize = 102.4 = ~102
			unitSize = bufSize / totalUnits
		}
	}

	// XXX Break into genCompressibleData()
	if compressPercent > 0 {
		compressibleBufferSize = int64(float64(unitSize) * float64(compressPercent) / 100.0)

		// Prepare compressible content that can be inserted in each unit buffer
		// if compressBufPattern is specified we use it else we depend on zeroFill or not
		if compressBufPatternSize == 0 {
			if zeroFill {
				// default is zero filled
				compressibleBuffer = make([]byte, compressibleBufferSize, compressibleBufferSize)
			} else {
				compressibleBuffer = bytes.Repeat([]byte("A"), int(compressibleBufferSize))
			}
		} else {
			// compressBufPattern is specified
			if compressibleBufferSize <= compressBufPatternSize {
				compressibleBuffer = compressBufPattern[0:compressibleBufferSize]
			} else {
				// Copy compressBufPattern multiple times to compressibleBuffer to fill it up
				compressibleBuffer = make([]byte, compressibleBufferSize, compressibleBufferSize)
				sizeToFill := compressBufPatternSize
				offset := int64(0)

				for sizeToFill > 0 {
					copy(compressibleBuffer[offset:], compressBufPattern[0:compressBufPatternSize])

					offset += compressBufPatternSize
					sizeToFill -= compressBufPatternSize
				}
			}
		}
	}

	// example 4096 - (40 * 102) = 4096 - 4080 = 16 = residueUnitSize
	var residueUnitSize int64 = bufSize - (totalUnits * unitSize)
	// assert( bufSize == (totalUnits * unitSize) + residueUnitSize )
	if bufSize != (totalUnits*unitSize)+residueUnitSize {
		printLog(string(debug.Stack()))
		panic("Software Bug in fillBuffer() or unsupported size.")
	}

	// example (40 * (20% / 100)) = 8
	var duplicateBufferCount int64 = int64(float64(totalUnits) * (float64(dedupPercent) / 100.0))
	// example 40 - 8 = 32
	var uniqueBufferCount int64 = totalUnits - duplicateBufferCount
	var currentOffset int64 = 0

	printLog(fmt.Sprintf("bufSize(%d)", bufSize))
	printLog(fmt.Sprintf("totalUnits(%d)", totalUnits))
	printLog(fmt.Sprintf("unitSize(%d)", unitSize))
	printLog(fmt.Sprintf("uniqueBufferCount(%d)", uniqueBufferCount))
	printLog(fmt.Sprintf("duplicateBufferCount(%d)", duplicateBufferCount))
	printLog(fmt.Sprintf("residueUnitSize(%d)", residueUnitSize))
	printLog(fmt.Sprintf("len(compressibleBuffer) = (%d)", len(compressibleBuffer)))

	// Fill up the unique buffers. XXX: Single rand.Read() ??
	for i := int64(0); i < uniqueBufferCount; i++ {
		_, err := rand.Read(bufToFill[currentOffset : currentOffset+unitSize])
		if err != nil {
			printLog(string(debug.Stack()))
			panic("Could not generate random data")
		}
		if compressPercent > 0 {
			// Copy compressible data within unit buffer, from offset+1 so its not continuos
			// w.r.t previous placement
			copy(bufToFill[currentOffset+1:], compressibleBuffer)
		}
		currentOffset += unitSize
	}
	// Fill up the duplicate buffers using one of the unique buffers.
	for i := int64(0); i < duplicateBufferCount; i++ {
		copy(bufToFill[currentOffset:], bufToFill[0:unitSize])
		if compressPercent > 0 {
			// Copy compressible data within unit buffer, from offset+1 so its not continuos
			// w.r.t previous placement
			copy(bufToFill[currentOffset+1:], compressibleBuffer)
		}
		currentOffset += unitSize
	}

	// calulatedDedupPercent cannot be always exactly equal. Any other better assertion?
	// assert (duplicated buffers / total buffers * 100 == dedupPercent)
	// calulatedDedupPercent := int32(math.Round((float64(duplicateBufferCount) / float64(totalUnits)) * 100))
	// if calulatedDedupPercent != dedupPercent {
	// 	printLog(string(debug.Stack()))
	// 	panic("Software Bug in fillBuffer()")
	// }

	// Fill up the residue
	if residueUnitSize > 0 {
		// Fill up the residue buffer uniquely
		_, err := rand.Read(bufToFill[currentOffset : currentOffset+residueUnitSize-1])
		if err != nil {
			printLog(string(debug.Stack()))
			panic("Could not generate random data")
		}
	}
	printLog(fmt.Sprintf("Time(fillBuffer()) (%s)\n", time.Since(timeGenData)))
}
