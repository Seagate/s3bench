package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
)

func generateHash(chunk []byte) string {
	hasher := sha1.New()
	hasher.Write(chunk)
	return hex.EncodeToString(hasher.Sum(nil))
}

// Deduplicates data at using given chunk size
func dedup(dataBuffer []byte, chunkSize int64) []byte {
	var dedupBuffer []byte
	var dataBufferLength = int64(len(dataBuffer))
	var totalchunks int64 = dataBufferLength / chunkSize
	var residueChunkSize = dataBufferLength % chunkSize

	// Map of hash(chunk), offset within dataBuffer
	var dedupHashMap = make(map[string]int64)

	// For each chunk within dataBuffer, test for duplicate chunks.
	currentOffset := int64(0)
	for i := int64(0); i < totalchunks; i++ {
		currentChunk := dataBuffer[currentOffset : currentOffset+chunkSize]
		// Generate hash for current block
		hashValue := generateHash(currentChunk)
		// Check if hash present in map
		chunkOffset, collision := dedupHashMap[hashValue]
		if collision {
			possibleCollisionChunk := dataBuffer[chunkOffset : chunkOffset+chunkSize]
			// Check for duplicate with byte-by-byte compare.
			if bytes.Equal(currentChunk, possibleCollisionChunk) {
				// duplicate found, discard to dedup it
			} else {
				// Rare: Hash collision for 2 different data sets. XXX store offset for duplicate block
				// not a duplicate, store the chunk in dedupBuffer
				dedupBuffer = append(dedupBuffer, currentChunk...)
			}
		} else {
			// Insert in Map and store the chunk in dedupBuffer
			dedupHashMap[hashValue] = currentOffset
			dedupBuffer = append(dedupBuffer, currentChunk...)
		}
		currentOffset += chunkSize
	}
	if residueChunkSize != 0 {
		dedupBuffer = append(dedupBuffer, dataBuffer[currentOffset:currentOffset+residueChunkSize]...)
	}
	return dedupBuffer
}
