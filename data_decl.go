package main

import "time"

var (
	gitHash   string
	buildDate string
)

const (
	opRead      = "Read"
	opWrite     = "Write"
	opHeadObj   = "HeadObj"
	opGetObjTag = "GetObjTag"
	opPutObjTag = "PutObjTag"
	opValidate  = "Validate"
)

const (
	maxPartSize        = int64(100 * 1024) //int64(5 * 1024 * 1024) max part size is 100kb
	maxRetries         = 3
	awsAccessKeyID     = "yD-Xf1u4Qd2ww_o2zW1l7Q"
	awsSecretAccessKey = "6Tub8JEOoWZhfgjfTWWg5h7KEt67+CZdPjgLBOpb"
	awsBucketRegion    = "US"
)

type Req struct {
	top string
	req interface{}
}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
	ttfb     time.Duration
}

// Specifies the parameters for a given test
type Params struct {
	requests         chan Req
	responses        chan Resp
	numSamples       uint
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucketName       string
	endpoints        []string
	verbose          bool
	headObj          bool
	sampleReads      uint
	clientDelay      int
	jsonOutput       bool
	deleteAtOnce     int
	putObjTag        bool
	getObjTag        bool
	numTags          uint
	readObj          bool
	tagNamePrefix    string
	tagValPrefix     string
	reportFormat     string
	validate         bool
	skipWrite        bool
	skipRead         bool
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	opDurations      []float64
	totalDuration    time.Duration
	opTtfb           []float64
	opErrors         []string
}
