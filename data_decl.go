package main

import "time"

var (
	gitHash   string
	buildDate string
)

const (
	opRead  = "Read"
	opWrite = "Write"
	opHeadObj = "HeadObj"
	opGetObjTag = "GetObjTag"
	opPutObjTag = "PutObjTag"
	opValidate = "Validate"
	opMpUpl = "MultipartUpload"
)

type Req struct {
	top string
	key string
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
	s3MaxRetries          int
	s3Disable100Continue  bool
	httpClientTimeout     int
	connectTimeout        int
	TLSHandshakeTimeout   int
	maxIdleConnsPerHost   int
	idleConnTimeout       int
	responseHeaderTimeout int
	deleteClients         int
	protocolDebug         int
	deleteOnly            bool
	multipartSize         int64
	zero                  bool
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

// Delete op
type DeleteReq struct {
	opName  string
	dltReq  interface{}
}

// Delete resp
type DeleteResp struct {
	opName  string
	dur     time.Duration
	err     error
}
