package main

import (
	"time"
	"sync"
)

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
	opCopyObj = "CopyObject"
	opDelObj = "DeleteObjects"
	opDelTag = "DeleteTags"
)

// Req is used to submit reqs
type Req struct {
	top string
	key string
	req interface{}
}

// Resp is used to submit resps
type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
	ttfb     time.Duration
}

// Params contains applications settings
type Params struct {
	requests         chan Req
	responses        chan Resp
	numSamples       uint
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucketName       string
	profile          string
	endpoints        []string
	headObj          bool
	sampleReads      uint
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
	label                 string
	outstream             string
	outtype               string
	copies                int
}

// Result repr result of the op
type Result struct {
	operation        string
	bytesTransmitted int64
	opDurations      []float64
	totalDuration    time.Duration
	opTtfb           []float64
	opErrors         []string
}

// DeleteReq is used to submit delete req
type DeleteReq struct {
	opName  string
	dltReq  interface{}
}

// DeleteResp is used to submit delete resp
type DeleteResp struct {
	opName  string
	dur     time.Duration
	err     error
}

// MpDetails describe multipart upload req
type MpDetails struct {
	partsUploaded int32
	partsTags sync.Map
	startTime time.Time
	ttfb time.Duration
	uplID *string
}
