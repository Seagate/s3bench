package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha512"
	"hash"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
	"net"
	"net/http"
	"math"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var bufferBytes []byte
var data_hash_base32 string
var data_hash [sha512.Size]byte

// multipart
type MpDetails struct {
	parts_uploaded int32
	parts_tags sync.Map
	start_time time.Time
	ttfb time.Duration
	upl_id *string
}
var mp_info map[string]*MpDetails
var mp_parts int32

// true if created
// false if existed
func (params *Params) prepareBucket(cfg *aws.Config) bool {
	cfg.Endpoint = aws.String(params.endpoints[0])
	svc := s3.New(session.New(), cfg)
	req, _ := svc.CreateBucketRequest(
		&s3.CreateBucketInput{Bucket: aws.String(params.bucketName)})

	err := req.Send()

	if err == nil {
		return true
	} else if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou:") &&
		!strings.Contains(err.Error(), "BucketAlreadyExists:") {
		panic("Failed to create bucket: " + err.Error())
	}

	return false
}

func main() {
	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	region := flag.String("region", "igneous-test", "AWS region to use, eg: us-west-1|us-east-1, etc")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", "loadgen_test", "prefix of the object name that will be used")
	objectSize := flag.String("objectSize", "80Mb", "size of individual requests (must be smaller than main memory)")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	skipCleanup := flag.Bool("skipCleanup", false, "skip deleting objects created by this tool at the end of the run")
	verbose := flag.Bool("verbose", false, "print verbose per thread status")
	headObj := flag.Bool("headObj", false, "head-object request instead of reading obj content")
	sampleReads := flag.Int("sampleReads", 1, "number of reads of each sample")
	jsonOutput := flag.Bool("jsonOutput", false, "print results in forma of json")
	deleteAtOnce := flag.Int("deleteAtOnce", 1000, "number of objs to delete at once")
	putObjTag := flag.Bool("putObjTag", false, "put object's tags")
	getObjTag := flag.Bool("getObjTag", false, "get object's tags")
	numTags := flag.Int("numTags", 10, "number of tags to create, for objects it should in range [1..10]")
	tagNamePrefix := flag.String("tagNamePrefix", "tag_name_", "prefix of the tag name that will be used")
	tagValPrefix := flag.String("tagValPrefix", "tag_val_", "prefix of the tag value that will be used")
	version := flag.Bool("version", false, "print version info")
	reportFormat := flag.String("reportFormat", "Version;Parameters;Parameters:numClients;Parameters:numSamples;Parameters:objectSize (MB);Parameters:sampleReads;Parameters:readObj;Parameters:headObj;Parameters:putObjTag;Parameters:getObjTag;Tests:Operation;Tests:RPS;Tests:Total Requests Count;Tests:Errors Count;Tests:Total Throughput (MB/s);Tests:Total Duration (s);Tests:Total Transferred (MB);Tests:Duration Max;Tests:Duration Avg;Tests:Duration Min;Tests:Ttfb Max;Tests:Ttfb Avg;Tests:Ttfb Min;-Tests:Duration 25th-ile;-Tests:Duration 50th-ile;-Tests:Duration 75th-ile;-Tests:Ttfb 25th-ile;-Tests:Ttfb 50th-ile;-Tests:Ttfb 75th-ile;", "rearrange output fields")
	validate := flag.Bool("validate", false, "validate stored data")
	skipWrite := flag.Bool("skipWrite", false, "do not run Write test")
	skipRead := flag.Bool("skipRead", false, "do not run Read test")
	s3MaxRetries := flag.Int("s3MaxRetries", -1, "The maximum number of times that a request will be retried for failures. Defaults to -1, which defers the max retry setting to the service specific configuration")
	s3Disable100Continue := flag.Bool("s3Disable100Continue", false, "Set this to `true` to disable the SDK adding the `Expect: 100-Continue` header to PUT requests over 2MB of content. 100-Continue instructs the HTTP client not to send the body until the service responds with a `continue` status. This is useful to prevent sending the request body until after the request is authenticated, and validated. You should use this flag to disable 100-Continue if you experience issues with proxies or third party S3 compatible services")
	httpClientTimeout := flag.Int("httpClientTimeout", 0, "Specifies a time limit in Milliseconds for requests made by this Client. The timeout includes connection time, any redirects, and reading the response body. The timer remains running after Get, Head, Post, or Do return and will interrupt reading of the Response.Body. A Timeout of zero means no timeout")
	connectTimeout := flag.Int("connectTimeout", 0, "Maximum amount of time a dial will wait for a connect to complete. The default is no timeout")
	TLSHandshakeTimeout := flag.Int("TLSHandshakeTimeout", 0, "Specifies the maximum amount of time waiting to wait for a TLS handshake. Zero means no timeout")
	maxIdleConnsPerHost := flag.Int("maxIdleConnsPerHost", 0, "If non-zero, controls the maximum idle (keep-alive) connections to keep per-host. If zero, system default value is used")
	idleConnTimeout := flag.Int("idleConnTimeout", 0, "Max amount of time in Milliseconds an idle (keep-alive) connection will remain idle before closing itself. Zero means no limit")
	responseHeaderTimeout := flag.Int("responseHeaderTimeout", 0, "If non-zero, specifies the amount of time in Milliseconds to wait for a server's response headers after fully writing the request (including its body, if any). This time does not include the time to read the response body")
	deleteClients := flag.Int("deleteClients", 1, "Number of cocurent clients to send delete requests")
	protocolDebug := flag.Int("protocolDebug", 0, "Trace client-server exchange")
	deleteOnly := flag.Bool("deleteOnly", false, "Delete existing objects in the bucket")
	multipartSize := flag.String("multipartSize", "0b", "Run MultipartUpload with specified part size")
	zero := flag.Bool("zero", false, "Fill object content with all zeroes instead of random data")

	flag.Parse()

	if *version {
		fmt.Printf("%s-%s\n", buildDate, gitHash)
		os.Exit(0)
	}

	if *numClients > *numSamples || *numSamples < 1 {
		fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", *numClients, *numSamples)
		os.Exit(1)
	}

	if *endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *deleteAtOnce < 1 {
		fmt.Println("Cannot delete less than 1 obj at once")
		os.Exit(1)
	}

	if *numTags < 1 {
		fmt.Println("-numTags cannot be less than 1")
		os.Exit(1)
	}

	if *deleteClients < 1 {
		fmt.Println("-deleteClients cannot be less than 1")
		os.Exit(1)
	}

	if *s3MaxRetries < -1 {
		*s3MaxRetries = -1
	}

	if *httpClientTimeout < 0 {
		*httpClientTimeout = 0
	}

	if *connectTimeout < 0 {
		*connectTimeout = 0
	}

	if *TLSHandshakeTimeout < 0 {
		*TLSHandshakeTimeout = 0
	}

	if *maxIdleConnsPerHost < 0 {
		*maxIdleConnsPerHost = 0
	}

	if *idleConnTimeout < 0 {
		*idleConnTimeout = 0
	}
	if *responseHeaderTimeout < 0 {
		*responseHeaderTimeout = 0
	}

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       uint(*numSamples),
		numClients:       uint(*numClients),
		objectSize:       parse_size(*objectSize),
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
		verbose:          *verbose,
		headObj:          *headObj,
		sampleReads:      uint(*sampleReads),
		jsonOutput:       *jsonOutput,
		deleteAtOnce:     *deleteAtOnce,
		putObjTag:        *putObjTag || *getObjTag,
		getObjTag:        *getObjTag,
		numTags:          uint(*numTags),
		readObj:          !*skipRead,
		tagNamePrefix:    *tagNamePrefix,
		tagValPrefix:     *tagValPrefix,
		reportFormat:     *reportFormat,
		validate:         *validate,
		skipWrite:        *skipWrite,
		skipRead:         *skipRead,
		s3MaxRetries:          *s3MaxRetries,
		s3Disable100Continue:  *s3Disable100Continue,
		httpClientTimeout:     *httpClientTimeout,
		connectTimeout:        *connectTimeout,
		TLSHandshakeTimeout:   *TLSHandshakeTimeout,
		maxIdleConnsPerHost:   *maxIdleConnsPerHost,
		idleConnTimeout:       *idleConnTimeout,
		responseHeaderTimeout: *responseHeaderTimeout,
		deleteClients:         *deleteClients,
		protocolDebug:         *protocolDebug,
		deleteOnly:            *deleteOnly,
		multipartSize:         parse_size(*multipartSize),
		zero:                  *zero,
	}

	if params.deleteOnly {
		params.skipWrite = true
		params.skipRead = true
		params.putObjTag = false
		params.getObjTag = false
		params.headObj = false
		params.readObj = false
		params.validate = false
		params.multipartSize = 0
	}

	if !params.skipWrite || params.multipartSize > 0 {
		// Generate the data from which we will do the writting
		params.printf("Generating in-memory sample data...\n")
		timeGenData := time.Now()
		bufferBytes = make([]byte, params.objectSize, params.objectSize)
		if !params.zero {
			_, err := rand.Read(bufferBytes)
			if err != nil {
				panic("Could not allocate a buffer")
			}
		}
		data_hash = sha512.Sum512(bufferBytes)
		data_hash_base32 = to_b32(data_hash[:])
		params.printf("Done (%s)\n", time.Since(timeGenData))
	}

	if params.multipartSize > 0 {
		mp_parts = int32(math.Ceil(float64(params.objectSize) / float64(params.multipartSize)))
		mp_info = make(map[string]*MpDetails)
		params.skipWrite = true
	}

	httpClient := http.Client{
		Timeout:   time.Duration(*httpClientTimeout) * time.Millisecond,
		Transport: &http.Transport {
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(*connectTimeout) * time.Second,
				KeepAlive: 0, // Use system default value
			}).DialContext,
			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   time.Duration(*TLSHandshakeTimeout) * time.Second,
			MaxIdleConns:          0, // Unlimited
			MaxIdleConnsPerHost:   *maxIdleConnsPerHost,
			IdleConnTimeout:       time.Duration(*idleConnTimeout) * time.Millisecond,
			ResponseHeaderTimeout: time.Duration(*responseHeaderTimeout) * time.Millisecond,
		},
	}

	s3LogLevel := aws.LogOff
	if params.protocolDebug > 0 {
		s3LogLevel = aws.LogDebugWithRequestRetries | aws.LogDebugWithRequestErrors
	}
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String(*region),
		S3ForcePathStyle: aws.Bool(true),

		LogLevel:         &s3LogLevel,

		HTTPClient:            &httpClient,
		MaxRetries:            aws.Int(*s3MaxRetries),
		S3Disable100Continue:  aws.Bool(*s3Disable100Continue),
	}

	if data_hash_base32 == "" {
		var err error
		data_hash_base32, err = params.getObjectHash(cfg)
		if err != nil {
			panic(fmt.Sprintf("Cannot read object hash:> %v", err))
		}
		var hash_from_b32 []byte
		hash_from_b32, err = from_b32(data_hash_base32)
		if err != nil {
			panic(fmt.Sprintf("Cannot convert object hash:> %v", err))
		}
		copy(data_hash[:], hash_from_b32)
	}

	bucket_created := params.prepareBucket(cfg)

	params.StartClients(cfg)

	testResults := []Result{}

	if !params.skipWrite {
		params.printf("Running %s test...\n", opWrite)
		testResults = append(testResults, params.Run(opWrite))
	}
	if params.multipartSize > 0 {
		params.printf("Running %s test...\n", opMpUpl)
		testResults = append(testResults, params.Run(opMpUpl))
	}
	if params.putObjTag {
		params.printf("Running %s test...\n", opPutObjTag)
		testResults = append(testResults, params.Run(opPutObjTag))
	}
	if params.getObjTag {
		params.printf("Running %s test...\n", opGetObjTag)
		testResults = append(testResults, params.Run(opGetObjTag))
	}
	if params.headObj {
		params.printf("Running %s test...\n", opHeadObj)
		testResults = append(testResults, params.Run(opHeadObj))
	}
	if params.readObj {
		params.printf("Running %s test...\n", opRead)
		testResults = append(testResults, params.Run(opRead))
	}
	if params.validate {
		params.printf("Running %s test...\n", opValidate)
		testResults = append(testResults, params.Run(opValidate))
	}

	// Do cleanup if required
	if !*skipCleanup {
		params.printf("Cleaning up %d objects...\n", *numSamples)

		delOpCh := make(chan DeleteReq, 2 * params.deleteClients)
		delRespCh := make(chan DeleteResp, params.deleteClients)
		doDel := func() {
			svc := s3.New(session.New(), cfg)
			for dval := range delOpCh {
				delStartTime := time.Now()
				var derr error
				switch r := dval.dltReq.(type) {
				case *s3.DeleteObjectTaggingInput:
					_, derr = svc.DeleteObjectTagging(r)

				case *s3.DeleteObjectsInput:
					_, derr = svc.DeleteObjects(r)

				default:
					panic("Developer error")
				}

				delRespCh <- DeleteResp{
					opName: dval.opName,
					dur: time.Since(delStartTime),
					err: derr,
				}
			}
		}

		for i := 0; i < params.deleteClients; i++ {
			go doDel()
		}

		if params.putObjTag {
			go func() {
				for i := 0; i < *numSamples; i++ {
					key := genObjName(params.objectNamePrefix, data_hash_base32, uint(i))
					deleteObjectTaggingInput := &s3.DeleteObjectTaggingInput{
						Bucket: aws.String(*bucketName),
						Key:    key,
					}
					delOpCh <- DeleteReq{
						opName: fmt.Sprintf("Delete tags for %v/%v", *bucketName, *key),
						dltReq: deleteObjectTaggingInput,
					}
				}
			}()

			for i := 0; i < *numSamples; i++ {
				dresp := <- delRespCh
				params.printf("%s done in %v | err %v\n", dresp.opName, dresp.dur, dresp.err)
			}
		}

		dltpars := *numSamples / params.deleteAtOnce
		if *numSamples % params.deleteAtOnce != 0 {
			dltpars++
		}
		keyList := make([]*s3.ObjectIdentifier, 0, params.deleteAtOnce)

		go func() {
			for i := 0; i < *numSamples; i++ {
				key := aws.String("")
				if params.deleteOnly {
					key = aws.String(fmt.Sprintf("%s_%d", params.objectNamePrefix, uint(i)))
				} else {
					key = genObjName(params.objectNamePrefix, data_hash_base32, uint(i))
				}
				bar := s3.ObjectIdentifier{ Key: key, }
				keyList = append(keyList, &bar)
				if len(keyList) == params.deleteAtOnce || i == *numSamples-1 {
					dltpar := &s3.DeleteObjectsInput{
						Bucket: aws.String(*bucketName),
						Delete: &s3.Delete{
							Objects: keyList}}

					delOpCh <- DeleteReq{
						opName: fmt.Sprintf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i),
						dltReq: dltpar,
					}

					keyList = make([]*s3.ObjectIdentifier, 0, params.deleteAtOnce)
				}
			}
		}()

		for i := 0; i < dltpars; i++ {
			dresp := <- delRespCh
			params.printf("%s done in %v | err %v\n", dresp.opName, dresp.dur, dresp.err)
		}

		if bucket_created {
			svc := s3.New(session.New(), cfg)
			params.printf("Deleting bucket...\n")
			dltpar := &s3.DeleteBucketInput{
				Bucket: aws.String(*bucketName)}
			_, err := svc.DeleteBucket(dltpar)
			if err == nil {
				params.printf("Succeeded\n")
			} else {
				params.printf("Failed (%v)\n", err)
			}
		}
	}

	params.reportPrint(params.reportPrepare(testResults))
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	opSamples := params.spo(op)
	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, opSamples), operation: op}
	for i := uint(0); i < opSamples; i++ {
		resp := <-params.responses
		if resp.err != nil {
			errStr := fmt.Sprintf("%v(%d) completed in %0.2fs with error %s",
				op, i+1, resp.duration.Seconds(), resp.err)
			result.opErrors = append(result.opErrors, errStr)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
			result.opTtfb = append(result.opTtfb, resp.ttfb.Seconds())
		}
		params.printf("operation %s(%d) completed in %.2fs|%s\n", op, i+1, resp.duration.Seconds(), resp.err)
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	sort.Float64s(result.opTtfb)
	return result
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)
	opSamples := params.spo(op)
	for i := uint(0); i < opSamples; i++ {
		key := genObjName(params.objectNamePrefix, data_hash_base32, i % params.numSamples)
		if op == opWrite {
			params.requests <- Req{
				top: op,
				key: *key,
				req : &s3.PutObjectInput{
					Bucket: bucket,
					Key:    key,
					Body:   bytes.NewReader(bufferBytes),
				},
			}
		} else if op == opMpUpl {
			params.requests <- Req{
				top: op,
				key: *key,
				req: &s3.CreateMultipartUploadInput{
					Bucket: bucket,
					Key:    key,
				},
			}
		} else if op == opRead || op == opValidate {
				params.requests <- Req{
					top: op,
					key: *key,
					req: &s3.GetObjectInput{
						Bucket: bucket,
						Key:    key,
					},
				}
		} else if op == opHeadObj {
				params.requests <- Req{
					top: op,
					key: *key,
					req: &s3.HeadObjectInput{
						Bucket: bucket,
						Key:    key,
					},
				}
		} else if op == opPutObjTag {
			tagSet := make([]*s3.Tag, 0, params.numTags)
			for iTag := uint(0); iTag < params.numTags; iTag++ {
				tag_name := fmt.Sprintf("%s%d", params.tagNamePrefix, iTag)
				tag_value := fmt.Sprintf("%s%d", params.tagValPrefix, iTag)
				tagSet = append(tagSet, &s3.Tag {
						Key:   &tag_name,
						Value: &tag_value,
						})
			}
			params.requests <- Req{
				top: op,
				req: &s3.PutObjectTaggingInput{
					Bucket: bucket,
					Key:    key,
					Tagging: &s3.Tagging{ TagSet: tagSet, },
				},
			}
		} else if op == opGetObjTag {
			params.requests <- Req{
				top: op,
				req: &s3.GetObjectTaggingInput{
					Bucket: bucket,
					Key:    key,
				},
			}
		} else {
			panic("Developer error")
		}
	}
}

func (params *Params) StartClients(cfg *aws.Config) {
	for i := 0; i < int(params.numClients); i++ {
		cfg.Endpoint = aws.String(params.endpoints[i%len(params.endpoints)])
		go params.startClient(cfg)
	}
}

func (params *Params) genMpUpload(uplId *string, key *string) {
	bucket := aws.String(params.bucketName)
	for i := int64(0); i < int64(mp_parts); i++ {
		part_num := i + 1
		low := i * params.multipartSize
		high := IntMin((i + 1) * params.multipartSize, int64(len(bufferBytes)))
		params.requests <- Req{
			top: opMpUpl,
			key: *key,
			req : &s3.UploadPartInput{
				Bucket: bucket,
				Key:    key,
				PartNumber: &part_num,
				UploadId: uplId,
				Body:   bytes.NewReader(bufferBytes[low : high]),
			},
		}
	}
}

func (params *Params) genCompleteUpload(uplId *string, key *string) {
	bucket := aws.String(params.bucketName)
	parts := s3.CompletedMultipartUpload{}
	for i := int64(1); i <= int64(mp_parts); i++ {
		eTag, _ := mp_info[*key].parts_tags.Load(i)
		sETag := eTag.(string)
		pn := i
		p := s3.CompletedPart{PartNumber: &pn, ETag: &sETag,}
		parts.Parts = append(parts.Parts, &p)
	}
	params.requests <- Req{
		top: opMpUpl,
		key: *key,
		req : &s3.CompleteMultipartUploadInput{
			Bucket: bucket,
			Key:    key,
			UploadId: uplId,
			MultipartUpload: &parts,
		},
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {
	svc := s3.New(session.New(), cfg)
	for request := range params.requests {
		putStartTime := time.Now()
		var ttfb time.Duration
		var err error
		var numBytes int64 = 0
		cur_op := request.top
		cur_key := request.key
		var hasher hash.Hash = nil
		req_fin := true

		switch r := request.req.(type) {
		case *s3.PutObjectInput:
			req, _ := svc.PutObjectRequest(r)
			// Disable payload checksum calculation (very expensive)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
			ttfb = time.Since(putStartTime)
			if err == nil {
				numBytes = params.objectSize
			}
		case *s3.GetObjectInput:
			req, resp := svc.GetObjectRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
			if err == nil {
				if cur_op == opRead {
					numBytes, err = io.Copy(ioutil.Discard, resp.Body)
				} else if cur_op == opValidate {
					hasher = sha512.New()
					numBytes, err = io.Copy(hasher, resp.Body)
				}
			}
			if err != nil {
				numBytes = 0
			} else if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}
			if cur_op == opValidate && err == nil {
				cur_sum := hasher.Sum(nil)
				if !bytes.Equal(cur_sum, data_hash[:]) {
					cur_sum_enc := to_b32(cur_sum[:])
					err = fmt.Errorf("Read data checksum %s is not eq to write data checksum %s", cur_sum_enc, data_hash_base32)
				}
			}
		case *s3.HeadObjectInput:
			req, resp := svc.HeadObjectRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
			if err == nil {
				numBytes = *resp.ContentLength
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d, resp %v", params.objectSize, numBytes, resp)
			}
		case *s3.PutObjectTaggingInput:
			req, _ := svc.PutObjectTaggingRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
		case *s3.GetObjectTaggingInput:
			req, _ := svc.GetObjectTaggingRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
		case *s3.CreateMultipartUploadInput:
			mp_info[cur_key] = &MpDetails{start_time: putStartTime, parts_uploaded: 0, ttfb: 0, upl_id: nil,}
			req, resp := svc.CreateMultipartUploadRequest(r)
			err = req.Send()
			if err == nil {
				mp_info[cur_key].ttfb = time.Since(putStartTime)
				mp_info[cur_key].upl_id = resp.UploadId
				go params.genMpUpload(resp.UploadId, resp.Key)
				req_fin = false
			}
			numBytes = 0
			ttfb = time.Since(putStartTime)
		case *s3.UploadPartInput:
			req, resp := svc.UploadPartRequest(r)
			err = req.Send()
			if err == nil {
				req_fin = false
				mp_info[cur_key].parts_tags.Store(*r.PartNumber, *resp.ETag)
				cval := atomic.AddInt32(&mp_info[cur_key].parts_uploaded, 1)
				if cval == mp_parts {
					go params.genCompleteUpload(mp_info[cur_key].upl_id, &cur_key)
				}
			}
			numBytes = params.multipartSize
			putStartTime = mp_info[cur_key].start_time
			ttfb = mp_info[cur_key].ttfb
		case *s3.CompleteMultipartUploadInput:
			req, _ := svc.CompleteMultipartUploadRequest(r)
			err = req.Send()
			putStartTime = mp_info[cur_key].start_time
			ttfb = mp_info[cur_key].ttfb
			numBytes = params.objectSize
		default:
			panic("Developer error")
		}

		if req_fin {
			params.responses <- Resp{err, time.Since(putStartTime), numBytes, ttfb}
		}
	}
}
