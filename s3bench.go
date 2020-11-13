package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha512"
	"flag"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	mathrand "math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var bufferBytes [][]byte
var data_hash_base32 string
var data_hash [sha512.Size]byte

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
	clientDelay := flag.Int("clientDelay", 1, "delay in ms before client starts. if negative value provided delay will be randomized in interval [0, abs{clientDelay})")
	jsonOutput := flag.Bool("jsonOutput", false, "print results in forma of json")
	deleteAtOnce := flag.Int("deleteAtOnce", 1000, "number of objs to delete at once")
	putObjTag := flag.Bool("putObjTag", false, "put object's tags")
	getObjTag := flag.Bool("getObjTag", false, "get object's tags")
	numTags := flag.Int("numTags", 10, "number of tags to create, for objects it should in range [1..10]")
	tagNamePrefix := flag.String("tagNamePrefix", "tag_name_", "prefix of the tag name that will be used")
	tagValPrefix := flag.String("tagValPrefix", "tag_val_", "prefix of the tag value that will be used")
	version := flag.Bool("version", false, "print version info")
	reportFormat := flag.String("reportFormat", "Version;Parameters;Parameters:numClients;Parameters:numSamples;Parameters:objectSize (MB);Parameters:sampleReads;Parameters:clientDelay;Parameters:readObj;Parameters:headObj;Parameters:putObjTag;Parameters:getObjTag;Tests:Operation;Tests:Total Requests Count;Tests:Errors Count;Tests:Total Throughput (MB/s);Tests:Duration Max;Tests:Duration Avg;Tests:Duration Min;Tests:Ttfb Max;Tests:Ttfb Avg;Tests:Ttfb Min;-Tests:Duration 25th-ile;-Tests:Duration 50th-ile;-Tests:Duration 75th-ile;-Tests:Ttfb 25th-ile;-Tests:Ttfb 50th-ile;-Tests:Ttfb 75th-ile;", "rearrange output fields")
	validate := flag.Bool("validate", false, "validate stored data")
	skipWrite := flag.Bool("skipWrite", false, "do not run Write test")
	skipRead := flag.Bool("skipRead", false, "do not run Read test")
	reductionBlockSizeStr := flag.String("reductionBlockSize", "4Kb", "Block size for deduplication and compression")
	cortxUnitSizeStr := flag.String("cortxUnitSize", "0b", "0: ignored. Blocks are duplicated only within every cortxUnitSize of data. Must be a multiple of reductionBlockSize")
	dedupPercent := flag.Int("dedupPercent", 0, "Percentage of duplicate data within objects")
	compressPercent := flag.Int("compressPercent", 0, "Percentage of compressible data within objects")
	compressBufferPattern := flag.String("compressBufferPattern", "", "Buffer pattern to be used for compression")
	compessBufferPatternFile := flag.String("compessBufferPatternFile", "", "File name containing compressible data used as compressBufferPattern")
	compressZeroFill := flag.Bool("compressZeroFill", false, "true: compressible data is zero'ed, (default)false: filled with fixed char 'A'")
	testReductionFile := flag.String("testReductionFile", "", "Dump generated object data to test file for verification purposes.")
	uniqueDataPerRequest := flag.Bool("uniqueDataPerRequest", false, "Each S3 PUT request will have it's own data, different from other S3 PUTs. Without this flag being set all PUT requests get the same data")

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

	if parse_size(*reductionBlockSizeStr) < 1 {
		fmt.Println("reductionBlockSize cannot be less than 1")
		os.Exit(1)
	}

	var cortxUnitSize int64 = parse_size(*cortxUnitSizeStr)
	var reductionBlockSize int64 = parse_size(*reductionBlockSizeStr)
	if *dedupPercent < 0 || *compressPercent < 0 || *dedupPercent > 100 || *compressPercent > 100 {
		fmt.Printf("Unsupported values for dedupPercent(%d) or compressPercent(%d)", *dedupPercent, *compressPercent)
		os.Exit(1)
	} else if *dedupPercent > 0 || *compressPercent > 0 {
		if cortxUnitSize > 0 {
			if cortxUnitSize%reductionBlockSize != 0 {
				fmt.Println("cortxUnitSize should be a multiple of reductionBlockSize.")
				os.Exit(1)
			}
			objSize := parse_size(*objectSize)
			if objSize < cortxUnitSize {
				fmt.Println("objectSize should be greater than cortxUnitSize.")
				os.Exit(1)
			}
		}
	} else {
		// both dedupPercent or compressPercent are equal to zero
		// no dedup or compressible data expected.
		cortxUnitSize = 0
	}

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:                 make(chan Req),
		responses:                make(chan Resp),
		numSamples:               uint(*numSamples),
		numClients:               uint(*numClients),
		objectSize:               parse_size(*objectSize),
		objectNamePrefix:         *objectNamePrefix,
		bucketName:               *bucketName,
		endpoints:                strings.Split(*endpoint, ","),
		verbose:                  *verbose,
		headObj:                  *headObj,
		sampleReads:              uint(*sampleReads),
		clientDelay:              *clientDelay,
		jsonOutput:               *jsonOutput,
		deleteAtOnce:             *deleteAtOnce,
		putObjTag:                *putObjTag || *getObjTag,
		getObjTag:                *getObjTag,
		numTags:                  uint(*numTags),
		readObj:                  !(*putObjTag || *getObjTag || *headObj) && !*skipRead,
		tagNamePrefix:            *tagNamePrefix,
		tagValPrefix:             *tagValPrefix,
		reportFormat:             *reportFormat,
		validate:                 *validate,
		skipWrite:                *skipWrite,
		skipRead:                 *skipRead,
		reductionBlockSize:       reductionBlockSize,
		cortxUnitSize:            cortxUnitSize,
		dedupPercent:             int32(*dedupPercent),
		compressPercent:          int32(*dedupPercent),
		compressBufferPattern:    *compressBufferPattern,
		compessBufferPatternFile: *compessBufferPatternFile,
		compressZeroFill:         *compressZeroFill,
		uniqueDataPerRequest:     *uniqueDataPerRequest,
	}

	if !params.skipWrite {
		// Generate the data from which we will do the writting
		params.printf("Generating in-memory sample data...\n")

		// Allocate the buffers
		// If uniqueDataPerRequest is enabled, create seperate buffer filled with data
		// per s3 put obj request, else just allocate & populate one buffer and use references
		// to same buffer
		bufferBytes = make([][]byte, params.numSamples, params.numSamples)
		var numberOfBuffers uint = 1
		if params.uniqueDataPerRequest {
			numberOfBuffers = params.numSamples
			// Allocate multiple buffers, one per s3 request
			for i := uint(0); i < params.numSamples; i++ {
				bufferBytes[i] = make([]byte, params.objectSize, params.objectSize)
			}
		} else {
			// Allocate only one buffer and use references to this single buffer.
			bufferBytes[0] = make([]byte, params.objectSize, params.objectSize)
			for i := uint(1); i < params.numSamples; i++ {
				bufferBytes[i] = bufferBytes[0] // point to the same buffer
			}
		}

		// Fill the buffers
		timeGenData := time.Now()
		if *dedupPercent > 0 || *compressPercent > 0 {
			totalUnits := int64(1)
			lastUnitSize := int64(0)
			if cortxUnitSize > 0 {
				// Apply fillBuffer() for each cortUnitSize 'ed buffer
				totalUnits = params.objectSize / cortxUnitSize
				if params.objectSize%cortxUnitSize != 0 {
					// We have one more unit at the end which is less than cortxUnitSize
					totalUnits++
					lastUnitSize = params.objectSize % cortxUnitSize
				}
			} else {
				// Treat entire object as single unit
				cortxUnitSize = params.objectSize
			}
			// Fill each preallocated buffer with data as per data reduction settings.
			var wg sync.WaitGroup
			for i := uint(0); i < numberOfBuffers; i++ {
				wg.Add(1)
				go func(bufferIndex uint) {
					defer wg.Done()
					for unitNumber := int64(0); unitNumber < totalUnits; unitNumber++ {
						startOffset := unitNumber * cortxUnitSize
						endOffset := startOffset + cortxUnitSize
						if unitNumber == totalUnits-1 {
							// This is last unit, check if its not equal to cortxUnitSize and update the end offset
							if lastUnitSize != 0 && lastUnitSize != cortxUnitSize {
								endOffset = startOffset + lastUnitSize
							}
						}
						fillBuffer(bufferBytes[bufferIndex][startOffset:endOffset], reductionBlockSize, int32(*dedupPercent),
							int32(*compressPercent), []byte(*compressBufferPattern), *compressZeroFill)
					}
				}(i)
			}
			wg.Wait()
		} else {
			for i := uint(0); i < numberOfBuffers; i++ {
				_, err := rand.Read(bufferBytes[i])
				if err != nil {
					panic("Could not allocate a buffer")
				}
			}
		}
		data_hash = sha512.Sum512(bufferBytes[0])
		data_hash_base32 = to_b32(data_hash[:])
		params.printf("Done (%s)\n", time.Since(timeGenData))

		// If data verification is expected, dump generated data to file and exit.
		if *testReductionFile != "" {
			fmt.Println("data_hash = ", data_hash)
			fmt.Println("data_hash_base32 = ", data_hash_base32)
			err := ioutil.WriteFile(*testReductionFile, bufferBytes[0], 0644)
			if err != nil {
				fmt.Println("Error writing to testReductionFile.")
				os.Exit(1)
			} else {
				os.Exit(0)
			}
		}
	}

	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String(*region),
		S3ForcePathStyle: aws.Bool(true),
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
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, params.deleteAtOnce)
		for i := 0; i < *numSamples; i++ {
			key := genObjName(params.objectNamePrefix, data_hash_base32, uint(i))

			if params.putObjTag {
				deleteObjectTaggingInput := &s3.DeleteObjectTaggingInput{
					Bucket: aws.String(*bucketName),
					Key:    key,
				}
				_, err := svc.DeleteObjectTagging(deleteObjectTaggingInput)
				params.printf("Delete tags %s |err %v\n", *key, err)
			}
			bar := s3.ObjectIdentifier{Key: key}
			keyList = append(keyList, &bar)
			if len(keyList) == params.deleteAtOnce || i == *numSamples-1 {
				params.printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				dltpar := &s3.DeleteObjectsInput{
					Bucket: aws.String(*bucketName),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(dltpar)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					params.printf("Succeeded\n")
				} else {
					params.printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		params.printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, *numSamples, time.Since(delStartTime))

		if bucket_created {
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
		key := genObjName(params.objectNamePrefix, data_hash_base32, i%params.numSamples)
		if op == opWrite {
			params.requests <- Req{
				top: op,
				req: &s3.PutObjectInput{
					Bucket: bucket,
					Key:    key,
					Body:   bytes.NewReader(bufferBytes[i]),
				},
			}
		} else if op == opRead || op == opValidate {
			params.requests <- Req{
				top: op,
				req: &s3.GetObjectInput{
					Bucket: bucket,
					Key:    key,
				},
			}
		} else if op == opHeadObj {
			params.requests <- Req{
				top: op,
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
				tagSet = append(tagSet, &s3.Tag{
					Key:   &tag_name,
					Value: &tag_value,
				})
			}
			params.requests <- Req{
				top: op,
				req: &s3.PutObjectTaggingInput{
					Bucket:  bucket,
					Key:     key,
					Tagging: &s3.Tagging{TagSet: tagSet},
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
		if params.clientDelay > 0 {
			time.Sleep(time.Duration(params.clientDelay) *
				time.Millisecond)
		} else if params.clientDelay < 0 {
			time.Sleep(time.Duration(mathrand.Intn(-params.clientDelay)) *
				time.Millisecond)
		}
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
		var hasher hash.Hash = nil

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
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes, ttfb}
	}
}
