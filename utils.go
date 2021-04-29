package main

import (
	"log"
	"os"
	"fmt"
	"strconv"
	"regexp"
	"encoding/base32"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func startLog(lbl string) *os.File {
	flog, err := os.OpenFile(fmt.Sprintf("s3bench-%s.log", lbl),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Panic(err)
	}
	log.SetOutput(flog)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	return flog
}

func toB32(dt []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(dt)
}

func fromB32(s string) ([]byte, error) {
	return base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(s)
}

func parseSize(sz string) int64 {
	sizes := map[string]int64 {
		"b": 1,
		"Kb": 1024,
		"Mb": 1024 * 1024,
		"Gb": 1024 * 1024 * 1024,
	}
	re := regexp.MustCompile(`^(\d+)([bKMG]{1,2})$`)
	mm := re.FindStringSubmatch(sz)
	if len(mm) != 3 {
		log.Panic("Invalid objectSize value format")
	}
	val, err := strconv.ParseInt(string(mm[1]), 10, 64)
	mult, ex := sizes[string(mm[2])]
	if !ex || err != nil {
		log.Panic("Invalid objectSize value")
	}
	return val * mult
}

func getSampleIndex(smpl string) string {
	re := regexp.MustCompile(`((?:_\d+){1,2})$`)
	mm := re.FindStringSubmatch(smpl)
	if len(mm) == 2 {
		return mm[1]
	}
	return ""
}

func (params Params) totalOps(op string) uint {
	if op == opWrite || op == opMpUpl {
		return params.numSamples
	} else if op == opCopyObj {
		return params.numSamples * uint(params.copies)
	} else if op == opPutObjTag || op == opValidate || op == opDelObj || op == opDelTag {
		return params.numSamples * uint(params.copies + 1)
	}

	return params.numSamples * params.sampleReads * uint(params.copies + 1)
}

func (params Params) samplesWOcopies(op string) uint {
	if op == opWrite || op == opPutObjTag || op == opValidate ||
		op == opMpUpl || op == opDelObj || op == opDelTag {
		return params.numSamples
	} else if op == opCopyObj {
		return params.numSamples
	}

	return params.numSamples * params.sampleReads
}

func percentile(dt []float64, i int) float64 {
	ln := len(dt)
	if i >= 100 {
		i = ln - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(ln))
	}
	return dt[i]
}

func avg(dt []float64) float64 {
	ln := float64(len(dt))
	sm := float64(0)
	for _, el := range dt {
		sm += el
	}
	return sm / ln
}

func genObjName(pref string, hsh string, idx uint, cpIdx int) *string {
	ret := pref
	if hsh != "" {
		ret = fmt.Sprintf("%s_%s", pref, hsh)
	}
	ret = fmt.Sprintf("%s_%d", ret, idx)
	if cpIdx >= 0 {
		ret = fmt.Sprintf("%s_%d", ret, cpIdx)
	}
	return aws.String(ret)
}

func (params *Params) getObjectHash(cfg *aws.Config) (string, error){
	cfg.Endpoint = aws.String(params.endpoints[0])
	svc := s3.New(session.New(), cfg)

	result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(params.bucketName),
		MaxKeys: aws.Int64(1),
		Prefix: aws.String(params.objectNamePrefix),
	})

	if err != nil {
		return "", err
	}
	if len(result.Contents) == 0 {
		return "", fmt.Errorf("Empty bucket")
	}

	re := regexp.MustCompile(`^.*_([A-Z2-7]+)_[0-9]+$`)
	mm := re.FindStringSubmatch(*result.Contents[0].Key)
	if len(mm) != 2 {
		return "", fmt.Errorf("Invalid object name format")
	}

	return mm[1], nil
}

func intMin(a, b int64) int64 {
	if a > b {
		return b
	}

	return a
}

func findIdxOf(vals []string, v string) int {
	for idx, el := range vals {
		if el == v {
			return idx
		}
	}
	return -1
}

func createReportOut(outtype, outstream string) (*os.File, error) {
	fmd := os.O_CREATE|os.O_WRONLY

	if outtype != "csv+" {
		fmd = fmd|os.O_TRUNC
	} else {
		fmd = fmd|os.O_APPEND
	}

	ostrm, err := os.OpenFile(outstream, fmd, 0777)
	return ostrm, err
}

func nil2int(x interface{}) uint {
	if x == nil {
		return 0
	}

	return 1
}
