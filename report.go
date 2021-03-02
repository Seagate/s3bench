package main

import (
	"log"
	"fmt"
	"sort"
	"strings"
	"encoding/json"
	"os"
)

func keysSort(keys []string, format []string) []string {
	sort.Strings(keys)
	curFormated := 0

	for _, fv := range format {
		fv = strings.TrimSpace(fv)
		shouldDel := strings.HasPrefix(fv, "-")
		if shouldDel {
			fv = fv[1:]
		}
		ci := findIdxOf(keys, fv)
		if ci < 0 {
			continue
		}
		// delete old pos
		keys = append(keys[:ci], keys[ci+1:]...)

		if !shouldDel {
			// insert new pos
			keys = append(keys[:curFormated], append([]string{fv}, keys[curFormated:]...)...)
			curFormated++
		}
	}

	return keys
}

func formatFilter(format []string, key string) []string {
	ret := []string{}
	for _, v := range format {
		if strings.HasPrefix(v, key + ":") {
			ret = append(ret, v[len(key + ":"):])
		} else if strings.HasPrefix(v, "-" + key + ":") {
			ret = append(ret, "-" + v[len("-" + key + ":"):])
		}
	}

	return ret
}

func mapPrint(m map[string]interface{}, repFormat []string, prefix string, outf *os.File) {
	var mkeys []string
	for k := range m {
		mkeys = append(mkeys, k)
	}
	mkeys = keysSort(mkeys, repFormat)
	for _, k := range mkeys {
		v := m[k]
		fmt.Fprintf(outf, "%s %-27s", prefix, k+":")
		switch val := v.(type) {
		case []string:
			if len(val) == 0 {
				fmt.Fprintf(outf, " []\n")
			} else {
				fmt.Fprintln(outf)
				for _, s := range val {
					fmt.Fprintf(outf, "%s%s %s\n", prefix, prefix, s)
				}
			}
		case map[string]interface{}:
			fmt.Fprintln(outf)
			mapPrint(val, formatFilter(repFormat, k), prefix + "   ", outf)
		case []map[string]interface{}:
			if len(val) == 0 {
				fmt.Fprintf(outf, " []\n")
			} else {
				valFormat := formatFilter(repFormat, k)
				for _, m := range val {
					fmt.Fprintln(outf)
					mapPrint(m, valFormat, prefix + "   ", outf)
				}
			}
		case float64:
			fmt.Fprintf(outf, " %.3f\n", val)
		default:
			fmt.Fprintf(outf, " %v\n", val)
		}
	}
}

func reportFilter(m map[string]interface{}, repFormat []string) map[string]interface{} {
	ret := make(map[string]interface{})
	var mkeys []string
	for k := range m {
		mkeys = append(mkeys, k)
	}
	mkeys = keysSort(mkeys, repFormat)
	for _, k := range mkeys {
		v := m[k]
		switch val := v.(type) {
		case map[string]interface{}:
			ret[k] = reportFilter(val, formatFilter(repFormat, k))
		case []map[string]interface{}:
			tarr := make([]map[string]interface{}, 0, len(val))
			valFormat := formatFilter(repFormat, k)
			for _, m := range val {
				tarr = append(tarr, reportFilter(m, valFormat))
			}
			ret[k] = tarr
		default:
			ret[k] = val
		}
	}
	return ret
}

func csvHeaders(m map[string]interface{}, repFormat []string, sep string) string {
	ret := ""
	var mkeys []string
	for k := range m {
		mkeys = append(mkeys, k)
	}
	mkeys = keysSort(mkeys, repFormat)
	for _, k := range mkeys {
		v := m[k]
		switch val := v.(type) {
		case map[string]interface{}:
			ret = fmt.Sprintf("%s%s%s", ret, sep, csvHeaders(val, formatFilter(repFormat, k), sep))
		case []map[string]interface{}:
			valFormat := formatFilter(repFormat, k)
			for _, m := range val {
				ret = fmt.Sprintf("%s%s%s", ret, sep, csvHeaders(m, valFormat, sep))
			}
		default:
			ret = fmt.Sprintf("%s%s%s", ret, sep, k)
		}
	}
	return strings.Trim(ret, sep)
}

func csvValues(m map[string]interface{}, repFormat []string, sep string, inValSep string) string {
	ret := ""
	var mkeys []string
	for k := range m {
		mkeys = append(mkeys, k)
	}
	mkeys = keysSort(mkeys, repFormat)
	for _, k := range mkeys {
		v := m[k]
		switch val := v.(type) {
		case []string:
			retV := ""
			for _, s := range val {
				retV = fmt.Sprintf("%s%s%s", retV, inValSep, s)
			}
			ret = fmt.Sprintf("%s%s%s", ret, sep, strings.Trim(retV, inValSep))
		case map[string]interface{}:
			ret = fmt.Sprintf("%s%s%s", ret, sep, csvValues(val, formatFilter(repFormat, k), sep, inValSep))
		case []map[string]interface{}:
			valFormat := formatFilter(repFormat, k)
			for _, m := range val {
				ret = fmt.Sprintf("%s%s%s", ret, sep, csvValues(m, valFormat, sep, inValSep))
			}
		default:
			ret = fmt.Sprintf("%s%s%v", ret, sep, val)
		}
	}
	return strings.Trim(ret, sep)
}

func (params Params) reportPrepare(tests []Result) map[string]interface{} {
	report := make(map[string]interface{})
	report["Version"] = fmt.Sprintf("%s-%s", buildDate, gitHash)
	report["Parameters"] = params.report()
	testreps := make([]map[string]interface{}, 0, len(tests))
	for _, r := range tests {
		testreps = append(testreps, r.report())
	}
	report["Tests"] = testreps
	return report
}

func (params Params) reportPrint(tests []Result, outf *os.File) {
	report := params.reportPrepare(tests)
	b, err := json.MarshalIndent(report, "", "    ")
	if err != nil {
		log.Printf("Cannot generate JSON report %v", err)
		log.Printf("Report:> %+v", report)
	} else {
		log.Printf("Report:> %v", string(b))
	}

	fltr := strings.Split(params.reportFormat, ";")
	report = reportFilter(report, fltr)

	if params.outtype == "json" {
		b, err = json.Marshal(report)
		if err != nil {
			log.Printf("Cannot generate JSON report %v", err)
		}
		fmt.Fprintln(outf, string(b))
		return

	}

	if params.outtype == "csv" {
		fmt.Fprintln(outf, csvHeaders(report, fltr, "\t"))
	}

	if strings.HasPrefix(params.outtype, "csv") {
		fmt.Fprintln(outf, csvValues(report, fltr, "\t", "|"))
		return
	}

	mapPrint(report, fltr, "", outf)
}

func (r Result) report() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["Operation"] = r.operation

	totreqs := len(r.opDurations)
	totdur := r.totalDuration.Seconds()
	ret["Total Requests Count"] = totreqs
	ret["Total Transferred (MB)"] = 0
	ret["Total Throughput (MB/s)"] = 0
	if r.operation == opWrite || r.operation == opRead || r.operation == opValidate {
		ret["Total Transferred (MB)"] = float64(r.bytesTransmitted)/(1024*1024)
		ret["Total Throughput (MB/s)"] = (float64(r.bytesTransmitted)/(1024*1024))/totdur
	}
	ret["Total Duration (s)"] = totdur

	if len(r.opDurations) > 0 {
		ret["Duration Max"] = percentile(r.opDurations, 100)
		ret["Duration Avg"] = avg(r.opDurations)
		ret["Duration Min"] = percentile(r.opDurations, 0)
		ret["Duration 99th-ile"] = percentile(r.opDurations, 99)
		ret["Duration 90th-ile"] = percentile(r.opDurations, 90)
		ret["Duration 75th-ile"] = percentile(r.opDurations, 75)
		ret["Duration 50th-ile"] = percentile(r.opDurations, 50)
		ret["Duration 25th-ile"] = percentile(r.opDurations, 25)
	}

	if len(r.opTtfb) > 0 {
		ret["Ttfb Max"] = percentile(r.opTtfb, 100)
		ret["Ttfb Avg"] = avg(r.opTtfb)
		ret["Ttfb Min"] = percentile(r.opTtfb, 0)
		ret["Ttfb 99th-ile"] = percentile(r.opTtfb, 99)
		ret["Ttfb 90th-ile"] = percentile(r.opTtfb, 90)
		ret["Ttfb 75th-ile"] = percentile(r.opTtfb, 75)
		ret["Ttfb 50th-ile"] = percentile(r.opTtfb, 50)
		ret["Ttfb 25th-ile"] = percentile(r.opTtfb, 25)
	}

	toterrs := len(r.opErrors)
	ret["Errors Count"] = toterrs
	ret["Errors"] = r.opErrors
	ret["RPS"] = float64(totreqs) / totdur
	return ret
}

func (params Params) report() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["endpoints"] =  params.endpoints
	ret["bucket"] = params.bucketName
	ret["objectNamePrefix"] = params.objectNamePrefix
	ret["objectSize (MB)"] = float64(params.objectSize)/(1024*1024)
	ret["numClients"] = params.numClients
	ret["numSamples"] = params.numSamples
	ret["sampleReads"] = params.sampleReads
	ret["headObj"] = params.headObj
	ret["deleteAtOnce"] = params.deleteAtOnce
	ret["numTags"] = params.numTags
	ret["putObjTag"] = params.putObjTag
	ret["getObjTag"] = params.getObjTag
	ret["readObj"] = params.readObj
	ret["tagNamePrefix"] = params.tagNamePrefix
	ret["tagValPrefix"] = params.tagValPrefix
	ret["reportFormat"] = params.reportFormat
	ret["validate"] = params.validate
	ret["skipWrite"] = params.skipWrite
	ret["skipRead"] = params.skipRead
	ret["s3MaxRetries"] = params.s3MaxRetries
	ret["s3Disable100Continue"] = params.s3Disable100Continue
	ret["httpClientTimeout"] = params.httpClientTimeout
	ret["connectTimeout"] = params.connectTimeout
	ret["TLSHandshakeTimeout"] = params.TLSHandshakeTimeout
	ret["maxIdleConnsPerHost"] = params.maxIdleConnsPerHost
	ret["idleConnTimeout"] = params.idleConnTimeout
	ret["responseHeaderTimeout"] = params.responseHeaderTimeout
	ret["deleteClients"] = params.deleteClients
	ret["protocolDebug"] = params.protocolDebug
	ret["deleteOnly"] = params.deleteOnly
	ret["multipartSize"] = params.multipartSize
	ret["zero"] = params.zero
	ret["profile"] = params.profile
	ret["label"] = params.label
	ret["outstream"] = params.outstream
	ret["outtype"] = params.outtype
	return ret
}
