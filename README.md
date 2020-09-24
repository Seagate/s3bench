# Initial
Cloned from
```
git clone -b data_verification_tool https://github.com/Seagate/s3bench/
```

# S3 Bench
This tool offers the ability to run the data verification against
an S3-compatible endpoint. It does a series of put operations followed by a
series of get operations and displays the corresponding error statistics. The tool
uses the AWS Go SDK.

## Requirements
* Go

## Installation
In case golang is not installed.

```
wget https://golang.org/dl/go1.15.2.linux-amd64.tar.gz
tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin
```
Check go version as : 
```
go version
```
Expected output : go version go1.15.2 linux/amd64

## Usage
The s3bench command is self-describing. In order to see all the available options
just run s3bench -help.

### Example input
The following will run a benchmark from 2 concurrent clients, which in
aggregate will put a total of 3 unique new objects. Each object will be
exactly 1024 Kbytes. The objects will be placed in a bucket named loadgen.
The S3 endpoint will be ran against http://s3.seagate.com . Object name will be prefixed with loadgen_test.

```
./s3bench -accessKey <access_key> -accessSecret <access_secret> -bucket <bucketname> -endpoint http://s3.seagate.com -numClients 2 -numSamples 3 -objectSize 1Mb -skipCleanup -verbose
```

For more information on this, please refer to [AmazonS3 documentation.](https://aws.amazon.com/documentation/s3/)



### Example output
The output will consist of details for every request being made as well as the
current average throughput. At the end of the run summaries of the put and get
operations will be displayed.

```
Generating in-memory sample data...
Data hashes of objects

A7U7ME6IF6UILNI24K6RFTETEZOBZP4YSRMRVTDLONC5S766OHUY74EG5W76QIKCP7KVI6FND2PNQ3TFS25W634AL436ZC2G4ZGVL6Q
P4GRBEVLF32AZ3OELPPHDRP5XDYWNPNZQ5NTHQM2X32AYQO6YA2DHYDYT5E6FSDBAL3WASZJQ77GS2LXB7U5R6PAZ6MQT5GZMEYFNII
CXTYGG3X73MGNQ7DPFX2BHDRLX6TST47YFR5HR3B2QIJ44RAO4BARWYS7BETQEQEVBBL3EFLJVLBX6R2N6WSKHDXZHCKK4SVBBMKT6I

Done (29.46795ms)
Running Write test...
Creating hash file ...
Creating hash file ...
operation Write(1) completed in 0.21s|%!s(<nil>)
Creating hash file ...
operation Write(2) completed in 0.24s|%!s(<nil>)
operation Write(3) completed in 0.27s|%!s(<nil>)
 Version:                    -
 Parameters:
    numClients:                 1
    numSamples:                 3
    objectSize (MB):            1.000
    sampleReads:                1
    clientDelay:                1
    readObj:                    false
    headObj:                    false
    putObjTag:                  false
    getObjTag:                  false
    bucket:                     pranavbucket
    deleteAtOnce:               1000
    endpoints:
       http://s3.seagate.com
    jsonOutput:                 false
    numTags:                    10
    objectNamePrefix:           loadgen_test
    reportFormat:               Version;Parameters;Parameters:numClients;Parameters:numSamples;Parameters:objectSize (MB);Parameters:sampleReads;Parameters:clientDelay;Parameters:readObj;Parameters:headObj;Parameters:putObjTag;Parameters:getObjTag;Tests:Operation;Tests:Total Requests Count;Tests:Errors Count;Tests:Total Throughput (MB/s);Tests:Duration Max;Tests:Duration Avg;Tests:Duration Min;Tests:Ttfb Max;Tests:Ttfb Avg;Tests:Ttfb Min;-Tests:Duration 25th-ile;-Tests:Duration 50th-ile;-Tests:Duration 75th-ile;-Tests:Ttfb 25th-ile;-Tests:Ttfb 50th-ile;-Tests:Ttfb 75th-ile;
    skipRead:                   true
    skipWrite:                  false
    tagNamePrefix:              tag_name_
    tagValPrefix:               tag_val_
    validate:                   false
    verbose:                    true
 Tests:
    Operation:                  Write
    Total Requests Count:       3
    Errors Count:               0
    Total Throughput (MB/s):    4.132
    Duration Max:               0.268
    Duration Avg:               0.242
    Duration Min:               0.214
    Ttfb Max:                   0.268
    Ttfb Avg:                   0.242
    Ttfb Min:                   0.214
    Duration 90th-ile:          0.268
    Duration 99th-ile:          0.268
    Errors:                     []
    Total Duration (s):         0.726
    Total Transferred (MB):     3.000
    Ttfb 90th-ile:              0.268
    Ttfb 99th-ile:              0.268

```
###To run the wrapper script

```
./data_verify.sh -K <access_key> -S <access_secret> -b <bucketname> -w <workload_type> -i <no. of iterations> -s <no. of samples> -c <no. of clients>
```

### Example output

```
Log file for this run : /var/log/last_run_s3bench_log.log !

Log file : /var/log/s3bench_log.log !

```
