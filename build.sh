#!/bin/bash

set -ex

# init module system to prevent build fails
go mod init s3bench || true

now=$(date +'%Y-%m-%d-%T')
githash=$(git rev-parse HEAD)

echo "Building version $now-$githash..."

go build -ldflags "-X main.gitHash=$githash -X main.buildDate=$now"

cd ./s3bench_hash
go build ./s3bench_hash.go
cd -

echo "Complete"
