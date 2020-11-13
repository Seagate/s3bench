#!/bin/bash

set -e

now=$(date +'%Y-%m-%d-%T')
githash=$(git rev-parse HEAD)

echo "Building version $now-$githash..."

rm -rf build/
go build -ldflags "-X main.gitHash=$githash -X main.buildDate=$now" -o build/s3bench

echo "Complete"
