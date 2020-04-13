#!/bin/bash


export GO111MODULE=on

# golang proxy 
# export GOPROXY=https://athens.azurefd.net
export GOPROXY=https://goproxy.io

case $1 in
linux32)
    rm -f process_data_linux32
    env GOOS=linux GOARCH=386 go build -o process_data_linux32
    exit 0
    ;;
linux64)
    rm -f process_data_linux64
    env GOOS=linux GOARCH=amd64 go build -o process_data_linux64
    exit 0
    ;;
mac)
    rm -f process_data_mac64
    env GOOS=darwin GOARCH=amd64 go build -o process_data_mac64
    exit 0
    ;;
local)
    rm -f process_data
    go build
    exit 0
    ;;
*)
    echo "Usage: build.sh { linux32 | linux64 | mac | local}"
    exit 0
    ;;
esac
