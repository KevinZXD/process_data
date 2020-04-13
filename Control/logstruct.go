package Control

import (
	"errors"
	"strconv"
	"strings"
	"time"
	"process_data/lib/wredis"
)

type LogStruct struct {
	Uid         string
	Adid        string
	LogFilePath string
	LogFileName string
}

func NewFreqLogger(
	msg []byte,
	logpath string,
	lognumber int,
) (*LogStruct, error) {
	logArr := strings.Split(string(msg), "|")
	if len(logArr) < 4 {
		return nil, errors.New("logger not valid")
	}
	fileDateDir := time.Now().Format("2006-01-02/15/")
	bucket := wredis.FNV32aHash(logArr[4]) % uint64(lognumber)
	loggerStruct := LogStruct{
		Uid:         logArr[4],
		Adid:        logArr[0],
		LogFilePath: logpath + fileDateDir,
		LogFileName: strconv.FormatUint(bucket, 10) + "_pvlog.txt",
	}
	return &loggerStruct, nil
}
func (l *LogStruct) Ignore() bool {
	if len(l.Uid) == 0 || len(l.Adid) == 0 {
		return true
	}
	return false
}



func (l *LogStruct) GetRedisKey() string {
	if len(l.Adid) > 0 {
		return l.Adid
	}
	return ""
}
