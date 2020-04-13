package logging

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type FreqLog struct {
	FileName     string
	LogPath      string
	LastCreate   time.Time
	fileInfo     *os.File
	MaxBytes     int
	bytesWritten int64
	acquire      sync.Mutex
	files        []string
}

func NewFreqLog(logpath string, filename string) *FreqLog {
	return &FreqLog{
		FileName: filename,
		LogPath:  logpath,
	}
}
func (l *FreqLog) Validate() error {
	if len(l.FileName) == 0 {
		return errors.New("filename is empty")
	}
	if err := os.MkdirAll(l.LogPath, 0755); err != nil {
		return err
	}
	return nil
}

//TODO file extension no txt
func (l *FreqLog) openNew() error {
	fileExt := filepath.Ext(l.FileName)
	if fileExt == "" {
		fileExt = ".txt"
	}
	fileName := strings.TrimSuffix(l.FileName, fileExt)
	createTime := time.Now()
	newfilePath := filepath.Join(l.LogPath, fileName)
	filePointer, err := os.OpenFile(newfilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	l.fileInfo = filePointer
	l.LastCreate = createTime
	l.bytesWritten = 0
	return nil
}

func (l *FreqLog) Write(b []byte) (n int, err error) {
	l.acquire.Lock()
	defer l.acquire.Unlock()
	realFilePath := filepath.Join(l.LogPath, l.FileName)
	fileObj, err := os.OpenFile(realFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer fileObj.Close()
	writeObj := bufio.NewWriterSize(fileObj, 4096)
	if b[len(b)-1] != '\n' {
		b = append(b, '\n')
	}
	//b = append(b, '\n')
	if _, err := writeObj.Write(b); err == nil {
		if err := writeObj.Flush(); err != nil {
			return 0, err
		} else {
			return 0, nil
		}
	} else {
		return 0, err
	}
}
