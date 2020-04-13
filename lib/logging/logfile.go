package logging

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	ltime "process_data/lib/time"
)

var (
	defaultFileNameDateFormat = "20060102.150405"
)

//LogFile is used to setup a file based logger that also performs log rotation
type LogFile struct {
	//Name of the log file
	// New file name has the format : filename-timestampformat.extension
	FileName string `toml:"filename"`

	//Path to the log file
	LogPath string `toml:"path"`

	// TimestampFormat sets the format used for marshaling timestamps.
	FileNameDateFormat string `toml:"file_name_date_format"`

	FileNameDateAlign bool `toml:"file_name_date_align"`

	//Duration between each file rotation operation
	RotationDuration ltime.Duration `toml:"rotation_duration"`
	RotationCount    int            `toml:"rotation_count"`

	//jastCreated represents the creation time of the latest log
	lastCreated time.Time

	//fileInfo is the pointer to the current file being written to
	fileInfo *os.File

	//MaxBytes is the maximum number of desired bytes for a log file
	MaxBytes int `rotated_bytes`

	//bytesWritten is the number of bytes written in the current log file
	bytesWritten int64

	//acquire is the mutex utilized to ensure we have no concurrency issues
	acquire sync.Mutex
	files   []string
}

func (l *LogFile) Validate() error {
	if len(l.FileName) == 0 {
		return errors.New("Filename is empty")
	}
	if l.RotationDuration.Duration == 0 {
		l.RotationDuration.Duration = 24 * time.Hour
	}
	if l.FileNameDateFormat == "" {
		l.FileNameDateFormat = defaultFileNameDateFormat
	}
	if l.RotationCount == 0 {
		l.RotationCount = 3
	}
	if err := os.MkdirAll(l.LogPath, 0755); err != nil {
		return err
	}

	if err := l.rotateFileScanDisk(); err != nil {
		return err
	}

	return nil
}

// 扫描硬盘，删除多余日志文件
func (l *LogFile) rotateFileScanDisk() error {
	// TODO
	return nil
}

// 通过维护一个list来实现rotate的日志文件个数
func (l *LogFile) rotateFile(filename string) error {
	n := len(l.files)
	if n == 0 {
		l.files = append(l.files, filename)
		return nil
	}
	latestLogFile := l.files[n-1]
	if latestLogFile == filename {
		return nil
	}
	l.files = append(l.files, filename)
	if len(l.files) <= l.RotationCount {
		return nil
	}
	earliestLogFile := l.files[0]
	l.files = l.files[1:]
	if err := os.Remove(earliestLogFile); err != nil {
		return err
	}
	return nil
}

func (l *LogFile) openNew() error {
	// Extract the file extention
	fileExt := filepath.Ext(l.FileName)
	// If we have no file extension we append .log
	if fileExt == "" {
		fileExt = ".log"
	}
	// Remove the file extention from the filename
	fileName := strings.TrimSuffix(l.FileName, fileExt)
	// New file name has the format : filename-timestamp.extension
	now := time.Now()
	createTime := now
	if l.FileNameDateAlign {
		unixNano := now.UnixNano()
		seconds := (unixNano - unixNano%int64(l.RotationDuration.Duration)) / int64(time.Second)
		createTime = time.Unix(seconds, 0)
	}
	// newfileName := fileName + "-" + strconv.FormatInt(createTime.UnixNano(), 10) + fileExt
	newfileName := fileName + "-" + createTime.Format(l.FileNameDateFormat) + fileExt
	newfilePath := filepath.Join(l.LogPath, newfileName)
	os.MkdirAll(l.LogPath, 0755)
	// Try creating a file. We truncate the file because we are the only authority to write the logs
	filePointer, err := os.OpenFile(newfilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if err != nil {
		return err
	}
	l.fileInfo = filePointer
	l.lastCreated = createTime
	l.bytesWritten = 0

	l.rotateFile(newfilePath)
	return nil
}

func (l *LogFile) rotate() error {
	// Get the time from the last point of contact
	timeElapsed := time.Since(l.lastCreated)
	// Rotate if we hit the byte file limit or the time limit
	if (l.bytesWritten >= int64(l.MaxBytes) && (l.MaxBytes > 0)) || timeElapsed >= l.RotationDuration.Duration {
		l.fileInfo.Close()
		return l.openNew()
	}
	return nil
}

func (l *LogFile) Write(b []byte) (n int, err error) {
	l.acquire.Lock()
	defer l.acquire.Unlock()
	//Create a new file if we have no file to write to
	if l.fileInfo == nil {
		if err := l.openNew(); err != nil {
			return 0, err
		}
	}
	// Check for the last contact and rotate if necessary
	if err := l.rotate(); err != nil {
		return 0, err
	}
	l.bytesWritten += int64(len(b))
	return l.fileInfo.Write(b)
}
