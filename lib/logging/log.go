package logging

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/labstack/gommon/log"
)

const (
	logFormat = `${time_rfc3339} ${level} ${prefix} ${short_file}:${line}`
)

// Logger defines the logging interface.
type Logger interface {
	Output() io.Writer
	SetOutput(w io.Writer)
	Prefix() string
	SetPrefix(p string)
	Level() log.Lvl
	SetLevel(v log.Lvl)
	Print(i ...interface{})
	Printf(format string, args ...interface{})
	Printj(j log.JSON)
	Debug(i ...interface{})
	Debugf(format string, args ...interface{})
	Debugj(j log.JSON)
	Info(i ...interface{})
	Infof(format string, args ...interface{})
	Infoj(j log.JSON)
	Warn(i ...interface{})
	Warnf(format string, args ...interface{})
	Warnj(j log.JSON)
	Error(i ...interface{})
	Errorf(format string, args ...interface{})
	Errorj(j log.JSON)
	Fatal(i ...interface{})
	Fatalj(j log.JSON)
	Fatalf(format string, args ...interface{})
	Panic(i ...interface{})
	Panicj(j log.JSON)
	Panicf(format string, args ...interface{})
}

type LogConfig struct {
	// 日志级别 默认为 info, 取值范围：debug,info, warn,error
	Level string `toml:"level"`
	level log.Lvl

	// 日志文件配置
	File *LogFile `toml:"file"`
}

func (c *LogConfig) Validate() error {
	c.level = log.INFO
	if len(c.Level) != 0 {
		lvl, err := parseLevel(c.Level)
		if err != nil {
			return err
		}
		c.level = lvl
	}

	if err := c.File.Validate(); err != nil {
		return err
	}
	return nil
}

func DefaultLogger() Logger {
	l := log.New("")
	l.DisableColor()
	l.SetLevel(log.DEBUG)
	l.SetHeader(logFormat)

	return l
}

func parseLevel(lvl string) (log.Lvl, error) {
	switch strings.ToLower(lvl) {
	case "error":
		return log.ERROR, nil
	case "warn", "warning":
		return log.WARN, nil
	case "info":
		return log.INFO, nil
	case "debug":
		return log.DEBUG, nil
	}

	var l log.Lvl
	return l, fmt.Errorf("not a valid log Level: %q", lvl)
}

func SetLevel(lg Logger, name string) error {
	lvl, err := parseLevel(name)
	if err == nil {
		lg.SetLevel(lvl)
	}
	return nil
}

func NewLoggerWithConfig(config *LogConfig) Logger {
	l := log.New("")

	level := log.INFO
	if len(config.Level) != 0 {
		lvl, err := parseLevel(config.Level)
		if err != nil {
			panic(err.Error())
		}
		level = lvl
	}
	l.DisableColor()
	l.SetLevel(level)
	l.SetHeader(logFormat)

	if len(config.File.FileName) != 0 {
		// logFile := config.File
		if err := config.File.Validate(); err != nil {
			panic(err.Error())
		}
		l.SetOutput(config.File)
	} else {
		l.SetOutput(os.Stdout)
	}

	return l
}
