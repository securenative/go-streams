package go_streams

import (
	"fmt"
	"time"
)

type LogLevel int

const (
	Debug LogLevel = 1
	Info  LogLevel = 2
	Warn  LogLevel = 3
	Error LogLevel = 4
)

type Logger interface {
	Debug(message string, args ...interface{})
	Info(message string, args ...interface{})
	Warn(message string, args ...interface{})
	Error(message string, args ...interface{})
}

type defaultLogger struct {
	level LogLevel
}

func (l *defaultLogger) write(level LogLevel, levelStr, str string) {
	if level >= l.level {
		fmt.Printf("%s [%s] - %s\n", time.Now().Format(time.RFC3339), levelStr, str)
	}
}

func (l *defaultLogger) Debug(message string, args ...interface{}) {
	l.write(Debug, "DEBUG", fmt.Sprintf(message, args...))
}

func (l *defaultLogger) Info(message string, args ...interface{}) {
	l.write(Info, "INFO", fmt.Sprintf(message, args...))
}

func (l *defaultLogger) Warn(message string, args ...interface{}) {
	l.write(Warn, "WARN", fmt.Sprintf(message, args...))
}

func (l *defaultLogger) Error(message string, args ...interface{}) {
	l.write(Error, "ERROR", fmt.Sprintf(message, args...))
}

var logger Logger = &defaultLogger{Debug}

func Log() Logger {
	return logger
}

func SetLogger(l Logger) {
	logger = l
}

func SetLogLevel(level LogLevel) {
	l, ok := logger.(*defaultLogger)
	if ok {
		l.level = level
	}
}
