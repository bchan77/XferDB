package logger

import (
	"fmt"
	"time"
)

type Logger struct {
	prefix string
}

func New(prefix string) *Logger {
	return &Logger{prefix: prefix}
}

func (l *Logger) Info(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] INFO  %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), l.prefix, msg)
}

func (l *Logger) Error(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] ERROR %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), l.prefix, msg)
}

func (l *Logger) Debug(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	fmt.Printf("[%s] DEBUG %s: %s\n", time.Now().Format("2006-01-02 15:04:05"), l.prefix, msg)
}
