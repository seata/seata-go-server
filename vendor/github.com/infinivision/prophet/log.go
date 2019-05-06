package prophet

import (
	"fmt"
	stdLog "log"
	"os"
)

var (
	log Logger
)

// Logger logger
type Logger interface {
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Warn(v ...interface{})
	Warnf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
}

type emptyLog struct{}

func (l *emptyLog) Info(v ...interface{})                  {}
func (l *emptyLog) Infof(format string, v ...interface{})  {}
func (l *emptyLog) Debug(v ...interface{})                 {}
func (l *emptyLog) Debugf(format string, v ...interface{}) {}
func (l *emptyLog) Warn(v ...interface{})                  {}
func (l *emptyLog) Warnf(format string, v ...interface{})  {}
func (l *emptyLog) Error(v ...interface{})                 {}
func (l *emptyLog) Errorf(format string, v ...interface{}) {}
func (l *emptyLog) Fatal(v ...interface{}) {
	stdLog.Output(4, fmt.Sprintln(v...))
	os.Exit(-1)
}
func (l *emptyLog) Fatalf(format string, v ...interface{}) {
	stdLog.Output(4, fmt.Sprintf(format, v...))
	os.Exit(-1)
}

func init() {
	log = &emptyLog{}
}

// SetLogger set the log for prophet
func SetLogger(l Logger) {
	log = l
	log.Infof("prophet: logger set")
}
