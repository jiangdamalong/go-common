package log

import (
	"fmt"
	cpplog "github.com/jiangdamalong/common"
	"time"
)

type CommonLogger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
	Panicf(format string, args ...interface{})
}

var logger CommonLogger = &stdLogIf{}

func SetLogger(l CommonLogger) {
	logger = l
}

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

func Warnf(format string, args ...interface{}) {
	logger.Warnf(format, args...)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

func Fatalf(format string, args ...interface{}) {
	logger.Fatalf(format, args...)
}

func Panicf(format string, args ...interface{}) {
	logger.Panicf(format, args...)
}

type stdLogIf struct {
}

func (l *stdLogIf) Debugf(format string, args ...interface{}) {
	fmt.Printf("[%+v][debug]", time.Now().Format("2006-01-02 15:04:05.999"))
	fmt.Printf(format, args...)
	fmt.Println()
}

func (l *stdLogIf) Infof(format string, args ...interface{}) {
	fmt.Printf("[%+v][info]", time.Now().Format("2006-01-02 15:04:05.999"))
	fmt.Printf(format, args...)
	fmt.Println()
}

func (l *stdLogIf) Warnf(format string, args ...interface{}) {
	fmt.Printf("[%+v][warn]", time.Now().Format("2006-01-02 15:04:05.999"))
	fmt.Printf(format, args...)
	fmt.Println()
}

func (l *stdLogIf) Errorf(format string, args ...interface{}) {
	fmt.Printf("[%+v][error]", time.Now().Format("2006-01-02 15:04:05.999"))
	fmt.Printf(format, args...)
	fmt.Println()
}

func (l *stdLogIf) Fatalf(format string, args ...interface{}) {
	fmt.Printf("[%+v][fatal]", time.Now().Format("2006-01-02 15:04:05.999"))
	fmt.Printf(format, args...)
	fmt.Println()
}

func (l *stdLogIf) Panicf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

type logIf struct {
}

func (l *logIf) Debugf(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.DebugLevel, 2, format, args...)
}

func (l *logIf) Infof(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.InfoLevel, 2, format, args...)
}

func (l *logIf) Warnf(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.WarnLevel, 2, format, args...)
}

func (l *logIf) Errorf(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.ErrorLevel, 2, format, args...)
}

func (l *logIf) Fatalf(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.FatalLevel, 2, format, args...)
}

func (l *logIf) Panicf(format string, args ...interface{}) {
	cpplog.StLogger.WriteLogf(cpplog.PanicLevel, 2, format, args...)
}

func Start(dir, prefix string) {
	cpplog.Start(dir, prefix)
	logger = new(logIf)
}

func SetLevel(level int) {
	cpplog.SetLevel(level)
}

func GetLogger() CommonLogger {
	return logger
}
