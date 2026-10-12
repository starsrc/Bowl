package utils

import (
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
)

var mu sync.Mutex
var loggers = make(map[string]*LogHandle)
var framePlaceHolder = runtime.Frame{Function: "???", File: "???", Line: 0}

// LogHandle 包装 logrus 日志句柄
type LogHandle struct {
	logrus.Logger

	name     string
	logid    string
	pid      int
	lvl      *logrus.Level
	colorful bool
}

// Format 格式化单条日志输出
func (l *LogHandle) Format(e *logrus.Entry) ([]byte, error) {
	lvl := e.Level
	if l.lvl != nil {
		lvl = *l.lvl
	}
	lvlStr := strings.ToUpper(lvl.String())
	if l.colorful {
		var color int
		switch lvl {
		case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
			color = 31 // 红色
		case logrus.WarnLevel:
			color = 33 // 黄色
		case logrus.InfoLevel:
			color = 34 // 蓝色
		default:
			color = 35 // 品红
		}
		lvlStr = fmt.Sprintf("\033[1;%dm%s\033[0m", color, lvlStr)
	}
	const timeFormat = "2006/01/02 15:04:05.000000"
	caller := e.Caller
	if caller == nil {
		caller = &framePlaceHolder
	}
	str := fmt.Sprintf("%s%v %s[%d] <%v>: %v [%s@%s:%d]",
		l.logid,
		e.Time.Format(timeFormat),
		l.name,
		l.pid,
		lvlStr,
		strings.TrimRight(e.Message, "\n"),
		MethodName(caller.Function),
		path.Base(caller.File),
		caller.Line)

	if len(e.Data) != 0 {
		str += " " + fmt.Sprint(e.Data)
	}
	if !strings.HasSuffix(str, "\n") {
		str += "\n"
	}
	return []byte(str), nil
}

// MethodName 提取更易读的函数调用名称
func MethodName(fullFuncName string) string {
	firstSlash := strings.Index(fullFuncName, "/")
	if firstSlash != -1 && firstSlash < len(fullFuncName)-1 {
		fullFuncName = fullFuncName[firstSlash+1:]
	}
	lastDot := strings.LastIndex(fullFuncName, ".")
	if lastDot == -1 || lastDot == len(fullFuncName)-1 {
		return fullFuncName
	}
	method := fullFuncName[lastDot+1:]
	if strings.HasPrefix(method, "func") && len(method) > 4 && method[4] >= '0' && method[4] <= '9' {
		candidate := MethodName(fullFuncName[:lastDot])
		if candidate != "" {
			method = candidate
		}
	}
	return method
}

// Log 兼容 aws.Logger 接口
func (l *LogHandle) Log(args ...interface{}) {
	l.Debugln(args...)
}

func newLogger(name string) *LogHandle {
	l := &LogHandle{
		Logger:   *logrus.New(),
		name:     name,
		pid:      os.Getpid(),
		colorful: SupportANSIColor(os.Stderr.Fd()),
	}
	l.Formatter = l
	l.SetReportCaller(true)
	return l
}

// GetLogger 获取或创建命名日志记录器
func GetLogger(name string) *LogHandle {
	mu.Lock()
	defer mu.Unlock()

	if logger, ok := loggers[name]; ok {
		return logger
	}
	logger := newLogger(name)
	loggers[name] = logger
	return logger
}

// SetLogLevel 全局调整所有已创建日志句柄的级别
func SetLogLevel(lvl logrus.Level) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.Level = lvl
	}
}

// DisableLogColor 关闭终端彩色高亮
func DisableLogColor() {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.colorful = false
	}
}

// SetOutput 设置日志输出重定向目标
func SetOutput(w io.Writer) {
	mu.Lock()
	defer mu.Unlock()
	for _, logger := range loggers {
		logger.SetOutput(w)
	}
}
