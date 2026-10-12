package utils

import (
	"errors"
	"syscall"
)

var (
	// ErrNotSUP 不支持的操作错误
	ErrNotSUP = errors.New("not supported")
	// ErrFuncTimeout 函数执行超时错误
	ErrFuncTimeout = errors.New("function timeout")
	// ErrSkipped 任务已跳过
	ErrSkipped = errors.New("skipped")
	// ErrExtlink 外部链接错误号
	ErrExtlink = syscall.Errno(1000)
)
