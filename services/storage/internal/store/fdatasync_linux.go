//go:build linux

package store

import (
	"os"
	"syscall"
)

const syncFileRangeWrite = 2

func syncFileData(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}

func startAsyncWriteback(file *os.File, offset, nbytes int64) error {
	return syscall.SyncFileRange(int(file.Fd()), offset, nbytes, syncFileRangeWrite)
}
