//go:build linux

package engine

import (
	"os"
	"syscall"
)

func syncFileData(file *os.File) error {
	return syscall.Fdatasync(int(file.Fd()))
}
