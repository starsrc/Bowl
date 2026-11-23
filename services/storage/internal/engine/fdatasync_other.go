//go:build !linux

package engine

import "os"

func syncFileData(file *os.File) error {
	return file.Sync()
}
