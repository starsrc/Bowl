//go:build !linux

package store

import "os"

func syncFileData(file *os.File) error {
	return file.Sync()
}

func startAsyncWriteback(_ *os.File, _, _ int64) error {
	return nil
}
