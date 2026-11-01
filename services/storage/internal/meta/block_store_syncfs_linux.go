//go:build linux

package meta

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func syncFSDataDir(dataDir string) (bool, error) {
	f, err := os.Open(dataDir)
	if err != nil {
		return true, fmt.Errorf("%w: failed to open data directory %s for syncfs: %v", ErrIO, dataDir, err)
	}
	syncErr := unix.Syncfs(int(f.Fd()))
	closeErr := f.Close()
	if syncErr != nil {
		return true, fmt.Errorf("%w: failed to syncfs data directory %s: %v", ErrIO, dataDir, syncErr)
	}
	if closeErr != nil {
		return true, fmt.Errorf("%w: failed to close data directory %s after syncfs: %v", ErrIO, dataDir, closeErr)
	}
	return true, nil
}
