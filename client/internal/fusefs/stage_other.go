//go:build !linux

package fusefs

import (
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

func CheckTmpfs(_ string) (bool, error) {
	return false, nil
}

func openHostTmpfile(dir string) (*os.File, error) {
	return createAndUnlinkTempFile(dir)
}

func fallocateHostFile(f *os.File, mode uint32, offset, length int64, curSize uint64) error {
	return fallocateHostFileFallback(f, mode, offset, length, curSize)
}

func findHostDataExtents(f *os.File, size uint64) ([]tmpExtent, error) {
	return scanNonZeroExtents(f, 0, size)
}

func hostFileAllocatedBytes(f *os.File) uint64 {
	if f == nil {
		return 0
	}
	var st unix.Stat_t
	err := unix.Fstat(int(f.Fd()), &st)
	runtime.KeepAlive(f)
	if err != nil || st.Blocks <= 0 {
		return 0
	}
	return uint64(st.Blocks) * 512
}
