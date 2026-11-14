//go:build linux

package fusefs

import (
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// CheckTmpfs 检查目标路径所在的文件系统是否为 Linux tmpfs（TMPFS_MAGIC = 0x01021994）。
func CheckTmpfs(path string) (bool, error) {
	var st unix.Statfs_t
	for {
		err := unix.Statfs(path, &st)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return false, err
		}
		return uint64(st.Type) == uint64(unix.TMPFS_MAGIC), nil
	}
}

func openHostTmpfile(dir string) (*os.File, error) {
	if dir != "" {
		fd, err := unix.Open(dir, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
		if err == nil {
			return os.NewFile(uintptr(fd), "bowl-tmpfile"), nil
		}
		if !errors.Is(err, unix.EOPNOTSUPP) &&
			!errors.Is(err, unix.ENOTSUP) &&
			!errors.Is(err, unix.EISDIR) &&
			!errors.Is(err, unix.EINVAL) &&
			!errors.Is(err, unix.ENOSYS) {
			return nil, err
		}
	}
	return createAndUnlinkTempFile(dir)
}

func fallocateHostFile(f *os.File, mode uint32, offset, length int64, curSize uint64) error {
	fd := int(f.Fd())
	var err error
	for {
		err = unix.Fallocate(fd, mode, offset, length)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	runtime.KeepAlive(f)
	if err == nil {
		return nil
	}
	if errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) || errors.Is(err, unix.ENOSYS) {
		return fallocateHostFileFallback(f, mode, offset, length, curSize)
	}
	return err
}

func findHostDataExtents(f *os.File, size uint64) ([]stageExtent, error) {
	if size == 0 {
		return nil, nil
	}
	fd := int(f.Fd())
	defer runtime.KeepAlive(f)

	var extents []stageExtent
	var pos uint64
	for pos < size {
		dataOff, err := unix.Seek(fd, int64(pos), unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP) {
				return scanNonZeroExtents(f, pos, size)
			}
			return nil, err
		}
		if dataOff < 0 || uint64(dataOff) >= size {
			break
		}
		holeOff, err := unix.Seek(fd, dataOff, unix.SEEK_HOLE)
		if err != nil {
			switch {
			case errors.Is(err, unix.ENXIO):
				holeOff = int64(size)
			case errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOTSUP):
				return scanNonZeroExtents(f, uint64(dataOff), size)
			default:
				return nil, err
			}
		}
		end := min(uint64(holeOff), size)
		start := uint64(dataOff)
		if end > start {
			extents = appendSplitExtents(extents, start, end)
		}
		pos = end
	}
	return extents, nil
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
