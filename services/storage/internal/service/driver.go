package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	connect "connectrpc.com/connect/v2"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// HandleContext represents opaque driver-specific handle state.
type HandleContext = any

// FileStorageDriver defines the internal storage driver interface for file I/O operations.
type FileStorageDriver interface {
	// Open opens an existing file or creates a driver handle.
	Open(ctx context.Context, diskID string, ino uint64, openMode bowlv1.OpenMode, flags uint32) (HandleContext, error)

	// Read reads file data at a given offset and size.
	Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error)

	// Write writes file data at a given offset.
	Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error)

	// Flush flushes dirty data for an open handle.
	Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error

	// Fsync synchronizes file data to persistent storage.
	Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error

	// Truncate truncates or extends a file to the specified size.
	Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error

	// Fallocate preallocates space or punches a hole in the file.
	Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error

	// CopyFileRange copies a range of data between files or within the same file.
	CopyFileRange(ctx context.Context, srcHandle HandleContext, srcDiskID string, srcIno uint64, srcOffset uint64, dstHandle HandleContext, dstDiskID string, dstIno uint64, dstOffset uint64, length uint64) (uint64, error)

	// Lseek seeks for data or holes within the file.
	Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error)

	// Close closes the driver handle and frees resources.
	Close(ctx context.Context, handle HandleContext) error

	// DeleteInode removes physical storage resources for an unlinked/deleted inode.
	DeleteInode(ctx context.Context, diskID string, ino uint64) error
}

// legacyFileHandle represents open file context for LegacyFileDriver.
type legacyFileHandle struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
	diskID   string
	ino      uint64
	openMode bowlv1.OpenMode
	flags    uint32
}

// LegacyFileDriver implements FileStorageDriver using monolithic local file storage (file_<ino>.bin).
type LegacyFileDriver struct {
	dataDir string
}

// NewLegacyFileDriver constructs a LegacyFileDriver with the specified data root.
func NewLegacyFileDriver(dataDir string) *LegacyFileDriver {
	return &LegacyFileDriver{dataDir: dataDir}
}

// FilePath computes the local file path for storing chunk data of a specific Inode.
// Data root path: <DATA_DIR>/<disk_id>/file_<ino>.bin (ADR 0009).
func (d *LegacyFileDriver) FilePath(diskID string, ino uint64) string {
	base := d.dataDir
	if base == "" {
		base = filepath.Join(os.TempDir(), "bowl_data")
	}
	return filepath.Join(base, diskID, fmt.Sprintf("file_%d.bin", ino))
}

// Open opens an existing file or creates a new file handle.
func (d *LegacyFileDriver) Open(ctx context.Context, diskID string, ino uint64, openMode bowlv1.OpenMode, flags uint32) (HandleContext, error) {
	filePath := d.FilePath(diskID, ino)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	isWriteRequested := openMode == bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY || openMode == bowlv1.OpenMode_OPEN_MODE_READ_WRITE
	var file *os.File
	var err error
	if isWriteRequested {
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
		}
	} else {
		file, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
		}
	}

	return &legacyFileHandle{
		file:     file,
		filePath: filePath,
		diskID:   diskID,
		ino:      ino,
		openMode: openMode,
		flags:    flags,
	}, nil
}

// Read reads file data at a given offset and size.
func (d *LegacyFileDriver) Read(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, size uint32) ([]byte, bool, error) {
	if size == 0 {
		return []byte{}, false, nil
	}

	filePath := d.FilePath(diskID, ino)
	var osFile *os.File
	var lh *legacyFileHandle

	if handle != nil {
		if h, ok := handle.(*legacyFileHandle); ok {
			lh = h
			lh.mu.Lock()
			defer lh.mu.Unlock()
			filePath = lh.filePath
			osFile = lh.file
		}
	}

	// Check if parent disk directory exists
	if _, err := os.Stat(filepath.Dir(filePath)); os.IsNotExist(err) {
		if lh != nil && lh.file != nil {
			_ = lh.file.Close()
			lh.file = nil
		}
		return nil, false, connect.Errorf(connect.CodeNotFound, "disk storage directory does not exist: %s", diskID)
	}

	if osFile != nil {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			if lh != nil {
				_ = lh.file.Close()
				lh.file = nil
			}
			return nil, false, connect.Errorf(connect.CodeNotFound, "chunk file was deleted: %s", filePath)
		}
	} else {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return []byte{}, true, nil
		}
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
		if err != nil {
			return nil, false, connect.Errorf(connect.CodeInternal, "failed to open file for read: %s", err)
		}
		if lh != nil {
			lh.file = f
			osFile = f
		} else {
			defer f.Close()
			osFile = f
		}
	}

	fi, err := osFile.Stat()
	if err != nil {
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
	}
	fileSize := uint64(fi.Size())
	if offset >= fileSize {
		return []byte{}, true, nil
	}

	available := fileSize - offset
	toRead := uint64(size)
	eof := false
	if toRead >= available {
		toRead = available
		eof = true
	}

	buf := make([]byte, toRead)
	n, err := osFile.ReadAt(buf, int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, false, connect.Errorf(connect.CodeInternal, "failed to read file: %s", err)
	}

	return buf[:n], eof || (errors.Is(err, io.EOF) && uint64(n) == available), nil
}

// Write writes data to local file at offset.
func (d *LegacyFileDriver) Write(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, data []byte) (uint32, error) {
	filePath := d.FilePath(diskID, ino)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	var osFile *os.File
	var lh *legacyFileHandle

	if handle != nil {
		if h, ok := handle.(*legacyFileHandle); ok {
			lh = h
			lh.mu.Lock()
			defer lh.mu.Unlock()
			filePath = lh.filePath
			if lh.file == nil {
				f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "failed to open file for write: %s", err)
				}
				lh.file = f
			}
			osFile = lh.file
		}
	}

	if osFile == nil {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to open file for write: %s", err)
		}
		defer f.Close()
		osFile = f
	}

	var written uint32
	if len(data) > 0 {
		n, err := osFile.WriteAt(data, int64(offset))
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to write file: %s", err)
		}
		written = uint32(n)
	}
	return written, nil
}

// Flush flushes dirty data to disk.
func (d *LegacyFileDriver) Flush(ctx context.Context, handle HandleContext, diskID string, ino uint64) error {
	if handle == nil {
		return nil
	}
	if lh, ok := handle.(*legacyFileHandle); ok {
		lh.mu.Lock()
		defer lh.mu.Unlock()
		if lh.file != nil {
			if err := lh.file.Sync(); err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to sync file: %s", err)
			}
		}
	}
	return nil
}

// Fsync synchronizes file data to disk.
func (d *LegacyFileDriver) Fsync(ctx context.Context, handle HandleContext, diskID string, ino uint64, datasync bool) error {
	if handle != nil {
		if lh, ok := handle.(*legacyFileHandle); ok {
			lh.mu.Lock()
			defer lh.mu.Unlock()
			if lh.file != nil {
				if err := lh.file.Sync(); err != nil {
					return connect.Errorf(connect.CodeInternal, "failed to sync file: %s", err)
				}
				return nil
			}
		}
	}

	filePath := d.FilePath(diskID, ino)
	if f, err := os.OpenFile(filePath, os.O_RDWR, 0o644); err == nil {
		_ = f.Sync()
		_ = f.Close()
	}
	return nil
}

// Truncate truncates or extends file to specified size.
func (d *LegacyFileDriver) Truncate(ctx context.Context, diskID string, ino uint64, size uint64) error {
	filePath := d.FilePath(diskID, ino)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		if size == 0 {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
		}
	}
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to open file for truncate: %s", err)
	}
	defer f.Close()
	if err := f.Truncate(int64(size)); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to truncate file: %s", err)
	}
	return nil
}

// Fallocate handles space preallocation, punch hole, and zero-range operations.
func (d *LegacyFileDriver) Fallocate(ctx context.Context, handle HandleContext, diskID string, ino uint64, mode uint32, offset uint64, length uint64) error {
	filePath := d.FilePath(diskID, ino)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	var osFile *os.File
	var lh *legacyFileHandle

	if handle != nil {
		if h, ok := handle.(*legacyFileHandle); ok {
			lh = h
			lh.mu.Lock()
			defer lh.mu.Unlock()
			filePath = lh.filePath
			if lh.file == nil {
				f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
				}
				lh.file = f
			}
			osFile = lh.file
		}
	}

	if osFile == nil {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
		}
		defer f.Close()
		osFile = f
	}

	fi, err := osFile.Stat()
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
	}
	currentSize := uint64(fi.Size())

	// Punch hole (mode & 2 or FALLOC_FL_PUNCH_HOLE)
	if mode&uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE) != 0 || mode == 2 {
		if offset < currentSize {
			writeLen := length
			if offset+writeLen > currentSize {
				writeLen = currentSize - offset
			}
			zeros := make([]byte, writeLen)
			if _, err := osFile.WriteAt(zeros, int64(offset)); err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to write zeros for punch hole: %s", err)
			}
		}
	} else {
		// Mode 0 (preallocate) or other modes extending size
		keepSize := (mode & uint32(bowlv1.FallocateMode_FALLOCATE_MODE_KEEP_SIZE)) != 0
		if !keepSize && offset+length > currentSize {
			newSize := offset + length
			if err := osFile.Truncate(int64(newSize)); err != nil {
				return connect.Errorf(connect.CodeInternal, "failed to extend file: %s", err)
			}
		}
	}
	return nil
}

// CopyFileRange copies a range of data between files or within the same file.
func (d *LegacyFileDriver) CopyFileRange(
	ctx context.Context,
	srcHandle HandleContext,
	srcDiskID string,
	srcIno uint64,
	srcOffset uint64,
	dstHandle HandleContext,
	dstDiskID string,
	dstIno uint64,
	dstOffset uint64,
	length uint64,
) (uint64, error) {
	if length == 0 {
		return 0, nil
	}

	var srcFile *os.File
	var dstFile *os.File

	if srcHandle != nil {
		if h, ok := srcHandle.(*legacyFileHandle); ok {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.file == nil {
				f, err := os.OpenFile(h.filePath, os.O_RDONLY, 0o644)
				if err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "failed to open src file: %s", err)
				}
				h.file = f
			}
			srcFile = h.file
		}
	}
	if srcFile == nil {
		srcPath := d.FilePath(srcDiskID, srcIno)
		f, err := os.OpenFile(srcPath, os.O_RDONLY, 0o644)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to open src file: %s", err)
		}
		defer f.Close()
		srcFile = f
	}

	if dstHandle != nil {
		if h, ok := dstHandle.(*legacyFileHandle); ok {
			h.mu.Lock()
			defer h.mu.Unlock()
			if h.file == nil {
				if err := os.MkdirAll(filepath.Dir(h.filePath), 0o755); err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
				}
				f, err := os.OpenFile(h.filePath, os.O_CREATE|os.O_RDWR, 0o644)
				if err != nil {
					return 0, connect.Errorf(connect.CodeInternal, "failed to open dst file: %s", err)
				}
				h.file = f
			}
			dstFile = h.file
		}
	}
	if dstFile == nil {
		dstPath := d.FilePath(dstDiskID, dstIno)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
		}
		f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to open dst file: %s", err)
		}
		defer f.Close()
		dstFile = f
	}

	srcFi, err := srcFile.Stat()
	if err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to stat src file: %s", err)
	}
	srcSize := uint64(srcFi.Size())
	if srcOffset >= srcSize {
		return 0, nil
	}

	available := srcSize - srcOffset
	toCopy := length
	if toCopy > available {
		toCopy = available
	}

	buf := make([]byte, toCopy)
	n, err := srcFile.ReadAt(buf, int64(srcOffset))
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, connect.Errorf(connect.CodeInternal, "failed to read src file: %s", err)
	}
	if n == 0 {
		return 0, nil
	}
	buf = buf[:n]

	written, err := dstFile.WriteAt(buf, int64(dstOffset))
	if err != nil {
		return 0, connect.Errorf(connect.CodeInternal, "failed to write dst file: %s", err)
	}

	return uint64(written), nil
}

// Lseek finds data or hole intervals within a file (FUSE_LSEEK).
func (d *LegacyFileDriver) Lseek(ctx context.Context, handle HandleContext, diskID string, ino uint64, offset uint64, whence uint32) (uint64, error) {
	filePath := d.FilePath(diskID, ino)
	var osFile *os.File
	var lh *legacyFileHandle

	if handle != nil {
		if h, ok := handle.(*legacyFileHandle); ok {
			lh = h
			lh.mu.Lock()
			defer lh.mu.Unlock()
			filePath = lh.filePath
			if lh.file == nil {
				if _, err := os.Stat(filePath); !os.IsNotExist(err) {
					f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
					if err != nil {
						return 0, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
					}
					lh.file = f
				}
			}
			osFile = lh.file
		}
	}

	if osFile == nil {
		if _, err := os.Stat(filePath); !os.IsNotExist(err) {
			f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
			if err != nil {
				return 0, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
			}
			defer f.Close()
			osFile = f
		}
	}

	var fileSize uint64
	if osFile != nil {
		fi, err := osFile.Stat()
		if err != nil {
			return 0, connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
		}
		fileSize = uint64(fi.Size())
	}

	switch whence {
	case 0, 1: // SEEK_SET (0), SEEK_CUR (1)
		return offset, nil
	case 2: // SEEK_END (2)
		return fileSize + offset, nil
	case uint32(bowlv1.LseekWhence_LSEEK_WHENCE_DATA): // 3
		if offset >= fileSize {
			return 0, connect.Errorf(connect.CodeOutOfRange, "offset %d is at or beyond EOF %d", offset, fileSize)
		}
		if osFile == nil {
			return 0, connect.Errorf(connect.CodeOutOfRange, "no data found at or after offset %d", offset)
		}
		foundOffset, found, err := scanFileForByte(osFile, offset, fileSize, false)
		if err != nil {
			return 0, err
		}
		if !found {
			return 0, connect.Errorf(connect.CodeOutOfRange, "no data found at or after offset %d", offset)
		}
		return foundOffset, nil
	case uint32(bowlv1.LseekWhence_LSEEK_WHENCE_HOLE): // 4
		if offset > fileSize {
			return 0, connect.Errorf(connect.CodeOutOfRange, "offset %d is beyond EOF %d", offset, fileSize)
		}
		if offset == fileSize || osFile == nil {
			return fileSize, nil
		}
		foundOffset, found, err := scanFileForByte(osFile, offset, fileSize, true)
		if err != nil {
			return 0, err
		}
		if !found {
			return fileSize, nil
		}
		return foundOffset, nil
	default:
		return 0, connect.Errorf(connect.CodeInvalidArgument, "unsupported lseek whence: %d", whence)
	}
}

// scanFileForByte scans a range of file for the first zero or non-zero byte.
func scanFileForByte(file *os.File, start, end uint64, lookForZero bool) (uint64, bool, error) {
	const bufSize = 32768
	buf := make([]byte, bufSize)
	curr := start
	for curr < end {
		toRead := int64(bufSize)
		if curr+uint64(toRead) > end {
			toRead = int64(end - curr)
		}
		n, err := file.ReadAt(buf[:toRead], int64(curr))
		if err != nil && !errors.Is(err, io.EOF) {
			return 0, false, connect.Errorf(connect.CodeInternal, "failed to scan file: %s", err)
		}
		if n == 0 {
			break
		}
		for i := 0; i < n; i++ {
			b := buf[i]
			if (lookForZero && b == 0) || (!lookForZero && b != 0) {
				return curr + uint64(i), true, nil
			}
		}
		curr += uint64(n)
	}
	return 0, false, nil
}

// Close closes the underlying OS file handle.
func (d *LegacyFileDriver) Close(ctx context.Context, handle HandleContext) error {
	if handle == nil {
		return nil
	}
	if lh, ok := handle.(*legacyFileHandle); ok {
		lh.mu.Lock()
		defer lh.mu.Unlock()
		if lh.file != nil {
			err := lh.file.Close()
			lh.file = nil
			return err
		}
	}
	return nil
}

// DeleteInode removes physical storage resources for an unlinked/deleted inode.
func (d *LegacyFileDriver) DeleteInode(ctx context.Context, diskID string, ino uint64) error {
	filePath := d.FilePath(diskID, ino)
	_ = os.Remove(filePath)
	return nil
}
