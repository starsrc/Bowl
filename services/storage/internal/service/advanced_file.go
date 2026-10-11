package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	connect "connectrpc.com/connect/v2"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// Flush flushes dirty data of an open file handle to persistent storage (FUSE_FLUSH).
func (s *StorageService) Flush(ctx context.Context, req *bowlv1.FlushRequest) (*bowlv1.FlushResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	fh := req.GetFh()
	if fh == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh must be greater than 0")
	}

	_, _, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	val, ok := s.openHandles.Load(fh)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
	}
	handle := val.(*OpenFileHandle)
	if handle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
	}
	if req.GetIno() != 0 && req.GetIno() != handle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", req.GetIno(), handle.Handle.GetIno())
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()

	if handle.file != nil {
		if err := handle.file.Sync(); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to sync file: %s", err)
		}
	}

	return &bowlv1.FlushResponse{}, nil
}

// Fsync synchronizes file data and metadata to persistent storage (FUSE_FSYNC).
func (s *StorageService) Fsync(ctx context.Context, req *bowlv1.FsyncRequest) (*bowlv1.FsyncResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	fh := req.GetFh()
	targetIno := req.GetIno()
	var handle *OpenFileHandle

	if fh != 0 {
		val, ok := s.openHandles.Load(fh)
		if !ok {
			return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
		}
		handle = val.(*OpenFileHandle)
		if handle.SessionID != sessionID {
			return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
		}
		if targetIno != 0 && targetIno != handle.Handle.GetIno() {
			return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", targetIno, handle.Handle.GetIno())
		}
		targetIno = handle.Handle.GetIno()
	} else if targetIno == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh or ino is required")
	}

	if handle != nil {
		handle.mu.Lock()
		defer handle.mu.Unlock()
		if handle.file != nil {
			if err := handle.file.Sync(); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to sync file: %s", err)
			}
		}
	} else {
		filePath := s.filePath(sess.DiskID, targetIno)
		if f, err := os.OpenFile(filePath, os.O_RDWR, 0o644); err == nil {
			_ = f.Sync()
			_ = f.Close()
		}
	}

	if s.db != nil {
		now := time.Now()
		updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
		if _, err := s.db.Exec(ctx, updateSQL, now, int64(targetIno)); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to update inode mtime: %s", err)
		}
	}

	return &bowlv1.FsyncResponse{}, nil
}

// Fallocate handles space preallocation, punch hole, and zero-range operations (FUSE_FALLOCATE).
func (s *StorageService) Fallocate(ctx context.Context, req *bowlv1.FallocateRequest) (*bowlv1.FallocateResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	sess, schemaName, err := s.checkWriteLease(sessionID)
	if err != nil {
		return nil, err
	}

	length := req.GetLength()
	if length == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "length must be greater than 0")
	}
	offset := req.GetOffset()

	fh := req.GetFh()
	targetIno := req.GetIno()
	var handle *OpenFileHandle

	if fh != 0 {
		val, ok := s.openHandles.Load(fh)
		if !ok {
			return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
		}
		handle = val.(*OpenFileHandle)
		if handle.SessionID != sessionID {
			return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
		}
		if handle.Handle.GetOpenMode() == bowlv1.OpenMode_OPEN_MODE_READ_ONLY {
			return nil, connect.NewError(connect.CodePermissionDenied, "file handle is open for read-only")
		}
		if targetIno != 0 && targetIno != handle.Handle.GetIno() {
			return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", targetIno, handle.Handle.GetIno())
		}
		targetIno = handle.Handle.GetIno()
	} else if targetIno == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh or ino is required")
	}

	filePath := s.filePath(sess.DiskID, targetIno)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	var osFile *os.File
	if handle != nil {
		handle.mu.Lock()
		defer handle.mu.Unlock()
		if handle.file == nil {
			f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
			if err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
			}
			handle.file = f
		}
		osFile = handle.file
	} else {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
		}
		defer f.Close()
		osFile = f
	}

	fi, err := osFile.Stat()
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
	}
	currentSize := uint64(fi.Size())
	if s.db != nil {
		if curInode, err := s.getInode(ctx, schemaName, targetIno); err == nil {
			if curInode.GetSize() > currentSize {
				currentSize = curInode.GetSize()
			}
		}
	}

	mode := req.GetMode()
	now := time.Now()

	// Punch hole (mode & 2 or FALLOC_FL_PUNCH_HOLE)
	if mode&uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE) != 0 || mode == 2 {
		if offset < currentSize {
			writeLen := length
			if offset+writeLen > currentSize {
				writeLen = currentSize - offset
			}
			zeros := make([]byte, writeLen)
			if _, err := osFile.WriteAt(zeros, int64(offset)); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to write zeros for punch hole: %s", err)
			}
		}
		if s.db != nil {
			updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
			if _, err := s.db.Exec(ctx, updateSQL, now, int64(targetIno)); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to update inode metadata: %s", err)
			}
		}
	} else {
		// Mode 0 (preallocate) or other modes extending size
		keepSize := (mode & uint32(bowlv1.FallocateMode_FALLOCATE_MODE_KEEP_SIZE)) != 0
		newSize := currentSize
		if !keepSize && offset+length > currentSize {
			newSize = offset + length
			if err := osFile.Truncate(int64(newSize)); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to extend file: %s", err)
			}
		}
		if s.db != nil {
			updateSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
			if _, err := s.db.Exec(ctx, updateSQL, int64(newSize), now, int64(targetIno)); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to update inode metadata: %s", err)
			}
		}
	}

	return &bowlv1.FallocateResponse{}, nil
}

// CopyFileRange copies a range of data between files or within the same file (FUSE_COPY_FILE_RANGE).
func (s *StorageService) CopyFileRange(ctx context.Context, req *bowlv1.CopyFileRangeRequest) (*bowlv1.CopyFileRangeResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	_, schemaName, err := s.checkWriteLease(sessionID)
	if err != nil {
		return nil, err
	}

	length := req.GetLength()
	if length == 0 {
		return &bowlv1.CopyFileRangeResponse{Copied: 0}, nil
	}

	srcFH := req.GetSrcFh()
	dstFH := req.GetDstFh()
	if srcFH == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "src_fh must be greater than 0")
	}
	if dstFH == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "dst_fh must be greater than 0")
	}

	valIn, ok := s.openHandles.Load(srcFH)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "src file handle %d not found", srcFH)
	}
	srcHandle := valIn.(*OpenFileHandle)
	if srcHandle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "src file handle %d does not belong to session %s", srcFH, sessionID)
	}
	if srcHandle.Handle.GetOpenMode() == bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY {
		return nil, connect.NewError(connect.CodePermissionDenied, "src file handle is open for write-only")
	}
	if req.GetSrcIno() != 0 && req.GetSrcIno() != srcHandle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "src_ino %d does not match src_fh ino %d", req.GetSrcIno(), srcHandle.Handle.GetIno())
	}
	srcIno := srcHandle.Handle.GetIno()

	valOut, ok := s.openHandles.Load(dstFH)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "dst file handle %d not found", dstFH)
	}
	dstHandle := valOut.(*OpenFileHandle)
	if dstHandle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "dst file handle %d does not belong to session %s", dstFH, sessionID)
	}
	if dstHandle.Handle.GetOpenMode() == bowlv1.OpenMode_OPEN_MODE_READ_ONLY {
		return nil, connect.NewError(connect.CodePermissionDenied, "dst file handle is open for read-only")
	}
	if req.GetDstIno() != 0 && req.GetDstIno() != dstHandle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "dst_ino %d does not match dst_fh ino %d", req.GetDstIno(), dstHandle.Handle.GetIno())
	}
	dstIno := dstHandle.Handle.GetIno()

	// Prevent deadlocks by ordering mutex acquisition
	if srcFH == dstFH {
		srcHandle.mu.Lock()
		defer srcHandle.mu.Unlock()
	} else if srcFH < dstFH {
		srcHandle.mu.Lock()
		dstHandle.mu.Lock()
		defer srcHandle.mu.Unlock()
		defer dstHandle.mu.Unlock()
	} else {
		dstHandle.mu.Lock()
		srcHandle.mu.Lock()
		defer dstHandle.mu.Unlock()
		defer srcHandle.mu.Unlock()
	}

	if srcHandle.file == nil {
		srcPath := s.filePath(srcHandle.DiskID, srcIno)
		f, err := os.OpenFile(srcPath, os.O_RDONLY, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open src file: %s", err)
		}
		srcHandle.file = f
	}

	if dstHandle.file == nil {
		dstPath := s.filePath(dstHandle.DiskID, dstIno)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
		}
		f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open dst file: %s", err)
		}
		dstHandle.file = f
	}

	srcFi, err := srcHandle.file.Stat()
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to stat src file: %s", err)
	}
	srcSize := uint64(srcFi.Size())
	srcOffset := req.GetSrcOffset()
	if srcOffset >= srcSize {
		return &bowlv1.CopyFileRangeResponse{Copied: 0}, nil
	}

	available := srcSize - srcOffset
	toCopy := length
	if toCopy > available {
		toCopy = available
	}

	buf := make([]byte, toCopy)
	n, err := srcHandle.file.ReadAt(buf, int64(srcOffset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to read src file: %s", err)
	}
	if n == 0 {
		return &bowlv1.CopyFileRangeResponse{Copied: 0}, nil
	}
	buf = buf[:n]

	dstOffset := req.GetDstOffset()
	written, err := dstHandle.file.WriteAt(buf, int64(dstOffset))
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to write dst file: %s", err)
	}

	if s.db != nil {
		now := time.Now()
		newEnd := int64(dstOffset) + int64(written)
		updateSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
		if _, err := s.db.Exec(ctx, updateSQL, newEnd, now, int64(dstIno)); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to update dst inode metadata: %s", err)
		}
	}

	return &bowlv1.CopyFileRangeResponse{
		Copied: uint64(written),
	}, nil
}

// Lseek finds data or hole intervals within a file (FUSE_LSEEK).
func (s *StorageService) Lseek(ctx context.Context, req *bowlv1.LseekRequest) (*bowlv1.LseekResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}

	_, _, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	fh := req.GetFh()
	if fh == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh must be greater than 0")
	}

	val, ok := s.openHandles.Load(fh)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
	}
	handle := val.(*OpenFileHandle)
	if handle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
	}
	if req.GetIno() != 0 && req.GetIno() != handle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", req.GetIno(), handle.Handle.GetIno())
	}
	targetIno := handle.Handle.GetIno()

	handle.mu.Lock()
	defer handle.mu.Unlock()

	filePath := s.filePath(handle.DiskID, targetIno)
	if handle.file == nil {
		if _, err := os.Stat(filePath); !os.IsNotExist(err) {
			f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
			if err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to open file: %s", err)
			}
			handle.file = f
		}
	}

	var fileSize uint64
	if handle.file != nil {
		fi, err := handle.file.Stat()
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
		}
		fileSize = uint64(fi.Size())
	}

	offset := req.GetOffset()
	whence := req.GetWhence()

	switch whence {
	case 0, 1: // SEEK_SET (0), SEEK_CUR (1)
		return &bowlv1.LseekResponse{Offset: offset}, nil
	case 2: // SEEK_END (2)
		return &bowlv1.LseekResponse{Offset: fileSize + offset}, nil
	case bowlv1.LseekWhence_LSEEK_WHENCE_DATA: // 3
		if offset >= fileSize {
			return nil, connect.Errorf(connect.CodeOutOfRange, "offset %d is at or beyond EOF %d", offset, fileSize)
		}
		if handle.file == nil {
			return nil, connect.Errorf(connect.CodeOutOfRange, "no data found at or after offset %d", offset)
		}
		foundOffset, found, err := s.scanFileForByte(handle.file, offset, fileSize, false)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, connect.Errorf(connect.CodeOutOfRange, "no data found at or after offset %d", offset)
		}
		return &bowlv1.LseekResponse{Offset: foundOffset}, nil
	case bowlv1.LseekWhence_LSEEK_WHENCE_HOLE: // 4
		if offset > fileSize {
			return nil, connect.Errorf(connect.CodeOutOfRange, "offset %d is beyond EOF %d", offset, fileSize)
		}
		if offset == fileSize || handle.file == nil {
			return &bowlv1.LseekResponse{Offset: fileSize}, nil
		}
		foundOffset, found, err := s.scanFileForByte(handle.file, offset, fileSize, true)
		if err != nil {
			return nil, err
		}
		if !found {
			return &bowlv1.LseekResponse{Offset: fileSize}, nil
		}
		return &bowlv1.LseekResponse{Offset: foundOffset}, nil
	default:
		return nil, connect.Errorf(connect.CodeInvalidArgument, "unsupported lseek whence: %d", whence)
	}
}

// scanFileForByte scans a range of file for the first zero or non-zero byte.
func (s *StorageService) scanFileForByte(file *os.File, start, end uint64, lookForZero bool) (uint64, bool, error) {
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
