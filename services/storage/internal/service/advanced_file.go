package service

import (
	"context"
	"fmt"
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
	driverHandle := handle.driverHandle
	handle.mu.Unlock()

	if s.driver != nil {
		if err := s.driver.Flush(ctx, driverHandle, handle.DiskID, handle.Handle.GetIno()); err != nil {
			return nil, toConnectError(err)
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

	var driverHandle HandleContext
	if handle != nil {
		handle.mu.Lock()
		driverHandle = handle.driverHandle
		handle.mu.Unlock()
	}

	if s.driver != nil {
		if err := s.driver.Fsync(ctx, driverHandle, sess.DiskID, targetIno, req.GetDatasync()); err != nil {
			return nil, toConnectError(err)
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

	var driverHandle HandleContext
	if handle != nil {
		handle.mu.Lock()
		driverHandle = handle.driverHandle
		handle.mu.Unlock()
	}

	if s.driver != nil {
		if err := s.driver.Fallocate(ctx, driverHandle, sess.DiskID, targetIno, req.GetMode(), offset, length); err != nil {
			return nil, toConnectError(err)
		}
	}

	mode := req.GetMode()
	now := time.Now()

	// Punch hole (mode & 2 or FALLOC_FL_PUNCH_HOLE)
	if mode&uint32(bowlv1.FallocateMode_FALLOCATE_MODE_PUNCH_HOLE) != 0 || mode == 2 {
		if s.db != nil {
			updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
			if _, err := s.db.Exec(ctx, updateSQL, now, int64(targetIno)); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to update inode metadata: %s", err)
			}
		}
	} else {
		// Mode 0 (preallocate) or other modes extending size
		keepSize := (mode & uint32(bowlv1.FallocateMode_FALLOCATE_MODE_KEEP_SIZE)) != 0
		var currentSize uint64
		if s.db != nil {
			if curInode, err := s.getInode(ctx, schemaName, targetIno); err == nil {
				currentSize = curInode.GetSize()
			}
		}
		newSize := currentSize
		if !keepSize && offset+length > currentSize {
			newSize = offset + length
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

	srcHandle.mu.Lock()
	srcDriverHandle := srcHandle.driverHandle
	srcHandle.mu.Unlock()

	dstHandle.mu.Lock()
	dstDriverHandle := dstHandle.driverHandle
	dstHandle.mu.Unlock()

	copied, err := s.driver.CopyFileRange(
		ctx,
		srcDriverHandle,
		srcHandle.DiskID,
		srcIno,
		req.GetSrcOffset(),
		dstDriverHandle,
		dstHandle.DiskID,
		dstIno,
		req.GetDstOffset(),
		length,
	)
	if err != nil {
		return nil, toConnectError(err)
	}

	if s.db != nil && copied > 0 {
		now := time.Now()
		newEnd := int64(req.GetDstOffset()) + int64(copied)
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
		Copied: copied,
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
	driverHandle := handle.driverHandle
	handle.mu.Unlock()

	offset, err := s.driver.Lseek(ctx, driverHandle, handle.DiskID, targetIno, req.GetOffset(), uint32(req.GetWhence()))
	if err != nil {
		return nil, toConnectError(err)
	}

	return &bowlv1.LseekResponse{Offset: offset}, nil
}
