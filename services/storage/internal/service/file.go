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
	"github.com/jackc/pgx/v5"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

// filePath computes the local file path for storing chunk data of a specific Inode.
// Data root path: <DATA_DIR>/<disk_id>/file_<ino>.bin (ADR 0009).
func (s *StorageService) filePath(diskID string, ino uint64) string {
	base := s.dataDir
	if base == "" {
		base = filepath.Join(os.TempDir(), "bowl_data")
	}
	return filepath.Join(base, diskID, fmt.Sprintf("file_%d.bin", ino))
}

// Open opens an existing file, allocates a session-bound file handle, and returns it (FUSE_OPEN).
func (s *StorageService) Open(ctx context.Context, req *bowlv1.OpenRequest) (*bowlv1.OpenResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	ino := req.GetIno()
	if ino == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	// 1. Verify Inode exists in PostgreSQL
	if s.db != nil {
		inode, err := s.getInode(ctx, schemaName, ino)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, connect.Errorf(connect.CodeNotFound, "inode not found: %d", ino)
			}
			return nil, connect.Errorf(connect.CodeInternal, "failed to query inode: %s", err)
		}
		if inode.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
			return nil, connect.NewError(connect.CodeInvalidArgument, "cannot open directory with open; use opendir")
		}
	}

	// 2. Resolve OpenMode
	openMode := req.GetOpenMode()
	if openMode == bowlv1.OpenMode_OPEN_MODE_UNSPECIFIED {
		switch req.GetFlags() & 0x3 {
		case 1:
			openMode = bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY
		case 2:
			openMode = bowlv1.OpenMode_OPEN_MODE_READ_WRITE
		default:
			openMode = bowlv1.OpenMode_OPEN_MODE_READ_ONLY
		}
	}

	// 3. Check write lease if writing is requested
	isWriteRequested := openMode == bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY || openMode == bowlv1.OpenMode_OPEN_MODE_READ_WRITE
	if isWriteRequested {
		if sess.ReadOnly {
			return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only")
		}
		if !sess.IsWriteLease || sess.Revoked || (!sess.LeaseExpiresAt.IsZero() && time.Now().After(sess.LeaseExpiresAt)) {
			return nil, connect.NewError(connect.CodePermissionDenied, "write permission denied: write lease expired or not held")
		}
	}

	// 4. Open underlying OS file
	filePath := s.filePath(sess.DiskID, ino)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	var file *os.File
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

	// 5. Allocate and register file handle
	fh := s.nextFH.Add(1)
	isWriteLease := sess.IsWriteLease && !sess.ReadOnly && !sess.Revoked && (sess.LeaseExpiresAt.IsZero() || time.Now().Before(sess.LeaseExpiresAt))
	fileHandle := &bowlv1.FileHandle{
		Fh:           fh,
		Ino:          ino,
		OpenMode:     openMode,
		Flags:        req.GetFlags(),
		ClientId:     sess.ClientID,
		IsWriteLease: isWriteLease,
		OpenFlags:    0,
	}

	s.openHandles.Store(fh, &OpenFileHandle{
		Handle:    fileHandle,
		SessionID: sess.ID,
		DiskID:    sess.DiskID,
		CreatedAt: time.Now(),
		file:      file,
	})

	return &bowlv1.OpenResponse{
		FileHandle: fileHandle,
	}, nil
}

// Read reads file data at a given offset and size from local chunk storage (FUSE_READ).
func (s *StorageService) Read(ctx context.Context, req *bowlv1.ReadRequest) (*bowlv1.ReadResponse, error) {
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

	// 1. Lookup file handle
	val, ok := s.openHandles.Load(fh)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
	}
	handle := val.(*OpenFileHandle)
	if handle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
	}

	// 2. Validate Inode
	if req.GetIno() != 0 && req.GetIno() != handle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", req.GetIno(), handle.Handle.GetIno())
	}
	ino := handle.Handle.GetIno()

	// 3. Check access mode
	if handle.Handle.GetOpenMode() == bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY {
		return nil, connect.NewError(connect.CodePermissionDenied, "file handle is open for write-only")
	}

	// 4. Zero size read
	if req.GetSize() == 0 {
		return &bowlv1.ReadResponse{
			Data: []byte{},
			Eof:  false,
		}, nil
	}

	// 5. Read from local file
	filePath := s.filePath(handle.DiskID, ino)
	handle.mu.Lock()
	defer handle.mu.Unlock()

	if handle.file == nil {
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			return &bowlv1.ReadResponse{
				Data: []byte{},
				Eof:  true,
			}, nil
		}
		f, err := os.OpenFile(filePath, os.O_RDONLY, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open file for read: %s", err)
		}
		handle.file = f
	}

	fi, err := handle.file.Stat()
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to stat file: %s", err)
	}
	fileSize := uint64(fi.Size())
	offset := req.GetOffset()
	if offset >= fileSize {
		return &bowlv1.ReadResponse{
			Data: []byte{},
			Eof:  true,
		}, nil
	}

	available := fileSize - offset
	toRead := uint64(req.GetSize())
	eof := false
	if toRead >= available {
		toRead = available
		eof = true
	}

	buf := make([]byte, toRead)
	n, err := handle.file.ReadAt(buf, int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to read file: %s", err)
	}

	return &bowlv1.ReadResponse{
		Data: buf[:n],
		Eof:  eof || (errors.Is(err, io.EOF) && uint64(n) == available),
	}, nil
}

// Write writes binary data to local file at offset and updates Inode metadata (FUSE_WRITE).
func (s *StorageService) Write(ctx context.Context, req *bowlv1.WriteRequest) (*bowlv1.WriteResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	fh := req.GetFh()
	if fh == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh must be greater than 0")
	}

	// 1. Check write lease
	_, schemaName, err := s.checkWriteLease(sessionID)
	if err != nil {
		return nil, err
	}

	// 2. Lookup handle
	val, ok := s.openHandles.Load(fh)
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "file handle %d not found", fh)
	}
	handle := val.(*OpenFileHandle)
	if handle.SessionID != sessionID {
		return nil, connect.Errorf(connect.CodePermissionDenied, "file handle %d does not belong to session %s", fh, sessionID)
	}

	// 3. Validate Inode
	if req.GetIno() != 0 && req.GetIno() != handle.Handle.GetIno() {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "ino %d does not match handle ino %d", req.GetIno(), handle.Handle.GetIno())
	}
	ino := handle.Handle.GetIno()

	// 4. Check access mode
	if handle.Handle.GetOpenMode() == bowlv1.OpenMode_OPEN_MODE_READ_ONLY {
		return nil, connect.NewError(connect.CodePermissionDenied, "file handle is open for read-only")
	}

	// 5. Write data into local file
	data := req.GetData()
	filePath := s.filePath(handle.DiskID, ino)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to create directory: %s", err)
	}

	handle.mu.Lock()
	defer handle.mu.Unlock()

	if handle.file == nil {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to open file for write: %s", err)
		}
		handle.file = f
	}

	var written uint32
	if len(data) > 0 {
		n, err := handle.file.WriteAt(data, int64(req.GetOffset()))
		if err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to write file: %s", err)
		}
		written = uint32(n)
	}

	// 6. Update Inode size and mtime in PostgreSQL
	if s.db != nil {
		now := time.Now()
		newEnd := int64(req.GetOffset()) + int64(written)
		updateSQL := fmt.Sprintf(`
UPDATE "%s".inodes
SET size = GREATEST(size, $1::bigint),
    blocks = (GREATEST(size, $1::bigint) + 511) / 512,
    mtime = $2,
    ctime = $2
WHERE ino = $3;
`, schemaName)
		if _, err := s.db.Exec(ctx, updateSQL, newEnd, now, int64(ino)); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to update inode metadata: %s", err)
		}
	}

	return &bowlv1.WriteResponse{
		Written: written,
	}, nil
}

// Release closes and unregisters an open file handle (FUSE_RELEASE).
func (s *StorageService) Release(ctx context.Context, req *bowlv1.ReleaseRequest) (*bowlv1.ReleaseResponse, error) {
	sessionID := strings.TrimSpace(req.GetSessionId())
	if sessionID == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	fh := req.GetFh()
	if fh == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh must be greater than 0")
	}

	// 1. Verify session exists
	if _, found := s.sessions.Get(sessionID); !found {
		return nil, connect.Errorf(connect.CodeNotFound, "session not found: %s", sessionID)
	}

	// 2. Lookup file handle
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

	// 3. Unregister and close file
	s.openHandles.Delete(fh)

	handle.mu.Lock()
	if handle.file != nil {
		_ = handle.file.Close()
		handle.file = nil
	}
	handle.mu.Unlock()

	return &bowlv1.ReleaseResponse{}, nil
}
