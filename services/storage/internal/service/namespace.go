package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/session"
)

// checkWriteLease validates write lease state for mutating operations.
func (s *StorageService) checkWriteLease(sessionID string) (*session.Session, string, error) {
	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, "", err
	}
	if sess.ReadOnly || !sess.IsWriteLease || sess.Revoked || (!sess.LeaseExpiresAt.IsZero() && time.Now().After(sess.LeaseExpiresAt)) {
		return nil, "", connect.NewError(connect.CodePermissionDenied, "write permission denied: session is read-only or lease expired")
	}
	return sess, schemaName, nil
}

// allocateNextIno queries the next available Inode number monotonically.
func (s *StorageService) allocateNextIno(ctx context.Context, schemaName string) (uint64, error) {
	if s.db == nil {
		return 0, errors.New("database not initialized")
	}
	query := fmt.Sprintf(`SELECT COALESCE(MAX(ino), 1) + 1 FROM "%s".inodes;`, schemaName)
	var nextIno int64
	if err := s.db.QueryRow(ctx, query).Scan(&nextIno); err != nil {
		return 0, err
	}
	return uint64(nextIno), nil
}

// validateEntryName checks entry name validity.
func validateEntryName(name string) error {
	if name == "" || name == "." || name == ".." {
		return connect.NewError(connect.CodeInvalidArgument, "invalid entry name")
	}
	if strings.Contains(name, "/") || strings.Contains(name, "\x00") {
		return connect.NewError(connect.CodeInvalidArgument, "name cannot contain slash or null byte")
	}
	if len(name) > 255 {
		return connect.NewError(connect.CodeInvalidArgument, "name exceeds maximum length of 255 bytes")
	}
	return nil
}

// parseOpenMode maps open flags to OpenMode enum.
func parseOpenMode(flags uint32) bowlv1.OpenMode {
	switch flags & 0x3 {
	case 1:
		return bowlv1.OpenMode_OPEN_MODE_WRITE_ONLY
	case 2:
		return bowlv1.OpenMode_OPEN_MODE_READ_WRITE
	default:
		if flags == 0 {
			return bowlv1.OpenMode_OPEN_MODE_READ_WRITE
		}
		return bowlv1.OpenMode_OPEN_MODE_READ_ONLY
	}
}

// Create creates a new regular file, registers a file handle, and returns metadata (FUSE_CREATE).
func (s *StorageService) Create(ctx context.Context, req *bowlv1.CreateRequest) (*bowlv1.CreateResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if err := validateEntryName(name); err != nil {
		return nil, err
	}

	sess, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Check parent directory exists and is a directory
	parentInode, err := s.getInode(ctx, schemaName, req.GetParentIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent inode %d not found", req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent inode: %s", err)
	}
	if parentInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent inode %d is not a directory", req.GetParentIno())
	}

	// 2. Check entry doesn't already exist
	_, err = s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err == nil {
		return nil, connect.Errorf(connect.CodeAlreadyExists, "entry %q already exists in parent %d", name, req.GetParentIno())
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check entry existence: %s", err)
	}

	// 3. Mode & umask calculation
	mode := req.GetMode()
	if mode == 0 {
		mode = 0o644
	}
	cleanMode := (mode & 0o7777) &^ req.GetUmask()

	// 4. Allocate next ino
	newIno, err := s.allocateNextIno(ctx, schemaName)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to allocate inode: %s", err)
	}

	now := time.Now()

	// 5. Insert inode
	insertSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation)
VALUES ($1, $2, $3, $4, $5, 1, 0, 0, 0, 0, 0, $6, $6, $6, '', 0);
`, schemaName)
	if _, err := s.db.Exec(ctx, insertSQL, int64(newIno), int64(req.GetParentIno()), name, int(bowlv1.InodeType_INODE_TYPE_FILE), int(cleanMode), now); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to insert file inode: %s", err)
	}

	// 6. Update parent directory timestamps
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	_, _ = s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno()))

	// 7. Allocate file handle
	openMode := parseOpenMode(req.GetFlags())
	var driverHandle HandleContext
	if s.driver != nil {
		var err error
		driverHandle, err = s.driver.Open(ctx, sess.DiskID, newIno, openMode, req.GetFlags())
		if err != nil {
			return nil, toConnectError(err)
		}
	}

	fh := s.nextFH.Add(1)
	fileHandle := &bowlv1.FileHandle{
		Fh:           fh,
		Ino:          newIno,
		OpenMode:     openMode,
		Flags:        req.GetFlags(),
		ClientId:     sess.ClientID,
		IsWriteLease: true,
		OpenFlags:    0,
	}
	s.openHandles.Store(fh, &OpenFileHandle{
		Handle:       fileHandle,
		SessionID:    sess.ID,
		DiskID:       sess.DiskID,
		CreatedAt:    now,
		driverHandle: driverHandle,
	})

	inode := &bowlv1.Inode{
		Ino:         newIno,
		ParentIno:   req.GetParentIno(),
		Type:        bowlv1.InodeType_INODE_TYPE_FILE,
		Mode:        cleanMode,
		Uid:         0,
		Gid:         0,
		Size:        0,
		Blocks:      0,
		Nlink:       1,
		Rdev:        0,
		Atime:       timestamppb.New(now),
		Mtime:       timestamppb.New(now),
		Ctime:       timestamppb.New(now),
		Target:      "",
		Generation:  0,
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.CreateResponse{
		Inode:        inode,
		FileHandle:   fileHandle,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// Mkdir creates a subdirectory, increments parent nlink, and returns metadata (FUSE_MKDIR).
func (s *StorageService) Mkdir(ctx context.Context, req *bowlv1.MkdirRequest) (*bowlv1.MkdirResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if err := validateEntryName(name); err != nil {
		return nil, err
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Check parent directory exists and is a directory
	parentInode, err := s.getInode(ctx, schemaName, req.GetParentIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent inode %d not found", req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent inode: %s", err)
	}
	if parentInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent inode %d is not a directory", req.GetParentIno())
	}

	// 2. Check entry doesn't already exist
	_, err = s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err == nil {
		return nil, connect.Errorf(connect.CodeAlreadyExists, "entry %q already exists in parent %d", name, req.GetParentIno())
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check entry existence: %s", err)
	}

	// 3. Mode & umask calculation
	mode := req.GetMode()
	if mode == 0 {
		mode = 0o755
	}
	cleanMode := (mode & 0o7777) &^ req.GetUmask()

	// 4. Allocate next ino
	newIno, err := s.allocateNextIno(ctx, schemaName)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to allocate inode: %s", err)
	}

	now := time.Now()

	// 5. Insert directory inode (nlink=2, size=4096, blocks=8)
	insertSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation)
VALUES ($1, $2, $3, $4, $5, 2, 0, 0, 4096, 8, 0, $6, $6, $6, '', 0);
`, schemaName)
	if _, err := s.db.Exec(ctx, insertSQL, int64(newIno), int64(req.GetParentIno()), name, int(bowlv1.InodeType_INODE_TYPE_DIRECTORY), int(cleanMode), now); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to insert directory inode: %s", err)
	}

	// 6. Increment parent directory nlink by 1 and update timestamps
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink + 1, mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	if _, err := s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno())); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to update parent directory nlink: %s", err)
	}

	inode := &bowlv1.Inode{
		Ino:         newIno,
		ParentIno:   req.GetParentIno(),
		Type:        bowlv1.InodeType_INODE_TYPE_DIRECTORY,
		Mode:        cleanMode,
		Uid:         0,
		Gid:         0,
		Size:        4096,
		Blocks:      8,
		Nlink:       2,
		Rdev:        0,
		Atime:       timestamppb.New(now),
		Mtime:       timestamppb.New(now),
		Ctime:       timestamppb.New(now),
		Target:      "",
		Generation:  0,
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.MkdirResponse{
		Inode:        inode,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}

// Rmdir removes an empty directory and decrements parent nlink (FUSE_RMDIR).
func (s *StorageService) Rmdir(ctx context.Context, req *bowlv1.RmdirRequest) (*bowlv1.RmdirResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if name == "" || name == "." || name == ".." {
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid entry name")
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Find child directory under parent
	targetInode, err := s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "directory %q not found in parent %d", name, req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query directory: %s", err)
	}

	// 2. Prevent removing root directory
	if targetInode.GetIno() == 1 {
		return nil, connect.NewError(connect.CodePermissionDenied, "cannot remove root directory")
	}

	// 3. Ensure target is a directory
	if targetInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeInvalidArgument, "entry %q is not a directory; use unlink", name)
	}

	// 4. Check if directory is empty (no child entries with parent = target_ino AND ino != target_ino)
	countQuery := fmt.Sprintf(`SELECT count(*) FROM "%s".inodes WHERE parent = $1 AND ino != $1;`, schemaName)
	var childCount int64
	if err := s.db.QueryRow(ctx, countQuery, int64(targetInode.GetIno())).Scan(&childCount); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check directory contents: %s", err)
	}
	if childCount > 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, "directory not empty")
	}

	// 5. Delete directory inode
	deleteSQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE ino = $1;`, schemaName)
	if _, err := s.db.Exec(ctx, deleteSQL, int64(targetInode.GetIno())); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to delete directory inode: %s", err)
	}

	// 6. Decrement parent directory nlink by 1 and update timestamps
	now := time.Now()
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = GREATEST(nlink - 1, 1), mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	if _, err := s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno())); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to update parent directory nlink: %s", err)
	}

	return &bowlv1.RmdirResponse{}, nil
}

// Unlink removes a non-directory entry or decrements its link count (FUSE_UNLINK).
func (s *StorageService) Unlink(ctx context.Context, req *bowlv1.UnlinkRequest) (*bowlv1.UnlinkResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if name == "" || name == "." || name == ".." {
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid entry name")
	}

	sess, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Find entry under parent
	targetInode, err := s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "entry %q not found in parent %d", name, req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query entry: %s", err)
	}

	// 2. Reject directory unlinking (POSIX: must use rmdir)
	if targetInode.GetType() == bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.NewError(connect.CodeInvalidArgument, "cannot unlink directory; use rmdir")
	}

	now := time.Now()

	// 3. Decrement nlink; delete inode if nlink <= 1 and lookup_count == 0
	if targetInode.GetNlink() <= 1 {
		var lookupCount int64
		_ = s.db.QueryRow(ctx, fmt.Sprintf(`SELECT lookup_count FROM "%s".inodes WHERE ino = $1;`, schemaName), int64(targetInode.GetIno())).Scan(&lookupCount)
		if lookupCount <= 0 {
			deleteSQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE ino = $1;`, schemaName)
			if _, err := s.db.Exec(ctx, deleteSQL, int64(targetInode.GetIno())); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to delete inode: %s", err)
			}
			deleteXAttrSQL := fmt.Sprintf(`DELETE FROM "%s".xattrs WHERE ino = $1;`, schemaName)
			_, _ = s.db.Exec(ctx, deleteXAttrSQL, int64(targetInode.GetIno()))
			if s.driver != nil {
				_ = s.driver.DeleteInode(ctx, sess.DiskID, targetInode.GetIno())
			}
		} else {
			// Unlinked but still referenced by lookup count: set nlink = 0 and detach from parent
			deletedName := fmt.Sprintf(".deleted.%d", targetInode.GetIno())
			updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = 0, parent = 0, name = $1, ctime = $2 WHERE ino = $3;`, schemaName)
			if _, err := s.db.Exec(ctx, updateSQL, deletedName, now, int64(targetInode.GetIno())); err != nil {
				return nil, connect.Errorf(connect.CodeInternal, "failed to unlink inode: %s", err)
			}
		}
	} else {
		deleteEntrySQL := fmt.Sprintf(`DELETE FROM "%s".inodes WHERE parent = $1 AND name = $2;`, schemaName)
		if _, err := s.db.Exec(ctx, deleteEntrySQL, int64(req.GetParentIno()), name); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to delete directory entry: %s", err)
		}

		updateSQL := fmt.Sprintf(`UPDATE "%s".inodes SET nlink = nlink - 1, ctime = $1 WHERE ino = $2;`, schemaName)
		if _, err := s.db.Exec(ctx, updateSQL, now, int64(targetInode.GetIno())); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to decrement nlink: %s", err)
		}
	}

	// 4. Update parent directory timestamps
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	_, _ = s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno()))

	return &bowlv1.UnlinkResponse{}, nil
}

// Mknod creates a special device, FIFO, or socket node (FUSE_MKNOD).
func (s *StorageService) Mknod(ctx context.Context, req *bowlv1.MknodRequest) (*bowlv1.MknodResponse, error) {
	if req.GetParentIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "parent_ino must be greater than 0")
	}
	name := req.GetName()
	if err := validateEntryName(name); err != nil {
		return nil, err
	}

	_, schemaName, err := s.checkWriteLease(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	// 1. Check parent directory exists and is a directory
	parentInode, err := s.getInode(ctx, schemaName, req.GetParentIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "parent inode %d not found", req.GetParentIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query parent inode: %s", err)
	}
	if parentInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "parent inode %d is not a directory", req.GetParentIno())
	}

	// 2. Check entry doesn't already exist
	_, err = s.getInodeByParentAndName(ctx, schemaName, req.GetParentIno(), name)
	if err == nil {
		return nil, connect.Errorf(connect.CodeAlreadyExists, "entry %q already exists in parent %d", name, req.GetParentIno())
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.Errorf(connect.CodeInternal, "failed to check entry existence: %s", err)
	}

	// 3. Determine node type
	nodeType := req.GetType()
	if nodeType == bowlv1.InodeType_INODE_TYPE_UNSPECIFIED {
		switch req.GetMode() & 0o170000 {
		case 0o010000:
			nodeType = bowlv1.InodeType_INODE_TYPE_FIFO
		case 0o020000:
			nodeType = bowlv1.InodeType_INODE_TYPE_CHAR_DEV
		case 0o060000:
			nodeType = bowlv1.InodeType_INODE_TYPE_BLOCK_DEV
		case 0o140000:
			nodeType = bowlv1.InodeType_INODE_TYPE_SOCKET
		default:
			nodeType = bowlv1.InodeType_INODE_TYPE_FIFO
		}
	}
	if nodeType == bowlv1.InodeType_INODE_TYPE_DIRECTORY || nodeType == bowlv1.InodeType_INODE_TYPE_SYMLINK {
		return nil, connect.NewError(connect.CodeInvalidArgument, "invalid node type for mknod")
	}

	// 4. Mode & umask calculation
	mode := req.GetMode()
	if mode == 0 {
		mode = 0o666
	}
	cleanMode := (mode & 0o7777) &^ req.GetUmask()

	// 5. Allocate next ino
	newIno, err := s.allocateNextIno(ctx, schemaName)
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to allocate inode: %s", err)
	}

	now := time.Now()

	// 6. Insert special node
	insertSQL := fmt.Sprintf(`
INSERT INTO "%s".inodes (ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation)
VALUES ($1, $2, $3, $4, $5, 1, 0, 0, 0, 0, $6, $7, $7, $7, '', 0);
`, schemaName)
	if _, err := s.db.Exec(ctx, insertSQL, int64(newIno), int64(req.GetParentIno()), name, int(nodeType), int(cleanMode), int64(req.GetRdev()), now); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to insert special inode: %s", err)
	}

	// 7. Update parent directory timestamps
	updateParentSQL := fmt.Sprintf(`UPDATE "%s".inodes SET mtime = $1, ctime = $1 WHERE ino = $2;`, schemaName)
	_, _ = s.db.Exec(ctx, updateParentSQL, now, int64(req.GetParentIno()))

	inode := &bowlv1.Inode{
		Ino:         newIno,
		ParentIno:   req.GetParentIno(),
		Type:        nodeType,
		Mode:        cleanMode,
		Uid:         0,
		Gid:         0,
		Size:        0,
		Blocks:      0,
		Nlink:       1,
		Rdev:        req.GetRdev(),
		Atime:       timestamppb.New(now),
		Mtime:       timestamppb.New(now),
		Ctime:       timestamppb.New(now),
		Target:      "",
		Generation:  0,
		AttrTimeout: durationpb.New(1 * time.Second),
	}

	return &bowlv1.MknodResponse{
		Inode:        inode,
		EntryTimeout: durationpb.New(1 * time.Second),
	}, nil
}
