package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

type dirChildRecord struct {
	name  string
	inode *bowlv1.Inode
}

func (s *StorageService) getOpenDirHandle(fh uint64) (*OpenDirHandle, bool) {
	val, ok := s.openDirHandles.Load(fh)
	if !ok {
		return nil, false
	}
	handle, ok := val.(*OpenDirHandle)
	return handle, ok
}

// OpenDir opens a directory for iteration, assigns a directory handle (dh), and binds it to the session.
func (s *StorageService) OpenDir(ctx context.Context, req *bowlv1.OpenDirRequest) (*bowlv1.OpenDirResponse, error) {
	if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino must be greater than 0")
	}

	sess, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	dirInode, err := s.getInode(ctx, schemaName, req.GetIno())
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "directory inode %d not found", req.GetIno())
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query directory inode: %s", err)
	}
	if dirInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "inode %d is not a directory", req.GetIno())
	}

	dh := s.nextDH.Add(1)
	openHandle := &OpenDirHandle{
		DH:        dh,
		Ino:       req.GetIno(),
		SessionID: sess.ID,
		DiskID:    sess.DiskID,
		Flags:     req.GetFlags(),
		CreatedAt: time.Now(),
	}
	fileHandle := &bowlv1.FileHandle{
		Fh:           dh,
		Ino:          req.GetIno(),
		OpenMode:     parseOpenMode(req.GetFlags()),
		Flags:        req.GetFlags(),
		ClientId:     sess.ClientID,
		IsWriteLease: sess.IsWriteLease,
		OpenFlags:    0,
	}
	openHandle.Handle = fileHandle
	s.openDirHandles.Store(dh, openHandle)

	return &bowlv1.OpenDirResponse{
		FileHandle: fileHandle,
	}, nil
}

// listDirEntries is a shared helper for ReadDir and ReadDirPlus with offset cookies and pagination.
func (s *StorageService) listDirEntries(
	ctx context.Context,
	sessionID string,
	reqIno uint64,
	reqFH uint64,
	offset uint64,
	limit uint32,
	plus bool,
) ([]*bowlv1.DirEntry, error) {
	sess, schemaName, err := s.getDiskSchemaForSession(sessionID)
	if err != nil {
		return nil, err
	}

	targetIno := reqIno
	if reqFH != 0 {
		dh, ok := s.getOpenDirHandle(reqFH)
		if !ok {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found", reqFH)
		}
		if dh.SessionID != sess.ID {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found in session", reqFH)
		}
		if reqIno != 0 && reqIno != dh.Ino {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d belongs to inode %d, not %d", reqFH, dh.Ino, reqIno)
		}
		targetIno = dh.Ino
	} else if reqIno == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino or fh is required")
	}

	dirInode, err := s.getInode(ctx, schemaName, targetIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "directory inode %d not found", targetIno)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query directory inode: %s", err)
	}
	if dirInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "inode %d is not a directory", targetIno)
	}

	query := fmt.Sprintf(`
SELECT ino, parent, name, type, mode, nlink, uid, gid, size, blocks, rdev, atime, mtime, ctime, target, generation
FROM "%s".inodes
WHERE parent = $1 AND ino != $1
ORDER BY ino ASC, name ASC;
`, schemaName)

	rows, err := s.db.Query(ctx, query, int64(targetIno))
	if err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "failed to query directory children: %s", err)
	}
	defer rows.Close()

	var children []dirChildRecord
	for rows.Next() {
		var (
			resIno        int64
			resParent     int64
			resName       string
			resType       int
			resMode       int
			resNlink      int
			resUid        int
			resGid        int
			resSize       int64
			resBlocks     int64
			resRdev       int64
			resAtime      time.Time
			resMtime      time.Time
			resCtime      time.Time
			resTarget     string
			resGeneration int64
		)
		if err := rows.Scan(
			&resIno,
			&resParent,
			&resName,
			&resType,
			&resMode,
			&resNlink,
			&resUid,
			&resGid,
			&resSize,
			&resBlocks,
			&resRdev,
			&resAtime,
			&resMtime,
			&resCtime,
			&resTarget,
			&resGeneration,
		); err != nil {
			return nil, connect.Errorf(connect.CodeInternal, "failed to scan directory child row: %s", err)
		}

		childInode := &bowlv1.Inode{
			Ino:         uint64(resIno),
			ParentIno:   uint64(resParent),
			Type:        bowlv1.InodeType(resType),
			Mode:        uint32(resMode),
			Uid:         uint32(resUid),
			Gid:         uint32(resGid),
			Size:        uint64(resSize),
			Blocks:      uint64(resBlocks),
			Nlink:       uint32(resNlink),
			Rdev:        uint32(resRdev),
			Atime:       timestamppb.New(resAtime),
			Mtime:       timestamppb.New(resMtime),
			Ctime:       timestamppb.New(resCtime),
			Target:      resTarget,
			Generation:  uint64(resGeneration),
			AttrTimeout: durationpb.New(1 * time.Second),
		}
		children = append(children, dirChildRecord{
			name:  resName,
			inode: childInode,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, connect.Errorf(connect.CodeInternal, "rows iteration error: %s", err)
	}

	var allEntries []*bowlv1.DirEntry

	// Entry 1 (offset 1): "."
	dotEntry := &bowlv1.DirEntry{
		Name:       ".",
		Ino:        dirInode.GetIno(),
		Type:       bowlv1.InodeType_INODE_TYPE_DIRECTORY,
		Offset:     1,
		Generation: dirInode.GetGeneration(),
	}
	if plus {
		dotEntry.Attr = dirInode
		dotEntry.EntryTimeout = durationpb.New(1 * time.Second)
	}
	allEntries = append(allEntries, dotEntry)

	// Entry 2 (offset 2): ".."
	parentIno := dirInode.GetParentIno()
	if parentIno == 0 {
		parentIno = 1
	}
	dotDotEntry := &bowlv1.DirEntry{
		Name:   "..",
		Ino:    parentIno,
		Type:   bowlv1.InodeType_INODE_TYPE_DIRECTORY,
		Offset: 2,
	}
	if plus {
		var parentInode *bowlv1.Inode
		if parentIno == dirInode.GetIno() {
			parentInode = dirInode
		} else {
			parentInode, _ = s.getInode(ctx, schemaName, parentIno)
		}
		if parentInode == nil {
			parentInode = &bowlv1.Inode{
				Ino:         parentIno,
				ParentIno:   1,
				Type:        bowlv1.InodeType_INODE_TYPE_DIRECTORY,
				Mode:        0o755,
				Nlink:       2,
				AttrTimeout: durationpb.New(1 * time.Second),
			}
		}
		dotDotEntry.Attr = parentInode
		dotDotEntry.Generation = parentInode.GetGeneration()
		dotDotEntry.EntryTimeout = durationpb.New(1 * time.Second)
	}
	allEntries = append(allEntries, dotDotEntry)

	// Children entries (offset 3..)
	for idx, child := range children {
		curOffset := uint64(idx + 3)
		entry := &bowlv1.DirEntry{
			Name:       child.name,
			Ino:        child.inode.GetIno(),
			Type:       child.inode.GetType(),
			Offset:     curOffset,
			Generation: child.inode.GetGeneration(),
		}
		if plus {
			entry.Attr = child.inode
			entry.EntryTimeout = durationpb.New(1 * time.Second)
		}
		allEntries = append(allEntries, entry)
	}

	if limit == 0 {
		limit = 100
	}
	start := int(offset)
	if start >= len(allEntries) {
		return []*bowlv1.DirEntry{}, nil
	}
	end := start + int(limit)
	if end > len(allEntries) {
		end = len(allEntries)
	}
	return allEntries[start:end], nil
}

// ReadDir streams directory entries using offset cookies (FUSE_READDIR).
func (s *StorageService) ReadDir(ctx context.Context, req *bowlv1.ReadDirRequest) (*bowlv1.ReadDirResponse, error) {
	entries, err := s.listDirEntries(ctx, req.GetSessionId(), req.GetIno(), req.GetFh(), req.GetOffset(), req.GetLimit(), false)
	if err != nil {
		return nil, err
	}
	return &bowlv1.ReadDirResponse{
		Entries: entries,
	}, nil
}

// ReadDirPlus streams directory entries and prefetches full Inode metadata and timeouts (FUSE_READDIRPLUS).
func (s *StorageService) ReadDirPlus(ctx context.Context, req *bowlv1.ReadDirPlusRequest) (*bowlv1.ReadDirPlusResponse, error) {
	entries, err := s.listDirEntries(ctx, req.GetSessionId(), req.GetIno(), req.GetFh(), req.GetOffset(), req.GetLimit(), true)
	if err != nil {
		return nil, err
	}
	return &bowlv1.ReadDirPlusResponse{
		Entries: entries,
	}, nil
}

// ReleaseDir closes and frees an active directory handle (FUSE_RELEASEDIR).
func (s *StorageService) ReleaseDir(ctx context.Context, req *bowlv1.ReleaseDirRequest) (*bowlv1.ReleaseDirResponse, error) {
	if req.GetFh() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "fh is required")
	}

	sess, _, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	dh, ok := s.getOpenDirHandle(req.GetFh())
	if !ok {
		return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found", req.GetFh())
	}
	if dh.SessionID != sess.ID {
		return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found in session", req.GetFh())
	}
	if req.GetIno() != 0 && req.GetIno() != dh.Ino {
		return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d belongs to inode %d, not %d", req.GetFh(), dh.Ino, req.GetIno())
	}

	s.openDirHandles.Delete(req.GetFh())
	return &bowlv1.ReleaseDirResponse{}, nil
}

// FsyncDir synchronizes directory metadata to persistent storage (FUSE_FSYNCDIR).
func (s *StorageService) FsyncDir(ctx context.Context, req *bowlv1.FsyncDirRequest) (*bowlv1.FsyncDirResponse, error) {
	sess, schemaName, err := s.getDiskSchemaForSession(req.GetSessionId())
	if err != nil {
		return nil, err
	}

	targetIno := req.GetIno()
	if req.GetFh() != 0 {
		dh, ok := s.getOpenDirHandle(req.GetFh())
		if !ok {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found", req.GetFh())
		}
		if dh.SessionID != sess.ID {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d not found in session", req.GetFh())
		}
		if req.GetIno() != 0 && req.GetIno() != dh.Ino {
			return nil, connect.Errorf(connect.CodeNotFound, "directory handle %d belongs to inode %d, not %d", req.GetFh(), dh.Ino, req.GetIno())
		}
		targetIno = dh.Ino
	} else if req.GetIno() == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "ino or fh is required")
	}

	dirInode, err := s.getInode(ctx, schemaName, targetIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, connect.Errorf(connect.CodeNotFound, "directory inode %d not found", targetIno)
		}
		return nil, connect.Errorf(connect.CodeInternal, "failed to query directory inode: %s", err)
	}
	if dirInode.GetType() != bowlv1.InodeType_INODE_TYPE_DIRECTORY {
		return nil, connect.Errorf(connect.CodeFailedPrecondition, "inode %d is not a directory", targetIno)
	}

	return &bowlv1.FsyncDirResponse{}, nil
}
