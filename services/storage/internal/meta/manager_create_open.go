package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Mknod 创建特殊文件或普通文件节点并与父目录联动。
func (m *manager) Mknod(ctx context.Context, params MknodParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	switch modeType {
	case S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK:
	default:
		return nil, ErrInvalidArgument
	}

	rdev := uint32(0)
	if modeType == S_IFCHR || modeType == S_IFBLK {
		rdev = params.Rdev
	}

	mutationTracker := m.newDirMutationTracker()
	defer mutationTracker.done()

	var result *Inode
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if isDotOrDotDot(params.Name) {
			return ErrAlreadyExists
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		targetMode := modeType | ((params.Mode & PermMask) &^ (params.Umask & PermMask))

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:    int32(targetMode),
			Nlink:   1,
			Uid:     int32(params.UID),
			Gid:     int32(gid),
			Rdev:    int64(rdev),
			RootIno: parent.RootIno,
		})
		if err != nil {
			return fmt.Errorf("create inode: %w", err)
		}

		err = q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.ParentIno),
			Name:      params.Name,
			Ino:       newInode.Ino,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		if parent.RootIno > 0 {
			if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
				RootIno:     parent.RootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}); err != nil {
				return fmt.Errorf("increment disk used_inodes for root_ino %d: %w", parent.RootIno, err)
			}
		}

		result = m.newInode(newInode)
		mutationTracker.track(params.ParentIno)
		m.onDirEntryMutated(params.ParentIno)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		default:
			return nil, fmt.Errorf("mknod tx: %w", err)
		}
	}

	m.onDirEntryMutated(params.ParentIno)
	return result, nil
}

// Mkdir 在指定父目录下创建子目录并与父目录硬链接联动。
func (m *manager) Mkdir(ctx context.Context, params MkdirParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 65535 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFDIR {
		return nil, ErrInvalidArgument
	}

	mutationTracker := m.newDirMutationTracker()
	defer mutationTracker.done()

	var result *Inode
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if isDotOrDotDot(params.Name) {
			return ErrAlreadyExists
		}

		if uint32(parent.Nlink) >= MaxNlink {
			return ErrTooManyLinks
		}

		gid := params.GID
		targetMode := S_IFDIR | ((params.Mode & PermMask) &^ (params.Umask & PermMask))
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
			targetMode |= S_ISGID
		}

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:      int32(targetMode),
			Nlink:     2,
			Uid:       int32(params.UID),
			Gid:       int32(gid),
			Rdev:      0,
			ParentIno: int64(params.ParentIno),
			RootIno:   parent.RootIno,
		})
		if err != nil {
			return fmt.Errorf("create inode: %w", err)
		}

		if parent.RootIno > 0 {
			if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
				RootIno:     parent.RootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}); err != nil {
				return fmt.Errorf("increment disk used_inodes for root_ino %d: %w", parent.RootIno, err)
			}
		}

		err = q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.ParentIno),
			Name:      params.Name,
			Ino:       newInode.Ino,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		if err := q.IncrementInodeNlinkAndTouch(ctx, parent.Ino); err != nil {
			return fmt.Errorf("increment parent inode %d nlink and touch: %w", parent.Ino, err)
		}

		result = m.newInode(newInode)
		mutationTracker.track(params.ParentIno)
		m.onDirEntryMutated(params.ParentIno)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		case errors.Is(err, ErrTooManyLinks):
			return nil, ErrTooManyLinks
		default:
			return nil, fmt.Errorf("mkdir tx: %w", err)
		}
	}

	m.onDirEntryMutated(params.ParentIno)
	return result, nil
}

// Symlink 在指定父目录下创建指向目标路径的符号链接节点。
func (m *manager) Symlink(ctx context.Context, params SymlinkParams) (*Inode, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if len(params.Target) == 0 || bytes.IndexByte(params.Target, 0) >= 0 {
		return nil, ErrInvalidArgument
	}
	if len(params.Target) > MaxSymlinkTargetBytes {
		return nil, ErrNameTooLong
	}

	mutationTracker := m.newDirMutationTracker()
	defer mutationTracker.done()

	var result *Inode
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if isDotOrDotDot(params.Name) {
			return ErrAlreadyExists
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		newInode, err := q.CreateSymlinkInode(ctx, db.CreateSymlinkInodeParams{
			Mode:          int32(S_IFLNK | 0o777),
			Size:          int64(len(params.Target)),
			Nlink:         1,
			Uid:           int32(params.UID),
			Gid:           int32(gid),
			Rdev:          0,
			SymlinkTarget: params.Target,
			RootIno:       parent.RootIno,
		})
		if err != nil {
			return fmt.Errorf("create symlink inode: %w", err)
		}

		err = q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.ParentIno),
			Name:      params.Name,
			Ino:       newInode.Ino,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		if parent.RootIno > 0 {
			if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
				RootIno:     parent.RootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}); err != nil {
				return fmt.Errorf("increment disk used_inodes for root_ino %d: %w", parent.RootIno, err)
			}
		}

		result = m.newInode(newInode)
		mutationTracker.track(params.ParentIno)
		m.onDirEntryMutated(params.ParentIno)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		default:
			return nil, fmt.Errorf("symlink tx: %w", err)
		}
	}

	m.onDirEntryMutated(params.ParentIno)
	return result, nil
}

// Create 在指定父目录下原子创建常规文件并分配初始文件句柄。
// 严禁在 flags 中携带 O_TMPFILE 标志位（匿名临时文件必须使用 TmpFile 接口创建），否则静态入参校验直接返回 ErrInvalidArgument。
func (m *manager) Create(ctx context.Context, params CreateParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if err := validateEntryName(params.Name); err != nil {
		return nil, err
	}
	if params.Mode > 0xffff {
		return nil, ErrInvalidArgument
	}
	if (params.Flags & O_TMPFILE) != 0 {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return nil, ErrInvalidArgument
	}

	targetMode := S_IFREG | ((params.Mode & PermMask) &^ (params.Umask & PermMask))

	mutationTracker := m.newDirMutationTracker()
	defer mutationTracker.done()

	var result *CreateResult
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		if isDotOrDotDot(params.Name) {
			return ErrAlreadyExists
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:    int32(targetMode),
			Nlink:   1,
			Uid:     int32(params.UID),
			Gid:     int32(gid),
			Rdev:    0,
			RootIno: parent.RootIno,
		})
		if err != nil {
			return fmt.Errorf("create inode: %w", err)
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if parent.RootIno > 0 {
			if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
				RootIno:     parent.RootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}); err != nil {
				return fmt.Errorf("increment disk used_inodes for root_ino %d: %w", parent.RootIno, err)
			}
		}

		err = q.CreateEntry(ctx, db.CreateEntryParams{
			ParentIno: int64(params.ParentIno),
			Name:      params.Name,
			Ino:       newInode.Ino,
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return ErrAlreadyExists
			}
			return fmt.Errorf("create entry: %w", err)
		}

		if err := q.TouchInodeTimestamps(ctx, parent.Ino); err != nil {
			return fmt.Errorf("touch parent inode %d timestamps: %w", parent.Ino, err)
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       newInode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			return fmt.Errorf("create file handle for inode %d: %w", newInode.Ino, err)
		}

		inode := m.newInode(newInode)
		var openFlags uint32
		if m.openKeepCache {
			openFlags = FOPEN_KEEP_CACHE
		}
		result = &CreateResult{
			Inode:               inode,
			Fh:                  uint64(handle.Fh),
			OpenFlags:           openFlags,
			EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
			AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
		}
		mutationTracker.track(params.ParentIno)
		m.onDirEntryMutated(params.ParentIno)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrAlreadyExists):
			return nil, ErrAlreadyExists
		default:
			return nil, fmt.Errorf("create tx: %w", err)
		}
	}

	m.onDirEntryMutated(params.ParentIno)
	return result, nil
}

// TmpFile 在指定父目录下原子创建未绑定 Entry (nlink = 0) 的匿名临时常规文件并分配初始文件句柄。
func (m *manager) TmpFile(ctx context.Context, params TmpFileParams) (*CreateResult, error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if params.Mode > 0xffff {
		return nil, ErrInvalidArgument
	}

	modeType := params.Mode & S_IFMT
	if modeType != 0 && modeType != S_IFREG {
		return nil, ErrInvalidArgument
	}

	accMode := params.Flags & O_ACCMODE
	if accMode != O_WRONLY && accMode != O_RDWR {
		return nil, ErrInvalidArgument
	}

	targetMode := S_IFREG | ((params.Mode & PermMask) &^ (params.Umask & PermMask))

	var result *CreateResult
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		parent, err := q.GetInodeForUpdate(ctx, int64(params.ParentIno))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get parent inode for update %d: %w", params.ParentIno, err)
		}

		if (uint32(parent.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if parent.Nlink == 0 {
			return ErrNotFound
		}

		gid := params.GID
		if (uint32(parent.Mode) & S_ISGID) != 0 {
			gid = uint32(parent.Gid)
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		newInode, err := q.CreateInode(ctx, db.CreateInodeParams{
			Mode:    int32(targetMode),
			Nlink:   0,
			Uid:     int32(params.UID),
			Gid:     int32(gid),
			Rdev:    0,
			RootIno: parent.RootIno,
		})
		if err != nil {
			return fmt.Errorf("create tmpfile inode: %w", err)
		}

		if parent.RootIno > 0 {
			if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
				RootIno:     parent.RootIno,
				DeltaBytes:  0,
				DeltaInodes: 1,
			}); err != nil {
				return fmt.Errorf("increment disk used_inodes for root_ino %d: %w", parent.RootIno, err)
			}
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       newInode.Ino,
			Flags:     int32(params.Flags | O_TMPFILE),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			return fmt.Errorf("create file handle for tmpfile inode %d: %w", newInode.Ino, err)
		}

		inode := m.newInode(newInode)
		var openFlags uint32
		if m.openKeepCache {
			openFlags = FOPEN_KEEP_CACHE
		}
		result = &CreateResult{
			Inode:               inode,
			Fh:                  uint64(handle.Fh),
			OpenFlags:           openFlags,
			EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
			AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
		}
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		default:
			return nil, fmt.Errorf("tmpfile tx: %w", err)
		}
	}

	return result, nil
}

// Open 打开指定 Inode 节点并分配文件句柄 (FileHandle)。
// 严禁在 flags 中携带 O_TMPFILE 标志位（严防伪造 O_TMPFILE 绕过独占限制或非法复活孤儿节点），否则静态入参校验直接返回 ErrInvalidArgument。
func (m *manager) Open(ctx context.Context, params OpenParams) (*OpenResult, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}
	if (params.Flags & O_TMPFILE) != 0 {
		return nil, ErrInvalidArgument
	}

	var result *OpenResult
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		fileType := uint32(inode.Mode) & S_IFMT
		if fileType == S_IFDIR {
			return ErrIsDir
		}
		if fileType == S_IFLNK {
			return ErrInvalidArgument
		}

		if inode.Nlink == 0 {
			hasHandles, err := q.HasFileHandlesByIno(ctx, inode.Ino)
			if err != nil {
				return fmt.Errorf("check has file handles for inode %d: %w", inode.Ino, err)
			}
			if !hasHandles {
				return ErrNotFound
			}
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if (params.Flags & O_TRUNC) != 0 {
			if (params.Flags & O_ACCMODE) == O_RDONLY {
				return ErrInvalidArgument
			}
			if fileType == S_IFREG {
				truncatedMode := uint32(inode.Mode) &^ (S_ISUID | S_ISGID)
				_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
					Ino:         inode.Ino,
					Mode:        int32(truncatedMode),
					Size:        0,
					Uid:         inode.Uid,
					Gid:         inode.Gid,
					SetAtimeNow: false,
					Atime:       inode.Atime,
					SetMtimeNow: true,
					Mtime:       pgtype.Timestamptz{},
					Flags:       inode.Flags,
				})
				if err != nil {
					return fmt.Errorf("truncate inode attr for inode %d: %w", inode.Ino, err)
				}
				if inode.RootIno > 0 && inode.Size > 0 {
					if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
						RootIno:     inode.RootIno,
						DeltaBytes:  -inode.Size,
						DeltaInodes: 0,
					}); err != nil {
						return fmt.Errorf("deduct disk used_bytes on open truncate for inode %d: %w", inode.Ino, err)
					}
				}
				if inode.Size > 0 {
					if err := deleteAllChunksByIno(ctx, q, inode.Ino); err != nil {
						return fmt.Errorf("delete all chunks on open truncate for inode %d: %w", inode.Ino, err)
					}
				}
			}
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       inode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" {
				return ErrNotFound
			}
			return fmt.Errorf("create file handle for inode %d: %w", inode.Ino, err)
		}

		var openFlags uint32
		if m.openKeepCache {
			openFlags = FOPEN_KEEP_CACHE
		}
		result = &OpenResult{
			Fh:        uint64(handle.Fh),
			OpenFlags: openFlags,
		}
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		default:
			return nil, fmt.Errorf("open tx: %w", err)
		}
	}

	return result, nil
}

// OpenDir 打开指定目录 Inode 节点并分配目录 FileHandle。
func (m *manager) OpenDir(ctx context.Context, params OpenDirParams) (*OpenDirResult, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || (params.Flags&O_ACCMODE) != O_RDONLY || (params.Flags&O_TRUNC) != 0 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	var result *OpenDirResult
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if inode.Nlink == 0 {
			hasHandles, err := q.HasFileHandlesByIno(ctx, inode.Ino)
			if err != nil {
				return fmt.Errorf("check has file handles for inode %d: %w", inode.Ino, err)
			}
			if !hasHandles {
				return ErrNotFound
			}
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		handle, err := q.CreateFileHandle(ctx, db.CreateFileHandleParams{
			Ino:       inode.Ino,
			Flags:     int32(params.Flags),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" {
				return ErrNotFound
			}
			return fmt.Errorf("create file handle for inode %d: %w", inode.Ino, err)
		}

		var openFlags uint32
		if m.openDirCache {
			openFlags = FOPEN_CACHE_DIR
		}
		result = &OpenDirResult{
			Fh:        uint64(handle.Fh),
			OpenFlags: openFlags,
		}
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		default:
			return nil, fmt.Errorf("opendir tx: %w", err)
		}
	}

	return result, nil
}
