package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

type pendingDirCursor struct {
	ino              uint64
	fh               uint64
	nextOffset       uint64
	lastName         []byte
	sqlOffset        int64
	dirMutated       bool
	sessionID        uint64
	handleCreatedAt  time.Time
	startMutationSeq uint64
}

// validateDirHandleWithHandle 校验目录会话有效性，确认 Inode 存在、为目录且 FileHandle 属于该 Inode，并返回 FileHandle 元数据。
func (m *manager) validateDirHandleWithHandle(ctx context.Context, q db.Querier, ino, fh uint64, sessionID *uint64) (*db.Inode, *db.FileHandle, error) {
	dirInode, err := q.GetInode(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("get dir inode %d: %w", ino, err)
	}

	if (uint32(dirInode.Mode) & S_IFMT) != S_IFDIR {
		return nil, nil, ErrNotDir
	}

	handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
		Fh:        int64(fh),
		Ino:       int64(ino),
		SessionID: toPgSessionID(sessionID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrBadFileHandle
		}
		return nil, nil, fmt.Errorf("get file handle (fh=%d, ino=%d): %w", fh, ino, err)
	}

	return &dirInode, &handle, nil
}

// validateDirHandle 校验目录会话有效性，保持既有接口签名供外部模块复用。
func (m *manager) validateDirHandle(ctx context.Context, q db.Querier, ino, fh uint64, sessionID *uint64) (*db.Inode, error) {
	inode, _, err := m.validateDirHandleWithHandle(ctx, q, ino, fh, sessionID)
	return inode, err
}

// ReadDir 读取指定目录的子项流（基本条目），支持确定性分页游标与 . / .. 合成。
func (m *manager) ReadDir(ctx context.Context, params ReadDirParams) ([]DirEntry, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	startMutationSeq := m.getDirMutationSeq(params.Ino)

	var entries []DirEntry
	var pendingCursors []pendingDirCursor
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		dirInode, handle, err := m.validateDirHandleWithHandle(ctx, q, params.Ino, params.Fh, params.SessionID)
		if err != nil {
			return err
		}

		if dirInode.Nlink == 0 {
			entries = []DirEntry{}
			return nil
		}

		var sid uint64
		if handle.SessionID.Valid && handle.SessionID.Int64 > 0 {
			sid = uint64(handle.SessionID.Int64)
		}
		var handleCreatedAt time.Time
		if handle.CreatedAt.Valid {
			handleCreatedAt = handle.CreatedAt.Time
		}

		rem := int(params.Limit)
		entries = make([]DirEntry, 0, min(rem, 16))

		if params.Offset < 1 && rem > 0 {
			entries = append(entries, DirEntry{
				Ino:        uint64(dirInode.Ino),
				Name:       []byte("."),
				Mode:       uint32(dirInode.Mode),
				NextOffset: 1,
			})
			rem--
		}

		if params.Offset < 2 && rem > 0 {
			parentInode, err := q.GetParentInode(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					parentInode = *dirInode
				} else {
					return fmt.Errorf("get parent inode for %d: %w", params.Ino, err)
				}
			}
			entries = append(entries, DirEntry{
				Ino:        uint64(parentInode.Ino),
				Name:       []byte(".."),
				Mode:       uint32(parentInode.Mode),
				NextOffset: 2,
			})
			rem--
		}

		if rem > 0 {
			var anchor dirCursorAnchor
			var anchorOk bool
			if params.Offset >= 3 {
				anchor, anchorOk = m.lookupDirCursorAnchor(params.Ino, params.Fh, params.Offset)
			}

			if anchorOk && anchor.dirMutated && len(anchor.lastName) > 0 {
				rows, err := q.ListDirEntriesAfterName(ctx, db.ListDirEntriesAfterNameParams{
					ParentIno: int64(params.Ino),
					Limit:     int32(rem),
					AfterName: anchor.lastName,
				})
				if err != nil {
					return fmt.Errorf("list dir entries after name for parent %d (limit=%d, after=%s): %w", params.Ino, rem, string(anchor.lastName), err)
				}

				for i, row := range rows {
					nextOffset := params.Offset + uint64(i) + 1
					entries = append(entries, DirEntry{
						Ino:        uint64(row.Ino),
						Name:       row.Name,
						Mode:       uint32(row.Mode),
						NextOffset: nextOffset,
					})
					pendingCursors = append(pendingCursors, pendingDirCursor{
						ino:              params.Ino,
						fh:               params.Fh,
						nextOffset:       nextOffset,
						lastName:         row.Name,
						sqlOffset:        int64(nextOffset - 2),
						dirMutated:       true,
						sessionID:        sid,
						handleCreatedAt:  handleCreatedAt,
						startMutationSeq: startMutationSeq,
					})
				}
			} else {
				var sqlOffset int64
				if params.Offset >= 2 {
					sqlOffset = m.getDirCursorOffset(params.Ino, params.Fh, params.Offset)
				}
				rows, err := q.ListDirEntries(ctx, db.ListDirEntriesParams{
					ParentIno: int64(params.Ino),
					Limit:     int32(rem),
					Offset:    sqlOffset,
				})
				if err != nil {
					return fmt.Errorf("list dir entries for parent %d (limit=%d, offset=%d): %w", params.Ino, rem, sqlOffset, err)
				}

				for i, row := range rows {
					nextOffset := uint64(sqlOffset) + uint64(i) + 3
					if params.Offset >= 2 && uint64(sqlOffset)+2 != params.Offset {
						nextOffset = max(nextOffset, params.Offset+uint64(i)+1)
					}
					entries = append(entries, DirEntry{
						Ino:        uint64(row.Ino),
						Name:       row.Name,
						Mode:       uint32(row.Mode),
						NextOffset: nextOffset,
					})
					pendingCursors = append(pendingCursors, pendingDirCursor{
						ino:              params.Ino,
						fh:               params.Fh,
						nextOffset:       nextOffset,
						lastName:         row.Name,
						sqlOffset:        sqlOffset + int64(i) + 1,
						dirMutated:       false,
						sessionID:        sid,
						handleCreatedAt:  handleCreatedAt,
						startMutationSeq: startMutationSeq,
					})
				}
			}
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
			m.clearDirCursors(params.Ino, params.Fh)
		} else if errors.Is(err, ErrBadFileHandle) {
			var reqSid uint64
			if params.SessionID != nil {
				reqSid = *params.SessionID
			}
			m.clearDirCursorsForSession(params.Ino, params.Fh, reqSid)
		}
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("readdir tx: %w", err)
		}
	}

	for _, pc := range pendingCursors {
		m.recordDirCursor(pc.ino, pc.fh, pc.nextOffset, pc.lastName, pc.sqlOffset, pc.dirMutated, pc.sessionID, pc.handleCreatedAt, pc.startMutationSeq)
	}

	if entries == nil {
		entries = []DirEntry{}
	}
	return entries, nil
}

// ReadDirPlus 读取指定目录的子项流并附带完整 Inode 属性与缓存时效，支持确定性分页游标与 . / .. 合成。
func (m *manager) ReadDirPlus(ctx context.Context, params ReadDirPlusParams) ([]DirEntryPlus, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Limit == 0 || params.Limit > MaxReadDirLimit ||
		params.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	startMutationSeq := m.getDirMutationSeq(params.Ino)

	var entries []DirEntryPlus
	var pendingCursors []pendingDirCursor
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		dirInode, handle, err := m.validateDirHandleWithHandle(ctx, q, params.Ino, params.Fh, params.SessionID)
		if err != nil {
			return err
		}

		if dirInode.Nlink == 0 {
			entries = []DirEntryPlus{}
			return nil
		}

		var sid uint64
		if handle.SessionID.Valid && handle.SessionID.Int64 > 0 {
			sid = uint64(handle.SessionID.Int64)
		}
		var handleCreatedAt time.Time
		if handle.CreatedAt.Valid {
			handleCreatedAt = handle.CreatedAt.Time
		}

		rem := int(params.Limit)
		entries = make([]DirEntryPlus, 0, min(rem, 16))

		if params.Offset < 1 && rem > 0 {
			entries = append(entries, DirEntryPlus{
				Ino:                 uint64(dirInode.Ino),
				Name:                []byte("."),
				NextOffset:          1,
				Inode:               convertDBInodeToEntity(*dirInode, 0, m.attrTimeout),
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  m.attrTimeout,
			})
			rem--
		}

		if params.Offset < 2 && rem > 0 {
			parentInode, err := q.GetParentInode(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					parentInode = *dirInode
				} else {
					return fmt.Errorf("get parent inode for %d: %w", params.Ino, err)
				}
			}
			entries = append(entries, DirEntryPlus{
				Ino:                 uint64(parentInode.Ino),
				Name:                []byte(".."),
				NextOffset:          2,
				Inode:               convertDBInodeToEntity(parentInode, 0, m.attrTimeout),
				EntryTimeoutSeconds: 0,
				AttrTimeoutSeconds:  m.attrTimeout,
			})
			rem--
		}

		if rem > 0 {
			var anchor dirCursorAnchor
			var anchorOk bool
			if params.Offset >= 3 {
				anchor, anchorOk = m.lookupDirCursorAnchor(params.Ino, params.Fh, params.Offset)
			}

			if anchorOk && anchor.dirMutated && len(anchor.lastName) > 0 {
				rows, err := q.ListDirEntriesAfterName(ctx, db.ListDirEntriesAfterNameParams{
					ParentIno: int64(params.Ino),
					Limit:     int32(rem),
					AfterName: anchor.lastName,
				})
				if err != nil {
					return fmt.Errorf("list dir entries after name for parent %d (limit=%d, after=%s): %w", params.Ino, rem, string(anchor.lastName), err)
				}

				for i, row := range rows {
					nextOffset := params.Offset + uint64(i) + 1
					inode := m.newInodeFromListDirAfterNameRow(row)
					entries = append(entries, DirEntryPlus{
						Ino:                 uint64(row.Ino),
						Name:                row.Name,
						NextOffset:          nextOffset,
						Inode:               inode,
						EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
						AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
					})
					pendingCursors = append(pendingCursors, pendingDirCursor{
						ino:              params.Ino,
						fh:               params.Fh,
						nextOffset:       nextOffset,
						lastName:         row.Name,
						sqlOffset:        int64(nextOffset - 2),
						dirMutated:       true,
						sessionID:        sid,
						handleCreatedAt:  handleCreatedAt,
						startMutationSeq: startMutationSeq,
					})
				}
			} else {
				var sqlOffset int64
				if params.Offset >= 2 {
					sqlOffset = m.getDirCursorOffset(params.Ino, params.Fh, params.Offset)
				}
				rows, err := q.ListDirEntries(ctx, db.ListDirEntriesParams{
					ParentIno: int64(params.Ino),
					Limit:     int32(rem),
					Offset:    sqlOffset,
				})
				if err != nil {
					return fmt.Errorf("list dir entries for parent %d (limit=%d, offset=%d): %w", params.Ino, rem, sqlOffset, err)
				}

				for i, row := range rows {
					nextOffset := uint64(sqlOffset) + uint64(i) + 3
					if params.Offset >= 2 && uint64(sqlOffset)+2 != params.Offset {
						nextOffset = max(nextOffset, params.Offset+uint64(i)+1)
					}
					inode := m.newInodeFromListDirRow(row)
					entries = append(entries, DirEntryPlus{
						Ino:                 uint64(row.Ino),
						Name:                row.Name,
						NextOffset:          nextOffset,
						Inode:               inode,
						EntryTimeoutSeconds: inode.EntryTimeoutSeconds,
						AttrTimeoutSeconds:  inode.AttrTimeoutSeconds,
					})
					pendingCursors = append(pendingCursors, pendingDirCursor{
						ino:              params.Ino,
						fh:               params.Fh,
						nextOffset:       nextOffset,
						lastName:         row.Name,
						sqlOffset:        sqlOffset + int64(i) + 1,
						dirMutated:       false,
						sessionID:        sid,
						handleCreatedAt:  handleCreatedAt,
						startMutationSeq: startMutationSeq,
					})
				}
			}
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrNotDir) {
			m.clearDirCursors(params.Ino, params.Fh)
		} else if errors.Is(err, ErrBadFileHandle) {
			var reqSid uint64
			if params.SessionID != nil {
				reqSid = *params.SessionID
			}
			m.clearDirCursorsForSession(params.Ino, params.Fh, reqSid)
		}
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNotDir):
			return nil, ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("readdirplus tx: %w", err)
		}
	}

	for _, pc := range pendingCursors {
		m.recordDirCursor(pc.ino, pc.fh, pc.nextOffset, pc.lastName, pc.sqlOffset, pc.dirMutated, pc.sessionID, pc.handleCreatedAt, pc.startMutationSeq)
	}

	if entries == nil {
		entries = []DirEntryPlus{}
	}
	return entries, nil
}

// validateXattrName 校验扩展属性名称合法性。
// len(name) == 0 或 len(name) > MaxXattrNameBytes 时返回 ErrRange；
// name 包含 NUL 字节 (\x00) 时返回 ErrInvalidArgument。
func validateXattrName(name []byte) error {
	if len(name) == 0 || len(name) > MaxXattrNameBytes {
		return ErrRange
	}
	if bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isAllowedUserXattrFileType 判定文件模式是否允许访问 user.* 命名空间扩展属性。
// 仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)。
func isAllowedUserXattrFileType(mode uint32) bool {
	fileType := mode & S_IFMT
	return fileType == S_IFREG || fileType == S_IFDIR || fileType == S_IFSOCK
}

// SetXattr 设置指定 Inode 的扩展属性。
// 遵循 Linux FUSE FUSE_SETXATTR 协议与 POSIX setxattr(2) 语义，执行 4 步求值状态机：
//  1. 入参静态校验（不查库）：校验 Ino、Flags、Name 与 Value 边界及非法字符；
//  2. 排他锁与存在性校验：读写事务内通过 GetInodeForUpdate 获取行锁，支持孤儿节点；
//  3. user.* 文件类型准入校验：针对 user.* 命名空间限制仅常规文件、目录与套接字，非 user.* 命名空间直接放行；
//  4. 属性存在性、Flags 状态机、配额校验、空值归一化与持久化：判断 XATTR_CREATE / XATTR_REPLACE 状态，
//     并在新增属性时执行 65536 字节名称列表配额拦截（覆盖不拦截），写入后刷新 Inode ctime。
func (m *manager) SetXattr(ctx context.Context, params SetXattrParams) error {
	// 第 1 步：入参静态校验（不查库）
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Flags&^(XattrCreate|XattrReplace) != 0 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(params.Name); err != nil {
		return err
	}
	if len(params.Value) > MaxXattrValueBytes {
		return ErrTooBig
	}

	// 开启读写事务
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：排他锁与存在性校验
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(params.Name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrPerm
		}

		// 第 4 步：属性存在性、flags 独立判定、配额与 Upsert
		_, err = q.GetInodeXattr(ctx, db.GetInodeXattrParams{
			Ino:  int64(params.Ino),
			Name: params.Name,
		})

		exists := true
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				exists = false
			} else {
				return fmt.Errorf("get inode xattr (ino=%d): %w", params.Ino, err)
			}
		}

		if exists {
			if (params.Flags & XattrCreate) != 0 {
				return ErrAlreadyExists
			}
		} else {
			if (params.Flags & XattrReplace) != 0 {
				return ErrNoData
			}

			// 仅当新增属性名时，查询该 Inode 现有全部属性名总长，校验 65536 字节配额
			totalBytes, err := q.GetInodeXattrNamesTotalBytes(ctx, int64(params.Ino))
			if err != nil {
				return fmt.Errorf("get inode xattr names total bytes %d: %w", params.Ino, err)
			}
			if totalBytes+int64(len(params.Name))+1 > MaxXattrListBytes {
				return ErrNoSpace
			}
		}

		// 若 value == nil，归一化为 []byte{}，防止 pgx 序列化为 SQL NULL
		val := params.Value
		if val == nil {
			val = []byte{}
		}

		// 写入数据库
		if err := q.UpsertInodeXattr(ctx, db.UpsertInodeXattrParams{
			Ino:   int64(params.Ino),
			Name:  params.Name,
			Value: val,
		}); err != nil {
			return fmt.Errorf("upsert inode xattr (ino=%d): %w", params.Ino, err)
		}

		// 刷新 Inode ctime
		if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
			return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrPerm):
			return ErrPerm
		case errors.Is(err, ErrAlreadyExists):
			return ErrAlreadyExists
		case errors.Is(err, ErrNoData):
			return ErrNoData
		case errors.Is(err, ErrNoSpace):
			return ErrNoSpace
		case errors.Is(err, ErrRange):
			return ErrRange
		case errors.Is(err, ErrTooBig):
			return ErrTooBig
		default:
			return fmt.Errorf("setxattr (ino=%d): %w", params.Ino, err)
		}
	}

	return nil
}

// GetXattr 获取指定 Inode 的扩展属性值。
// 遵循 Linux FUSE FUSE_GETXATTR 协议与 POSIX getxattr(2) 语义，执行 4 步求值状态机：
// 1. 入参静态校验（不查库）：校验 Ino 与 Name 边界及非法空字符；
// 2. 只读快照隔离与存在性校验：在 RepeatableRead 只读事务内通过 GetInode 查询节点，支持孤儿节点；
// 3. user.* 文件类型准入校验：针对 user.* 命名空间限制仅常规文件、目录与套接字，对其它非法文件类型直接返回 ErrNoData（对齐 Linux xattr_permission 读掩码行为），非 user.* 命名空间不限制；
// 4. 读取属性值：调用 GetInodeXattr 读取扩展属性值，未命中返回 ErrNoData，命中则原样返回 value（含 0 字节空切片与非 UTF-8 原始字节）。遵循 noatime 策略不刷新 atime。
func (m *manager) GetXattr(ctx context.Context, ino uint64, name []byte) ([]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return nil, err
	}

	var value []byte

	// 开启只读事务 (RepeatableRead)
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 存在性校验
		inode, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrNoData
		}

		// 第 4 步：读取属性值
		xattr, err := q.GetInodeXattr(ctx, db.GetInodeXattrParams{
			Ino:  int64(ino),
			Name: name,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNoData
			}
			return fmt.Errorf("get inode xattr (ino=%d): %w", ino, err)
		}

		value = xattr.Value
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrRange):
			return nil, ErrRange
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrNoData):
			return nil, ErrNoData
		default:
			return nil, fmt.Errorf("getxattr (ino=%d): %w", ino, err)
		}
	}

	return value, nil
}

// ListXattr 列出指定 Inode 的全部扩展属性名称列表。
// 遵循 Linux FUSE FUSE_LISTXATTR 协议与 POSIX listxattr(2) 语义，执行 3 步求值状态机：
// 1. 入参静态校验（不查库）：校验 Ino 必须大于 0 且在 int64 正数范围内 (<= math.MaxInt64)，否则返回 ErrInvalidArgument；
// 2. 只读快照隔离与 Inode 存在性校验：在 RepeatableRead 只读事务（tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead)）中调用 GetInode(ino)；若未命中返回 ErrNotFound；支持孤儿节点 (nlink == 0)；
// 3. 查询属性名列表：调用 ListInodeXattrNames(ino) 查询该节点绑定的全部属性名称；
//   - 结果按名称原始字节序升序（BYTEA ASC）返回；不按调用方过滤（对齐 Linux 内核 FUSE 行为）；
//   - 若无属性，返回空切片 [][]byte{}；
//   - 遵循 noatime 策略不刷新 atime。
func (m *manager) ListXattr(ctx context.Context, ino uint64) ([][]byte, error) {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	names := [][]byte{}

	// 开启只读事务 (RepeatableRead)
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 存在性校验
		_, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		// 第 3 步：查询属性名列表（按 BYTEA ASC 升序排列，不按命名空间过滤）
		items, err := q.ListInodeXattrNames(ctx, int64(ino))
		if err != nil {
			return fmt.Errorf("list inode xattr names (ino=%d): %w", ino, err)
		}

		if len(items) > 0 {
			names = items
		}
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		default:
			return nil, fmt.Errorf("listxattr (ino=%d): %w", ino, err)
		}
	}

	return names, nil
}

// RemoveXattr 移除指定 Inode 的扩展属性。
// 遵循 Linux FUSE FUSE_REMOVEXATTR 协议与 POSIX removexattr(2) 语义，执行 4 步求值状态机：
// 1. 入参静态校验（不查库）：
//   - ino == 0 || ino > math.MaxInt64 返回 ErrInvalidArgument；
//   - len(name) == 0 || len(name) > MaxXattrNameBytes 返回 ErrRange；
//   - bytes.IndexByte(name, 0) >= 0 返回 ErrInvalidArgument；
//
// 2. Inode 行级排他锁与存在性校验：在读写事务内调用 GetInodeForUpdate(ino)；未命中返回 ErrNotFound；支持孤儿节点 (nlink == 0)；
// 3. user.* 文件类型准入校验：若以 user. 为前缀，仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)；对符号链接、字符设备、块设备、FIFO 返回 ErrPerm（对齐 Linux xattr_permission 写掩码行为）；非 user.* 命名空间不限制；
// 4. 删除属性与刷新 ctime：调用 DeleteInodeXattr(ino, name)；若受影响行数为 0 返回 ErrNoData；删除成功后调用 TouchInodeCtime(ino) 刷新 ctime（使用 clock_timestamp()，不修改 mtime 与 atime）。
func (m *manager) RemoveXattr(ctx context.Context, ino uint64, name []byte) error {
	// 第 1 步：入参静态校验（不查库）
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		// 第 2 步：Inode 行级排他锁与存在性校验
		inode, err := q.GetInodeForUpdate(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", ino, err)
		}

		// 第 3 步：user.* 文件类型准入校验
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(uint32(inode.Mode)) {
			return ErrPerm
		}

		// 第 4 步：删除属性与刷新 ctime
		rowsAffected, err := q.DeleteInodeXattr(ctx, db.DeleteInodeXattrParams{
			Ino:  int64(ino),
			Name: name,
		})
		if err != nil {
			return fmt.Errorf("delete inode xattr (ino=%d): %w", ino, err)
		}
		if rowsAffected == 0 {
			return ErrNoData
		}

		if err := q.TouchInodeCtime(ctx, int64(ino)); err != nil {
			return fmt.Errorf("touch inode ctime %d: %w", ino, err)
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrPerm):
			return ErrPerm
		case errors.Is(err, ErrNoData):
			return ErrNoData
		case errors.Is(err, ErrRange):
			return ErrRange
		default:
			return fmt.Errorf("removexattr (ino=%d): %w", ino, err)
		}
	}

	return nil
}
