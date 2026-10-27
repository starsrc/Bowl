package meta

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// SetAttr 修改指定 Inode 的属性并实现读写事务悲观锁更新、POSIX 约束检查与时间戳联动。
func (m *manager) SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error) {
	// 事务前入参边界防御
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// 至少包含一项有效待更新属性
	if input.Mode == nil && input.UID == nil && input.GID == nil && input.Size == nil &&
		input.Atime == nil && input.Mtime == nil && !input.AtimeNow && !input.MtimeNow &&
		!input.CtimeNow && input.Flags == nil {
		return nil, ErrInvalidArgument
	}

	// 可选文件句柄与客户端会话标识静态边界防御
	if input.Fh != nil && (*input.Fh == 0 || *input.Fh > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(input.SessionID); err != nil {
		return nil, err
	}

	// 时间戳策略互斥防御
	if (input.Atime != nil && input.AtimeNow) || (input.Mtime != nil && input.MtimeNow) {
		return nil, ErrInvalidArgument
	}

	// Mode 范围边界防御（不超过 16 位 0o177777 = 65535）
	if input.Mode != nil && *input.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	// UID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.UID != nil && *input.UID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// GID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.GID != nil && *input.GID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// Size 范围边界防御（防止转换为 PostgreSQL BIGINT 符号位溢出）
	if input.Size != nil && *input.Size > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// Flags 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.Flags != nil && *input.Flags > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	var result *Inode
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		current, err := q.GetInodeForUpdate(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", ino, err)
		}

		currentMode := uint32(current.Mode)
		fileType := currentMode & S_IFMT

		// POSIX 文件类型与 Size 约束
		if input.Size != nil {
			if fileType == S_IFDIR {
				return ErrIsDir
			}
			if fileType != S_IFREG {
				return ErrInvalidArgument
			}
		}

		// 可选文件句柄归属校验与 ftruncate 写权限强制检查
		var handleCreatedAt pgtype.Timestamptz
		if input.Fh != nil {
			handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
				Fh:        int64(*input.Fh),
				Ino:       int64(ino),
				SessionID: toPgSessionID(input.SessionID),
			})
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrBadFileHandle
				}
				return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", *input.Fh, ino, err)
			}
			handleCreatedAt = handle.CreatedAt
			if input.Size != nil {
				accMode := uint32(handle.Flags) & O_ACCMODE
				if accMode != O_WRONLY && accMode != O_RDWR {
					return ErrBadFileHandle
				}
			}
		}

		if input.SessionID != nil {
			if err := m.touchActiveClientSessionForHandle(ctx, q, *input.SessionID, handleCreatedAt); err != nil {
				return err
			}
		}

		// POSIX Mode 篡改防御、文件类型保留与特权位剥离
		targetMode := currentMode
		if input.Mode != nil {
			if fileType == S_IFLNK {
				return ErrInvalidArgument
			}
			inputType := *input.Mode & S_IFMT
			if inputType != 0 && inputType != fileType {
				return ErrInvalidArgument
			}
			targetMode = fileType | (*input.Mode & PermMask)
		} else if fileType == S_IFREG && (input.UID != nil || input.GID != nil || input.Size != nil) {
			// 未显式指定 mode 时，常规文件修改 uid/gid/size 自动剥离 S_ISUID 和 S_ISGID
			targetMode &^= (S_ISUID | S_ISGID)
		}

		targetUID := uint32(current.Uid)
		if input.UID != nil {
			targetUID = *input.UID
		}

		targetGID := uint32(current.Gid)
		if input.GID != nil {
			targetGID = *input.GID
		}

		targetSize := uint64(current.Size)
		if input.Size != nil {
			targetSize = *input.Size
		}

		// 容量配额校验与联动更新（仅针对常规文件且关联有效 Disk）
		if input.Size != nil && fileType == S_IFREG && current.RootIno > 0 {
			oldSize := uint64(current.Size)
			if targetSize > oldSize {
				deltaBytes := targetSize - oldSize
				diskRow, err := q.GetDiskByRootInoForUpdate(ctx, current.RootIno)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						// 说明该 root_ino 未绑定 Disk（如裸 Inode 测试场景），直接跳过配额检查与更新
					} else {
						return fmt.Errorf("get disk by root_ino %d for update: %w", current.RootIno, err)
					}
				} else {
					if uint64(diskRow.UsedBytes) > uint64(diskRow.MaxBytes) || deltaBytes > uint64(diskRow.MaxBytes)-uint64(diskRow.UsedBytes) {
						return ErrNoSpace
					}
					if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
						RootIno:     current.RootIno,
						DeltaBytes:  int64(deltaBytes),
						DeltaInodes: 0,
					}); err != nil {
						return fmt.Errorf("add disk quota by root_ino %d: %w", current.RootIno, err)
					}
				}
			} else if targetSize < oldSize {
				deltaBytes := int64(targetSize) - int64(oldSize)
				if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
					RootIno:     current.RootIno,
					DeltaBytes:  deltaBytes,
					DeltaInodes: 0,
				}); err != nil {
					return fmt.Errorf("add disk quota by root_ino %d: %w", current.RootIno, err)
				}
			}
		}

		var setAtimeNow bool
		var atime pgtype.Timestamptz
		if input.AtimeNow {
			setAtimeNow = true
		} else if input.Atime != nil {
			atime = pgtype.Timestamptz{Time: *input.Atime, Valid: true}
		} else {
			atime = current.Atime
		}

		var setMtimeNow bool
		var mtime pgtype.Timestamptz
		if input.MtimeNow || (input.Size != nil && input.Mtime == nil) {
			setMtimeNow = true
		} else if input.Mtime != nil {
			mtime = pgtype.Timestamptz{Time: *input.Mtime, Valid: true}
		} else {
			mtime = current.Mtime
		}

		targetFlags := current.Flags
		if input.Flags != nil {
			targetFlags = int32(*input.Flags)
		}

		updated, err := q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
			Ino:         int64(ino),
			Mode:        int32(targetMode),
			Size:        int64(targetSize),
			Uid:         int32(targetUID),
			Gid:         int32(targetGID),
			SetAtimeNow: setAtimeNow,
			Atime:       atime,
			SetMtimeNow: setMtimeNow,
			Mtime:       mtime,
			Flags:       targetFlags,
		})
		if err != nil {
			return fmt.Errorf("update inode attr %d: %w", ino, err)
		}

		result = m.newInode(updated)
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			if errors.Is(err, errExpiredSessionBadHandle) && input.SessionID != nil {
				m.cleanExpiredSessionByID(ctx, *input.SessionID)
			}
			return nil, ErrBadFileHandle
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNoSpace):
			return nil, ErrNoSpace
		default:
			return nil, fmt.Errorf("setattr tx: %w", err)
		}
	}

	return result, nil
}

// Fallocate 为常规文件预分配物理空间或打洞去分配空间（对接 Linux FUSE FUSE_FALLOCATE 协议）。
// 遵循 Linux fallocate(2) 与 ADR-0010 规范。
func (m *manager) Fallocate(ctx context.Context, params FallocateParams) error {
	// 1. 入参边界与整型溢出防御
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Length == 0 || params.Length > math.MaxInt64 ||
		params.Offset > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Offset > uint64(math.MaxInt64)-params.Length {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	// 2. 模式位校验
	if params.Mode == FallocFlPunchHole {
		return ErrInvalidArgument
	}
	if params.Mode != 0 && params.Mode != FallocFlKeepSize && params.Mode != (FallocFlPunchHole|FallocFlKeepSize) {
		return ErrNotSupported
	}

	// 3. 开启读写事务
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		current, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		fileType := uint32(current.Mode) & S_IFMT
		if fileType == S_IFDIR {
			return ErrIsDir
		}
		if fileType != S_IFREG {
			return ErrNotSupported
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(params.Fh),
			Ino:       int64(params.Ino),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		accMode := uint32(handle.Flags) & O_ACCMODE
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if params.SessionID != nil {
			if err := m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, handle.CreatedAt); err != nil {
				return err
			}
		}

		switch params.Mode {
		case 0:
			targetSize := params.Offset + params.Length
			if targetSize > uint64(current.Size) {
				// 若关联了 Disk 根节点，进行容量配额校验与递增
				if current.RootIno > 0 {
					deltaBytes := targetSize - uint64(current.Size)
					diskRow, err := q.GetDiskByRootInoForUpdate(ctx, current.RootIno)
					if err != nil {
						if !errors.Is(err, pgx.ErrNoRows) {
							return fmt.Errorf("get disk by root_ino %d for update: %w", current.RootIno, err)
						}
					} else {
						if uint64(diskRow.UsedBytes) > uint64(diskRow.MaxBytes) || deltaBytes > uint64(diskRow.MaxBytes)-uint64(diskRow.UsedBytes) {
							return ErrNoSpace
						}
						if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
							RootIno:     current.RootIno,
							DeltaBytes:  int64(deltaBytes),
							DeltaInodes: 0,
						}); err != nil {
							return fmt.Errorf("add disk quota by root_ino %d: %w", current.RootIno, err)
						}
					}
				}

				_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
					Ino:         current.Ino,
					Mode:        current.Mode,
					Size:        int64(targetSize),
					Uid:         current.Uid,
					Gid:         current.Gid,
					SetAtimeNow: false,
					Atime:       current.Atime,
					SetMtimeNow: true,
					Mtime:       pgtype.Timestamptz{},
					Flags:       current.Flags,
				})
				if err != nil {
					return fmt.Errorf("update inode attr %d: %w", current.Ino, err)
				}
			} else {
				if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
					return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
				}
			}
		case FallocFlKeepSize:
			if err := q.TouchInodeCtime(ctx, int64(params.Ino)); err != nil {
				return fmt.Errorf("touch inode ctime %d: %w", params.Ino, err)
			}
		case FallocFlPunchHole | FallocFlKeepSize:
			newMode := uint32(current.Mode) &^ (S_ISUID | S_ISGID)
			_, err = q.UpdateInodeAttr(ctx, db.UpdateInodeAttrParams{
				Ino:         current.Ino,
				Mode:        int32(newMode),
				Size:        current.Size,
				Uid:         current.Uid,
				Gid:         current.Gid,
				SetAtimeNow: false,
				Atime:       current.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
				Flags:       current.Flags,
			})
			if err != nil {
				return fmt.Errorf("update inode attr %d: %w", current.Ino, err)
			}
		}

		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrNotSupported):
			return ErrNotSupported
		case errors.Is(err, ErrBadFileHandle):
			if errors.Is(err, errExpiredSessionBadHandle) && params.SessionID != nil {
				m.cleanExpiredSessionByID(ctx, *params.SessionID)
			}
			return ErrBadFileHandle
		case errors.Is(err, ErrNoSpace):
			return ErrNoSpace
		default:
			return fmt.Errorf("fallocate (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
		}
	}

	return nil
}
