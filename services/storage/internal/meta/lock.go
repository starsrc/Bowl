package meta

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// GetLk 执行 FUSE_GETLK (31) 冲突锁探测操作。
// 遵循 ADR-0012：
// 1. 静态校验参数合法性（拒绝 F_UNLCK、start > end、非法 lk_flags 及非整文件区间 flock）；
// 2. 在只读事务中校验目标 Inode 存在且非目录（目录返回 ErrIsDir）、校验 FileHandle 存在且归属目标 Inode（不限制读写打开模式位，支持 Orphan Inode）；
// 3. 过滤已超期的 client_sessions 租约，在目标命名空间（lk_flags == 0 排除同一 (session_id, lock_owner)，lk_flags == FuseLkFlock 排除同一 (session_id, fh)）内按确定性顺序检索首个冲突锁；
// 4. 若无冲突则返回 {Start: req.Start, End: req.End, Type: F_UNLCK, PID: 0}，不修改 Inode 时间戳。
func (m *manager) GetLk(ctx context.Context, params GetLkParams) (*FileLock, error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.SessionID == 0 || params.SessionID > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if params.LkFlags > FuseLkFlock {
		return nil, ErrInvalidArgument
	}
	if params.Lk.Type != F_RDLCK && params.Lk.Type != F_WRLCK {
		return nil, ErrInvalidArgument
	}
	if params.Lk.Start > OffsetMax || params.Lk.End > OffsetMax || params.Lk.Start > params.Lk.End {
		return nil, ErrInvalidArgument
	}
	if params.LkFlags == FuseLkFlock && (params.Lk.Start != 0 || params.Lk.End != OffsetMax) {
		return nil, ErrInvalidArgument
	}

	var result *FileLock
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInode(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:  int64(params.Fh),
			Ino: int64(params.Ino),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		conflict, err := q.GetFirstConflictingFileLock(ctx, db.GetFirstConflictingFileLockParams{
			Ino:         int64(params.Ino),
			LkFlags:     int32(params.LkFlags),
			StartOffset: int64(params.Lk.Start),
			EndOffset:   int64(params.Lk.End),
			ReqType:     int16(params.Lk.Type),
			SessionID:   int64(params.SessionID),
			LockOwner:   int64(params.LockOwner),
			Fh:          int64(params.Fh),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				result = &FileLock{
					Start: params.Lk.Start,
					End:   params.Lk.End,
					Type:  F_UNLCK,
					PID:   0,
				}
				return nil
			}
			return fmt.Errorf("get first conflicting file lock (ino=%d): %w", params.Ino, err)
		}

		result = &FileLock{
			Start: uint64(conflict.StartOffset),
			End:   uint64(conflict.EndOffset),
			Type:  uint32(conflict.LockType),
			PID:   uint32(conflict.Pid),
		}
		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("getlk tx: %w", err)
		}
	}

	return result, nil
}
