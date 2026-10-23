package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// checkFileHandleReadOnly 在只读 RepeatableRead 事务内校验 Inode 与 FileHandle 归属有效性。
func (m *manager) checkFileHandleReadOnly(ctx context.Context, ino uint64, fh uint64, sessionID *uint64, opName string) error {
	if ino == 0 || ino > math.MaxInt64 || fh == 0 || fh > math.MaxInt64 {
		return ErrInvalidArgument
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInode(ctx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(fh),
			Ino:       int64(ino),
			SessionID: toPgSessionID(sessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", fh, ino, err)
		}

		return nil
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("%s tx: %w", opName, err)
		}
	}

	return nil
}

// Flush 冲刷会话句柄并校验合法性（不销毁文件句柄）。
// 遵循 ADR-0012：
// 1. 当未传入 LockOwner 与 SessionID（LockOwner == nil && SessionID == nil）时，保持只读事务校验 (ino, fh) 合法性；
// 2. 当传入 LockOwner 或 SessionID 时，在 READ COMMITTED 写事务中执行 GetInodeForUpdate(ino)、校验 (ino, fh) 有效后，
//    若携带 SessionID 则续期会话并触发孤儿节点兜底回收；若携带 LockOwner 则原子删除该 (session_id, ino, *lock_owner)（若未传 session_id 则按 (ino, *lock_owner)）持有的全部 POSIX 记录锁（lk_flags == 0），
//    无论这些锁最初是通过哪个 FileHandle 申请或合并的；Flush 不销毁 FileHandle，且绝不清理 BSD flock 锁。
func (m *manager) Flush(ctx context.Context, params FlushParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	if params.LockOwner == nil && params.SessionID == nil {
		return m.checkFileHandleReadOnly(ctx, params.Ino, params.Fh, nil, "flush")
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.GetFileHandle(ctx, db.GetFileHandleParams{
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

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if params.LockOwner != nil {
			if params.SessionID != nil {
				_, err = q.DeletePosixFileLocksBySessionAndOwner(ctx, db.DeletePosixFileLocksBySessionAndOwnerParams{
					SessionID: int64(*params.SessionID),
					Ino:       int64(params.Ino),
					LockOwner: int64(*params.LockOwner),
				})
				if err != nil {
					return fmt.Errorf("delete posix file locks by session and owner (session_id=%d, ino=%d, lock_owner=%d): %w", *params.SessionID, params.Ino, *params.LockOwner, err)
				}
			} else {
				_, err = q.DeletePosixFileLocksByOwner(ctx, db.DeletePosixFileLocksByOwnerParams{
					Ino:       int64(params.Ino),
					LockOwner: int64(*params.LockOwner),
				})
				if err != nil {
					return fmt.Errorf("delete posix file locks by owner (ino=%d, lock_owner=%d): %w", params.Ino, *params.LockOwner, err)
				}
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
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("flush tx: %w", err)
		}
	}

	if params.LockOwner != nil {
		m.notifyLockWaiters(params.Ino)
	}
	return nil
}

// Fsync 同步文件节点与句柄关联的状态。
// 注：params.Datasync 保留用于 Linux FUSE / POSIX 协议契约对齐，当前元数据在 PostgreSQL 事务提交时已完成持久化。
func (m *manager) Fsync(ctx context.Context, params FsyncParams) error {
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	return m.checkFileHandleReadOnly(ctx, params.Ino, params.Fh, params.SessionID, "fsync")
}

// Release 关闭并销毁文件句柄，按标志位清理 BSD flock 句柄锁，且在孤儿节点无活跃句柄时执行最终物理回收。
// 遵循 ADR-0012：
// 1. 静态校验 ino、fh、release_flags（允许 0、FuseReleaseFlush、FuseReleaseFlockUnlock 及其组合）与可选 session_id 范围；
// 2. 在 READ COMMITTED 写事务中锁定目标 Inode 并删除 file_handles(fh)；
// 3. 若传入 session_id，续期客户端会话并执行无引用孤儿节点兜底回收；仅当 (release_flags & FuseReleaseFlockUnlock) != 0 时，原子删除该 (session_id, ino, fh)（未传 session_id 时按 (ino, fh)）绑定的 BSD flock 锁（lk_flags == FuseLkFlock），绝不越权清理 POSIX 记录锁；
// 4. 若 nlink == 0 且无剩余活跃句柄，执行孤儿节点物理回收（数据库 ON DELETE CASCADE 自动清理残留锁）。
func (m *manager) Release(ctx context.Context, params ReleaseParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if (params.ReleaseFlags &^ (FuseReleaseFlush | FuseReleaseFlockUnlock)) != 0 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		_, err = q.DeleteFileHandle(ctx, db.DeleteFileHandleParams{
			Fh:        int64(params.Fh),
			Ino:       int64(params.Ino),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("delete file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if (params.ReleaseFlags & FuseReleaseFlockUnlock) != 0 {
			if params.SessionID != nil {
				_, err = q.DeleteFlockFileLocksBySessionAndFh(ctx, db.DeleteFlockFileLocksBySessionAndFhParams{
					SessionID: int64(*params.SessionID),
					Ino:       int64(params.Ino),
					Fh:        int64(params.Fh),
				})
				if err != nil {
					return fmt.Errorf("delete flock file locks by session and fh (session_id=%d, ino=%d, fh=%d): %w", *params.SessionID, params.Ino, params.Fh, err)
				}
			} else {
				_, err = q.DeleteFlockFileLocksByFh(ctx, db.DeleteFlockFileLocksByFhParams{
					Ino: int64(params.Ino),
					Fh:  int64(params.Fh),
				})
				if err != nil {
					return fmt.Errorf("delete flock file locks by fh (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
				}
			}
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, params.Ino); err != nil {
				return err
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
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("release tx: %w", err)
		}
	}

	if (params.ReleaseFlags & FuseReleaseFlockUnlock) != 0 {
		m.notifyLockWaiters(params.Ino)
	}
	return nil
}

// FsyncDir 同步目录节点与目录句柄关联的状态（对接 Linux FUSE FUSE_FSYNCDIR = 30 协议）。
// 注：params.Datasync 保留用于 Linux FUSE / POSIX 协议契约对齐，当前元数据在 PostgreSQL 事务提交时已完成持久化。
func (m *manager) FsyncDir(ctx context.Context, params FsyncDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		_, err := m.validateDirHandle(ctx, q, params.Ino, params.Fh, params.SessionID)
		return err
	}, tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("fsyncdir tx: %w", err)
		}
	}

	return nil
}

// ReleaseDir 关闭并销毁目录句柄，且在孤儿目录无活跃句柄时执行最终物理回收。
func (m *manager) ReleaseDir(ctx context.Context, params ReleaseDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		_, err = q.DeleteFileHandle(ctx, db.DeleteFileHandleParams{
			Fh:        int64(params.Fh),
			Ino:       int64(params.Ino),
			SessionID: toPgSessionID(params.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("delete file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if err := m.renewOptionalClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, params.Ino); err != nil {
				return err
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
		case errors.Is(err, ErrNotDir):
			return ErrNotDir
		case errors.Is(err, ErrBadFileHandle):
			return ErrBadFileHandle
		default:
			return fmt.Errorf("releasedir tx: %w", err)
		}
	}

	return nil
}

// RenewSession 显式延长客户端会话租约有效期并顺带回收孤儿节点。
// 遵循 ADR-0013：
// 1. 静态校验入参合法性（sessionID == 0 或 > math.MaxInt64 返回 ErrInvalidArgument / EINVAL）；
// 2. 在读写事务中执行 m.renewClientSession(ctx, q, sessionID)；
// 3. 自动清理超期会话、更新当前会话租约、通过 FOR UPDATE SKIP LOCKED 非阻塞物理回收无有效句柄引用的 Orphan Inode；
// 4. 成功时返回刷新后的租约绝对截止时间 expires_at。
func (m *manager) RenewSession(ctx context.Context, sessionID uint64) (time.Time, error) {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return time.Time{}, ErrInvalidArgument
	}

	var expiresAt time.Time
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		exp, err := m.renewClientSession(ctx, q, sessionID)
		if err != nil {
			return err
		}
		expiresAt = exp
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrInvalidArgument) {
			return time.Time{}, ErrInvalidArgument
		}
		return time.Time{}, fmt.Errorf("renew session tx: %w", err)
	}

	return expiresAt, nil
}
