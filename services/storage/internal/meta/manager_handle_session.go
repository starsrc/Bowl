package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

var managerPosixLockInodes sync.Map

// RecordPosixLockActivity 记录指定 Inode 曾申请过 POSIX 记录锁（lk_flags == 0）。
func (m *manager) RecordPosixLockActivity(ino uint64) {
	if m == nil || ino == 0 {
		return
	}
	v, ok := managerPosixLockInodes.Load(m)
	if !ok {
		actual, _ := managerPosixLockInodes.LoadOrStore(m, &sync.Map{})
		v = actual
	}
	v.(*sync.Map).Store(ino, true)
}

func (m *manager) hasPosixLockActivity(ino uint64) bool {
	if m == nil || ino == 0 {
		return false
	}
	v, ok := managerPosixLockInodes.Load(m)
	if !ok {
		return false
	}
	_, exists := v.(*sync.Map).Load(ino)
	return exists
}

// RecordPosixLockActivityForTest 仅供单测标记指定 Inode 已存在 POSIX 记录锁活动。
func RecordPosixLockActivityForTest(mgr Manager, ino uint64) {
	if m, ok := mgr.(*manager); ok {
		m.RecordPosixLockActivity(ino)
	}
}

func (m *manager) tryThrottledSessionTouchReadOnly(ctx context.Context, sessionID uint64, handleCreatedAt pgtype.Timestamptz) (bool, error) {
	pp, throttle := m.shouldThrottleSessionTouch(ctx)
	if !throttle || pp == nil || pp.Pool() == nil {
		return false, nil
	}
	interval := m.writeSessionTouchInterval
	if interval <= 0 {
		interval = DefaultWriteSessionTouchInterval
	}
	now := time.Now()
	m.writeSessionTouchMu.Lock()
	if m.writeSessionTouches == nil {
		m.writeSessionTouches = make(map[uint64]writeSessionTouchState)
	}
	prev, hadPrev := m.writeSessionTouches[sessionID]
	inWindow := interval > 0 && hadPrev && !prev.lastTouchedAt.IsZero() && now.Sub(prev.lastTouchedAt) < interval
	m.writeSessionTouchMu.Unlock()
	if !inWindow {
		return false, nil
	}
	if handleCreatedAt.Valid && prev.sessionCreatedAt.Valid && handleCreatedAt.Time.Before(prev.sessionCreatedAt.Time) {
		return true, ErrBadFileHandle
	}
	if !m.hasDeferredOrphans() {
		return true, nil
	}

	var sessionCreatedAt pgtype.Timestamptz
	var alive, hasPendingCleanup bool
	err := pp.Pool().QueryRow(ctx, `
		SELECT
			cs.created_at,
			(cs.expires_at > clock_timestamp()) AS alive,
			(
				EXISTS (SELECT 1 FROM client_sessions e WHERE e.expires_at <= clock_timestamp())
				OR EXISTS (
					SELECT 1 FROM inodes i
					WHERE i.nlink = 0
					  AND NOT EXISTS (SELECT 1 FROM file_handles fh WHERE fh.ino = i.ino)
				)
			) AS has_pending_cleanup
		FROM client_sessions cs
		WHERE cs.session_id = $1
	`, int64(sessionID)).Scan(&sessionCreatedAt, &alive, &hasPendingCleanup)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			m.clearWriteSessionTouch(sessionID)
			return true, errExpiredSessionBadHandle
		}
		return true, fmt.Errorf("check active client session %d: %w", sessionID, err)
	}
	if !alive {
		m.clearWriteSessionTouch(sessionID)
		return true, errExpiredSessionBadHandle
	}
	if handleCreatedAt.Valid && sessionCreatedAt.Valid && handleCreatedAt.Time.Before(sessionCreatedAt.Time) {
		m.recordWriteSessionTouch(sessionID, time.Time{}, sessionCreatedAt)
		return true, ErrBadFileHandle
	}
	if !hasPendingCleanup {
		m.recordWriteSessionTouch(sessionID, time.Time{}, sessionCreatedAt)
		return true, nil
	}
	return false, nil
}

func (m *manager) canSkipHandleSessionTouchInWindow(ctx context.Context, sessionID uint64) bool {
	if _, throttle := m.shouldThrottleSessionTouch(ctx); !throttle {
		return false
	}
	if m.hasDeferredOrphans() {
		return false
	}
	interval := m.writeSessionTouchInterval
	if interval <= 0 {
		interval = DefaultWriteSessionTouchInterval
	}
	now := time.Now()
	m.writeSessionTouchMu.Lock()
	prev, hadPrev := m.writeSessionTouches[sessionID]
	inWindow := interval > 0 && hadPrev && !prev.lastTouchedAt.IsZero() && now.Sub(prev.lastTouchedAt) < interval
	m.writeSessionTouchMu.Unlock()
	return inWindow
}

func (m *manager) lookupHandleCreatedAt(ctx context.Context, q db.Querier, fh, ino, sessionID uint64) pgtype.Timestamptz {
	pp, _ := m.store.(poolProvider)
	dbtx := queryDBTX(q, pp)
	if dbtx == nil {
		return pgtype.Timestamptz{}
	}
	var createdAt pgtype.Timestamptz
	_ = dbtx.QueryRow(
		ctx,
		`SELECT created_at FROM file_handles WHERE fh = $1 AND ino = $2 AND session_id = $3`,
		int64(fh), int64(ino), int64(sessionID),
	).Scan(&createdAt)
	return createdAt
}

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
// 遵循 ADR-0012 / ADR-0023：
//  1. 当未传入 LockOwner 与 SessionID（LockOwner == nil && SessionID == nil）时，保持只读事务校验 (ino, fh) 合法性；
//  2. 当目标 Inode 未持有任何 POSIX 记录锁时，免除 GetInodeForUpdate 排他锁与 DeletePosixFileLocks* 写 SQL，仅校验节点、句柄归属与（节流后的）会话有效性；
//  3. 当目标 Inode 持有 POSIX 记录锁且传入 LockOwner 时，在 READ COMMITTED 写事务中执行 GetInodeForUpdate(ino) 并原子删除该 (session_id, ino, *lock_owner)（若未传 session_id 则按 (ino, *lock_owner)）持有的全部 POSIX 记录锁（lk_flags == 0）；
//     Flush 不销毁 FileHandle，且绝不清理 BSD flock 锁。
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

	needsPosixUnlock := false
	pp, hasPool := m.store.(poolProvider)
	hasActivePool := hasPool && pp != nil && pp.Pool() != nil

	if params.LockOwner != nil && m.hasPosixLockActivity(params.Ino) {
		needsPosixUnlock = true
	}
	var snapshot flushSnapshot
	if hasActivePool && !needsPosixUnlock {
		var err error
		snapshot, err = queryFlushSnapshot(ctx, pp.Pool(), params)
		if err != nil {
			return err
		}
		if snapshot.hasPosixLocks {
			m.RecordPosixLockActivity(params.Ino)
			needsPosixUnlock = true
		}
	}

	if !needsPosixUnlock {
		if hasActivePool {
			if (uint32(snapshot.mode) & S_IFMT) == S_IFDIR {
				return ErrIsDir
			}

			if !snapshot.hasHandle {
				m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, ErrBadFileHandle)
				return ErrBadFileHandle
			}

			if params.SessionID != nil {
				handled, touchErr := m.tryThrottledSessionTouchReadOnly(ctx, *params.SessionID, snapshot.handleCreatedAt)
				if handled {
					if touchErr != nil {
						if errors.Is(touchErr, ErrBadFileHandle) {
							m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, touchErr)
							return ErrBadFileHandle
						}
						return fmt.Errorf("flush tx: %w", touchErr)
					}
					return nil
				}

				err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
					return m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, snapshot.handleCreatedAt)
				})
				if err != nil {
					if errors.Is(err, ErrBadFileHandle) {
						m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, err)
						return ErrBadFileHandle
					}
					return fmt.Errorf("flush tx: %w", err)
				}
			}
			return nil
		}

		err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
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

			if params.SessionID != nil {
				if err := m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, handle.CreatedAt); err != nil {
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
				m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, err)
				return ErrBadFileHandle
			default:
				return fmt.Errorf("flush tx: %w", err)
			}
		}
		return nil
	}

	var lastUndo *inFlightTxUndo
	var committed bool
	defer func() {
		if !committed && lastUndo != nil {
			lastUndo.Rollback()
		}
	}()

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if lastUndo != nil {
			lastUndo.Rollback()
		}
		currentUndo := newInFlightTxUndo(m, params.Ino)
		lastUndo = currentUndo

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

		if params.SessionID != nil {
			if err := m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, handle.CreatedAt); err != nil {
				return err
			}
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
			m.markInFlightGrantsFlushedPOSIX(params.Ino, params.SessionID, *params.LockOwner, currentUndo)
		}

		return nil
	})
	if err == nil {
		committed = true
		if lastUndo != nil {
			lastUndo.Discard()
		}
	} else {
		if lastUndo != nil {
			lastUndo.Rollback()
		}
	}

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, err)
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

// flushDurabilityBarrier 按「先刷物理数据块，后刷 PostgreSQL WAL」的顺序触发物理落盘屏障。
func (m *manager) flushDurabilityBarrier(ctx context.Context) error {
	if stagedBS, ok := m.getBlockStore().(StagedBlockStore); ok && stagedBS != nil {
		if err := stagedBS.SyncPending(ctx); err != nil {
			if errors.Is(err, ErrIO) {
				return err
			}
			return fmt.Errorf("%w: sync pending blocks: %v", ErrIO, err)
		}
	}
	if pp, ok := m.store.(poolProvider); ok && pp != nil && pp.Pool() != nil {
		var lsn string
		if err := pp.Pool().QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()::text").Scan(&lsn); err != nil {
			return fmt.Errorf("%w: flush wal: %v", ErrIO, err)
		}
	}
	return nil
}

// Fsync 同步文件节点与句柄关联的状态，并触发块存储与数据库 WAL 物理落盘屏障。
func (m *manager) Fsync(ctx context.Context, params FsyncParams) error {
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	if err := m.checkFileHandleReadOnly(ctx, params.Ino, params.Fh, params.SessionID, "fsync"); err != nil {
		return err
	}
	return m.flushDurabilityBarrier(ctx)
}

// Release 关闭并销毁文件句柄，按标志位清理 BSD flock 句柄锁，且在孤儿节点无活跃句柄时执行最终物理回收。
// 遵循 ADR-0012 / ADR-0023：
// 1. 静态校验 ino、fh、release_flags（允许 0、FuseReleaseFlush、FuseReleaseFlockUnlock 及其组合）与可选 session_id 范围；
// 2. 对正常未解链节点（nlink > 0）且未置位 FuseReleaseFlockUnlock 时，先通过 GetInode 无锁校验节点存在与类型，免除 GetInodeForUpdate 排他锁；
// 3. 优先核验/续期会话并删除 file_handles(fh)；仅当 (release_flags & FuseReleaseFlockUnlock) != 0 时，锁定 Inode 并原子删除该 (session_id, ino, fh)（未传 session_id 时按 (ino, fh)）绑定的 BSD flock 锁（lk_flags == FuseLkFlock），绝不越权清理 POSIX 记录锁；
// 4. 若 nlink == 0（含并发 Unlink/Rename 导致 nlink 归零）且无剩余活跃句柄，在同一事务内对 Inode 加排他锁并执行孤儿节点物理回收与配额退还。
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

	var lastUndo *inFlightTxUndo
	var committed bool
	defer func() {
		if !committed && lastUndo != nil {
			lastUndo.Rollback()
		}
	}()

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if lastUndo != nil {
			lastUndo.Rollback()
		}
		currentUndo := newInFlightTxUndo(m, params.Ino)
		lastUndo = currentUndo

		var (
			inode       db.Inode
			err         error
			lockedInode bool
		)
		if (params.ReleaseFlags&FuseReleaseFlockUnlock) != 0 || m.hasPosixLockActivity(params.Ino) {
			inode, err = q.GetInodeForUpdate(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
			}
			lockedInode = true
		} else {
			inode, err = q.GetInode(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode %d: %w", params.Ino, err)
			}
			if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
				return ErrIsDir
			}
			if inode.Nlink == 0 {
				inode, err = q.GetInodeForUpdate(ctx, int64(params.Ino))
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return ErrNotFound
					}
					return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
				}
				lockedInode = true
			}
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		// 1. 校验会话有效性并原子续期（在 5s 内存节流窗口且无延迟孤儿时直接交由 DeleteFileHandle 内联校验；慢路径则先续期防止被 reclaimUnreferencedOrphans 抢先回收）
		if params.SessionID != nil && !m.canSkipHandleSessionTouchInWindow(ctx, *params.SessionID) {
			handleCreatedAt := m.lookupHandleCreatedAt(ctx, q, params.Fh, params.Ino, *params.SessionID)
			if err := m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, handleCreatedAt); err != nil {
				return err
			}
		}

		// 2. 删除文件句柄（若句柄不存在直接返回 ErrBadFileHandle）
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
			m.markInFlightGrantsFlushedOrReleased(params.Ino, params.SessionID, params.Fh, currentUndo)
		}

		if !lockedInode {
			if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r.RawDBTX() != nil {
				var curNlink int32
				if err := r.RawDBTX().QueryRow(ctx, `SELECT nlink FROM inodes WHERE ino = $1`, int64(params.Ino)).Scan(&curNlink); err == nil && curNlink == 0 {
					inode, err = q.GetInodeForUpdate(ctx, int64(params.Ino))
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return nil
						}
						return fmt.Errorf("get inode %d for update on orphan check: %w", params.Ino, err)
					}
					lockedInode = true
				}
			}
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, inode); err != nil {
				return err
			}
		}

		return nil
	})
	if err == nil {
		committed = true
		if lastUndo != nil {
			lastUndo.Discard()
		}
	} else {
		if lastUndo != nil {
			lastUndo.Rollback()
		}
	}

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return ErrNotFound
		case errors.Is(err, ErrIsDir):
			return ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, err)
			return ErrBadFileHandle
		default:
			return fmt.Errorf("release tx: %w", err)
		}
	}

	if (params.ReleaseFlags&FuseReleaseFlockUnlock) != 0 || m.hasPosixLockActivity(params.Ino) || params.SessionID != nil {
		m.notifyLockWaiters(params.Ino)
	}
	return nil
}

// FsyncDir 同步目录节点与目录句柄关联的状态（对接 Linux FUSE FUSE_FSYNCDIR = 30 协议），并触发块存储与数据库 WAL 物理落盘屏障。
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

	return m.flushDurabilityBarrier(ctx)
}

// ReleaseDir 关闭并销毁目录句柄，且在孤儿目录无活跃句柄时执行最终物理回收。
func (m *manager) ReleaseDir(ctx context.Context, params ReleaseDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		lockedInode := false
		inode, err := q.GetInode(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
			return ErrNotDir
		}

		if inode.Nlink == 0 {
			inode, err = q.GetInodeForUpdate(ctx, int64(params.Ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return fmt.Errorf("get inode %d for update: %w", params.Ino, err)
			}
			lockedInode = true
			if (uint32(inode.Mode) & S_IFMT) != S_IFDIR {
				return ErrNotDir
			}
		}

		// 1. 校验会话有效性并原子续期（在 5s 内存节流窗口且无延迟孤儿时直接交由 DeleteFileHandle 内联校验；慢路径则先续期防止被 reclaimUnreferencedOrphans 抢先回收）
		if params.SessionID != nil && !m.canSkipHandleSessionTouchInWindow(ctx, *params.SessionID) {
			handleCreatedAt := m.lookupHandleCreatedAt(ctx, q, params.Fh, params.Ino, *params.SessionID)
			if err := m.touchActiveClientSessionForHandle(ctx, q, *params.SessionID, handleCreatedAt); err != nil {
				return err
			}
		}

		// 2. 删除目录句柄（若句柄不存在直接返回 ErrBadFileHandle）
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

		if !lockedInode {
			if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r.RawDBTX() != nil {
				var curNlink int32
				if err := r.RawDBTX().QueryRow(ctx, `SELECT nlink FROM inodes WHERE ino = $1`, int64(params.Ino)).Scan(&curNlink); err == nil && curNlink == 0 {
					inode, err = q.GetInodeForUpdate(ctx, int64(params.Ino))
					if err != nil {
						if errors.Is(err, pgx.ErrNoRows) {
							return nil
						}
						return fmt.Errorf("get inode %d for update on orphan check: %w", params.Ino, err)
					}
					lockedInode = true
				}
			}
		}

		if inode.Nlink == 0 {
			if err := m.reclaimOrphanIfNoHandles(ctx, q, inode); err != nil {
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
			m.handleBadFileHandleSessionCleanup(ctx, params.SessionID, err)
			return ErrBadFileHandle
		default:
			return fmt.Errorf("releasedir tx: %w", err)
		}
	}

	m.clearDirCursors(params.Ino, params.Fh)
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
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
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
