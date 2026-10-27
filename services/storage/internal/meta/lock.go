package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

func validateCommonLockRequest(ino, fh, sessionID uint64, lkFlags uint32, lk FileLock) error {
	if ino == 0 || ino > math.MaxInt64 ||
		fh == 0 || fh > math.MaxInt64 ||
		sessionID == 0 || sessionID > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if lkFlags > FuseLkFlock {
		return ErrInvalidArgument
	}
	if lk.Start > OffsetMax || lk.End > OffsetMax || lk.Start > lk.End {
		return ErrInvalidArgument
	}
	if lkFlags == FuseLkFlock && (lk.Start != 0 || lk.End != OffsetMax) {
		return ErrInvalidArgument
	}
	return nil
}

func validatePosixOpenMode(lockType uint32, handleFlags int32) error {
	accMode := uint32(handleFlags) & O_ACCMODE
	switch lockType {
	case F_RDLCK:
		if accMode != O_RDONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}
	case F_WRLCK:
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}
	}
	return nil
}

var errExpiredSessionBadHandle = fmt.Errorf("%w: session expired", ErrBadFileHandle)

// touchActiveClientSession 仅原子延长未过期的活跃会话租约；若会话已过期或不存在则返回 ErrBadFileHandle。
func (m *manager) touchActiveClientSession(ctx context.Context, q db.Querier, sessionID uint64) error {
	return m.touchActiveClientSessionForHandle(ctx, q, sessionID, pgtype.Timestamptz{})
}

func (m *manager) touchActiveClientSessionForHandle(ctx context.Context, q db.Querier, sessionID uint64, handleCreatedAt pgtype.Timestamptz) error {
	leaseTTL := m.sessionLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultSessionLeaseTTL
	}

	deletedCount, err := q.DeleteExpiredClientSessions(ctx)
	if err != nil {
		return fmt.Errorf("delete expired client sessions: %w", err)
	}
	if deletedCount > 0 {
		m.notifyAllLockWaiters()
	}

	session, err := q.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sessionID),
		ExpiresAt: pgtype.Timestamptz{
			Time:  time.Now().Add(leaseTTL),
			Valid: true,
		},
	})
	if err != nil {
		return fmt.Errorf("upsert client session %d: %w", sessionID, err)
	}
	if session.CreatedAt.Valid && session.UpdatedAt.Valid && session.CreatedAt.Time.Equal(session.UpdatedAt.Time) {
		return errExpiredSessionBadHandle
	}
	if handleCreatedAt.Valid && session.CreatedAt.Valid && handleCreatedAt.Time.Before(session.CreatedAt.Time) {
		return ErrBadFileHandle
	}
	if err := m.reclaimUnreferencedOrphans(ctx, q); err != nil {
		return err
	}
	return nil
}

func (m *manager) cleanExpiredSessionByID(ctx context.Context, sessionID uint64) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = m.execWriteTxWithRetry(cleanupCtx, func(q db.Querier) error {
		deletedCount, err := q.DeleteExpiredClientSessions(cleanupCtx)
		if err != nil {
			return err
		}
		if deletedCount > 0 {
			m.notifyAllLockWaiters()
		}
		return nil
	})
}

func (m *manager) renewClientSession(ctx context.Context, q db.Querier, sessionID uint64) (time.Time, error) {
	leaseTTL := m.sessionLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultSessionLeaseTTL
	}

	deletedCount, err := q.DeleteExpiredClientSessions(ctx)
	if err != nil {
		return time.Time{}, fmt.Errorf("delete expired client sessions: %w", err)
	}
	if deletedCount > 0 {
		m.notifyAllLockWaiters()
	}
	session, err := q.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sessionID),
		ExpiresAt: pgtype.Timestamptz{
			Time:  time.Now().Add(leaseTTL),
			Valid: true,
		},
	})
	if err != nil {
		return time.Time{}, fmt.Errorf("upsert client session %d: %w", sessionID, err)
	}
	if err := m.reclaimUnreferencedOrphans(ctx, q); err != nil {
		return time.Time{}, err
	}
	return session.ExpiresAt.Time, nil
}

// GetLk 执行 FUSE_GETLK (31) 冲突锁探测操作。
// 遵循 ADR-0012：
// 1. 静态校验参数合法性（拒绝 F_UNLCK、start > end、非法 lk_flags 及非整文件区间 flock）；
// 2. 在只读事务中校验目标 Inode 存在且非目录（目录返回 ErrIsDir）、校验 FileHandle 存在且归属目标 Inode（不限制读写打开模式位，支持 Orphan Inode）；
// 3. 过滤已超期的 client_sessions 租约，在目标命名空间（lk_flags == 0 排除同一 (session_id, lock_owner)，lk_flags == FuseLkFlock 排除同一 (session_id, fh)）内按确定性顺序检索首个冲突锁；
// 4. 若无冲突则返回 {Start: req.Start, End: req.End, Type: F_UNLCK, PID: 0}，不修改 Inode 时间戳。
func (m *manager) GetLk(ctx context.Context, params GetLkParams) (*FileLock, error) {
	if err := validateCommonLockRequest(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return nil, err
	}
	if params.Lk.Type != F_RDLCK && params.Lk.Type != F_WRLCK {
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
			Fh:        int64(params.Fh),
			Ino:       int64(params.Ino),
			SessionID: pgtype.Int8{Int64: int64(params.SessionID), Valid: true},
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

type lockNotifyEntry struct {
	ch       chan struct{}
	refCount int
}

func (m *manager) acquireLockNotifyChan(ino uint64) (<-chan struct{}, func()) {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		m.lockNotifyChans = make(map[uint64]*lockNotifyEntry)
	}
	entry, ok := m.lockNotifyChans[ino]
	if !ok {
		entry = &lockNotifyEntry{
			ch: make(chan struct{}),
		}
		m.lockNotifyChans[ino] = entry
	}
	entry.refCount++

	var once sync.Once
	release := func() {
		once.Do(func() {
			m.lockNotifyMu.Lock()
			defer m.lockNotifyMu.Unlock()
			entry.refCount--
			if entry.refCount <= 0 {
				if cur, exists := m.lockNotifyChans[ino]; exists && cur == entry {
					delete(m.lockNotifyChans, ino)
				}
			}
		})
	}
	return entry.ch, release
}

func (m *manager) waitLockChan(ino uint64) <-chan struct{} {
	ch, _ := m.acquireLockNotifyChan(ino)
	return ch
}

func (m *manager) notifyLockWaiters(ino uint64) {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		return
	}
	if entry, ok := m.lockNotifyChans[ino]; ok {
		close(entry.ch)
		delete(m.lockNotifyChans, ino)
	}
}

func (m *manager) notifyAllLockWaiters() {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		return
	}
	for ino, entry := range m.lockNotifyChans {
		close(entry.ch)
		delete(m.lockNotifyChans, ino)
	}
}

func (m *manager) cleanLockWaitersForInos(inos []int64) {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		return
	}
	for _, ino := range inos {
		uino := uint64(ino)
		if entry, ok := m.lockNotifyChans[uino]; ok {
			close(entry.ch)
			delete(m.lockNotifyChans, uino)
		}
	}
}

// SetLk 执行 FUSE_SETLK (32) 非阻塞加解锁操作。
// 遵循 ADR-0012：
// 1. 采用 READ COMMITTED 写事务，通过 GetInodeForUpdate(ino) 串行化同一 Inode 上的并发锁变更；
// 2. 校验 Inode 存在且非目录（支持 Orphan Inode），校验 FileHandle 存在且归属目标 Inode；
// 3. 对 POSIX 记录锁（lk_flags == 0）校验打开句柄读写模式位（F_RDLCK 需读权限，F_WRLCK 需写权限，F_UNLCK 不限）；
// 4. 清理 expires_at <= clock_timestamp() 的超期会话并续期当前 session_id 租约；
// 5. 执行冲突检测与锁区间变更，冲突时返回 ErrAgain（EAGAIN），严格不修改 Inode 时间戳。
func (m *manager) SetLk(ctx context.Context, params SetLkParams) error {
	if err := validateCommonLockRequest(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return err
	}
	if params.Lk.Type != F_RDLCK && params.Lk.Type != F_WRLCK && params.Lk.Type != F_UNLCK {
		return ErrInvalidArgument
	}

	var flockConflictAfterDelete bool
	var shouldNotify bool
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

		flockConflictAfterDelete = false
		shouldNotify = false
		inode, err := q.GetInodeForUpdate(ctx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
		}

		if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(params.Fh),
			Ino:       int64(params.Ino),
			SessionID: pgtype.Int8{Int64: int64(params.SessionID), Valid: true},
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
		}

		if params.LkFlags == 0 {
			if err := validatePosixOpenMode(params.Lk.Type, handle.Flags); err != nil {
				return err
			}
		}

		if err := m.touchActiveClientSessionForHandle(ctx, q, params.SessionID, handle.CreatedAt); err != nil {
			return err
		}

		if params.LkFlags == FuseLkFlock {
			if err := applyFlockSetLk(ctx, q, params, &flockConflictAfterDelete, &shouldNotify); err != nil {
				return err
			}
			m.recordExternalLockOpInFlightGrants(params.Ino, params.SessionID, params.Fh, params.LockOwner, true, 0, OffsetMax, 0, currentUndo)
			return nil
		}
		if err := applyPosixSetLk(ctx, q, params, &shouldNotify); err != nil {
			return err
		}
		m.recordExternalLockOpInFlightGrants(params.Ino, params.SessionID, params.Fh, params.LockOwner, false, params.Lk.Start, params.Lk.End, 0, currentUndo)
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
			if errors.Is(err, errExpiredSessionBadHandle) {
				m.cleanExpiredSessionByID(ctx, params.SessionID)
			}
			return ErrBadFileHandle
		case errors.Is(err, ErrAgain):
			return ErrAgain
		default:
			return fmt.Errorf("setlk tx: %w", err)
		}
	}

	if shouldNotify {
		m.notifyLockWaiters(params.Ino)
	}

	if flockConflictAfterDelete {
		return ErrAgain
	}

	return nil
}

func rangesOverlap(start1, end1, start2, end2 uint64) bool {
	return start1 <= end2 && end1 >= start2
}

func locksConflict(type1, type2 uint32) bool {
	return type1 == F_WRLCK || type2 == F_WRLCK
}

type lockSegment struct {
	start uint64
	end   uint64
	ltype uint32
	pid   uint32
}

func applyPosixSetLk(ctx context.Context, q db.Querier, params SetLkParams, shouldNotify *bool) error {
	ino := int64(params.Ino)
	sessionID := int64(params.SessionID)
	owner := int64(params.LockOwner)
	reqStart := params.Lk.Start
	reqEnd := params.Lk.End
	reqType := params.Lk.Type

	activeLocks, err := q.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     ino,
		LkFlags: 0,
	})
	if err != nil {
		return fmt.Errorf("list active posix file locks (ino=%d): %w", params.Ino, err)
	}

	if reqType == F_UNLCK {
		return applyPosixUnlock(ctx, q, params, activeLocks, shouldNotify)
	}

	var callerHasActiveLocks bool
	for _, l := range activeLocks {
		if l.SessionID == sessionID && l.LockOwner == owner {
			callerHasActiveLocks = true
			continue
		}
		lStart := uint64(l.StartOffset)
		lEnd := uint64(l.EndOffset)
		if rangesOverlap(lStart, lEnd, reqStart, reqEnd) && locksConflict(reqType, uint32(l.LockType)) {
			return ErrAgain
		}
	}

	if !callerHasActiveLocks {
		waiters, err := q.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     ino,
			LkFlags: 0,
		})
		if err != nil {
			return fmt.Errorf("list pending posix lock waiters (ino=%d): %w", params.Ino, err)
		}
		for _, w := range waiters {
			if w.SessionID == sessionID && w.LockOwner == owner {
				continue
			}
			wStart := uint64(w.StartOffset)
			wEnd := uint64(w.EndOffset)
			if rangesOverlap(wStart, wEnd, reqStart, reqEnd) && locksConflict(reqType, uint32(w.LockType)) {
				return ErrAgain
			}
		}
	}

	_, err = applyPosixGrant(ctx, q, params, activeLocks, shouldNotify)
	return err
}

func applyPosixUnlock(ctx context.Context, q db.Querier, params SetLkParams, activeLocks []db.FileLock, shouldNotify *bool) error {
	ino := int64(params.Ino)
	sessionID := int64(params.SessionID)
	owner := int64(params.LockOwner)
	reqStart := params.Lk.Start
	reqEnd := params.Lk.End

	var deleteIDs []int64
	var remnants []lockSegment
	for _, l := range activeLocks {
		if l.SessionID != sessionID || l.LockOwner != owner {
			continue
		}
		lStart := uint64(l.StartOffset)
		lEnd := uint64(l.EndOffset)
		if !rangesOverlap(lStart, lEnd, reqStart, reqEnd) {
			continue
		}
		deleteIDs = append(deleteIDs, l.ID)
		if lStart < reqStart {
			remnants = append(remnants, lockSegment{
				start: lStart,
				end:   reqStart - 1,
				ltype: uint32(l.LockType),
				pid:   uint32(l.Pid),
			})
		}
		if lEnd > reqEnd && reqEnd < OffsetMax {
			remnants = append(remnants, lockSegment{
				start: reqEnd + 1,
				end:   lEnd,
				ltype: uint32(l.LockType),
				pid:   uint32(l.Pid),
			})
		}
	}

	if len(deleteIDs) > 0 {
		if err := q.DeleteFileLocksByIDs(ctx, deleteIDs); err != nil {
			return fmt.Errorf("delete posix file locks (ino=%d): %w", params.Ino, err)
		}
		if shouldNotify != nil {
			*shouldNotify = true
		}
	}
	for _, seg := range remnants {
		_, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
			SessionID:   sessionID,
			Ino:         ino,
			Fh:          int64(params.Fh),
			LockOwner:   owner,
			LkFlags:     0,
			StartOffset: int64(seg.start),
			EndOffset:   int64(seg.end),
			LockType:    int16(seg.ltype),
			Pid:         int32(seg.pid),
		})
		if err != nil {
			return fmt.Errorf("create remnant posix file lock (ino=%d): %w", params.Ino, err)
		}
	}
	return nil
}

func applyPosixGrant(ctx context.Context, q db.Querier, params SetLkParams, activeLocks []db.FileLock, shouldNotify *bool) (int64, error) {
	ino := int64(params.Ino)
	sessionID := int64(params.SessionID)
	owner := int64(params.LockOwner)
	reqStart := params.Lk.Start
	reqEnd := params.Lk.End
	reqType := params.Lk.Type
	reqPID := params.Lk.PID

	var deleteIDs []int64
	var remnants []lockSegment
	mergedStart := reqStart
	mergedEnd := reqEnd

	for _, l := range activeLocks {
		if l.SessionID != sessionID || l.LockOwner != owner {
			continue
		}
		lStart := uint64(l.StartOffset)
		lEnd := uint64(l.EndOffset)
		lType := uint32(l.LockType)

		overlaps := rangesOverlap(lStart, lEnd, reqStart, reqEnd)
		adjacentSameType := lType == reqType &&
			((lEnd < OffsetMax && lEnd+1 == reqStart) || (reqEnd < OffsetMax && reqEnd+1 == lStart))

		if !overlaps && !adjacentSameType {
			continue
		}

		deleteIDs = append(deleteIDs, l.ID)
		if lType == F_WRLCK && reqType == F_RDLCK && overlaps && shouldNotify != nil {
			*shouldNotify = true
		}
		if lType == reqType {
			if lStart < mergedStart {
				mergedStart = lStart
			}
			if lEnd > mergedEnd {
				mergedEnd = lEnd
			}
		} else {
			if lStart < reqStart {
				remnants = append(remnants, lockSegment{
					start: lStart,
					end:   reqStart - 1,
					ltype: lType,
					pid:   uint32(l.Pid),
				})
			}
			if lEnd > reqEnd && reqEnd < OffsetMax {
				remnants = append(remnants, lockSegment{
					start: reqEnd + 1,
					end:   lEnd,
					ltype: lType,
					pid:   uint32(l.Pid),
				})
			}
		}
	}

	if len(deleteIDs) > 0 {
		if err := q.DeleteFileLocksByIDs(ctx, deleteIDs); err != nil {
			return 0, fmt.Errorf("delete overlapping posix file locks (ino=%d): %w", params.Ino, err)
		}
	}

	for _, seg := range remnants {
		_, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
			SessionID:   sessionID,
			Ino:         ino,
			Fh:          int64(params.Fh),
			LockOwner:   owner,
			LkFlags:     0,
			StartOffset: int64(seg.start),
			EndOffset:   int64(seg.end),
			LockType:    int16(seg.ltype),
			Pid:         int32(seg.pid),
		})
		if err != nil {
			return 0, fmt.Errorf("create split posix file lock (ino=%d): %w", params.Ino, err)
		}
	}

	created, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   sessionID,
		Ino:         ino,
		Fh:          int64(params.Fh),
		LockOwner:   owner,
		LkFlags:     0,
		StartOffset: int64(mergedStart),
		EndOffset:   int64(mergedEnd),
		LockType:    int16(reqType),
		Pid:         int32(reqPID),
	})
	if err != nil {
		return 0, fmt.Errorf("create posix file lock (ino=%d): %w", params.Ino, err)
	}

	return created.ID, nil
}

func applyFlockSetLk(ctx context.Context, q db.Querier, params SetLkParams, conflictAfterDelete *bool, shouldNotify *bool) error {
	ino := int64(params.Ino)
	sessionID := int64(params.SessionID)
	fh := int64(params.Fh)
	reqType := params.Lk.Type

	deletedCount, err := q.DeleteFlockFileLocksBySessionAndFh(ctx, db.DeleteFlockFileLocksBySessionAndFhParams{
		SessionID: sessionID,
		Ino:       ino,
		Fh:        fh,
	})
	if err != nil {
		return fmt.Errorf("delete existing flock file locks (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
	}
	if deletedCount > 0 && shouldNotify != nil {
		*shouldNotify = true
	}

	if reqType == F_UNLCK {
		return nil
	}

	activeLocks, err := q.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     ino,
		LkFlags: int32(FuseLkFlock),
	})
	if err != nil {
		return fmt.Errorf("list active flock file locks (ino=%d): %w", params.Ino, err)
	}

	for _, l := range activeLocks {
		if l.SessionID == sessionID && l.Fh == fh {
			continue
		}
		if locksConflict(reqType, uint32(l.LockType)) {
			*conflictAfterDelete = true
			return nil
		}
	}

	if deletedCount == 0 {
		waiters, err := q.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
			Ino:     ino,
			LkFlags: int32(FuseLkFlock),
		})
		if err != nil {
			return fmt.Errorf("list pending flock lock waiters (ino=%d): %w", params.Ino, err)
		}
		for _, w := range waiters {
			if w.SessionID == sessionID && w.Fh == fh {
				continue
			}
			if locksConflict(reqType, uint32(w.LockType)) {
				*conflictAfterDelete = true
				return nil
			}
		}
	}

	_, err = q.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   sessionID,
		Ino:         ino,
		Fh:          fh,
		LockOwner:   int64(params.LockOwner),
		LkFlags:     int32(FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(OffsetMax),
		LockType:    int16(reqType),
		Pid:         int32(params.Lk.PID),
	})
	if err != nil {
		return fmt.Errorf("create flock file lock (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
	}

	return nil
}

type lockHolderID struct {
	sessionID int64
	id        int64
}

func holderOfLock(l db.FileLock, lkFlags uint32) lockHolderID {
	if lkFlags == FuseLkFlock {
		return lockHolderID{sessionID: l.SessionID, id: l.Fh}
	}
	return lockHolderID{sessionID: l.SessionID, id: l.LockOwner}
}

func holderOfWaiter(w db.FileLockWaiter, lkFlags uint32) lockHolderID {
	if lkFlags == FuseLkFlock {
		return lockHolderID{sessionID: w.SessionID, id: w.Fh}
	}
	return lockHolderID{sessionID: w.SessionID, id: w.LockOwner}
}

func hasDeadlockCycle(
	reqHolder lockHolderID,
	reqStart, reqEnd uint64,
	reqType uint32,
	reqSessionID, reqRequestID int64,
	lkFlags uint32,
	activeLocks []db.FileLock,
	waiters []db.FileLockWaiter,
) bool {
	adj := make(map[lockHolderID]map[lockHolderID]struct{})
	addEdge := func(from, to lockHolderID) {
		if from == to {
			return
		}
		if adj[from] == nil {
			adj[from] = make(map[lockHolderID]struct{})
		}
		adj[from][to] = struct{}{}
	}

	for _, l := range activeLocks {
		h := holderOfLock(l, lkFlags)
		if h == reqHolder {
			continue
		}
		lStart := uint64(l.StartOffset)
		lEnd := uint64(l.EndOffset)
		if rangesOverlap(lStart, lEnd, reqStart, reqEnd) && locksConflict(reqType, uint32(l.LockType)) {
			addEdge(reqHolder, h)
		}
	}

	for _, w := range waiters {
		if w.SessionID == reqSessionID && w.RequestID == reqRequestID {
			continue
		}
		wHolder := holderOfWaiter(w, lkFlags)
		wStart := uint64(w.StartOffset)
		wEnd := uint64(w.EndOffset)
		wType := uint32(w.LockType)
		for _, l := range activeLocks {
			h := holderOfLock(l, lkFlags)
			if h == wHolder {
				continue
			}
			lStart := uint64(l.StartOffset)
			lEnd := uint64(l.EndOffset)
			if rangesOverlap(lStart, lEnd, wStart, wEnd) && locksConflict(wType, uint32(l.LockType)) {
				addEdge(wHolder, h)
			}
		}
	}

	startTargets := adj[reqHolder]
	if len(startTargets) == 0 {
		return false
	}

	visited := make(map[lockHolderID]bool)
	queue := make([]lockHolderID, 0, len(startTargets))
	for target := range startTargets {
		if target == reqHolder {
			return true
		}
		visited[target] = true
		queue = append(queue, target)
	}

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for next := range adj[curr] {
			if next == reqHolder {
				return true
			}
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

// SetLkw 执行 FUSE_SETLKW (33) 阻塞等待加解锁操作。
// 遵循 ADR-0012：
// 1. 以 (session_id, request_id) 驱动 file_lock_waiters 的 PENDING / GRANTED / CANCELLED 原子状态机，杜绝幽灵悬挂锁；
// 2. 在 GetInodeForUpdate(ino) 下对同一 Inode 同命名空间构建有界等待图，检测直接或环形死锁并返回 ErrDeadlock (EDEADLK)；
// 3. 对未持有该节点活跃锁的新请求执行防写锁饥饿排队拦截，并在冲突时释放数据库行锁于事务外阻塞等待；
// 4. 对于 BSD flock 模式转换遇冲突进入等待时，首轮事务即持久化删除调用方原有的旧 flock 锁并广播唤醒。
func (m *manager) SetLkw(ctx context.Context, params SetLkwParams) error {
	if err := validateCommonLockRequest(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return err
	}
	if params.RequestID == 0 || params.RequestID > OffsetMax {
		return ErrInvalidArgument
	}
	if params.Lk.Type != F_RDLCK && params.Lk.Type != F_WRLCK && params.Lk.Type != F_UNLCK {
		return ErrInvalidArgument
	}

	setLkParams := SetLkParams{
		Ino:       params.Ino,
		Fh:        params.Fh,
		LockOwner: params.LockOwner,
		Lk:        params.Lk,
		LkFlags:   params.LkFlags,
		SessionID: params.SessionID,
	}

	ino := int64(params.Ino)
	sessionID := int64(params.SessionID)
	requestID := int64(params.RequestID)
	fh := int64(params.Fh)
	owner := int64(params.LockOwner)
	reqStart := params.Lk.Start
	reqEnd := params.Lk.End
	reqType := params.Lk.Type

	reqHolder := lockHolderID{sessionID: sessionID, id: owner}
	if params.LkFlags == FuseLkFlock {
		reqHolder = lockHolderID{sessionID: sessionID, id: fh}
	}

	backoff := 5 * time.Millisecond
	const maxBackoff = 50 * time.Millisecond

	var waiterEverPersisted bool
	var flockOriginallyHeld bool
	var preGrantOwnerLocks []db.FileLock
	var affectedStart, affectedEnd uint64
	for attempt := 0; ; attempt++ {
		done, err := func() (bool, error) {
			notifyCh, releaseNotify := m.acquireLockNotifyChan(params.Ino)
			defer releaseNotify()

			var granted bool
			var unlocked bool
			var shouldNotify bool
			var deadlockDetected bool
			var interrupted bool
			var lastUndo *inFlightTxUndo
			var committed bool
			defer func() {
				if !committed && !granted && lastUndo != nil {
					lastUndo.Rollback()
				}
			}()

			txErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
				if lastUndo != nil {
					lastUndo.Rollback()
				}
				currentUndo := newInFlightTxUndo(m, params.Ino)
				lastUndo = currentUndo

				granted = false
				unlocked = false
				shouldNotify = false
				deadlockDetected = false
				interrupted = false
				preGrantOwnerLocks = nil
				affectedStart = 0
				affectedEnd = 0
				inode, err := q.GetInodeForUpdate(ctx, ino)
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return ErrNotFound
					}
					return fmt.Errorf("get inode for update %d: %w", params.Ino, err)
				}

				if (uint32(inode.Mode) & S_IFMT) == S_IFDIR {
					return ErrIsDir
				}

				existingWaiter, waiterErr := q.GetFileLockWaiter(ctx, db.GetFileLockWaiterParams{
					SessionID: sessionID,
					RequestID: requestID,
				})
				hasExistingWaiter := waiterErr == nil
				if waiterErr != nil && !errors.Is(waiterErr, pgx.ErrNoRows) {
					return fmt.Errorf("get file lock waiter (session_id=%d, request_id=%d): %w", params.SessionID, params.RequestID, waiterErr)
				}
				if hasExistingWaiter {
					waiterEverPersisted = true
				}

				if ctx.Err() != nil || (hasExistingWaiter && existingWaiter.Status == WaiterStatusCancelled) {
					if hasExistingWaiter {
						_, _ = q.DeleteFileLockWaiter(ctx, db.DeleteFileLockWaiterParams{
							SessionID: sessionID,
							RequestID: requestID,
						})
					}
					interrupted = true
					return nil
				}

				handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
					Fh:        fh,
					Ino:       ino,
					SessionID: pgtype.Int8{Int64: sessionID, Valid: true},
				})
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						return ErrBadFileHandle
					}
					return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", params.Fh, params.Ino, err)
				}

				if params.LkFlags == 0 {
					if err := validatePosixOpenMode(reqType, handle.Flags); err != nil {
						return err
					}
				}

				if err := m.touchActiveClientSessionForHandle(ctx, q, params.SessionID, handle.CreatedAt); err != nil {
					return err
				}

				if params.LkFlags == FuseLkFlock && (attempt == 0 || reqType == F_UNLCK) {
					deletedCount, err := q.DeleteFlockFileLocksBySessionAndFh(ctx, db.DeleteFlockFileLocksBySessionAndFhParams{
						SessionID: sessionID,
						Ino:       ino,
						Fh:        fh,
					})
					if err != nil {
						return fmt.Errorf("delete existing flock file locks (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
					}
					if deletedCount > 0 {
						flockOriginallyHeld = true
						shouldNotify = true
					}
				}

				if reqType == F_UNLCK {
					if params.LkFlags == 0 {
						activeLocks, err := q.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
							Ino:     ino,
							LkFlags: 0,
						})
						if err != nil {
							return fmt.Errorf("list active posix file locks (ino=%d): %w", params.Ino, err)
						}
						if err := applyPosixUnlock(ctx, q, setLkParams, activeLocks, &shouldNotify); err != nil {
							return err
						}
					}
					if hasExistingWaiter {
						_, _ = q.DeleteFileLockWaiter(ctx, db.DeleteFileLockWaiterParams{
							SessionID: sessionID,
							RequestID: requestID,
						})
					}
					m.recordExternalLockOpInFlightGrants(params.Ino, params.SessionID, params.Fh, params.LockOwner, params.LkFlags == FuseLkFlock, reqStart, reqEnd, 0, currentUndo)
					affectedStart = reqStart
					affectedEnd = reqEnd
					unlocked = true
					return nil
				}

				activeLocks, err := q.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
					Ino:     ino,
					LkFlags: int32(params.LkFlags),
				})
				if err != nil {
					return fmt.Errorf("list active file locks (ino=%d, lk_flags=%d): %w", params.Ino, params.LkFlags, err)
				}

				waiters, err := q.ListPendingFileLockWaitersByInoAndFlags(ctx, db.ListPendingFileLockWaitersByInoAndFlagsParams{
					Ino:     ino,
					LkFlags: int32(params.LkFlags),
				})
				if err != nil {
					return fmt.Errorf("list pending file lock waiters (ino=%d, lk_flags=%d): %w", params.Ino, params.LkFlags, err)
				}

				callerHasActiveLocks := flockOriginallyHeld
				var hasActiveConflict bool
				for _, l := range activeLocks {
					h := holderOfLock(l, params.LkFlags)
					if h == reqHolder {
						callerHasActiveLocks = true
						continue
					}
					lStart := uint64(l.StartOffset)
					lEnd := uint64(l.EndOffset)
					if rangesOverlap(lStart, lEnd, reqStart, reqEnd) && locksConflict(reqType, uint32(l.LockType)) {
						hasActiveConflict = true
					}
				}

				var hasEarlierWaiterConflict bool
				if !callerHasActiveLocks {
					for _, w := range waiters {
						if w.SessionID == sessionID && w.RequestID == requestID {
							// waiters 已按 created_at ASC, session_id ASC, request_id ASC 排序，
							// 扫描到自身说明排在自身之前的更早等待者均不冲突
							break
						}
						wHolder := holderOfWaiter(w, params.LkFlags)
						if wHolder == reqHolder {
							continue
						}
						wStart := uint64(w.StartOffset)
						wEnd := uint64(w.EndOffset)
						if rangesOverlap(wStart, wEnd, reqStart, reqEnd) && locksConflict(reqType, uint32(w.LockType)) {
							hasEarlierWaiterConflict = true
							break
						}
					}
				}

				if !hasActiveConflict && !hasEarlierWaiterConflict {
					var txPreGrantLocks []db.FileLock
					affStart := reqStart
					affEnd := reqEnd
					if params.LkFlags == 0 {
						for _, l := range activeLocks {
							if l.SessionID == sessionID && l.LockOwner == owner {
								lStart := uint64(l.StartOffset)
								lEnd := uint64(l.EndOffset)
								if rangesOverlap(lStart, lEnd, reqStart, reqEnd) {
									cStart := max(lStart, reqStart)
									cEnd := min(lEnd, reqEnd)
									clipped := l
									clipped.StartOffset = int64(cStart)
									clipped.EndOffset = int64(cEnd)
									txPreGrantLocks = append(txPreGrantLocks, clipped)
								}
							}
						}
					} else {
						affStart = 0
						affEnd = OffsetMax
					}

					var grantedLockID int64
					if params.LkFlags == FuseLkFlock {
						if attempt > 0 {
							if _, err := q.DeleteFlockFileLocksBySessionAndFh(ctx, db.DeleteFlockFileLocksBySessionAndFhParams{
								SessionID: sessionID,
								Ino:       ino,
								Fh:        fh,
							}); err != nil {
								return fmt.Errorf("delete existing flock file locks (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
							}
						}
						createdLock, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
							SessionID:   sessionID,
							Ino:         ino,
							Fh:          fh,
							LockOwner:   owner,
							LkFlags:     int32(FuseLkFlock),
							StartOffset: 0,
							EndOffset:   int64(OffsetMax),
							LockType:    int16(reqType),
							Pid:         int32(params.Lk.PID),
						})
						if err != nil {
							return fmt.Errorf("create flock file lock (ino=%d, fh=%d): %w", params.Ino, params.Fh, err)
						}
						grantedLockID = createdLock.ID
					} else {
						createdID, err := applyPosixGrant(ctx, q, setLkParams, activeLocks, &shouldNotify)
						if err != nil {
							return err
						}
						grantedLockID = createdID
					}

					if hasExistingWaiter {
						if _, err := q.DeleteFileLockWaiter(ctx, db.DeleteFileLockWaiterParams{
							SessionID: sessionID,
							RequestID: requestID,
						}); err != nil {
							return fmt.Errorf("delete granted file lock waiter (session_id=%d, request_id=%d): %w", params.SessionID, params.RequestID, err)
						}
					}
					regID, effectivePreGrantLocks := m.registerInFlightGrant(
						params.Ino, params.SessionID, params.RequestID, params.Fh, params.LockOwner,
						params.LkFlags == FuseLkFlock, grantedLockID,
						reqStart, reqEnd, txPreGrantLocks, currentUndo,
					)
					m.recordExternalLockOpInFlightGrants(
						params.Ino, params.SessionID, params.Fh, params.LockOwner,
						params.LkFlags == FuseLkFlock, reqStart, reqEnd, regID, currentUndo,
					)
					preGrantOwnerLocks = effectivePreGrantLocks
					affectedStart = affStart
					affectedEnd = affEnd
					granted = true
					return nil
				}

				if hasDeadlockCycle(reqHolder, reqStart, reqEnd, reqType, sessionID, requestID, params.LkFlags, activeLocks, waiters) {
					if hasExistingWaiter {
						if _, err := q.DeleteFileLockWaiter(ctx, db.DeleteFileLockWaiterParams{
							SessionID: sessionID,
							RequestID: requestID,
						}); err != nil {
							return fmt.Errorf("delete deadlocked file lock waiter (session_id=%d, request_id=%d): %w", params.SessionID, params.RequestID, err)
						}
					}
					deadlockDetected = true
					return nil
				}

				_, err = q.UpsertFileLockWaiter(ctx, db.UpsertFileLockWaiterParams{
					SessionID:   sessionID,
					RequestID:   requestID,
					Ino:         ino,
					Fh:          fh,
					LockOwner:   owner,
					LkFlags:     int32(params.LkFlags),
					StartOffset: int64(reqStart),
					EndOffset:   int64(reqEnd),
					LockType:    int16(reqType),
					Pid:         int32(params.Lk.PID),
					Status:      WaiterStatusPending,
				})
				if err != nil {
					return fmt.Errorf("upsert file lock waiter (session_id=%d, request_id=%d): %w", params.SessionID, params.RequestID, err)
				}
				waiterEverPersisted = true

				return nil
			})

			if shouldNotify {
				m.notifyLockWaiters(params.Ino)
			}

			if txErr == nil {
				committed = true
				if lastUndo != nil {
					lastUndo.Discard()
				}
			} else {
				if !granted && lastUndo != nil {
					lastUndo.Rollback()
				}
			}

			if txErr != nil {
				if granted {
					if reqType != F_UNLCK {
						m.rollbackGrantedLock(params, preGrantOwnerLocks, affectedStart, affectedEnd, lastUndo)
					}
					return true, ErrInterrupted
				}
				if (waiterEverPersisted && !errors.Is(txErr, ErrBadFileHandle)) || errors.Is(txErr, context.Canceled) || errors.Is(txErr, context.DeadlineExceeded) || ctx.Err() != nil {
					m.cleanupCancelledWaiter(params.Ino, sessionID, requestID)
				}
				if errors.Is(txErr, context.Canceled) || errors.Is(txErr, context.DeadlineExceeded) || ctx.Err() != nil {
					return true, ErrInterrupted
				}
				switch {
				case errors.Is(txErr, ErrInvalidArgument):
					return true, ErrInvalidArgument
				case errors.Is(txErr, ErrNotFound):
					return true, ErrNotFound
				case errors.Is(txErr, ErrIsDir):
					return true, ErrIsDir
				case errors.Is(txErr, ErrBadFileHandle):
					if errors.Is(txErr, errExpiredSessionBadHandle) && !waiterEverPersisted {
						m.cleanExpiredSessionByID(ctx, params.SessionID)
					}
					return true, ErrBadFileHandle
				case errors.Is(txErr, ErrDeadlock):
					return true, ErrDeadlock
				case errors.Is(txErr, ErrInterrupted):
					return true, ErrInterrupted
				default:
					return true, fmt.Errorf("setlkw tx: %w", txErr)
				}
			}

			if interrupted {
				m.notifyLockWaiters(params.Ino)
				return true, ErrInterrupted
			}
			if deadlockDetected {
				m.notifyLockWaiters(params.Ino)
				return true, ErrDeadlock
			}
			if unlocked {
				if ctx.Err() != nil {
					return true, ErrInterrupted
				}
				return true, nil
			}
			if granted {
				if ctx.Err() != nil {
					if reqType != F_UNLCK {
						m.rollbackGrantedLock(params, preGrantOwnerLocks, affectedStart, affectedEnd, nil)
					}
					return true, ErrInterrupted
				}
				m.unregisterInFlightGrant(params.Ino, params.SessionID, params.RequestID)
				return true, nil
			}

			half := backoff / 2
			waitDuration := half + rand.N(half+1)
			timer := time.NewTimer(waitDuration)
			select {
			case <-ctx.Done():
				timer.Stop()
				m.cleanupCancelledWaiter(params.Ino, sessionID, requestID)
				return true, ErrInterrupted
			case <-notifyCh:
				timer.Stop()
			case <-timer.C:
				if backoff < maxBackoff {
					backoff *= 2
					if backoff > maxBackoff {
						backoff = maxBackoff
					}
				}
			}
			return false, nil
		}()

		if done {
			return err
		}
	}
}

func (m *manager) cleanupCancelledWaiter(ino uint64, sessionID, requestID int64) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var shouldNotify bool
	err := m.execWriteTxWithRetry(cleanupCtx, func(q db.Querier) error {
		shouldNotify = false

		_, err := q.GetInodeForUpdate(cleanupCtx, int64(ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}

		deletedCount, err := q.DeleteExpiredClientSessions(cleanupCtx)
		if err != nil {
			return err
		}
		if deletedCount > 0 {
			if err := m.reclaimUnreferencedOrphans(cleanupCtx, q); err != nil {
				return err
			}
			shouldNotify = true
		}

		waiter, err := q.GetFileLockWaiter(cleanupCtx, db.GetFileLockWaiterParams{
			SessionID: sessionID,
			RequestID: requestID,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}

		if waiter.Status == WaiterStatusGranted {
			if waiter.LkFlags == int32(FuseLkFlock) {
				if _, err := q.DeleteFlockFileLocksBySessionAndFh(cleanupCtx, db.DeleteFlockFileLocksBySessionAndFhParams{
					SessionID: sessionID,
					Ino:       int64(ino),
					Fh:        waiter.Fh,
				}); err != nil {
					return err
				}
			} else {
				activeLocks, err := q.ListActiveFileLocksByInoAndFlags(cleanupCtx, db.ListActiveFileLocksByInoAndFlagsParams{
					Ino:     int64(ino),
					LkFlags: 0,
				})
				if err != nil {
					return err
				}
				unlockParams := SetLkParams{
					Ino:       ino,
					Fh:        uint64(waiter.Fh),
					LockOwner: uint64(waiter.LockOwner),
					Lk: FileLock{
						Start: uint64(waiter.StartOffset),
						End:   uint64(waiter.EndOffset),
						Type:  F_UNLCK,
						PID:   uint32(waiter.Pid),
					},
					LkFlags:   0,
					SessionID: uint64(sessionID),
				}
				if err := applyPosixUnlock(cleanupCtx, q, unlockParams, activeLocks, &shouldNotify); err != nil {
					return err
				}
			}
			shouldNotify = true
		}

		n, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
			SessionID: sessionID,
			RequestID: requestID,
		})
		if err != nil {
			return err
		}
		if n > 0 {
			shouldNotify = true
		}
		return nil
	})
	if err == nil && shouldNotify {
		m.notifyLockWaiters(ino)
	}
}

func mergeSegments(segments []lockSegment) []lockSegment {
	if len(segments) == 0 {
		return nil
	}
	slices.SortFunc(segments, func(a, b lockSegment) int {
		if a.start != b.start {
			if a.start < b.start {
				return -1
			}
			return 1
		}
		if a.end < b.end {
			return -1
		}
		if a.end > b.end {
			return 1
		}
		return 0
	})

	var merged []lockSegment
	for _, seg := range segments {
		if seg.start > seg.end {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, seg)
			continue
		}
		last := &merged[len(merged)-1]
		if last.ltype == seg.ltype &&
			(last.end >= seg.start || (last.end < OffsetMax && last.end+1 == seg.start)) {
			if seg.end > last.end {
				last.end = seg.end
			}
		} else {
			merged = append(merged, seg)
		}
	}
	return merged
}

type byteRange struct {
	start uint64
	end   uint64
}

type inFlightGrantKey struct {
	ino       uint64
	sessionID uint64
	requestID uint64
}

type inFlightGrant struct {
	regID                  uint64
	ino                    uint64
	sessionID              uint64
	requestID              uint64
	fh                     uint64
	lockOwner              uint64
	isFlock                bool
	grantedLockID          int64
	pendingRanges          []byteRange
	preGrantOwnerLocks     []db.FileLock
	flushedOrReleased      bool
	flushedOrReleasedCount int
	activeUndos            map[*inFlightTxUndo]struct{}
}

func subtractRangesList(ranges []byteRange, subStart, subEnd uint64) []byteRange {
	var result []byteRange
	for _, r := range ranges {
		if !rangesOverlap(r.start, r.end, subStart, subEnd) {
			result = append(result, r)
			continue
		}
		if r.start < subStart {
			result = append(result, byteRange{start: r.start, end: subStart - 1})
		}
		if r.end > subEnd && subEnd < OffsetMax {
			result = append(result, byteRange{start: subEnd + 1, end: r.end})
		}
	}
	return result
}

func subtractMultipleRanges(base []byteRange, toRemove []byteRange) []byteRange {
	res := base
	for _, rem := range toRemove {
		res = subtractRangesList(res, rem.start, rem.end)
	}
	return res
}

func mergeByteRanges(ranges []byteRange) []byteRange {
	if len(ranges) == 0 {
		return nil
	}
	slices.SortFunc(ranges, func(a, b byteRange) int {
		if a.start != b.start {
			if a.start < b.start {
				return -1
			}
			return 1
		}
		if a.end < b.end {
			return -1
		}
		if a.end > b.end {
			return 1
		}
		return 0
	})
	var merged []byteRange
	for _, r := range ranges {
		if r.start > r.end {
			continue
		}
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if last.end >= r.start || (last.end < math.MaxUint64 && last.end+1 >= r.start) {
			if r.end > last.end {
				last.end = r.end
			}
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

func intersectByteRanges(r1 []byteRange, r2 []byteRange) []byteRange {
	var result []byteRange
	for _, a := range r1 {
		for _, b := range r2 {
			if rangesOverlap(a.start, a.end, b.start, b.end) {
				iStart := max(a.start, b.start)
				iEnd := min(a.end, b.end)
				result = append(result, byteRange{start: iStart, end: iEnd})
			}
		}
	}
	return mergeByteRanges(result)
}

type inFlightTxUndo struct {
	m              *manager
	ino            uint64
	attemptRegID   uint64
	flushedEntries map[uint64]struct{}
	removedRanges  map[uint64][]byteRange
}

func newInFlightTxUndo(m *manager, ino uint64) *inFlightTxUndo {
	return &inFlightTxUndo{
		m:              m,
		ino:            ino,
		flushedEntries: make(map[uint64]struct{}),
		removedRanges:  make(map[uint64][]byteRange),
	}
}

func (u *inFlightTxUndo) recordFlushedLocked(g *inFlightGrant) {
	if u == nil || g == nil {
		return
	}
	if _, ok := u.flushedEntries[g.regID]; !ok {
		u.flushedEntries[g.regID] = struct{}{}
		g.flushedOrReleasedCount++
		g.flushedOrReleased = true
	}
	if len(g.pendingRanges) > 0 {
		u.recordRemovedRangesLocked(g, g.pendingRanges)
		g.pendingRanges = nil
	}
}

func (u *inFlightTxUndo) recordRemovedRangesLocked(g *inFlightGrant, removed []byteRange) {
	if u == nil || g == nil || len(removed) == 0 {
		return
	}
	u.removedRanges[g.regID] = mergeByteRanges(append(u.removedRanges[g.regID], removed...))
	if g.activeUndos == nil {
		g.activeUndos = make(map[*inFlightTxUndo]struct{})
	}
	g.activeUndos[u] = struct{}{}
}

func (u *inFlightTxUndo) recordRegistered(regID uint64) {
	if u == nil {
		return
	}
	u.attemptRegID = regID
}

func (u *inFlightTxUndo) Rollback() {
	if u == nil {
		return
	}
	u.m.inFlightGrantMu.Lock()
	defer u.m.inFlightGrantMu.Unlock()

	if u.attemptRegID > 0 {
		for k, g := range u.m.inFlightGrants {
			if g.ino == u.ino && g.regID == u.attemptRegID {
				delete(u.m.inFlightGrants, k)
				break
			}
		}
		u.attemptRegID = 0
	}

	for regID := range u.flushedEntries {
		for _, g := range u.m.inFlightGrants {
			if g.ino == u.ino && g.regID == regID {
				if g.flushedOrReleasedCount > 0 {
					g.flushedOrReleasedCount--
				}
				g.flushedOrReleased = g.flushedOrReleasedCount > 0
				break
			}
		}
	}
	clear(u.flushedEntries)

	for regID, rem := range u.removedRanges {
		for _, g := range u.m.inFlightGrants {
			if g.ino == u.ino && g.regID == regID {
				delete(g.activeUndos, u)
				if !g.flushedOrReleased && len(rem) > 0 {
					g.pendingRanges = mergeByteRanges(append(g.pendingRanges, rem...))
				}
				break
			}
		}
	}
	for _, g := range u.m.inFlightGrants {
		if g.ino == u.ino {
			delete(g.activeUndos, u)
		}
	}
	clear(u.removedRanges)
}

func (u *inFlightTxUndo) Discard() {
	if u == nil {
		return
	}
	u.m.inFlightGrantMu.Lock()
	defer u.m.inFlightGrantMu.Unlock()

	for _, g := range u.m.inFlightGrants {
		if g.ino == u.ino {
			delete(g.activeUndos, u)
		}
	}
	u.attemptRegID = 0
	clear(u.flushedEntries)
	clear(u.removedRanges)
}

func (m *manager) registerInFlightGrant(
	ino, sessionID, requestID, fh, lockOwner uint64,
	isFlock bool,
	grantedLockID int64,
	reqStart, reqEnd uint64,
	preGrantOwnerLocks []db.FileLock,
	undo *inFlightTxUndo,
) (uint64, []db.FileLock) {
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()

	m.nextInFlightGrantID++
	regID := m.nextInFlightGrantID

	var ranges []byteRange
	if !isFlock {
		ranges = []byteRange{{start: reqStart, end: reqEnd}}
	}

	effectivePreGrantLocks := preGrantOwnerLocks
	if !isFlock {
		var predecessors []*inFlightGrant
		for _, g := range m.inFlightGrants {
			if g.ino == ino && !g.isFlock && g.sessionID == sessionID && g.lockOwner == lockOwner && !g.flushedOrReleased {
				predecessors = append(predecessors, g)
			}
		}
		slices.SortFunc(predecessors, func(a, b *inFlightGrant) int {
			if a.regID < b.regID {
				return -1
			}
			return 1
		})

		target := []byteRange{{start: reqStart, end: reqEnd}}
		for _, pred := range predecessors {
			takenFromPending := intersectByteRanges(pred.pendingRanges, target)

			var stolenFromUndos []byteRange
			var emptyUndos []*inFlightTxUndo
			for otherUndo := range pred.activeUndos {
				if otherUndo == undo {
					continue
				}
				stolen := intersectByteRanges(otherUndo.removedRanges[pred.regID], target)
				if len(stolen) > 0 {
					otherUndo.removedRanges[pred.regID] = subtractMultipleRanges(otherUndo.removedRanges[pred.regID], stolen)
					if len(otherUndo.removedRanges[pred.regID]) == 0 {
						delete(otherUndo.removedRanges, pred.regID)
						emptyUndos = append(emptyUndos, otherUndo)
					}
					stolenFromUndos = append(stolenFromUndos, stolen...)
				}
			}
			for _, eu := range emptyUndos {
				delete(pred.activeUndos, eu)
			}
			stolenFromUndos = mergeByteRanges(stolenFromUndos)

			taken := mergeByteRanges(append(takenFromPending, stolenFromUndos...))
			if len(taken) == 0 {
				continue
			}

			// 1) 从 effectivePreGrantLocks 中扣除 taken 区间
			var remaining []db.FileLock
			for _, l := range effectivePreGrantLocks {
				lStart := uint64(l.StartOffset)
				lEnd := uint64(l.EndOffset)
				lRemnants := subtractMultipleRanges([]byteRange{{start: lStart, end: lEnd}}, taken)
				for _, rem := range lRemnants {
					cp := l
					cp.StartOffset = int64(rem.start)
					cp.EndOffset = int64(rem.end)
					remaining = append(remaining, cp)
				}
			}

			// 2) 从 pred.preGrantOwnerLocks 中提取与 taken 重叠的片段并继承
			for _, pl := range pred.preGrantOwnerLocks {
				plStart := uint64(pl.StartOffset)
				plEnd := uint64(pl.EndOffset)
				inheritedRanges := intersectByteRanges([]byteRange{{start: plStart, end: plEnd}}, taken)
				for _, ir := range inheritedRanges {
					cp := pl
					cp.StartOffset = int64(ir.start)
					cp.EndOffset = int64(ir.end)
					remaining = append(remaining, cp)
				}
			}
			effectivePreGrantLocks = remaining

			// 3) 从 pred.pendingRanges 中剪除 taken
			if undo != nil {
				undo.recordRemovedRangesLocked(pred, taken)
			}
			pred.pendingRanges = subtractMultipleRanges(pred.pendingRanges, taken)
		}
	}

	key := inFlightGrantKey{
		ino:       ino,
		sessionID: sessionID,
		requestID: requestID,
	}

	m.inFlightGrants[key] = &inFlightGrant{
		regID:                  regID,
		ino:                    ino,
		sessionID:              sessionID,
		requestID:              requestID,
		fh:                     fh,
		lockOwner:              lockOwner,
		isFlock:                isFlock,
		grantedLockID:          grantedLockID,
		pendingRanges:          ranges,
		preGrantOwnerLocks:     effectivePreGrantLocks,
		flushedOrReleased:      false,
		flushedOrReleasedCount: 0,
		activeUndos:            make(map[*inFlightTxUndo]struct{}),
	}

	undo.recordRegistered(regID)

	return regID, effectivePreGrantLocks
}

func (m *manager) unregisterInFlightGrant(ino, sessionID, requestID uint64) {
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()
	delete(m.inFlightGrants, inFlightGrantKey{
		ino:       ino,
		sessionID: sessionID,
		requestID: requestID,
	})
}

func (m *manager) unregisterInFlightGrantByRegID(ino, regID uint64) {
	if regID == 0 {
		return
	}
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()
	for k, g := range m.inFlightGrants {
		if g.ino == ino && g.regID == regID {
			delete(m.inFlightGrants, k)
			break
		}
	}
}

func (m *manager) getInFlightGrant(ino, sessionID, requestID uint64) *inFlightGrant {
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()
	grant, ok := m.inFlightGrants[inFlightGrantKey{
		ino:       ino,
		sessionID: sessionID,
		requestID: requestID,
	}]
	if !ok {
		return nil
	}
	cp := *grant
	cp.pendingRanges = append([]byteRange(nil), grant.pendingRanges...)
	cp.preGrantOwnerLocks = append([]db.FileLock(nil), grant.preGrantOwnerLocks...)
	cp.activeUndos = nil
	return &cp
}

func (m *manager) markInFlightGrantsFlushedPOSIX(ino uint64, sessionID *uint64, lockOwner uint64, undo *inFlightTxUndo) {
	if sessionID == nil {
		return
	}
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()

	for _, g := range m.inFlightGrants {
		if g.ino != ino || g.isFlock || g.lockOwner != lockOwner {
			continue
		}
		if g.sessionID != *sessionID {
			continue
		}
		if undo != nil {
			undo.recordFlushedLocked(g)
		} else {
			g.flushedOrReleasedCount++
			g.flushedOrReleased = true
			g.pendingRanges = nil
		}
	}
}

func (m *manager) invalidateInFlightGrantsOnFlush(ino uint64, sessionID *uint64, lockOwner uint64) {
	m.markInFlightGrantsFlushedPOSIX(ino, sessionID, lockOwner, nil)
}

func (m *manager) markInFlightGrantsFlushedOrReleased(ino uint64, sessionID *uint64, fh uint64, undo *inFlightTxUndo) {
	if sessionID == nil {
		return
	}
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()

	for _, g := range m.inFlightGrants {
		if g.ino != ino || !g.isFlock || g.fh != fh {
			continue
		}
		if g.sessionID != *sessionID {
			continue
		}
		if undo != nil {
			undo.recordFlushedLocked(g)
		} else {
			g.flushedOrReleasedCount++
			g.flushedOrReleased = true
			g.pendingRanges = nil
		}
	}
}

func (m *manager) invalidateInFlightGrantsOnFlockRelease(ino uint64, sessionID *uint64, fh uint64) {
	m.markInFlightGrantsFlushedOrReleased(ino, sessionID, fh, nil)
}

func (m *manager) recordExternalLockOpInFlightGrants(
	ino, sessionID, fh, lockOwner uint64,
	isFlock bool,
	opStart, opEnd uint64,
	currentRegID uint64,
	undo *inFlightTxUndo,
) {
	m.inFlightGrantMu.Lock()
	defer m.inFlightGrantMu.Unlock()

	for _, g := range m.inFlightGrants {
		if g.ino != ino {
			continue
		}
		if currentRegID > 0 && g.regID >= currentRegID {
			continue
		}
		if isFlock {
			if g.isFlock && g.sessionID == sessionID && g.fh == fh {
				if undo != nil {
					undo.recordFlushedLocked(g)
				} else {
					g.flushedOrReleasedCount++
					g.flushedOrReleased = true
					g.pendingRanges = nil
				}
			}
		} else {
			if !g.isFlock && g.sessionID == sessionID && g.lockOwner == lockOwner {
				target := []byteRange{{start: opStart, end: opEnd}}
				removed := intersectByteRanges(g.pendingRanges, target)
				if len(removed) > 0 {
					g.pendingRanges = subtractMultipleRanges(g.pendingRanges, removed)
				}

				var stolenFromUndos []byteRange
				var emptyUndos []*inFlightTxUndo
				for otherUndo := range g.activeUndos {
					if otherUndo == undo {
						continue
					}
					stolen := intersectByteRanges(otherUndo.removedRanges[g.regID], target)
					if len(stolen) > 0 {
						otherUndo.removedRanges[g.regID] = subtractMultipleRanges(otherUndo.removedRanges[g.regID], stolen)
						if len(otherUndo.removedRanges[g.regID]) == 0 {
							delete(otherUndo.removedRanges, g.regID)
							emptyUndos = append(emptyUndos, otherUndo)
						}
						stolenFromUndos = append(stolenFromUndos, stolen...)
					}
				}
				for _, eu := range emptyUndos {
					delete(g.activeUndos, eu)
				}
				stolenFromUndos = mergeByteRanges(stolenFromUndos)

				taken := mergeByteRanges(append(removed, stolenFromUndos...))
				if len(taken) > 0 && undo != nil {
					undo.recordRemovedRangesLocked(g, taken)
				}
			}
		}
	}
}

func (m *manager) updateInFlightGrantsOnLockOp(
	ino, sessionID, fh, lockOwner uint64,
	isFlock bool,
	opStart, opEnd uint64,
	currentRegID uint64,
) {
	m.recordExternalLockOpInFlightGrants(ino, sessionID, fh, lockOwner, isFlock, opStart, opEnd, currentRegID, nil)
}

func (m *manager) rollbackGrantedLock(params SetLkwParams, preGrantOwnerLocks []db.FileLock, affectedStart, affectedEnd uint64, lastUndo *inFlightTxUndo) {
	defer m.unregisterInFlightGrant(params.Ino, params.SessionID, params.RequestID)

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var shouldNotify bool
	var pendingUndoAction int // 0: none, 1: rollback, 2: discard
	err := m.execWriteTxWithRetry(cleanupCtx, func(q db.Querier) error {
		shouldNotify = false
		pendingUndoAction = 0

		_, err := q.GetInodeForUpdate(cleanupCtx, int64(params.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			return err
		}

		deletedCount, err := q.DeleteExpiredClientSessions(cleanupCtx)
		if err != nil {
			return err
		}
		if deletedCount > 0 {
			if err := m.reclaimUnreferencedOrphans(cleanupCtx, q); err != nil {
				return err
			}
			shouldNotify = true
		}

		curGrant := m.getInFlightGrant(params.Ino, params.SessionID, params.RequestID)
		if curGrant == nil {
			if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
				SessionID: int64(params.SessionID),
				RequestID: int64(params.RequestID),
			}); err != nil {
				return err
			}
			shouldNotify = true
			return nil
		}

		if curGrant.flushedOrReleased {
			if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
				SessionID: int64(params.SessionID),
				RequestID: int64(params.RequestID),
			}); err != nil {
				return err
			}
			shouldNotify = true
			return nil
		}

		if params.LkFlags == FuseLkFlock {
			var toDelete []int64
			activeLocks, err := q.ListActiveFileLocksByInoAndFlags(cleanupCtx, db.ListActiveFileLocksByInoAndFlagsParams{
				Ino:     int64(params.Ino),
				LkFlags: int32(FuseLkFlock),
			})
			if err != nil {
				return err
			}
			for _, fl := range activeLocks {
				if fl.SessionID == int64(params.SessionID) && fl.Fh == int64(params.Fh) {
					if curGrant.grantedLockID > 0 && fl.ID == curGrant.grantedLockID {
						toDelete = append(toDelete, fl.ID)
					}
					break
				}
			}
			if lastUndo != nil && curGrant.grantedLockID > 0 {
				var lockCommitted bool
				for _, id := range toDelete {
					if id == curGrant.grantedLockID {
						lockCommitted = true
						break
					}
				}
				if !lockCommitted {
					pendingUndoAction = 1 // rollback
					if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
						SessionID: int64(params.SessionID),
						RequestID: int64(params.RequestID),
					}); err != nil {
						return err
					}
					lastUndo.Rollback()
					pendingUndoAction = 0
					shouldNotify = true
					return nil
				}
			}
			if lastUndo != nil {
				pendingUndoAction = 2 // discard
			}
			if len(toDelete) > 0 {
				if err := q.DeleteFileLocksByIDs(cleanupCtx, toDelete); err != nil {
					return err
				}
				shouldNotify = true
			}
			if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
				SessionID: int64(params.SessionID),
				RequestID: int64(params.RequestID),
			}); err != nil {
				return err
			}
			shouldNotify = true
			return nil
		}

		if len(curGrant.pendingRanges) == 0 {
			if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
				SessionID: int64(params.SessionID),
				RequestID: int64(params.RequestID),
			}); err != nil {
				return err
			}
			shouldNotify = true
			return nil
		}

		activeLocks, err := q.ListActiveFileLocksByInoAndFlags(cleanupCtx, db.ListActiveFileLocksByInoAndFlagsParams{
			Ino:     int64(params.Ino),
			LkFlags: 0,
		})
		if err != nil {
			return err
		}

		if lastUndo != nil && curGrant.grantedLockID > 0 {
			var lockCommitted bool
			for _, l := range activeLocks {
				if l.ID == curGrant.grantedLockID {
					lockCommitted = true
					break
				}
			}
			if !lockCommitted {
				pendingUndoAction = 1 // rollback
				if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
					SessionID: int64(params.SessionID),
					RequestID: int64(params.RequestID),
				}); err != nil {
					return err
				}
				lastUndo.Rollback()
				pendingUndoAction = 0
				shouldNotify = true
				return nil
			}
		}
		if lastUndo != nil {
			pendingUndoAction = 2 // discard
		}

		sessionID := int64(params.SessionID)
		owner := int64(params.LockOwner)

		var curOwnerLocks []db.FileLock
		var externalLocks []db.FileLock
		for _, l := range activeLocks {
			if l.SessionID == sessionID && l.LockOwner == owner {
				curOwnerLocks = append(curOwnerLocks, l)
			} else {
				externalLocks = append(externalLocks, l)
			}
		}

		var idsToDelete []int64
		var retainedSegments []lockSegment
		var curOwnerRanges []byteRange

		for _, l := range curOwnerLocks {
			lStart := uint64(l.StartOffset)
			lEnd := uint64(l.EndOffset)
			curOwnerRanges = append(curOwnerRanges, byteRange{start: lStart, end: lEnd})

			var overlapsWithPending bool
			for _, pr := range curGrant.pendingRanges {
				if rangesOverlap(lStart, lEnd, pr.start, pr.end) {
					overlapsWithPending = true
					break
				}
			}

			var adjacentToPending bool
			if !overlapsWithPending {
				for _, pr := range curGrant.pendingRanges {
					if (pr.start > 0 && lEnd+1 == pr.start) || (pr.end < OffsetMax && pr.end+1 == lStart) {
						adjacentToPending = true
						break
					}
				}
			}

			if !overlapsWithPending && !adjacentToPending {
				continue
			}

			idsToDelete = append(idsToDelete, l.ID)
			if overlapsWithPending {
				remnants := subtractMultipleRanges([]byteRange{{start: lStart, end: lEnd}}, curGrant.pendingRanges)
				for _, rem := range remnants {
					retainedSegments = append(retainedSegments, lockSegment{
						start: rem.start,
						end:   rem.end,
						ltype: uint32(l.LockType),
						pid:   uint32(l.Pid),
					})
				}
			} else if adjacentToPending {
				retainedSegments = append(retainedSegments, lockSegment{
					start: lStart,
					end:   lEnd,
					ltype: uint32(l.LockType),
					pid:   uint32(l.Pid),
				})
			}
		}

		validRestoreRanges := intersectByteRanges(curOwnerRanges, curGrant.pendingRanges)

		var restoredSegments []lockSegment
		for _, oldLock := range curGrant.preGrantOwnerLocks {
			oldStart := uint64(oldLock.StartOffset)
			oldEnd := uint64(oldLock.EndOffset)
			oldType := uint32(oldLock.LockType)
			oldPID := uint32(oldLock.Pid)

			oldFragments := intersectByteRanges([]byteRange{{start: oldStart, end: oldEnd}}, validRestoreRanges)
			for _, frag := range oldFragments {
				cutPoints := []uint64{frag.start}
				if frag.end < OffsetMax {
					cutPoints = append(cutPoints, frag.end+1)
				}

				for _, ext := range externalLocks {
					extStart := uint64(ext.StartOffset)
					extEnd := uint64(ext.EndOffset)
					if rangesOverlap(extStart, extEnd, frag.start, frag.end) {
						extS := max(extStart, frag.start)
						extE := min(extEnd, frag.end)
						cutPoints = append(cutPoints, extS)
						if extE < OffsetMax {
							cutPoints = append(cutPoints, extE+1)
						}
					}
				}

				slices.Sort(cutPoints)
				cutPoints = slices.Compact(cutPoints)

				for i := 0; i < len(cutPoints); i++ {
					subStart := cutPoints[i]
					if subStart > frag.end {
						continue
					}
					var subEnd uint64
					if i+1 < len(cutPoints) {
						subEnd = cutPoints[i+1] - 1
					} else {
						subEnd = frag.end
					}
					if subStart > subEnd || subEnd > frag.end {
						continue
					}

					var subExtLocks []db.FileLock
					var hasExtWrLock bool
					for _, ext := range externalLocks {
						if rangesOverlap(uint64(ext.StartOffset), uint64(ext.EndOffset), subStart, subEnd) {
							subExtLocks = append(subExtLocks, ext)
							if ext.LockType == int16(F_WRLCK) {
								hasExtWrLock = true
							}
						}
					}

					if hasExtWrLock {
						continue
					}

					if len(subExtLocks) == 0 {
						restoredSegments = append(restoredSegments, lockSegment{
							start: subStart,
							end:   subEnd,
							ltype: oldType,
							pid:   oldPID,
						})
						continue
					}

					if oldType == F_RDLCK {
						restoredSegments = append(restoredSegments, lockSegment{
							start: subStart,
							end:   subEnd,
							ltype: F_RDLCK,
							pid:   oldPID,
						})
					} else if oldType == F_WRLCK {
						isDowngradedToRead := params.Lk.Type == F_RDLCK &&
							rangesOverlap(params.Lk.Start, params.Lk.End, subStart, subEnd)
						if isDowngradedToRead {
							restoredSegments = append(restoredSegments, lockSegment{
								start: subStart,
								end:   subEnd,
								ltype: F_RDLCK,
								pid:   oldPID,
							})
						}
					}
				}
			}
		}

		if len(idsToDelete) > 0 {
			if err := q.DeleteFileLocksByIDs(cleanupCtx, idsToDelete); err != nil {
				return err
			}
		}

		allSegments := append(retainedSegments, restoredSegments...)
		mergedSegments := mergeSegments(allSegments)

		for _, seg := range mergedSegments {
			if _, err := q.CreateFileLock(cleanupCtx, db.CreateFileLockParams{
				SessionID:   sessionID,
				Ino:         int64(params.Ino),
				Fh:          int64(params.Fh),
				LockOwner:   owner,
				LkFlags:     0,
				StartOffset: int64(seg.start),
				EndOffset:   int64(seg.end),
				LockType:    int16(seg.ltype),
				Pid:         int32(seg.pid),
			}); err != nil {
				return err
			}
		}

		if _, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
			SessionID: int64(params.SessionID),
			RequestID: int64(params.RequestID),
		}); err != nil {
			return err
		}
		shouldNotify = true
		return nil
	})
	if err == nil {
		if pendingUndoAction == 1 && lastUndo != nil {
			lastUndo.Rollback()
		} else if pendingUndoAction == 2 && lastUndo != nil {
			lastUndo.Discard()
		}
		if shouldNotify {
			m.notifyLockWaiters(params.Ino)
		}
	} else if lastUndo != nil {
		lastUndo.Rollback()
	}
}
