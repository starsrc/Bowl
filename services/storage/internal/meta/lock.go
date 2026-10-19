package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
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

func (m *manager) renewClientSession(ctx context.Context, q db.Querier, sessionID uint64) (time.Time, error) {
	leaseTTL := m.sessionLeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = DefaultSessionLeaseTTL
	}
	if err := q.DeleteExpiredClientSessions(ctx); err != nil {
		return time.Time{}, fmt.Errorf("delete expired client sessions: %w", err)
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
	if err := q.DeleteUnreferencedOrphanInodes(ctx); err != nil {
		return time.Time{}, fmt.Errorf("delete unreferenced orphan inodes: %w", err)
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

func (m *manager) waitLockChan(ino uint64) <-chan struct{} {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		m.lockNotifyChans = make(map[uint64]chan struct{})
	}
	ch, ok := m.lockNotifyChans[ino]
	if !ok {
		ch = make(chan struct{})
		m.lockNotifyChans[ino] = ch
	}
	return ch
}

func (m *manager) notifyLockWaiters(ino uint64) {
	m.lockNotifyMu.Lock()
	defer m.lockNotifyMu.Unlock()
	if m.lockNotifyChans == nil {
		return
	}
	if ch, ok := m.lockNotifyChans[ino]; ok {
		close(ch)
		delete(m.lockNotifyChans, ino)
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
	err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
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

		if _, err := m.renewClientSession(ctx, q, params.SessionID); err != nil {
			return err
		}

		if params.LkFlags == FuseLkFlock {
			return applyFlockSetLk(ctx, q, params, &flockConflictAfterDelete, &shouldNotify)
		}
		return applyPosixSetLk(ctx, q, params, &shouldNotify)
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

	return applyPosixGrant(ctx, q, params, activeLocks, shouldNotify)
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

func applyPosixGrant(ctx context.Context, q db.Querier, params SetLkParams, activeLocks []db.FileLock, shouldNotify *bool) error {
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
			return fmt.Errorf("delete overlapping posix file locks (ino=%d): %w", params.Ino, err)
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
			return fmt.Errorf("create split posix file lock (ino=%d): %w", params.Ino, err)
		}
	}

	_, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
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
		return fmt.Errorf("create posix file lock (ino=%d): %w", params.Ino, err)
	}

	return nil
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

	var flockOriginallyHeld bool
	for attempt := 0; ; attempt++ {
		notifyCh := m.waitLockChan(params.Ino)

		var granted bool
		var shouldNotify bool
		var deadlockDetected bool
		var interrupted bool

		err := m.store.ExecTxWithOptions(ctx, func(q db.Querier) error {
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

			if _, err := m.renewClientSession(ctx, q, params.SessionID); err != nil {
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
				granted = true
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
				if params.LkFlags == FuseLkFlock {
					_, err := q.CreateFileLock(ctx, db.CreateFileLockParams{
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
				} else {
					if err := applyPosixGrant(ctx, q, setLkParams, activeLocks, &shouldNotify); err != nil {
						return err
					}
				}

				if hasExistingWaiter {
					if _, err := q.DeleteFileLockWaiter(ctx, db.DeleteFileLockWaiterParams{
						SessionID: sessionID,
						RequestID: requestID,
					}); err != nil {
						return fmt.Errorf("delete granted file lock waiter (session_id=%d, request_id=%d): %w", params.SessionID, params.RequestID, err)
					}
				}
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

			return nil
		})

		if shouldNotify {
			m.notifyLockWaiters(params.Ino)
		}

		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
				m.cleanupCancelledWaiter(params.Ino, sessionID, requestID)
				return ErrInterrupted
			}
			switch {
			case errors.Is(err, ErrInvalidArgument):
				return ErrInvalidArgument
			case errors.Is(err, ErrNotFound):
				return ErrNotFound
			case errors.Is(err, ErrIsDir):
				return ErrIsDir
			case errors.Is(err, ErrBadFileHandle):
				return ErrBadFileHandle
			case errors.Is(err, ErrDeadlock):
				return ErrDeadlock
			case errors.Is(err, ErrInterrupted):
				return ErrInterrupted
			default:
				return fmt.Errorf("setlkw tx: %w", err)
			}
		}

		if interrupted {
			m.notifyLockWaiters(params.Ino)
			return ErrInterrupted
		}
		if deadlockDetected {
			m.notifyLockWaiters(params.Ino)
			return ErrDeadlock
		}
		if granted {
			return nil
		}

		half := backoff / 2
		waitDuration := half + rand.N(half+1)
		timer := time.NewTimer(waitDuration)
		select {
		case <-ctx.Done():
			timer.Stop()
			m.cleanupCancelledWaiter(params.Ino, sessionID, requestID)
			return ErrInterrupted
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
	}
}

func (m *manager) cleanupCancelledWaiter(ino uint64, sessionID, requestID int64) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var removed int64
	_ = m.store.ExecTxWithOptions(cleanupCtx, func(q db.Querier) error {
		_, err := q.GetInodeForUpdate(cleanupCtx, int64(ino))
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		n, err := q.DeleteFileLockWaiter(cleanupCtx, db.DeleteFileLockWaiterParams{
			SessionID: sessionID,
			RequestID: requestID,
		})
		if err != nil {
			return err
		}
		removed = n
		return nil
	})
	if removed > 0 {
		m.notifyLockWaiters(ino)
	}
}

