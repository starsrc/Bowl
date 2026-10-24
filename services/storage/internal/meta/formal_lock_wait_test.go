package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// formalWaiterKey 唯一标识 file_lock_waiters 中的一条阻塞锁请求 (session_id, request_id)。
type formalWaiterKey struct {
	SessionID uint64
	RequestID uint64
}

// formalAsyncWaiter 跟踪后台正在阻塞等待的 SetLkw 协程及其取消函数与结果通道。
type formalAsyncWaiter struct {
	Params              meta.SetLkwParams
	Cancel              context.CancelFunc
	DoneCh              <-chan error
	FlockOriginallyHeld bool
}

// formalLockWaitHarness 封装 T13 阻塞锁、死锁检测、取消清理与短租约过期对拍所需的测试运行时。
type formalLockWaitHarness struct {
	Env              *formalLockTestEnv
	NextRequestID    uint64
	AsyncWaiters     map[formalWaiterKey]*formalAsyncWaiter
	FlockOrigHeldMap map[formalWaiterKey]bool
	CancelledWaiters []formalWaiterKey
	PurgedSessions   []uint64
}

func newFormalLockWaitHarness(env *formalLockTestEnv) *formalLockWaitHarness {
	return &formalLockWaitHarness{
		Env:              env,
		NextRequestID:    1,
		AsyncWaiters:     make(map[formalWaiterKey]*formalAsyncWaiter),
		FlockOrigHeldMap: make(map[formalWaiterKey]bool),
	}
}

func (h *formalLockWaitHarness) allocRequestID() uint64 {
	id := h.NextRequestID
	h.NextRequestID++
	return id
}

func formalWaiterOwnerKey(w *formalLockWaiterEntry) formalLockOwnerKey {
	if w.LkFlags == meta.FuseLkFlock {
		return formalLockOwnerKey{SessionID: w.SessionID, ID: w.Fh}
	}
	return formalLockOwnerKey{SessionID: w.SessionID, ID: w.LockOwner}
}

// removeWaiterByKey 从内存参考模型 Waiters 队列中移除指定 (session_id, request_id) 的等待记录。
func (m *formalLockModel) removeWaiterByKey(sessionID, requestID uint64) {
	kept := m.Waiters[:0]
	for _, w := range m.Waiters {
		if w.SessionID == sessionID && w.RequestID == requestID {
			continue
		}
		kept = append(kept, w)
	}
	m.Waiters = kept
}

// upsertPendingWaiter 在内存参考模型中插入或更新一条 PENDING 状态的锁等待记录。
func (m *formalLockModel) upsertPendingWaiter(params meta.SetLkwParams) {
	for _, w := range m.Waiters {
		if w.SessionID == params.SessionID && w.RequestID == params.RequestID {
			w.Ino = params.Ino
			w.Fh = params.Fh
			w.LockOwner = params.LockOwner
			w.LkFlags = params.LkFlags
			w.Start = params.Lk.Start
			w.End = params.Lk.End
			w.Type = params.Lk.Type
			w.PID = params.Lk.PID
			w.Status = meta.WaiterStatusPending
			return
		}
	}
	seq := m.nextWaiterSeq
	m.nextWaiterSeq++
	m.Waiters = append(m.Waiters, &formalLockWaiterEntry{
		SeqID:     seq,
		SessionID: params.SessionID,
		RequestID: params.RequestID,
		Ino:       params.Ino,
		Fh:        params.Fh,
		LockOwner: params.LockOwner,
		LkFlags:   params.LkFlags,
		Start:     params.Lk.Start,
		End:       params.Lk.End,
		Type:      params.Lk.Type,
		PID:       params.Lk.PID,
		Status:    meta.WaiterStatusPending,
	})
}

// recordExpiredSessionsBeforePurge 在模型触发 renewClientSession 之前记录即将被物理清除的已过期会话 ID。
func (h *formalLockWaitHarness) recordExpiredSessionsBeforePurge() {
	for sid, sess := range h.Env.Model.Sessions {
		if sess.Expired {
			h.PurgedSessions = append(h.PurgedSessions, sid)
		}
	}
}

// evalSetLkwInitial 在内存参考模型上推演 SetLkw 的首轮事务（attempt == 0）：
// 返回：
// - willBlock: 是否进入 file_lock_waiters 的 PENDING 阻塞等待状态；
// - flockOriginallyHeld: 若为 FuseLkFlock，调用方在首轮删除旧锁前是否持有活跃 flock；
// - expErr: 期望同步返回的错误（nil 表示立即授锁成功或进入阻塞等待）。
func (h *formalLockWaitHarness) evalSetLkwInitial(params meta.SetLkwParams) (willBlock bool, flockOriginallyHeld bool, expErr error) {
	m := h.Env.Model

	if err := lockOracleValidateCommon(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return false, false, err
	}
	if params.RequestID == 0 || params.RequestID > meta.OffsetMax {
		return false, false, meta.ErrInvalidArgument
	}
	if params.Lk.Type != meta.F_RDLCK && params.Lk.Type != meta.F_WRLCK && params.Lk.Type != meta.F_UNLCK {
		return false, false, meta.ErrInvalidArgument
	}

	inode, ok := m.Inodes[params.Ino]
	if !ok {
		return false, false, meta.ErrNotFound
	}
	if inode.IsDir {
		return false, false, meta.ErrIsDir
	}

	handle, valid := m.isHandleValid(params.Fh, params.Ino, &params.SessionID)
	if !valid {
		return false, false, meta.ErrBadFileHandle
	}

	if params.LkFlags == 0 {
		if err := lockOracleValidatePosixOpenMode(params.Lk.Type, handle.Flags); err != nil {
			return false, false, err
		}
	}

	h.recordExpiredSessionsBeforePurge()
	m.purgeExpiredSessionsAndUpsert(params.SessionID)

	if params.LkFlags == meta.FuseLkFlock {
		var deletedCount int
		kept := m.FlockLocks[:0]
		for _, l := range m.FlockLocks {
			if l.Ino == params.Ino && l.SessionID == params.SessionID && l.Fh == params.Fh {
				deletedCount++
				continue
			}
			kept = append(kept, l)
		}
		m.FlockLocks = kept
		if deletedCount > 0 {
			flockOriginallyHeld = true
		}
	}

	if params.Lk.Type == meta.F_UNLCK {
		if params.LkFlags == 0 {
			_ = m.applyPosixSetLk(meta.SetLkParams{
				Ino:       params.Ino,
				Fh:        params.Fh,
				LockOwner: params.LockOwner,
				Lk:        params.Lk,
				LkFlags:   0,
				SessionID: params.SessionID,
			})
		}
		m.removeWaiterByKey(params.SessionID, params.RequestID)
		delete(h.FlockOrigHeldMap, formalWaiterKey{SessionID: params.SessionID, RequestID: params.RequestID})
		return false, flockOriginallyHeld, nil
	}

	reqOwnerKey := formalLockOwnerKey{SessionID: params.SessionID, ID: params.LockOwner}
	if params.LkFlags == meta.FuseLkFlock {
		reqOwnerKey = formalLockOwnerKey{SessionID: params.SessionID, ID: params.Fh}
	}

	callerHasActiveLocks := flockOriginallyHeld
	var hasActiveConflict bool

	if params.LkFlags == meta.FuseLkFlock {
		for _, l := range m.activeFlockLocksByIno(params.Ino) {
			ownerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh}
			if ownerKey == reqOwnerKey {
				callerHasActiveLocks = true
				continue
			}
			if lockOracleConflicts(params.Lk.Type, l.Type) {
				hasActiveConflict = true
			}
		}
	} else {
		for _, l := range m.activePosixLocksByIno(params.Ino) {
			ownerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
			if ownerKey == reqOwnerKey {
				callerHasActiveLocks = true
				continue
			}
			if lockOracleRangesOverlap(l.Start, l.End, params.Lk.Start, params.Lk.End) &&
				lockOracleConflicts(params.Lk.Type, l.Type) {
				hasActiveConflict = true
			}
		}
	}

	var hasEarlierWaiterConflict bool
	if !callerHasActiveLocks {
		waiters := m.pendingWaitersByInoAndFlags(params.Ino, params.LkFlags)
		for _, w := range waiters {
			if w.SessionID == params.SessionID && w.RequestID == params.RequestID {
				break
			}
			wOwnerKey := formalWaiterOwnerKey(w)
			if wOwnerKey == reqOwnerKey {
				continue
			}
			if lockOracleRangesOverlap(w.Start, w.End, params.Lk.Start, params.Lk.End) &&
				lockOracleConflicts(params.Lk.Type, w.Type) {
				hasEarlierWaiterConflict = true
				break
			}
		}
	}

	if !hasActiveConflict && !hasEarlierWaiterConflict {
		if params.LkFlags == meta.FuseLkFlock {
			m.grantFlockLockUnchecked(params.SessionID, params.Ino, params.Fh, params.LockOwner, params.Lk)
		} else {
			m.grantPosixLockUnchecked(params.SessionID, params.Ino, params.Fh, params.LockOwner, params.Lk)
		}
		m.removeWaiterByKey(params.SessionID, params.RequestID)
		delete(h.FlockOrigHeldMap, formalWaiterKey{SessionID: params.SessionID, RequestID: params.RequestID})
		return false, flockOriginallyHeld, nil
	}

	if m.HasDeadlockCycle(
		params.Ino,
		params.LkFlags,
		reqOwnerKey,
		params.Lk.Start,
		params.Lk.End,
		params.Lk.Type,
		params.SessionID,
		params.RequestID,
	) {
		m.removeWaiterByKey(params.SessionID, params.RequestID)
		delete(h.FlockOrigHeldMap, formalWaiterKey{SessionID: params.SessionID, RequestID: params.RequestID})
		return false, flockOriginallyHeld, meta.ErrDeadlock
	}

	m.upsertPendingWaiter(params)
	key := formalWaiterKey{SessionID: params.SessionID, RequestID: params.RequestID}
	h.FlockOrigHeldMap[key] = flockOriginallyHeld
	return true, flockOriginallyHeld, nil
}

// isWaiterReadyInModel 判断内存参考模型中的某条 PENDING 等待者当前是否满足唤醒授锁条件。
func (h *formalLockWaitHarness) isWaiterReadyInModel(w *formalLockWaiterEntry) bool {
	m := h.Env.Model
	if !m.isSessionActive(w.SessionID) {
		return false
	}

	key := formalWaiterKey{SessionID: w.SessionID, RequestID: w.RequestID}
	callerHasActiveLocks := h.FlockOrigHeldMap[key]
	reqOwnerKey := formalWaiterOwnerKey(w)

	if w.LkFlags == meta.FuseLkFlock {
		for _, l := range m.activeFlockLocksByIno(w.Ino) {
			ownerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh}
			if ownerKey == reqOwnerKey {
				callerHasActiveLocks = true
				continue
			}
			if lockOracleConflicts(w.Type, l.Type) {
				return false
			}
		}
	} else {
		for _, l := range m.activePosixLocksByIno(w.Ino) {
			ownerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
			if ownerKey == reqOwnerKey {
				callerHasActiveLocks = true
				continue
			}
			if lockOracleRangesOverlap(l.Start, l.End, w.Start, w.End) &&
				lockOracleConflicts(w.Type, l.Type) {
				return false
			}
		}
	}

	if !callerHasActiveLocks {
		waiters := m.pendingWaitersByInoAndFlags(w.Ino, w.LkFlags)
		for _, earlier := range waiters {
			if earlier.SessionID == w.SessionID && earlier.RequestID == w.RequestID {
				break
			}
			earlierOwnerKey := formalWaiterOwnerKey(earlier)
			if earlierOwnerKey == reqOwnerKey {
				continue
			}
			if lockOracleRangesOverlap(earlier.Start, earlier.End, w.Start, w.End) &&
				lockOracleConflicts(w.Type, earlier.Type) {
				return false
			}
		}
	}

	return true
}

// settleWokenWaiters 在锁释放、降级、取消或过期清理动作后，按内存参考模型推导所有应被唤醒的 PENDING 等待者，
// 等待对应的后台 SetLkw 协程完成授锁返回 nil，并同步更新内存模型。
func (h *formalLockWaitHarness) settleWokenWaiters(t *testing.T) {
	t.Helper()
	m := h.Env.Model

	for {
		var readyWaiter *formalLockWaiterEntry
		// 按全局等待顺序 (SeqID ASC, SessionID ASC, RequestID ASC) 扫描
		for _, w := range m.Waiters {
			if w.Status != meta.WaiterStatusPending {
				continue
			}
			if h.isWaiterReadyInModel(w) {
				readyWaiter = w
				break
			}
		}
		if readyWaiter == nil {
			break
		}

		key := formalWaiterKey{SessionID: readyWaiter.SessionID, RequestID: readyWaiter.RequestID}
		aw, ok := h.AsyncWaiters[key]
		if !ok {
			t.Fatalf("内部测试框架错误: 模型等待者 (sess=%d, req=%d) 未找到关联的后台协程%s",
				readyWaiter.SessionID, readyWaiter.RequestID, h.Env.FormatFailureReport())
		}

		select {
		case actErr := <-aw.DoneCh:
			aw.Cancel()
			delete(h.AsyncWaiters, key)
			delete(h.FlockOrigHeldMap, key)
			m.RecordStepf("唤醒授锁对拍 | SetLkw(ino=%d, fh=%d, sess=%d, req=%d, owner=%d, flags=%d, type=%s, range=[%d,%d]) -> 唤醒返回=%v",
				readyWaiter.Ino, readyWaiter.Fh, readyWaiter.SessionID, readyWaiter.RequestID, readyWaiter.LockOwner,
				readyWaiter.LkFlags, formatLockTypeCN(readyWaiter.Type), readyWaiter.Start, readyWaiter.End, actErr)
			if actErr != nil {
				t.Fatalf("阻塞等待者 (sess=%d, req=%d) 唤醒后预期授锁成功(nil)，实际返回 err=%v%s",
					readyWaiter.SessionID, readyWaiter.RequestID, actErr, h.Env.FormatFailureReport())
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("阻塞等待者 (sess=%d, req=%d, ino=%d, flags=%d, range=[%d,%d], type=%s) 在前序冲突解除后 2s 内未被唤醒%s",
				readyWaiter.SessionID, readyWaiter.RequestID, readyWaiter.Ino, readyWaiter.LkFlags,
				readyWaiter.Start, readyWaiter.End, formatLockTypeCN(readyWaiter.Type), h.Env.FormatFailureReport())
		}

		lk := meta.FileLock{
			Start: readyWaiter.Start,
			End:   readyWaiter.End,
			Type:  readyWaiter.Type,
			PID:   readyWaiter.PID,
		}
		if readyWaiter.LkFlags == meta.FuseLkFlock {
			m.grantFlockLockUnchecked(readyWaiter.SessionID, readyWaiter.Ino, readyWaiter.Fh, readyWaiter.LockOwner, lk)
		} else {
			m.grantPosixLockUnchecked(readyWaiter.SessionID, readyWaiter.Ino, readyWaiter.Fh, readyWaiter.LockOwner, lk)
		}
		m.removeWaiterByKey(readyWaiter.SessionID, readyWaiter.RequestID)
	}
}

// waitForDBWaiterPending 轮询数据库底表 file_lock_waiters，确认后台阻塞的 SetLkw 已写入 status = WaiterStatusPending (1)。
func (h *formalLockWaitHarness) waitForDBWaiterPending(t *testing.T, sessionID, requestID uint64, doneCh <-chan error) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		select {
		case earlyErr := <-doneCh:
			t.Fatalf("预期进入 PENDING 阻塞等待的 SetLkw(sess=%d, req=%d) 提前退出: err=%v%s",
				sessionID, requestID, earlyErr, h.Env.FormatFailureReport())
		default:
		}

		var status int16
		err := h.Env.Pool.QueryRow(h.Env.Ctx, `
			SELECT status
			FROM file_lock_waiters
			WHERE session_id = $1 AND request_id = $2
		`, int64(sessionID), int64(requestID)).Scan(&status)
		if err == nil && status == meta.WaiterStatusPending {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}

	t.Fatalf("等待 SetLkw(sess=%d, req=%d) 在底表 file_lock_waiters 写入 PENDING 超时%s",
		sessionID, requestID, h.Env.FormatFailureReport())
}

// execAndVerifySetLkw 在内存参考模型与真实 meta.Manager 上执行 SetLkw：
// - 若模型判定不阻塞（立即授锁、立即解锁、参数/句柄错误或 EDEADLK 死锁拒绝），同步调用并校验返回值；
// - 若模型判定需进入 PENDING 阻塞等待，启动后台 goroutine 调用 SetLkw 并轮询确认底表出现 PENDING 记录。
func (h *formalLockWaitHarness) execAndVerifySetLkw(t *testing.T, params meta.SetLkwParams, desc string) {
	t.Helper()
	m := h.Env.Model

	willBlock, flockOrigHeld, expErr := h.evalSetLkwInitial(params)
	if !willBlock {
		actErr := h.Env.Mgr.SetLkw(h.Env.Ctx, params)
		m.RecordStepf("%s | SetLkw[同步](ino=%d, fh=%d, sess=%d, req=%d, owner=%d, flags=%d, type=%s, range=[%d,%d], pid=%d) -> 期望=%v, 实际=%v",
			desc, params.Ino, params.Fh, params.SessionID, params.RequestID, params.LockOwner, params.LkFlags,
			formatLockTypeCN(params.Lk.Type), params.Lk.Start, params.Lk.End, params.Lk.PID, expErr, actErr)

		if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
			t.Fatalf("SetLkw 同步对拍失败 (%s): 模型期望 err=%v, 实际 err=%v%s",
				desc, expErr, actErr, h.Env.FormatFailureReport())
		}
		h.settleWokenWaiters(t)
		return
	}

	wCtx, cancel := context.WithCancel(h.Env.Ctx)
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- h.Env.Mgr.SetLkw(wCtx, params)
	}()

	h.waitForDBWaiterPending(t, params.SessionID, params.RequestID, doneCh)

	key := formalWaiterKey{SessionID: params.SessionID, RequestID: params.RequestID}
	h.AsyncWaiters[key] = &formalAsyncWaiter{
		Params:              params,
		Cancel:              cancel,
		DoneCh:              doneCh,
		FlockOriginallyHeld: flockOrigHeld,
	}

	m.RecordStepf("%s | SetLkw[入队PENDING](ino=%d, fh=%d, sess=%d, req=%d, owner=%d, flags=%d, type=%s, range=[%d,%d], pid=%d) -> 状态=PENDING",
		desc, params.Ino, params.Fh, params.SessionID, params.RequestID, params.LockOwner, params.LkFlags,
		formatLockTypeCN(params.Lk.Type), params.Lk.Start, params.Lk.End, params.Lk.PID)

	if flockOrigHeld {
		h.settleWokenWaiters(t)
	}
}

// cancelAndVerifyWaiter 取消指定 (sessionID, requestID) 的阻塞 SetLkw 请求，验证其返回 ErrInterrupted (EINTR)，
// 并自动结算因该等待者移除而被级联唤醒的后续兼容等待者。
func (h *formalLockWaitHarness) cancelAndVerifyWaiter(t *testing.T, sessionID, requestID uint64, desc string) {
	t.Helper()
	m := h.Env.Model
	key := formalWaiterKey{SessionID: sessionID, RequestID: requestID}

	aw, ok := h.AsyncWaiters[key]
	if !ok {
		t.Fatalf("取消等待者失败: 未找到活跃等待者 (sess=%d, req=%d)%s",
			sessionID, requestID, h.Env.FormatFailureReport())
	}

	m.removeWaiterByKey(sessionID, requestID)
	delete(h.AsyncWaiters, key)
	delete(h.FlockOrigHeldMap, key)
	h.CancelledWaiters = append(h.CancelledWaiters, key)

	aw.Cancel()

	var actErr error
	select {
	case actErr = <-aw.DoneCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("取消 SetLkw(sess=%d, req=%d) 后等待返回 ErrInterrupted 超时%s",
			sessionID, requestID, h.Env.FormatFailureReport())
	}

	m.RecordStepf("%s | CancelSetLkw(ino=%d, sess=%d, req=%d, owner=%d, flags=%d, type=%s, range=[%d,%d]) -> 期望=%v, 实际=%v",
		desc, aw.Params.Ino, sessionID, requestID, aw.Params.LockOwner, aw.Params.LkFlags,
		formatLockTypeCN(aw.Params.Lk.Type), aw.Params.Lk.Start, aw.Params.Lk.End, meta.ErrInterrupted, actErr)

	if !errors.Is(actErr, meta.ErrInterrupted) {
		t.Fatalf("取消 SetLkw 对拍失败 (%s): 期望 ErrInterrupted, 实际 err=%v%s",
			desc, actErr, h.Env.FormatFailureReport())
	}

	h.settleWokenWaiters(t)
}

// execAndVerifySetLkAndWake 在执行 SetLk 后自动结算被唤醒的阻塞等待者。
func (h *formalLockWaitHarness) execAndVerifySetLkAndWake(t *testing.T, params meta.SetLkParams, desc string) {
	t.Helper()
	h.recordExpiredSessionsBeforePurge()
	execAndVerifySetLk(t, h.Env, params, desc)
	h.settleWokenWaiters(t)
}

// execAndVerifyFlushAndWake 在执行 Flush 后自动结算被唤醒的阻塞等待者。
func (h *formalLockWaitHarness) execAndVerifyFlushAndWake(t *testing.T, params meta.FlushParams, desc string) {
	t.Helper()
	if params.SessionID != nil {
		h.recordExpiredSessionsBeforePurge()
	}
	execAndVerifyFlush(t, h.Env, params, desc)
	h.settleWokenWaiters(t)
}

// execAndVerifyReleaseAndWake 在执行 Release 后自动结算被唤醒的阻塞等待者。
func (h *formalLockWaitHarness) execAndVerifyReleaseAndWake(t *testing.T, params meta.ReleaseParams, desc string) error {
	t.Helper()
	if params.SessionID != nil {
		h.recordExpiredSessionsBeforePurge()
	}
	err := execAndVerifyRelease(t, h.Env, params, desc)
	h.settleWokenWaiters(t)
	return err
}

// execAndVerifyRenewSessionAndWake 在内存模型与真实 meta.Manager 上执行显式 RenewSession，清理已超期会话并结算被唤醒的阻塞等待者。
func (h *formalLockWaitHarness) execAndVerifyRenewSessionAndWake(t *testing.T, sessionID uint64, desc string) {
	t.Helper()
	m := h.Env.Model

	if sessionID == 0 || sessionID > meta.OffsetMax {
		_, actErr := h.Env.Mgr.RenewSession(h.Env.Ctx, sessionID)
		m.RecordStepf("%s | RenewSession(sess=%d) -> 期望=%v, 实际=%v", desc, sessionID, meta.ErrInvalidArgument, actErr)
		if !errors.Is(actErr, meta.ErrInvalidArgument) {
			t.Fatalf("RenewSession 非法入参对拍失败: 期望 ErrInvalidArgument, 实际 %v%s", actErr, h.Env.FormatFailureReport())
		}
		return
	}

	h.recordExpiredSessionsBeforePurge()
	m.purgeExpiredSessionsAndUpsert(sessionID)

	expTime, actErr := h.Env.Mgr.RenewSession(h.Env.Ctx, sessionID)
	m.RecordStepf("%s | RenewSession(sess=%d) -> expiresAt=%v, err=%v", desc, sessionID, expTime, actErr)
	if actErr != nil {
		t.Fatalf("RenewSession(sess=%d) 失败: %v%s", sessionID, actErr, h.Env.FormatFailureReport())
	}
	m.RegisterSession(sessionID, expTime)

	// 若被清除的过期会话自身存在后台阻塞的 SetLkw 协程，它会因句柄级联销毁而唤醒并返回 ErrBadFileHandle
	for key, aw := range h.AsyncWaiters {
		if !m.isSessionActive(key.SessionID) {
			select {
			case wErr := <-aw.DoneCh:
				aw.Cancel()
				delete(h.AsyncWaiters, key)
				delete(h.FlockOrigHeldMap, key)
				if !errors.Is(wErr, meta.ErrBadFileHandle) {
					t.Fatalf("过期会话 (sess=%d, req=%d) 的阻塞 SetLkw 被唤醒后期望返回 ErrBadFileHandle, 实际返回 %v%s",
						key.SessionID, key.RequestID, wErr, h.Env.FormatFailureReport())
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("过期会话 (sess=%d, req=%d) 的阻塞 SetLkw 未在 2s 内退出%s",
					key.SessionID, key.RequestID, h.Env.FormatFailureReport())
			}
		}
	}

	h.settleWokenWaiters(t)
}

// auditFormalLockInv3And4 扫描 PostgreSQL 底表 file_locks、file_lock_waiters、file_handles、client_sessions 与 inodes，
// 强制审计 LOCK-INV-1、LOCK-INV-2、LOCK-INV-3 与 LOCK-INV-4：
// 1. 调用 auditFormalLockInv2 完成 LOCK-INV-1 与 LOCK-INV-2 全量校验；
// 2. 扫描底表 file_lock_waiters，校验 status == WaiterStatusPending (1)、lk_flags ∈ {0, 1}、区间合法性；
// 3. 审计 LOCK-INV-3（局部等待图无环）：对每个 (ino, lk_flags) 基于底表活跃 file_locks 与 PENDING file_lock_waiters 构建等待图，证明图中恒不存在环路；
// 4. 审计 LOCK-INV-4（等待队列一致性、取消清理零残留与租约过期级联清零）：
//   - 底表活跃 PENDING file_lock_waiters 集合与内存模型 pendingWaitersByInoAndFlags 100% 一致；
//   - 所有已取消的 (session_id, request_id) 在底表 file_lock_waiters 中记录数恒为 0；
//   - 所有已过期并经 RenewSession 清理的会话在 client_sessions、file_handles、file_locks、file_lock_waiters 中记录数恒为 0。
func auditFormalLockInv3And4(t *testing.T, h *formalLockWaitHarness) {
	t.Helper()

	env := h.Env
	ctx := env.Ctx
	m := env.Model

	// 1. 先执行 LOCK-INV-1 与 LOCK-INV-2 全量底表审计
	auditFormalLockInv2(t, env)

	inoInt64s := make([]int64, len(m.FileInos))
	for i, ino := range m.FileInos {
		inoInt64s[i] = int64(ino)
	}

	// 2. 检查底表 file_lock_waiters 中是否存在非 PENDING 状态或非法 lk_flags / 非法区间的脏记录
	var abnormalWaiterCount int
	err := env.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM file_lock_waiters
		WHERE ino = ANY($1::bigint[])
		  AND (status <> 1 OR lk_flags NOT IN (0, 1) OR start_offset < 0 OR end_offset < start_offset OR (lk_flags = 1 AND (start_offset <> 0 OR end_offset <> $2)))
	`, inoInt64s, int64(meta.OffsetMax)).Scan(&abnormalWaiterCount)
	if err != nil {
		t.Fatalf("SQL 检查异常 file_lock_waiters 记录失败: %v%s", err, env.FormatFailureReport())
	}
	if abnormalWaiterCount != 0 {
		t.Fatalf("违反 LOCK-INV-3/LOCK-INV-4: 底表 file_lock_waiters 存在 %d 条状态或区间异常记录%s",
			abnormalWaiterCount, env.FormatFailureReport())
	}

	// 3. 批量扫描底表活跃的 file_lock_waiters 记录（按 created_at ASC, session_id ASC, request_id ASC 排序）
	type dbWaiterRow struct {
		SessionID uint64
		RequestID uint64
		Ino       uint64
		Fh        uint64
		LockOwner uint64
		LkFlags   uint32
		Start     uint64
		End       uint64
		Type      uint32
		PID       uint32
		Status    int16
	}

	wRows, err := env.Pool.Query(ctx, `
		SELECT w.session_id, w.request_id, w.ino, w.fh, w.lock_owner, w.lk_flags, w.start_offset, w.end_offset, w.lock_type, w.pid, w.status
		FROM file_lock_waiters w
		JOIN client_sessions s ON w.session_id = s.session_id
		WHERE w.ino = ANY($1::bigint[])
		  AND w.status = 1
		  AND s.expires_at > clock_timestamp()
		ORDER BY w.created_at ASC, w.session_id ASC, w.request_id ASC
	`, inoInt64s)
	if err != nil {
		t.Fatalf("SQL 扫描 file_lock_waiters 失败: %v%s", err, env.FormatFailureReport())
	}

	type nsKey struct {
		Ino     uint64
		LkFlags uint32
	}
	waitersByNS := make(map[nsKey][]dbWaiterRow)
	for wRows.Next() {
		var (
			sid, reqID, rowIno, fh, owner, startOff, endOff int64
			lkFlags, pid                                    int32
			lockType, status                                int16
		)
		if scanErr := wRows.Scan(&sid, &reqID, &rowIno, &fh, &owner, &lkFlags, &startOff, &endOff, &lockType, &pid, &status); scanErr != nil {
			wRows.Close()
			t.Fatalf("扫描 file_lock_waiters 行失败: %v%s", scanErr, env.FormatFailureReport())
		}
		k := nsKey{Ino: uint64(rowIno), LkFlags: uint32(lkFlags)}
		waitersByNS[k] = append(waitersByNS[k], dbWaiterRow{
			SessionID: uint64(sid),
			RequestID: uint64(reqID),
			Ino:       uint64(rowIno),
			Fh:        uint64(fh),
			LockOwner: uint64(owner),
			LkFlags:   uint32(lkFlags),
			Start:     uint64(startOff),
			End:       uint64(endOff),
			Type:      uint32(lockType),
			PID:       uint32(pid),
			Status:    status,
		})
	}
	wRows.Close()
	if err := wRows.Err(); err != nil {
		t.Fatalf("遍历 file_lock_waiters 行出错: %v%s", err, env.FormatFailureReport())
	}

	// 4. 对每个 (ino, lk_flags) 审计 LOCK-INV-3（底表等待图恒无环）与 LOCK-INV-4（底表等待队列与参考模型 100% 一致）
	for _, ino := range m.FileInos {
		for _, lkFlags := range []uint32{0, meta.FuseLkFlock} {
			k := nsKey{Ino: ino, LkFlags: lkFlags}
			actualWaiters := waitersByNS[k]
			expectedWaiters := m.pendingWaitersByInoAndFlags(ino, lkFlags)

			if len(actualWaiters) != len(expectedWaiters) {
				t.Fatalf("违反 LOCK-INV-4(等待队列数量不一致): Ino=%d lk_flags=%d 模型期望 %d 条 PENDING 等待者, 底表实际 %d 条\n模型=%+v\n底表=%+v%s",
					ino, lkFlags, len(expectedWaiters), len(actualWaiters), expectedWaiters, actualWaiters, env.FormatFailureReport())
			}

			for i := range expectedWaiters {
				exp := expectedWaiters[i]
				act := actualWaiters[i]
				if exp.SessionID != act.SessionID ||
					exp.RequestID != act.RequestID ||
					exp.Ino != act.Ino ||
					exp.Fh != act.Fh ||
					exp.LockOwner != act.LockOwner ||
					exp.LkFlags != act.LkFlags ||
					exp.Start != act.Start ||
					exp.End != act.End ||
					exp.Type != act.Type ||
					exp.PID != act.PID ||
					exp.Status != act.Status {
					t.Fatalf("违反 LOCK-INV-4(第 %d 条等待者记录不一致): Ino=%d lk_flags=%d\n模型期望=%+v\n底表实际=%+v%s",
						i, ino, lkFlags, exp, act, env.FormatFailureReport())
				}
			}

			// LOCK-INV-3 底表等待有向图无环校验：
			// 基于当前命名空间下的活跃锁与底表 PENDING 等待者构建等待有向图，检验任意节点出发是否存在回到自身的环
			adj := make(map[formalLockOwnerKey]map[formalLockOwnerKey]struct{})
			addEdge := func(from, to formalLockOwnerKey) {
				if from == to {
					return
				}
				if adj[from] == nil {
					adj[from] = make(map[formalLockOwnerKey]struct{})
				}
				adj[from][to] = struct{}{}
			}

			if lkFlags == meta.FuseLkFlock {
				activeFlocks := m.activeFlockLocksByIno(ino)
				for _, w := range actualWaiters {
					wOwnerKey := formalLockOwnerKey{SessionID: w.SessionID, ID: w.Fh}
					for _, l := range activeFlocks {
						lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh}
						if wOwnerKey != lOwnerKey && lockOracleConflicts(w.Type, l.Type) {
							addEdge(wOwnerKey, lOwnerKey)
						}
					}
				}
			} else {
				activePosix := m.activePosixLocksByIno(ino)
				for _, w := range actualWaiters {
					wOwnerKey := formalLockOwnerKey{SessionID: w.SessionID, ID: w.LockOwner}
					for _, l := range activePosix {
						lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
						if wOwnerKey != lOwnerKey &&
							lockOracleRangesOverlap(l.Start, l.End, w.Start, w.End) &&
							lockOracleConflicts(w.Type, l.Type) {
							addEdge(wOwnerKey, lOwnerKey)
						}
					}
				}
			}

			for startNode := range adj {
				visited := make(map[formalLockOwnerKey]bool)
				var queue []formalLockOwnerKey
				for next := range adj[startNode] {
					visited[next] = true
					queue = append(queue, next)
				}
				for len(queue) > 0 {
					curr := queue[0]
					queue = queue[1:]
					for next := range adj[curr] {
						if next == startNode {
							t.Fatalf("违反 LOCK-INV-3(底表等待图存在死锁环): Ino=%d lk_flags=%d 存在从 %+v 回到自身的环路%s",
								ino, lkFlags, startNode, env.FormatFailureReport())
						}
						if !visited[next] {
							visited[next] = true
							queue = append(queue, next)
						}
					}
				}
			}
		}
	}

	// 5. 审计 LOCK-INV-4(取消清理零残留)：验证所有已取消的 (session_id, request_id) 在 file_lock_waiters 中不存在任何记录
	for _, cKey := range h.CancelledWaiters {
		var residualCount int
		err := env.Pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM file_lock_waiters
			WHERE session_id = $1 AND request_id = $2
		`, int64(cKey.SessionID), int64(cKey.RequestID)).Scan(&residualCount)
		if err != nil {
			t.Fatalf("SQL 校验已取消等待者 (sess=%d, req=%d) 失败: %v%s",
				cKey.SessionID, cKey.RequestID, err, env.FormatFailureReport())
		}
		if residualCount != 0 {
			t.Fatalf("违反 LOCK-INV-4(取消清理残留): 已取消的等待者 (sess=%d, req=%d) 在底表 file_lock_waiters 中仍残留 %d 条记录%s",
				cKey.SessionID, cKey.RequestID, residualCount, env.FormatFailureReport())
		}
	}

	// 6. 审计 LOCK-INV-4(过期会话级联清零)：验证所有已被 RenewSession 清理的过期会话名下无任何锁、等待者或句柄残留
	for _, purgedSid := range h.PurgedSessions {
		if m.isSessionActive(purgedSid) {
			// 若该会话后续被重新注册激活，则跳过历史过期检查
			continue
		}
		var lockCnt, waiterCnt, handleCnt, sessCnt int
		err := env.Pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM file_locks WHERE session_id = $1),
				(SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = $1),
				(SELECT COUNT(*) FROM file_handles WHERE session_id = $1),
				(SELECT COUNT(*) FROM client_sessions WHERE session_id = $1)
		`, int64(purgedSid)).Scan(&lockCnt, &waiterCnt, &handleCnt, &sessCnt)
		if err != nil {
			t.Fatalf("SQL 校验过期会话 %d 级联清理失败: %v%s", purgedSid, err, env.FormatFailureReport())
		}
		if lockCnt != 0 || waiterCnt != 0 || handleCnt != 0 || sessCnt != 0 {
			t.Fatalf("违反 LOCK-INV-4(过期会话级联残留): 过期会话 %d 在 RenewSession 后仍残留 locks=%d, waiters=%d, handles=%d, sessions=%d%s",
				purgedSid, lockCnt, waiterCnt, handleCnt, sessCnt, env.FormatFailureReport())
		}
	}
}

// runFormalLockT13WakeupAndStarvationScenarios 确定性验证：
// 1. SetLkw 参数与句柄异常边界（RequestID=0、超限、EBADF、EISDIR、ENOENT、非整文件 flock）；
// 2. SetLkw 阻塞排队与多路径唤醒（SetLk(F_UNLCK) 局部裂解解锁、SetLkw(F_UNLCK)、SetLk(F_RDLCK) 写锁原地降级、携带 lock_owner 的 Flush、携带 FuseReleaseFlockUnlock 的 Release）；
// 3. BSD flock 在 SetLkw 中的非原子模式转换（首轮删除旧锁并持久化，避免双共享锁升级互锁）；
// 4. 防写锁饥饿规则（存在更早冲突 PENDING 等待者时新请求不可插队，而已持锁拥有者豁免等待队列检查）。
func runFormalLockT13WakeupAndStarvationScenarios(t *testing.T) {
	const testName = "TestFormalLock_T13_SetLkwDeadlockCancel/DeterministicWakeupAndStarvation"
	env := setupFormalLockEnv(t, 0x202604201, 1, 3)
	env.TestName = testName
	h := newFormalLockWaitHarness(env)
	m := env.Model

	ino := m.FileInos[0]
	s1, s2, s3 := m.SessionList[0], m.SessionList[1], m.SessionList[2]
	ownerA, ownerB, ownerC := uint64(1001), uint64(2002), uint64(3003)

	fh1RO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDONLY)
	fh1WO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_WRONLY)
	fh1RW, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDWR)
	fh2RO, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDONLY)
	fh2RW, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDWR)
	fh3RO, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDONLY)
	fh3RW, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDWR)

	// -------------------------------------------------------------------------
	// 1. SetLkw 静态与事务边界校验 (EINVAL / EBADF / EISDIR / ENOENT)
	// -------------------------------------------------------------------------
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: 0,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 10},
	}, "SetLkw_RequestID为0返回EINVAL")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: meta.OffsetMax + 1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 10},
	}, "SetLkw_RequestID超OffsetMax返回EINVAL")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 10},
	}, "SetLkw_FuseLkFlock非整文件区间返回EINVAL")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 10},
	}, "SetLkw_只读句柄申请POSIX写锁返回EBADF")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "SetLkw_只写句柄申请POSIX读锁返回EBADF")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: m.DirIno, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "SetLkw_目录节点返回EISDIR")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: 999999999, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "SetLkw_不存在Inode返回ENOENT")
	auditFormalLockInv3And4(t, h)

	// -------------------------------------------------------------------------
	// 2. SetLkw 阻塞排队与 SetLk(F_UNLCK) 挖空裂解唤醒、写锁原地降级唤醒、SetLkw(F_UNLCK) 唤醒
	// -------------------------------------------------------------------------
	// 2.1 (s1, ownerA) 通过 SetLkw 立即获取 [0, 100] 写锁 (pid=101)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 101},
	}, "(s1,ownerA)立即获取[0,100]写锁")
	auditFormalLockInv3And4(t, h)

	// 2.2 (s2, ownerB) 请求 SetLkw([20, 50], F_WRLCK, pid=201) -> 因冲突进入 PENDING
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_WRLCK, PID: 201},
	}, "(s2,ownerB)请求[20,50]写锁阻塞入队PENDING")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	auditFormalLockInv3And4(t, h)

	// 2.3 (s1, ownerA) 调用 SetLk([20, 50], F_UNLCK) 将自身锁裂解为 [0,19] 与 [51,100]，自动唤醒 (s2, ownerB) 获锁 [20, 50]
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_UNLCK, PID: 102},
	}, "(s1,ownerA)挖空解锁[20,50]唤醒(s2,ownerB)")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	auditFormalLockInv3And4(t, h)

	// 2.4 (s3, ownerC) 请求 SetLkw([20, 50], F_RDLCK, pid=301) -> 被 (s2, ownerB) 的写锁阻塞进入 PENDING
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_RDLCK, PID: 301},
	}, "(s3,ownerC)请求[20,50]读锁阻塞入队PENDING")
	auditFormalLockInv3And4(t, h)

	// 2.5 (s2, ownerB) 调用 SetLk([20, 50], F_RDLCK) 将写锁原地降级为读锁 -> 自动唤醒 (s3, ownerC) 共享读锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2,
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_RDLCK, PID: 202},
	}, "(s2,ownerB)将[20,50]写锁原地降级为读锁唤醒(s3,ownerC)")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	auditFormalLockInv3And4(t, h)

	// 2.6 (s1, ownerA) 请求 SetLkw([20, 50], F_WRLCK, pid=103) -> 等待 s2 与 s3 的双读锁释放
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_WRLCK, PID: 103},
	}, "(s1,ownerA)请求[20,50]写锁等待s2与s3释放读锁")
	auditFormalLockInv3And4(t, h)

	// s2 先通过 SetLk(F_UNLCK) 释放 -> 因 s3 仍持有读锁，s1 继续保持 PENDING
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2,
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_UNLCK, PID: 203},
	}, "(s2,ownerB)释放[20,50]读锁(s3仍持锁故s1继续等待)")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	auditFormalLockInv3And4(t, h)

	// s3 通过 SetLkw(F_UNLCK) 释放 -> s1 被唤醒获锁并与左右紧邻 [0,19]、[51,100] 合并为 [0,100] (pid=103)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, RequestID: h.allocRequestID(),
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_UNLCK, PID: 302},
	}, "(s3,ownerC)通过SetLkw(F_UNLCK)释放[20,50]读锁唤醒s1合并为[0,100]")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	require.Len(t, m.activePosixLocksByIno(ino), 1)
	require.Equal(t, uint32(103), m.activePosixLocksByIno(ino)[0].PID)
	auditFormalLockInv3And4(t, h)

	// -------------------------------------------------------------------------
	// 3. Flush(lock_owner) 唤醒 POSIX SetLkw 与 Release(FuseReleaseFlockUnlock) 唤醒 BSD flock SetLkw
	// -------------------------------------------------------------------------
	// 3.1 (s1, fh1RW) 同时获取 BSD flock F_WRLCK (pid=401)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 401},
	}, "(s1,fh1RW)获取BSD_flock写锁")

	// (s2, ownerB) 在 POSIX 命名空间阻塞等待 [0, 100] 写锁
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 204},
	}, "(s2,ownerB)阻塞等待POSIX[0,100]写锁")
	auditFormalLockInv3And4(t, h)

	// s1 调用携带 ownerA 的 Flush -> 释放 (s1, ownerA) 的 POSIX 锁并唤醒 (s2, ownerB)，同时保留 (s1, fh1RW) 的 BSD flock
	h.execAndVerifyFlushAndWake(t, meta.FlushParams{
		Ino:       ino,
		Fh:        fh1RO,
		LockOwner: &ownerA,
		SessionID: &s1,
	}, "Flush(s1,ownerA)释放POSIX锁唤醒(s2,ownerB)并保留BSD_flock")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	require.Len(t, m.activeFlockLocksByIno(ino), 1)
	auditFormalLockInv3And4(t, h)

	// 3.2 (s2, fh2RO) 在 BSD flock 命名空间阻塞等待整文件写锁
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 402},
	}, "(s2,fh2RO)阻塞等待BSD_flock写锁")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 1)
	auditFormalLockInv3And4(t, h)

	// s1 调用携带 FuseReleaseFlockUnlock 的 Release(fh1RW) -> 销毁 fh1RW、释放 flock 并自动唤醒 (s2, fh2RO)
	relErr := h.execAndVerifyReleaseAndWake(t, meta.ReleaseParams{
		Ino:          ino,
		Fh:           fh1RW,
		LockOwner:    &ownerA,
		ReleaseFlags: meta.FuseReleaseFlockUnlock,
		SessionID:    &s1,
	}, "Release(s1,fh1RW,FuseReleaseFlockUnlock)释放flock唤醒(s2,fh2RO)")
	require.NoError(t, relErr)
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock))
	auditFormalLockInv3And4(t, h)

	fh1RW = replenishHandleAfterRelease(t, env, ino, s1, meta.O_RDWR)

	// 3.3 BSD flock 在 SetLkw 中的非原子模式转换：
	// (s2, fh2RO) 降级为 F_RDLCK，(s1, fh1RW) 也获取 F_RDLCK。
	// 当 (s1, fh1RW) 调用 SetLkw(FuseLkFlock, F_WRLCK) 时，首轮 attempt == 0 先删除 (s1, fh1RW) 的旧 F_RDLCK 并进入 PENDING；
	// 随后 (s2, fh2RO) 调用 SetLkw(FuseLkFlock, F_WRLCK) 时，首轮删除自身 F_RDLCK 后无活跃冲突，且因 flockOriginallyHeld=true 直接获锁而不死锁！
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 403},
	}, "(s2,fh2RO)降级BSD_flock为F_RDLCK")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 404},
	}, "(s1,fh1RW)共享获取BSD_flock读锁")
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 405},
	}, "(s1,fh1RW)调用SetLkw升级flock写锁(首轮删除旧读锁后阻塞等待s2)")
	require.False(t, m.hasActiveFlockOnHandle(ino, s1, fh1RW), "SetLkw(FuseLkFlock) 首轮阻塞时必须已持久化删除调用方旧锁")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 1)
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 406},
	}, "(s2,fh2RO)调用SetLkw升级flock写锁(因s1旧锁已删且自身flockOriginallyHeld=true直接获锁)")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 1, "(s1,fh1RW) 继续等待 (s2,fh2RO) 释放")
	auditFormalLockInv3And4(t, h)

	// (s2, fh2RO) 释放 flock -> (s1, fh1RW) 自动唤醒获锁；随后清理 (s1, fh1RW) 与 (s2, ownerB) 的锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 407},
	}, "(s2,fh2RO)释放flock写锁唤醒(s1,fh1RW)")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock))
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 408},
	}, "清理(s1,fh1RW)的BSD_flock")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 205},
	}, "清理(s2,ownerB)的POSIX写锁")
	auditFormalLockInv3And4(t, h)

	// -------------------------------------------------------------------------
	// 4. 防写锁饥饿规则与已持锁拥有者豁免验证（POSIX 与 BSD flock 双命名空间）
	// -------------------------------------------------------------------------
	// 4.1 POSIX 命名空间：
	// (s1, ownerA) 持有 [0, 100] F_RDLCK；
	// (s2, ownerB) 调用 SetLkw([0, 100], F_WRLCK) 阻塞排队；
	// 此时未持锁的新请求 (s3, ownerC) 申请 [0, 100] F_RDLCK：
	// - SetLk 返回 ErrAgain（不可插队）；
	// - SetLkw 进入 PENDING 排在 s2 之后（不可插队）！
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 501},
	}, "防饥饿:(s1,ownerA)获取[0,100]读锁")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 502},
	}, "防饥饿:(s2,ownerB)请求[0,100]写锁进入PENDING")

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 503},
	}, "防饥饿:未持锁(s3,ownerC)调用SetLk申请共享读锁被PENDING写锁拦截返回EAGAIN")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 504},
	}, "防饥饿:未持锁(s3,ownerC)调用SetLkw申请共享读锁必须排在s2之后进入PENDING")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 2)
	auditFormalLockInv3And4(t, h)

	// 已持有活跃锁的 (s1, ownerA) 对自身锁执行重入扩张或原位升级时，因 callerHasActiveLocks=true，不受队列尾等待者 s2 阻塞！
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 50, End: 120, Type: meta.F_RDLCK, PID: 505},
	}, "已持锁豁免:(s1,ownerA)调用SetLk扩张读锁至[0,120]不受PENDING写锁阻塞")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 120, Type: meta.F_WRLCK, PID: 506},
	}, "已持锁豁免:(s1,ownerA)调用SetLkw将自身[0,120]读锁原位升级为写锁不受队列尾s2阻塞")
	auditFormalLockInv3And4(t, h)

	// 当 (s1, ownerA) 释放 [0, 120] 锁后，排在队首的 (s2, ownerB) 优先唤醒获得 [0, 100] 写锁，而排在后面的 (s3, ownerC) 继续等待！
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 120, Type: meta.F_UNLCK, PID: 507},
	}, "(s1,ownerA)释放[0,120]后队首(s2,ownerB)优先获写锁而(s3,ownerC)继续等待")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	require.Equal(t, s3, m.pendingWaitersByInoAndFlags(ino, 0)[0].SessionID)
	auditFormalLockInv3And4(t, h)

	// 当 (s2, ownerB) 释放 [0, 100] 写锁后，(s3, ownerC) 被唤醒并获得 [0, 100] 读锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 508},
	}, "(s2,ownerB)释放[0,100]写锁后唤醒(s3,ownerC)获得读锁")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	auditFormalLockInv3And4(t, h)

	// 清理 (s3, ownerC) 的读锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 509},
	}, "清理(s3,ownerC)读锁")

	// 4.2 BSD flock 命名空间防写锁饥饿验证：
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 510},
	}, "BSD_flock防饥饿:(s1,fh1RO)获取F_RDLCK")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 511},
	}, "BSD_flock防饥饿:(s2,fh2RW)请求F_WRLCK进入PENDING")

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 512},
	}, "BSD_flock防饥饿:未持锁(s3,fh3RW)调用SetLk申请F_RDLCK被PENDING写锁拦截返回EAGAIN")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 513},
	}, "BSD_flock防饥饿:未持锁(s3,fh3RW)调用SetLkw申请F_RDLCK进入PENDING排队")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 2)
	auditFormalLockInv3And4(t, h)

	// 依次解锁 s1 -> s2 获锁 -> 解锁 s2 -> s3 获锁 -> 解锁 s3
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 514},
	}, "BSD_flock:(s1,fh1RO)解锁后队首(s2,fh2RW)获写锁")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 1)
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 515},
	}, "BSD_flock:(s2,fh2RW)解锁后唤醒(s3,fh3RW)获读锁")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock))
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 516},
	}, "BSD_flock:清理(s3,fh3RW)读锁")
	auditFormalLockInv3And4(t, h)
}

// runFormalLockT13DeadlockScenarios 确定性验证同 (ino, lk_flags) 局部等待图有界死锁检测（EDEADLK）与 LOCK-INV-3 审计：
// 1. 双读锁同时阻塞升级写锁二元环：A(RD) + B(RD) -> A.SetLkw(WR) PENDING -> B.SetLkw(WR) 立即返回 ErrDeadlock；
// 2. 三方循环等待三元环：A([0,10]) -> B([20,30]) -> C([40,50]) -> A([0,10]) 立即返回 ErrDeadlock，且底表绝不写入成环等待者；
// 3. 跨 (ino, lk_flags) 命名空间正交等待不误报死锁（A 在 POSIX 等待 B，B 在 BSD flock 等待 A）及 flock 等待图环路检测。
func runFormalLockT13DeadlockScenarios(t *testing.T) {
	const testName = "TestFormalLock_T13_SetLkwDeadlockCancel/DeterministicDeadlockCycle"
	env := setupFormalLockEnv(t, 0x202604202, 1, 3)
	env.TestName = testName
	h := newFormalLockWaitHarness(env)
	m := env.Model

	ino := m.FileInos[0]
	s1, s2, s3 := m.SessionList[0], m.SessionList[1], m.SessionList[2]
	ownerA, ownerB, ownerC := uint64(1001), uint64(2002), uint64(3003)

	fh1RW, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDWR)
	fh2RW, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDWR)
	fh3RW, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDWR)

	// -------------------------------------------------------------------------
	// 场景 1: 双读锁同时阻塞升级写锁二元环 (A <-> B)
	// -------------------------------------------------------------------------
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 601},
	}, "二元死锁:A(s1,ownerA)获取[0,100]读锁")

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 602},
	}, "二元死锁:B(s2,ownerB)获取[0,100]读锁")

	// A 请求升级 [0, 100] 写锁 -> 与 B 冲突，无环，进入 PENDING (A -> B)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 603},
	}, "二元死锁:A请求升级[0,100]写锁进入PENDING(等待B)")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	auditFormalLockInv3And4(t, h)

	// B 请求升级 [0, 100] 写锁 -> 检测到 B -> A -> B 环路，立即返回 ErrDeadlock (EDEADLK)，绝不写入 file_lock_waiters
	deadlockReqID := h.allocRequestID()
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: deadlockReqID,
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 604},
	}, "二元死锁:B请求升级[0,100]写锁触发B->A->B死锁环返回EDEADLK")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1, "成环请求严禁写入 file_lock_waiters")
	auditFormalLockInv3And4(t, h)

	// B 释放 [0, 100] 读锁后，A 立即被唤醒并成功升级为 [0, 100] 写锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 605},
	}, "二元死锁解除:B释放读锁后A立即唤醒升级为写锁")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	auditFormalLockInv3And4(t, h)

	// 清理 A 的锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 606},
	}, "清理A的[0,100]写锁")

	// -------------------------------------------------------------------------
	// 场景 2: 三方循环等待三元环 (A -> B -> C -> A)
	// -------------------------------------------------------------------------
	// A 持 [0, 10] 写锁，B 持 [20, 30] 写锁，C 持 [40, 50] 写锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 701},
	}, "三方死锁:A持有[0,10]写锁")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_WRLCK, PID: 702},
	}, "三方死锁:B持有[20,30]写锁")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 40, End: 50, Type: meta.F_WRLCK, PID: 703},
	}, "三方死锁:C持有[40,50]写锁")

	// A 请求 [20, 30] -> 等待 B (A -> B)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 20, End: 30, Type: meta.F_WRLCK, PID: 704},
	}, "三方死锁:A请求[20,30]进入PENDING(A->B)")

	// B 请求 [40, 50] -> 等待 C (B -> C)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 40, End: 50, Type: meta.F_WRLCK, PID: 705},
	}, "三方死锁:B请求[40,50]进入PENDING(B->C)")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 2)
	auditFormalLockInv3And4(t, h)

	// C 请求 [0, 10] -> 检测到 C -> A -> B -> C 三元环，立即返回 ErrDeadlock (EDEADLK)
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 706},
	}, "三方死锁:C请求[0,10]触发C->A->B->C三元环立即返回EDEADLK")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 2, "C 的成环请求严禁写入 file_lock_waiters")
	auditFormalLockInv3And4(t, h)

	// C 释放 [40, 50] -> B 自动唤醒获锁 [40, 50]；随后 B 释放 [20, 50] -> A 自动唤醒获锁 [20, 30]
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 40, End: 50, Type: meta.F_UNLCK, PID: 707},
	}, "三方死锁解链1:C释放[40,50]唤醒B")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 20, End: 50, Type: meta.F_UNLCK, PID: 708},
	}, "三方死锁解链2:B释放[20,50]唤醒A")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	auditFormalLockInv3And4(t, h)

	// 清理 A 的锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 50, Type: meta.F_UNLCK, PID: 709},
	}, "清理A的全部POSIX锁")

	// -------------------------------------------------------------------------
	// 场景 3: 跨命名空间 (POSIX vs BSD flock) 等待图正交隔离验证与 BSD flock 等待图环检测
	// -------------------------------------------------------------------------
	// A 持 POSIX [0, 50] 写锁，B 持 BSD flock 整文件写锁。
	// A 在 BSD flock 命名空间等待 B，同时 B 在 POSIX 命名空间等待 A -> 因处于不同 lk_flags 命名空间，局部等待图互不干扰，均正常入队 PENDING！
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 50, Type: meta.F_WRLCK, PID: 801},
	}, "跨命名空间等待:A持有POSIX[0,50]写锁")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 802},
	}, "跨命名空间等待:B持有BSD_flock写锁")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: h.allocRequestID(),
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 803},
	}, "跨命名空间等待:A在FuseLkFlock命名空间等待B(PENDING)")

	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: h.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 50, Type: meta.F_WRLCK, PID: 804},
	}, "跨命名空间等待:B在POSIX命名空间等待A(正交命名空间不误报死锁,进入PENDING)")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 1)
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock), 1)
	auditFormalLockInv3And4(t, h)

	// 分别释放 POSIX 与 BSD flock 锁，验证双命名空间等待者各自唤醒授锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 50, Type: meta.F_UNLCK, PID: 805},
	}, "跨命名空间解链:A释放POSIX写锁唤醒B")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 806},
	}, "跨命名空间解链:B释放BSD_flock写锁唤醒A")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0))
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock))
	auditFormalLockInv3And4(t, h)

	// 清理剩余锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 50, Type: meta.F_UNLCK, PID: 807},
	}, "清理B的POSIX写锁")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 808},
	}, "清理A的BSD_flock写锁")
	auditFormalLockInv3And4(t, h)

	// -------------------------------------------------------------------------
	// 场景 4: BSD flock 命名空间下二次尝试 (attempt >= 1) 死锁环检测 (EDEADLK)
	// -------------------------------------------------------------------------
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 850},
	}, "BSD_flock死锁环:(s3,fh3RW)持有F_RDLCK")

	req851 := h.allocRequestID()
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, RequestID: req851,
		LkFlags: meta.FuseLkFlock,
		Lk:      meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 851},
	}, "BSD_flock死锁环:(s1,fh1RW)请求F_WRLCK进入PENDING")

	// (s1, fh1RW) 在已存在自身 PENDING waiter 时调用 SetLk(F_RDLCK)，成功共享获取活跃 F_RDLCK
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 852},
	}, "BSD_flock死锁环:(s1,fh1RW)通过SetLk获取活跃F_RDLCK")
	auditFormalLockInv3And4(t, h)

	// 验证模型 HasDeadlockCycle 对 FuseLkFlock 环路的判定：若 (s3, fh3RW) 尝试在不释放自身读锁的情况下等待 (s1, fh1RW)，HasDeadlockCycle 必返回 true
	require.True(t, m.HasDeadlockCycle(
		ino,
		meta.FuseLkFlock,
		formalLockOwnerKey{SessionID: s3, ID: fh3RW},
		0, meta.OffsetMax, meta.F_WRLCK,
		s3, 99999,
	), "HasDeadlockCycle 必须精确识别 FuseLkFlock 下的 (s3,fh3RW) -> (s1,fh1RW) -> (s3,fh3RW) 死锁环")

	// 先释放 (s1, fh1RW) 的 F_RDLCK 再释放 (s3, fh3RW) 的 F_RDLCK，使 req851 干净唤醒获锁
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 853},
	}, "BSD_flock:(s1,fh1RW)释放临时F_RDLCK")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerC, SessionID: s3, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 854},
	}, "BSD_flock:(s3,fh3RW)释放F_RDLCK唤醒(s1,fh1RW)获写锁")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock))
	auditFormalLockInv3And4(t, h)

	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 855},
	}, "BSD_flock:清理(s1,fh1RW)写锁")
	auditFormalLockInv3And4(t, h)
}

// runFormalLockT13CancelAndSessionExpiryScenarios 确定性验证：
// 1. SetLkw 阻塞期间上下文取消（context.WithCancel / WithTimeout）返回 ErrInterrupted (EINTR)，
//    底表 LOCK-INV-4 确认无残留 PENDING 记录、无悬挂锁，且取消中间阻塞的写锁等待者后自动级联唤醒后续兼容读锁等待者；
// 2. 毫秒级短租约 WithSessionLeaseTTL(40ms) 会话超期 + 显式 RenewSession 清理：
//    过期会话名下的 file_locks 与 file_lock_waiters 记录数全部清零，且因过期会话锁释放而满足条件的阻塞 SetLkw 等待者被正确唤醒授锁。
func runFormalLockT13CancelAndSessionExpiryScenarios(t *testing.T) {
	const testName = "TestFormalLock_T13_SetLkwDeadlockCancel/DeterministicCancelAndSessionExpiry"

	// -------------------------------------------------------------------------
	// 部分 A: 上下文取消 (WithCancel / WithTimeout) 与级联唤醒后面被挡住的兼容读锁等待者
	// -------------------------------------------------------------------------
	env := setupFormalLockEnv(t, 0x202604203, 1, 3)
	env.TestName = testName
	h := newFormalLockWaitHarness(env)
	m := env.Model

	ino := m.FileInos[0]
	s1, s2, s3 := m.SessionList[0], m.SessionList[1], m.SessionList[2]
	ownerA, ownerB, ownerC := uint64(1001), uint64(2002), uint64(3003)

	fh1RO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDONLY)
	fh1RW, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDWR)
	fh2RW, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDWR)
	fh3RO, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDONLY)

	// 1. (s1, ownerA) 持有 [0, 100] F_RDLCK
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 901},
	}, "取消级联唤醒:(s1,ownerA)获取[0,100]读锁")

	// 2. (s2, ownerB) 调用 SetLkw([0, 100], F_WRLCK) 阻塞排队
	s2ReqID := h.allocRequestID()
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: s2ReqID,
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 902},
	}, "取消级联唤醒:(s2,ownerB)请求[0,100]写锁进入PENDING")

	// 3. (s3, ownerC) 调用 SetLkw([0, 100], F_RDLCK) 因被 s2 的 PENDING 写锁挡住而进入 PENDING
	s3ReqID := h.allocRequestID()
	h.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, RequestID: s3ReqID,
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 903},
	}, "取消级联唤醒:(s3,ownerC)请求[0,100]读锁被s2挡住进入PENDING")
	require.Len(t, m.pendingWaitersByInoAndFlags(ino, 0), 2)
	auditFormalLockInv3And4(t, h)

	// 4. 取消 (s2, s2ReqID)：
	// - s2 返回 ErrInterrupted (EINTR)；
	// - cleanupCancelledWaiter 删除 (s2, s2ReqID) 并触发 notifyLockWaiters(ino)；
	// - 排在 s2 后面的兼容读锁等待者 (s3, s3ReqID) 自动被级联唤醒并成功获得 [0, 100] 共享读锁！
	h.cancelAndVerifyWaiter(t, s2, s2ReqID, "取消中间阻塞写锁s2并级联唤醒后续读锁s3")
	require.Empty(t, m.pendingWaitersByInoAndFlags(ino, 0), "s2 取消后 s3 必须已被级联唤醒授锁，等待队列应为空")
	require.Len(t, m.activePosixLocksByIno(ino), 2, "s1 与 s3 应共同持有 [0, 100] 共享读锁")
	auditFormalLockInv3And4(t, h)

	// 5. 验证 context.WithTimeout 超时取消：
	// 先释放 s3 的读锁并将 s1 升级为 [0, 100] 写锁，然后用 15ms WithTimeout 调用 SetLkw
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh3RO, LockOwner: ownerC, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 904},
	}, "释放s3读锁")
	h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 905},
	}, "s1升级[0,100]写锁")

	timeoutReqID := h.allocRequestID()
	tCtx, tCancel := context.WithTimeout(env.Ctx, 15*time.Millisecond)
	timeoutErr := env.Mgr.SetLkw(tCtx, meta.SetLkwParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, RequestID: timeoutReqID,
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 906},
	})
	tCancel()
	require.ErrorIs(t, timeoutErr, meta.ErrInterrupted, "WithTimeout 超时应返回 ErrInterrupted (EINTR)")
	h.CancelledWaiters = append(h.CancelledWaiters, formalWaiterKey{SessionID: s2, RequestID: timeoutReqID})
	auditFormalLockInv3And4(t, h)

	// -------------------------------------------------------------------------
	// 部分 B: 毫秒级短租约 WithSessionLeaseTTL(40ms) 会话超期 + RenewSession 锁与等待队列级联清零及唤醒
	// -------------------------------------------------------------------------
	shortEnv := setupFormalLockEnv(t, 0x202604204, 2, 2, meta.WithSessionLeaseTTL(40*time.Millisecond))
	shortEnv.TestName = testName
	shortH := newFormalLockWaitHarness(shortEnv)
	shortM := shortEnv.Model

	file0, file1 := shortM.FileInos[0], shortM.FileInos[1]
	sExp, sKeep := shortM.SessionList[0], shortM.SessionList[1]

	fhExpF0, _ := shortM.findHandleForSessionAndMode(file0, sExp, meta.O_RDWR)
	fhExpF1, _ := shortM.findHandleForSessionAndMode(file1, sExp, meta.O_RDWR)
	fhKeepF0, _ := shortM.findHandleForSessionAndMode(file0, sKeep, meta.O_RDWR)
	fhKeepF1, _ := shortM.findHandleForSessionAndMode(file1, sKeep, meta.O_RDWR)

	// 1. 令 sExp 在 file0 上持有 POSIX 写锁 [0, 100] 与 BSD flock 写锁，
	//    同时令 sKeep 在 file1 上持有 [0, 100] 写锁，并在底表为 sExp 在 file1 上写入一条 PENDING 等待记录（模拟崩溃客户端遗留的锁与等待记录）
	shortH.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: file0, Fh: fhExpF0, LockOwner: ownerA, SessionID: sExp, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1001},
	}, "短租约:sExp在file0获取POSIX[0,100]写锁")
	shortH.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: file0, Fh: fhExpF0, LockOwner: ownerA, SessionID: sExp, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 1002},
	}, "短租约:sExp在file0获取BSD_flock写锁")
	shortH.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: file1, Fh: fhKeepF1, LockOwner: ownerB, SessionID: sKeep, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1003},
	}, "短租约:sKeep在file1获取POSIX[0,100]写锁")

	expWaiterReqID := shortH.allocRequestID()
	_, err := shortEnv.Pool.Exec(shortEnv.Ctx, `
		INSERT INTO file_lock_waiters (session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid, status)
		VALUES ($1, $2, $3, $4, $5, 0, 0, 100, 1, 1004, 1)
	`, int64(sExp), int64(expWaiterReqID), int64(file1), int64(fhExpF1), int64(ownerA))
	require.NoError(t, err)
	shortM.upsertPendingWaiter(meta.SetLkwParams{
		Ino: file1, Fh: fhExpF1, LockOwner: ownerA, SessionID: sExp, RequestID: expWaiterReqID,
		LkFlags: 0, Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1004},
	})
	auditFormalLockInv3And4(t, shortH)

	// 2. 等待 20ms 后先为 sKeep 续期（此时 sExp 距 40ms TTL 尚未到期），再等待 25ms 使 sExp 超期而 sKeep 仍有效！
	time.Sleep(20 * time.Millisecond)
	_, err = shortEnv.Mgr.RenewSession(shortEnv.Ctx, sKeep)
	require.NoError(t, err)

	time.Sleep(25 * time.Millisecond)
	shortM.MarkSessionExpired(sExp)

	// 此时 sExp 在底表中已超期但尚未物理删除（惰性清理）；
	// 令 sKeep 调用 RenewSession 触发同步清理，验证 sExp 的 file_locks、file_lock_waiters、file_handles、client_sessions 全部清零！
	shortH.execAndVerifyRenewSessionAndWake(t, sKeep, "短租约:sKeep调用RenewSession同步清理过期sExp的锁与等待队列")
	auditFormalLockInv3And4(t, shortH)

	// 3. 验证过期会话锁释放唤醒正在阻塞的 SetLkw 等待者：
	// 注册新会话 sExp2，打开 file0 句柄并获取 [0, 100] F_WRLCK；
	// 随后 sKeep 调用 SetLkw([0, 100], F_WRLCK) 进入 PENDING 阻塞等待；
	// 等待 sExp2 超期并执行 RenewSession(sKeep)，验证 sKeep 的阻塞 SetLkw 被唤醒授锁且 sExp2 记录全部清零！
	sExp2 := sExp + 9999
	exp2Time, err := shortEnv.Mgr.RenewSession(shortEnv.Ctx, sExp2)
	require.NoError(t, err)
	shortM.RegisterSession(sExp2, exp2Time)

	openExp2, err := shortEnv.Mgr.Open(shortEnv.Ctx, meta.OpenParams{
		Ino:       file0,
		Flags:     meta.O_RDWR,
		SessionID: &sExp2,
	})
	require.NoError(t, err)
	shortM.RegisterHandle(openExp2.Fh, file0, &sExp2, meta.O_RDWR)

	shortH.execAndVerifySetLkAndWake(t, meta.SetLkParams{
		Ino: file0, Fh: openExp2.Fh, LockOwner: ownerA, SessionID: sExp2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1005},
	}, "短租约唤醒:sExp2在file0获取[0,100]写锁")

	// 续期 sKeep 后，sKeep 发起阻塞 SetLkw([0, 100], F_WRLCK) 等待 sExp2
	_, err = shortEnv.Mgr.RenewSession(shortEnv.Ctx, sKeep)
	require.NoError(t, err)

	shortH.execAndVerifySetLkw(t, meta.SetLkwParams{
		Ino: file0, Fh: fhKeepF0, LockOwner: ownerB, SessionID: sKeep, RequestID: shortH.allocRequestID(),
		LkFlags: 0,
		Lk:      meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 1006},
	}, "短租约唤醒:sKeep在file0阻塞等待sExp2的[0,100]写锁")
	require.Len(t, shortM.pendingWaitersByInoAndFlags(file0, 0), 1)

	// 等待 45ms 使 sExp2 租约过期，标记 sExp2 过期并调用 RenewSession(sKeep)，验证 sKeep 被唤醒获锁且 sExp2 底表记录清零
	time.Sleep(45 * time.Millisecond)
	shortM.MarkSessionExpired(sExp2)
	shortH.execAndVerifyRenewSessionAndWake(t, sKeep, "短租约唤醒:sExp2超期后RenewSession清理并唤醒sKeep获锁")

	require.Empty(t, shortM.pendingWaitersByInoAndFlags(file0, 0))
	require.Len(t, shortM.activePosixLocksByIno(file0), 1)
	require.Equal(t, sKeep, shortM.activePosixLocksByIno(file0)[0].SessionID)
	auditFormalLockInv3And4(t, shortH)
}

// stepFormalLockT13Action 在单步随机状态机中混合生成：
// - 若当前存在 PENDING 阻塞等待者，高频执行唤醒（SetLk/SetLkw 解锁、Flush、Release、写锁降级）、防写锁饥饿碰撞、死锁环尝试（EDEADLK）或取消（EINTR）；
// - 若当前无 PENDING 等待者或随机命中新动作，生成 POSIX / BSD flock 的 SetLkw（含阻塞入队、立即授锁、死锁环、参数边界）与 SetLk / Flush / Release。
func stepFormalLockT13Action(t *testing.T, h *formalLockWaitHarness, rng *rand.Rand, stepIdx int) {
	t.Helper()

	env := h.Env
	m := env.Model
	pid := uint32(2000 + stepIdx)

	// 1. 若当前存在活跃的 PENDING 等待者，65% 概率针对性地围绕该等待者执行：
	//    - 防写锁饥饿探测（新拥有者尝试 SetLk 插队 -> EAGAIN）
	//    - 成环死锁探测（锁持有者反向调用 SetLkw 等待该等待者持有的锁 -> EDEADLK）
	//    - 上下文取消（cancelAndVerifyWaiter -> EINTR）
	//    - 前序冲突锁释放唤醒（SetLk(F_UNLCK) / SetLkw(F_UNLCK) / Flush / Release）
	if len(m.Waiters) > 0 && rng.IntN(100) < 65 {
		w := m.Waiters[0]
		actionRoll := rng.IntN(100)

		switch {
		case actionRoll < 20:
			// 20%: 测试防写锁饥饿或反向死锁检测
			if w.LkFlags == 0 {
				// 找一个与 w 冲突的活跃 POSIX 锁持有者，若 w 的持有者也持有某把活跃锁，则让该活跃锁持有者反向 SetLkw 请求 w 持有的锁以触发 EDEADLK！
				activePosix := m.activePosixLocksByIno(w.Ino)
				var wOwnedLock *formalPosixLockSegment
				var blockerLock *formalPosixLockSegment
				for _, l := range activePosix {
					if l.SessionID == w.SessionID && l.LockOwner == w.LockOwner {
						wOwnedLock = l
					} else if lockOracleRangesOverlap(l.Start, l.End, w.Start, w.End) && lockOracleConflicts(w.Type, l.Type) {
						blockerLock = l
					}
				}
				if wOwnedLock != nil && blockerLock != nil {
					fh, ok := m.findHandleForSessionAndMode(w.Ino, blockerLock.SessionID, meta.O_RDWR)
					require.True(t, ok)
					h.execAndVerifySetLkw(t, meta.SetLkwParams{
						Ino:       w.Ino,
						Fh:        fh,
						LockOwner: blockerLock.LockOwner,
						SessionID: blockerLock.SessionID,
						RequestID: h.allocRequestID(),
						LkFlags:   0,
						Lk: meta.FileLock{
							Start: wOwnedLock.Start,
							End:   wOwnedLock.End,
							Type:  meta.F_WRLCK,
							PID:   pid,
						},
					}, "随机状态机:反向SetLkw触发有界死锁检测(EDEADLK)")
					return
				}
			}
			// 否则挑一个非等待者自身的会话发起 SetLk 验证防写锁饥饿或冲突拦截
			sid := m.SessionList[rng.IntN(len(m.SessionList))]
			owner := m.OwnerList[rng.IntN(len(m.OwnerList))]
			fh, ok := m.findHandleForSessionAndMode(w.Ino, sid, meta.O_RDWR)
			require.True(t, ok)
			for _, pw := range m.pendingWaitersByInoAndFlags(w.Ino, w.LkFlags) {
				if (w.LkFlags == meta.FuseLkFlock && pw.SessionID == sid && pw.Fh == fh) ||
					(w.LkFlags == 0 && pw.SessionID == sid && pw.LockOwner == owner) {
					return
				}
			}
			h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
				Ino:       w.Ino,
				Fh:        fh,
				LockOwner: owner,
				SessionID: sid,
				LkFlags:   w.LkFlags,
				Lk: meta.FileLock{
					Start: w.Start,
					End:   w.End,
					Type:  meta.F_RDLCK,
					PID:   pid,
				},
			}, "随机状态机:存在PENDING等待者时发起SetLk碰撞")
			return

		case actionRoll < 50:
			// 30%: 取消该阻塞等待者 (EINTR) 并审计底表零残留
			h.cancelAndVerifyWaiter(t, w.SessionID, w.RequestID, "随机状态机:取消PENDING等待者(EINTR)")
			return

		default:
			// 50%: 释放阻挡该等待者的前序活跃锁（通过 SetLk(F_UNLCK)、SetLkw(F_UNLCK)、Flush 或 Release），触发自动唤醒授锁！
			if w.LkFlags == meta.FuseLkFlock {
				activeFlocks := m.activeFlockLocksByIno(w.Ino)
				for _, l := range activeFlocks {
					if l.SessionID == w.SessionID && l.Fh == w.Fh {
						continue
					}
					if lockOracleConflicts(w.Type, l.Type) {
						if hState, valid := m.isHandleValid(l.Fh, w.Ino, &l.SessionID); valid && rng.IntN(3) == 0 {
							sidCopy := l.SessionID
							accMode := hState.Flags & meta.O_ACCMODE
							err := h.execAndVerifyReleaseAndWake(t, meta.ReleaseParams{
								Ino:          w.Ino,
								Fh:           l.Fh,
								ReleaseFlags: meta.FuseReleaseFlockUnlock,
								SessionID:    &sidCopy,
							}, "随机状态机:通过Release(FuseReleaseFlockUnlock)释放前序flock唤醒等待者")
							require.NoError(t, err)
							replenishHandleAfterRelease(t, env, w.Ino, l.SessionID, accMode)
						} else {
							fh := l.Fh
							if _, valid := m.isHandleValid(fh, w.Ino, &l.SessionID); !valid {
								continue
							}
							h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
								Ino:       w.Ino,
								Fh:        fh,
								LockOwner: l.LockOwner,
								SessionID: l.SessionID,
								LkFlags:   meta.FuseLkFlock,
								Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: pid},
							}, "随机状态机:释放前序BSD_flock唤醒等待者")
						}
						return
					}
				}
			} else {
				activePosix := m.activePosixLocksByIno(w.Ino)
				for _, l := range activePosix {
					if l.SessionID == w.SessionID && l.LockOwner == w.LockOwner {
						continue
					}
					if lockOracleRangesOverlap(l.Start, l.End, w.Start, w.End) && lockOracleConflicts(w.Type, l.Type) {
						fh, ok := m.findHandleForSessionAndMode(w.Ino, l.SessionID, meta.O_RDWR)
						require.True(t, ok)
						unlockWay := rng.IntN(3)
						switch unlockWay {
						case 0:
							h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
								Ino:       w.Ino,
								Fh:        fh,
								LockOwner: l.LockOwner,
								SessionID: l.SessionID,
								LkFlags:   0,
								Lk:        meta.FileLock{Start: l.Start, End: l.End, Type: meta.F_UNLCK, PID: pid},
							}, "随机状态机:通过SetLk(F_UNLCK)释放前序POSIX锁唤醒等待者")
						case 1:
							h.execAndVerifySetLkw(t, meta.SetLkwParams{
								Ino:       w.Ino,
								Fh:        fh,
								LockOwner: l.LockOwner,
								SessionID: l.SessionID,
								RequestID: h.allocRequestID(),
								LkFlags:   0,
								Lk:        meta.FileLock{Start: l.Start, End: l.End, Type: meta.F_UNLCK, PID: pid},
							}, "随机状态机:通过SetLkw(F_UNLCK)释放前序POSIX锁唤醒等待者")
						default:
							sidCopy := l.SessionID
							ownerCopy := l.LockOwner
							h.execAndVerifyFlushAndWake(t, meta.FlushParams{
								Ino:       w.Ino,
								Fh:        fh,
								LockOwner: &ownerCopy,
								SessionID: &sidCopy,
							}, "随机状态机:通过Flush(lock_owner)释放前序POSIX锁唤醒等待者")
						}
						return
					}
				}
			}
		}
	}

	// 2. 常规随机动作生成
	ino := m.FileInos[rng.IntN(len(m.FileInos))]
	sid := m.SessionList[rng.IntN(len(m.SessionList))]
	owner := m.OwnerList[rng.IntN(len(m.OwnerList))]

	roll := rng.IntN(100)
	switch {
	case roll < 45:
		// 45%: POSIX SetLkw 动作（若该 (ino, 0) 下尚无 PENDING 等待者，则允许发起可能阻塞或死锁的 SetLkw；否则仅发起不冲突或同拥有者动作，确保唤醒顺序 100% 确定）
		start, end := generateRandomPosixRange(rng)
		var lkType uint32
		tRoll := rng.IntN(10)
		switch {
		case tRoll < 4:
			lkType = meta.F_RDLCK
		case tRoll < 8:
			lkType = meta.F_WRLCK
		default:
			lkType = meta.F_UNLCK
		}

		accMode := uint32(meta.O_RDWR)
		if lkType == meta.F_RDLCK && rng.IntN(2) == 0 {
			accMode = meta.O_RDONLY
		} else if lkType == meta.F_WRLCK && rng.IntN(2) == 0 {
			accMode = meta.O_WRONLY
		}
		fh, ok := m.findHandleForSessionAndMode(ino, sid, accMode)
		require.True(t, ok)

		// 若当前 (ino, 0) 已有 PENDING 等待者，改用非阻塞 SetLk 执行（且跳过等待者自身 ownerKey），避免多个互相冲突的异步等待者并发竞争同一锁导致非确定性获锁顺序
		if len(m.pendingWaitersByInoAndFlags(ino, 0)) > 0 {
			for _, pw := range m.pendingWaitersByInoAndFlags(ino, 0) {
				if pw.SessionID == sid && pw.LockOwner == owner {
					return
				}
			}
			if lkType != meta.F_UNLCK {
				h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
					Ino:       ino,
					Fh:        fh,
					LockOwner: owner,
					SessionID: sid,
					LkFlags:   0,
					Lk:        meta.FileLock{Start: start, End: end, Type: lkType, PID: pid},
				}, "随机状态机:POSIX_SetLk加锁")
				return
			}
		}

		h.execAndVerifySetLkw(t, meta.SetLkwParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: owner,
			SessionID: sid,
			RequestID: h.allocRequestID(),
			LkFlags:   0,
			Lk:        meta.FileLock{Start: start, End: end, Type: lkType, PID: pid},
		}, "随机状态机:POSIX_SetLkw加解锁")

	case roll < 75:
		// 30%: BSD flock SetLkw 动作
		modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
		fh, ok := m.findHandleForSessionAndMode(ino, sid, modes[rng.IntN(len(modes))])
		require.True(t, ok)

		var lkType uint32
		tRoll := rng.IntN(10)
		switch {
		case tRoll < 4:
			lkType = meta.F_RDLCK
		case tRoll < 8:
			lkType = meta.F_WRLCK
		default:
			lkType = meta.F_UNLCK
		}

		if len(m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock)) > 0 {
			for _, pw := range m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock) {
				if pw.SessionID == sid && pw.Fh == fh {
					return
				}
			}
			if lkType != meta.F_UNLCK {
				// 若已有 flock 等待者，避免使用已持有活跃 flock 的句柄去删锁唤醒或新增第二个冲突等待者，改由未持锁句柄调用 SetLk 检验防饥饿 EAGAIN
				if !m.hasActiveFlockOnHandle(ino, sid, fh) {
					h.execAndVerifySetLkAndWake(t, meta.SetLkParams{
						Ino:       ino,
						Fh:        fh,
						LockOwner: owner,
						SessionID: sid,
						LkFlags:   meta.FuseLkFlock,
						Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: lkType, PID: pid},
					}, "随机状态机:BSD_flock_SetLk防饥饿与冲突探测")
				}
				return
			}
		}

		h.execAndVerifySetLkw(t, meta.SetLkwParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: owner,
			SessionID: sid,
			RequestID: h.allocRequestID(),
			LkFlags:   meta.FuseLkFlock,
			Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: lkType, PID: pid},
		}, "随机状态机:BSD_flock_SetLkw加解锁")

	case roll < 90:
		// 15%: 执行显式 RenewSession 或 Flush
		if rng.IntN(2) == 0 {
			h.execAndVerifyRenewSessionAndWake(t, sid, "随机状态机:RenewSession会话续期")
		} else {
			fh, ok := m.findHandleForSessionAndMode(ino, sid, meta.O_RDWR)
			require.True(t, ok)
			sidCopy := sid
			ownerCopy := owner
			h.execAndVerifyFlushAndWake(t, meta.FlushParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: &ownerCopy,
				SessionID: &sidCopy,
			}, "随机状态机:Flush携带lock_owner")
		}

	default:
		// 10%: SetLkw 异常入参或只读/只写句柄权限不匹配边界
		fhRO, _ := m.findHandleForSessionAndMode(ino, sid, meta.O_RDONLY)
		switch rng.IntN(4) {
		case 0:
			h.execAndVerifySetLkw(t, meta.SetLkwParams{
				Ino: ino, Fh: fhRO, LockOwner: owner, SessionID: sid, RequestID: 0,
				Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: pid},
			}, "随机状态机:SetLkw_RequestID=0(EINVAL)")
		case 1:
			h.execAndVerifySetLkw(t, meta.SetLkwParams{
				Ino: ino, Fh: fhRO, LockOwner: owner, SessionID: sid, RequestID: h.allocRequestID(),
				Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: pid},
			}, "随机状态机:SetLkw_O_RDONLY写锁(EBADF)")
		case 2:
			h.execAndVerifySetLkw(t, meta.SetLkwParams{
				Ino: ino, Fh: fhRO, LockOwner: owner, SessionID: sid, RequestID: h.allocRequestID(),
				LkFlags: meta.FuseLkFlock,
				Lk:      meta.FileLock{Start: 0, End: 99, Type: meta.F_RDLCK, PID: pid},
			}, "随机状态机:SetLkw_flock非整文件区间(EINVAL)")
		default:
			h.execAndVerifySetLkw(t, meta.SetLkwParams{
				Ino: m.DirIno, Fh: fhRO, LockOwner: owner, SessionID: sid, RequestID: h.allocRequestID(),
				Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: pid},
			}, "随机状态机:SetLkw_目录节点(EISDIR)")
		}
	}
}

// TestFormalLock_T13_SetLkwDeadlockCancel 验证副状态机 SetLkw 阻塞排队唤醒、防写锁饥饿、
// 同 (ino, lk_flags) 有界死锁检测（EDEADLK）、上下文取消清理（EINTR）、毫秒级短租约过期 RenewSession 清理，
// 以及底表 LOCK-INV-1 ~ LOCK-INV-4 全量 SQL 审计。
func TestFormalLock_T13_SetLkwDeadlockCancel(t *testing.T) {
	const testName = "TestFormalLock_T13_SetLkwDeadlockCancel"

	t.Run("DeterministicWakeupAndStarvation", func(t *testing.T) {
		runFormalLockT13WakeupAndStarvationScenarios(t)
	})

	t.Run("DeterministicDeadlockCycle", func(t *testing.T) {
		runFormalLockT13DeadlockScenarios(t)
	})

	t.Run("DeterministicCancelAndSessionExpiry", func(t *testing.T) {
		runFormalLockT13CancelAndSessionExpiryScenarios(t)
	})

	cfg := loadFormalLockConfig(t)
	t.Logf("启动 T13 SetLkw 阻塞唤醒、死锁检测与取消清理形式化状态机验证: baseSeed=0x%x, runs=%d, steps=%d", cfg.BaseSeed, cfg.Runs, cfg.Steps)

	for runIdx := 0; runIdx < cfg.Runs; runIdx++ {
		runSeed := cfg.BaseSeed + uint64(runIdx)
		t.Run(fmt.Sprintf("Run_%d_Seed_0x%x", runIdx, runSeed), func(t *testing.T) {
			env := setupFormalLockEnv(t, runSeed, 2, 3)
			env.TestName = testName
			env.Steps = cfg.Steps
			h := newFormalLockWaitHarness(env)
			rng := rand.New(rand.NewPCG(runSeed, runSeed^0x6a09e667f3bcc908))

			for step := 1; step <= cfg.Steps; step++ {
				stepFormalLockT13Action(t, h, rng, step)
				verifyFormalLockT12GetLkProbes(t, env, rng)
				auditFormalLockInv3And4(t, h)
			}

			// 收尾阶段：若仍有阻塞中的 PENDING 等待者，全部执行取消清理并验证底表零残留
			for len(env.Model.Waiters) > 0 {
				w := env.Model.Waiters[0]
				h.cancelAndVerifyWaiter(t, w.SessionID, w.RequestID, "收尾清理剩余PENDING等待者")
			}
			auditFormalLockInv3And4(t, h)

			// 校验所有受测文件时间戳与初始快照严格一致
			for _, ino := range env.Model.FileInos {
				attrRes, err := env.Mgr.GetAttr(env.Ctx, ino)
				require.NoError(t, err)
				expInode := env.Model.Inodes[ino]
				require.True(t, attrRes.Attr.Atime.Equal(expInode.Atime), "GetAttr atime 不得被 SetLkw 或锁操作修改")
				require.True(t, attrRes.Attr.Mtime.Equal(expInode.Mtime), "GetAttr mtime 不得被 SetLkw 或锁操作修改")
				require.True(t, attrRes.Attr.Ctime.Equal(expInode.Ctime), "GetAttr ctime 不得被 SetLkw 或锁操作修改")
			}
		})
	}
}
