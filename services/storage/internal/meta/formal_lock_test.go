package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// formalLockConfig 定义文件锁副状态机的运行参数配置。
// 支持通过环境变量 BOWL_FORMAL_SEED、BOWL_FORMAL_RUNS、BOWL_FORMAL_STEPS 控制随机种子重放与深度。
type formalLockConfig struct {
	BaseSeed uint64
	Runs     int
	Steps    int
}

func loadFormalLockConfig(t *testing.T) formalLockConfig {
	t.Helper()

	defaultRuns := 1
	defaultSteps := 8
	if isFormalDeepMode() {
		defaultRuns = 2
		defaultSteps = 50
	}

	ov := parseFormalEnvOverrides(t, defaultRuns, defaultSteps)
	baseSeed := uint64(time.Now().UnixNano())
	if ov.HasFixedSeed {
		baseSeed = ov.FixedSeed
	}

	return formalLockConfig{
		BaseSeed: baseSeed,
		Runs:     ov.Runs,
		Steps:    ov.Steps,
	}
}

// formalLockInodeState 记录副状态机中受测 Inode 的元数据与初始时间戳快照。
type formalLockInodeState struct {
	Ino   uint64
	Mode  uint32
	IsDir bool
	Nlink uint32
	Atime time.Time
	Mtime time.Time
	Ctime time.Time
}

// formalLockSessionState 记录副状态机中的客户端会话状态。
type formalLockSessionState struct {
	SessionID uint64
	Expired   bool
}

// formalLockHandleState 记录副状态机中打开的文件句柄状态。
type formalLockHandleState struct {
	Fh        uint64
	Ino       uint64
	SessionID *uint64
	Flags     uint32
	Closed    bool
}

// formalPosixLockSegment 表示内存参考模型中的单条 POSIX 字节区间锁（lk_flags == 0）。
type formalPosixLockSegment struct {
	SeqID     int64
	SessionID uint64
	Ino       uint64
	Fh        uint64
	LockOwner uint64
	Start     uint64
	End       uint64
	Type      uint32
	PID       uint32
}

// formalBSDFileLock 表示内存参考模型中的单条 BSD flock 整文件锁（lk_flags == FuseLkFlock）。
type formalBSDFileLock struct {
	SeqID     int64
	SessionID uint64
	Ino       uint64
	Fh        uint64
	LockOwner uint64
	Type      uint32
	PID       uint32
}

// formalLockWaiterEntry 表示内存参考模型中的阻塞锁等待者记录（供 T13 阻塞队列与死锁检测扩展）。
type formalLockWaiterEntry struct {
	SeqID     int64
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

// formalLockOwnerKey 标识同命名空间下的锁归属键（POSIX 为 (session_id, lock_owner)，BSD flock 为 (session_id, fh)）。
type formalLockOwnerKey struct {
	SessionID uint64
	ID        uint64
}

// formalLockModel 是文件锁副状态机的纯 Go 内存参考模型（Reference Oracle）。
// 一次性涵盖 T11（POSIX 字节区间裂解合并与 GetLk 探针）、T12（BSD flock 与双命名空间正交 Flush/Release）
// 以及 T13（SetLkw 阻塞等待队列、防写锁饥饿、有界死锁检测与会话超期清理）所需的完整状态结构。
type formalLockModel struct {
	Seed        uint64
	DiskID      string
	RootIno     uint64
	FileInos    []uint64
	DirIno      uint64
	Inodes      map[uint64]*formalLockInodeState
	Sessions    map[uint64]*formalLockSessionState
	SessionList []uint64
	OwnerList   []uint64
	Handles     map[uint64]*formalLockHandleState
	HandleList  []uint64

	PosixLocks []*formalPosixLockSegment
	FlockLocks []*formalBSDFileLock
	Waiters    []*formalLockWaiterEntry

	nextLockSeq   int64
	nextWaiterSeq int64
	Trace         []string
}

func newFormalLockModel(seed uint64, diskID string, rootIno uint64) *formalLockModel {
	return &formalLockModel{
		Seed:          seed,
		DiskID:        diskID,
		RootIno:       rootIno,
		Inodes:        make(map[uint64]*formalLockInodeState),
		Sessions:      make(map[uint64]*formalLockSessionState),
		Handles:       make(map[uint64]*formalLockHandleState),
		nextLockSeq:   1,
		nextWaiterSeq: 1,
	}
}

func (m *formalLockModel) RecordStepf(format string, args ...any) {
	stepNo := len(m.Trace) + 1
	msg := fmt.Sprintf(format, args...)
	m.Trace = append(m.Trace, fmt.Sprintf("[步骤 %03d] %s", stepNo, msg))
}

func (m *formalLockModel) FormatTrace(testName string, steps int) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n=== 文件锁副状态机反例轨迹 (Seed=0x%016x / %d) ===\n", m.Seed, m.Seed))
	sb.WriteString(fmt.Sprintf("重放命令: BOWL_FORMAL_SEED=0x%016x BOWL_FORMAL_RUNS=1 BOWL_FORMAL_STEPS=%d GOWORK=off go -C services/storage test -v ./internal/meta -run '^%s$'\n", m.Seed, steps, testName))
	for _, line := range m.Trace {
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	return sb.String()
}

func (m *formalLockModel) RegisterInode(inode *meta.Inode, isDir bool) {
	m.Inodes[inode.Ino] = &formalLockInodeState{
		Ino:   inode.Ino,
		Mode:  inode.Attr.Mode,
		IsDir: isDir,
		Nlink: inode.Attr.Nlink,
		Atime: inode.Attr.Atime,
		Mtime: inode.Attr.Mtime,
		Ctime: inode.Attr.Ctime,
	}
	if isDir && inode.Ino != m.RootIno {
		m.DirIno = inode.Ino
	} else if !isDir {
		m.FileInos = append(m.FileInos, inode.Ino)
	}
}

func (m *formalLockModel) RegisterSession(sessionID uint64, expiresAt time.Time) {
	if _, exists := m.Sessions[sessionID]; !exists {
		m.SessionList = append(m.SessionList, sessionID)
	}
	m.Sessions[sessionID] = &formalLockSessionState{
		SessionID: sessionID,
		Expired:   false,
	}
}

func (m *formalLockModel) RegisterHandle(fh, ino uint64, sessionID *uint64, flags uint32) {
	var sessCopy *uint64
	if sessionID != nil {
		v := *sessionID
		sessCopy = &v
	}
	m.Handles[fh] = &formalLockHandleState{
		Fh:        fh,
		Ino:       ino,
		SessionID: sessCopy,
		Flags:     flags,
		Closed:    false,
	}
	m.HandleList = append(m.HandleList, fh)
}

func (m *formalLockModel) isSessionActive(sessionID uint64) bool {
	s, ok := m.Sessions[sessionID]
	return ok && !s.Expired
}

func (m *formalLockModel) isHandleValid(fh, ino uint64, sessionID *uint64) (*formalLockHandleState, bool) {
	h, ok := m.Handles[fh]
	if !ok || h.Closed || h.Ino != ino {
		return nil, false
	}
	if h.SessionID != nil {
		if !m.isSessionActive(*h.SessionID) {
			return nil, false
		}
	}
	if sessionID != nil {
		if h.SessionID == nil || *h.SessionID != *sessionID {
			return nil, false
		}
	} else {
		if h.SessionID != nil {
			return nil, false
		}
	}
	return h, true
}

// MarkSessionExpired 在内存模型中标记指定会话已超期（此时底表尚未物理清理，直到下次 renewClientSession 触发）。
func (m *formalLockModel) MarkSessionExpired(sessionID uint64) {
	if s, ok := m.Sessions[sessionID]; ok {
		s.Expired = true
	}
}

// purgeExpiredSessionsAndUpsert 模拟 renewClientSession：物理移除已过期会话名下的句柄、锁与等待者，并激活当前 sessionID。
func (m *formalLockModel) purgeExpiredSessionsAndUpsert(sessionID uint64) {
	for _, sid := range m.SessionList {
		sess, exists := m.Sessions[sid]
		if !exists || !sess.Expired {
			continue
		}
		for _, fh := range m.HandleList {
			if h, ok := m.Handles[fh]; ok && h.SessionID != nil && *h.SessionID == sid {
				h.Closed = true
			}
		}
		keptPosix := m.PosixLocks[:0]
		for _, l := range m.PosixLocks {
			if l.SessionID != sid {
				keptPosix = append(keptPosix, l)
			}
		}
		m.PosixLocks = keptPosix

		keptFlock := m.FlockLocks[:0]
		for _, l := range m.FlockLocks {
			if l.SessionID != sid {
				keptFlock = append(keptFlock, l)
			}
		}
		m.FlockLocks = keptFlock

		keptWaiters := m.Waiters[:0]
		for _, w := range m.Waiters {
			if w.SessionID != sid {
				keptWaiters = append(keptWaiters, w)
			}
		}
		m.Waiters = keptWaiters
		delete(m.Sessions, sid)
	}

	m.RegisterSession(sessionID, time.Now().Add(meta.DefaultSessionLeaseTTL))
}

func lockOracleRangesOverlap(start1, end1, start2, end2 uint64) bool {
	return start1 <= end2 && end1 >= start2
}

func lockOracleConflicts(type1, type2 uint32) bool {
	return type1 == meta.F_WRLCK || type2 == meta.F_WRLCK
}

func lockOracleValidateCommon(ino, fh, sessionID uint64, lkFlags uint32, lk meta.FileLock) error {
	if ino == 0 || ino > math.MaxInt64 ||
		fh == 0 || fh > math.MaxInt64 ||
		sessionID == 0 || sessionID > math.MaxInt64 {
		return meta.ErrInvalidArgument
	}
	if lkFlags > meta.FuseLkFlock {
		return meta.ErrInvalidArgument
	}
	if lk.Start > meta.OffsetMax || lk.End > meta.OffsetMax || lk.Start > lk.End {
		return meta.ErrInvalidArgument
	}
	if lkFlags == meta.FuseLkFlock && (lk.Start != 0 || lk.End != meta.OffsetMax) {
		return meta.ErrInvalidArgument
	}
	return nil
}

func lockOracleValidatePosixOpenMode(lockType uint32, handleFlags uint32) error {
	accMode := handleFlags & meta.O_ACCMODE
	switch lockType {
	case meta.F_RDLCK:
		if accMode != meta.O_RDONLY && accMode != meta.O_RDWR {
			return meta.ErrBadFileHandle
		}
	case meta.F_WRLCK:
		if accMode != meta.O_WRONLY && accMode != meta.O_RDWR {
			return meta.ErrBadFileHandle
		}
	}
	return nil
}

// activePosixLocksByIno 返回指定 Ino 上未过期会话持有的活跃 POSIX 锁，按 (Start ASC, End ASC, SeqID ASC) 排序。
func (m *formalLockModel) activePosixLocksByIno(ino uint64) []*formalPosixLockSegment {
	var out []*formalPosixLockSegment
	for _, l := range m.PosixLocks {
		if l.Ino == ino && m.isSessionActive(l.SessionID) {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		if out[i].End != out[j].End {
			return out[i].End < out[j].End
		}
		return out[i].SeqID < out[j].SeqID
	})
	return out
}

// activeFlockLocksByIno 返回指定 Ino 上未过期会话持有的活跃 BSD flock 锁，按 SeqID ASC 排序。
func (m *formalLockModel) activeFlockLocksByIno(ino uint64) []*formalBSDFileLock {
	var out []*formalBSDFileLock
	for _, l := range m.FlockLocks {
		if l.Ino == ino && m.isSessionActive(l.SessionID) {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SeqID < out[j].SeqID
	})
	return out
}

// pendingWaitersByInoAndFlags 返回指定 (Ino, LkFlags) 下未过期会话的 PENDING 等待者，按 (SeqID ASC, SessionID ASC, RequestID ASC) 排序。
func (m *formalLockModel) pendingWaitersByInoAndFlags(ino uint64, lkFlags uint32) []*formalLockWaiterEntry {
	var out []*formalLockWaiterEntry
	for _, w := range m.Waiters {
		if w.Ino == ino && w.LkFlags == lkFlags && w.Status == meta.WaiterStatusPending && m.isSessionActive(w.SessionID) {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SeqID != out[j].SeqID {
			return out[i].SeqID < out[j].SeqID
		}
		if out[i].SessionID != out[j].SessionID {
			return out[i].SessionID < out[j].SessionID
		}
		return out[i].RequestID < out[j].RequestID
	})
	return out
}

// splitAndRemovePosixLockRange 在内存模型中移除指定拥有者与 [reqStart, reqEnd] 相交（及授锁时紧邻同类型）的旧 POSIX 锁段，
// 并将被截断的左右残余锁段追加回模型，返回同类型合并后的 [mergedStart, mergedEnd]。
func (m *formalLockModel) splitAndRemovePosixLockRange(
	sessionID, ino, fh, lockOwner uint64,
	reqStart, reqEnd uint64,
	reqType uint32,
	isUnlock bool,
) (uint64, uint64) {
	activeLocks := m.activePosixLocksByIno(ino)
	deleteSet := make(map[int64]bool)
	var remnants []*formalPosixLockSegment
	mergedStart := reqStart
	mergedEnd := reqEnd

	for _, l := range activeLocks {
		if l.SessionID != sessionID || l.LockOwner != lockOwner {
			continue
		}
		overlaps := lockOracleRangesOverlap(l.Start, l.End, reqStart, reqEnd)
		adjacentSameType := !isUnlock && l.Type == reqType &&
			((l.End < meta.OffsetMax && l.End+1 == reqStart) || (reqEnd < meta.OffsetMax && reqEnd+1 == l.Start))
		if !overlaps && !adjacentSameType {
			continue
		}

		deleteSet[l.SeqID] = true
		if !isUnlock && l.Type == reqType {
			if l.Start < mergedStart {
				mergedStart = l.Start
			}
			if l.End > mergedEnd {
				mergedEnd = l.End
			}
		} else {
			if l.Start < reqStart {
				remnants = append(remnants, &formalPosixLockSegment{
					SessionID: sessionID,
					Ino:       ino,
					Fh:        fh,
					LockOwner: lockOwner,
					Start:     l.Start,
					End:       reqStart - 1,
					Type:      l.Type,
					PID:       l.PID,
				})
			}
			if l.End > reqEnd && reqEnd < meta.OffsetMax {
				remnants = append(remnants, &formalPosixLockSegment{
					SessionID: sessionID,
					Ino:       ino,
					Fh:        fh,
					LockOwner: lockOwner,
					Start:     reqEnd + 1,
					End:       l.End,
					Type:      l.Type,
					PID:       l.PID,
				})
			}
		}
	}

	if len(deleteSet) > 0 {
		kept := m.PosixLocks[:0]
		for _, l := range m.PosixLocks {
			if !deleteSet[l.SeqID] {
				kept = append(kept, l)
			}
		}
		m.PosixLocks = kept
	}

	for _, seg := range remnants {
		seg.SeqID = m.nextLockSeq
		m.nextLockSeq++
		m.PosixLocks = append(m.PosixLocks, seg)
	}

	return mergedStart, mergedEnd
}

// grantPosixLockUnchecked 在已确认无跨拥有者冲突的前提下，对指定拥有者执行 POSIX 区间覆盖、裂解与紧邻合并授锁。
func (m *formalLockModel) grantPosixLockUnchecked(sessionID, ino, fh, lockOwner uint64, lk meta.FileLock) {
	mergedStart, mergedEnd := m.splitAndRemovePosixLockRange(sessionID, ino, fh, lockOwner, lk.Start, lk.End, lk.Type, false)

	seq := m.nextLockSeq
	m.nextLockSeq++
	m.PosixLocks = append(m.PosixLocks, &formalPosixLockSegment{
		SeqID:     seq,
		SessionID: sessionID,
		Ino:       ino,
		Fh:        fh,
		LockOwner: lockOwner,
		Start:     mergedStart,
		End:       mergedEnd,
		Type:      lk.Type,
		PID:       lk.PID,
	})
}

// grantFlockLockUnchecked 在已确认无跨句柄冲突的前提下，对指定 (sessionID, ino, fh) 授予整文件 BSD flock 锁。
func (m *formalLockModel) grantFlockLockUnchecked(sessionID, ino, fh, lockOwner uint64, lk meta.FileLock) {
	kept := m.FlockLocks[:0]
	for _, l := range m.FlockLocks {
		if l.Ino == ino && l.SessionID == sessionID && l.Fh == fh {
			continue
		}
		kept = append(kept, l)
	}
	m.FlockLocks = kept

	seq := m.nextLockSeq
	m.nextLockSeq++
	m.FlockLocks = append(m.FlockLocks, &formalBSDFileLock{
		SeqID:     seq,
		SessionID: sessionID,
		Ino:       ino,
		Fh:        fh,
		LockOwner: lockOwner,
		Type:      lk.Type,
		PID:       lk.PID,
	})
}

// ApplyGetLk 在内存参考模型上执行 GetLk 冲突锁探测，不修改任何模型状态。
func (m *formalLockModel) ApplyGetLk(params meta.GetLkParams) (*meta.FileLock, error) {
	if err := lockOracleValidateCommon(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return nil, err
	}
	if params.Lk.Type != meta.F_RDLCK && params.Lk.Type != meta.F_WRLCK {
		return nil, meta.ErrInvalidArgument
	}

	inode, ok := m.Inodes[params.Ino]
	if !ok {
		return nil, meta.ErrNotFound
	}
	if inode.IsDir {
		return nil, meta.ErrIsDir
	}

	if _, valid := m.isHandleValid(params.Fh, params.Ino, &params.SessionID); !valid {
		return nil, meta.ErrBadFileHandle
	}

	if params.LkFlags == meta.FuseLkFlock {
		flocks := m.activeFlockLocksByIno(params.Ino)
		for _, l := range flocks {
			if l.SessionID == params.SessionID && l.Fh == params.Fh {
				continue
			}
			if lockOracleConflicts(params.Lk.Type, l.Type) {
				return &meta.FileLock{
					Start: 0,
					End:   meta.OffsetMax,
					Type:  l.Type,
					PID:   l.PID,
				}, nil
			}
		}
		return &meta.FileLock{
			Start: params.Lk.Start,
			End:   params.Lk.End,
			Type:  meta.F_UNLCK,
			PID:   0,
		}, nil
	}

	posixLocks := m.activePosixLocksByIno(params.Ino)
	for _, l := range posixLocks {
		if l.SessionID == params.SessionID && l.LockOwner == params.LockOwner {
			continue
		}
		if lockOracleRangesOverlap(l.Start, l.End, params.Lk.Start, params.Lk.End) &&
			lockOracleConflicts(params.Lk.Type, l.Type) {
			return &meta.FileLock{
				Start: l.Start,
				End:   l.End,
				Type:  l.Type,
				PID:   l.PID,
			}, nil
		}
	}

	return &meta.FileLock{
		Start: params.Lk.Start,
		End:   params.Lk.End,
		Type:  meta.F_UNLCK,
		PID:   0,
	}, nil
}

// ApplySetLk 在内存参考模型上执行 SetLk 非阻塞加解锁状态转移。
func (m *formalLockModel) ApplySetLk(params meta.SetLkParams) error {
	if err := lockOracleValidateCommon(params.Ino, params.Fh, params.SessionID, params.LkFlags, params.Lk); err != nil {
		return err
	}
	if params.Lk.Type != meta.F_RDLCK && params.Lk.Type != meta.F_WRLCK && params.Lk.Type != meta.F_UNLCK {
		return meta.ErrInvalidArgument
	}

	inode, ok := m.Inodes[params.Ino]
	if !ok {
		return meta.ErrNotFound
	}
	if inode.IsDir {
		return meta.ErrIsDir
	}

	handle, valid := m.isHandleValid(params.Fh, params.Ino, &params.SessionID)
	if !valid {
		return meta.ErrBadFileHandle
	}

	if params.LkFlags == 0 {
		if err := lockOracleValidatePosixOpenMode(params.Lk.Type, handle.Flags); err != nil {
			return err
		}
	}

	m.purgeExpiredSessionsAndUpsert(params.SessionID)

	if params.LkFlags == meta.FuseLkFlock {
		return m.applyFlockSetLk(params)
	}
	return m.applyPosixSetLk(params)
}

func (m *formalLockModel) applyFlockSetLk(params meta.SetLkParams) error {
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

	if params.Lk.Type == meta.F_UNLCK {
		return nil
	}

	activeFlocks := m.activeFlockLocksByIno(params.Ino)
	for _, l := range activeFlocks {
		if l.SessionID == params.SessionID && l.Fh == params.Fh {
			continue
		}
		if lockOracleConflicts(params.Lk.Type, l.Type) {
			return meta.ErrAgain
		}
	}

	if deletedCount == 0 {
		waiters := m.pendingWaitersByInoAndFlags(params.Ino, meta.FuseLkFlock)
		for _, w := range waiters {
			if w.SessionID == params.SessionID && w.Fh == params.Fh {
				continue
			}
			if lockOracleConflicts(params.Lk.Type, w.Type) {
				return meta.ErrAgain
			}
		}
	}

	m.grantFlockLockUnchecked(params.SessionID, params.Ino, params.Fh, params.LockOwner, params.Lk)
	return nil
}

func (m *formalLockModel) applyPosixSetLk(params meta.SetLkParams) error {
	reqStart := params.Lk.Start
	reqEnd := params.Lk.End
	reqType := params.Lk.Type

	if reqType == meta.F_UNLCK {
		m.splitAndRemovePosixLockRange(params.SessionID, params.Ino, params.Fh, params.LockOwner, reqStart, reqEnd, reqType, true)
		return nil
	}

	activeLocks := m.activePosixLocksByIno(params.Ino)
	var callerHasActiveLocks bool
	for _, l := range activeLocks {
		if l.SessionID == params.SessionID && l.LockOwner == params.LockOwner {
			callerHasActiveLocks = true
			continue
		}
		if lockOracleRangesOverlap(l.Start, l.End, reqStart, reqEnd) && lockOracleConflicts(reqType, l.Type) {
			return meta.ErrAgain
		}
	}

	if !callerHasActiveLocks {
		waiters := m.pendingWaitersByInoAndFlags(params.Ino, 0)
		for _, w := range waiters {
			if w.SessionID == params.SessionID && w.LockOwner == params.LockOwner {
				continue
			}
			if lockOracleRangesOverlap(w.Start, w.End, reqStart, reqEnd) && lockOracleConflicts(reqType, w.Type) {
				return meta.ErrAgain
			}
		}
	}

	m.grantPosixLockUnchecked(params.SessionID, params.Ino, params.Fh, params.LockOwner, params.Lk)
	return nil
}

// ApplyFlush 在内存参考模型上执行 Flush（供 T12 双命名空间正交对拍复用）。
func (m *formalLockModel) ApplyFlush(params meta.FlushParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return meta.ErrInvalidArgument
	}
	if params.SessionID != nil && (*params.SessionID == 0 || *params.SessionID > math.MaxInt64) {
		return meta.ErrInvalidArgument
	}

	inode, ok := m.Inodes[params.Ino]
	if !ok {
		return meta.ErrNotFound
	}
	if inode.IsDir {
		return meta.ErrIsDir
	}

	if _, valid := m.isHandleValid(params.Fh, params.Ino, params.SessionID); !valid {
		return meta.ErrBadFileHandle
	}

	if params.LockOwner == nil && params.SessionID == nil {
		return nil
	}

	if params.SessionID != nil {
		m.purgeExpiredSessionsAndUpsert(*params.SessionID)
	}

	if params.LockOwner != nil {
		owner := *params.LockOwner
		kept := m.PosixLocks[:0]
		for _, l := range m.PosixLocks {
			if l.Ino == params.Ino && l.LockOwner == owner {
				if params.SessionID == nil || l.SessionID == *params.SessionID {
					continue
				}
			}
			kept = append(kept, l)
		}
		m.PosixLocks = kept
	}
	return nil
}

// ApplyRelease 在内存参考模型上执行 Release（供 T12 双命名空间正交对拍复用）。
func (m *formalLockModel) ApplyRelease(params meta.ReleaseParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return meta.ErrInvalidArgument
	}
	if (params.ReleaseFlags &^ (meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock)) != 0 {
		return meta.ErrInvalidArgument
	}
	if params.SessionID != nil && (*params.SessionID == 0 || *params.SessionID > math.MaxInt64) {
		return meta.ErrInvalidArgument
	}

	inode, ok := m.Inodes[params.Ino]
	if !ok {
		return meta.ErrNotFound
	}
	if inode.IsDir {
		return meta.ErrIsDir
	}

	h, valid := m.isHandleValid(params.Fh, params.Ino, params.SessionID)
	if !valid {
		return meta.ErrBadFileHandle
	}

	if params.SessionID != nil {
		m.purgeExpiredSessionsAndUpsert(*params.SessionID)
	}

	h.Closed = true

	if (params.ReleaseFlags & meta.FuseReleaseFlockUnlock) != 0 {
		kept := m.FlockLocks[:0]
		for _, l := range m.FlockLocks {
			if l.Ino == params.Ino && l.Fh == params.Fh {
				if params.SessionID == nil || l.SessionID == *params.SessionID {
					continue
				}
			}
			kept = append(kept, l)
		}
		m.FlockLocks = kept
	}
	return nil
}

// HasDeadlockCycle 在内存参考模型上构建同 (Ino, LkFlags) 局部有向等待图检测是否存在环路（供 T13 死锁检测复用）。
func (m *formalLockModel) HasDeadlockCycle(
	ino uint64,
	lkFlags uint32,
	reqOwnerKey formalLockOwnerKey,
	reqStart, reqEnd uint64,
	reqType uint32,
	reqSessionID, reqRequestID uint64,
) bool {
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
		active := m.activeFlockLocksByIno(ino)
		waiters := m.pendingWaitersByInoAndFlags(ino, meta.FuseLkFlock)
		for _, l := range active {
			lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh}
			if lOwnerKey != reqOwnerKey && lockOracleConflicts(reqType, l.Type) {
				addEdge(reqOwnerKey, lOwnerKey)
			}
		}
		for _, w := range waiters {
			if w.SessionID == reqSessionID && w.RequestID == reqRequestID {
				continue
			}
			wOwnerKey := formalLockOwnerKey{SessionID: w.SessionID, ID: w.Fh}
			for _, l := range active {
				lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.Fh}
				if lOwnerKey != wOwnerKey && lockOracleConflicts(w.Type, l.Type) {
					addEdge(wOwnerKey, lOwnerKey)
				}
			}
		}
	} else {
		active := m.activePosixLocksByIno(ino)
		waiters := m.pendingWaitersByInoAndFlags(ino, 0)
		for _, l := range active {
			lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
			if lOwnerKey != reqOwnerKey && lockOracleRangesOverlap(l.Start, l.End, reqStart, reqEnd) && lockOracleConflicts(reqType, l.Type) {
				addEdge(reqOwnerKey, lOwnerKey)
			}
		}
		for _, w := range waiters {
			if w.SessionID == reqSessionID && w.RequestID == reqRequestID {
				continue
			}
			wOwnerKey := formalLockOwnerKey{SessionID: w.SessionID, ID: w.LockOwner}
			for _, l := range active {
				lOwnerKey := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
				if lOwnerKey != wOwnerKey && lockOracleRangesOverlap(l.Start, l.End, w.Start, w.End) && lockOracleConflicts(w.Type, l.Type) {
					addEdge(wOwnerKey, lOwnerKey)
				}
			}
		}
	}

	startTargets := adj[reqOwnerKey]
	if len(startTargets) == 0 {
		return false
	}
	visited := make(map[formalLockOwnerKey]bool)
	queue := make([]formalLockOwnerKey, 0, len(startTargets))
	for target := range startTargets {
		if target == reqOwnerKey {
			return true
		}
		visited[target] = true
		queue = append(queue, target)
	}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		for next := range adj[curr] {
			if next == reqOwnerKey {
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

// formalLockTestEnv 封装副状态机单轮运行的真实 PostgreSQL 连接池、meta.Manager 与内存参考模型。
type formalLockTestEnv struct {
	Ctx      context.Context
	Pool     *pgxpool.Pool
	Store    db.Store
	Mgr      meta.Manager
	Model    *formalLockModel
	TestName string
	Steps    int
}

// FormatFailureReport 格式化当前文件锁副状态机的失败反例轨迹与重放命令。
func (e *formalLockTestEnv) FormatFailureReport() string {
	steps := e.Steps
	if steps <= 0 {
		steps = 1
	}
	return e.Model.FormatTrace(e.TestName, steps)
}

// setupFormalLockEnv 初始化测试专属 Disk、常规文件 Inode、子目录 Inode、多客户端会话与三类打开模式句柄（O_RDONLY, O_WRONLY, O_RDWR）。
func setupFormalLockEnv(t *testing.T, seed uint64, fileCount int, sessionCount int, opts ...meta.Option) *formalLockTestEnv {
	t.Helper()

	ctx := context.Background()
	pool := setupFormalPostgresPool(t)

	store := db.NewStore(pool)
	mgr := meta.New(store, opts...)

	diskName := fmt.Sprintf("formal_lock_disk_%d_%d", seed&0xffffffff, time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	model := newFormalLockModel(seed, disk.DiskID, disk.RootIno)
	rootInode, err := mgr.GetAttr(ctx, disk.RootIno)
	require.NoError(t, err)
	model.RegisterInode(rootInode, true)

	// 创建一个测试子目录用于校验对目录调用 GetLk/SetLk 返回 ErrIsDir
	subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("lock_test_dir"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	subDirAttr, err := mgr.GetAttr(ctx, subDir.Ino)
	require.NoError(t, err)
	model.RegisterInode(subDirAttr, true)

	// 注册多个客户端会话与候选 LockOwner
	baseSessionID := uint64(time.Now().UnixNano()&0x3fffffffffffffff) + (seed & 0xffff)
	for i := 0; i < sessionCount; i++ {
		sid := baseSessionID + uint64(i*100+1)
		exp, err := mgr.RenewSession(ctx, sid)
		require.NoError(t, err)
		model.RegisterSession(sid, exp)
	}
	model.OwnerList = []uint64{1001, 1002, 2001, 2002}

	// 创建常规文件并为每个会话分别打开 O_RDONLY、O_WRONLY、O_RDWR 句柄
	for fIdx := 0; fIdx < fileCount; fIdx++ {
		firstSid := model.SessionList[0]
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("lock_file_%d.dat", fIdx)),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &firstSid,
		})
		require.NoError(t, err)
		fileIno := createRes.Inode.Ino

		// 重新查询 GetAttr 以获取数据库持久化的精确时间戳快照
		freshInode, err := mgr.GetAttr(ctx, fileIno)
		require.NoError(t, err)
		model.RegisterInode(freshInode, false)
		model.RegisterHandle(createRes.Fh, fileIno, &firstSid, meta.O_RDWR)

		for sIdx, sid := range model.SessionList {
			sidCopy := sid
			openModes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
			for _, flag := range openModes {
				if sIdx == 0 && flag == meta.O_RDWR {
					// 第一个会话已在 Create 时持有 O_RDWR 句柄
					continue
				}
				openRes, err := mgr.Open(ctx, meta.OpenParams{
					Ino:       fileIno,
					Flags:     flag,
					SessionID: &sidCopy,
				})
				require.NoError(t, err)
				model.RegisterHandle(openRes.Fh, fileIno, &sidCopy, flag)
			}
		}
	}

	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		if len(model.SessionList) > 0 {
			sids := make([]int64, len(model.SessionList))
			for i, sid := range model.SessionList {
				sids[i] = int64(sid)
			}
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])", sids)
		}
	})

	return &formalLockTestEnv{
		Ctx:      ctx,
		Pool:     pool,
		Store:    store,
		Mgr:      mgr,
		Model:    model,
		TestName: t.Name(),
		Steps:    1,
	}
}

// findHandleForSessionAndMode 查找指定会话在目标文件上对应打开模式的活跃句柄。
func (m *formalLockModel) findHandleForSessionAndMode(ino, sessionID uint64, accMode uint32) (uint64, bool) {
	for _, fh := range m.HandleList {
		h := m.Handles[fh]
		if !h.Closed && h.Ino == ino && h.SessionID != nil && *h.SessionID == sessionID && (h.Flags&meta.O_ACCMODE) == accMode {
			return h.Fh, true
		}
	}
	return 0, false
}

// auditFormalLockInv1 扫描数据库底表 file_locks 与 inodes，强制审计 LOCK-INV-1 与时间戳不变量：
//  1. 同一 (ino, lk_flags = 0) 下不同 (session_id, lock_owner) 的相交活跃锁必同为 F_RDLCK；
//  2. 同一 (session_id, ino, lock_owner, lk_flags = 0) 下的活跃锁区间必互不相交（prev.end < next.start），
//     且不存在紧邻（prev.end < OffsetMax && prev.end + 1 == next.start）且同类型的未合并碎片区间；
//  3. 底表活跃锁集合（区间 [start_offset, end_offset]、lock_type、pid、session_id、lock_owner）与内存参考模型 formalLockModel 100% 一致；
//  4. 目标文件 Inode 的 atime、mtime、ctime 与初始快照严格一致（文件锁操作绝不修改 Inode 时间戳）。
func auditFormalLockInv1(t *testing.T, env *formalLockTestEnv) {
	t.Helper()

	ctx := env.Ctx
	m := env.Model

	type dbPosixLockRow struct {
		ID        int64
		SessionID uint64
		Ino       uint64
		Fh        uint64
		LockOwner uint64
		Start     uint64
		End       uint64
		Type      uint32
		PID       uint32
	}

	inoInt64s := make([]int64, len(m.FileInos))
	for i, ino := range m.FileInos {
		inoInt64s[i] = int64(ino)
	}

	// 1. 批量扫描 inodes 底表与 GetAttr 校验 Inode 时间戳不变量
	tsRows, err := env.Pool.Query(ctx, `
		SELECT ino, atime, mtime, ctime
		FROM inodes
		WHERE ino = ANY($1::bigint[])
	`, inoInt64s)
	if err != nil {
		t.Fatalf("SQL 扫描 inodes 时间戳失败: %v%s", err, env.FormatFailureReport())
	}
	for tsRows.Next() {
		var rowIno int64
		var atime, mtime, ctime time.Time
		if scanErr := tsRows.Scan(&rowIno, &atime, &mtime, &ctime); scanErr != nil {
			tsRows.Close()
			t.Fatalf("扫描 inodes 时间戳行失败: %v%s", scanErr, env.FormatFailureReport())
		}
		uino := uint64(rowIno)
		expectedInode := m.Inodes[uino]
		if !atime.Equal(expectedInode.Atime) || !mtime.Equal(expectedInode.Mtime) || !ctime.Equal(expectedInode.Ctime) {
			tsRows.Close()
			t.Fatalf("违反文件锁时间戳不变量 (Ino=%d): 期望 atime=%v mtime=%v ctime=%v, 实际 atime=%v mtime=%v ctime=%v%s",
				uino,
				expectedInode.Atime, expectedInode.Mtime, expectedInode.Ctime,
				atime, mtime, ctime,
				env.FormatFailureReport(),
			)
		}
	}
	tsRows.Close()
	if err := tsRows.Err(); err != nil {
		t.Fatalf("遍历 inodes 时间戳出错: %v%s", err, env.FormatFailureReport())
	}

	// 2. 按所属 root_ino 批量扫描 PostgreSQL 底表 file_locks 中全部活跃 POSIX 锁（lk_flags = 0）
	rows, err := env.Pool.Query(ctx, `
		SELECT l.id, l.session_id, l.ino, l.fh, l.lock_owner, l.start_offset, l.end_offset, l.lock_type, l.pid
		FROM file_locks l
		JOIN inodes i ON l.ino = i.ino
		JOIN client_sessions s ON l.session_id = s.session_id
		WHERE i.root_ino = $1
		  AND l.lk_flags = 0
		  AND s.expires_at > clock_timestamp()
		ORDER BY l.ino ASC, l.start_offset ASC, l.end_offset ASC, l.id ASC
	`, int64(m.RootIno))
	if err != nil {
		t.Fatalf("SQL 批量扫描 file_locks 失败: %v%s", err, env.FormatFailureReport())
	}

	locksByIno := make(map[uint64][]dbPosixLockRow, len(m.FileInos))
	for rows.Next() {
		var (
			id, sid, rowIno, fh, owner, startOff, endOff int64
			lockType                                     int16
			pid                                          int32
		)
		if scanErr := rows.Scan(&id, &sid, &rowIno, &fh, &owner, &startOff, &endOff, &lockType, &pid); scanErr != nil {
			rows.Close()
			t.Fatalf("扫描 file_locks 行失败: %v%s", scanErr, env.FormatFailureReport())
		}
		if startOff < 0 || endOff < startOff {
			rows.Close()
			t.Fatalf("违反 LOCK-INV-1: 底表存在非法字节区间 [%d, %d]%s", startOff, endOff, env.FormatFailureReport())
		}
		uino := uint64(rowIno)
		if m.Inodes[uino] == nil || m.Inodes[uino].IsDir {
			rows.Close()
			t.Fatalf("违反 LOCK-INV-1: 底表存在非受测文件节点的幽灵锁 (Ino=%d, root_ino=%d)%s",
				uino, m.RootIno, env.FormatFailureReport())
		}
		locksByIno[uino] = append(locksByIno[uino], dbPosixLockRow{
			ID:        id,
			SessionID: uint64(sid),
			Ino:       uino,
			Fh:        uint64(fh),
			LockOwner: uint64(owner),
			Start:     uint64(startOff),
			End:       uint64(endOff),
			Type:      uint32(lockType),
			PID:       uint32(pid),
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历 file_locks 行出错: %v%s", err, env.FormatFailureReport())
	}

	for _, ino := range m.FileInos {
		dbLocks := locksByIno[ino]

		// 3. 审计 LOCK-INV-1(a): 不同 (session_id, lock_owner) 的相交活跃锁必同为 F_RDLCK
		for i := 0; i < len(dbLocks); i++ {
			for j := i + 1; j < len(dbLocks); j++ {
				a, b := dbLocks[i], dbLocks[j]
				if a.SessionID == b.SessionID && a.LockOwner == b.LockOwner {
					continue
				}
				if lockOracleRangesOverlap(a.Start, a.End, b.Start, b.End) {
					if a.Type != meta.F_RDLCK || b.Type != meta.F_RDLCK {
						t.Fatalf("违反 LOCK-INV-1(跨拥有者互斥): Ino=%d 下 (sess=%d,owner=%d)[%d,%d](type=%d) 与 (sess=%d,owner=%d)[%d,%d](type=%d) 相交但非全读锁%s",
							ino, a.SessionID, a.LockOwner, a.Start, a.End, a.Type,
							b.SessionID, b.LockOwner, b.Start, b.End, b.Type,
							env.FormatFailureReport(),
						)
					}
				}
			}
		}

		// 4. 审计 LOCK-INV-1(b): 同一 (session_id, ino, lock_owner) 下的活跃锁必互不相交，且不存在紧邻同类型碎片
		byOwner := make(map[formalLockOwnerKey][]dbPosixLockRow)
		for _, l := range dbLocks {
			k := formalLockOwnerKey{SessionID: l.SessionID, ID: l.LockOwner}
			byOwner[k] = append(byOwner[k], l)
		}
		for k, list := range byOwner {
			sort.Slice(list, func(i, j int) bool {
				if list[i].Start != list[j].Start {
					return list[i].Start < list[j].Start
				}
				return list[i].End < list[j].End
			})
			for i := 1; i < len(list); i++ {
				prev := list[i-1]
				next := list[i]
				if prev.End >= next.Start {
					t.Fatalf("违反 LOCK-INV-1(同拥有者重叠): Ino=%d (sess=%d,owner=%d) 存在相交锁 [%d,%d] 与 [%d,%d]%s",
						ino, k.SessionID, k.ID, prev.Start, prev.End, next.Start, next.End,
						env.FormatFailureReport(),
					)
				}
				if prev.End < meta.OffsetMax && prev.End+1 == next.Start && prev.Type == next.Type {
					t.Fatalf("违反 LOCK-INV-1(同拥有者紧邻同类型未合并碎片): Ino=%d (sess=%d,owner=%d) 存在紧邻同类型锁 [%d,%d](type=%d) 与 [%d,%d](type=%d)%s",
						ino, k.SessionID, k.ID, prev.Start, prev.End, prev.Type, next.Start, next.End, next.Type,
						env.FormatFailureReport(),
					)
				}
			}
		}

		// 5. 审计 LOCK-INV-1(c): 底表活跃锁集合与内存参考模型 formalLockModel 完全一致
		expectedLocks := m.activePosixLocksByIno(ino)
		if len(dbLocks) != len(expectedLocks) {
			t.Fatalf("违反 LOCK-INV-1(模型与底表锁数量不一致): Ino=%d 模型期望 %d 条锁, 底表实际 %d 条锁\n模型=%+v\n底表=%+v%s",
				ino, len(expectedLocks), len(dbLocks), expectedLocks, dbLocks, env.FormatFailureReport())
		}
		for i := range expectedLocks {
			exp := expectedLocks[i]
			act := dbLocks[i]
			if exp.SessionID != act.SessionID ||
				exp.LockOwner != act.LockOwner ||
				exp.Start != act.Start ||
				exp.End != act.End ||
				exp.Type != act.Type ||
				exp.PID != act.PID {
				t.Fatalf("违反 LOCK-INV-1(模型与底表第 %d 条锁不一致): Ino=%d\n模型期望: sess=%d owner=%d [%d,%d] type=%d pid=%d\n底表实际: sess=%d owner=%d [%d,%d] type=%d pid=%d%s",
					i, ino,
					exp.SessionID, exp.LockOwner, exp.Start, exp.End, exp.Type, exp.PID,
					act.SessionID, act.LockOwner, act.Start, act.End, act.Type, act.PID,
					env.FormatFailureReport(),
				)
			}
		}
	}
}

// verifyFormalLockGetLkProbes 在每步状态变更后执行 GetLk 只读查账探针：
// 覆盖随机区间探测、零点区间 [0,0] 探测、自锁忽略、多冲突锁确定性排序以及 F_UNLCK 非法入参边界。
func verifyFormalLockGetLkProbes(t *testing.T, env *formalLockTestEnv, rng *rand.Rand) {
	t.Helper()

	ctx := env.Ctx
	m := env.Model

	ino := m.FileInos[rng.IntN(len(m.FileInos))]
	sid := m.SessionList[rng.IntN(len(m.SessionList))]
	owner := m.OwnerList[rng.IntN(len(m.OwnerList))]
	// GetLk 不限制句柄读写模式，随机使用 O_RDONLY / O_WRONLY / O_RDWR 句柄验证
	modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
	fh, ok := m.findHandleForSessionAndMode(ino, sid, modes[rng.IntN(len(modes))])
	require.True(t, ok)

	var start, end uint64
	if rng.IntN(4) == 0 {
		// 显式高频覆盖 [0, 0] 探针（无冲突时返回 Start=0, End=0, PID=0, Type=F_UNLCK）
		start, end = 0, 0
	} else {
		a := uint64(rng.IntN(120))
		b := uint64(rng.IntN(120))
		if a > b {
			a, b = b, a
		}
		if rng.IntN(8) == 0 {
			b = meta.OffsetMax
		}
		start, end = a, b
	}

	probeType := meta.F_WRLCK
	if rng.IntN(2) == 0 {
		probeType = meta.F_RDLCK
	}

	params := meta.GetLkParams{
		Ino:       ino,
		Fh:        fh,
		LockOwner: owner,
		Lk: meta.FileLock{
			Start: start,
			End:   end,
			Type:  probeType,
			PID:   9000,
		},
		LkFlags:   0,
		SessionID: sid,
	}

	expLk, expErr := m.ApplyGetLk(params)
	actLk, actErr := env.Mgr.GetLk(ctx, params)
	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("GetLk 探针错误码不一致 (Ino=%d, sess=%d, owner=%d, [%d,%d], type=%d): 模型期望 err=%v, 实际 err=%v%s",
			ino, sid, owner, start, end, probeType, expErr, actErr, env.FormatFailureReport())
	}
	if expErr == nil {
		if *expLk != *actLk {
			t.Fatalf("GetLk 探针返回值不一致 (Ino=%d, sess=%d, owner=%d, [%d,%d], type=%d): 模型期望 %+v, 实际 %+v%s",
				ino, sid, owner, start, end, probeType, expLk, actLk, env.FormatFailureReport())
		}
	}

	// 探针 2: 若当前文件存在活跃锁，挑选第一条活跃锁的持有者探测自身锁区间，验证自锁被忽略或仅返回其他持有者的冲突锁
	active := m.activePosixLocksByIno(ino)
	if len(active) > 0 {
		first := active[0]
		selfFh, ok := m.findHandleForSessionAndMode(ino, first.SessionID, meta.O_RDONLY)
		if ok {
			selfProbe := meta.GetLkParams{
				Ino:       ino,
				Fh:        selfFh,
				LockOwner: first.LockOwner,
				Lk: meta.FileLock{
					Start: first.Start,
					End:   first.End,
					Type:  meta.F_WRLCK,
					PID:   7777,
				},
				LkFlags:   0,
				SessionID: first.SessionID,
			}
			expSelfLk, expSelfErr := m.ApplyGetLk(selfProbe)
			actSelfLk, actSelfErr := env.Mgr.GetLk(ctx, selfProbe)
			require.NoError(t, expSelfErr)
			require.NoError(t, actSelfErr)
			if *expSelfLk != *actSelfLk {
				t.Fatalf("GetLk 自锁忽略探针不一致: 模型期望 %+v, 实际 %+v%s", expSelfLk, actSelfLk, env.FormatFailureReport())
			}
		}
	}
}

func formatLockTypeCN(t uint32) string {
	switch t {
	case meta.F_RDLCK:
		return "F_RDLCK(读锁)"
	case meta.F_WRLCK:
		return "F_WRLCK(写锁)"
	case meta.F_UNLCK:
		return "F_UNLCK(解锁)"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// execAndVerifySetLk 同时在内存参考模型与真实 meta.Manager 上执行 SetLk 并核对返回错误码。
func execAndVerifySetLk(t *testing.T, env *formalLockTestEnv, params meta.SetLkParams, desc string) {
	t.Helper()

	m := env.Model
	expErr := m.ApplySetLk(params)
	actErr := env.Mgr.SetLk(env.Ctx, params)

	m.RecordStepf("%s | SetLk(ino=%d, fh=%d, sess=%d, owner=%d, flags=%d, type=%s, range=[%d,%d], pid=%d) -> 期望=%v, 实际=%v",
		desc, params.Ino, params.Fh, params.SessionID, params.LockOwner, params.LkFlags,
		formatLockTypeCN(params.Lk.Type), params.Lk.Start, params.Lk.End, params.Lk.PID, expErr, actErr)

	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("SetLk 对拍失败 (%s): 模型期望 err=%v, 实际 err=%v%s",
			desc, expErr, actErr, env.FormatFailureReport())
	}
}

// execAndVerifyGetLk 同时在内存参考模型与真实 meta.Manager 上执行 GetLk 并核对返回结果与错误码。
func execAndVerifyGetLk(t *testing.T, env *formalLockTestEnv, params meta.GetLkParams, desc string) *meta.FileLock {
	t.Helper()

	m := env.Model
	expLk, expErr := m.ApplyGetLk(params)
	actLk, actErr := env.Mgr.GetLk(env.Ctx, params)

	m.RecordStepf("%s | GetLk(ino=%d, fh=%d, sess=%d, owner=%d, flags=%d, type=%s, range=[%d,%d]) -> 期望=(%v, err=%v), 实际=(%v, err=%v)",
		desc, params.Ino, params.Fh, params.SessionID, params.LockOwner, params.LkFlags,
		formatLockTypeCN(params.Lk.Type), params.Lk.Start, params.Lk.End, expLk, expErr, actLk, actErr)

	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("GetLk 错误码对拍失败 (%s): 模型期望 err=%v, 实际 err=%v%s",
			desc, expErr, actErr, env.FormatFailureReport())
	}
	if expErr == nil && *expLk != *actLk {
		t.Fatalf("GetLk 返回锁对拍失败 (%s): 模型期望 %+v, 实际 %+v%s",
			desc, expLk, actLk, env.FormatFailureReport())
	}
	return actLk
}

// generateRandomPosixRange 生成高碰撞密度的字节区间（倾向于在 [0, 80] 密集网格及 OffsetMax 边界内产生相交、包含与紧邻关系）。
func generateRandomPosixRange(rng *rand.Rand) (uint64, uint64) {
	anchors := []uint64{0, 1, 9, 10, 11, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 60, 70, 80, 100}
	if rng.IntN(10) < 7 {
		a := anchors[rng.IntN(len(anchors))]
		b := anchors[rng.IntN(len(anchors))]
		if a > b {
			a, b = b, a
		}
		if rng.IntN(10) == 0 {
			b = meta.OffsetMax
		}
		return a, b
	}
	a := uint64(rng.IntN(100))
	b := uint64(rng.IntN(100))
	if a > b {
		a, b = b, a
	}
	if rng.IntN(12) == 0 {
		b = meta.OffsetMax
	}
	return a, b
}

// stepFormalLockPosixAction 基于随机数生成器生成一步 POSIX SetLk 动作（涵盖正常加解锁、裂解合并、跨拥有者冲突以及非法参数/无效句柄边界）。
func stepFormalLockPosixAction(t *testing.T, env *formalLockTestEnv, rng *rand.Rand, stepIdx int) {
	t.Helper()

	m := env.Model
	ino := m.FileInos[rng.IntN(len(m.FileInos))]
	sid := m.SessionList[rng.IntN(len(m.SessionList))]
	owner := m.OwnerList[rng.IntN(len(m.OwnerList))]
	pid := uint32(100 + stepIdx)

	roll := rng.IntN(100)
	switch {
	case roll < 75:
		// 75% 概率：使用匹配的读写句柄执行 POSIX F_RDLCK / F_WRLCK / F_UNLCK（高频触发共享、EAGAIN 冲突、裂解、覆盖、紧邻合并）
		start, end := generateRandomPosixRange(rng)
		var lkType uint32
		typeRoll := rng.IntN(10)
		switch {
		case typeRoll < 4:
			lkType = meta.F_RDLCK
		case typeRoll < 8:
			lkType = meta.F_WRLCK
		default:
			lkType = meta.F_UNLCK
		}

		// 选择权限匹配的句柄（或随机选取 O_RDWR）
		accMode := uint32(meta.O_RDWR)
		if lkType == meta.F_RDLCK && rng.IntN(2) == 0 {
			accMode = meta.O_RDONLY
		} else if lkType == meta.F_WRLCK && rng.IntN(2) == 0 {
			accMode = meta.O_WRONLY
		} else if lkType == meta.F_UNLCK {
			modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
			accMode = modes[rng.IntN(len(modes))]
		}
		fh, ok := m.findHandleForSessionAndMode(ino, sid, accMode)
		require.True(t, ok)

		execAndVerifySetLk(t, env, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: owner,
			Lk: meta.FileLock{
				Start: start,
				End:   end,
				Type:  lkType,
				PID:   pid,
			},
			LkFlags:   0,
			SessionID: sid,
		}, "随机POSIX区间加解锁")

	case roll < 85:
		// 10% 概率：句柄读写模式位不匹配或跨会话/无效句柄 -> 期望返回 ErrBadFileHandle (EBADF)
		start, end := generateRandomPosixRange(rng)
		subCase := rng.IntN(4)
		switch subCase {
		case 0:
			// O_RDONLY 申请 F_WRLCK -> ErrBadFileHandle
			fh, _ := m.findHandleForSessionAndMode(ino, sid, meta.O_RDONLY)
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: start, End: end, Type: meta.F_WRLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "只读句柄申请写锁(EBADF)")
		case 1:
			// O_WRONLY 申请 F_RDLCK -> ErrBadFileHandle
			fh, _ := m.findHandleForSessionAndMode(ino, sid, meta.O_WRONLY)
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: start, End: end, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "只写句柄申请读锁(EBADF)")
		case 2:
			// 跨会话句柄盗用 -> ErrBadFileHandle
			otherSid := m.SessionList[(rng.IntN(len(m.SessionList)-1)+1)%len(m.SessionList)]
			if otherSid == sid && len(m.SessionList) > 1 {
				otherSid = m.SessionList[0]
				if otherSid == sid {
					otherSid = m.SessionList[1]
				}
			}
			fh, _ := m.findHandleForSessionAndMode(ino, otherSid, meta.O_RDWR)
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: start, End: end, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "跨会话盗用句柄(EBADF)")
		default:
			// 不存在的句柄 Fh -> ErrBadFileHandle
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        99999999,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: start, End: end, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "不存在句柄(EBADF)")
		}

	case roll < 95:
		// 10% 概率：非法参数边界 -> 期望返回 ErrInvalidArgument (EINVAL)
		fh, _ := m.findHandleForSessionAndMode(ino, sid, meta.O_RDWR)
		baseParams := meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: owner,
			Lk:        meta.FileLock{Start: 10, End: 20, Type: meta.F_RDLCK, PID: pid},
			LkFlags:   0,
			SessionID: sid,
		}
		switch rng.IntN(6) {
		case 0:
			// start < 0 (uint64 表示为 > OffsetMax)
			baseParams.Lk.Start = meta.OffsetMax + 1
			baseParams.Lk.End = meta.OffsetMax + 10
			execAndVerifySetLk(t, env, baseParams, "非法区间(start>OffsetMax/EINVAL)")
		case 1:
			// end < start
			baseParams.Lk.Start = 50
			baseParams.Lk.End = 20
			execAndVerifySetLk(t, env, baseParams, "非法区间(end<start/EINVAL)")
		case 2:
			// 非法 typ
			baseParams.Lk.Type = 99
			execAndVerifySetLk(t, env, baseParams, "非法锁类型(typ=99/EINVAL)")
		case 3:
			// 非法 lk_flags
			baseParams.LkFlags = 2
			execAndVerifySetLk(t, env, baseParams, "非法标志位(lk_flags=2/EINVAL)")
		case 4:
			// 空 session_id (0)
			baseParams.SessionID = 0
			execAndVerifySetLk(t, env, baseParams, "空会话ID(session_id=0/EINVAL)")
		default:
			// GetLk 传入 F_UNLCK -> ErrInvalidArgument
			execAndVerifyGetLk(t, env, meta.GetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: 10, Type: meta.F_UNLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "GetLk传入F_UNLCK(EINVAL)")
		}

	default:
		// 5% 概率：对目录或不存在的 Inode 调用 SetLk -> ErrIsDir / ErrNotFound
		fh, _ := m.findHandleForSessionAndMode(ino, sid, meta.O_RDWR)
		if rng.IntN(2) == 0 {
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       m.DirIno,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "目录节点加锁(EISDIR)")
		} else {
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       888888888,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   0,
				SessionID: sid,
			}, "不存在节点加锁(ENOENT)")
		}
	}
}

// TestFormalLock_T11_DeterministicPosixScenarios 验证 T11 所有核心边界场景的确定性黄金路径对拍与 LOCK-INV-1 审计。
func TestFormalLock_T11_DeterministicPosixScenarios(t *testing.T) {
	env := setupFormalLockEnv(t, 0x20261024, 1, 3)
	m := env.Model

	ino := m.FileInos[0]
	s1, s2, s3 := m.SessionList[0], m.SessionList[1], m.SessionList[2]
	ownerA, ownerB := uint64(1001), uint64(2002)

	fh1RO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDONLY)
	fh1WO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_WRONLY)
	fh1RW, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDWR)
	fh2RW, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDWR)
	fh3RW, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDWR)

	// 1. 非法入参边界校验 (EINVAL) 与句柄模式校验 (EBADF)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: 0,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "空session_id返回EINVAL")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: meta.OffsetMax + 1, End: meta.OffsetMax + 5, Type: meta.F_RDLCK, PID: 10},
	}, "start<0(超OffsetMax)返回EINVAL")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 30, End: 20, Type: meta.F_RDLCK, PID: 10},
	}, "end<start返回EINVAL")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: 99, PID: 10},
	}, "非法typ返回EINVAL")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 7,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "非法lk_flags返回EINVAL")

	execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_UNLCK, PID: 10},
	}, "GetLk传入F_UNLCK返回EINVAL")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_WRLCK, PID: 10},
	}, "O_RDONLY句柄申请F_WRLCK返回EBADF")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "O_WRONLY句柄申请F_RDLCK返回EBADF")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 10, Type: meta.F_RDLCK, PID: 10},
	}, "跨会话使用fh2RW返回EBADF")

	// 2. 无锁区间幂等解锁 + GetLk 无冲突返回 F_UNLCK (Start=0, End=0, PID=0)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 11},
	}, "无锁区间F_UNLCK幂等成功")

	lkRes := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 0, End: 0, Type: meta.F_WRLCK, PID: 99},
	}, "无冲突探测[0,0]返回F_UNLCK+Start=0,End=0,PID=0")
	require.Equal(t, meta.FileLock{Start: 0, End: 0, Type: meta.F_UNLCK, PID: 0}, *lkRes)

	// 3. 同一 (session_id, ino, lock_owner) 下的中心裂解（保留被截断原记录的 pid）与紧邻同类型合并（更新 pid）
	// 3.1 申请 [10, 50] F_WRLCK (pid=101)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 10, End: 50, Type: meta.F_WRLCK, PID: 101},
	}, "申请[10,50]写锁(pid=101)")
	auditFormalLockInv1(t, env)

	// 3.2 在中间 [20, 30] 降级为 F_RDLCK (pid=202) -> 裂解为 [10,19](W,pid=101), [20,30](R,pid=202), [31,50](W,pid=101)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_RDLCK, PID: 202},
	}, "中间[20,30]降级为读锁(pid=202)触发左右裂解并保留原pid=101")
	auditFormalLockInv1(t, env)

	// 3.3 会话 2 在 [20, 30] 申请共享读锁 F_RDLCK (pid=301) -> 成功共享；但在 [15, 25] 申请读锁或 [20, 30] 申请写锁 -> 返回 ErrAgain 且原锁完整保留
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_RDLCK, PID: 301},
	}, "会话2在[20,30]共享F_RDLCK成功")
	auditFormalLockInv1(t, env)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2,
		Lk: meta.FileLock{Start: 15, End: 25, Type: meta.F_RDLCK, PID: 302},
	}, "会话2在[15,25]与会话1写锁[10,19]冲突返回EAGAIN")
	auditFormalLockInv1(t, env)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerB, SessionID: s3,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_WRLCK, PID: 401},
	}, "会话3在[20,30]与共享读锁冲突返回EAGAIN")
	auditFormalLockInv1(t, env)

	// 3.4 会话 3 调用 GetLk(F_WRLCK, [0, 100]) -> 应按 start_offset ASC, end_offset ASC, id ASC 返回首个冲突锁 [10, 19] (pid=101)
	firstConflict := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerB, SessionID: s3,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 402},
	}, "会话3探测[0,100]返回首个冲突锁[10,19]")
	require.Equal(t, meta.FileLock{Start: 10, End: 19, Type: meta.F_WRLCK, PID: 101}, *firstConflict)

	// 3.5 会话 2 释放 [20, 30] 读锁后，会话 1 在 [20, 30] 重新加 F_WRLCK (pid=505) -> 与左右紧邻 [10,19] 和 [31,50] 合并为 [10, 50] 且 pid 更新为 505
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_UNLCK, PID: 303},
	}, "会话2解锁[20,30]")

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 20, End: 30, Type: meta.F_WRLCK, PID: 505},
	}, "会话1在[20,30]加写锁(pid=505)与左右紧邻[10,19]、[31,50]合并为[10,50](pid=505)")
	auditFormalLockInv1(t, env)

	activeAfterCoalesce := m.activePosixLocksByIno(ino)
	require.Len(t, activeAfterCoalesce, 1)
	require.Equal(t, uint64(10), activeAfterCoalesce[0].Start)
	require.Equal(t, uint64(50), activeAfterCoalesce[0].End)
	require.Equal(t, uint32(505), activeAfterCoalesce[0].PID)

	// 3.6 验证 OffsetMax 边界加锁与中间挖空解锁
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 51, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 606},
	}, "会话1在[51,OffsetMax]紧邻加写锁(pid=606)合并为[10,OffsetMax]")
	auditFormalLockInv1(t, env)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1,
		Lk: meta.FileLock{Start: 100, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 707},
	}, "会话1解锁右边界[100,OffsetMax]保留[10,99](pid=606)")
	auditFormalLockInv1(t, env)
}

// TestFormalLock_T11_PosixRange 运行基于 PCG 64 位随机种子的 POSIX 字节区间锁状态机对账测试。
// 每一步随机生成 SetLk 动作并与内存模型 formalLockModel 对拍，随后执行 GetLk 只读探针与底表 LOCK-INV-1 全量 SQL 审计。
func TestFormalLock_T11_PosixRange(t *testing.T) {
	const testName = "TestFormalLock_T11_PosixRange"
	cfg := loadFormalLockConfig(t)
	t.Logf("启动 T11 POSIX 字节区间锁形式化状态机验证: baseSeed=0x%016x, runs=%d, steps=%d", cfg.BaseSeed, cfg.Runs, cfg.Steps)

	for runIdx := 0; runIdx < cfg.Runs; runIdx++ {
		runSeed := cfg.BaseSeed + uint64(runIdx)
		t.Run(fmt.Sprintf("Run_%d_Seed_0x%x", runIdx, runSeed), func(t *testing.T) {
			env := setupFormalLockEnv(t, runSeed, 2, 3)
			env.TestName = testName
			env.Steps = cfg.Steps
			rng := rand.New(rand.NewPCG(runSeed, runSeed^0x9e3779b97f4a7c15))

			for step := 1; step <= cfg.Steps; step++ {
				stepFormalLockPosixAction(t, env, rng, step)
				verifyFormalLockGetLkProbes(t, env, rng)
				auditFormalLockInv1(t, env)
			}

			// 每轮结束时额外通过公开接口 GetAttr 校验所有受测文件时间戳与初始快照严格一致
			for _, ino := range env.Model.FileInos {
				attrRes, err := env.Mgr.GetAttr(env.Ctx, ino)
				require.NoError(t, err)
				expInode := env.Model.Inodes[ino]
				require.True(t, attrRes.Attr.Atime.Equal(expInode.Atime), "GetAttr atime 不得被文件锁操作修改")
				require.True(t, attrRes.Attr.Mtime.Equal(expInode.Mtime), "GetAttr mtime 不得被文件锁操作修改")
				require.True(t, attrRes.Attr.Ctime.Equal(expInode.Ctime), "GetAttr ctime 不得被文件锁操作修改")
			}
		})
	}
}
