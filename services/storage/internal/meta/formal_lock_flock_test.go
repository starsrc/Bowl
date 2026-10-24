package meta_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func formatOptUint64(v *uint64) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *v)
}

// execAndVerifyFlush 同时在内存参考模型与真实 meta.Manager 上执行 Flush 并核对返回错误码。
func execAndVerifyFlush(t *testing.T, env *formalLockTestEnv, params meta.FlushParams, desc string, testName string, steps int) {
	t.Helper()

	m := env.Model
	expErr := m.ApplyFlush(params)
	actErr := env.Mgr.Flush(env.Ctx, params)

	m.RecordStepf("%s | Flush(ino=%d, fh=%d, sess=%s, owner=%s) -> 期望=%v, 实际=%v",
		desc, params.Ino, params.Fh, formatOptUint64(params.SessionID), formatOptUint64(params.LockOwner), expErr, actErr)

	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("Flush 对拍失败 (%s): 模型期望 err=%v, 实际 err=%v%s",
			desc, expErr, actErr, m.FormatTrace(testName, steps))
	}
}

// execAndVerifyRelease 同时在内存参考模型与真实 meta.Manager 上执行 Release 并核对返回错误码。
func execAndVerifyRelease(t *testing.T, env *formalLockTestEnv, params meta.ReleaseParams, desc string, testName string, steps int) error {
	t.Helper()

	m := env.Model
	expErr := m.ApplyRelease(params)
	actErr := env.Mgr.Release(env.Ctx, params)

	m.RecordStepf("%s | Release(ino=%d, fh=%d, sess=%s, owner=%s, release_flags=0x%x) -> 期望=%v, 实际=%v",
		desc, params.Ino, params.Fh, formatOptUint64(params.SessionID), formatOptUint64(params.LockOwner), params.ReleaseFlags, expErr, actErr)

	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("Release 对拍失败 (%s): 模型期望 err=%v, 实际 err=%v%s",
			desc, expErr, actErr, m.FormatTrace(testName, steps))
	}
	return actErr
}

// replenishHandleAfterRelease 在 Release 销毁句柄后动态调用 Open 补充同会话同打开模式的新句柄，以支撑长序列压测。
func replenishHandleAfterRelease(t *testing.T, env *formalLockTestEnv, ino uint64, sessionID uint64, flags uint32, testName string, steps int) uint64 {
	t.Helper()

	m := env.Model
	sidCopy := sessionID
	openRes, err := env.Mgr.Open(env.Ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     flags,
		SessionID: &sidCopy,
	})
	if err != nil {
		t.Fatalf("Release 后动态 Open(ino=%d, sess=%d, flags=0x%x) 补充句柄失败: %v%s",
			ino, sessionID, flags, err, m.FormatTrace(testName, steps))
	}
	m.purgeExpiredSessionsAndUpsert(sessionID)
	m.RegisterHandle(openRes.Fh, ino, &sidCopy, flags)
	m.RecordStepf("动态补充句柄 | Open(ino=%d, sess=%d, flags=0x%x) -> 新fh=%d",
		ino, sessionID, flags, openRes.Fh)
	return openRes.Fh
}

// hasActiveFlockOnHandle 检查指定 (ino, sessionID, fh) 当前是否在内存参考模型中持有活跃 BSD flock。
func (m *formalLockModel) hasActiveFlockOnHandle(ino, sessionID, fh uint64) bool {
	for _, l := range m.FlockLocks {
		if l.Ino == ino && l.SessionID == sessionID && l.Fh == fh && m.isSessionActive(l.SessionID) {
			return true
		}
	}
	return false
}

// findClosedHandle 查找一个已关闭的句柄（若存在），用于验证对已销毁句柄调用操作返回 ErrBadFileHandle。
func (m *formalLockModel) findClosedHandle(ino uint64) (*formalLockHandleState, bool) {
	for _, fh := range m.HandleList {
		h := m.Handles[fh]
		if h.Closed && h.Ino == ino {
			return h, true
		}
	}
	return nil, false
}

// auditFormalLockInv2 扫描数据库底表 file_locks、file_handles 与 inodes，强制审计 LOCK-INV-1 与 LOCK-INV-2：
// 1. 先执行 auditFormalLockInv1 审计 Inode 时间戳不变量与 lk_flags = 0 (POSIX) 的 LOCK-INV-1 及模型一致性；
// 2. 扫描 file_locks 校验不存在 lk_flags ∉ {0, FuseLkFlock} 的非法命名空间记录；
// 3. 校验每条 lk_flags = FuseLkFlock 记录必满足 start_offset == 0 && end_offset == OffsetMax；
// 4. 校验同一 (ino, lk_flags = FuseLkFlock) 下不同 (session_id, fh) 的活跃锁若共存，必全部为 F_RDLCK，且同一 (session_id, ino, fh) 至多 1 条记录；
// 5. 校验底表中的 lk_flags = FuseLkFlock 集合与内存参考模型 FlockLocks 100% 一致；
// 6. 校验底表中的活跃 file_handles 集合与内存参考模型 Handles 中未关闭的句柄 100% 一致（验证 Flush 不删句柄、Release 精确删句柄）。
func auditFormalLockInv2(t *testing.T, env *formalLockTestEnv, testName string, steps int) {
	t.Helper()

	// 1. 审计 Inode 时间戳与 lk_flags = 0 的 LOCK-INV-1 及 PosixLocks 100% 一致
	auditFormalLockInv1(t, env, testName, steps)

	ctx := env.Ctx
	m := env.Model

	inoInt64s := make([]int64, len(m.FileInos))
	for i, ino := range m.FileInos {
		inoInt64s[i] = int64(ino)
	}

	// 2. 检查是否存在非法 lk_flags 记录
	var invalidFlagCount int
	err := env.Pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM file_locks
		WHERE ino = ANY($1::bigint[])
		  AND lk_flags NOT IN (0, 1)
	`, inoInt64s).Scan(&invalidFlagCount)
	if err != nil {
		t.Fatalf("SQL 检查非法 lk_flags 失败: %v%s", err, m.FormatTrace(testName, steps))
	}
	if invalidFlagCount != 0 {
		t.Fatalf("违反 LOCK-INV-2: 底表存在 %d 条非法 lk_flags 记录%s", invalidFlagCount, m.FormatTrace(testName, steps))
	}

	// 3. 批量扫描底表 file_locks 中所有受测 Ino 的活跃 BSD flock 锁（lk_flags = FuseLkFlock）
	type dbFlockRow struct {
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

	rows, err := env.Pool.Query(ctx, `
		SELECT l.id, l.session_id, l.ino, l.fh, l.lock_owner, l.start_offset, l.end_offset, l.lock_type, l.pid
		FROM file_locks l
		JOIN client_sessions s ON l.session_id = s.session_id
		WHERE l.ino = ANY($1::bigint[])
		  AND l.lk_flags = 1
		  AND s.expires_at > clock_timestamp()
		ORDER BY l.ino ASC, l.id ASC
	`, inoInt64s)
	if err != nil {
		t.Fatalf("SQL 批量扫描 BSD flock file_locks 失败: %v%s", err, m.FormatTrace(testName, steps))
	}

	flocksByIno := make(map[uint64][]dbFlockRow, len(m.FileInos))
	for rows.Next() {
		var (
			id, sid, rowIno, fh, owner, startOff, endOff int64
			lockType                                     int16
			pid                                          int32
		)
		if scanErr := rows.Scan(&id, &sid, &rowIno, &fh, &owner, &startOff, &endOff, &lockType, &pid); scanErr != nil {
			rows.Close()
			t.Fatalf("扫描 BSD flock 行失败: %v%s", scanErr, m.FormatTrace(testName, steps))
		}
		if startOff != 0 || uint64(endOff) != meta.OffsetMax {
			rows.Close()
			t.Fatalf("违反 LOCK-INV-2(BSD flock 整文件区间约束): Ino=%d (sess=%d,fh=%d) 实际区间=[%d, %d], 期望=[0, %d]%s",
				rowIno, sid, fh, startOff, endOff, meta.OffsetMax, m.FormatTrace(testName, steps))
		}
		uino := uint64(rowIno)
		flocksByIno[uino] = append(flocksByIno[uino], dbFlockRow{
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
		t.Fatalf("遍历 BSD flock 行出错: %v%s", err, m.FormatTrace(testName, steps))
	}

	for _, ino := range m.FileInos {
		dbFlocks := flocksByIno[ino]

		// 4. 审计 LOCK-INV-2(唯一性与互斥性):
		// - 同一 (session_id, ino, fh) 至多 1 条记录；
		// - 同一 (ino, lk_flags = FuseLkFlock) 下不同 (session_id, fh) 若共存，必全部为 F_RDLCK。
		seenHolder := make(map[formalLockHolderID]int, len(dbFlocks))
		for _, l := range dbFlocks {
			h := formalLockHolderID{SessionID: l.SessionID, ID: l.Fh}
			seenHolder[h]++
			if seenHolder[h] > 1 {
				t.Fatalf("违反 LOCK-INV-2(同句柄多条flock记录): Ino=%d (sess=%d,fh=%d) 存在 %d 条活跃 flock 记录%s",
					ino, l.SessionID, l.Fh, seenHolder[h], m.FormatTrace(testName, steps))
			}
			if len(dbFlocks) > 1 && l.Type != meta.F_RDLCK {
				t.Fatalf("违反 LOCK-INV-2(跨句柄flock互斥): Ino=%d 存在 %d 条共存活跃 flock 记录，但 (sess=%d,fh=%d) 持有非读锁 type=%d%s",
					ino, len(dbFlocks), l.SessionID, l.Fh, l.Type, m.FormatTrace(testName, steps))
			}
		}

		// 5. 审计 LOCK-INV-2(模型与底表 FlockLocks 100% 一致)
		expectedFlocks := m.activeFlockLocksByIno(ino)
		if len(dbFlocks) != len(expectedFlocks) {
			t.Fatalf("违反 LOCK-INV-2(模型与底表 flock 数量不一致): Ino=%d 模型期望 %d 条 flock, 底表实际 %d 条 flock\n模型=%+v\n底表=%+v%s",
				ino, len(expectedFlocks), len(dbFlocks), expectedFlocks, dbFlocks, m.FormatTrace(testName, steps))
		}
		for i := range expectedFlocks {
			exp := expectedFlocks[i]
			act := dbFlocks[i]
			if exp.SessionID != act.SessionID ||
				exp.Ino != act.Ino ||
				exp.Fh != act.Fh ||
				exp.LockOwner != act.LockOwner ||
				exp.Type != act.Type ||
				exp.PID != act.PID {
				t.Fatalf("违反 LOCK-INV-2(模型与底表第 %d 条 flock 不一致): Ino=%d\n模型期望: sess=%d fh=%d owner=%d type=%d pid=%d\n底表实际: sess=%d fh=%d owner=%d type=%d pid=%d%s",
					i, ino,
					exp.SessionID, exp.Fh, exp.LockOwner, exp.Type, exp.PID,
					act.SessionID, act.Fh, act.LockOwner, act.Type, act.PID,
					m.FormatTrace(testName, steps),
				)
			}
		}
	}

	// 6. 审计底表 file_handles 集合与内存模型未关闭句柄 100% 一致
	fhRows, err := env.Pool.Query(ctx, `
		SELECT fh.fh, fh.ino, fh.flags, fh.session_id
		FROM file_handles fh
		LEFT JOIN client_sessions s ON fh.session_id = s.session_id
		WHERE fh.ino = ANY($1::bigint[])
		  AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
		ORDER BY fh.fh ASC
	`, inoInt64s)
	if err != nil {
		t.Fatalf("SQL 扫描 file_handles 失败: %v%s", err, m.FormatTrace(testName, steps))
	}

	type dbHandleRow struct {
		Fh        uint64
		Ino       uint64
		AccMode   uint32
		SessionID uint64
	}
	var actualHandles []dbHandleRow
	for fhRows.Next() {
		var fh, rowIno int64
		var flags int32
		var sid *int64
		if scanErr := fhRows.Scan(&fh, &rowIno, &flags, &sid); scanErr != nil {
			fhRows.Close()
			t.Fatalf("扫描 file_handles 行失败: %v%s", scanErr, m.FormatTrace(testName, steps))
		}
		var usid uint64
		if sid != nil {
			usid = uint64(*sid)
		}
		actualHandles = append(actualHandles, dbHandleRow{
			Fh:        uint64(fh),
			Ino:       uint64(rowIno),
			AccMode:   uint32(flags) & meta.O_ACCMODE,
			SessionID: usid,
		})
	}
	fhRows.Close()
	if err := fhRows.Err(); err != nil {
		t.Fatalf("遍历 file_handles 行出错: %v%s", err, m.FormatTrace(testName, steps))
	}

	var expectedHandles []dbHandleRow
	for _, fh := range m.HandleList {
		h := m.Handles[fh]
		if h.Closed {
			continue
		}
		var usid uint64
		if h.SessionID != nil {
			if !m.isSessionActive(*h.SessionID) {
				continue
			}
			usid = *h.SessionID
		}
		expectedHandles = append(expectedHandles, dbHandleRow{
			Fh:        h.Fh,
			Ino:       h.Ino,
			AccMode:   h.Flags & meta.O_ACCMODE,
			SessionID: usid,
		})
	}
	sort.Slice(expectedHandles, func(i, j int) bool {
		return expectedHandles[i].Fh < expectedHandles[j].Fh
	})

	if len(actualHandles) != len(expectedHandles) {
		t.Fatalf("违反句柄状态一致性: 模型期望 %d 个活跃句柄, 底表实际 %d 个活跃句柄\n模型=%+v\n底表=%+v%s",
			len(expectedHandles), len(actualHandles), expectedHandles, actualHandles, m.FormatTrace(testName, steps))
	}
	for i := range expectedHandles {
		if expectedHandles[i] != actualHandles[i] {
			t.Fatalf("违反句柄状态一致性(第 %d 个句柄不一致): 模型期望=%+v, 底表实际=%+v%s",
				i, expectedHandles[i], actualHandles[i], m.FormatTrace(testName, steps))
		}
	}
}

// verifyFormalLockT12GetLkProbes 在每步混合状态变更后执行双命名空间 GetLk 只读查账探针：
// 同时核对 lk_flags = 0 (POSIX) 与 lk_flags = FuseLkFlock (BSD flock) 的冲突探测及自锁忽略互不干扰。
func verifyFormalLockT12GetLkProbes(t *testing.T, env *formalLockTestEnv, rng *rand.Rand, testName string, steps int) {
	t.Helper()

	// 1. 执行 POSIX (lk_flags = 0) 探针
	verifyFormalLockGetLkProbes(t, env, rng, testName, steps)

	ctx := env.Ctx
	m := env.Model

	// 2. 随机挑选句柄执行 BSD flock (lk_flags = FuseLkFlock) 整文件区间 [0, OffsetMax] 探针
	ino := m.FileInos[rng.IntN(len(m.FileInos))]
	sid := m.SessionList[rng.IntN(len(m.SessionList))]
	owner := m.OwnerList[rng.IntN(len(m.OwnerList))]
	modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
	fh, ok := m.findHandleForSessionAndMode(ino, sid, modes[rng.IntN(len(modes))])
	require.True(t, ok)

	probeType := meta.F_WRLCK
	if rng.IntN(2) == 0 {
		probeType = meta.F_RDLCK
	}

	flockProbe := meta.GetLkParams{
		Ino:       ino,
		Fh:        fh,
		LockOwner: owner,
		Lk: meta.FileLock{
			Start: 0,
			End:   meta.OffsetMax,
			Type:  probeType,
			PID:   9500,
		},
		LkFlags:   meta.FuseLkFlock,
		SessionID: sid,
	}
	expLk, expErr := m.ApplyGetLk(flockProbe)
	actLk, actErr := env.Mgr.GetLk(ctx, flockProbe)
	if (expErr == nil) != (actErr == nil) || (expErr != nil && !errors.Is(actErr, expErr)) {
		t.Fatalf("GetLk(FuseLkFlock) 探针错误码不一致 (Ino=%d, sess=%d, fh=%d, type=%d): 模型期望 err=%v, 实际 err=%v%s",
			ino, sid, fh, probeType, expErr, actErr, m.FormatTrace(testName, steps))
	}
	if expErr == nil && *expLk != *actLk {
		t.Fatalf("GetLk(FuseLkFlock) 探针返回值不一致 (Ino=%d, sess=%d, fh=%d, type=%d): 模型期望 %+v, 实际 %+v%s",
			ino, sid, fh, probeType, expLk, actLk, m.FormatTrace(testName, steps))
	}

	// 3. 若当前文件存在由未关闭句柄持有的活跃 BSD flock，用该 (session_id, fh) 自测 GetLk(FuseLkFlock, F_WRLCK)，验证自锁被排除
	activeFlocks := m.activeFlockLocksByIno(ino)
	for _, fl := range activeFlocks {
		if _, valid := m.isHandleValid(fl.Fh, ino, &fl.SessionID); !valid {
			continue
		}
		selfFlockProbe := meta.GetLkParams{
			Ino:       ino,
			Fh:        fl.Fh,
			LockOwner: fl.LockOwner,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   8888,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: fl.SessionID,
		}
		expSelf, expSelfErr := m.ApplyGetLk(selfFlockProbe)
		actSelf, actSelfErr := env.Mgr.GetLk(ctx, selfFlockProbe)
		require.NoError(t, expSelfErr)
		require.NoError(t, actSelfErr)
		if *expSelf != *actSelf {
			t.Fatalf("GetLk(FuseLkFlock) 自锁忽略探针不一致: 模型期望 %+v, 实际 %+v%s",
				expSelf, actSelf, m.FormatTrace(testName, steps))
		}
		break
	}
}

// stepFormalLockT12Action 在单步中随机生成 POSIX SetLk、BSD flock SetLk、带/不带 lock_owner 的 Flush 以及带/不带 FuseReleaseFlockUnlock 的 Release。
func stepFormalLockT12Action(t *testing.T, env *formalLockTestEnv, rng *rand.Rand, stepIdx int, testName string, totalSteps int) {
	t.Helper()

	m := env.Model
	ino := m.FileInos[rng.IntN(len(m.FileInos))]
	sid := m.SessionList[rng.IntN(len(m.SessionList))]
	owner := m.OwnerList[rng.IntN(len(m.OwnerList))]
	pid := uint32(1000 + stepIdx)

	roll := rng.IntN(100)
	switch {
	case roll < 30:
		// 30% 概率：执行 POSIX 字节区间锁动作（含正常加解锁、裂解合并与异常入参/EBADF）
		stepFormalLockPosixAction(t, env, rng, stepIdx, testName, totalSteps)

	case roll < 68:
		// 38% 概率：执行 BSD flock (lk_flags = FuseLkFlock) 整文件加解锁、同句柄模式非原子转换与只读/只写句柄互斥测试
		modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
		accMode := modes[rng.IntN(len(modes))]
		fh, ok := m.findHandleForSessionAndMode(ino, sid, accMode)
		require.True(t, ok)

		// 若当前文件已有活跃 flock，35% 概率直接挑一个已持有活跃 flock 的有效句柄发起同句柄模式转换（覆盖 F_RDLCK -> F_WRLCK 非原子转换冲突删旧锁）
		activeFlocks := m.activeFlockLocksByIno(ino)
		if len(activeFlocks) > 0 && rng.IntN(100) < 35 {
			chosen := activeFlocks[rng.IntN(len(activeFlocks))]
			if _, valid := m.isHandleValid(chosen.Fh, ino, &chosen.SessionID); valid {
				sid = chosen.SessionID
				fh = chosen.Fh
				owner = chosen.LockOwner
			}
		}

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

		execAndVerifySetLk(t, env, meta.SetLkParams{
			Ino:       ino,
			Fh:        fh,
			LockOwner: owner,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  lkType,
				PID:   pid,
			},
			LkFlags:   meta.FuseLkFlock,
			SessionID: sid,
		}, fmt.Sprintf("随机BSD_flock加解锁(openMode=0x%x)", m.Handles[fh].Flags&meta.O_ACCMODE), testName, totalSteps)

	case roll < 76:
		// 8% 概率：BSD flock 非整文件区间或非法参数/无效句柄边界
		fh, ok := m.findHandleForSessionAndMode(ino, sid, meta.O_RDONLY)
		require.True(t, ok)

		switch rng.IntN(5) {
		case 0:
			// start != 0 -> ErrInvalidArgument
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 1, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: pid},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sid,
			}, "BSD_flock非零起点(start=1/EINVAL)", testName, totalSteps)
		case 1:
			// end != OffsetMax -> ErrInvalidArgument
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sid,
			}, "BSD_flock非整文件终点(end=100/EINVAL)", testName, totalSteps)
		case 2:
			// GetLk(FuseLkFlock) 非整文件区间 -> ErrInvalidArgument
			execAndVerifyGetLk(t, env, meta.GetLkParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: 50, Type: meta.F_WRLCK, PID: pid},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sid,
			}, "GetLk(FuseLkFlock)非整文件区间(EINVAL)", testName, totalSteps)
		case 3:
			// 对已关闭句柄或不存在句柄调用 SetLk(FuseLkFlock) -> ErrBadFileHandle
			badFh := uint64(99999999)
			if closedH, hasClosed := m.findClosedHandle(ino); hasClosed {
				badFh = closedH.Fh
			}
			execAndVerifySetLk(t, env, meta.SetLkParams{
				Ino:       ino,
				Fh:        badFh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: pid},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sid,
			}, "BSD_flock已关闭或无效句柄(EBADF)", testName, totalSteps)
		default:
			// 对目录调用 GetLk(FuseLkFlock) -> ErrIsDir
			execAndVerifyGetLk(t, env, meta.GetLkParams{
				Ino:       m.DirIno,
				Fh:        fh,
				LockOwner: owner,
				Lk:        meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: pid},
				LkFlags:   meta.FuseLkFlock,
				SessionID: sid,
			}, "目录节点GetLk(FuseLkFlock/EISDIR)", testName, totalSteps)
		}

	case roll < 88:
		// 12% 概率：执行 Flush 操作（带 lock_owner 仅清除目标 POSIX 锁且保留 flock 与句柄，以及异常边界）
		modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
		fh, ok := m.findHandleForSessionAndMode(ino, sid, modes[rng.IntN(len(modes))])
		require.True(t, ok)

		subCase := rng.IntN(6)
		switch subCase {
		case 0, 1, 2:
			// 携带 lock_owner 与 session_id：清除 (sid, ino, owner) 的 POSIX 锁，不删 BSD flock，不销毁 fh
			sidCopy := sid
			ownerCopy := owner
			execAndVerifyFlush(t, env, meta.FlushParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: &ownerCopy,
				SessionID: &sidCopy,
			}, "Flush携带lock_owner清理POSIX锁并保留flock", testName, totalSteps)
		case 3:
			// 仅携带 session_id 不带 lock_owner：只续期会话并校验句柄，不删任何锁
			sidCopy := sid
			execAndVerifyFlush(t, env, meta.FlushParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: nil,
				SessionID: &sidCopy,
			}, "Flush仅携带session_id不清理锁", testName, totalSteps)
		case 4:
			// 句柄绑定了 session_id 但 Flush 传 SessionID=nil -> ErrBadFileHandle
			ownerCopy := owner
			execAndVerifyFlush(t, env, meta.FlushParams{
				Ino:       ino,
				Fh:        fh,
				LockOwner: &ownerCopy,
				SessionID: nil,
			}, "Flush未传SessionID访问会话句柄(EBADF)", testName, totalSteps)
		default:
			// 对目录或已关闭句柄调用 Flush -> ErrIsDir / ErrBadFileHandle
			sidCopy := sid
			ownerCopy := owner
			if closedH, hasClosed := m.findClosedHandle(ino); hasClosed && rng.IntN(2) == 0 {
				execAndVerifyFlush(t, env, meta.FlushParams{
					Ino:       ino,
					Fh:        closedH.Fh,
					LockOwner: &ownerCopy,
					SessionID: &sidCopy,
				}, "Flush已关闭句柄(EBADF)", testName, totalSteps)
			} else {
				execAndVerifyFlush(t, env, meta.FlushParams{
					Ino:       m.DirIno,
					Fh:        fh,
					LockOwner: &ownerCopy,
					SessionID: &sidCopy,
				}, "Flush目录节点(EISDIR)", testName, totalSteps)
			}
		}

	default:
		// 12% 概率：执行 Release 操作（销毁句柄并在成功后动态 Open 补充新句柄）
		modes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
		accMode := modes[rng.IntN(len(modes))]
		fh, ok := m.findHandleForSessionAndMode(ino, sid, accMode)
		require.True(t, ok)

		subCase := rng.IntN(6)
		switch subCase {
		case 0, 1, 2:
			// 携带 FuseReleaseFlockUnlock（可选组合 FuseReleaseFlush 与 LockOwner）：销毁 fh，仅清除 (sid, ino, fh) 的 flock，绝不删其他 fh 的 flock 或 POSIX 锁
			sidCopy := sid
			ownerCopy := owner
			relFlags := meta.FuseReleaseFlockUnlock
			if rng.IntN(2) == 0 {
				relFlags |= meta.FuseReleaseFlush
			}
			var optOwner *uint64
			if rng.IntN(2) == 0 {
				optOwner = &ownerCopy
			}
			err := execAndVerifyRelease(t, env, meta.ReleaseParams{
				Ino:          ino,
				Fh:           fh,
				LockOwner:    optOwner,
				ReleaseFlags: relFlags,
				SessionID:    &sidCopy,
			}, "Release携带FuseReleaseFlockUnlock销毁句柄并清理自身flock", testName, totalSteps)
			require.NoError(t, err)
			replenishHandleAfterRelease(t, env, ino, sid, accMode, testName, totalSteps)

		case 3:
			// 不携带 FuseReleaseFlockUnlock（ReleaseFlags = 0 或 FuseReleaseFlush）：销毁 fh 但不清理锁
			// 为防止长序列早期遗留无法通过已关闭句柄解锁的孤立 flock 阻塞后续全部转化，当句柄未持有 flock 或处于最后 5 步收尾阶段时执行无解锁 Release，否则携带 FuseReleaseFlockUnlock
			sidCopy := sid
			ownerCopy := owner
			relFlags := uint32(0)
			if rng.IntN(2) == 0 {
				relFlags = meta.FuseReleaseFlush
			}
			if m.hasActiveFlockOnHandle(ino, sid, fh) && stepIdx <= totalSteps-5 {
				relFlags |= meta.FuseReleaseFlockUnlock
			}
			err := execAndVerifyRelease(t, env, meta.ReleaseParams{
				Ino:          ino,
				Fh:           fh,
				LockOwner:    &ownerCopy,
				ReleaseFlags: relFlags,
				SessionID:    &sidCopy,
			}, "Release销毁句柄(按标志位决定是否保留flock)", testName, totalSteps)
			require.NoError(t, err)
			replenishHandleAfterRelease(t, env, ino, sid, accMode, testName, totalSteps)

		case 4:
			// 非法 ReleaseFlags -> ErrInvalidArgument（句柄不被销毁）
			sidCopy := sid
			_ = execAndVerifyRelease(t, env, meta.ReleaseParams{
				Ino:          ino,
				Fh:           fh,
				ReleaseFlags: 0x80,
				SessionID:    &sidCopy,
			}, "Release非法ReleaseFlags(EINVAL)", testName, totalSteps)

		default:
			// 对已关闭句柄或跨会话句柄调用 Release -> ErrBadFileHandle
			sidCopy := sid
			badFh := uint64(88888888)
			if closedH, hasClosed := m.findClosedHandle(ino); hasClosed {
				badFh = closedH.Fh
			}
			_ = execAndVerifyRelease(t, env, meta.ReleaseParams{
				Ino:          ino,
				Fh:           badFh,
				ReleaseFlags: meta.FuseReleaseFlockUnlock,
				SessionID:    &sidCopy,
			}, "Release已关闭或不存在句柄(EBADF)", testName, totalSteps)
		}
	}
}

// runFormalLockT12DeterministicScenarios 确定性覆盖 T12 全部边界要求：
// 1. BSD flock 强制校验整文件区间 [0, OffsetMax]（否则返回 ErrInvalidArgument）；
// 2. BSD flock 允许 O_RDONLY 申请 F_WRLCK、允许 O_WRONLY 申请 F_RDLCK（不校验句柄读写位）；
// 3. 同一 (session_id, ino, fh) 模式转换时的非原子转换语义：先删旧 flock 再判冲突，冲突返回 ErrAgain 且旧 flock 已被持久化删除（对比 POSIX 锁冲突保留原锁）；
// 4. GetLk 与 SetLk 在 lk_flags = 0 与 lk_flags = FuseLkFlock 双命名空间之间完全隔离（且排除自身 (session_id, fh) 自锁）；
// 5. 携带 lock_owner 的 Flush 仅清除目标 (session_id, ino, lock_owner) 的 POSIX 锁，不销毁 FileHandle 且绝不删除任何 BSD flock；
// 6. 携带 FuseReleaseFlockUnlock 的 Release 销毁 FileHandle 并仅清除该 (session_id, ino, fh) 绑定的 BSD flock，绝不越权删除其他句柄的 flock 或同进程的 POSIX 锁；未携带 FuseReleaseFlockUnlock 时仅销毁句柄而不清理锁。
func runFormalLockT12DeterministicScenarios(t *testing.T) {
	const testName = "TestFormalLock_T12_FlockOrthogonality/DeterministicEdgeCases"
	env := setupFormalLockEnv(t, 0x20260417, 1, 3)
	m := env.Model

	ino := m.FileInos[0]
	s1, s2, s3 := m.SessionList[0], m.SessionList[1], m.SessionList[2]
	ownerA, ownerB := uint64(1001), uint64(2002)

	fh1RO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDONLY)
	fh1WO, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_WRONLY)
	fh1RW, _ := m.findHandleForSessionAndMode(ino, s1, meta.O_RDWR)
	fh2RO, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDONLY)
	fh2RW, _ := m.findHandleForSessionAndMode(ino, s2, meta.O_RDWR)
	fh3RW, _ := m.findHandleForSessionAndMode(ino, s3, meta.O_RDWR)

	// -------------------------------------------------------------------------
	// 场景 1: BSD flock 强制校验整文件区间 [0, OffsetMax] (非整文件区间返回 ErrInvalidArgument)
	// -------------------------------------------------------------------------
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 1, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 10},
	}, "BSD_flock非零起点[1,OffsetMax]返回EINVAL", testName, 1)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: 4096, Type: meta.F_WRLCK, PID: 10},
	}, "BSD_flock非OffsetMax终点[0,4096]返回EINVAL", testName, 1)

	execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 10, End: 100, Type: meta.F_WRLCK, PID: 10},
	}, "GetLk(FuseLkFlock)非整文件区间[10,100]返回EINVAL", testName, 1)

	// -------------------------------------------------------------------------
	// 场景 2: BSD flock 不校验句柄读写权限位（O_RDONLY 可加 F_WRLCK，O_WRONLY 可加 F_RDLCK）
	// -------------------------------------------------------------------------
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 101},
	}, "O_RDONLY只读句柄成功申请BSD_flock排他锁F_WRLCK", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// 同一 (s1, fh1RO) 自锁忽略探测：返回 F_UNLCK
	selfLk := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerB, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 999},
	}, "同一(s1,fh1RO)调用GetLk(FuseLkFlock)忽略自锁返回F_UNLCK", testName, 1)
	require.Equal(t, meta.F_UNLCK, selfLk.Type)

	// 同一会话 s1 下的另一个句柄 fh1WO (O_WRONLY) 探测并尝试申请 F_RDLCK -> 因 (s1, fh1WO) != (s1, fh1RO) 发生冲突
	conflictLk := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 999},
	}, "同会话不同句柄fh1WO探测到fh1RO的F_WRLCK冲突", testName, 1)
	require.Equal(t, meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 101}, *conflictLk)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 102},
	}, "同会话不同句柄fh1WO申请F_RDLCK与fh1RO的F_WRLCK冲突返回EAGAIN", testName, 1)

	// 将 fh1RO 的 F_WRLCK 原地降级为 F_RDLCK (pid=103)，验证 O_WRONLY 句柄 fh1WO 随后可成功共享申请 F_RDLCK
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 103},
	}, "fh1RO将flock从F_WRLCK降级为F_RDLCK(pid=103)", testName, 1)

	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 104},
	}, "O_WRONLY只写句柄fh1WO成功申请BSD_flock共享锁F_RDLCK(pid=104)", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// -------------------------------------------------------------------------
	// 场景 3: 同一 (session_id, ino, fh) 模式转换的非原子转换语义 vs POSIX 锁保留原锁对比
	// -------------------------------------------------------------------------
	// 3.1 先在 POSIX 命名空间 (lk_flags = 0) 建立共享读锁对比组：
	// (s1, ownerA) 与 (s2, ownerB) 均持有 [0, 100] F_RDLCK，当 (s1, ownerA) 升级为 F_WRLCK 冲突返回 EAGAIN 时，原 F_RDLCK 完整保留
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 201},
	}, "POSIX对比组:(s1,ownerA)获取[0,100]读锁", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_RDLCK, PID: 202},
	}, "POSIX对比组:(s2,ownerB)获取[0,100]读锁", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 203},
	}, "POSIX对比组:(s1,ownerA)升级[0,100]写锁遇冲突返回EAGAIN但保留原读锁", testName, 1)
	require.Len(t, m.activePosixLocksByIno(ino), 2, "POSIX 锁升级失败后原读锁必须保留")
	auditFormalLockInv2(t, env, testName, 1)

	// 3.2 在 BSD flock 命名空间 (lk_flags = FuseLkFlock) 验证非原子转换：
	// 当前 (s1, fh1RO) 与 (s1, fh1WO) 均持有 flock F_RDLCK。
	// 令 (s1, fh1RO) 请求升级为 F_WRLCK：第一步先删除 (s1, fh1RO) 的旧 F_RDLCK，第二步检测到与 (s1, fh1WO) 的 F_RDLCK 冲突，
	// 事务提交使旧锁删除持久化生效并返回 ErrAgain！
	require.True(t, m.hasActiveFlockOnHandle(ino, s1, fh1RO), "升级前 (s1, fh1RO) 持有 flock F_RDLCK")
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 105},
	}, "BSD_flock非原子转换:(s1,fh1RO)从F_RDLCK升级F_WRLCK遇冲突返回EAGAIN且旧F_RDLCK已被删除", testName, 1)
	require.False(t, m.hasActiveFlockOnHandle(ino, s1, fh1RO), "非原子转换冲突后 (s1, fh1RO) 的旧 flock 必须已被删除")
	require.Len(t, m.activeFlockLocksByIno(ino), 1, "仅剩 (s1, fh1WO) 的 1 条 flock")
	auditFormalLockInv2(t, env, testName, 1)

	// 进一步验证：当 (s1, fh1WO) 释放其 F_RDLCK 后，因 (s1, fh1RO) 的旧锁早已在刚才失败的升级中被删，此时会话 2 的 fh2RO 可直接获取排他锁 F_WRLCK！
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1WO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 106},
	}, "释放(s1,fh1WO)的flock读锁", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 301},
	}, "因(s1,fh1RO)旧锁已在非原子转换中删除，(s2,fh2RO)直接获取F_WRLCK成功", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// -------------------------------------------------------------------------
	// 场景 4: GetLk 与 SetLk 双命名空间完全隔离验证
	// -------------------------------------------------------------------------
	// 此时 (s2, fh2RO) 持有 BSD flock F_WRLCK [0, OffsetMax]。
	// 会话 1 先释放 (s2, ownerB) 的 POSIX 读锁，使 (s1, ownerA) 在 POSIX 命名空间升级 [0, OffsetMax] 全文件排他写锁 F_WRLCK 成功！
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerB, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_UNLCK, PID: 204},
	}, "释放(s2,ownerB)的POSIX读锁", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 205},
	}, "(s1,ownerA)在存在(s2,fh2RO)整文件BSD_flock写锁时仍能成功获取整文件POSIX写锁", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// 会话 3 调用 GetLk(lk_flags = 0) 只看到 (s1, ownerA) 的 POSIX 锁 (pid=205)，调用 GetLk(lk_flags = FuseLkFlock) 只看到 (s2, fh2RO) 的 flock 锁 (pid=301)
	posixConflict := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerA, SessionID: s3, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 400},
	}, "会话3在POSIX命名空间GetLk仅命中(s1,ownerA)的POSIX写锁(pid=205)", testName, 1)
	require.Equal(t, uint32(205), posixConflict.PID)

	flockConflict := execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh3RW, LockOwner: ownerA, SessionID: s3, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 400},
	}, "会话3在FuseLkFlock命名空间GetLk仅命中(s2,fh2RO)的flock写锁(pid=301)", testName, 1)
	require.Equal(t, uint32(301), flockConflict.PID)

	// -------------------------------------------------------------------------
	// 场景 5: 携带 lock_owner 的 Flush 对拍（仅删目标 POSIX 锁，不销毁句柄，绝不删除同 lock_owner 的 BSD flock）
	// -------------------------------------------------------------------------
	// 先释放 (s2, fh2RO) 的 flock，改由 (s1, fh1RW) 以相同 LockOwner = ownerA 申请 BSD flock F_WRLCK (pid=501)，
	// 同时令 (s1, ownerB) 与 (s2, ownerA) 也在其他区间持有 POSIX 锁
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RO, LockOwner: ownerB, SessionID: s2, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 302},
	}, "释放(s2,fh2RO)的BSD_flock", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 501},
	}, "(s1,fh1RW,ownerA)申请BSD_flock写锁(pid=501)", testName, 1)

	// 将 (s1, ownerA) 的 POSIX 写锁缩减到 [0, 50]，以便 (s1, ownerB) 锁 [60, 80]、(s2, ownerA) 锁 [90, 120]
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 51, End: meta.OffsetMax, Type: meta.F_UNLCK, PID: 206},
	}, "裁剪(s1,ownerA)的POSIX写锁为[0,50]", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerB, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 60, End: 80, Type: meta.F_RDLCK, PID: 207},
	}, "(s1,ownerB)获取POSIX读锁[60,80]", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh2RW, LockOwner: ownerA, SessionID: s2, LkFlags: 0,
		Lk: meta.FileLock{Start: 90, End: 120, Type: meta.F_WRLCK, PID: 208},
	}, "(s2,ownerA)获取POSIX写锁[90,120]", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// 通过 fh1RO 调用 Flush(Ino: ino, Fh: fh1RO, LockOwner: &ownerA, SessionID: &s1)
	// 验证：
	// 1) (s1, ino, ownerA) 的 POSIX 锁 [0, 50]（哪怕最初通过 fh1RW 申请）被清除；
	// 2) (s1, ino, ownerB) 的 POSIX 锁 [60, 80] 与 (s2, ino, ownerA) 的 POSIX 锁 [90, 120] 保留；
	// 3) (s1, ino, fh1RW, ownerA) 的 BSD flock (pid=501) 完整保留；
	// 4) fh1RO 句柄未被销毁，后续仍可正常调用 GetLk/SetLk。
	execAndVerifyFlush(t, env, meta.FlushParams{
		Ino:       ino,
		Fh:        fh1RO,
		LockOwner: &ownerA,
		SessionID: &s1,
	}, "Flush(s1,fh1RO,ownerA)仅清除(s1,ownerA)的POSIX锁且保留同ownerA的BSD_flock", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)
	require.Len(t, m.activePosixLocksByIno(ino), 2, "仅剩 (s1,ownerB) 与 (s2,ownerA) 两条 POSIX 锁")
	require.Len(t, m.activeFlockLocksByIno(ino), 1, "(s1,fh1RW,ownerA) 的 BSD flock 严禁被 Flush 删除")

	// -------------------------------------------------------------------------
	// 场景 6: 携带/不携带 FuseReleaseFlockUnlock 的 Release 对拍與句柄补充
	// -------------------------------------------------------------------------
	// 6.1 先将 (s1, fh1RW) 的 flock 降级为 F_RDLCK，并令 (s1, fh1RO) 也持有 flock F_RDLCK，同时令 (s1, ownerA) 重新持有 POSIX 写锁 [0, 30]
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RW, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 502},
	}, "(s1,fh1RW)降级BSD_flock为F_RDLCK(pid=502)", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 503},
	}, "(s1,fh1RO)获取共享BSD_flock读锁(pid=503)", testName, 1)
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: 0,
		Lk: meta.FileLock{Start: 0, End: 30, Type: meta.F_RDLCK, PID: 504},
	}, "(s1,ownerA)通过fh1RO获取POSIX读锁[0,30](pid=504)", testName, 1)
	auditFormalLockInv2(t, env, testName, 1)

	// 6.2 调用 Release(fh1RO, LockOwner: &ownerA, ReleaseFlags: FuseReleaseFlush | FuseReleaseFlockUnlock, SessionID: &s1)
	// 验证：
	// - fh1RO 被销毁（再次使用 fh1RO 调用 GetLk 返回 ErrBadFileHandle）；
	// - (s1, fh1RO) 的 BSD flock (pid=503) 被清除；
	// - 同会话另一句柄 (s1, fh1RW) 的 BSD flock (pid=502) 保留；
	// - 同会话同进程的 POSIX 写锁 [0, 30] (pid=504) 保留（即使传入了 LockOwner: &ownerA 与 FuseReleaseFlush）！
	relErr := execAndVerifyRelease(t, env, meta.ReleaseParams{
		Ino:          ino,
		Fh:           fh1RO,
		LockOwner:    &ownerA,
		ReleaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock,
		SessionID:    &s1,
	}, "Release(s1,fh1RO,FuseReleaseFlockUnlock)销毁fh1RO并仅清理fh1RO的flock", testName, 1)
	require.NoError(t, relErr)
	auditFormalLockInv2(t, env, testName, 1)
	require.False(t, m.hasActiveFlockOnHandle(ino, s1, fh1RO), "fh1RO 的 flock 必须已被清理")
	require.True(t, m.hasActiveFlockOnHandle(ino, s1, fh1RW), "同会话另一句柄 fh1RW 的 flock 严禁被越权清理")
	require.Len(t, m.activePosixLocksByIno(ino), 3, "POSIX 记录锁严禁被 Release 清理")

	// 对已销毁的 fh1RO 调用 GetLk -> 返回 ErrBadFileHandle
	execAndVerifyGetLk(t, env, meta.GetLkParams{
		Ino: ino, Fh: fh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 505},
	}, "对已Release销毁的fh1RO调用GetLk返回EBADF", testName, 1)

	// 动态 Open 补充 s1 的 O_RDONLY 新句柄 newFh1RO
	newFh1RO := replenishHandleAfterRelease(t, env, ino, s1, meta.O_RDONLY, testName, 1)
	require.NotEqual(t, fh1RO, newFh1RO)

	// 6.3 验证未携带 FuseReleaseFlockUnlock 的 Release（ReleaseFlags = 0）：仅销毁句柄而不清理该句柄绑定的 BSD flock
	execAndVerifySetLk(t, env, meta.SetLkParams{
		Ino: ino, Fh: newFh1RO, LockOwner: ownerA, SessionID: s1, LkFlags: meta.FuseLkFlock,
		Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 601},
	}, "新句柄newFh1RO获取BSD_flock读锁(pid=601)", testName, 1)

	relNoUnlockErr := execAndVerifyRelease(t, env, meta.ReleaseParams{
		Ino:          ino,
		Fh:           newFh1RO,
		LockOwner:    &ownerA,
		ReleaseFlags: 0,
		SessionID:    &s1,
	}, "Release(s1,newFh1RO,ReleaseFlags=0)仅销毁句柄而不清理flock", testName, 1)
	require.NoError(t, relNoUnlockErr)
	require.True(t, m.hasActiveFlockOnHandle(ino, s1, newFh1RO), "未携带 FuseReleaseFlockUnlock 时底表与模型中的 flock 必须保留")
	auditFormalLockInv2(t, env, testName, 1)
}

// TestFormalLock_T12_FlockOrthogonality 验证 BSD flock 非原子转换、POSIX/flock 双命名空间正交（含 Flush/Release）与底表 LOCK-INV-2 审计。
func TestFormalLock_T12_FlockOrthogonality(t *testing.T) {
	const testName = "TestFormalLock_T12_FlockOrthogonality"

	t.Run("DeterministicEdgeCases", func(t *testing.T) {
		runFormalLockT12DeterministicScenarios(t)
	})

	cfg := loadFormalLockConfig(t)
	t.Logf("启动 T12 BSD flock 与双命名空间正交形式化状态机验证: baseSeed=0x%x, runs=%d, steps=%d", cfg.BaseSeed, cfg.Runs, cfg.Steps)

	for runIdx := 0; runIdx < cfg.Runs; runIdx++ {
		runSeed := cfg.BaseSeed + uint64(runIdx)
		t.Run(fmt.Sprintf("Run_%d_Seed_0x%x", runIdx, runSeed), func(t *testing.T) {
			env := setupFormalLockEnv(t, runSeed, 2, 3)
			rng := rand.New(rand.NewPCG(runSeed, runSeed^0x517cc1b727220a95))

			for step := 1; step <= cfg.Steps; step++ {
				stepFormalLockT12Action(t, env, rng, step, testName, cfg.Steps)
				verifyFormalLockT12GetLkProbes(t, env, rng, testName, cfg.Steps)
				auditFormalLockInv2(t, env, testName, cfg.Steps)
			}

			// 每轮结束时通过公开接口 GetAttr 校验所有受测文件时间戳与初始快照严格一致
			for _, ino := range env.Model.FileInos {
				attrRes, err := env.Mgr.GetAttr(env.Ctx, ino)
				require.NoError(t, err)
				expInode := env.Model.Inodes[ino]
				require.True(t, attrRes.Attr.Atime.Equal(expInode.Atime), "GetAttr atime 不得被文件锁或 Flush/Release 修改")
				require.True(t, attrRes.Attr.Mtime.Equal(expInode.Mtime), "GetAttr mtime 不得被文件锁或 Flush/Release 修改")
				require.True(t, attrRes.Attr.Ctime.Equal(expInode.Ctime), "GetAttr ctime 不得被文件锁或 Flush/Release 修改")
			}
		})
	}
}
