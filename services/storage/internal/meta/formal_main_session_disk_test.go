package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T10 核心动作：RenewSession（含毫秒级短租约超期等待、失效句柄 EBADF 防诈尸核验与孤儿物理回收）、DeleteDisk（强制级联销毁整盘）
	registerFormalAction("RenewSession", 24, formalActionRenewSession)
	registerFormalAction("DeleteDisk", 16, formalActionDeleteDisk)

	// 注册 T10 底表数学不变量审计器：INV-3 (RenewSession 清扫后零无引用孤儿与过期会话级联清零) & INV-4 (DeleteDisk 后底表五表零记录残留)
	registerFormalAuditor("INV-3_INV-4_SessionAndDeleteDisk", formalAuditT10SessionAndDeleteDisk)
}

// ============================================================================
// 1. T10 参考模型推导函数与短租约/删盘辅助函数
// ============================================================================

// formalPredictRenewSession 根据内存参考模型预测 RenewSession / RenewSessionWithStatus 的静态入参错误码。
func formalPredictRenewSession(sessionID uint64) error {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return meta.ErrInvalidArgument
	}
	return nil
}

// formalPredictDeleteDisk 根据内存参考模型预测 DeleteDisk 的目标磁盘与预期错误码。
// 严格对齐 manager_disk.go 中 DeleteDisk 的 UUID 静态校验（非法格式或全零 Nil UUID 返回 ErrInvalidArgument）
// 与事务内存在性校验（未命中或已删除返回 ErrNotFound）。
func formalPredictDeleteDisk(m *formalMainModel, diskID string) (targetDisk *formalDisk, expectedErr error) {
	parsedUUID, err := uuid.Parse(diskID)
	if err != nil || parsedUUID == uuid.Nil {
		return nil, meta.ErrInvalidArgument
	}

	d, exists := m.Disks[diskID]
	if !exists || d.Deleted {
		return nil, meta.ErrNotFound
	}
	return d, nil
}

// nextIsolatedFormalSessionID 生成与当前测试种子及轮次严格隔离的客户端会话 ID，避免多测试间 client_sessions 互相干扰。
func nextIsolatedFormalSessionID(env *formalMainEnv) uint64 {
	base := uint64(90000000) + (env.Seed%10000000)*100 + uint64(env.RunIndex)*1000
	return base + env.NextSeq()
}

// verifyExpiredHandleAntiResurrection 对已过期会话 (expSID) 名下的文件句柄或目录句柄调用全套句柄接口，
// 强制验证：
// 1. Flush / Fsync / SetAttr(Fh) / Fallocate / Release / FsyncDir / ReadDir / ReadDirPlus / ReleaseDir 均返回 ErrBadFileHandle (EBADF)；
// 2. 已过期会话绝不能通过操作失效句柄“诈尸”自救（底表 client_sessions 中 expSID 的 expires_at <= clock_timestamp() 恒成立）。
func verifyExpiredHandleAntiResurrection(
	ctx context.Context,
	t *testing.T,
	mgr meta.Manager,
	expSID uint64,
	fileHandles []*formalHandle,
	dirHandles []*formalHandle,
) {
	t.Helper()
	sidCopy := expSID

	for _, fh := range fileHandles {
		// 1. Flush (携带过期 SessionID) -> ErrBadFileHandle
		owner := uint64(7001)
		err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       fh.Ino,
			Fh:        fh.Fh,
			LockOwner: &owner,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的句柄 fh=%d (ino=%d) 调用 Flush 必须返回 ErrBadFileHandle", expSID, fh.Fh, fh.Ino)

		// 2. Fsync (携带过期 SessionID) -> ErrBadFileHandle
		err = mgr.Fsync(ctx, meta.FsyncParams{
			Ino:       fh.Ino,
			Fh:        fh.Fh,
			Datasync:  true,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的句柄 fh=%d (ino=%d) 调用 Fsync 必须返回 ErrBadFileHandle", expSID, fh.Fh, fh.Ino)

		// 3. SetAttr(Fh) (携带过期 SessionID) -> ErrBadFileHandle
		fhVal := fh.Fh
		newUID := uint32(1002)
		_, err = mgr.SetAttr(ctx, fh.Ino, meta.SetAttrInput{
			UID:       &newUID,
			Fh:        &fhVal,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的句柄 fh=%d (ino=%d) 调用 SetAttr(Fh) 必须返回 ErrBadFileHandle", expSID, fh.Fh, fh.Ino)

		// 4. Fallocate (携带过期 SessionID，仅当目标为常规文件时；特殊节点在 Fh 校验前会返回 ErrNotSupported)
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:       fh.Ino,
			Fh:        fh.Fh,
			Offset:    0,
			Length:    64,
			Mode:      meta.FallocFlKeepSize,
			SessionID: &sidCopy,
		})
		require.Truef(t, errors.Is(err, meta.ErrBadFileHandle) || errors.Is(err, meta.ErrNotSupported),
			"过期会话 sid=%d 的句柄 fh=%d (ino=%d) 调用 Fallocate 必须被拒绝, 实际: %v", expSID, fh.Fh, fh.Ino, err)

		// 5. Release (携带过期 SessionID) -> ErrBadFileHandle（且事务回滚，不复活会话）
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:          fh.Ino,
			Fh:           fh.Fh,
			ReleaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock,
			SessionID:    &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的句柄 fh=%d (ino=%d) 调用 Release 必须返回 ErrBadFileHandle", expSID, fh.Fh, fh.Ino)
	}

	for _, dh := range dirHandles {
		// 6. FsyncDir -> ErrBadFileHandle
		err := mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       dh.Ino,
			Fh:        dh.Fh,
			Datasync:  false,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的目录句柄 fh=%d (ino=%d) 调用 FsyncDir 必须返回 ErrBadFileHandle", expSID, dh.Fh, dh.Ino)

		// 7. ReadDir -> ErrBadFileHandle
		_, err = mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       dh.Ino,
			Fh:        dh.Fh,
			Offset:    0,
			Limit:     16,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的目录句柄 fh=%d (ino=%d) 调用 ReadDir 必须返回 ErrBadFileHandle", expSID, dh.Fh, dh.Ino)

		// 8. ReadDirPlus -> ErrBadFileHandle
		_, err = mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       dh.Ino,
			Fh:        dh.Fh,
			Offset:    0,
			Limit:     16,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的目录句柄 fh=%d (ino=%d) 调用 ReadDirPlus 必须返回 ErrBadFileHandle", expSID, dh.Fh, dh.Ino)

		// 9. ReleaseDir -> ErrBadFileHandle（且事务回滚，不复活会话）
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dh.Ino,
			Fh:        dh.Fh,
			SessionID: &sidCopy,
		})
		require.ErrorIsf(t, err, meta.ErrBadFileHandle,
			"过期会话 sid=%d 的目录句柄 fh=%d (ino=%d) 调用 ReleaseDir 必须返回 ErrBadFileHandle", expSID, dh.Fh, dh.Ino)
	}
}

type pgxRowScanner interface {
	Scan(dest ...any) error
}

// assertSessionStillExpiredInDB 核验底表 client_sessions 中指定会话仍处于已过期状态（未被失效句柄操作“诈尸”续期）。
func assertSessionStillExpiredInDB(ctx context.Context, t *testing.T, env *formalMainEnv, expSID uint64) {
	t.Helper()
	var stillExpired bool
	err := env.Pool.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT expires_at <= clock_timestamp() FROM client_sessions WHERE session_id = $1),
			true
		)
	`, int64(expSID)).Scan(&stillExpired)
	require.NoError(t, err, "查询 client_sessions 核验过期状态失败")
	require.Truef(t, stillExpired,
		"防诈尸违例: 已过期会话 sid=%d 在调用失效句柄操作后竟然在 client_sessions 中复活 (expires_at > clock_timestamp())", expSID)
}

// ============================================================================
// 2. T10 动作实现：RenewSession 与 DeleteDisk 随机状态机生成与对拍
// ============================================================================

// formalActionRenewSession 在状态机中随机执行：
// 1. 毫秒级短租约 WithSessionLeaseTTL(20ms) 会话超期等待 + 失效句柄全接口 EBADF 防诈尸核验 + 显式 RenewSession 级联清理与孤儿节点物理回收/配额退还（含混合句柄孤儿保留对照）；
// 2. 正常创建新客户端会话或续期已有活跃会话（覆盖 RenewSession 与 RenewSessionWithStatus）；
// 3. 跨会话句柄访问拦截（有会话句柄传 nil/异构 SessionID、无会话句柄传非空 SessionID -> ErrBadFileHandle）；
// 4. 非法 SessionID (0 或 > math.MaxInt64) 静态校验拦截 (ErrInvalidArgument)。
func formalActionRenewSession(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	branch := env.Rng.IntN(100)

	switch {
	case branch < 48:
		// 重点场景 1：模拟毫秒级短租约 WithSessionLeaseTTL(20ms) 客户端会话过期 + 防诈尸核验 + 另一会话显式调用 RenewSession 触发孤儿物理回收与配额退还
		return formalStepShortLeaseExpireAndSweep(ctx, t, env)

	case branch < 74:
		// 重点场景 2：正常调用 RenewSession 或 RenewSessionWithStatus 创建新会话或续期已有活跃会话
		var targetSID uint64
		var existedBefore bool
		for _, sid := range env.Model.AllSessionIDs {
			if s, ok := env.Model.Sessions[sid]; ok && !s.Expired {
				if env.Rng.IntN(2) == 0 {
					targetSID = sid
					existedBefore = true
					break
				}
			}
		}
		if targetSID == 0 {
			targetSID = nextIsolatedFormalSessionID(env)
		}

		expectedErr := formalPredictRenewSession(targetSID)
		require.NoError(t, expectedErr)

		if env.Rng.IntN(2) == 0 {
			res, err := env.Mgr.RenewSessionWithStatus(ctx, targetSID)
			require.NoErrorf(t, err, "RenewSessionWithStatus(sid=%d) 预期成功", targetSID)
			require.True(t, res.ExpiresAt.After(time.Now().Add(-time.Second)), "RenewSessionWithStatus 返回的 ExpiresAt 必须有效")
			require.Equalf(t, !existedBefore, res.SessionRecreated,
				"RenewSessionWithStatus(sid=%d) SessionRecreated 预测不一致 (existedBefore=%v)", targetSID, existedBefore)
			modelSyncSessionRenewal(env.Model, &targetSID, env.LeaseTTL)
			return fmt.Sprintf("RenewSessionWithStatus[正常续期或建会话](sid=%d) -> 成功 (recreated=%v, expires_at=%s)",
				targetSID, res.SessionRecreated, res.ExpiresAt.Format(time.RFC3339Nano))
		}

		expAt, err := env.Mgr.RenewSession(ctx, targetSID)
		require.NoErrorf(t, err, "RenewSession(sid=%d) 预期成功", targetSID)
		require.True(t, expAt.After(time.Now().Add(-time.Second)), "RenewSession 返回的 ExpiresAt 必须有效")
		modelSyncSessionRenewal(env.Model, &targetSID, env.LeaseTTL)
		return fmt.Sprintf("RenewSession[正常续期或建会话](sid=%d, existed=%v) -> 成功 (expires_at=%s)",
			targetSID, existedBefore, expAt.Format(time.RFC3339Nano))

	case branch < 88:
		// 重点场景 3：有会话/无会话句柄跨会话越权访问拦截 (ErrBadFileHandle)
		return formalStepCrossSessionHandleGuard(ctx, t, env)

	default:
		// 重点场景 4：非法 SessionID (0 或 > math.MaxInt64) 调用 RenewSession 与 RenewSessionWithStatus -> ErrInvalidArgument
		var badSID uint64
		var caseTag string
		switch env.Rng.IntN(3) {
		case 0:
			badSID = 0
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
		case 1:
			badSID = uint64(math.MaxInt64) + 1
			caseTag = "非法SessionID=MaxInt64+1(ErrInvalidArgument)"
		default:
			badSID = ^uint64(0)
			caseTag = "非法SessionID=MaxUint64(ErrInvalidArgument)"
		}

		expectedErr := formalPredictRenewSession(badSID)
		require.ErrorIs(t, expectedErr, meta.ErrInvalidArgument)

		expAt, err := env.Mgr.RenewSession(ctx, badSID)
		require.ErrorIsf(t, err, meta.ErrInvalidArgument, "RenewSession[%s] 必须返回 ErrInvalidArgument", caseTag)
		require.True(t, expAt.IsZero())

		statusRes, err2 := env.Mgr.RenewSessionWithStatus(ctx, badSID)
		require.ErrorIsf(t, err2, meta.ErrInvalidArgument, "RenewSessionWithStatus[%s] 必须返回 ErrInvalidArgument", caseTag)
		require.True(t, statusRes.ExpiresAt.IsZero())

		return fmt.Sprintf("RenewSession[%s](sid=%d) -> 预期拒绝 (ErrInvalidArgument)", caseTag, badSID)
	}
}

// formalStepShortLeaseExpireAndSweep 使用 meta.WithSessionLeaseTTL(20ms) 构造待过期会话 expSID 的打开句柄、文件锁与四类孤儿节点
// （涵盖 unlink-whilst-open 常规文件、rmdir-whilst-open 孤儿目录、未具名化 TmpFile，以及同时被无会话/存活会话句柄引用的混合孤儿节点），
// 等待 25ms 使 expSID 租约超期，验证失效句柄操作全部返回 ErrBadFileHandle 且不诈尸，最后由存活会话 keepSID 调用 RenewSession
// 触发同步清扫与配额退还对拍。
func formalStepShortLeaseExpireAndSweep(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	disk := env.Model.PickActiveDisk(env.Rng)
	if disk == nil {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	expSID := nextIsolatedFormalSessionID(env)
	keepSID := nextIsolatedFormalSessionID(env)

	// 先在长租约 Manager 上注册 keepSID 与 expSID，确保构造前置数据期间租约始终有效绝不提前过期
	keepExpAt, err := env.Mgr.RenewSession(ctx, keepSID)
	require.NoError(t, err, "注册存活会话 keepSID 不应失败")
	env.Model.RecordSession(keepSID, keepExpAt)

	expExpAt, err := env.Mgr.RenewSession(ctx, expSID)
	require.NoError(t, err, "注册待过期会话 expSID 不应失败")
	env.Model.RecordSession(expSID, expExpAt)

	var expFileHandles []*formalHandle
	var expDirHandles []*formalHandle
	var scenarioTag string

	// 按随机数选择本次短租约过期回收的子场景（四类孤儿回收 + 混合句柄孤儿保留 + 锁级联清理）
	subCase := env.Rng.IntN(4)
	switch subCase {
	case 0:
		// 子场景 A：unlink-whilst-open 常规文件孤儿 (仅 expSID 持有句柄 + POSIX 写锁) + 混合句柄孤儿 (expSID + nil 无会话句柄共同持有)
		scenarioTag = "unlink-whilst-open常规文件孤儿回收+混合nil句柄孤儿保留+文件锁级联清理"
		fName1 := env.UniqueEntryName("exp_orphan_reg")
		cRes1, err := env.Mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fName1),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		f1Model := &formalInode{
			Ino: cRes1.Inode.Ino, Generation: cRes1.Inode.Generation, RootIno: disk.RootIno,
			Mode: cRes1.Inode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(f1Model)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: fName1}] = f1Model.Ino
		disk.UsedInodes++
		modelSyncSessionRenewal(env.Model, &expSID, env.LeaseTTL)
		h1 := &formalHandle{
			Fh: cRes1.Fh, Ino: f1Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
			SessionID: modelCloneSessionID(&expSID), IsDir: false,
		}
		env.Model.RecordNewHandle(h1)
		expFileHandles = append(expFileHandles, h1)

		// 若磁盘有剩余字节配额，为 f1 赋予非零大小以检验孤儿回收退还 used_bytes
		if disk.MaxBytes > disk.UsedBytes && (disk.MaxBytes-disk.UsedBytes) >= 1024 {
			sz := uint64(1024)
			_, err = env.Mgr.SetAttr(ctx, f1Model.Ino, meta.SetAttrInput{Size: &sz, Fh: &cRes1.Fh, SessionID: &expSID})
			require.NoError(t, err)
			f1Model.Size = sz
			disk.UsedBytes += sz
		}

		// 在 f1 上以 expSID 加一把 POSIX 写锁，验证会话过期后 RenewSession 级联清除 file_locks
		err = env.Mgr.SetLk(ctx, meta.SetLkParams{
			Ino: f1Model.Ino, Fh: cRes1.Fh, LockOwner: 9001, SessionID: expSID, LkFlags: 0,
			Lk: meta.FileLock{Start: 0, End: 128, Type: meta.F_WRLCK, PID: 88},
		})
		require.NoError(t, err)

		// Unlink f1 使其成为仅由 expSID 持有的 nlink == 0 孤儿常规文件
		err = env.Mgr.Unlink(ctx, disk.RootIno, []byte(fName1))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: fName1})
		f1Model.Nlink = 0

		// 同时创建一个混合句柄孤儿 fMix：同时绑定 expSID 句柄与 session_id == nil 无会话句柄！
		fNameMix := env.UniqueEntryName("mix_orphan_reg")
		cResMix, err := env.Mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fNameMix),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		fMixModel := &formalInode{
			Ino: cResMix.Inode.Ino, Generation: cResMix.Inode.Generation, RootIno: disk.RootIno,
			Mode: cResMix.Inode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fMixModel)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: fNameMix}] = fMixModel.Ino
		disk.UsedInodes++
		hMixExp := &formalHandle{
			Fh: cResMix.Fh, Ino: fMixModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
			SessionID: modelCloneSessionID(&expSID), IsDir: false,
		}
		env.Model.RecordNewHandle(hMixExp)
		expFileHandles = append(expFileHandles, hMixExp)

		// 再用 session_id == nil 打开 fMix
		openNilRes, err := env.Mgr.Open(ctx, meta.OpenParams{
			Ino:       fMixModel.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: nil,
		})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openNilRes.Fh, Ino: fMixModel.Ino, Flags: meta.O_RDONLY,
			SessionID: nil, IsDir: false,
		})

		// Unlink fMix 使其变为同时具有 expSID 句柄和 nil 无会话句柄的孤儿文件
		err = env.Mgr.Unlink(ctx, disk.RootIno, []byte(fNameMix))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: fNameMix})
		fMixModel.Nlink = 0

	case 1:
		// 子场景 B：rmdir-whilst-open 孤儿目录 (仅 expSID 持有目录句柄)
		scenarioTag = "rmdir-whilst-open孤儿目录短租约超期物理回收与used_inodes退还"
		dName := env.UniqueEntryName("exp_orphan_dir")
		dirNode, err := env.Mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte(dName),
			Mode:      0o755,
		})
		require.NoError(t, err)
		rootDir := env.Model.Inodes[disk.RootIno]
		rootDir.Nlink++
		disk.UsedInodes++
		dirModel := &formalInode{
			Ino: dirNode.Ino, Generation: dirNode.Generation, RootIno: disk.RootIno, ParentIno: disk.RootIno,
			Mode: dirNode.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(dirModel)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: dName}] = dirModel.Ino

		openDirRes, err := env.Mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dirModel.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &expSID, env.LeaseTTL)
		dh := &formalHandle{
			Fh: openDirRes.Fh, Ino: dirModel.Ino, Flags: meta.O_RDONLY,
			SessionID: modelCloneSessionID(&expSID), IsDir: true,
		}
		env.Model.RecordNewHandle(dh)
		expDirHandles = append(expDirHandles, dh)

		// Rmdir 该打开的空子目录，使其变为 nlink == 0 的孤儿目录
		err = env.Mgr.Rmdir(ctx, disk.RootIno, []byte(dName))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: dName})
		rootDir.Nlink--
		dirModel.Nlink = 0

	case 2:
		// 子场景 C：未具名化匿名 TmpFile (nlink == 0, S_IFREG, 非零大小, 仅 expSID 持有句柄)
		scenarioTag = "未具名化TmpFile短租约超期物理回收与used_bytes配额退还"
		tmpRes, err := env.Mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: disk.RootIno,
			Mode:      0o600,
			Flags:     meta.O_RDWR | meta.O_TMPFILE,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &expSID, env.LeaseTTL)
		tmpModel := &formalInode{
			Ino: tmpRes.Inode.Ino, Generation: tmpRes.Inode.Generation, RootIno: disk.RootIno,
			Mode: tmpRes.Inode.Attr.Mode, Size: 0, Nlink: 0, IsTmpFile: true,
		}
		env.Model.RecordNewInode(tmpModel)
		disk.UsedInodes++
		th := &formalHandle{
			Fh: tmpRes.Fh, Ino: tmpModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE,
			SessionID: modelCloneSessionID(&expSID), IsDir: false,
		}
		env.Model.RecordNewHandle(th)
		expFileHandles = append(expFileHandles, th)

		if disk.MaxBytes > disk.UsedBytes && (disk.MaxBytes-disk.UsedBytes) >= 2048 {
			sz := uint64(2048)
			_, err = env.Mgr.SetAttr(ctx, tmpModel.Ino, meta.SetAttrInput{Size: &sz, Fh: &tmpRes.Fh, SessionID: &expSID})
			require.NoError(t, err)
			tmpModel.Size = sz
			disk.UsedBytes += sz
		}

	default:
		// 子场景 D：unlink-whilst-open 特殊设备节点 (S_IFIFO, size == 0) + 同时被 keepSID 存活会话引用的混合孤儿常规文件
		scenarioTag = "特殊文件S_IFIFO孤儿回收+存活会话keepSID混合句柄孤儿保留"
		fifoName := env.UniqueEntryName("exp_orphan_fifo")
		fifoNode, err := env.Mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fifoName),
			Mode:      meta.S_IFIFO | 0o640,
		})
		require.NoError(t, err)
		fifoModel := &formalInode{
			Ino: fifoNode.Ino, Generation: fifoNode.Generation, RootIno: disk.RootIno,
			Mode: fifoNode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fifoModel)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: fifoName}] = fifoModel.Ino
		disk.UsedInodes++

		openFifoRes, err := env.Mgr.Open(ctx, meta.OpenParams{
			Ino:       fifoModel.Ino,
			Flags:     meta.O_RDWR,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &expSID, env.LeaseTTL)
		fhFifo := &formalHandle{
			Fh: openFifoRes.Fh, Ino: fifoModel.Ino, Flags: meta.O_RDWR,
			SessionID: modelCloneSessionID(&expSID), IsDir: false,
		}
		env.Model.RecordNewHandle(fhFifo)
		expFileHandles = append(expFileHandles, fhFifo)

		err = env.Mgr.Unlink(ctx, disk.RootIno, []byte(fifoName))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: fifoName})
		fifoModel.Nlink = 0

		// 同时创建由 expSID 与 keepSID 双会话共同持有的孤儿常规文件
		sharedName := env.UniqueEntryName("shared_sess_orphan")
		cShared, err := env.Mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(sharedName),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &expSID,
		})
		require.NoError(t, err)
		sharedModel := &formalInode{
			Ino: cShared.Inode.Ino, Generation: cShared.Inode.Generation, RootIno: disk.RootIno,
			Mode: cShared.Inode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(sharedModel)
		env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: sharedName}] = sharedModel.Ino
		disk.UsedInodes++
		fhSharedExp := &formalHandle{
			Fh: cShared.Fh, Ino: sharedModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
			SessionID: modelCloneSessionID(&expSID), IsDir: false,
		}
		env.Model.RecordNewHandle(fhSharedExp)
		expFileHandles = append(expFileHandles, fhSharedExp)

		openKeepRes, err := env.Mgr.Open(ctx, meta.OpenParams{
			Ino:       sharedModel.Ino,
			Flags:     meta.O_RDWR,
			SessionID: &keepSID,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &keepSID, env.LeaseTTL)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openKeepRes.Fh, Ino: sharedModel.Ino, Flags: meta.O_RDWR,
			SessionID: modelCloneSessionID(&keepSID), IsDir: false,
		})

		err = env.Mgr.Unlink(ctx, disk.RootIno, []byte(sharedName))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: sharedName})
		sharedModel.Nlink = 0
	}

	// 前置全部孤儿与句柄就绪后，使用 shortMgr (60ms 短租约) 将 expSID 租约原子收缩，并等待 75ms 使其在 PostgreSQL 中确定性过期
	const shortTTL = 60 * time.Millisecond
	shortMgr := meta.New(env.Store, meta.WithSessionLeaseTTL(shortTTL))
	_, err = shortMgr.RenewSession(ctx, expSID)
	require.NoError(t, err)
	time.Sleep(shortTTL + 15*time.Millisecond)
	if sess, ok := env.Model.Sessions[expSID]; ok {
		sess.Expired = true
	}

	// 验证所有绑定在过期会话 expSID 上的句柄调用全套句柄接口必返回 ErrBadFileHandle 且绝不诈尸自救
	verifyExpiredHandleAntiResurrection(ctx, t, shortMgr, expSID, expFileHandles, expDirHandles)
	assertSessionStillExpiredInDB(ctx, t, env, expSID)

	// 由存活会话 keepSID 显式调用 RenewSession 触发过期会话清理、孤儿节点物理回收与磁盘配额退还
	usedInodesBefore := disk.UsedInodes
	usedBytesBefore := disk.UsedBytes

	newExpAt, err := env.Mgr.RenewSession(ctx, keepSID)
	require.NoError(t, err, "存活会话 keepSID 调用 RenewSession 清扫过期会话与孤儿不应失败")
	require.True(t, newExpAt.After(time.Now().Add(-time.Second)))

	modelSyncSessionRenewal(env.Model, &keepSID, env.LeaseTTL)

	return fmt.Sprintf("RenewSession[20ms短租约超期清扫:%s](exp_sid=%d -> keep_sid=%d) -> 成功 (disk_inodes:%d->%d, disk_bytes:%d->%d)",
		scenarioTag, expSID, keepSID, usedInodesBefore, disk.UsedInodes, usedBytesBefore, disk.UsedBytes)
}

// formalStepCrossSessionHandleGuard 验证有会话句柄与无会话句柄之间的跨会话访问鉴权边界 (ErrBadFileHandle)。
func formalStepCrossSessionHandleGuard(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeFileHandles := env.Model.ActiveFileHandles()
	activeDirHandles := env.Model.ActiveDirHandles()
	otherSID := nextIsolatedFormalSessionID(env)

	if len(activeFileHandles) > 0 {
		h := activeFileHandles[env.Rng.IntN(len(activeFileHandles))]
		var badSID *uint64
		var tag string
		if h.SessionID == nil {
			badSID = &otherSID
			tag = "无会话文件句柄传入非空SessionID"
		} else if env.Rng.IntN(2) == 0 {
			badSID = nil
			tag = "有会话文件句柄传入nil-SessionID"
		} else {
			badSID = &otherSID
			tag = "有会话文件句柄传入异构SessionID"
		}

		require.ErrorIs(t, env.Mgr.Flush(ctx, meta.FlushParams{
			Ino: h.Ino, Fh: h.Fh, SessionID: badSID,
		}), meta.ErrBadFileHandle)
		require.ErrorIs(t, env.Mgr.Fsync(ctx, meta.FsyncParams{
			Ino: h.Ino, Fh: h.Fh, Datasync: true, SessionID: badSID,
		}), meta.ErrBadFileHandle)
		fhCopy := h.Fh
		uid := uint32(1000)
		_, err := env.Mgr.SetAttr(ctx, h.Ino, meta.SetAttrInput{
			UID: &uid, Fh: &fhCopy, SessionID: badSID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		require.ErrorIs(t, env.Mgr.Release(ctx, meta.ReleaseParams{
			Ino: h.Ino, Fh: h.Fh, SessionID: badSID,
		}), meta.ErrBadFileHandle)

		return fmt.Sprintf("CrossSessionGuard[%s](ino=%d, fh=%d, orig_sid=%s, req_sid=%s) -> 全部预期拒绝(ErrBadFileHandle)",
			tag, h.Ino, h.Fh, modelFormatSessionID(h.SessionID), modelFormatSessionID(badSID))
	}

	if len(activeDirHandles) > 0 {
		dh := activeDirHandles[env.Rng.IntN(len(activeDirHandles))]
		var badSID *uint64
		if dh.SessionID == nil {
			badSID = &otherSID
		}
		require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino: dh.Ino, Fh: dh.Fh, SessionID: badSID,
		}), meta.ErrBadFileHandle)
		_, err := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino: dh.Ino, Fh: dh.Fh, Offset: 0, Limit: 10, SessionID: badSID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino: dh.Ino, Fh: dh.Fh, Offset: 0, Limit: 10, SessionID: badSID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		require.ErrorIs(t, env.Mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: dh.Ino, Fh: dh.Fh, SessionID: badSID,
		}), meta.ErrBadFileHandle)

		return fmt.Sprintf("CrossSessionDirGuard(ino=%d, fh=%d, orig_sid=%s, req_sid=%s) -> 全部预期拒绝(ErrBadFileHandle)",
			dh.Ino, dh.Fh, modelFormatSessionID(dh.SessionID), modelFormatSessionID(badSID))
	}

	// 若当前尚无活跃句柄，正常注册一个新会话
	expAt, err := env.Mgr.RenewSession(ctx, otherSID)
	require.NoError(t, err)
	modelSyncSessionRenewal(env.Model, &otherSID, env.LeaseTTL)
	return fmt.Sprintf("RenewSession[初始建会话](sid=%d, expires_at=%s) -> 成功", otherSID, expAt.Format(time.RFC3339Nano))
}

// enrichDiskBeforeDelete 在执行 DeleteDisk 前为目标磁盘补充嵌套目录、多硬链接常规文件、活跃孤儿节点、TmpFile、打开句柄与文件锁，
// 确保随机状态机中的每一次合法 DeleteDisk 都真正验证复杂非空拓扑与活跃句柄/孤儿/锁的单事务强制级联销毁。
func enrichDiskBeforeDelete(ctx context.Context, t *testing.T, env *formalMainEnv, disk *formalDisk) {
	t.Helper()
	rootDir, ok := env.Model.Inodes[disk.RootIno]
	if !ok || rootDir.Deleted {
		return
	}

	// 1. 创建子目录与打开目录句柄
	subDirName := env.UniqueEntryName("del_subdir")
	subDir, err := env.Mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte(subDirName),
		Mode:      0o755,
	})
	require.NoError(t, err, "enrichDiskBeforeDelete: 创建子目录必须成功")
	rootDir.Nlink++
	disk.UsedInodes++
	subDirModel := &formalInode{
		Ino: subDir.Ino, Generation: subDir.Generation, RootIno: disk.RootIno, ParentIno: disk.RootIno,
		Mode: subDir.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
	}
	env.Model.RecordNewInode(subDirModel)
	env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: subDirName}] = subDirModel.Ino

	// 打开子目录句柄（不关闭，测试 DeleteDisk 级联清除目录句柄）
	odRes, err := env.Mgr.OpenDir(ctx, meta.OpenDirParams{Ino: subDirModel.Ino, Flags: meta.O_RDONLY})
	require.NoError(t, err, "enrichDiskBeforeDelete: 打开子目录句柄必须成功")
	env.Model.RecordNewHandle(&formalHandle{
		Fh: odRes.Fh, Ino: subDirModel.Ino, Flags: meta.O_RDONLY, IsDir: true,
	})

	// 2. 在子目录下创建常规文件并建立跨目录硬链接
	fName := env.UniqueEntryName("del_file")
	cRes, err := env.Mgr.Create(ctx, meta.CreateParams{
		ParentIno: subDirModel.Ino,
		Name:      []byte(fName),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	})
	require.NoError(t, err, "enrichDiskBeforeDelete: 创建常规文件必须成功")
	fModel := &formalInode{
		Ino: cRes.Inode.Ino, Generation: cRes.Inode.Generation, RootIno: disk.RootIno,
		Mode: cRes.Inode.Attr.Mode, Size: 0, Nlink: 1,
	}
	env.Model.RecordNewInode(fModel)
	env.Model.Entries[formalEntryKey{ParentIno: subDirModel.Ino, Name: fName}] = fModel.Ino
	disk.UsedInodes++
	env.Model.RecordNewHandle(&formalHandle{
		Fh: cRes.Fh, Ino: fModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR, IsDir: false,
	})

	// 建立跨目录硬链接
	linkName := env.UniqueEntryName("del_file_link")
	_, err = env.Mgr.Link(ctx, meta.LinkParams{
		Ino: fModel.Ino, NewParentIno: disk.RootIno, NewName: []byte(linkName),
	})
	require.NoError(t, err, "enrichDiskBeforeDelete: 建立跨目录硬链接必须成功")
	fModel.Nlink++
	env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: linkName}] = fModel.Ino

	// 3. 构造活跃孤儿常规文件 (unlink-whilst-open)
	orphanName := env.UniqueEntryName("del_orphan")
	oRes, err := env.Mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte(orphanName),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	})
	require.NoError(t, err, "enrichDiskBeforeDelete: 创建待解链常规文件必须成功")
	oModel := &formalInode{
		Ino: oRes.Inode.Ino, Generation: oRes.Inode.Generation, RootIno: disk.RootIno,
		Mode: oRes.Inode.Attr.Mode, Size: 0, Nlink: 1,
	}
	env.Model.RecordNewInode(oModel)
	env.Model.Entries[formalEntryKey{ParentIno: disk.RootIno, Name: orphanName}] = oModel.Ino
	disk.UsedInodes++
	env.Model.RecordNewHandle(&formalHandle{
		Fh: oRes.Fh, Ino: oModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR, IsDir: false,
	})
	err = env.Mgr.Unlink(ctx, disk.RootIno, []byte(orphanName))
	require.NoError(t, err, "enrichDiskBeforeDelete: Unlink 孤儿文件必须成功")
	delete(env.Model.Entries, formalEntryKey{ParentIno: disk.RootIno, Name: orphanName})
	oModel.Nlink = 0

	// 4. 创建一个匿名 TmpFile (nlink == 0) 留在目标盘上
	tmpRes, err := env.Mgr.TmpFile(ctx, meta.TmpFileParams{
		ParentIno: subDirModel.Ino,
		Mode:      0o600,
		Flags:     meta.O_RDWR | meta.O_TMPFILE,
	})
	require.NoError(t, err, "enrichDiskBeforeDelete: 创建 TmpFile 必须成功")
	tmpModel := &formalInode{
		Ino: tmpRes.Inode.Ino, Generation: tmpRes.Inode.Generation, RootIno: disk.RootIno,
		Mode: tmpRes.Inode.Attr.Mode, Size: 0, Nlink: 0, IsTmpFile: true,
	}
	env.Model.RecordNewInode(tmpModel)
	disk.UsedInodes++
	env.Model.RecordNewHandle(&formalHandle{
		Fh: tmpRes.Fh, Ino: tmpModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE, IsDir: false,
	})
}

// formalActionDeleteDisk 在状态机中随机生成：
// 1. 合法强制级联销毁整盘（确保始终至少保留 1 块存活 Disk 供后续步骤继续操作，并核验存活邻盘的账本与拓扑毫发无损）；
// 2. 不存在的合法 UUID 或已删除 DiskID 重复调用 DeleteDisk -> 返回 ErrNotFound；
// 3. 空字符串、全零 Nil UUID、或非法 UUID 字符串 -> 返回 ErrInvalidArgument。
func formalActionDeleteDisk(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	branch := env.Rng.IntN(100)
	var diskID string
	var caseTag string

	switch {
	case branch < 45:
		// 重点场景 1：合法强制级联删盘（若当前存活磁盘不足 2 块，先新建 1 块磁盘以保证删盘后至少保留 1 块存活磁盘）
		if len(env.Model.ActiveDisks()) < 2 {
			_ = formalCreateValidDiskStep(ctx, t, env)
		}
		activeDisks := env.Model.ActiveDisks()
		victim := activeDisks[env.Rng.IntN(len(activeDisks))]
		enrichDiskBeforeDelete(ctx, t, env, victim)
		diskID = victim.DiskID
		caseTag = "合法强制级联删盘(含嵌套目录树/硬链接/孤儿/TmpFile/打开句柄)"

	case branch < 72:
		// 重点场景 2：已删除磁盘重复调用 DeleteDisk 或不存在的随机 UUID -> ErrNotFound
		if len(env.Model.DeletedDiskIDs) > 0 && env.Rng.IntN(2) == 0 {
			for _, id := range env.Model.AllCreatedDiskIDs {
				if env.Model.DeletedDiskIDs[id] {
					diskID = id
					break
				}
			}
			caseTag = "重复删除已销毁磁盘(ErrNotFound)"
		} else {
			u, err := uuid.NewV7()
			require.NoError(t, err)
			diskID = u.String()
			caseTag = "不存在的随机UUID(ErrNotFound)"
		}

	default:
		// 重点场景 3：空字符串、全零 Nil UUID 或格式非法 UUID -> ErrInvalidArgument
		switch env.Rng.IntN(4) {
		case 0:
			diskID = ""
			caseTag = "空字符串DiskID(ErrInvalidArgument)"
		case 1:
			diskID = "00000000-0000-0000-0000-000000000000"
			caseTag = "全零Nil-UUID(ErrInvalidArgument)"
		case 2:
			diskID = "invalid-disk-uuid-format"
			caseTag = "非法格式UUID(ErrInvalidArgument)"
		default:
			diskID = "12345678-1234-1234-1234-1234567890zz"
			caseTag = "含非法十六进制字符UUID(ErrInvalidArgument)"
		}
	}

	targetDisk, expectedErr := formalPredictDeleteDisk(env.Model, diskID)
	err := env.Mgr.DeleteDisk(ctx, diskID)

	if expectedErr != nil {
		require.Errorf(t, err, "DeleteDisk[%s] (disk_id=%q) 预期错误 %v，但实际成功", caseTag, diskID, expectedErr)
		require.Truef(t, errors.Is(err, expectedErr),
			"DeleteDisk[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		return fmt.Sprintf("DeleteDisk[%s](disk_id=%q) -> 预期拒绝 (%v)", caseTag, diskID, expectedErr)
	}

	require.NoErrorf(t, err, "DeleteDisk[%s] (disk_id=%q) 预期成功，但返回错误: %v", caseTag, diskID, err)
	require.NotNil(t, targetDisk)

	deletedRootIno := targetDisk.RootIno
	deletedUsedInodes := targetDisk.UsedInodes
	deletedUsedBytes := targetDisk.UsedBytes

	// 同步在内存参考模型中级联清理该盘的 Disk、Inodes、Entries 与 Handles
	env.Model.RecordDeletedDisk(diskID)

	// 保底至少存在 1 块存活磁盘供后续步骤操作
	if len(env.Model.ActiveDisks()) == 0 {
		_ = formalCreateValidDiskStep(ctx, t, env)
	}

	return fmt.Sprintf("DeleteDisk[%s](disk_id=%s, root_ino=%d, 销毁前used_inodes=%d, used_bytes=%d) -> 成功 (底表五表级联清零)",
		caseTag, diskID, deletedRootIno, deletedUsedInodes, deletedUsedBytes)
}

// ============================================================================
// 3. T10 底表数学不变量审计：INV-3 (会话清扫后零无引用孤儿) & INV-4 (删盘后五表零残留)
// ============================================================================

// formalAuditT10SessionAndDeleteDisk 直接扫描 PostgreSQL 7 张核心底表，强制审计：
// 1. INV-4 级联删盘底表零残留：
//   - 对已执行 DeleteDisk 的所有 deletedDiskIDs 与 deletedRootInos，底表 disks、inodes、entries、file_handles、file_locks、file_lock_waiters 中归属该盘的记录数全部恒为 0；
//   - 对内存中已标记物理删除的所有 DeletedInos（含删盘级联销毁的节点与回收的孤儿节点），底表 inodes、entries、file_handles、file_locks、file_lock_waiters、inode_xattrs 中记录数全部恒为 0；
//
// 2. INV-3 会话租约与孤儿清扫收敛：
//   - 对已通过 RenewSession 清扫的过期会话，底表 client_sessions、file_handles、file_locks、file_lock_waiters 中归属该 session_id 的记录数全部恒为 0；
//   - 当不存在未清扫的过期会话时，底表中无有效句柄 (NOT EXISTS (fh.session_id IS NULL OR s.expires_at > clock_timestamp())) 的 nlink == 0 孤儿节点数恒为 0！
func formalAuditT10SessionAndDeleteDisk(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	// 1. 审计已删除 Disk (deletedDiskIDs & deletedRootInos) 在底表中的零残留
	if len(env.Model.DeletedDiskIDs) > 0 {
		delDiskIDs := make([]string, 0, len(env.Model.DeletedDiskIDs))
		for _, id := range env.Model.SortedDiskIDs() {
			if env.Model.DeletedDiskIDs[id] {
				delDiskIDs = append(delDiskIDs, id)
			}
		}
		var diskCount int64
		err := env.Pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM disks WHERE disk_id::text = ANY($1)
		`, delDiskIDs).Scan(&diskCount)
		require.NoError(t, err, "T10 INV-4 审计: 查询已删除 disk_id 失败")
		require.Zerof(t, diskCount, "INV-4 违例: 已删除磁盘在 disks 底表中仍残留 %d 条记录", diskCount)
	}

	if len(env.Model.DeletedRootInos) > 0 {
		delRoots := make([]int64, 0, len(env.Model.DeletedRootInos))
		for _, r := range env.Model.AllCreatedRootInos {
			if env.Model.DeletedRootInos[r] {
				delRoots = append(delRoots, int64(r))
			}
		}
		var diskCnt, inodeCnt int64
		err := env.Pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM disks WHERE root_ino = ANY($1)),
				(SELECT COUNT(*) FROM inodes WHERE root_ino = ANY($1))
		`, delRoots).Scan(&diskCnt, &inodeCnt)
		require.NoError(t, err, "T10 INV-4 审计: 查询 deletedRootInos 底表残留失败")
		require.Zerof(t, diskCnt, "INV-4 违例: deletedRootInos 在 disks 表残留 %d 条", diskCnt)
		require.Zerof(t, inodeCnt, "INV-4 违例: deletedRootInos 在 inodes 表残留 %d 条", inodeCnt)
	}

	// 2. 审计所有已物理删除 Inode (DeletedInos，含删盘销毁的全部节点与已回收的全部孤儿节点) 在各关联子表中的零残留
	if len(env.Model.DeletedInos) > 0 {
		sortedDel := env.Model.SortedDeletedInos()
		delInos := make([]int64, 0, len(sortedDel))
		for _, ino := range sortedDel {
			delInos = append(delInos, int64(ino))
		}
		var inodeCnt, entryCnt, handleCnt, lockCnt, waiterCnt, xattrCnt int64
		err := env.Pool.QueryRow(ctx, `
			SELECT
				(SELECT COUNT(*) FROM inodes WHERE ino = ANY($1)),
				(SELECT COUNT(*) FROM entries WHERE parent_ino = ANY($1) OR ino = ANY($1)),
				(SELECT COUNT(*) FROM file_handles WHERE ino = ANY($1)),
				(SELECT COUNT(*) FROM file_locks WHERE ino = ANY($1)),
				(SELECT COUNT(*) FROM file_lock_waiters WHERE ino = ANY($1)),
				(SELECT COUNT(*) FROM inode_xattrs WHERE ino = ANY($1))
		`, delInos).Scan(&inodeCnt, &entryCnt, &handleCnt, &lockCnt, &waiterCnt, &xattrCnt)
		require.NoError(t, err, "T10 审计: 查询 DeletedInos 子表级联残留失败")
		require.Zerof(t, inodeCnt, "INV-3/INV-4 违例: DeletedInos 在 inodes 表残留 %d 条", inodeCnt)
		require.Zerof(t, entryCnt, "INV-3/INV-4 违例: DeletedInos 在 entries 表残留 %d 条", entryCnt)
		require.Zerof(t, handleCnt, "INV-3/INV-4 违例: DeletedInos 在 file_handles 表残留 %d 条", handleCnt)
		require.Zerof(t, lockCnt, "INV-3/INV-4 违例: DeletedInos 在 file_locks 表残留 %d 条", lockCnt)
		require.Zerof(t, waiterCnt, "INV-3/INV-4 违例: DeletedInos 在 file_lock_waiters 表残留 %d 条", waiterCnt)
		require.Zerof(t, xattrCnt, "INV-3/INV-4 违例: DeletedInos 在 inode_xattrs 表残留 %d 条", xattrCnt)
	}

	// 3. 审计本轮创建的所有客户端会话 (AllSessionIDs)：
	//    - 已被 RenewSession 清理的过期会话在 client_sessions、file_handles、file_locks、file_lock_waiters 中记录数必须恒为 0；
	//    - 当前活跃会话在 client_sessions 中必须存在且 expires_at > clock_timestamp()。
	for _, sid := range env.Model.AllSessionIDs {
		sess, exists := env.Model.Sessions[sid]
		if !exists {
			// 已过期并被 RenewSession 清扫的会话
			var sessCnt, handleCnt, lockCnt, waiterCnt int64
			err := env.Pool.QueryRow(ctx, `
				SELECT
					(SELECT COUNT(*) FROM client_sessions WHERE session_id = $1),
					(SELECT COUNT(*) FROM file_handles WHERE session_id = $1),
					(SELECT COUNT(*) FROM file_locks WHERE session_id = $1),
					(SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = $1)
			`, int64(sid)).Scan(&sessCnt, &handleCnt, &lockCnt, &waiterCnt)
			require.NoError(t, err, "T10 INV-3 审计: 查询已清扫过期会话残留失败")
			require.Zerof(t, sessCnt, "INV-3 违例: 已清扫过期会话 sid=%d 在 client_sessions 中残留 %d 条", sid, sessCnt)
			require.Zerof(t, handleCnt, "INV-3 违例: 已清扫过期会话 sid=%d 在 file_handles 中残留 %d 条", sid, handleCnt)
			require.Zerof(t, lockCnt, "INV-3 违例: 已清扫过期会话 sid=%d 在 file_locks 中残留 %d 条", sid, lockCnt)
			require.Zerof(t, waiterCnt, "INV-3 违例: 已清扫过期会话 sid=%d 在 file_lock_waiters 中残留 %d 条", sid, waiterCnt)
		} else if !sess.Expired {
			var isAlive bool
			err := env.Pool.QueryRow(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM client_sessions WHERE session_id = $1 AND expires_at > clock_timestamp()
				)
			`, int64(sid)).Scan(&isAlive)
			require.NoError(t, err, "T10 INV-3 审计: 查询活跃会话状态失败")
			require.Truef(t, isAlive, "INV-3 违例: 内存模型中的活跃会话 sid=%d 在底表 client_sessions 中不存在或已过期", sid)
		}
	}

	// 4. 审计 INV-3 核心收敛条件：当无未清扫的过期会话时，当前测试所有存活 Disk 下无有效句柄引用的 nlink == 0 孤儿节点数恒为 0！
	hasUnsweptExpired := false
	for _, sid := range env.Model.AllSessionIDs {
		if s, ok := env.Model.Sessions[sid]; ok && s.Expired {
			hasUnsweptExpired = true
			break
		}
	}
	if !hasUnsweptExpired && len(env.Model.AllCreatedRootInos) > 0 {
		rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
		for _, r := range env.Model.AllCreatedRootInos {
			rootInos = append(rootInos, int64(r))
		}
		var unreferencedOrphanCount int64
		err := env.Pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM inodes i
			WHERE i.root_ino = ANY($1)
			  AND i.nlink = 0
			  AND NOT EXISTS (
			      SELECT 1
			      FROM file_handles fh
			      LEFT JOIN client_sessions s ON fh.session_id = s.session_id
			      WHERE fh.ino = i.ino
			        AND (fh.session_id IS NULL OR s.expires_at > clock_timestamp())
			  )
		`, rootInos).Scan(&unreferencedOrphanCount)
		require.NoError(t, err, "T10 INV-3 审计: 查询无有效句柄的孤儿节点数失败")
		require.Zerof(t, unreferencedOrphanCount,
			"INV-3 违例: RenewSession 同步清扫后，底表中仍残留 %d 个无有效句柄引用的 nlink=0 孤儿节点", unreferencedOrphanCount)
	}
}

// ============================================================================
// 4. T10 专属测试入口：确定性短租约/删盘全矩阵测试 + 全量 19 写接口状态机回归
// ============================================================================

// TestFormalMain_T10_SessionAndDeleteDisk 验证 T10 主状态机 RenewSession 短租约过期回收、
// 失效句柄 EBADF 防诈尸、混合句柄孤儿保留与回收、DeleteDisk 强制级联销毁整盘以及底表 INV-1 ~ INV-6 零残留收敛。
func TestFormalMain_T10_SessionAndDeleteDisk(t *testing.T) {
	t.Run("确定性边界_WithSessionLeaseTTL短租约过期_全句柄接口EBADF防诈尸_RenewSession级联清理句柄与锁_混合句柄孤儿保留与四类无引用孤儿物理回收配额退还_INV-3与INV-4闭环", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)

		// 配置毫秒级短租约 (20ms) 的 shortMgr 用于触发会话快速超期，并用标准 mgr 保障长耗时全量探针稳定执行
		const leaseTTL = 20 * time.Millisecond
		shortMgr := meta.New(store, meta.WithSessionLeaseTTL(leaseTTL))
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 60,
			Seed:       0x20260427,
			Rng:        newFormalRNG(0x20260427),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t10-sess-det",
			LeaseTTL:   leaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		runAllProbesAndAuditors := func() {
			formalProbeGetDisk(ctx, t, env)
			formalProbeStatFS(ctx, t, env)
			formalProbeLookup(ctx, t, env)
			formalProbeGetAttr(ctx, t, env)
			formalProbeReadlink(ctx, t, env)
			formalProbeFlush(ctx, t, env)
			formalProbeFsync(ctx, t, env)
			formalProbeReadDir(ctx, t, env)
			formalProbeReadDirPlus(ctx, t, env)
			formalProbeFsyncDir(ctx, t, env)
			formalAuditINV1DirTopology(ctx, t, env)
			formalAuditINV2DirNlink(ctx, t, env)
			formalAuditINV2NonDirNlink(ctx, t, env)
			formalAuditINV3OrphanAndHandles(ctx, t, env)
			formalAuditINV4DiskQuota(ctx, t, env)
			formalAuditNodeAndSymlinkQuota(ctx, t, env)
			formalAuditINV5CrossDiskIsolation(ctx, t, env)
			formalAuditINV6TmpFileMaterialization(ctx, t, env)
			formalAuditT10SessionAndDeleteDisk(ctx, t, env)
		}

		// 1. 验证 RenewSession 与 RenewSessionWithStatus 的非法入参边界 (0 与 > math.MaxInt64)
		for _, badSID := range []uint64{0, uint64(math.MaxInt64) + 1, ^uint64(0)} {
			_, err := shortMgr.RenewSession(ctx, badSID)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
			_, err = shortMgr.RenewSessionWithStatus(ctx, badSID)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		}

		// 2. 创建初始虚拟磁盘 disk1，注册两个客户端会话 sidExp (待过期) 与 sidKeep (保持存活)
		formalCreateValidDiskStep(ctx, t, env)
		disk1 := env.Model.ActiveDisks()[0]
		root1 := disk1.RootIno
		rootDir1 := env.Model.Inodes[root1]

		sidExp := nextIsolatedFormalSessionID(env)
		sidKeep := nextIsolatedFormalSessionID(env)

		resExpInit, err := shortMgr.RenewSessionWithStatus(ctx, sidExp)
		require.NoError(t, err)
		require.True(t, resExpInit.SessionRecreated, "首次创建 sidExp 时 SessionRecreated 必须为 true")
		env.Model.RecordSession(sidExp, resExpInit.ExpiresAt)

		resExpRenew, err := shortMgr.RenewSessionWithStatus(ctx, sidExp)
		require.NoError(t, err)
		require.False(t, resExpRenew.SessionRecreated, "未过期续期 sidExp 时 SessionRecreated 必须为 false")
		env.Model.RecordSession(sidExp, resExpRenew.ExpiresAt)

		resKeepInit, err := mgr.RenewSessionWithStatus(ctx, sidKeep)
		require.NoError(t, err)
		require.True(t, resKeepInit.SessionRecreated)
		env.Model.RecordSession(sidKeep, resKeepInit.ExpiresAt)

		// 3. 构造 6 个孤儿节点与文件锁/等待队列，全面覆盖四类无引用孤儿回收与两类混合句柄孤儿保留：
		//    - Orphan 1: unlink-whilst-open 常规文件 (S_IFREG, size=4096)，仅绑定 sidExp 句柄 + POSIX 锁 + BSD flock + waiter；
		//    - Orphan 2: unlink-whilst-open 特殊节点 (S_IFIFO, size=0)，仅绑定 sidExp 句柄；
		//    - Orphan 3: rmdir-whilst-open 孤儿目录 (S_IFDIR, nlink=0)，仅绑定 sidExp 目录句柄；
		//    - Orphan 4: 未具名化 TmpFile (S_IFREG, nlink=0, size=8192)，仅绑定 sidExp 句柄；
		//    - Orphan 5 (混合句柄 A): unlink-whilst-open 常规文件 (S_IFREG, size=2048)，同时绑定 sidExp 句柄 + session_id==nil 无会话句柄；
		//    - Orphan 6 (混合句柄 B): unlink-whilst-open 常规文件 (S_IFREG, size=1024)，同时绑定 sidExp 句柄 + sidKeep 存活会话句柄。
		var expFileHandles []*formalHandle
		var expDirHandles []*formalHandle

		// --- Orphan 1: unlink-whilst-open 常规文件 (size=4096, 仅 sidExp) ---
		c1, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: root1, Name: []byte("orphan_reg_only_exp"), Mode: meta.S_IFREG | 0o644,
			Flags: meta.O_CREAT | meta.O_RDWR, SessionID: &sidExp,
		})
		require.NoError(t, err)
		sz1 := uint64(4096)
		_, err = mgr.SetAttr(ctx, c1.Inode.Ino, meta.SetAttrInput{Size: &sz1, Fh: &c1.Fh, SessionID: &sidExp})
		require.NoError(t, err)
		orphan1Model := &formalInode{
			Ino: c1.Inode.Ino, Generation: c1.Inode.Generation, RootIno: root1,
			Mode: c1.Inode.Attr.Mode, Size: sz1, Nlink: 1,
		}
		env.Model.RecordNewInode(orphan1Model)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_reg_only_exp"}] = orphan1Model.Ino
		disk1.UsedInodes++
		disk1.UsedBytes += sz1
		h1 := &formalHandle{Fh: c1.Fh, Ino: orphan1Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: modelCloneSessionID(&sidExp)}
		env.Model.RecordNewHandle(h1)
		expFileHandles = append(expFileHandles, h1)

		// 在 Orphan 1 上为 sidExp 加上 POSIX 写锁与 BSD flock 写锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino: orphan1Model.Ino, Fh: c1.Fh, LockOwner: 101, SessionID: sidExp, LkFlags: 0,
			Lk: meta.FileLock{Start: 0, End: 100, Type: meta.F_WRLCK, PID: 11},
		}))
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino: orphan1Model.Ino, Fh: c1.Fh, LockOwner: 101, SessionID: sidExp, LkFlags: meta.FuseLkFlock,
			Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_WRLCK, PID: 11},
		}))

		require.NoError(t, mgr.Unlink(ctx, root1, []byte("orphan_reg_only_exp")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_reg_only_exp"})
		orphan1Model.Nlink = 0

		// --- Orphan 2: unlink-whilst-open 特殊节点 S_IFIFO (size=0, 仅 sidExp) ---
		fifoNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: root1, Name: []byte("orphan_fifo_only_exp"), Mode: meta.S_IFIFO | 0o640,
		})
		require.NoError(t, err)
		orphan2Model := &formalInode{
			Ino: fifoNode.Ino, Generation: fifoNode.Generation, RootIno: root1,
			Mode: fifoNode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(orphan2Model)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_fifo_only_exp"}] = orphan2Model.Ino
		disk1.UsedInodes++
		openFifo, err := mgr.Open(ctx, meta.OpenParams{Ino: orphan2Model.Ino, Flags: meta.O_RDWR, SessionID: &sidExp})
		require.NoError(t, err)
		h2 := &formalHandle{Fh: openFifo.Fh, Ino: orphan2Model.Ino, Flags: meta.O_RDWR, SessionID: modelCloneSessionID(&sidExp)}
		env.Model.RecordNewHandle(h2)
		expFileHandles = append(expFileHandles, h2)
		require.NoError(t, mgr.Unlink(ctx, root1, []byte("orphan_fifo_only_exp")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_fifo_only_exp"})
		orphan2Model.Nlink = 0

		// --- Orphan 3: rmdir-whilst-open 孤儿目录 (仅 sidExp) ---
		dirNode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: root1, Name: []byte("orphan_dir_only_exp"), Mode: 0o755,
		})
		require.NoError(t, err)
		rootDir1.Nlink++
		disk1.UsedInodes++
		orphan3Model := &formalInode{
			Ino: dirNode.Ino, Generation: dirNode.Generation, RootIno: root1, ParentIno: root1,
			Mode: dirNode.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(orphan3Model)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_dir_only_exp"}] = orphan3Model.Ino
		openDir3, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: orphan3Model.Ino, Flags: meta.O_RDONLY, SessionID: &sidExp})
		require.NoError(t, err)
		dh3 := &formalHandle{Fh: openDir3.Fh, Ino: orphan3Model.Ino, Flags: meta.O_RDONLY, SessionID: modelCloneSessionID(&sidExp), IsDir: true}
		env.Model.RecordNewHandle(dh3)
		expDirHandles = append(expDirHandles, dh3)
		require.NoError(t, mgr.Rmdir(ctx, root1, []byte("orphan_dir_only_exp")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_dir_only_exp"})
		rootDir1.Nlink--
		orphan3Model.Nlink = 0

		// --- Orphan 4: 未具名化 TmpFile (size=8192, 仅 sidExp) ---
		tmp4, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: root1, Mode: 0o600, Flags: meta.O_RDWR | meta.O_TMPFILE, SessionID: &sidExp,
		})
		require.NoError(t, err)
		sz4 := uint64(8192)
		_, err = mgr.SetAttr(ctx, tmp4.Inode.Ino, meta.SetAttrInput{Size: &sz4, Fh: &tmp4.Fh, SessionID: &sidExp})
		require.NoError(t, err)
		orphan4Model := &formalInode{
			Ino: tmp4.Inode.Ino, Generation: tmp4.Inode.Generation, RootIno: root1,
			Mode: tmp4.Inode.Attr.Mode, Size: sz4, Nlink: 0, IsTmpFile: true,
		}
		env.Model.RecordNewInode(orphan4Model)
		disk1.UsedInodes++
		disk1.UsedBytes += sz4
		h4 := &formalHandle{Fh: tmp4.Fh, Ino: orphan4Model.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE, SessionID: modelCloneSessionID(&sidExp)}
		env.Model.RecordNewHandle(h4)
		expFileHandles = append(expFileHandles, h4)

		// --- Orphan 5 (混合句柄 A): 同时绑定 sidExp 句柄 + session_id==nil 无会话句柄 (size=2048) ---
		c5, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: root1, Name: []byte("orphan_mix_nil"), Mode: meta.S_IFREG | 0o644,
			Flags: meta.O_CREAT | meta.O_RDWR, SessionID: &sidExp,
		})
		require.NoError(t, err)
		sz5 := uint64(2048)
		_, err = mgr.SetAttr(ctx, c5.Inode.Ino, meta.SetAttrInput{Size: &sz5, Fh: &c5.Fh, SessionID: &sidExp})
		require.NoError(t, err)
		orphan5Model := &formalInode{
			Ino: c5.Inode.Ino, Generation: c5.Inode.Generation, RootIno: root1,
			Mode: c5.Inode.Attr.Mode, Size: sz5, Nlink: 1,
		}
		env.Model.RecordNewInode(orphan5Model)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_mix_nil"}] = orphan5Model.Ino
		disk1.UsedInodes++
		disk1.UsedBytes += sz5
		h5Exp := &formalHandle{Fh: c5.Fh, Ino: orphan5Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: modelCloneSessionID(&sidExp)}
		env.Model.RecordNewHandle(h5Exp)
		expFileHandles = append(expFileHandles, h5Exp)

		open5Nil, err := mgr.Open(ctx, meta.OpenParams{Ino: orphan5Model.Ino, Flags: meta.O_RDWR, SessionID: nil})
		require.NoError(t, err)
		h5Nil := &formalHandle{Fh: open5Nil.Fh, Ino: orphan5Model.Ino, Flags: meta.O_RDWR, SessionID: nil}
		env.Model.RecordNewHandle(h5Nil)

		require.NoError(t, mgr.Unlink(ctx, root1, []byte("orphan_mix_nil")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_mix_nil"})
		orphan5Model.Nlink = 0

		// --- Orphan 6 (混合句柄 B): 同时绑定 sidExp 句柄 + sidKeep 存活会话句柄 (size=1024) ---
		c6, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: root1, Name: []byte("orphan_mix_keep"), Mode: meta.S_IFREG | 0o644,
			Flags: meta.O_CREAT | meta.O_RDWR, SessionID: &sidExp,
		})
		require.NoError(t, err)
		sz6 := uint64(1024)
		_, err = mgr.SetAttr(ctx, c6.Inode.Ino, meta.SetAttrInput{Size: &sz6, Fh: &c6.Fh, SessionID: &sidExp})
		require.NoError(t, err)
		orphan6Model := &formalInode{
			Ino: c6.Inode.Ino, Generation: c6.Inode.Generation, RootIno: root1,
			Mode: c6.Inode.Attr.Mode, Size: sz6, Nlink: 1,
		}
		env.Model.RecordNewInode(orphan6Model)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_mix_keep"}] = orphan6Model.Ino
		disk1.UsedInodes++
		disk1.UsedBytes += sz6
		h6Exp := &formalHandle{Fh: c6.Fh, Ino: orphan6Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: modelCloneSessionID(&sidExp)}
		env.Model.RecordNewHandle(h6Exp)
		expFileHandles = append(expFileHandles, h6Exp)

		open6Keep, err := mgr.Open(ctx, meta.OpenParams{Ino: orphan6Model.Ino, Flags: meta.O_RDWR, SessionID: &sidKeep})
		require.NoError(t, err)
		h6Keep := &formalHandle{Fh: open6Keep.Fh, Ino: orphan6Model.Ino, Flags: meta.O_RDWR, SessionID: modelCloneSessionID(&sidKeep)}
		env.Model.RecordNewHandle(h6Keep)

		// 在 Orphan 6 上由 sidKeep 持有 [200, 300] 写锁，供后续 sidExp 通过公开接口 SetLkw 产生真实冲突入队等待记录
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino: orphan6Model.Ino, Fh: open6Keep.Fh, LockOwner: 601, SessionID: sidKeep, LkFlags: 0,
			Lk: meta.FileLock{Start: 200, End: 300, Type: meta.F_WRLCK, PID: 61},
		}))

		require.NoError(t, mgr.Unlink(ctx, root1, []byte("orphan_mix_keep")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_mix_keep"})
		orphan6Model.Nlink = 0

		// 此时 disk1 上共有 1(root) + 6(orphans) = 7 个 Inode，已用字节 4096 + 8192 + 2048 + 1024 = 15360
		require.Equal(t, uint64(7), disk1.UsedInodes)
		require.Equal(t, uint64(15360), disk1.UsedBytes)
		runAllProbesAndAuditors()

		// 4. 验证跨会话访问拒绝 (ErrBadFileHandle)：
		//    - 用 sidKeep 访问 sidExp 的句柄 h1 -> ErrBadFileHandle
		//    - 用 sidExp 访问无会话句柄 h5Nil -> ErrBadFileHandle
		//    - 用 nil 访问 sidKeep 的句柄 h6Keep -> ErrBadFileHandle
		require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: h1.Ino, Fh: h1.Fh, SessionID: &sidKeep}), meta.ErrBadFileHandle)
		require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: h5Nil.Ino, Fh: h5Nil.Fh, SessionID: &sidExp}), meta.ErrBadFileHandle)
		require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: h6Keep.Ino, Fh: h6Keep.Fh, SessionID: nil}), meta.ErrBadFileHandle)
		require.ErrorIs(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: dh3.Ino, Fh: dh3.Fh, SessionID: &sidKeep}), meta.ErrBadFileHandle)

		// 5. 通过公开接口 SetLkw (搭配 1ms 租约的 tinyMgr) 让 sidExp 在冲突写锁上真实入队 file_lock_waiters 并在首次轮询间隔内自然超期
		tinyMgr := meta.New(store, meta.WithSessionLeaseTTL(1*time.Millisecond))
		tinyCtx, tinyCancel := context.WithTimeout(ctx, 2*time.Second)
		defer tinyCancel()
		err = tinyMgr.SetLkw(tinyCtx, meta.SetLkwParams{
			Ino: orphan6Model.Ino, Fh: c6.Fh, LockOwner: 102, SessionID: sidExp, RequestID: 88001, LkFlags: 0,
			Lk: meta.FileLock{Start: 200, End: 300, Type: meta.F_WRLCK, PID: 12},
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "sidExp 在阻塞等待期间租约过期应返回 ErrBadFileHandle")
		var pendingWaiterCnt int64
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_lock_waiters WHERE session_id = $1 AND request_id = 88001`, int64(sidExp)).Scan(&pendingWaiterCnt))
		require.Equal(t, int64(1), pendingWaiterCnt, "公开接口 SetLkw 必须在 file_lock_waiters 中留下待级联清扫的等待记录")
		env.Model.Sessions[sidExp].Expired = true

		// 6. 验证过期会话 sidExp 的全部文件句柄与目录句柄调用 9 个句柄接口均返回 ErrBadFileHandle，且无法“诈尸”自救
		verifyExpiredHandleAntiResurrection(ctx, t, shortMgr, sidExp, expFileHandles, expDirHandles)
		assertSessionStillExpiredInDB(ctx, t, env, sidExp)

		// 7. 显式调用 RenewSession(sidKeep) 触发同步清扫：
		//    - sidExp 从 client_sessions 删除，其名下 5 个文件句柄、1 个目录句柄、2 把锁、1 条等待记录全部级联清零；
		//    - Orphan 1, 2, 3, 4 因已无任何有效句柄，在同一事务内被物理删除，精确退还 4 个 used_inodes (7 -> 3) 与 12288 字节 used_bytes (15360 -> 3072)；
		//    - Orphan 5 (仍持 nil 句柄) 与 Orphan 6 (仍持 sidKeep 句柄) 必须保留且配额不退还！
		_, err = mgr.RenewSession(ctx, sidKeep)
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &sidKeep, leaseTTL)

		require.True(t, env.Model.DeletedInos[orphan1Model.Ino], "Orphan 1 (常规文件) 必须被物理回收")
		require.True(t, env.Model.DeletedInos[orphan2Model.Ino], "Orphan 2 (特殊节点) 必须被物理回收")
		require.True(t, env.Model.DeletedInos[orphan3Model.Ino], "Orphan 3 (孤儿目录) 必须被物理回收")
		require.True(t, env.Model.DeletedInos[orphan4Model.Ino], "Orphan 4 (未具名化TmpFile) 必须被物理回收")
		require.False(t, env.Model.DeletedInos[orphan5Model.Ino], "Orphan 5 (含nil无会话句柄) 绝不能被提前回收")
		require.False(t, env.Model.DeletedInos[orphan6Model.Ino], "Orphan 6 (含sidKeep存活会话句柄) 绝不能被提前回收")
		require.Equal(t, uint64(3), disk1.UsedInodes, "回收 4 个孤儿后剩余 root + Orphan5 + Orphan6 = 3 个节点")
		require.Equal(t, uint64(3072), disk1.UsedBytes, "回收 Orphan1(4096)+Orphan4(8192) 后剩余 Orphan5(2048)+Orphan6(1024) = 3072 字节")
		runAllProbesAndAuditors()

		// 8. 依次关闭 Orphan 5 的 nil 句柄与 Orphan 6 的 sidKeep 句柄，验证最终全部物理回收且配额归零
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: orphan5Model.Ino, Fh: h5Nil.Fh, SessionID: nil}))
		h5Nil.Closed = true
		env.Model.ClosedHandles[h5Nil.Fh] = h5Nil
		delete(env.Model.Handles, h5Nil.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphan5Model.Ino))
		require.Equal(t, uint64(2), disk1.UsedInodes)
		require.Equal(t, uint64(1024), disk1.UsedBytes)
		runAllProbesAndAuditors()

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: orphan6Model.Ino, Fh: h6Keep.Fh, SessionID: &sidKeep}))
		modelSyncSessionRenewal(env.Model, &sidKeep, leaseTTL)
		h6Keep.Closed = true
		env.Model.ClosedHandles[h6Keep.Fh] = h6Keep
		delete(env.Model.Handles, h6Keep.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphan6Model.Ino))
		require.Equal(t, uint64(1), disk1.UsedInodes)
		require.Equal(t, uint64(0), disk1.UsedBytes)
		runAllProbesAndAuditors()
	})

	t.Run("确定性边界_DeleteDisk参数校验与强制级联销毁复杂拓扑_孤儿_TmpFile_句柄_文件锁_底表五表零残留与邻盘隔离验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 40,
			Seed:       0x20260428,
			Rng:        newFormalRNG(0x20260428),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t10-del-det",
			LeaseTTL:   meta.DefaultSessionLeaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		runAllProbesAndAuditors := func() {
			formalProbeGetDisk(ctx, t, env)
			formalProbeStatFS(ctx, t, env)
			formalProbeLookup(ctx, t, env)
			formalProbeGetAttr(ctx, t, env)
			formalProbeReadlink(ctx, t, env)
			formalProbeFlush(ctx, t, env)
			formalProbeFsync(ctx, t, env)
			formalProbeReadDir(ctx, t, env)
			formalProbeReadDirPlus(ctx, t, env)
			formalProbeFsyncDir(ctx, t, env)
			formalAuditINV1DirTopology(ctx, t, env)
			formalAuditINV2DirNlink(ctx, t, env)
			formalAuditINV2NonDirNlink(ctx, t, env)
			formalAuditINV3OrphanAndHandles(ctx, t, env)
			formalAuditINV4DiskQuota(ctx, t, env)
			formalAuditNodeAndSymlinkQuota(ctx, t, env)
			formalAuditINV5CrossDiskIsolation(ctx, t, env)
			formalAuditINV6TmpFileMaterialization(ctx, t, env)
			formalAuditT10SessionAndDeleteDisk(ctx, t, env)
		}

		// 1. 验证 DeleteDisk 静态与动态错误码边界
		for _, invalidID := range []string{
			"",
			"00000000-0000-0000-0000-000000000000",
			"not-a-uuid",
			"12345678-1234-1234-1234-1234567890gg",
		} {
			require.ErrorIs(t, mgr.DeleteDisk(ctx, invalidID), meta.ErrInvalidArgument)
		}
		nonExistentUUID, err := uuid.NewV7()
		require.NoError(t, err)
		require.ErrorIs(t, mgr.DeleteDisk(ctx, nonExistentUUID.String()), meta.ErrNotFound)

		// 2. 创建两块虚拟磁盘：diskVictim (待强制级联销毁) 与 diskSurvivor (存活对照盘)
		formalCreateValidDiskStep(ctx, t, env)
		formalCreateValidDiskStep(ctx, t, env)
		disks := env.Model.ActiveDisks()
		require.Len(t, disks, 2)
		diskVictim, diskSurvivor := disks[0], disks[1]

		sid := nextIsolatedFormalSessionID(env)
		expAt, err := mgr.RenewSession(ctx, sid)
		require.NoError(t, err)
		env.Model.RecordSession(sid, expAt)

		// 3. 在 diskVictim 与 diskSurvivor 上分别构建包含多级子目录、多硬链接常规文件、符号链接、
		//    unlink-whilst-open 孤儿常规文件、rmdir-whilst-open 孤儿目录、未具名化 TmpFile、打开句柄与文件锁的完整结构
		populateComplexDisk := func(d *formalDisk, prefix string) {
			rootIno := d.RootIno
			rootModel := env.Model.Inodes[rootIno]

			// 3.1 多级目录 dirL1/dirL2
			d1, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte(prefix + "_d1"), Mode: 0o755})
			require.NoError(t, err)
			rootModel.Nlink++
			d.UsedInodes++
			d1Model := &formalInode{
				Ino: d1.Ino, Generation: d1.Generation, RootIno: rootIno, ParentIno: rootIno,
				Mode: d1.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
			}
			env.Model.RecordNewInode(d1Model)
			env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: prefix + "_d1"}] = d1Model.Ino

			d2, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: d1Model.Ino, Name: []byte(prefix + "_d2"), Mode: 0o755})
			require.NoError(t, err)
			d1Model.Nlink++
			d.UsedInodes++
			d2Model := &formalInode{
				Ino: d2.Ino, Generation: d2.Generation, RootIno: rootIno, ParentIno: d1Model.Ino,
				Mode: d2.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
			}
			env.Model.RecordNewInode(d2Model)
			env.Model.Entries[formalEntryKey{ParentIno: d1Model.Ino, Name: prefix + "_d2"}] = d2Model.Ino

			// 打开 d2 目录句柄
			odRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: d2Model.Ino, Flags: meta.O_RDONLY, SessionID: &sid})
			require.NoError(t, err)
			env.Model.RecordNewHandle(&formalHandle{
				Fh: odRes.Fh, Ino: d2Model.Ino, Flags: meta.O_RDONLY, SessionID: modelCloneSessionID(&sid), IsDir: true,
			})

			// 3.2 多硬链接常规文件 (nlink = 2, size = 4096) + 打开句柄 + POSIX 锁 + BSD flock + Xattr
			cReg, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: d2Model.Ino, Name: []byte(prefix + "_reg"), Mode: meta.S_IFREG | 0o644,
				Flags: meta.O_CREAT | meta.O_RDWR, SessionID: &sid,
			})
			require.NoError(t, err)
			regSz := uint64(4096)
			_, err = mgr.SetAttr(ctx, cReg.Inode.Ino, meta.SetAttrInput{Size: &regSz, Fh: &cReg.Fh, SessionID: &sid})
			require.NoError(t, err)
			regModel := &formalInode{
				Ino: cReg.Inode.Ino, Generation: cReg.Inode.Generation, RootIno: rootIno,
				Mode: cReg.Inode.Attr.Mode, Size: regSz, Nlink: 1,
			}
			env.Model.RecordNewInode(regModel)
			env.Model.Entries[formalEntryKey{ParentIno: d2Model.Ino, Name: prefix + "_reg"}] = regModel.Ino
			d.UsedInodes++
			d.UsedBytes += regSz
			env.Model.RecordNewHandle(&formalHandle{
				Fh: cReg.Fh, Ino: regModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: modelCloneSessionID(&sid),
			})

			_, err = mgr.Link(ctx, meta.LinkParams{Ino: regModel.Ino, NewParentIno: rootIno, NewName: []byte(prefix + "_reg_hardlink")})
			require.NoError(t, err)
			regModel.Nlink = 2
			env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: prefix + "_reg_hardlink"}] = regModel.Ino

			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino: regModel.Ino, Fh: cReg.Fh, LockOwner: 501, SessionID: sid, LkFlags: 0,
				Lk: meta.FileLock{Start: 0, End: 512, Type: meta.F_WRLCK, PID: 55},
			}))
			require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
				Ino: regModel.Ino, Fh: cReg.Fh, LockOwner: 501, SessionID: sid, LkFlags: meta.FuseLkFlock,
				Lk: meta.FileLock{Start: 0, End: meta.OffsetMax, Type: meta.F_RDLCK, PID: 55},
			}))
			require.NoError(t, mgr.SetXattr(ctx, meta.SetXattrParams{
				Ino: regModel.Ino, Name: []byte("user.tag"), Value: []byte(prefix),
			}))

			// 3.3 unlink-whilst-open 活跃孤儿文件 (nlink = 0, size = 2048)
			cOrphan, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: rootIno, Name: []byte(prefix + "_orphan_f"), Mode: meta.S_IFREG | 0o600,
				Flags: meta.O_CREAT | meta.O_RDWR, SessionID: nil,
			})
			require.NoError(t, err)
			orphanSz := uint64(2048)
			_, err = mgr.SetAttr(ctx, cOrphan.Inode.Ino, meta.SetAttrInput{Size: &orphanSz})
			require.NoError(t, err)
			orphanModel := &formalInode{
				Ino: cOrphan.Inode.Ino, Generation: cOrphan.Inode.Generation, RootIno: rootIno,
				Mode: cOrphan.Inode.Attr.Mode, Size: orphanSz, Nlink: 0,
			}
			env.Model.RecordNewInode(orphanModel)
			d.UsedInodes++
			d.UsedBytes += orphanSz
			env.Model.RecordNewHandle(&formalHandle{
				Fh: cOrphan.Fh, Ino: orphanModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: nil,
			})
			require.NoError(t, mgr.Unlink(ctx, rootIno, []byte(prefix+"_orphan_f")))

			// 3.4 rmdir-whilst-open 活跃孤儿目录 (nlink = 0)
			oDir, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte(prefix + "_orphan_d"), Mode: 0o755})
			require.NoError(t, err)
			oDirModel := &formalInode{
				Ino: oDir.Ino, Generation: oDir.Generation, RootIno: rootIno, ParentIno: rootIno,
				Mode: oDir.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 0,
			}
			env.Model.RecordNewInode(oDirModel)
			d.UsedInodes++
			odOrphan, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: oDirModel.Ino, Flags: meta.O_RDONLY, SessionID: nil})
			require.NoError(t, err)
			env.Model.RecordNewHandle(&formalHandle{
				Fh: odOrphan.Fh, Ino: oDirModel.Ino, Flags: meta.O_RDONLY, SessionID: nil, IsDir: true,
			})
			require.NoError(t, mgr.Rmdir(ctx, rootIno, []byte(prefix+"_orphan_d")))

			// 3.5 未具名化 TmpFile (nlink = 0, size = 1024)
			tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
				ParentIno: rootIno, Mode: 0o600, Flags: meta.O_RDWR | meta.O_TMPFILE, SessionID: nil,
			})
			require.NoError(t, err)
			tmpSz := uint64(1024)
			_, err = mgr.SetAttr(ctx, tmpRes.Inode.Ino, meta.SetAttrInput{Size: &tmpSz})
			require.NoError(t, err)
			tmpModel := &formalInode{
				Ino: tmpRes.Inode.Ino, Generation: tmpRes.Inode.Generation, RootIno: rootIno,
				Mode: tmpRes.Inode.Attr.Mode, Size: tmpSz, Nlink: 0, IsTmpFile: true,
			}
			env.Model.RecordNewInode(tmpModel)
			d.UsedInodes++
			d.UsedBytes += tmpSz
			env.Model.RecordNewHandle(&formalHandle{
				Fh: tmpRes.Fh, Ino: tmpModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE, SessionID: nil,
			})
		}

		populateComplexDisk(diskVictim, "victim")
		populateComplexDisk(diskSurvivor, "survivor")
		runAllProbesAndAuditors()

		survivorInodesBefore := diskSurvivor.UsedInodes
		survivorBytesBefore := diskSurvivor.UsedBytes

		// 4. 执行 DeleteDisk(diskVictim.DiskID)，验证单事务强制级联销毁整盘且底表零残留
		require.NoError(t, mgr.DeleteDisk(ctx, diskVictim.DiskID))
		env.Model.RecordDeletedDisk(diskVictim.DiskID)
		runAllProbesAndAuditors()

		// 5. 再次对已删除的 diskVictim 调用 DeleteDisk / GetDisk / StatFS 必须全部返回 ErrNotFound
		require.ErrorIs(t, mgr.DeleteDisk(ctx, diskVictim.DiskID), meta.ErrNotFound)
		_, err = mgr.GetDisk(ctx, diskVictim.DiskID)
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = mgr.StatFS(ctx, diskVictim.RootIno)
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 6. 验证存活邻盘 diskSurvivor 的配额账本与全部节点/句柄丝毫无损
		require.Equal(t, survivorInodesBefore, diskSurvivor.UsedInodes)
		require.Equal(t, survivorBytesBefore, diskSurvivor.UsedBytes)
		gotSurvivor, err := mgr.GetDisk(ctx, diskSurvivor.DiskID)
		require.NoError(t, err)
		require.Equal(t, survivorInodesBefore, gotSurvivor.UsedInodes)
		require.Equal(t, survivorBytesBefore, gotSurvivor.UsedBytes)
	})

	t.Run("随机状态机_RenewSession短租约过期清扫与DeleteDisk强制级联删盘专项高频碰撞对拍", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 16, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 12, Fn: formalActionUpdateDisk},
				{Name: "DeleteDisk", Weight: 22, Fn: formalActionDeleteDisk},
				{Name: "RenewSession", Weight: 32, Fn: formalActionRenewSession},
				{Name: "Mkdir", Weight: 16, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 14, Fn: formalActionRmdir},
				{Name: "Create", Weight: 20, Fn: formalActionCreate},
				{Name: "TmpFile", Weight: 18, Fn: formalActionTmpFile},
				{Name: "Open", Weight: 18, Fn: formalActionOpen},
				{Name: "Release", Weight: 16, Fn: formalActionRelease},
				{Name: "OpenDir", Weight: 14, Fn: formalActionOpenDir},
				{Name: "ReleaseDir", Weight: 12, Fn: formalActionReleaseDir},
				{Name: "Link", Weight: 14, Fn: formalActionLink},
				{Name: "Unlink", Weight: 18, Fn: formalActionUnlink},
				{Name: "SetAttr", Weight: 16, Fn: formalActionSetAttr},
				{Name: "Fallocate", Weight: 14, Fn: formalActionFallocate},
			},
		})
	})
}
