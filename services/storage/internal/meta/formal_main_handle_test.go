package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T6 核心文件句柄与孤儿生命周期动作：Create、Open、Release
	registerFormalAction("Create", 25, formalActionCreate)
	registerFormalAction("Open", 30, formalActionOpen)
	registerFormalAction("Release", 25, formalActionRelease)

	// 注册 T6 核心只读查账探针：Flush (无 lock_owner、无 session_id)、Fsync
	registerFormalProbe("Flush", formalProbeFlush)
	registerFormalProbe("Fsync", formalProbeFsync)

	// 注册 T6 底表数学不变量审计器：INV-3 (孤儿节点生命周期与活跃句柄存续)
	registerFormalAuditor("INV-3_OrphanAndHandles", formalAuditINV3OrphanAndHandles)
}

// ============================================================================
// 1. T6 参考模型辅助函数与 Create / Open / Release 预测器
// ============================================================================

// modelValidateOptionalSessionID 对应被测代码中 validateOptionalSessionID 的静态校验规则。
func modelValidateOptionalSessionID(sessionID *uint64) error {
	if sessionID != nil && (*sessionID == 0 || *sessionID > meta.OffsetMax) {
		return meta.ErrInvalidArgument
	}
	return nil
}

// modelCloneSessionID 复制可选的 SessionID 指针，避免测试步骤间共享可变指针。
func modelCloneSessionID(sessionID *uint64) *uint64 {
	if sessionID == nil {
		return nil
	}
	v := *sessionID
	return &v
}

// modelSessionIDEqual 判断两个可选 SessionID 是否相等（对应 SQL 中的 IS NOT DISTINCT FROM）。
func modelSessionIDEqual(a, b *uint64) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// modelFormatSessionID 格式化可选 SessionID 用于轨迹账单输出。
func modelFormatSessionID(sessionID *uint64) string {
	if sessionID == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *sessionID)
}

// modelSyncSessionRenewal 在携带非 nil SessionID 的写操作事务提交后，同步更新内存模型的会话租约与超期孤儿回收。
func modelSyncSessionRenewal(m *formalMainModel, sessionID *uint64, leaseTTL time.Duration) {
	if sessionID == nil {
		return
	}
	if leaseTTL <= 0 {
		leaseTTL = meta.DefaultSessionLeaseTTL
	}

	// 1. 清理已标记过期的客户端会话及其级联句柄 (对应 DeleteExpiredClientSessions + ON DELETE CASCADE)
	for sid, sess := range m.Sessions {
		if sess.Expired {
			for fh, h := range m.Handles {
				if h.SessionID != nil && *h.SessionID == sid {
					h.Closed = true
					m.ClosedHandles[fh] = h
					delete(m.Handles, fh)
				}
			}
			if sid != *sessionID {
				delete(m.Sessions, sid)
			}
		}
	}

	// 2. 插入或续期当前会话 (对应 UpsertClientSession)
	m.RecordSession(*sessionID, time.Now().Add(leaseTTL))

	// 3. 回收所有已失去全部有效句柄引用的孤儿节点 (对应 reclaimUnreferencedOrphans)
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && inode.Nlink == 0 {
			m.ReclaimOrphanInModel(ino)
		}
	}
}

// formalPredictCreate 根据内存参考模型预测 Create 的目标 Mode、GID 与预期错误码。
// 严格对齐 manager_create_open.go 中 Create 的静态校验与写事务校验优先级。
func formalPredictCreate(
	m *formalMainModel,
	params meta.CreateParams,
) (targetMode uint32, targetGID uint32, expectedErr error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return 0, 0, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return 0, 0, err
	}
	if err := modelValidateEntryName(params.Name); err != nil {
		return 0, 0, err
	}
	if params.Mode > 0xffff {
		return 0, 0, meta.ErrInvalidArgument
	}
	if (params.Flags & meta.O_TMPFILE) != 0 {
		return 0, 0, meta.ErrInvalidArgument
	}

	modeType := params.Mode & meta.S_IFMT
	if modeType != 0 && modeType != meta.S_IFREG {
		return 0, 0, meta.ErrInvalidArgument
	}

	parent, exists := m.Inodes[params.ParentIno]
	if !exists || parent.Deleted {
		return 0, 0, meta.ErrNotFound
	}
	if !parent.IsDir() {
		return 0, 0, meta.ErrNotDir
	}
	if parent.Nlink == 0 {
		return 0, 0, meta.ErrNotFound
	}
	if modelIsDotOrDotDot(params.Name) {
		return 0, 0, meta.ErrAlreadyExists
	}
	if _, dup := m.Entries[formalEntryKey{ParentIno: params.ParentIno, Name: string(params.Name)}]; dup {
		return 0, 0, meta.ErrAlreadyExists
	}

	targetGID = params.GID
	if (parent.Mode & meta.S_ISGID) != 0 {
		targetGID = parent.GID
	}
	targetMode = meta.S_IFREG | ((params.Mode & meta.PermMask) &^ (params.Umask & meta.PermMask))
	return targetMode, targetGID, nil
}

// formalPredictOpen 根据内存参考模型预测 Open 的目标节点、是否触发常规文件 O_TRUNC 截断以及预期错误码。
// 严格对齐 manager_create_open.go 中 Open 的静态校验、文件类型拦截、孤儿句柄存续检查与 O_TRUNC 权限检查。
func formalPredictOpen(
	m *formalMainModel,
	params meta.OpenParams,
) (targetNode *formalInode, willTruncateReg bool, expectedErr error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, false, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return nil, false, err
	}
	if (params.Flags & meta.O_TMPFILE) != 0 {
		return nil, false, meta.ErrInvalidArgument
	}

	inode, exists := m.Inodes[params.Ino]
	if !exists || inode.Deleted {
		return nil, false, meta.ErrNotFound
	}
	if inode.IsDir() {
		return nil, false, meta.ErrIsDir
	}
	if inode.IsLnk() {
		return nil, false, meta.ErrInvalidArgument
	}

	// 孤儿文件重打开检查：nlink == 0 时必须仍存在至少一条有效 FileHandle，否则返回 ErrNotFound
	if inode.Nlink == 0 {
		if !m.HasValidHandlesForIno(inode.Ino) {
			return nil, false, meta.ErrNotFound
		}
	}

	if (params.Flags & meta.O_TRUNC) != 0 {
		if (params.Flags & meta.O_ACCMODE) == meta.O_RDONLY {
			return nil, false, meta.ErrInvalidArgument
		}
		if inode.IsReg() {
			willTruncateReg = true
		}
	}

	return inode, willTruncateReg, nil
}

// formalPredictRelease 根据内存参考模型预测 Release 的目标节点、目标句柄与预期错误码。
// 严格对齐 manager_handle_session.go 中 Release 的静态校验、目录拦截、会话续期与句柄匹配删除逻辑。
func formalPredictRelease(
	m *formalMainModel,
	params meta.ReleaseParams,
) (targetNode *formalInode, targetHandle *formalHandle, expectedErr error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return nil, nil, meta.ErrInvalidArgument
	}
	if (params.ReleaseFlags &^ (meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock)) != 0 {
		return nil, nil, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return nil, nil, err
	}

	inode, exists := m.Inodes[params.Ino]
	if !exists || inode.Deleted {
		return nil, nil, meta.ErrNotFound
	}
	if inode.IsDir() {
		return nil, nil, meta.ErrIsDir
	}

	h, hExists := m.Handles[params.Fh]
	if !hExists || h.Closed || h.Ino != params.Ino {
		return nil, nil, meta.ErrBadFileHandle
	}
	if !modelSessionIDEqual(h.SessionID, params.SessionID) {
		return nil, nil, meta.ErrBadFileHandle
	}
	if !m.IsSessionValid(h.SessionID) {
		return nil, nil, meta.ErrBadFileHandle
	}

	return inode, h, nil
}

// findOrphanNonDirsInModel 返回内存模型中所有 nlink == 0 且尚未物理删除的非目录孤儿节点。
func findOrphanNonDirsInModel(m *formalMainModel) []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && !inode.IsDir() && inode.Nlink == 0 {
			res = append(res, inode)
		}
	}
	return res
}

// pickOptionalFormalSessionID 在长租约模式下按概率选择 nil 或与当前 Seed 隔离的客户端会话 ID；在毫秒级短租约测试下保持 nil 以免干扰过期控制。
func pickOptionalFormalSessionID(env *formalMainEnv) *uint64 {
	if env.LeaseTTL < 5*time.Second {
		return nil
	}
	if env.Rng.IntN(4) == 0 {
		base := (env.Seed & 0x3fffffff) + 7000000 + uint64(env.RunIndex)*10
		sid := base + uint64(env.Rng.IntN(2)) + 1
		if s, exists := env.Model.Sessions[sid]; exists && s.Expired {
			return nil
		}
		return &sid
	}
	return nil
}

// ============================================================================
// 2. T6 动作实现：Create、Open、Release 随机操作生成与逐操作对拍
// ============================================================================

// formalActionCreate 在状态机中随机生成常规文件原子创建并分配初始句柄、Umask/特权位计算、父目录 S_ISGID 属组继承、
// 初始文件水位注入以及各类非法标志位/模式/名称/父节点异常的 Create 请求并与参考模型对拍。
func formalActionCreate(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	uids := []uint32{0, 1000, 1001, 2002}
	gids := []uint32{0, 1000, 2000, 3003}
	umasks := []uint32{0, 0o022, 0o027, 0o077}
	validFlags := []uint32{
		meta.O_RDONLY,
		meta.O_WRONLY,
		meta.O_RDWR,
		meta.O_CREAT | meta.O_RDWR,
		meta.O_CREAT | meta.O_EXCL | meta.O_RDWR,
		meta.O_CREAT | meta.O_TRUNC | meta.O_WRONLY,
		meta.O_CREAT | meta.O_RDWR | meta.O_APPEND,
	}

	var params meta.CreateParams
	params.UID = uids[env.Rng.IntN(len(uids))]
	params.GID = gids[env.Rng.IntN(len(gids))]
	params.Umask = umasks[env.Rng.IntN(len(umasks))]
	params.Flags = validFlags[env.Rng.IntN(len(validFlags))]
	params.SessionID = pickOptionalFormalSessionID(env)

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 48:
		// 正常创建常规文件并分配初始句柄（覆盖 Mode 含/不含 S_IFREG、含 S_ISUID/S_ISGID/S_ISVTX、父目录 S_ISGID 继承）
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		if env.Rng.IntN(3) == 0 {
			for _, d := range activeDirs {
				if (d.Mode & meta.S_ISGID) != 0 {
					parent = d
					break
				}
			}
		}
		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName("create_f"))

		switch env.Rng.IntN(5) {
		case 0:
			caseTag = "正常创建(Mode仅权限位0o664)"
			params.Mode = 0o664
		case 1:
			caseTag = "正常创建(显式S_IFREG|0o644)"
			params.Mode = meta.S_IFREG | 0o644
		case 2:
			caseTag = "正常创建(带S_ISUID|S_ISGID特权位)"
			params.Mode = meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755
			params.Umask = 0o022
		case 3:
			caseTag = "正常创建(带S_ISVTX粘滞位)"
			params.Mode = meta.S_ISVTX | 0o777
			params.Umask = 0o027
		default:
			caseTag = "正常创建(常规读写标志)"
			params.Mode = meta.S_IFREG | 0o666
		}

	case branch < 58:
		// 同名目录项冲突 (ErrAlreadyExists)
		entries := env.Model.SortedEntries()
		if len(entries) > 0 {
			chosen := entries[env.Rng.IntN(len(entries))]
			caseTag = "同名目录项冲突(ErrAlreadyExists)"
			params.ParentIno = chosen.ParentIno
			params.Name = []byte(chosen.Name)
			params.Mode = meta.S_IFREG | 0o644
		} else {
			caseTag = "特殊名称(.)冲突(ErrAlreadyExists)"
			params.ParentIno = activeDirs[0].Ino
			params.Name = []byte(".")
			params.Mode = meta.S_IFREG | 0o644
		}

	case branch < 65:
		// 特殊名称 "." 或 ".." (ErrAlreadyExists)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = 0o644
		if env.Rng.IntN(2) == 0 {
			caseTag = "特殊名称(.)(ErrAlreadyExists)"
			params.Name = []byte(".")
		} else {
			caseTag = "特殊名称(..)(ErrAlreadyExists)"
			params.Name = []byte("..")
		}

	case branch < 75:
		// 非法 Flags (携带 O_TMPFILE) 或非法 Mode (非 S_IFREG 文件类型或超 0xffff)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName("bad_create_mode"))
		params.Mode = 0o644
		switch env.Rng.IntN(6) {
		case 0:
			caseTag = "非法Flags携带O_TMPFILE(ErrInvalidArgument)"
			params.Flags = meta.O_RDWR | meta.O_TMPFILE
		case 1:
			caseTag = "非法Mode带S_IFDIR(ErrInvalidArgument)"
			params.Mode = meta.S_IFDIR | 0o755
		case 2:
			caseTag = "非法Mode带S_IFLNK(ErrInvalidArgument)"
			params.Mode = meta.S_IFLNK | 0o777
		case 3:
			caseTag = "非法Mode带S_IFIFO(ErrInvalidArgument)"
			params.Mode = meta.S_IFIFO | 0o644
		case 4:
			caseTag = "非法Mode带S_IFCHR(ErrInvalidArgument)"
			params.Mode = meta.S_IFCHR | 0o644
		default:
			caseTag = "非法Mode超0xffff(ErrInvalidArgument)"
			params.Mode = 0x10000 | 0o644
		}

	case branch < 83:
		// 非法名称：空名、含 '/'、含 '\x00' 或超长 (> 255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = 0o644
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "空名称(ErrInvalidArgument)"
			params.Name = []byte("")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			params.Name = []byte("bad/create")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			params.Name = []byte("bad\x00create")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			params.Name = []byte(strings.Repeat("c", meta.MaxFileNameBytes+1+env.Rng.IntN(16)))
		}

	case branch < 91:
		// 父节点非目录 (ErrNotDir)
		nonDirs := env.Model.ActiveNonDirs()
		if len(nonDirs) > 0 {
			target := nonDirs[env.Rng.IntN(len(nonDirs))]
			if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
				caseTag = "父节点非目录(ErrNotDir)"
				params.ParentIno = target.Ino
				params.Name = []byte(env.UniqueEntryName("under_file"))
				params.Mode = 0o644
			} else {
				caseTag = "不存在父节点(ErrNotFound)"
				params.ParentIno = uint64(math.MaxInt64 - 112233)
				params.Name = []byte(env.UniqueEntryName("ghost"))
				params.Mode = 0o644
			}
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 112233)
			params.Name = []byte(env.UniqueEntryName("ghost"))
			params.Mode = 0o644
		}

	default:
		// 孤儿父目录 (nlink==0)、已物理删除父节点、非法 ParentIno=0 或非法 SessionID
		orphanDirs := findOrphanDirsInModel(env.Model)
		params.ParentIno = activeDirs[0].Ino
		params.Name = []byte(env.UniqueEntryName("err_create"))
		params.Mode = 0o644
		switch {
		case len(orphanDirs) > 0 && env.Rng.IntN(3) == 0:
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.ParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		case env.Rng.IntN(3) == 0:
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			params.ParentIno = 0
		case env.Rng.IntN(2) == 0:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		default:
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 667788)
		}
	}

	expectedMode, expectedGID, expectedErr := formalPredictCreate(env.Model, params)
	got, err := env.Mgr.Create(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Create[%s] (parent=%d, name=%q, mode=0o%o, flags=0x%x) 预期错误 %v，但实际成功: %+v",
			caseTag, params.ParentIno, string(params.Name), params.Mode, params.Flags, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Create[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Create[%s](parent=%d, name=%q, mode=0o%o, flags=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.ParentIno, string(params.Name), params.Mode, params.Flags, expectedErr)
	}

	require.NoErrorf(t, err, "Create[%s] (parent=%d, name=%q) 预期成功，但返回错误: %v",
		caseTag, params.ParentIno, string(params.Name), err)
	require.NotNil(t, got)
	require.NotNil(t, got.Inode)
	require.Greater(t, got.Fh, uint64(0), "Create 分配的 Fh 必须 > 0")

	parentModel := env.Model.Inodes[params.ParentIno]
	require.NotNil(t, parentModel)

	newFileModel := &formalInode{
		Ino:        got.Inode.Ino,
		Generation: got.Inode.Generation,
		RootIno:    parentModel.RootIno,
		ParentIno:  0,
		Mode:       expectedMode,
		UID:        params.UID,
		GID:        expectedGID,
		Rdev:       0,
		Size:       0,
		Nlink:      1,
		Flags:      0,
	}
	env.Model.RecordNewInode(newFileModel)
	env.Model.Entries[formalEntryKey{ParentIno: params.ParentIno, Name: string(params.Name)}] = got.Inode.Ino
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted {
		disk.UsedInodes++
	}

	// 先同步可选会话续期，再登记新分配的文件句柄
	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)
	env.Model.RecordNewHandle(&formalHandle{
		Fh:        got.Fh,
		Ino:       got.Inode.Ino,
		Flags:     params.Flags,
		SessionID: modelCloneSessionID(params.SessionID),
		IsDir:     false,
		Closed:    false,
	})

	assertFormalInodeMatchesEntity(t, newFileModel, got.Inode, fmt.Sprintf("Create[%s]返回值", caseTag))

	// 按 45% 概率为新创建的常规文件注入初始非零大小（同时显式保留 expectedMode 中的 S_ISUID/S_ISGID 特权位），
	// 使后续 Open(O_TRUNC) 截断配额退还与 unlink-whilst-open 孤儿物理回收在随机序列中高频碰撞非零字节账本。
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted && env.Rng.IntN(100) < 45 {
		avail := uint64(0)
		if disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		if avail >= 512 {
			maxSeed := avail
			if maxSeed > 16384 {
				maxSeed = 16384
			}
			seedSize := uint64(env.Rng.IntN(int(maxSeed-256))) + 256
			modeKeep := expectedMode
			resized, resizeErr := env.Mgr.SetAttr(ctx, newFileModel.Ino, meta.SetAttrInput{
				Size: &seedSize,
				Mode: &modeKeep,
			})
			require.NoError(t, resizeErr, "Create 后初始化非零文件大小不应失败")
			require.Equal(t, seedSize, resized.Attr.Size)
			require.Equal(t, expectedMode, resized.Attr.Mode)
			newFileModel.Size = seedSize
			disk.UsedBytes += seedSize
		}
	}

	return fmt.Sprintf("Create[%s](parent=%d, name=%q, mode=0o%o, flags=0x%x, sid=%s) -> 成功 (ino=%d, fh=%d, final_mode=0o%o, gid=%d, size=%d)",
		caseTag, params.ParentIno, string(params.Name), params.Mode, params.Flags, modelFormatSessionID(params.SessionID),
		got.Inode.Ino, got.Fh, newFileModel.Mode, newFileModel.GID, newFileModel.Size)
}

// formalActionOpen 在状态机中随机生成常规/特殊节点打开、活跃孤儿节点 (nlink==0) 重打开、
// O_TRUNC 截断配额退还与特权位剥离、以及目录/符号链接/无句柄孤儿/O_RDONLY|O_TRUNC/O_TMPFILE 拦截请求并与参考模型对拍。
func formalActionOpen(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 梳理各类候选节点：
	// 1. 仍持有有效句柄的 nlink == 0 非目录孤儿节点（支持重打开！）
	// 2. 无有效句柄的 nlink == 0 孤儿节点（应返回 ErrNotFound）
	// 3. 存活常规文件 (S_IFREG, nlink > 0)
	// 4. 存活特殊节点 (S_IFIFO / S_IFCHR / S_IFBLK / S_IFSOCK, nlink > 0)
	// 5. 存活符号链接 (S_IFLNK)
	var openableOrphans []*formalInode
	var deadOrphans []*formalInode
	var liveRegFiles []*formalInode
	var liveSpecialNodes []*formalInode

	for _, node := range env.Model.ActiveNonDirs() {
		if node.IsLnk() {
			continue
		}
		if node.Nlink == 0 {
			if env.Model.HasValidHandlesForIno(node.Ino) {
				openableOrphans = append(openableOrphans, node)
			} else {
				deadOrphans = append(deadOrphans, node)
			}
			continue
		}
		if node.IsReg() {
			liveRegFiles = append(liveRegFiles, node)
		} else {
			liveSpecialNodes = append(liveSpecialNodes, node)
		}
	}
	symlinks := env.Model.ActiveSymlinks()

	var params meta.OpenParams
	params.SessionID = pickOptionalFormalSessionID(env)

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 20 && len(openableOrphans) > 0:
		// 重点场景 1：重打开 nlink == 0 但仍持有有效句柄的孤儿文件（含普通打开与 O_TRUNC 截断孤儿文件）
		target := openableOrphans[env.Rng.IntN(len(openableOrphans))]
		params.Ino = target.Ino
		if target.IsReg() && env.Rng.IntN(3) == 0 {
			caseTag = "重打开nlink=0活跃孤儿文件(携带O_RDWR|O_TRUNC)"
			params.Flags = meta.O_RDWR | meta.O_TRUNC
		} else {
			caseTag = "重打开nlink=0活跃孤儿文件(新增并发句柄)"
			openModes := []uint32{meta.O_RDONLY, meta.O_WRONLY, meta.O_RDWR}
			params.Flags = openModes[env.Rng.IntN(len(openModes))]
		}

	case branch < 45 && len(liveRegFiles) > 0:
		// 重点场景 2：打开常规文件并携带 O_TRUNC（验证 Size 归零、S_ISUID/S_ISGID 剥离与磁盘 used_bytes 退还）
		// 优先选择 Size > 0 或带 S_ISUID/S_ISGID 的常规文件
		target := liveRegFiles[env.Rng.IntN(len(liveRegFiles))]
		for _, f := range liveRegFiles {
			if f.Size > 0 || (f.Mode&(meta.S_ISUID|meta.S_ISGID)) != 0 {
				target = f
				break
			}
		}
		params.Ino = target.Ino
		if env.Rng.IntN(2) == 0 {
			params.Flags = meta.O_WRONLY | meta.O_TRUNC
			caseTag = "打开常规文件(O_WRONLY|O_TRUNC截断与配额退还)"
		} else {
			params.Flags = meta.O_RDWR | meta.O_TRUNC
			caseTag = "打开常规文件(O_RDWR|O_TRUNC截断与配额退还)"
		}

	case branch < 62 && (len(liveRegFiles) > 0 || len(liveSpecialNodes) > 0):
		// 重点场景 3：不带 O_TRUNC 打开常规文件，或打开特殊设备/FIFO/SOCK 节点（含特殊节点带 O_TRUNC 忽略大小修改）
		if len(liveSpecialNodes) > 0 && (len(liveRegFiles) == 0 || env.Rng.IntN(3) == 0) {
			target := liveSpecialNodes[env.Rng.IntN(len(liveSpecialNodes))]
			params.Ino = target.Ino
			if env.Rng.IntN(2) == 0 {
				caseTag = "打开特殊设备/FIFO/SOCK节点(O_RDWR|O_TRUNC忽略截断)"
				params.Flags = meta.O_RDWR | meta.O_TRUNC
			} else {
				caseTag = "打开特殊设备/FIFO/SOCK节点(O_RDONLY)"
				params.Flags = meta.O_RDONLY
			}
		} else {
			target := liveRegFiles[env.Rng.IntN(len(liveRegFiles))]
			params.Ino = target.Ino
			flagsPool := []uint32{
				meta.O_RDONLY,
				meta.O_WRONLY,
				meta.O_RDWR,
				meta.O_RDWR | meta.O_APPEND,
				meta.O_CREAT | meta.O_RDWR,
			}
			params.Flags = flagsPool[env.Rng.IntN(len(flagsPool))]
			caseTag = "正常打开常规文件"
		}

	case branch < 72:
		// 对目录调用 Open -> 必须返回 ErrIsDir (EISDIR)
		dir := activeDirs[env.Rng.IntN(len(activeDirs))]
		caseTag = "对目录调用Open拦截(ErrIsDir)"
		params.Ino = dir.Ino
		params.Flags = meta.O_RDONLY

	case branch < 80 && len(symlinks) > 0:
		// 对符号链接 S_IFLNK 调用 Open -> 必须返回 ErrInvalidArgument (EINVAL)
		sym := symlinks[env.Rng.IntN(len(symlinks))]
		caseTag = "对符号链接S_IFLNK调用Open拦截(ErrInvalidArgument)"
		params.Ino = sym.Ino
		params.Flags = meta.O_RDONLY

	case branch < 88:
		// 非法标志组合：O_RDONLY | O_TRUNC 或携带 O_TMPFILE -> 必须返回 ErrInvalidArgument
		if len(liveRegFiles) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "只读模式携带O_TRUNC拦截(ErrInvalidArgument)"
			params.Ino = liveRegFiles[env.Rng.IntN(len(liveRegFiles))].Ino
			params.Flags = meta.O_RDONLY | meta.O_TRUNC
		} else {
			caseTag = "Open携带O_TMPFILE拦截(ErrInvalidArgument)"
			params.Ino = activeDirs[0].Ino
			if len(liveRegFiles) > 0 {
				params.Ino = liveRegFiles[0].Ino
			}
			params.Flags = meta.O_RDWR | meta.O_TMPFILE
		}

	case branch < 95:
		// 已物理删除节点、无有效句柄的孤儿节点或不存在的 Ino -> ErrNotFound
		params.Flags = meta.O_RDONLY
		if len(deadOrphans) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "无有效句柄nlink=0孤儿节点(ErrNotFound)"
			params.Ino = deadOrphans[env.Rng.IntN(len(deadOrphans))].Ino
		} else if len(env.Model.DeletedInos) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "已物理回收节点(ErrNotFound)"
			for ino := range env.Model.DeletedInos {
				params.Ino = ino
				break
			}
		} else {
			caseTag = "不存在的Ino(ErrNotFound)"
			params.Ino = uint64(math.MaxInt64 - 543210)
		}

	default:
		// 非法静态入参：Ino == 0、Ino > math.MaxInt64、非法 SessionID
		params.Flags = meta.O_RDONLY
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "非法Ino=0(ErrInvalidArgument)"
			params.Ino = 0
		case 1:
			caseTag = "非法Ino超MaxInt64(ErrInvalidArgument)"
			params.Ino = uint64(math.MaxInt64) + 10
		default:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			params.Ino = activeDirs[0].Ino
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		}
	}

	targetNode, willTruncateReg, expectedErr := formalPredictOpen(env.Model, params)
	got, err := env.Mgr.Open(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Open[%s] (ino=%d, flags=0x%x) 预期错误 %v，但实际成功: %+v",
			caseTag, params.Ino, params.Flags, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Open[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Open[%s](ino=%d, flags=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.Flags, expectedErr)
	}

	require.NoErrorf(t, err, "Open[%s] (ino=%d, flags=0x%x) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.Flags, err)
	require.NotNil(t, got)
	require.Greater(t, got.Fh, uint64(0), "Open 分配的 Fh 必须 > 0")
	require.NotNil(t, targetNode)

	oldSize := targetNode.Size
	oldMode := targetNode.Mode

	// 若携带 O_TRUNC 且目标为常规文件，同步更新参考模型：Size 归零、剥离 S_ISUID/S_ISGID、扣减所属磁盘 used_bytes
	if willTruncateReg {
		targetNode.Mode &^= (meta.S_ISUID | meta.S_ISGID)
		targetNode.Size = 0
		if oldSize > 0 {
			if disk, ok := env.Model.DiskByRootIno[targetNode.RootIno]; ok && !disk.Deleted {
				if disk.UsedBytes >= oldSize {
					disk.UsedBytes -= oldSize
				} else {
					disk.UsedBytes = 0
				}
			}
		}
	}

	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)
	env.Model.RecordNewHandle(&formalHandle{
		Fh:        got.Fh,
		Ino:       targetNode.Ino,
		Flags:     params.Flags,
		SessionID: modelCloneSessionID(params.SessionID),
		IsDir:     false,
		Closed:    false,
	})

	return fmt.Sprintf("Open[%s](ino=%d, nlink=%d, flags=0x%x, sid=%s) -> 成功 (fh=%d, trunc=%v, size:%d->%d, mode:0o%o->0o%o)",
		caseTag, targetNode.Ino, targetNode.Nlink, params.Flags, modelFormatSessionID(params.SessionID),
		got.Fh, willTruncateReg, oldSize, targetNode.Size, oldMode, targetNode.Mode)
}

// formalActionRelease 在状态机中随机生成正常释放文件句柄、触发孤儿节点最终物理回收与配额退还、
// 以及已释放句柄/跨节点句柄/目录句柄/会话不匹配/目录节点/非法标志位拦截的 Release 请求并与参考模型对拍。
func formalActionRelease(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	activeFileHandles := env.Model.ActiveFileHandles()
	activeDirHandles := env.Model.ActiveDirHandles()
	nonDirs := env.Model.ActiveNonDirs()

	validReleaseFlags := []uint32{
		0,
		meta.FuseReleaseFlush,
		meta.FuseReleaseFlockUnlock,
		meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock,
	}

	var params meta.ReleaseParams
	params.ReleaseFlags = validReleaseFlags[env.Rng.IntN(len(validReleaseFlags))]

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 52 && len(activeFileHandles) > 0:
		// 正常释放活跃文件句柄（优先按概率挑选 nlink == 0 孤儿文件的句柄以高频验证孤儿延迟回收）
		chosen := activeFileHandles[env.Rng.IntN(len(activeFileHandles))]
		if env.Rng.IntN(2) == 0 {
			for _, h := range activeFileHandles {
				if inode, ok := env.Model.Inodes[h.Ino]; ok && inode.Nlink == 0 {
					chosen = h
					break
				}
			}
		}
		params.Ino = chosen.Ino
		params.Fh = chosen.Fh
		params.SessionID = modelCloneSessionID(chosen.SessionID)
		if inode, ok := env.Model.Inodes[chosen.Ino]; ok && inode.Nlink == 0 {
			caseTag = "正常释放孤儿文件句柄(检验nlink=0回收)"
		} else {
			caseTag = "正常释放存活文件句柄"
		}

	case branch < 64 && len(env.Model.ClosedHandles) > 0:
		// 对已关闭的句柄再次调用 Release（若节点仍存活则返回 ErrBadFileHandle，若孤儿节点已物理回收则返回 ErrNotFound）
		var closedList []*formalHandle
		for _, fh := range env.Model.AllCreatedFhs {
			if ch, ok := env.Model.ClosedHandles[fh]; ok && !ch.IsDir {
				closedList = append(closedList, ch)
			}
		}
		if len(closedList) > 0 {
			chosen := closedList[env.Rng.IntN(len(closedList))]
			caseTag = "重复释放已关闭文件句柄"
			params.Ino = chosen.Ino
			params.Fh = chosen.Fh
			params.SessionID = modelCloneSessionID(chosen.SessionID)
		} else {
			caseTag = "释放不存在句柄Fh(ErrBadFileHandle)"
			params.Ino = activeDirs[0].Ino
			if len(nonDirs) > 0 {
				params.Ino = nonDirs[0].Ino
			}
			params.Fh = uint64(math.MaxInt64 - 111222)
		}

	case branch < 74 && len(nonDirs) > 0:
		// 句柄不匹配：不存在的 Fh、跨 Ino 混用其他文件的 Fh、或混用 OpenDir 创建的目录 Fh -> ErrBadFileHandle
		targetNode := nonDirs[env.Rng.IntN(len(nonDirs))]
		params.Ino = targetNode.Ino
		if len(activeDirHandles) > 0 && env.Rng.IntN(3) == 0 {
			dirH := activeDirHandles[env.Rng.IntN(len(activeDirHandles))]
			caseTag = "传入目录句柄Fh调用文件Release(ErrBadFileHandle)"
			params.Fh = dirH.Fh
			params.SessionID = modelCloneSessionID(dirH.SessionID)
		} else if len(activeFileHandles) > 0 && env.Rng.IntN(2) == 0 {
			var otherH *formalHandle
			for _, h := range activeFileHandles {
				if h.Ino != targetNode.Ino {
					otherH = h
					break
				}
			}
			if otherH != nil {
				caseTag = "跨Ino句柄不匹配(ErrBadFileHandle)"
				params.Fh = otherH.Fh
				params.SessionID = modelCloneSessionID(otherH.SessionID)
			} else {
				caseTag = "不存在的句柄Fh(ErrBadFileHandle)"
				params.Fh = uint64(math.MaxInt64 - 222333)
			}
		} else {
			caseTag = "不存在的句柄Fh(ErrBadFileHandle)"
			params.Fh = uint64(math.MaxInt64 - 222333)
		}

	case branch < 82 && len(activeFileHandles) > 0 && env.LeaseTTL >= 5*time.Second:
		// 会话 SessionID 不匹配 -> ErrBadFileHandle (且原句柄保持打开不被误删)
		chosen := activeFileHandles[env.Rng.IntN(len(activeFileHandles))]
		params.Ino = chosen.Ino
		params.Fh = chosen.Fh
		if chosen.SessionID == nil {
			wrongSID := uint64(1009)
			params.SessionID = &wrongSID
			caseTag = "无会话句柄传入非空SessionID不匹配(ErrBadFileHandle)"
		} else {
			if env.Rng.IntN(2) == 0 {
				params.SessionID = nil
				caseTag = "绑定会话句柄传入nil-SessionID不匹配(ErrBadFileHandle)"
			} else {
				wrongSID := *chosen.SessionID + 100
				params.SessionID = &wrongSID
				caseTag = "绑定会话句柄传入异构SessionID不匹配(ErrBadFileHandle)"
			}
		}

	case branch < 89:
		// 对目录 Ino 调用 Release -> 必须返回 ErrIsDir (EISDIR)
		dir := activeDirs[env.Rng.IntN(len(activeDirs))]
		caseTag = "对目录节点调用Release拦截(ErrIsDir)"
		params.Ino = dir.Ino
		params.Fh = 1

	case branch < 95:
		// 对不存在或已物理删除的 Ino 调用 Release -> ErrNotFound
		caseTag = "不存在或已删除Ino(ErrNotFound)"
		params.Fh = 1
		if len(env.Model.DeletedInos) > 0 && env.Rng.IntN(2) == 0 {
			for ino := range env.Model.DeletedInos {
				params.Ino = ino
				break
			}
		} else {
			params.Ino = uint64(math.MaxInt64 - 333444)
		}

	default:
		// 非法静态入参：Ino==0、Fh==0、未知 ReleaseFlags 位、非法 SessionID==0
		params.Ino = activeDirs[0].Ino
		params.Fh = 1
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "非法Ino=0(ErrInvalidArgument)"
			params.Ino = 0
		case 1:
			caseTag = "非法Fh=0(ErrInvalidArgument)"
			params.Fh = 0
		case 2:
			caseTag = "非法ReleaseFlags未知标志位(ErrInvalidArgument)"
			params.ReleaseFlags = 0x100
		default:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		}
	}

	targetNode, targetHandle, expectedErr := formalPredictRelease(env.Model, params)
	err := env.Mgr.Release(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Release[%s] (ino=%d, fh=%d, flags=0x%x, sid=%s) 预期错误 %v，但实际成功",
			caseTag, params.Ino, params.Fh, params.ReleaseFlags, modelFormatSessionID(params.SessionID), expectedErr)
		require.Truef(t, errors.Is(err, expectedErr),
			"Release[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		return fmt.Sprintf("Release[%s](ino=%d, fh=%d, flags=0x%x, sid=%s) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.Fh, params.ReleaseFlags, modelFormatSessionID(params.SessionID), expectedErr)
	}

	require.NoErrorf(t, err, "Release[%s] (ino=%d, fh=%d) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.Fh, err)
	require.NotNil(t, targetNode)
	require.NotNil(t, targetHandle)

	// 同步更新内存参考模型：
	// 1. 同步可选会话续期；
	// 2. 将句柄标记为已关闭并移入 ClosedHandles；
	// 3. 若目标节点 nlink == 0，调用 ReclaimOrphanInModel（当且仅当再无任何有效句柄时物理删除孤儿节点并退还 used_inodes 与 used_bytes）。
	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)
	targetHandle.Closed = true
	env.Model.ClosedHandles[targetHandle.Fh] = targetHandle
	delete(env.Model.Handles, targetHandle.Fh)

	reclaimed := false
	if targetNode.Nlink == 0 {
		reclaimed = env.Model.ReclaimOrphanInModel(targetNode.Ino)
	}

	return fmt.Sprintf("Release[%s](ino=%d, fh=%d, nlink=%d, sid=%s) -> 成功 (孤儿物理回收=%v)",
		caseTag, targetNode.Ino, targetHandle.Fh, targetNode.Nlink, modelFormatSessionID(params.SessionID), reclaimed)
}

// ============================================================================
// 3. T6 只读查账探针：Flush (无 lock_owner、无 session_id) 与 Fsync
// ============================================================================

// formalProbeFlush 在每步写操作后以只读模式 (LockOwner == nil && SessionID == nil) 全面核对：
// 1. 所有无会话有效非目录句柄（含 nlink == 0 的活跃孤儿文件句柄）连续调用两次 Flush 均幂等成功，且绝不销毁句柄；
// 2. 所有绑定了非 nil SessionID 的非目录句柄，在未传 SessionID (SessionID == nil) 调用只读 Flush 时返回 ErrBadFileHandle；
// 3. 所有已释放句柄、不存在句柄、跨节点 ino 不匹配句柄、或 OpenDir 创建的目录句柄传入非目录 ino 时返回 ErrBadFileHandle；
// 4. 对目录节点调用只读 Flush 返回 ErrIsDir；对已物理删除节点返回 ErrNotFound；对非法 0 参数返回 ErrInvalidArgument。
func formalProbeFlush(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	activeFileHandles := env.Model.ActiveFileHandles()
	activeDirHandles := env.Model.ActiveDirHandles()

	// 1. 核对所有有效非目录句柄（含 nlink == 0 的活跃孤儿文件句柄）
	for _, h := range activeFileHandles {
		err := env.Mgr.Flush(ctx, meta.FlushParams{
			Ino:       h.Ino,
			Fh:        h.Fh,
			LockOwner: nil,
			SessionID: nil,
		})
		if h.SessionID == nil {
			require.NoErrorf(t, err, "有效无会话文件句柄 (fh=%d, ino=%d) 首次调用只读 Flush 不应失败", h.Fh, h.Ino)
			// 连续第二次调用只读 Flush，证明 Flush 绝不销毁文件句柄
			err2 := env.Mgr.Flush(ctx, meta.FlushParams{
				Ino:       h.Ino,
				Fh:        h.Fh,
				LockOwner: nil,
				SessionID: nil,
			})
			require.NoErrorf(t, err2, "有效无会话文件句柄 (fh=%d, ino=%d) 二次幂等调用只读 Flush 不应失败", h.Fh, h.Ino)
		} else {
			require.Errorf(t, err, "绑定会话句柄 (fh=%d, ino=%d, sid=%d) 以 SessionID=nil 调用只读 Flush 应返回 ErrBadFileHandle",
				h.Fh, h.Ino, *h.SessionID)
			require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
				"预期 ErrBadFileHandle, 实际: %v", err)
		}
	}

	// 2. 核对已释放的非目录句柄 (ClosedHandles)
	for _, fh := range env.Model.AllCreatedFhs {
		ch, isClosed := env.Model.ClosedHandles[fh]
		if !isClosed || ch.IsDir {
			continue
		}
		err := env.Mgr.Flush(ctx, meta.FlushParams{
			Ino:       ch.Ino,
			Fh:        ch.Fh,
			LockOwner: nil,
			SessionID: nil,
		})
		if env.Model.DeletedInos[ch.Ino] {
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"已物理删除节点 ino=%d 的已关闭句柄 fh=%d 调用 Flush 应返回 ErrNotFound, 实际: %v",
				ch.Ino, ch.Fh, err)
		} else if inode, ok := env.Model.Inodes[ch.Ino]; ok && !inode.Deleted && !inode.IsDir() {
			if inode.Nlink > 0 || env.Model.HasValidHandlesForIno(inode.Ino) {
				require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
					"存活节点 ino=%d 的已释放句柄 fh=%d 调用 Flush 应返回 ErrBadFileHandle, 实际: %v",
					ch.Ino, ch.Fh, err)
			}
		}
	}

	// 3. 抽样存活非目录节点，核对不存在句柄、跨节点 ino 不匹配句柄、以及目录句柄 (OpenDir) 返回 ErrBadFileHandle
	for _, node := range env.Model.ActiveNonDirs() {
		if node.Nlink == 0 && !env.Model.HasValidHandlesForIno(node.Ino) {
			continue
		}
		// 3.1 不存在的 Fh -> ErrBadFileHandle
		err := env.Mgr.Flush(ctx, meta.FlushParams{
			Ino:       node.Ino,
			Fh:        uint64(math.MaxInt64 - 888111),
			LockOwner: nil,
			SessionID: nil,
		})
		require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
			"节点 ino=%d 传入不存在 Fh 调用 Flush 应返回 ErrBadFileHandle, 实际: %v", node.Ino, err)

		// 3.2 跨节点 ino 不匹配句柄 -> ErrBadFileHandle
		for _, otherH := range activeFileHandles {
			if otherH.Ino != node.Ino {
				errCross := env.Mgr.Flush(ctx, meta.FlushParams{
					Ino:       node.Ino,
					Fh:        otherH.Fh,
					LockOwner: nil,
					SessionID: nil,
				})
				require.Truef(t, errors.Is(errCross, meta.ErrBadFileHandle),
					"节点 ino=%d 传入异构节点 (ino=%d) 的句柄 fh=%d 调用 Flush 应返回 ErrBadFileHandle, 实际: %v",
					node.Ino, otherH.Ino, otherH.Fh, errCross)
				break
			}
		}

		// 3.3 传入 OpenDir 创建的目录句柄 Fh 与非目录 Ino -> ErrBadFileHandle
		if len(activeDirHandles) > 0 {
			dirH := activeDirHandles[0]
			errDirFh := env.Mgr.Flush(ctx, meta.FlushParams{
				Ino:       node.Ino,
				Fh:        dirH.Fh,
				LockOwner: nil,
				SessionID: nil,
			})
			require.Truef(t, errors.Is(errDirFh, meta.ErrBadFileHandle),
				"非目录节点 ino=%d 传入目录句柄 fh=%d 调用 Flush 应返回 ErrBadFileHandle, 实际: %v",
				node.Ino, dirH.Fh, errDirFh)
		}
		break
	}

	// 4. 核对目录节点调用 Flush 返回 ErrIsDir
	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) > 0 {
		fhToUse := uint64(1)
		if len(activeDirHandles) > 0 {
			fhToUse = activeDirHandles[0].Fh
		}
		err := env.Mgr.Flush(ctx, meta.FlushParams{
			Ino:       activeDirs[0].Ino,
			Fh:        fhToUse,
			LockOwner: nil,
			SessionID: nil,
		})
		require.Truef(t, errors.Is(err, meta.ErrIsDir),
			"目录节点 ino=%d 调用 Flush 应返回 ErrIsDir, 实际: %v", activeDirs[0].Ino, err)
	}

	// 5. 核对静态非法参数与不存在 Ino
	require.ErrorIs(t, env.Mgr.Flush(ctx, meta.FlushParams{Ino: 0, Fh: 1}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.Flush(ctx, meta.FlushParams{Ino: 1, Fh: 0}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.Flush(ctx, meta.FlushParams{Ino: uint64(math.MaxInt64 - 919191), Fh: 1}), meta.ErrNotFound)
}

// formalProbeFsync 在每步写操作后全面核对只读 Fsync：
// 1. 所有有效非目录句柄（含无会话句柄与绑定会话句柄、含 nlink == 0 的活跃孤儿文件句柄）调用 Fsync(Datasync=false/true) 均幂等成功；
// 2. 会话不匹配、不存在/已释放句柄、跨节点 ino 不匹配句柄、或目录句柄 (OpenDir) 均返回 ErrBadFileHandle；
// 3. 目录节点返回 ErrIsDir，已删除/不存在节点返回 ErrNotFound，非法入参返回 ErrInvalidArgument。
func formalProbeFsync(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	activeFileHandles := env.Model.ActiveFileHandles()
	activeDirHandles := env.Model.ActiveDirHandles()

	// 1. 核对所有有效非目录句柄（含 nlink == 0 活跃孤儿文件句柄）
	for idx, h := range activeFileHandles {
		err := env.Mgr.Fsync(ctx, meta.FsyncParams{
			Ino:       h.Ino,
			Fh:        h.Fh,
			Datasync:  idx%2 == 0,
			SessionID: h.SessionID,
		})
		require.NoErrorf(t, err, "有效文件句柄 (fh=%d, ino=%d, sid=%s) 调用 Fsync 不应失败",
			h.Fh, h.Ino, modelFormatSessionID(h.SessionID))

		// 抽样核对 SessionID 不匹配时返回 ErrBadFileHandle
		if idx == 0 {
			var mismatchedSID *uint64
			if h.SessionID == nil {
				bogus := uint64(999888)
				mismatchedSID = &bogus
			}
			errMismatch := env.Mgr.Fsync(ctx, meta.FsyncParams{
				Ino:       h.Ino,
				Fh:        h.Fh,
				Datasync:  true,
				SessionID: mismatchedSID,
			})
			require.Truef(t, errors.Is(errMismatch, meta.ErrBadFileHandle),
				"Fsync 传入不匹配 SessionID 应返回 ErrBadFileHandle, 实际: %v", errMismatch)
		}
	}

	// 2. 核对已释放的非目录句柄 (ClosedHandles)
	for _, fh := range env.Model.AllCreatedFhs {
		ch, isClosed := env.Model.ClosedHandles[fh]
		if !isClosed || ch.IsDir {
			continue
		}
		err := env.Mgr.Fsync(ctx, meta.FsyncParams{
			Ino:       ch.Ino,
			Fh:        ch.Fh,
			Datasync:  false,
			SessionID: ch.SessionID,
		})
		if env.Model.DeletedInos[ch.Ino] {
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"已物理删除节点 ino=%d 的已关闭句柄 fh=%d 调用 Fsync 应返回 ErrNotFound, 实际: %v",
				ch.Ino, ch.Fh, err)
		} else if inode, ok := env.Model.Inodes[ch.Ino]; ok && !inode.Deleted && !inode.IsDir() {
			if inode.Nlink > 0 || env.Model.HasValidHandlesForIno(inode.Ino) {
				require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
					"存活节点 ino=%d 的已释放句柄 fh=%d 调用 Fsync 应返回 ErrBadFileHandle, 实际: %v",
					ch.Ino, ch.Fh, err)
			}
		}
	}

	// 3. 抽样存活非目录节点，核对不存在句柄、跨节点 ino 不匹配句柄、以及目录句柄 (OpenDir) 返回 ErrBadFileHandle
	for _, node := range env.Model.ActiveNonDirs() {
		if node.Nlink == 0 && !env.Model.HasValidHandlesForIno(node.Ino) {
			continue
		}
		err := env.Mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      node.Ino,
			Fh:       uint64(math.MaxInt64 - 777222),
			Datasync: true,
		})
		require.Truef(t, errors.Is(err, meta.ErrBadFileHandle),
			"节点 ino=%d 传入不存在 Fh 调用 Fsync 应返回 ErrBadFileHandle, 实际: %v", node.Ino, err)

		for _, otherH := range activeFileHandles {
			if otherH.Ino != node.Ino {
				errCross := env.Mgr.Fsync(ctx, meta.FsyncParams{
					Ino:       node.Ino,
					Fh:        otherH.Fh,
					Datasync:  false,
					SessionID: otherH.SessionID,
				})
				require.Truef(t, errors.Is(errCross, meta.ErrBadFileHandle),
					"节点 ino=%d 传入异构节点 (ino=%d) 的句柄 fh=%d 调用 Fsync 应返回 ErrBadFileHandle, 实际: %v",
					node.Ino, otherH.Ino, otherH.Fh, errCross)
				break
			}
		}

		if len(activeDirHandles) > 0 {
			dirH := activeDirHandles[0]
			errDirFh := env.Mgr.Fsync(ctx, meta.FsyncParams{
				Ino:       node.Ino,
				Fh:        dirH.Fh,
				Datasync:  false,
				SessionID: dirH.SessionID,
			})
			require.Truef(t, errors.Is(errDirFh, meta.ErrBadFileHandle),
				"非目录节点 ino=%d 传入目录句柄 fh=%d 调用 Fsync 应返回 ErrBadFileHandle, 实际: %v",
				node.Ino, dirH.Fh, errDirFh)
		}
		break
	}

	// 4. 核对目录节点调用 Fsync 返回 ErrIsDir
	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) > 0 {
		fhToUse := uint64(1)
		if len(activeDirHandles) > 0 {
			fhToUse = activeDirHandles[0].Fh
		}
		err := env.Mgr.Fsync(ctx, meta.FsyncParams{
			Ino:      activeDirs[0].Ino,
			Fh:       fhToUse,
			Datasync: false,
		})
		require.Truef(t, errors.Is(err, meta.ErrIsDir),
			"目录节点 ino=%d 调用 Fsync 应返回 ErrIsDir, 实际: %v", activeDirs[0].Ino, err)
	}

	// 5. 核对静态非法参数与不存在 Ino
	zeroSID := uint64(0)
	require.ErrorIs(t, env.Mgr.Fsync(ctx, meta.FsyncParams{Ino: 0, Fh: 1}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.Fsync(ctx, meta.FsyncParams{Ino: 1, Fh: 0}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.Fsync(ctx, meta.FsyncParams{Ino: 1, Fh: 1, SessionID: &zeroSID}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.Fsync(ctx, meta.FsyncParams{Ino: uint64(math.MaxInt64 - 929292), Fh: 1}), meta.ErrNotFound)
}

// ============================================================================
// 4. T6 底表数学不变量审计：INV-3 (孤儿节点生命周期与活跃句柄存续)
// ============================================================================

type dbAuditHandleRow struct {
	Fh        uint64
	Ino       uint64
	Flags     uint32
	SessionID *uint64
	RootIno   uint64
	IsValid   bool
}

// formalAuditINV3OrphanAndHandles 直接扫描 PostgreSQL 底表 inodes、file_handles 与 client_sessions，强制校验 INV-3：
// 1. 底表中任意 nlink == 0 的 Orphan Inode 必在 file_handles 表中存在关联句柄；
//    且当无过期未清扫会话时（或在 RenewSession 同步清扫后），任意 nlink == 0 的 Orphan Inode 必关联至少一条有效句柄
//    (fh.session_id IS NULL OR s.expires_at > clock_timestamp())；
// 2. 底表 file_handles 记录集合（fh、ino、flags、session_id）与内存参考模型 Handles 完全一致，已关闭句柄零残留。
func formalAuditINV3OrphanAndHandles(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
	for _, r := range env.Model.AllCreatedRootInos {
		rootInos = append(rootInos, int64(r))
	}

	// 1. 扫描底表 file_handles JOIN inodes LEFT JOIN client_sessions
	handleRows, err := env.Pool.Query(ctx, `
		SELECT
			fh.fh,
			fh.ino,
			fh.flags,
			fh.session_id,
			i.root_ino,
			(fh.session_id IS NULL OR (s.session_id IS NOT NULL AND s.expires_at > clock_timestamp())) AS is_valid
		FROM file_handles fh
		JOIN inodes i ON i.ino = fh.ino
		LEFT JOIN client_sessions s ON s.session_id = fh.session_id
		WHERE i.root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "INV-3 审计: 查询底表 file_handles 失败")
	defer handleRows.Close()

	dbHandlesByFh := make(map[uint64]dbAuditHandleRow)
	totalHandlesByIno := make(map[uint64]int)
	validHandlesByIno := make(map[uint64]int)

	for handleRows.Next() {
		var fh, ino, rootIno int64
		var flags int32
		var pgSID pgtype.Int8
		var isValid bool

		err := handleRows.Scan(&fh, &ino, &flags, &pgSID, &rootIno, &isValid)
		require.NoError(t, err, "INV-3 审计: 扫描 file_handles 行失败")

		var sidPtr *uint64
		if pgSID.Valid {
			uSID := uint64(pgSID.Int64)
			sidPtr = &uSID
		}

		uFh := uint64(fh)
		uIno := uint64(ino)
		uRoot := uint64(rootIno)

		dbHandlesByFh[uFh] = dbAuditHandleRow{
			Fh:        uFh,
			Ino:       uIno,
			Flags:     uint32(flags),
			SessionID: sidPtr,
			RootIno:   uRoot,
			IsValid:   isValid,
		}
		totalHandlesByIno[uIno]++
		if isValid {
			validHandlesByIno[uIno]++
		}
	}
	require.NoError(t, handleRows.Err())

	// 2. 核对底表 file_handles 与内存参考模型 Handles / ClosedHandles 完全一致
	for uFh, dbH := range dbHandlesByFh {
		require.Falsef(t, env.Model.DeletedRootInos[dbH.RootIno],
			"INV-3 违例: 已删除磁盘 root_ino=%d 的句柄 fh=%d 仍在底表 file_handles 中残留", dbH.RootIno, uFh)
		require.Falsef(t, env.Model.DeletedInos[dbH.Ino],
			"INV-3 违例: 已物理删除节点 ino=%d 的句柄 fh=%d 仍在底表 file_handles 中残留", dbH.Ino, uFh)

		_, isClosed := env.Model.ClosedHandles[uFh]
		require.Falsef(t, isClosed,
			"INV-3 违例: 已释放句柄 fh=%d (ino=%d) 仍在底表 file_handles 中残留", uFh, dbH.Ino)

		modelH, exists := env.Model.Handles[uFh]
		require.Truef(t, exists && !modelH.Closed,
			"INV-3 违例: 底表存在句柄 fh=%d (ino=%d)，但内存参考模型 Handles 中缺失或已标记关闭", uFh, dbH.Ino)
		require.Equalf(t, modelH.Ino, dbH.Ino,
			"INV-3 违例: 句柄 fh=%d 的 ino 不一致: 模型=%d, 底表=%d", uFh, modelH.Ino, dbH.Ino)
		require.Equalf(t, modelH.Flags, dbH.Flags,
			"INV-3 违例: 句柄 fh=%d 的 flags 不一致: 模型=0x%x, 底表=0x%x", uFh, modelH.Flags, dbH.Flags)
		require.Truef(t, modelSessionIDEqual(modelH.SessionID, dbH.SessionID),
			"INV-3 违例: 句柄 fh=%d 的 session_id 不一致: 模型=%s, 底表=%s",
			uFh, modelFormatSessionID(modelH.SessionID), modelFormatSessionID(dbH.SessionID))

		if env.Model.IsSessionValid(modelH.SessionID) {
			require.Truef(t, dbH.IsValid,
				"INV-3 违例: 句柄 fh=%d 在内存模型中会话有效，但在底表中会话已过期或不存在", uFh)
		}
	}

	for uFh, modelH := range env.Model.Handles {
		if modelH.Closed {
			continue
		}
		_, exists := dbHandlesByFh[uFh]
		require.Truef(t, exists,
			"INV-3 违例: 内存模型未关闭句柄 fh=%d (ino=%d) 在底表 file_handles 中缺失", uFh, modelH.Ino)
	}

	// 3. 扫描底表中所有 nlink == 0 的 Orphan Inode，断言每个孤儿节点必关联有效 FileHandle
	hasUnsweptExpiredSession := false
	for _, s := range env.Model.Sessions {
		if s.Expired {
			hasUnsweptExpiredSession = true
			break
		}
	}

	orphanRows, err := env.Pool.Query(ctx, `
		SELECT ino, mode, size, nlink, root_ino
		FROM inodes
		WHERE root_ino = ANY($1) AND nlink = 0
	`, rootInos)
	require.NoError(t, err, "INV-3 审计: 查询底表孤儿 inodes 失败")
	defer orphanRows.Close()

	dbOrphanInos := make(map[uint64]bool)
	for orphanRows.Next() {
		var ino, size, rootIno int64
		var mode, nlink int32
		err := orphanRows.Scan(&ino, &mode, &size, &nlink, &rootIno)
		require.NoError(t, err, "INV-3 审计: 扫描孤儿 inodes 行失败")

		uIno := uint64(ino)
		dbOrphanInos[uIno] = true

		modelNode, exists := env.Model.Inodes[uIno]
		require.Truef(t, exists && !modelNode.Deleted,
			"INV-3 违例: 底表残留 nlink=0 孤儿节点 ino=%d (mode=0o%o, size=%d)，但内存模型中已物理删除或不存在",
			uIno, uint32(mode), size)
		require.Zerof(t, modelNode.Nlink,
			"INV-3 违例: 底表孤儿节点 ino=%d 的 nlink=0，但内存模型中 Nlink=%d", uIno, modelNode.Nlink)

		require.GreaterOrEqualf(t, totalHandlesByIno[uIno], 1,
			"INV-3 违例: 底表孤儿节点 ino=%d (nlink=0) 在 file_handles 表中不存在任何关联句柄", uIno)

		if !hasUnsweptExpiredSession || env.Model.HasValidHandlesForIno(uIno) {
			require.GreaterOrEqualf(t, validHandlesByIno[uIno], 1,
				"INV-3 违例: 底表孤儿节点 ino=%d (nlink=0) 在 file_handles 表中未关联任何有效句柄 (session_id IS NULL 或未过期会话)",
				uIno)
		}
	}
	require.NoError(t, orphanRows.Err())

	// 反向核验：内存参考模型中所有 nlink == 0 且未删除的孤儿节点必在底表孤儿集合中
	for _, ino := range env.Model.AllCreatedInos {
		if mNode, ok := env.Model.Inodes[ino]; ok && !mNode.Deleted && mNode.Nlink == 0 {
			require.Truef(t, dbOrphanInos[ino],
				"INV-3 违例: 内存模型中的活跃孤儿节点 ino=%d (nlink=0) 在底表 inodes 中缺失", ino)
		}
	}
}

// ============================================================================
// 5. T6 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T6_FileHandleOrphan 验证 T6 主状态机 Create/Open/Flush/Fsync/Release 文件句柄生命周期、
// unlink-whilst-open 孤儿延迟回收、O_TRUNC 截断配额退还与底表 INV-3 审计。
func TestFormalMain_T6_FileHandleOrphan(t *testing.T) {
	t.Run("确定性边界_Create全参数矩阵_Open截断O_TRUNC配额退还与特权位剥离_unlink-whilst-open多句柄延迟回收_只读Flush与Fsync探针_INV-3全矩阵验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 40,
			Seed:       0x20260423,
			Rng:        newFormalRNG(0x20260423),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t6-det",
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
			formalAuditINV1DirTopology(ctx, t, env)
			formalAuditINV2DirNlink(ctx, t, env)
			formalAuditINV2NonDirNlink(ctx, t, env)
			formalAuditINV3OrphanAndHandles(ctx, t, env)
			formalAuditINV4DiskQuota(ctx, t, env)
			formalAuditNodeAndSymlinkQuota(ctx, t, env)
			formalAuditINV5CrossDiskIsolation(ctx, t, env)
		}

		// 1. 创建初始测试虚拟磁盘与带 S_ISGID 的子目录 sgid_dir
		formalCreateValidDiskStep(ctx, t, env)
		disk := env.Model.ActiveDisks()[0]
		rootIno := disk.RootIno
		rootDir := env.Model.Inodes[rootIno]

		sgidDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sgid_dir"),
			Mode:      meta.S_ISGID | 0o775,
			Umask:     0o022,
			UID:       1000,
			GID:       6000,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: sgidDir.Ino, Generation: sgidDir.Generation, RootIno: rootIno, ParentIno: rootIno,
			Mode: sgidDir.Attr.Mode, UID: 1000, GID: 6000, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sgid_dir"}] = sgidDir.Ino
		rootDir.Nlink++
		disk.UsedInodes++
		runAllProbesAndAuditors()

		// 2. 验证 Create 正常创建（Umask 计算、S_ISUID/S_ISGID 特权位、父目录 S_ISGID 属组继承、绑定会话与无会话句柄分配）
		// 2.1 在 sgid_dir 下调用 Create（Mode=0o664 不带 S_IFREG，传入 GID=9999，Umask=0o022，无会话）
		c1Res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("c1_in_sgid"),
			Mode:      meta.S_ISUID | meta.S_ISGID | 0o775,
			Umask:     0o022,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			UID:       1001,
			GID:       9999,
			SessionID: nil,
		})
		require.NoError(t, err)
		require.NotNil(t, c1Res)
		require.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), c1Res.Inode.Attr.Mode)
		require.Equal(t, uint32(6000), c1Res.Inode.Attr.GID, "Create 在 S_ISGID 父目录下必须继承父目录 GID=6000")
		require.Equal(t, uint32(1), c1Res.Inode.Attr.Nlink)
		require.Equal(t, uint64(0), c1Res.Inode.Attr.Size)

		c1Model := &formalInode{
			Ino: c1Res.Inode.Ino, Generation: c1Res.Inode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: c1Res.Inode.Attr.Mode, UID: 1001, GID: 6000, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(c1Model)
		env.Model.Entries[formalEntryKey{ParentIno: sgidDir.Ino, Name: "c1_in_sgid"}] = c1Model.Ino
		disk.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: c1Res.Fh, Ino: c1Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR, SessionID: nil,
		})

		// 为 c1_in_sgid 注入 8192 字节大小并保留 S_ISUID|S_ISGID 特权位
		c1Size := uint64(8192)
		c1KeepMode := c1Model.Mode
		_, err = mgr.SetAttr(ctx, c1Model.Ino, meta.SetAttrInput{Size: &c1Size, Mode: &c1KeepMode})
		require.NoError(t, err)
		c1Model.Size = c1Size
		disk.UsedBytes += c1Size
		runAllProbesAndAuditors()

		// 2.2 验证 Create 全部错误码边界
		// 携带 O_TMPFILE -> ErrInvalidArgument
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno, Name: []byte("bad_tmpfile"), Mode: 0o644, Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 非法 Mode 类型位 (S_IFDIR, S_IFLNK, S_IFIFO, > 0xffff) -> ErrInvalidArgument
		for _, badMode := range []uint32{
			meta.S_IFDIR | 0o755,
			meta.S_IFLNK | 0o777,
			meta.S_IFIFO | 0o644,
			0x10000 | 0o644,
		} {
			_, err = mgr.Create(ctx, meta.CreateParams{
				ParentIno: rootIno, Name: []byte("bad_mode"), Mode: badMode, Flags: meta.O_RDWR,
			})
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		}

		// 同名冲突与 "." / ".." -> ErrAlreadyExists
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino, Name: []byte("c1_in_sgid"), Mode: 0o644, Flags: meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino, Name: []byte("."), Mode: 0o644, Flags: meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino, Name: []byte(".."), Mode: 0o644, Flags: meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)

		// 父节点非目录 -> ErrNotDir
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: c1Model.Ino, Name: []byte("under_file"), Mode: 0o644, Flags: meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrNotDir)

		// 3. 验证 Open(O_TRUNC) 截断常规文件、剥离 S_ISUID/S_ISGID 特权位、退还磁盘 used_bytes，以及特殊节点忽略 O_TRUNC
		// 3.1 只读模式携带 O_RDONLY | O_TRUNC -> 必须返回 ErrInvalidArgument，且原文件大小 (8192) 与特权位丝毫无损
		_, err = mgr.Open(ctx, meta.OpenParams{
			Ino:   c1Model.Ino,
			Flags: meta.O_RDONLY | meta.O_TRUNC,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument, "O_RDONLY|O_TRUNC 必须返回 ErrInvalidArgument")
		require.Equal(t, uint64(8192), disk.UsedBytes)
		runAllProbesAndAuditors()

		// 3.2 携带 O_WRONLY | O_TRUNC 打开 c1_in_sgid (并绑定会话 sid=1001)：
		// 预期：Size 从 8192 归零，disk.UsedBytes 从 8192 扣减至 0，Mode 剥离 S_ISUID|S_ISGID 变为 S_IFREG|0o755
		sid1 := uint64(1001)
		openTruncRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       c1Model.Ino,
			Flags:     meta.O_WRONLY | meta.O_TRUNC,
			SessionID: &sid1,
		})
		require.NoError(t, err)
		require.NotNil(t, openTruncRes)
		c1Model.Size = 0
		c1Model.Mode &^= (meta.S_ISUID | meta.S_ISGID)
		disk.UsedBytes = 0
		modelSyncSessionRenewal(env.Model, &sid1, env.LeaseTTL)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openTruncRes.Fh, Ino: c1Model.Ino, Flags: meta.O_WRONLY | meta.O_TRUNC, SessionID: modelCloneSessionID(&sid1),
		})
		runAllProbesAndAuditors()

		// 3.3 创建 FIFO 特殊节点与符号链接，验证 Open 对特殊节点忽略 O_TRUNC、对符号链接返回 ErrInvalidArgument、对目录返回 ErrIsDir
		fifoNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: rootIno, Name: []byte("fifo_special"), Mode: meta.S_IFIFO | 0o644,
		})
		require.NoError(t, err)
		fifoModel := &formalInode{
			Ino: fifoNode.Ino, Generation: fifoNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: fifoNode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fifoModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "fifo_special"}] = fifoModel.Ino
		disk.UsedInodes++

		openFifoRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   fifoModel.Ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})
		require.NoError(t, err, "特殊节点 (FIFO) 携带 O_RDWR|O_TRUNC 调用 Open 应成功并忽略截断")
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openFifoRes.Fh, Ino: fifoModel.Ino, Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		symNode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno, Name: []byte("sym_node"), Target: []byte("/target/path"),
		})
		require.NoError(t, err)
		symModel := &formalInode{
			Ino: symNode.Ino, Generation: symNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: symNode.Attr.Mode, Size: uint64(len("/target/path")), Nlink: 1, SymlinkTarget: []byte("/target/path"),
		}
		env.Model.RecordNewInode(symModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sym_node"}] = symModel.Ino
		disk.UsedInodes++

		_, err = mgr.Open(ctx, meta.OpenParams{Ino: symModel.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrInvalidArgument, "对符号链接调用 Open 必须返回 ErrInvalidArgument")
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: sgidDir.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrIsDir, "对目录调用 Open 必须返回 ErrIsDir")
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: c1Model.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE})
		require.ErrorIs(t, err, meta.ErrInvalidArgument, "Open 携带 O_TMPFILE 必须返回 ErrInvalidArgument")
		runAllProbesAndAuditors()

		// 4. 验证 unlink-whilst-open 延迟回收、孤儿文件重打开 (Open on nlink==0)、孤儿文件 O_TRUNC 配额退还与多句柄渐进 Release
		// 先为 c1_in_sgid 赋予 4096 字节大小
		c1NewSize := uint64(4096)
		_, err = mgr.SetAttr(ctx, c1Model.Ino, meta.SetAttrInput{Size: &c1NewSize})
		require.NoError(t, err)
		c1Model.Size = c1NewSize
		disk.UsedBytes += c1NewSize

		// 此时 c1_in_sgid 持有 2 个有效句柄：c1Res.Fh (nil 会话) 与 openTruncRes.Fh (sid1 会话)。
		// 执行 Unlink 将其从目录树解链：nlink 1 -> 0，但因仍有活跃句柄，保留为孤儿节点，暂不退还 used_inodes 和 4096 字节 used_bytes！
		usedInodesBeforeOrphan := disk.UsedInodes
		err = mgr.Unlink(ctx, sgidDir.Ino, []byte("c1_in_sgid"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: sgidDir.Ino, Name: "c1_in_sgid"})
		c1Model.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(c1Model.Ino))
		require.Equal(t, usedInodesBeforeOrphan, disk.UsedInodes)
		require.Equal(t, uint64(4096), disk.UsedBytes)
		runAllProbesAndAuditors()

		// 4.1 对 nlink == 0 但仍有有效句柄的孤儿文件 c1Model 调用 Open 重新打开 -> 必须成功并分配第 3 个句柄！
		reopenOrphanRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   c1Model.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err, "仍有活跃句柄的 nlink=0 孤儿文件必须支持再次 Open")
		env.Model.RecordNewHandle(&formalHandle{
			Fh: reopenOrphanRes.Fh, Ino: c1Model.Ino, Flags: meta.O_RDONLY,
		})
		runAllProbesAndAuditors()

		// 4.2 测试 Release 的 SessionID 校验：openTruncRes.Fh 绑定了 sid1，若传 nil 或错误 sid2 调用 Release 必返回 ErrBadFileHandle
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: c1Model.Ino, Fh: openTruncRes.Fh, SessionID: nil})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "绑定会话的句柄在未传 SessionID 时 Release 必须返回 ErrBadFileHandle")
		sidWrong := uint64(2002)
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: c1Model.Ino, Fh: openTruncRes.Fh, SessionID: &sidWrong})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "SessionID 不匹配时 Release 必须返回 ErrBadFileHandle")

		// 4.3 依次释放第 1 个句柄 (c1Res.Fh) 与第 2 个句柄 (openTruncRes.Fh)：
		// 因还剩第 3 个句柄 (reopenOrphanRes.Fh)，孤儿节点依然存活且 4096 字节配额未退还！
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:          c1Model.Ino,
			Fh:           c1Res.Fh,
			ReleaseFlags: meta.FuseReleaseFlush,
		})
		require.NoError(t, err)
		env.Model.Handles[c1Res.Fh].Closed = true
		env.Model.ClosedHandles[c1Res.Fh] = env.Model.Handles[c1Res.Fh]
		delete(env.Model.Handles, c1Res.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(c1Model.Ino))

		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:          c1Model.Ino,
			Fh:           openTruncRes.Fh,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
			SessionID:    &sid1,
		})
		require.NoError(t, err)
		env.Model.Handles[openTruncRes.Fh].Closed = true
		env.Model.ClosedHandles[openTruncRes.Fh] = env.Model.Handles[openTruncRes.Fh]
		delete(env.Model.Handles, openTruncRes.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(c1Model.Ino))
		require.Equal(t, uint64(4096), disk.UsedBytes, "孤儿节点尚存最后一个句柄时仍不退还配额")
		runAllProbesAndAuditors()

		// 4.4 对已关闭的 c1Res.Fh 再次调用 Release（此时 c1Model 仍存活） -> 返回 ErrBadFileHandle
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: c1Model.Ino, Fh: c1Res.Fh})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// 4.5 释放孤儿节点的最后一个句柄 (reopenOrphanRes.Fh) -> 在同一事务内物理删除孤儿 Inode 并退还 used_inodes - 1 与 used_bytes - 4096！
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: c1Model.Ino,
			Fh:  reopenOrphanRes.Fh,
		})
		require.NoError(t, err)
		env.Model.Handles[reopenOrphanRes.Fh].Closed = true
		env.Model.ClosedHandles[reopenOrphanRes.Fh] = env.Model.Handles[reopenOrphanRes.Fh]
		delete(env.Model.Handles, reopenOrphanRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(c1Model.Ino), "最后一个句柄释放后必须物理回收孤儿节点")
		require.Equal(t, usedInodesBeforeOrphan-1, disk.UsedInodes, "孤儿物理回收后 used_inodes 必须 -1")
		require.Equal(t, uint64(0), disk.UsedBytes, "孤儿物理回收后 used_bytes 必须退还归零")
		runAllProbesAndAuditors()

		// 4.6 孤儿节点物理删除后，再次尝试 Open(c1Model.Ino) 必须返回 ErrNotFound
		_, err = mgr.Open(ctx, meta.OpenParams{Ino: c1Model.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrNotFound, "已物理回收的孤儿节点调用 Open 必须返回 ErrNotFound")

		// 5. 验证 OpenDir 创建的目录句柄在调用 Flush / Fsync / Release 时的跨类型隔离
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: sgidDir.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openDirRes.Fh, Ino: sgidDir.Ino, Flags: meta.O_RDONLY, IsDir: true,
		})
		// 将目录句柄 Fh 传给非目录节点 fifoModel 调用 Flush / Fsync / Release -> 均返回 ErrBadFileHandle
		require.ErrorIs(t, mgr.Flush(ctx, meta.FlushParams{Ino: fifoModel.Ino, Fh: openDirRes.Fh}), meta.ErrBadFileHandle)
		require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: fifoModel.Ino, Fh: openDirRes.Fh}), meta.ErrBadFileHandle)
		require.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{Ino: fifoModel.Ino, Fh: openDirRes.Fh}), meta.ErrBadFileHandle)

		// 正常释放目录句柄与 FIFO 句柄
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: sgidDir.Ino, Fh: openDirRes.Fh}))
		env.Model.Handles[openDirRes.Fh].Closed = true
		env.Model.ClosedHandles[openDirRes.Fh] = env.Model.Handles[openDirRes.Fh]
		delete(env.Model.Handles, openDirRes.Fh)

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: fifoModel.Ino, Fh: openFifoRes.Fh}))
		env.Model.Handles[openFifoRes.Fh].Closed = true
		env.Model.ClosedHandles[openFifoRes.Fh] = env.Model.Handles[openFifoRes.Fh]
		delete(env.Model.Handles, openFifoRes.Fh)

		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_Create与Open及Release文件句柄生命周期_unlink-whilst-open延迟回收_Flush与Fsync探针及底表INV-3全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 8, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 8, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 12, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 12, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 8, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 12, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 8, Fn: formalActionSymlink},
				{Name: "Link", Weight: 15, Fn: formalActionLink},
				{Name: "Unlink", Weight: 25, Fn: formalActionUnlink},
				{Name: "Create", Weight: 28, Fn: formalActionCreate},
				{Name: "Open", Weight: 32, Fn: formalActionOpen},
				{Name: "Release", Weight: 28, Fn: formalActionRelease},
			},
			Probes: []formalNamedProbe{
				{Name: "GetDisk", Fn: formalProbeGetDisk},
				{Name: "StatFS", Fn: formalProbeStatFS},
				{Name: "Lookup", Fn: formalProbeLookup},
				{Name: "GetAttr", Fn: formalProbeGetAttr},
				{Name: "Readlink", Fn: formalProbeReadlink},
				{Name: "Flush", Fn: formalProbeFlush},
				{Name: "Fsync", Fn: formalProbeFsync},
			},
			Auditors: []formalNamedAuditor{
				{Name: "INV-1_DirTopology", Fn: formalAuditINV1DirTopology},
				{Name: "INV-2_DirNlink", Fn: formalAuditINV2DirNlink},
				{Name: "INV-2_NonDirNlink", Fn: formalAuditINV2NonDirNlink},
				{Name: "INV-3_OrphanAndHandles", Fn: formalAuditINV3OrphanAndHandles},
				{Name: "INV-4_DiskQuota", Fn: formalAuditINV4DiskQuota},
				{Name: "INV-4_NodeAndSymlinkQuota", Fn: formalAuditNodeAndSymlinkQuota},
				{Name: "INV-5_CrossDiskIsolation", Fn: formalAuditINV5CrossDiskIsolation},
			},
		})
	})
}
