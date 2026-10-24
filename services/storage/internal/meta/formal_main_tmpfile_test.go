package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T8 核心匿名临时文件动作：TmpFile（含普通可具名化与 O_EXCL 独占临时文件创建、一次性 Link 具名化与防二次复活对拍）
	registerFormalAction("TmpFile", 25, formalActionTmpFile)

	// 注册 T8 底表数学不变量审计器：INV-6 (匿名临时文件一次性具名化与 O_TMPFILE 标志位守恒)
	registerFormalAuditor("INV-6_TmpFileMaterialization", formalAuditINV6TmpFileMaterialization)
}

// ============================================================================
// 1. T8 参考模型推导函数：TmpFile 预测器
// ============================================================================

// formalPredictTmpFile 根据内存参考模型预测 TmpFile 的目标 Mode、GID、写入底表句柄的 Flags 以及预期错误码。
// 严格对齐 manager_create_open.go 中 TmpFile 的静态校验、访问模式检查与写事务内父目录校验顺序。
func formalPredictTmpFile(
	m *formalMainModel,
	params meta.TmpFileParams,
) (targetMode uint32, targetGID uint32, handleFlags uint32, expectedErr error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return 0, 0, 0, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return 0, 0, 0, err
	}
	if params.Mode > 0xffff {
		return 0, 0, 0, meta.ErrInvalidArgument
	}

	modeType := params.Mode & meta.S_IFMT
	if modeType != 0 && modeType != meta.S_IFREG {
		return 0, 0, 0, meta.ErrInvalidArgument
	}

	accMode := params.Flags & meta.O_ACCMODE
	if accMode != meta.O_WRONLY && accMode != meta.O_RDWR {
		return 0, 0, 0, meta.ErrInvalidArgument
	}

	parent, exists := m.Inodes[params.ParentIno]
	if !exists || parent.Deleted {
		return 0, 0, 0, meta.ErrNotFound
	}
	if !parent.IsDir() {
		return 0, 0, 0, meta.ErrNotDir
	}
	if parent.Nlink == 0 {
		return 0, 0, 0, meta.ErrNotFound
	}

	targetGID = params.GID
	if (parent.Mode & meta.S_ISGID) != 0 {
		targetGID = parent.GID
	}
	targetMode = meta.S_IFREG | ((params.Mode & meta.PermMask) &^ (params.Umask & meta.PermMask))
	handleFlags = params.Flags | meta.O_TMPFILE
	return targetMode, targetGID, handleFlags, nil
}

// ============================================================================
// 2. T8 动作实现：TmpFile 随机操作生成、父目录时间戳零变动核验与一次性 Link 对拍
// ============================================================================

// pickOptionalTmpFileSessionID 生成基于当前测试种子隔离的可选 SessionID，避免多工作树并行测试时在 client_sessions 表发生跨测试会话清理碰撞。
func pickOptionalTmpFileSessionID(env *formalMainEnv) *uint64 {
	if env.LeaseTTL < 5*time.Second {
		return nil
	}
	if env.Rng.IntN(4) == 0 {
		base := uint64(8000000) + (env.Seed%1000000)*10 + uint64(env.RunIndex)*2
		candidates := []uint64{base + 1, base + 2}
		sid := candidates[env.Rng.IntN(len(candidates))]
		if s, exists := env.Model.Sessions[sid]; exists && s.Expired {
			return nil
		}
		return &sid
	}
	return nil
}

// formalActionTmpFile 在状态机中随机生成：
// 1. 普通非 O_EXCL 与 O_EXCL 独占匿名临时文件创建（覆盖 O_WRONLY/O_RDWR、Umask/特权位、父目录 S_ISGID 属组继承、
//    不写 entries 表、不刷新父目录 mtime/ctime/nlink、递增所属磁盘 used_inodes + 1，以及按概率注入非零 Size 水位）；
// 2. 针对匿名 TmpFile 的一次性 Link 具名化、O_EXCL 永久拒绝 Link、已具名化后 Unlink 防二次复活、以及 Create/Open 拒绝 O_TMPFILE 组合对拍；
// 3. 各类非法访问模式 (O_RDONLY / O_ACCMODE)、非法 Mode 类型位、非目录父节点 (ErrNotDir)、孤儿父目录 (ErrNotFound) 拦截对拍。
func formalActionTmpFile(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 按 16% 概率且当前存在匿名 TmpFile 或存活文件时，执行针对 TmpFile 一次性具名化 / 防二次复活 / 伪造 O_TMPFILE 拦截的专项组合步
	if env.Rng.IntN(100) < 16 {
		if desc, ok := formalStepTmpFileMaterializeOrReject(ctx, t, env, activeDirs); ok {
			return desc
		}
	}

	uids := []uint32{0, 1000, 1001, 2002}
	gids := []uint32{0, 1000, 2000, 3003}
	umasks := []uint32{0, 0o022, 0o027, 0o077}
	validFlagsPool := []uint32{
		meta.O_WRONLY,
		meta.O_RDWR,
		meta.O_WRONLY | meta.O_TMPFILE,
		meta.O_RDWR | meta.O_TMPFILE,
		meta.O_RDWR | meta.O_APPEND,
		meta.O_WRONLY | meta.O_EXCL,
		meta.O_RDWR | meta.O_EXCL,
		meta.O_RDWR | meta.O_TMPFILE | meta.O_EXCL,
	}

	var params meta.TmpFileParams
	params.UID = uids[env.Rng.IntN(len(uids))]
	params.GID = gids[env.Rng.IntN(len(gids))]
	params.Umask = umasks[env.Rng.IntN(len(umasks))]
	params.Flags = validFlagsPool[env.Rng.IntN(len(validFlagsPool))]
	params.SessionID = pickOptionalTmpFileSessionID(env)

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 52:
		// 正常创建匿名临时文件（覆盖非 O_EXCL 可具名化与 O_EXCL 独占、Mode 仅权限位或带 S_IFREG、带特权位、父目录 S_ISGID 继承）
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

		isExcl := (params.Flags & meta.O_EXCL) != 0
		switch env.Rng.IntN(5) {
		case 0:
			params.Mode = 0o640
			if isExcl {
				caseTag = "正常创建O_EXCL独占TmpFile(Mode=0o640)"
			} else {
				caseTag = "正常创建可具名化TmpFile(Mode=0o640)"
			}
		case 1:
			params.Mode = meta.S_IFREG | 0o600
			if isExcl {
				caseTag = "正常创建O_EXCL独占TmpFile(显式S_IFREG)"
			} else {
				caseTag = "正常创建可具名化TmpFile(显式S_IFREG)"
			}
		case 2:
			params.Mode = meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755
			params.Umask = 0o022
			caseTag = "正常创建TmpFile(带S_ISUID|S_ISGID特权位)"
		case 3:
			params.Mode = meta.S_ISVTX | 0o777
			params.Umask = 0o027
			caseTag = "正常创建TmpFile(带S_ISVTX粘滞位与Umask)"
		default:
			params.Mode = meta.S_IFREG | 0o666
			caseTag = "正常创建TmpFile(标准权限)"
		}

	case branch < 66:
		// 非法访问模式 Flags：O_RDONLY (0) 或 O_ACCMODE (3) -> ErrInvalidArgument
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = 0o600
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "非法Flags为O_RDONLY(ErrInvalidArgument)"
			params.Flags = meta.O_RDONLY
		case 1:
			caseTag = "非法Flags为O_TMPFILE|O_RDONLY(ErrInvalidArgument)"
			params.Flags = meta.O_TMPFILE | meta.O_RDONLY
		case 2:
			caseTag = "非法Flags为O_RDONLY|O_EXCL(ErrInvalidArgument)"
			params.Flags = meta.O_RDONLY | meta.O_EXCL
		default:
			caseTag = "非法Flags访问模式位等于3(O_ACCMODE)(ErrInvalidArgument)"
			params.Flags = meta.O_ACCMODE | meta.O_TMPFILE
		}

	case branch < 80:
		// 非法 Mode：非 S_IFREG 文件类型位或超过 0o177777 (0xffff) -> ErrInvalidArgument
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Flags = meta.O_RDWR
		switch env.Rng.IntN(7) {
		case 0:
			caseTag = "非法Mode带S_IFDIR(ErrInvalidArgument)"
			params.Mode = meta.S_IFDIR | 0o755
		case 1:
			caseTag = "非法Mode带S_IFLNK(ErrInvalidArgument)"
			params.Mode = meta.S_IFLNK | 0o777
		case 2:
			caseTag = "非法Mode带S_IFIFO(ErrInvalidArgument)"
			params.Mode = meta.S_IFIFO | 0o644
		case 3:
			caseTag = "非法Mode带S_IFCHR(ErrInvalidArgument)"
			params.Mode = meta.S_IFCHR | 0o644
		case 4:
			caseTag = "非法Mode带S_IFBLK(ErrInvalidArgument)"
			params.Mode = meta.S_IFBLK | 0o644
		case 5:
			caseTag = "非法Mode带S_IFSOCK(ErrInvalidArgument)"
			params.Mode = meta.S_IFSOCK | 0o644
		default:
			caseTag = "非法Mode超0o177777(ErrInvalidArgument)"
			params.Mode = 0x10000 | 0o600
		}

	case branch < 90:
		// 父节点非目录 (ErrNotDir)
		nonDirs := env.Model.ActiveNonDirs()
		params.Mode = 0o600
		params.Flags = meta.O_RDWR
		var validNonDir *formalInode
		for _, nd := range nonDirs {
			if nd.Nlink > 0 || env.Model.HasValidHandlesForIno(nd.Ino) {
				validNonDir = nd
				break
			}
		}
		if validNonDir != nil {
			caseTag = "父节点非目录(ErrNotDir)"
			params.ParentIno = validNonDir.Ino
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 425001)
		}

	default:
		// 孤儿父目录 (nlink==0 -> ErrNotFound)、不存在父节点 (ErrNotFound) 或非法 ParentIno / SessionID (ErrInvalidArgument)
		orphanDirs := findOrphanDirsInModel(env.Model)
		params.ParentIno = activeDirs[0].Ino
		params.Mode = 0o600
		params.Flags = meta.O_RDWR
		switch {
		case len(orphanDirs) > 0 && env.Rng.IntN(2) == 0:
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.ParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		case env.Rng.IntN(4) == 0:
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			params.ParentIno = 0
		case env.Rng.IntN(3) == 0:
			caseTag = "非法ParentIno超MaxInt64(ErrInvalidArgument)"
			params.ParentIno = uint64(math.MaxInt64) + 1
		case env.Rng.IntN(2) == 0:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		default:
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 425002)
		}
	}

	expectedMode, expectedGID, expectedHandleFlags, expectedErr := formalPredictTmpFile(env.Model, params)

	// 若预测成功，先记录调用前父目录的 GetAttr 快照，以严格验证 TmpFile 不修改父目录 mtime / ctime / nlink
	var parentBeforeAttr *meta.Inode
	if expectedErr == nil {
		var preErr error
		parentBeforeAttr, preErr = env.Mgr.GetAttr(ctx, params.ParentIno)
		require.NoErrorf(t, preErr, "TmpFile 前读取父目录 ino=%d 属性不应失败", params.ParentIno)
	}

	got, err := env.Mgr.TmpFile(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "TmpFile[%s] (parent=%d, mode=0o%o, flags=0x%x) 预期错误 %v，但实际成功: %+v",
			caseTag, params.ParentIno, params.Mode, params.Flags, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"TmpFile[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("TmpFile[%s](parent=%d, mode=0o%o, flags=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.ParentIno, params.Mode, params.Flags, expectedErr)
	}

	require.NoErrorf(t, err, "TmpFile[%s] (parent=%d, mode=0o%o, flags=0x%x) 预期成功，但返回错误: %v",
		caseTag, params.ParentIno, params.Mode, params.Flags, err)
	require.NotNil(t, got)
	require.NotNil(t, got.Inode)
	require.Greater(t, got.Fh, uint64(0), "TmpFile 分配的 Fh 必须 > 0")
	require.Equal(t, uint32(0), got.Inode.Attr.Nlink, "TmpFile 创建的匿名节点 nlink 必须为 0")
	require.Equal(t, uint64(0), got.Inode.Attr.ParentIno, "TmpFile 创建的匿名节点 parent_ino 必须为 0")
	require.Equal(t, uint64(0), got.Inode.Attr.Size, "TmpFile 创建的初始节点 size 必须为 0")

	// 核验父目录的 mtime、ctime 与 nlink 均未被修改
	parentAfterAttr, postErr := env.Mgr.GetAttr(ctx, params.ParentIno)
	require.NoErrorf(t, postErr, "TmpFile 后读取父目录 ino=%d 属性不应失败", params.ParentIno)
	require.Truef(t, parentBeforeAttr.Attr.Mtime.Equal(parentAfterAttr.Attr.Mtime),
		"TmpFile 严禁修改父目录 mtime (before=%v, after=%v)", parentBeforeAttr.Attr.Mtime, parentAfterAttr.Attr.Mtime)
	require.Truef(t, parentBeforeAttr.Attr.Ctime.Equal(parentAfterAttr.Attr.Ctime),
		"TmpFile 严禁修改父目录 ctime (before=%v, after=%v)", parentBeforeAttr.Attr.Ctime, parentAfterAttr.Attr.Ctime)
	require.Equalf(t, parentBeforeAttr.Attr.Nlink, parentAfterAttr.Attr.Nlink,
		"TmpFile 严禁修改父目录 nlink (before=%d, after=%d)", parentBeforeAttr.Attr.Nlink, parentAfterAttr.Attr.Nlink)

	// 核验 entries 底表中绝无指向该匿名节点的记录
	var entryCount int
	err = env.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM entries WHERE ino = $1`, int64(got.Inode.Ino)).Scan(&entryCount)
	require.NoError(t, err, "查询 entries 表核验 TmpFile 零目录项失败")
	require.Zero(t, entryCount, "TmpFile 严禁向 entries 表写入任何记录")

	parentModel := env.Model.Inodes[params.ParentIno]
	require.NotNil(t, parentModel)

	// 注意顺序：先同步可选会话续期（对应事务内 renewOptionalClientSession 在 CreateInode 之前执行），
	// 再登记 nlink=0 的新 TmpFile 节点与初始句柄，防止 modelSyncSessionRenewal 将尚未登记句柄的新节点误判为无引用孤儿
	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)

	newTmpModel := &formalInode{
		Ino:           got.Inode.Ino,
		Generation:    got.Inode.Generation,
		RootIno:       parentModel.RootIno,
		ParentIno:     0,
		Mode:          expectedMode,
		UID:           params.UID,
		GID:           expectedGID,
		Rdev:          0,
		Size:          0,
		Nlink:         0,
		Flags:         0,
		IsTmpFile:     true,
		TmpFileExcl:   (expectedHandleFlags & meta.O_EXCL) != 0,
		TmpFileLinked: false,
	}
	env.Model.RecordNewInode(newTmpModel)
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted {
		disk.UsedInodes++
	}

	env.Model.RecordNewHandle(&formalHandle{
		Fh:        got.Fh,
		Ino:       got.Inode.Ino,
		Flags:     expectedHandleFlags,
		SessionID: modelCloneSessionID(params.SessionID),
		IsDir:     false,
		Closed:    false,
	})

	assertFormalInodeMatchesEntity(t, newTmpModel, got.Inode, fmt.Sprintf("TmpFile[%s]返回值", caseTag))

	// 按 45% 概率为新创建的匿名 TmpFile 注入非零逻辑大小（保留 expectedMode 特权位），
	// 使匿名 TmpFile 在具名化前、具名化后以及未具名化直接 Release 物理销毁时均能高频检验 used_bytes 账本守恒
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
			resized, resizeErr := env.Mgr.SetAttr(ctx, newTmpModel.Ino, meta.SetAttrInput{
				Size: &seedSize,
				Mode: &modeKeep,
			})
			require.NoError(t, resizeErr, "TmpFile 后通过 SetAttr 初始化非零大小不应失败")
			require.Equal(t, seedSize, resized.Attr.Size)
			require.Equal(t, expectedMode, resized.Attr.Mode)
			newTmpModel.Size = seedSize
			disk.UsedBytes += seedSize
		}
	}

	return fmt.Sprintf("TmpFile[%s](parent=%d, mode=0o%o, flags=0x%x, sid=%s) -> 成功 (ino=%d, fh=%d, excl=%v, final_mode=0o%o, gid=%d, size=%d)",
		caseTag, params.ParentIno, params.Mode, params.Flags, modelFormatSessionID(params.SessionID),
		got.Inode.Ino, got.Fh, newTmpModel.TmpFileExcl, newTmpModel.Mode, newTmpModel.GID, newTmpModel.Size)
}

// formalStepTmpFileMaterializeOrReject 专门针对当前模型中的匿名 TmpFile 或存活文件执行高密度分流验证：
// - 若存在可具名化 TmpFile (nlink == 0 且句柄含 O_TMPFILE 不含 O_EXCL)：执行一次性 Link 具名化，或具名化后立即 Unlink 再尝试二次 Link 验证防复活；
// - 若存在 O_EXCL 独占 TmpFile 或已具名化后被 Unlink 的孤儿节点：调用 Link 验证必返回 ErrNotFound，或尝试通过 Open(..., O_TMPFILE) 伪造绕过验证必返回 ErrInvalidArgument；
// - 验证 Create(..., O_TMPFILE) 与 Open(..., O_TMPFILE) 强制拒绝并返回 ErrInvalidArgument。
func formalStepTmpFileMaterializeOrReject(
	ctx context.Context,
	t *testing.T,
	env *formalMainEnv,
	activeDirs []*formalInode,
) (string, bool) {
	t.Helper()

	var linkableTmpFiles []*formalInode
	var exclOrUnlinkedOrphans []*formalInode
	for _, node := range env.Model.ActiveRegFiles() {
		if node.Nlink == 0 {
			if env.Model.HasLinkableTmpFileHandleForIno(node.Ino) {
				linkableTmpFiles = append(linkableTmpFiles, node)
			} else {
				exclOrUnlinkedOrphans = append(exclOrUnlinkedOrphans, node)
			}
		}
	}

	subRoll := env.Rng.IntN(100)
	switch {
	case subRoll < 45 && len(linkableTmpFiles) > 0:
		// 对可具名化的非 O_EXCL TmpFile 执行首次 Link 具名化（并按 40% 概率紧接着 Unlink + 二次 Link 验证防二次复活）
		src := linkableTmpFiles[env.Rng.IntN(len(linkableTmpFiles))]
		var targetDir *formalInode
		for _, d := range activeDirs {
			if d.RootIno == src.RootIno {
				targetDir = d
				break
			}
		}
		if targetDir == nil {
			return "", false
		}

		entryName := env.UniqueEntryName("tmp_mat")
		linkParams := meta.LinkParams{
			Ino:          src.Ino,
			NewParentIno: targetDir.Ino,
			NewName:      []byte(entryName),
		}
		predSrc, isMat, predErr := formalPredictLink(env.Model, linkParams)
		require.NoError(t, predErr)
		require.True(t, isMat, "非 O_EXCL 的匿名 TmpFile 首次 Link 必须预测为具名化")
		require.Equal(t, src, predSrc)

		gotInode, err := env.Mgr.Link(ctx, linkParams)
		require.NoError(t, err, "非 O_EXCL TmpFile 首次 Link 具名化不应失败")
		require.NotNil(t, gotInode)
		require.Equal(t, uint32(1), gotInode.Attr.Nlink)

		env.Model.Entries[formalEntryKey{ParentIno: targetDir.Ino, Name: entryName}] = src.Ino
		src.Nlink = 1
		src.TmpFileLinked = true
		env.Model.ClearTmpFileFlagForIno(src.Ino)
		assertFormalInodeMatchesEntity(t, src, gotInode, "TmpFile首次具名化Link返回值")

		// 按 40% 概率在同一步内紧接着执行 Unlink 降回 nlink == 0，并立即验证二次 Link 复活必被拒绝 (ErrNotFound)
		if env.Rng.IntN(100) < 40 && env.Model.HasValidHandlesForIno(src.Ino) {
			err = env.Mgr.Unlink(ctx, targetDir.Ino, []byte(entryName))
			require.NoError(t, err, "具名化后的 TmpFile 执行 Unlink 不应失败")
			delete(env.Model.Entries, formalEntryKey{ParentIno: targetDir.Ino, Name: entryName})
			src.Nlink = 0
			reclaimed := env.Model.ReclaimOrphanInModel(src.Ino)
			require.False(t, reclaimed, "仍持有有效句柄时 Unlink 后不应物理回收")

			resurrectName := env.UniqueEntryName("tmp_resurrect_deny")
			_, errResurrect := env.Mgr.Link(ctx, meta.LinkParams{
				Ino:          src.Ino,
				NewParentIno: targetDir.Ino,
				NewName:      []byte(resurrectName),
			})
			require.ErrorIs(t, errResurrect, meta.ErrNotFound,
				"已具名化后被 Unlink 降回 nlink=0 的 TmpFile 严禁二次 Link 复活")

			return fmt.Sprintf("TmpFileOnceMaterializeThenUnlink[防二次复活](ino=%d, dir=%d, name=%q) -> 首次Link成功(nlink:0->1,清除O_TMPFILE) -> Unlink(nlink:1->0) -> 二次Link预期拒绝(ErrNotFound)",
				src.Ino, targetDir.Ino, entryName), true
		}

		return fmt.Sprintf("TmpFileOnceMaterialize[首次具名化](ino=%d, dir=%d, name=%q) -> 成功 (nlink:0->1, 已清除O_TMPFILE标志位)",
			src.Ino, targetDir.Ino, entryName), true

	case subRoll < 75 && len(exclOrUnlinkedOrphans) > 0:
		// 对 O_EXCL 独占 TmpFile 或已解链 nlink==0 孤儿节点调用 Link -> 必返回 ErrNotFound；
		// 同时尝试调用 Open(..., O_TMPFILE) 伪造可具名化句柄 -> 必返回 ErrInvalidArgument
		orphan := exclOrUnlinkedOrphans[env.Rng.IntN(len(exclOrUnlinkedOrphans))]
		var targetDir *formalInode
		for _, d := range activeDirs {
			if d.RootIno == orphan.RootIno {
				targetDir = d
				break
			}
		}
		if targetDir == nil {
			targetDir = activeDirs[0]
		}

		_, errForgeOpen := env.Mgr.Open(ctx, meta.OpenParams{
			Ino:   orphan.Ino,
			Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		require.ErrorIs(t, errForgeOpen, meta.ErrInvalidArgument, "Open 携带 O_TMPFILE 必须返回 ErrInvalidArgument")

		denyName := env.UniqueEntryName("excl_or_orphan_deny")
		_, errLink := env.Mgr.Link(ctx, meta.LinkParams{
			Ino:          orphan.Ino,
			NewParentIno: targetDir.Ino,
			NewName:      []byte(denyName),
		})
		require.ErrorIs(t, errLink, meta.ErrNotFound,
			"O_EXCL 独占 TmpFile 或已解链孤儿节点调用 Link 必须返回 ErrNotFound")

		tag := "普通或已解链孤儿防复活"
		if orphan.IsTmpFile && orphan.TmpFileExcl {
			tag = "O_EXCL独占TmpFile禁止具名化"
		} else if orphan.IsTmpFile && orphan.TmpFileLinked {
			tag = "已具名化后Unlink的TmpFile禁止二次复活"
		}
		return fmt.Sprintf("TmpFileRejectLink[%s](ino=%d, dir=%d, name=%q) -> Open(O_TMPFILE)拒绝(ErrInvalidArgument) & Link拒绝(ErrNotFound)",
			tag, orphan.Ino, targetDir.Ino, denyName), true

	default:
		// 显式验证 Create 与 Open 传入 O_TMPFILE 标志位时强制拒绝并返回 ErrInvalidArgument
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		forgedName := env.UniqueEntryName("forged_create_tmp")
		_, errCreate := env.Mgr.Create(ctx, meta.CreateParams{
			ParentIno: parent.Ino,
			Name:      []byte(forgedName),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
		})
		require.ErrorIs(t, errCreate, meta.ErrInvalidArgument, "Create 携带 O_TMPFILE 必须返回 ErrInvalidArgument")

		targetIno := parent.Ino
		if regFiles := env.Model.ActiveRegFiles(); len(regFiles) > 0 {
			targetIno = regFiles[env.Rng.IntN(len(regFiles))].Ino
		}
		_, errOpen := env.Mgr.Open(ctx, meta.OpenParams{
			Ino:   targetIno,
			Flags: meta.O_WRONLY | meta.O_TMPFILE,
		})
		require.ErrorIs(t, errOpen, meta.ErrInvalidArgument, "Open 携带 O_TMPFILE 必须返回 ErrInvalidArgument")

		return fmt.Sprintf("RejectForgedTmpFileFlag(parent=%d, open_ino=%d) -> Create与Open携带O_TMPFILE均预期拒绝(ErrInvalidArgument)",
			parent.Ino, targetIno), true
	}
}

// ============================================================================
// 3. T8 底表数学不变量审计：INV-6 (匿名临时文件一次性具名化)
// ============================================================================

// formalAuditINV6TmpFileMaterialization 直接扫描 PostgreSQL 底表 file_handles JOIN inodes，强制审计数学不变量 INV-6：
// 1. 任何 inodes.nlink > 0 的具名节点关联的所有 file_handles 中，(flags & O_TMPFILE) 恒为 0；
// 2. 任何携带 O_TMPFILE 标志位的 file_handles，其关联 inodes 必为常规文件 ((mode & S_IFMT) == S_IFREG)、nlink == 0 且 parent_ino == 0；
// 3. 任何携带 O_TMPFILE 标志位的 file_handles，在内存参考模型中必对应由 TmpFile 创建且尚未执行过 Link 具名化的节点 (IsTmpFile && !TmpFileLinked)；
// 4. 任何非 TmpFile 创建的节点（Create/Mknod/Mkdir/Symlink）或已完成过一次性 Link 具名化的 TmpFile 节点 (TmpFileLinked == true)，
//    其关联的所有底表与模型句柄中 (flags & O_TMPFILE) 恒为 0。
func formalAuditINV6TmpFileMaterialization(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
	for _, r := range env.Model.AllCreatedRootInos {
		rootInos = append(rootInos, int64(r))
	}

	rows, err := env.Pool.Query(ctx, `
		SELECT
			fh.fh,
			fh.ino,
			fh.flags,
			i.mode,
			i.nlink,
			i.parent_ino,
			i.root_ino
		FROM file_handles fh
		JOIN inodes i ON i.ino = fh.ino
		WHERE i.root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "INV-6 审计: 查询底表 file_handles JOIN inodes 失败")
	defer rows.Close()

	dbTmpFileHandleCountByIno := make(map[uint64]int)

	for rows.Next() {
		var fh, ino, parentIno, rootIno int64
		var flags, mode, nlink int32

		err := rows.Scan(&fh, &ino, &flags, &mode, &nlink, &parentIno, &rootIno)
		require.NoError(t, err, "INV-6 审计: 扫描 file_handles JOIN inodes 行失败")

		uFh := uint64(fh)
		uIno := uint64(ino)
		uFlags := uint32(flags)
		uMode := uint32(mode)
		hasTmpFlag := (uFlags & meta.O_TMPFILE) != 0

		// 断言 1：任何 inodes.nlink > 0 的节点关联的所有 file_handles 中，(flags & O_TMPFILE) 恒为 0
		if nlink > 0 {
			require.Falsef(t, hasTmpFlag,
				"INV-6 违例: 具名节点 ino=%d (nlink=%d, root_ino=%d) 的句柄 fh=%d 仍携带 O_TMPFILE 标志位 (flags=0x%x)",
				uIno, nlink, rootIno, uFh, uFlags)
		}

		// 断言 2：任何携带 O_TMPFILE 标志位的 file_handles，其关联 inodes 必为 S_IFREG、nlink == 0 且 parent_ino == 0
		if hasTmpFlag {
			dbTmpFileHandleCountByIno[uIno]++
			require.Equalf(t, uint32(meta.S_IFREG), uMode&meta.S_IFMT,
				"INV-6 违例: 携带 O_TMPFILE 的句柄 fh=%d 关联节点 ino=%d 不是常规文件 S_IFREG (mode=0o%o)",
				uFh, uIno, uMode)
			require.Zerof(t, nlink,
				"INV-6 违例: 携带 O_TMPFILE 的句柄 fh=%d 关联节点 ino=%d 的 nlink (%d) != 0",
				uFh, uIno, nlink)
			require.Zerof(t, parentIno,
				"INV-6 违例: 携带 O_TMPFILE 的句柄 fh=%d 关联节点 ino=%d 的 parent_ino (%d) != 0",
				uFh, uIno, parentIno)

			modelNode, exists := env.Model.Inodes[uIno]
			require.Truef(t, exists && !modelNode.Deleted,
				"INV-6 违例: 携带 O_TMPFILE 的句柄 fh=%d 关联节点 ino=%d 在内存模型中不存在或已删除", uFh, uIno)
			require.Truef(t, modelNode.IsTmpFile,
				"INV-6 违例: 节点 ino=%d 并非由 TmpFile 创建，但其底表句柄 fh=%d 非法携带 O_TMPFILE (flags=0x%x)",
				uIno, uFh, uFlags)
			require.Falsef(t, modelNode.TmpFileLinked,
				"INV-6 违例: TmpFile 节点 ino=%d 已完成过一次性 Link 具名化，但其底表句柄 fh=%d 仍残留 O_TMPFILE (flags=0x%x)",
				uIno, uFh, uFlags)
		}
	}
	require.NoError(t, rows.Err())

	// 断言 3：核对内存模型中所有已完成过具名化 (TmpFileLinked == true) 或非 TmpFile 创建的节点，在底表与模型中均无 O_TMPFILE 句柄
	for _, ino := range env.Model.AllCreatedInos {
		modelNode, ok := env.Model.Inodes[ino]
		if !ok || modelNode.Deleted {
			continue
		}
		if !modelNode.IsTmpFile || modelNode.TmpFileLinked {
			require.Zerof(t, dbTmpFileHandleCountByIno[ino],
				"INV-6 违例: 节点 ino=%d (IsTmpFile=%v, TmpFileLinked=%v, Nlink=%d) 在底表中存在 %d 个携带 O_TMPFILE 的句柄",
				ino, modelNode.IsTmpFile, modelNode.TmpFileLinked, modelNode.Nlink, dbTmpFileHandleCountByIno[ino])
		}
		if modelNode.IsTmpFile && !modelNode.TmpFileLinked {
			require.Zerof(t, modelNode.Nlink,
				"INV-6 违例: 未执行过 Link 具名化的匿名 TmpFile 节点 ino=%d 的 Nlink (%d) 必须为 0",
				ino, modelNode.Nlink)
		}
	}

	for fh, h := range env.Model.Handles {
		if h.Closed {
			continue
		}
		if modelNode, ok := env.Model.Inodes[h.Ino]; ok && !modelNode.Deleted {
			if modelNode.Nlink > 0 || !modelNode.IsTmpFile || modelNode.TmpFileLinked {
				require.Zerof(t, h.Flags&meta.O_TMPFILE,
					"INV-6 违例: 内存模型句柄 fh=%d (ino=%d, nlink=%d, isTmp=%v, linked=%v) 非法残留 O_TMPFILE 标志位",
					fh, h.Ino, modelNode.Nlink, modelNode.IsTmpFile, modelNode.TmpFileLinked)
			}
		}
	}
}

// ============================================================================
// 4. T8 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T8_TmpFile 验证 T8 主状态机 TmpFile 匿名临时文件创建、父目录时间戳不刷新、
// 非 O_EXCL 一次性 Link 具名化与防二次复活、O_EXCL 永久拒绝 Link、Create/Open 拒绝 O_TMPFILE 以及底表 INV-6 审计。
func TestFormalMain_T8_TmpFile(t *testing.T) {
	t.Run("确定性边界_TmpFile全参数矩阵与S_ISGID继承_父目录时间戳零变动_非O_EXCL一次性具名化与防二次复活_O_EXCL永久拒绝Link_Create与Open拒绝O_TMPFILE_INV-6全量验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 50,
			Seed:       0x20260425,
			Rng:        newFormalRNG(0x20260425),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t8-det",
			LeaseTTL:   meta.DefaultSessionLeaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		runFullProbesAndAuditors := func() {
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
			formalAuditINV6TmpFileMaterialization(ctx, t, env)
		}
		auditCalls := 0
		runAllProbesAndAuditors := func() {
			auditCalls++
			if !isFormalFixedSeedReplay() && auditCalls > 1 {
				return
			}
			runFullProbesAndAuditors()
		}
		defer func() {
			if !t.Failed() {
				runFullProbesAndAuditors()
			}
		}()

		// 1. 创建 2 块独立虚拟磁盘 disk1 与 disk2，并在 disk1 下创建带 S_ISGID 的子目录 sgid_dir (GID=7777) 与普通文件 reg_parent
		formalCreateValidDiskStep(ctx, t, env)
		formalCreateValidDiskStep(ctx, t, env)
		disks := env.Model.ActiveDisks()
		require.Len(t, disks, 2)
		disk1, disk2 := disks[0], disks[1]
		root1, root2 := disk1.RootIno, disk2.RootIno
		rootDir1 := env.Model.Inodes[root1]

		sgidDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: root1,
			Name:      []byte("sgid_dir"),
			Mode:      meta.S_ISGID | 0o775,
			Umask:     0o022,
			UID:       1000,
			GID:       7777,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: sgidDir.Ino, Generation: sgidDir.Generation, RootIno: root1, ParentIno: root1,
			Mode: sgidDir.Attr.Mode, UID: 1000, GID: 7777, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "sgid_dir"}] = sgidDir.Ino
		rootDir1.Nlink++
		disk1.UsedInodes++

		regNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: root1, Name: []byte("reg_file"), Mode: meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: regNode.Ino, Generation: regNode.Generation, RootIno: root1, ParentIno: 0,
			Mode: regNode.Attr.Mode, Size: 0, Nlink: 1,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "reg_file"}] = regNode.Ino
		disk1.UsedInodes++
		runAllProbesAndAuditors()

		// 2. 验证 TmpFile 静态入参校验与父目录状态错误码全矩阵
		zeroSID := uint64(0)
		overflowSID := uint64(math.MaxInt64) + 1
		invalidTmpCases := []struct {
			name        string
			params      meta.TmpFileParams
			expectedErr error
		}{
			{
				name:        "ParentIno==0",
				params:      meta.TmpFileParams{ParentIno: 0, Mode: 0o600, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "ParentIno>MaxInt64",
				params:      meta.TmpFileParams{ParentIno: uint64(math.MaxInt64) + 1, Mode: 0o600, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "SessionID==0",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &zeroSID},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "SessionID>MaxInt64",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0o600, Flags: meta.O_RDWR, SessionID: &overflowSID},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Flags为O_RDONLY",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0o600, Flags: meta.O_RDONLY},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Flags为O_TMPFILE|O_RDONLY",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0o600, Flags: meta.O_TMPFILE | meta.O_RDONLY},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Flags为O_ACCMODE(3)",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0o600, Flags: meta.O_ACCMODE},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode>0o177777",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: 0x10000 | 0o600, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFDIR",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFDIR | 0o755, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFLNK",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFLNK | 0o777, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFIFO",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFIFO | 0o644, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFCHR",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFCHR | 0o644, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFBLK",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFBLK | 0o644, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "Mode为S_IFSOCK",
				params:      meta.TmpFileParams{ParentIno: root1, Mode: meta.S_IFSOCK | 0o644, Flags: meta.O_RDWR},
				expectedErr: meta.ErrInvalidArgument,
			},
			{
				name:        "父节点不存在(ErrNotFound)",
				params:      meta.TmpFileParams{ParentIno: uint64(math.MaxInt64 - 999), Mode: 0o600, Flags: meta.O_RDWR},
				expectedErr: meta.ErrNotFound,
			},
			{
				name:        "父节点为非目录(ErrNotDir)",
				params:      meta.TmpFileParams{ParentIno: regNode.Ino, Mode: 0o600, Flags: meta.O_RDWR},
				expectedErr: meta.ErrNotDir,
			},
		}
		for _, tc := range invalidTmpCases {
			_, _, _, predErr := formalPredictTmpFile(env.Model, tc.params)
			require.ErrorIsf(t, predErr, tc.expectedErr, "用例 %s 预测错误码不匹配", tc.name)
			res, err := mgr.TmpFile(ctx, tc.params)
			require.ErrorIsf(t, err, tc.expectedErr, "用例 %s 实际错误码不匹配", tc.name)
			require.Nil(t, res)
		}

		// 构造 rmdir-whilst-open 孤儿父目录 (nlink == 0)，验证在其下调用 TmpFile 返回 ErrNotFound
		orphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: root1, Name: []byte("orphan_parent"), Mode: 0o755})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: orphanDir.Ino, Generation: orphanDir.Generation, RootIno: root1, ParentIno: root1,
			Mode: orphanDir.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "orphan_parent"}] = orphanDir.Ino
		rootDir1.Nlink++
		disk1.UsedInodes++

		openOrphanDir, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: orphanDir.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{Fh: openOrphanDir.Fh, Ino: orphanDir.Ino, Flags: meta.O_RDONLY, IsDir: true})

		require.NoError(t, mgr.Rmdir(ctx, root1, []byte("orphan_parent")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "orphan_parent"})
		rootDir1.Nlink--
		env.Model.Inodes[orphanDir.Ino].Nlink = 0

		_, err = mgr.TmpFile(ctx, meta.TmpFileParams{ParentIno: orphanDir.Ino, Mode: 0o600, Flags: meta.O_RDWR})
		require.ErrorIs(t, err, meta.ErrNotFound, "孤儿父目录 (nlink==0) 下调用 TmpFile 必须返回 ErrNotFound")

		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: orphanDir.Ino, Fh: openOrphanDir.Fh}))
		env.Model.Handles[openOrphanDir.Fh].Closed = true
		env.Model.ClosedHandles[openOrphanDir.Fh] = env.Model.Handles[openOrphanDir.Fh]
		delete(env.Model.Handles, openOrphanDir.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphanDir.Ino))
		runAllProbesAndAuditors()

		// 3. 验证 TmpFile 正常创建、Umask 与特权位掩码、父目录 S_ISGID 属组继承、不写 entries 表、不刷新父目录 mtime/ctime/nlink、
		//    以及未具名化直接 Release 物理销毁与配额退还
		parentBefore, err := mgr.GetAttr(ctx, sgidDir.Ino)
		require.NoError(t, err)
		usedInodesBeforeTmp1 := disk1.UsedInodes

		time.Sleep(5 * time.Millisecond)

		tmp1Res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: sgidDir.Ino,
			Mode:      meta.S_ISUID | meta.S_ISGID | meta.S_ISVTX | 0o775,
			Umask:     0o022,
			UID:       1001,
			GID:       2002,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		require.NotNil(t, tmp1Res)
		require.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|meta.S_ISVTX|0o755), tmp1Res.Inode.Attr.Mode)
		require.Equal(t, uint32(7777), tmp1Res.Inode.Attr.GID, "TmpFile 在 S_ISGID 父目录下必须继承父目录 GID=7777")
		require.Equal(t, uint32(0), tmp1Res.Inode.Attr.Nlink)
		require.Equal(t, uint64(0), tmp1Res.Inode.Attr.ParentIno)
		require.Equal(t, uint64(0), tmp1Res.Inode.Attr.Size)

		// 验证父目录 mtime、ctime、nlink 丝毫未变
		parentAfter, err := mgr.GetAttr(ctx, sgidDir.Ino)
		require.NoError(t, err)
		require.True(t, parentBefore.Attr.Mtime.Equal(parentAfter.Attr.Mtime), "TmpFile 不得刷新父目录 mtime")
		require.True(t, parentBefore.Attr.Ctime.Equal(parentAfter.Attr.Ctime), "TmpFile 不得刷新父目录 ctime")
		require.Equal(t, parentBefore.Attr.Nlink, parentAfter.Attr.Nlink, "TmpFile 不得改变父目录 nlink")

		tmp1Model := &formalInode{
			Ino: tmp1Res.Inode.Ino, Generation: tmp1Res.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: tmp1Res.Inode.Attr.Mode, UID: 1001, GID: 7777, Size: 0, Nlink: 0, IsTmpFile: true, TmpFileExcl: false,
		}
		env.Model.RecordNewInode(tmp1Model)
		disk1.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: tmp1Res.Fh, Ino: tmp1Model.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		require.Equal(t, usedInodesBeforeTmp1+1, disk1.UsedInodes)
		runAllProbesAndAuditors()

		// 为未具名化的 tmp1 写入 8192 字节大小，随后直接 Release 关闭唯一句柄 -> 当场物理销毁并退还 used_inodes - 1 与 used_bytes - 8192
		tmp1Size := uint64(8192)
		keepMode := tmp1Model.Mode
		_, err = mgr.SetAttr(ctx, tmp1Model.Ino, meta.SetAttrInput{Size: &tmp1Size, Mode: &keepMode})
		require.NoError(t, err)
		tmp1Model.Size = tmp1Size
		disk1.UsedBytes += tmp1Size
		runAllProbesAndAuditors()

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: tmp1Model.Ino, Fh: tmp1Res.Fh}))
		env.Model.Handles[tmp1Res.Fh].Closed = true
		env.Model.ClosedHandles[tmp1Res.Fh] = env.Model.Handles[tmp1Res.Fh]
		delete(env.Model.Handles, tmp1Res.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(tmp1Model.Ino), "未具名化的 TmpFile 关闭最后一个句柄必须当场物理销毁")
		require.Equal(t, usedInodesBeforeTmp1, disk1.UsedInodes)
		require.Equal(t, uint64(0), disk1.UsedBytes)
		_, err = mgr.GetAttr(ctx, tmp1Model.Ino)
		require.ErrorIs(t, err, meta.ErrNotFound)
		runAllProbesAndAuditors()

		// 4. 验证非 O_EXCL 的 TmpFile 一次性 Link 具名化、清除所有关联句柄的 O_TMPFILE 标志位、以及 Unlink 后严禁二次复活
		sid1 := uint64(8425001)
		tmp2Res, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: sgidDir.Ino,
			Mode:      meta.S_IFREG | 0o640,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_WRONLY,
			SessionID: &sid1,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &sid1, env.LeaseTTL)
		tmp2Model := &formalInode{
			Ino: tmp2Res.Inode.Ino, Generation: tmp2Res.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: tmp2Res.Inode.Attr.Mode, UID: 1001, GID: 7777, Size: 0, Nlink: 0, IsTmpFile: true,
		}
		env.Model.RecordNewInode(tmp2Model)
		disk1.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: tmp2Res.Fh, Ino: tmp2Model.Ino, Flags: meta.O_WRONLY | meta.O_TMPFILE, SessionID: modelCloneSessionID(&sid1),
		})

		// 在具名化前通过 Open 为 tmp2 分配第二个句柄 fh2，并通过 SetAttr 设置 4096 字节大小
		openTmp2Res, err := mgr.Open(ctx, meta.OpenParams{Ino: tmp2Model.Ino, Flags: meta.O_RDWR})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openTmp2Res.Fh, Ino: tmp2Model.Ino, Flags: meta.O_RDWR,
		})
		tmp2Size := uint64(4096)
		_, err = mgr.SetAttr(ctx, tmp2Model.Ino, meta.SetAttrInput{Size: &tmp2Size})
		require.NoError(t, err)
		tmp2Model.Size = tmp2Size
		disk1.UsedBytes += tmp2Size
		runAllProbesAndAuditors()

		// 4.1 跨盘 Link 具名化必须被拦截并返回 ErrCrossDevice (EXDEV)，且 O_TMPFILE 保持未清除
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: tmp2Model.Ino, NewParentIno: root2, NewName: []byte("cross_disk_tmp")})
		require.ErrorIs(t, err, meta.ErrCrossDevice)
		runAllProbesAndAuditors()

		// 4.2 同盘首次 Link 具名化成功：nlink 0 -> 1，原子清除该 Inode 关联句柄中的 O_TMPFILE 位
		linkedTmp2, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          tmp2Model.Ino,
			NewParentIno: sgidDir.Ino,
			NewName:      []byte("materialized_tmp2.txt"),
		})
		require.NoError(t, err)
		require.Equal(t, uint32(1), linkedTmp2.Attr.Nlink)
		env.Model.Entries[formalEntryKey{ParentIno: sgidDir.Ino, Name: "materialized_tmp2.txt"}] = tmp2Model.Ino
		tmp2Model.Nlink = 1
		tmp2Model.TmpFileLinked = true
		env.Model.ClearTmpFileFlagForIno(tmp2Model.Ino)
		runAllProbesAndAuditors()

		// 4.3 具名化后（nlink == 1）可像普通文件一样继续创建第 2 条硬链接 (nlink: 1 -> 2)
		linkedTmp2Second, err := mgr.Link(ctx, meta.LinkParams{
			Ino:          tmp2Model.Ino,
			NewParentIno: root1,
			NewName:      []byte("materialized_tmp2_hl2.txt"),
		})
		require.NoError(t, err)
		require.Equal(t, uint32(2), linkedTmp2Second.Attr.Nlink)
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "materialized_tmp2_hl2.txt"}] = tmp2Model.Ino
		tmp2Model.Nlink = 2
		runAllProbesAndAuditors()

		// 4.4 依次 Unlink 两条硬链接使其降回 nlink == 0（此时仍持有 tmp2Res.Fh 与 openTmp2Res.Fh 两个活跃句柄）
		require.NoError(t, mgr.Unlink(ctx, root1, []byte("materialized_tmp2_hl2.txt")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "materialized_tmp2_hl2.txt"})
		tmp2Model.Nlink = 1

		require.NoError(t, mgr.Unlink(ctx, sgidDir.Ino, []byte("materialized_tmp2.txt")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: sgidDir.Ino, Name: "materialized_tmp2.txt"})
		tmp2Model.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(tmp2Model.Ino))
		runAllProbesAndAuditors()

		// 4.5 尝试对已具名化又被 Unlink 至 nlink == 0 的节点再次调用 Link -> 必须返回 ErrNotFound (ENOENT)，严禁二次复活！
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          tmp2Model.Ino,
			NewParentIno: sgidDir.Ino,
			NewName:      []byte("resurrect_tmp2_forbidden.txt"),
		})
		require.ErrorIs(t, err, meta.ErrNotFound, "具名化后被 Unlink 降回 nlink=0 的节点严禁二次 Link 复活")

		// 4.6 关闭两个句柄 -> 触发物理销毁并退还 4096 字节与 1 个 Inode 配额
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: tmp2Model.Ino, Fh: tmp2Res.Fh, SessionID: &sid1}))
		env.Model.Handles[tmp2Res.Fh].Closed = true
		env.Model.ClosedHandles[tmp2Res.Fh] = env.Model.Handles[tmp2Res.Fh]
		delete(env.Model.Handles, tmp2Res.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(tmp2Model.Ino))

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: tmp2Model.Ino, Fh: openTmp2Res.Fh}))
		env.Model.Handles[openTmp2Res.Fh].Closed = true
		env.Model.ClosedHandles[openTmp2Res.Fh] = env.Model.Handles[openTmp2Res.Fh]
		delete(env.Model.Handles, openTmp2Res.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(tmp2Model.Ino))
		require.Equal(t, uint64(0), disk1.UsedBytes)
		runAllProbesAndAuditors()

		// 5. 验证携带 O_EXCL 的独占 TmpFile 永久拒绝 Link（含阻止通过二次 Open 绕过 O_EXCL）
		exclRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: sgidDir.Ino,
			Mode:      0o600,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_RDWR | meta.O_EXCL,
		})
		require.NoError(t, err)
		exclModel := &formalInode{
			Ino: exclRes.Inode.Ino, Generation: exclRes.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: exclRes.Inode.Attr.Mode, UID: 1001, GID: 7777, Size: 0, Nlink: 0, IsTmpFile: true, TmpFileExcl: true,
		}
		env.Model.RecordNewInode(exclModel)
		disk1.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: exclRes.Fh, Ino: exclModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE | meta.O_EXCL,
		})

		// 5.1 直接调用 Link 具名化 -> 必须返回 ErrNotFound
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          exclModel.Ino,
			NewParentIno: sgidDir.Ino,
			NewName:      []byte("excl_must_fail_1.txt"),
		})
		require.ErrorIs(t, err, meta.ErrNotFound, "携带 O_EXCL 的独占 TmpFile 必须拒绝 Link")

		// 5.2 尝试通过 Open(..., O_RDWR|O_TMPFILE) 伪造不含 O_EXCL 的 O_TMPFILE 句柄 -> 必须返回 ErrInvalidArgument
		_, err = mgr.Open(ctx, meta.OpenParams{
			Ino:   exclModel.Ino,
			Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument, "Open 携带 O_TMPFILE 必须返回 ErrInvalidArgument")

		// 5.3 调用普通 Open(..., O_RDWR) 获取第 2 个句柄，再次尝试 Link -> 仍必须返回 ErrNotFound
		openExclLegit, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   exclModel.Ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openExclLegit.Fh, Ino: exclModel.Ino, Flags: meta.O_RDWR,
		})
		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          exclModel.Ino,
			NewParentIno: sgidDir.Ino,
			NewName:      []byte("excl_must_fail_2.txt"),
		})
		require.ErrorIs(t, err, meta.ErrNotFound, "二次普通 Open 后 O_EXCL TmpFile 依然严禁 Link 具名化")
		runAllProbesAndAuditors()

		// 释放两个句柄物理回收 exclModel
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: exclModel.Ino, Fh: exclRes.Fh}))
		env.Model.Handles[exclRes.Fh].Closed = true
		env.Model.ClosedHandles[exclRes.Fh] = env.Model.Handles[exclRes.Fh]
		delete(env.Model.Handles, exclRes.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(exclModel.Ino))

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: exclModel.Ino, Fh: openExclLegit.Fh}))
		env.Model.Handles[openExclLegit.Fh].Closed = true
		env.Model.ClosedHandles[openExclLegit.Fh] = env.Model.Handles[openExclLegit.Fh]
		delete(env.Model.Handles, openExclLegit.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(exclModel.Ino))
		runAllProbesAndAuditors()

		// 6. 验证 Create 与 Open 强制拒绝 O_TMPFILE 标志位，且普通 Create 文件 Unlink 后严禁通过 Link 复活
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("forged_create.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		normCreateRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("norm_file.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.NoError(t, err)
		normModel := &formalInode{
			Ino: normCreateRes.Inode.Ino, Generation: normCreateRes.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: normCreateRes.Inode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(normModel)
		env.Model.Entries[formalEntryKey{ParentIno: sgidDir.Ino, Name: "norm_file.txt"}] = normModel.Ino
		disk1.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: normCreateRes.Fh, Ino: normModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
		})

		_, err = mgr.Open(ctx, meta.OpenParams{Ino: normModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		require.NoError(t, mgr.Unlink(ctx, sgidDir.Ino, []byte("norm_file.txt")))
		delete(env.Model.Entries, formalEntryKey{ParentIno: sgidDir.Ino, Name: "norm_file.txt"})
		normModel.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(normModel.Ino))

		_, err = mgr.Link(ctx, meta.LinkParams{
			Ino:          normModel.Ino,
			NewParentIno: sgidDir.Ino,
			NewName:      []byte("norm_resurrect_deny.txt"),
		})
		require.ErrorIs(t, err, meta.ErrNotFound, "普通被 Unlink 的孤儿文件严禁通过 Link 复活")

		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: normModel.Ino, Fh: normCreateRes.Fh}))
		env.Model.Handles[normCreateRes.Fh].Closed = true
		env.Model.ClosedHandles[normCreateRes.Fh] = env.Model.Handles[normCreateRes.Fh]
		delete(env.Model.Handles, normCreateRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(normModel.Ino))
		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_TmpFile匿名创建_一次性Link具名化与防二次复活_O_EXCL独占隔离及底表INV-6全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			InitHook: func(ctx context.Context, t *testing.T, env *formalMainEnv) {
				env.StepIndex = 0
				d1 := formalCreateValidDiskStep(ctx, t, env)
				d2 := formalCreateValidDiskStep(ctx, t, env)
				env.AppendTrace(0, fmt.Sprintf("[初始双盘] %s; %s", d1, d2))
			},
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 6, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 6, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 10, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 12, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 8, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 8, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 6, Fn: formalActionSymlink},
				{Name: "Create", Weight: 18, Fn: formalActionCreate},
				{Name: "TmpFile", Weight: 36, Fn: formalActionTmpFile},
				{Name: "Link", Weight: 32, Fn: formalActionLink},
				{Name: "Unlink", Weight: 26, Fn: formalActionUnlink},
				{Name: "Open", Weight: 22, Fn: formalActionOpen},
				{Name: "Release", Weight: 26, Fn: formalActionRelease},
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
				{Name: "INV-6_TmpFileMaterialization", Fn: formalAuditINV6TmpFileMaterialization},
			},
		})
	})
}
