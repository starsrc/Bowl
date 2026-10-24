package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T2 核心目录拓扑动作：Mkdir、Rmdir
	registerFormalAction("Mkdir", 30, formalActionMkdir)
	registerFormalAction("Rmdir", 25, formalActionRmdir)

	// 注册 T2 核心只读查账探针：Lookup、GetAttr
	registerFormalProbe("Lookup", formalProbeLookup)
	registerFormalProbe("GetAttr", formalProbeGetAttr)

	// 注册 T2 底表数学不变量审计器：INV-1 (目录树无环与父子双向一致)、INV-2 (目录硬链接计数守恒)
	registerFormalAuditor("INV-1_DirTopology", formalAuditINV1DirTopology)
	registerFormalAuditor("INV-2_DirNlink", formalAuditINV2DirNlink)
}

// ============================================================================
// 1. 通用目录项名称与属性参考模型辅助函数
// ============================================================================

// modelValidateEntryName 对应被测代码中 validateEntryName 的静态校验规则（不允许空名）。
func modelValidateEntryName(name []byte) error {
	if len(name) > meta.MaxFileNameBytes {
		return meta.ErrNameTooLong
	}
	if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return meta.ErrInvalidArgument
	}
	return nil
}

// modelValidateEntryNameAllowEmpty 对应被测代码中 validateEntryNameAllowEmpty 的静态校验规则（允许空名进入事务）。
func modelValidateEntryNameAllowEmpty(name []byte) error {
	if len(name) > meta.MaxFileNameBytes {
		return meta.ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return meta.ErrInvalidArgument
	}
	return nil
}

// modelIsDotOrDotDot 判断目录项名称是否为 "." 或 ".."。
func modelIsDotOrDotDot(name []byte) bool {
	return bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte(".."))
}

// assertFormalInodeMatchesEntity 核对公开接口返回的 *meta.Inode 与内存参考模型 *formalInode 的全部关键属性。
func assertFormalInodeMatchesEntity(t *testing.T, expected *formalInode, got *meta.Inode, contextMsg string) {
	t.Helper()
	require.NotNilf(t, got, "%s: 返回的 Inode 不应为 nil", contextMsg)
	assert.Equalf(t, expected.Ino, got.Ino, "%s: Ino 不一致", contextMsg)
	assert.Equalf(t, expected.Mode, got.Attr.Mode, "%s: Mode 不一致 (expected=0o%o, got=0o%o)",
		contextMsg, expected.Mode, got.Attr.Mode)
	assert.Equalf(t, expected.UID, got.Attr.UID, "%s: UID 不一致", contextMsg)
	assert.Equalf(t, expected.GID, got.Attr.GID, "%s: GID 不一致", contextMsg)
	assert.Equalf(t, expected.ExpectedAttrSize(), got.Attr.Size, "%s: Size 不一致", contextMsg)
	assert.Equalf(t, expected.Nlink, got.Attr.Nlink, "%s: Nlink 不一致", contextMsg)
	assert.Equalf(t, expected.ParentIno, got.Attr.ParentIno, "%s: ParentIno 不一致", contextMsg)
	assert.Equalf(t, expected.ExpectedAttrRdev(), got.Attr.Rdev, "%s: Rdev 不一致", contextMsg)
	assert.Equalf(t, meta.DefaultBlockSize, got.Attr.Blksize, "%s: Blksize 不一致", contextMsg)
	assert.Equalf(t, expected.ExpectedAttrBlocks(), got.Attr.Blocks, "%s: Blocks 不一致", contextMsg)
	assert.Equalf(t, expected.Flags, got.Attr.Flags, "%s: Flags 不一致", contextMsg)
}

// findOrphanDirsInModel 返回内存模型中所有 nlink == 0 且尚未物理删除的孤儿目录。
func findOrphanDirsInModel(m *formalMainModel) []*formalInode {
	var res []*formalInode
	for _, ino := range m.AllCreatedInos {
		if inode, ok := m.Inodes[ino]; ok && !inode.Deleted && inode.IsDir() && inode.Nlink == 0 {
			res = append(res, inode)
		}
	}
	return res
}

// ============================================================================
// 2. T2 动作实现：Mkdir 与 Rmdir 状态生成与逐操作对拍
// ============================================================================

// formalPredictMkdir 根据内存参考模型预测 Mkdir 的错误码与目标 Mode/GID。
func formalPredictMkdir(m *formalMainModel, params meta.MkdirParams) (targetMode uint32, targetGID uint32, expectedErr error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return 0, 0, meta.ErrInvalidArgument
	}
	if err := modelValidateEntryName(params.Name); err != nil {
		return 0, 0, err
	}
	if params.Mode > 65535 {
		return 0, 0, meta.ErrInvalidArgument
	}
	modeType := params.Mode & meta.S_IFMT
	if modeType != 0 && modeType != meta.S_IFDIR {
		return 0, 0, meta.ErrInvalidArgument
	}

	parent, err := m.validateParentDirForCreate(params.ParentIno, params.Name)
	if err != nil {
		return 0, 0, err
	}
	if parent.Nlink >= meta.MaxNlink {
		return 0, 0, meta.ErrTooManyLinks
	}

	targetGID = params.GID
	targetMode = meta.S_IFDIR | ((params.Mode & meta.PermMask) &^ (params.Umask & meta.PermMask))
	if (parent.Mode & meta.S_ISGID) != 0 {
		targetGID = parent.GID
		targetMode |= meta.S_ISGID
	}
	return targetMode, targetGID, nil
}

// formalActionMkdir 在状态机中随机生成合法嵌套创建及各类边界异常的 Mkdir 请求并与参考模型对拍。
func formalActionMkdir(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	branch := env.Rng.IntN(100)
	var params meta.MkdirParams
	var caseTag string

	// 预设常见 UID / GID / Umask 组合
	uids := []uint32{0, 1000, 1001, 2002}
	gids := []uint32{0, 1000, 2000, 3003}
	umasks := []uint32{0, 0o022, 0o027, 0o077}

	params.UID = uids[env.Rng.IntN(len(uids))]
	params.GID = gids[env.Rng.IntN(len(gids))]
	params.Umask = umasks[env.Rng.IntN(len(umasks))]

	switch {
	case branch < 45:
		// 正常嵌套创建子目录（覆盖普通权限、带 S_ISGID 以供后续子目录继承、带 S_ISVTX 粘滞位、显式 S_IFDIR）
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName("dir"))

		modeRoll := env.Rng.IntN(5)
		switch modeRoll {
		case 0:
			caseTag = "正常创建(含S_ISGID位)"
			params.Mode = meta.S_ISGID | 0o775
			params.Umask = 0o022
		case 1:
			caseTag = "正常创建(含S_ISVTX粘滞位与Umask)"
			params.Mode = meta.S_ISVTX | 0o777
			params.Umask = 0o027
		case 2:
			caseTag = "正常创建(显式S_IFDIR)"
			params.Mode = meta.S_IFDIR | 0o755
		case 3:
			caseTag = "正常创建(父含S_ISGID继承验证)"
			// 优先挑选带 S_ISGID 的存活父目录以高频触发 SGID 继承分支
			for _, d := range activeDirs {
				if (d.Mode & meta.S_ISGID) != 0 {
					params.ParentIno = d.Ino
					break
				}
			}
			params.Mode = 0o750
		default:
			caseTag = "正常创建(标准权限)"
			params.Mode = 0o755
		}

	case branch < 57:
		// 同名冲突 (ErrAlreadyExists)
		entries := env.Model.SortedEntries()
		if len(entries) > 0 {
			chosen := entries[env.Rng.IntN(len(entries))]
			caseTag = "同名目录项冲突(ErrAlreadyExists)"
			params.ParentIno = chosen.ParentIno
			params.Name = []byte(chosen.Name)
			params.Mode = 0o755
		} else {
			caseTag = "特殊名称(.)冲突"
			params.ParentIno = activeDirs[0].Ino
			params.Name = []byte(".")
			params.Mode = 0o755
		}

	case branch < 65:
		// 特殊名称 "." 或 ".."
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = 0o755
		if env.Rng.IntN(2) == 0 {
			caseTag = "特殊名称(.)"
			params.Name = []byte(".")
		} else {
			caseTag = "特殊名称(..)"
			params.Name = []byte("..")
		}

	case branch < 74:
		// 非法名称：空名、含 '/'、含 '\x00' 或超长 (>255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = 0o755
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "空名称(ErrInvalidArgument)"
			params.Name = []byte("")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			params.Name = []byte("bad/dir")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			params.Name = []byte("bad\x00dir")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			params.Name = []byte(strings.Repeat("n", meta.MaxFileNameBytes+1+env.Rng.IntN(16)))
		}

	case branch < 82:
		// 父节点为非目录 (ErrNotDir)
		nonDirs := env.Model.ActiveNonDirs()
		if len(nonDirs) > 0 {
			target := nonDirs[env.Rng.IntN(len(nonDirs))]
			caseTag = "父节点非目录(ErrNotDir)"
			params.ParentIno = target.Ino
			params.Name = []byte(env.UniqueEntryName("under_file"))
			params.Mode = 0o755
		} else {
			caseTag = "不存在的父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 123456)
			params.Name = []byte(env.UniqueEntryName("ghost_sub"))
			params.Mode = 0o755
		}

	case branch < 91:
		// 父节点不存在、已物理删除、或为 nlink==0 的孤儿目录 (ErrNotFound)
		orphanDirs := findOrphanDirsInModel(env.Model)
		if len(orphanDirs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.ParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		} else if delIno, ok := env.Model.FirstDeletedIno(); ok && env.Rng.IntN(2) == 0 {
			caseTag = "已删除父节点(ErrNotFound)"
			params.ParentIno = delIno
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 654321)
		}
		params.Name = []byte(env.UniqueEntryName("orphan_child"))
		params.Mode = 0o755

	default:
		// 非法 ParentIno 或非法 Mode 类型位
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.Name = []byte(env.UniqueEntryName("bad_param"))
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			params.ParentIno = 0
			params.Mode = 0o755
		case 1:
			caseTag = "非法Mode带S_IFREG(ErrInvalidArgument)"
			params.ParentIno = parent.Ino
			params.Mode = meta.S_IFREG | 0o755
		default:
			caseTag = "非法Mode超65535(ErrInvalidArgument)"
			params.ParentIno = parent.Ino
			params.Mode = 70000
		}
	}

	expectedMode, expectedGID, expectedErr := formalPredictMkdir(env.Model, params)
	got, err := env.Mgr.Mkdir(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Mkdir[%s] (parent=%d, name=%q, mode=0o%o) 预期错误 %v，但实际成功: %+v",
			caseTag, params.ParentIno, string(params.Name), params.Mode, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Mkdir[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Mkdir[%s](parent=%d, name=%q, mode=0o%o) -> 预期拒绝 (%v)",
			caseTag, params.ParentIno, string(params.Name), params.Mode, expectedErr)
	}

	require.NoErrorf(t, err, "Mkdir[%s] (parent=%d, name=%q) 预期成功，但返回错误: %v",
		caseTag, params.ParentIno, string(params.Name), err)
	require.NotNil(t, got)

	parentModel := env.Model.Inodes[params.ParentIno]
	require.NotNil(t, parentModel)

	// 同步更新内存参考模型
	newDirModel := &formalInode{
		Ino:        got.Ino,
		Generation: got.Generation,
		RootIno:    parentModel.RootIno,
		ParentIno:  params.ParentIno,
		Mode:       expectedMode,
		UID:        params.UID,
		GID:        expectedGID,
		Rdev:       0,
		Size:       meta.DefaultDirSize,
		Nlink:      2,
		Flags:      0,
	}
	env.Model.RecordNewInode(newDirModel)
	env.Model.Entries[formalEntryKey{ParentIno: params.ParentIno, Name: string(params.Name)}] = got.Ino
	parentModel.Nlink++
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted {
		disk.UsedInodes++
	}

	// 核对 Mkdir 返回值与更新后的参考模型完全一致
	assertFormalInodeMatchesEntity(t, newDirModel, got, fmt.Sprintf("Mkdir[%s]返回值", caseTag))

	return fmt.Sprintf("Mkdir[%s](parent=%d, name=%q, mode=0o%o, umask=0o%o) -> 成功 (ino=%d, final_mode=0o%o, gid=%d, parent_nlink=%d)",
		caseTag, params.ParentIno, string(params.Name), params.Mode, params.Umask,
		got.Ino, got.Attr.Mode, got.Attr.GID, parentModel.Nlink)
}

// formalPredictRmdir 根据内存参考模型预测 Rmdir 的目标子目录 Inode 与错误码。
func formalPredictRmdir(m *formalMainModel, parentIno uint64, name []byte) (targetChild *formalInode, expectedErr error) {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return nil, meta.ErrInvalidArgument
	}
	if err := modelValidateEntryNameAllowEmpty(name); err != nil {
		return nil, err
	}

	if len(name) == 0 || modelIsDotOrDotDot(name) {
		parent, exists := m.Inodes[parentIno]
		if !exists || parent.Deleted {
			return nil, meta.ErrNotFound
		}
		if !parent.IsDir() {
			return nil, meta.ErrNotDir
		}
		if parent.Nlink == 0 {
			return nil, meta.ErrNotFound
		}
		if len(name) == 0 {
			return nil, meta.ErrNotFound
		}
		if bytes.Equal(name, []byte(".")) {
			return nil, meta.ErrInvalidArgument
		}
		return nil, meta.ErrNotEmpty
	}

	childIno, hasEntry := m.Entries[formalEntryKey{ParentIno: parentIno, Name: string(name)}]
	if !hasEntry {
		parent, exists := m.Inodes[parentIno]
		if !exists || parent.Deleted {
			return nil, meta.ErrNotFound
		}
		if !parent.IsDir() {
			return nil, meta.ErrNotDir
		}
		return nil, meta.ErrNotFound
	}

	parent, pExists := m.Inodes[parentIno]
	if !pExists || parent.Deleted {
		return nil, meta.ErrNotFound
	}
	if !parent.IsDir() {
		return nil, meta.ErrNotDir
	}
	if parent.Nlink == 0 {
		return nil, meta.ErrNotFound
	}

	child, cExists := m.Inodes[childIno]
	if !cExists || child.Deleted {
		return nil, meta.ErrNotFound
	}
	if !child.IsDir() {
		return nil, meta.ErrNotDir
	}
	if len(m.ChildEntriesOf(child.Ino)) > 0 {
		return nil, meta.ErrNotEmpty
	}

	return child, nil
}

// formalActionRmdir 在状态机中随机生成移除空子目录及各类非空/非目录/非法名称拦截的 Rmdir 请求并与参考模型对拍。
func formalActionRmdir(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 梳理当前目录项：空子目录项、非空子目录项、非目录项
	var emptyDirEntries []formalEntryKey
	var nonEmptyDirEntries []formalEntryKey
	var nonDirEntries []formalEntryKey

	for _, ek := range env.Model.SortedEntries() {
		childIno := env.Model.Entries[ek]
		child, ok := env.Model.Inodes[childIno]
		if !ok || child.Deleted {
			continue
		}
		if child.IsDir() {
			if len(env.Model.ChildEntriesOf(child.Ino)) == 0 {
				emptyDirEntries = append(emptyDirEntries, ek)
			} else {
				nonEmptyDirEntries = append(nonEmptyDirEntries, ek)
			}
		} else {
			nonDirEntries = append(nonDirEntries, ek)
		}
	}

	branch := env.Rng.IntN(100)
	var parentIno uint64
	var name []byte
	var caseTag string

	switch {
	case branch < 40 && len(emptyDirEntries) > 0:
		// 正常移除空子目录
		chosen := emptyDirEntries[env.Rng.IntN(len(emptyDirEntries))]
		caseTag = "正常移除空子目录"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 55 && len(nonEmptyDirEntries) > 0:
		// 非空目录拦截 (ErrNotEmpty)
		chosen := nonEmptyDirEntries[env.Rng.IntN(len(nonEmptyDirEntries))]
		caseTag = "非空目录拦截(ErrNotEmpty)"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 68 && len(nonDirEntries) > 0:
		// 目标为非目录拦截 (ErrNotDir)
		chosen := nonDirEntries[env.Rng.IntN(len(nonDirEntries))]
		caseTag = "目标为非目录拦截(ErrNotDir)"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 76:
		// 特殊名称 "." (ErrInvalidArgument) 或 ".." (ErrNotEmpty) 或 "" (ErrNotFound)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		parentIno = parent.Ino
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "特殊名称(.)(ErrInvalidArgument)"
			name = []byte(".")
		case 1:
			caseTag = "特殊名称(..)(ErrNotEmpty)"
			name = []byte("..")
		default:
			caseTag = "空名称(ErrNotFound)"
			name = []byte("")
		}

	case branch < 84:
		// 目标目录项不存在 (ErrNotFound)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		caseTag = "目标不存在(ErrNotFound)"
		parentIno = parent.Ino
		name = []byte(env.UniqueEntryName("missing_dir"))

	case branch < 92:
		// 父节点非目录 (ErrNotDir) 或父节点不存在/孤儿目录 (ErrNotFound)
		nonDirs := env.Model.ActiveNonDirs()
		if len(nonDirs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "父节点非目录(ErrNotDir)"
			parentIno = nonDirs[env.Rng.IntN(len(nonDirs))].Ino
			name = []byte("sub")
		} else {
			caseTag = "父节点不存在(ErrNotFound)"
			parentIno = uint64(math.MaxInt64 - 333333)
			name = []byte("sub")
		}

	default:
		// 非法参数：parentIno == 0、含 '/'、含 '\x00'、超长名称
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			parentIno = 0
			name = []byte("sub")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			parentIno = parent.Ino
			name = []byte("bad/sub")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			parentIno = parent.Ino
			name = []byte("bad\x00sub")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			parentIno = parent.Ino
			name = []byte(strings.Repeat("r", meta.MaxFileNameBytes+2))
		}
	}

	targetChild, expectedErr := formalPredictRmdir(env.Model, parentIno, name)
	err := env.Mgr.Rmdir(ctx, parentIno, name)

	if expectedErr != nil {
		require.Errorf(t, err, "Rmdir[%s] (parent=%d, name=%q) 预期错误 %v，但实际成功",
			caseTag, parentIno, string(name), expectedErr)
		require.Truef(t, errors.Is(err, expectedErr),
			"Rmdir[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		return fmt.Sprintf("Rmdir[%s](parent=%d, name=%q) -> 预期拒绝 (%v)",
			caseTag, parentIno, string(name), expectedErr)
	}

	require.NoErrorf(t, err, "Rmdir[%s] (parent=%d, name=%q) 预期成功，但返回错误: %v",
		caseTag, parentIno, string(name), err)
	require.NotNil(t, targetChild)

	// 同步更新内存参考模型
	parentModel := env.Model.Inodes[parentIno]
	require.NotNil(t, parentModel)
	require.GreaterOrEqual(t, parentModel.Nlink, uint32(3), "移除子目录前父目录 nlink 至少为 3")

	delete(env.Model.Entries, formalEntryKey{ParentIno: parentIno, Name: string(name)})
	parentModel.Nlink--
	targetChild.Nlink = 0
	reclaimed := env.Model.ReclaimOrphanInModel(targetChild.Ino)

	return fmt.Sprintf("Rmdir[%s](parent=%d, name=%q, child_ino=%d) -> 成功 (parent_nlink=%d, 物理回收=%v)",
		caseTag, parentIno, string(name), targetChild.Ino, parentModel.Nlink, reclaimed)
}

// ============================================================================
// 3. T2 只读查账探针：Lookup 与 GetAttr
// ============================================================================

// formalProbeLookup 在每步写操作后全面核对 Lookup 的正向目录项解析、"."/".." 解析与各类错误码语义。
func formalProbeLookup(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	// 1. 核对内存模型中所有现存目录项 (parent_ino, name) -> child_ino
	for _, ek := range env.Model.SortedEntries() {
		childIno := env.Model.Entries[ek]
		expectedChild, ok := env.Model.Inodes[childIno]
		require.Truef(t, ok && !expectedChild.Deleted,
			"Lookup 探针: 目录项 (%d, %q) 指向的子节点 ino=%d 在内存模型中不存在", ek.ParentIno, ek.Name, childIno)

		got, err := env.Mgr.Lookup(ctx, ek.ParentIno, []byte(ek.Name))
		require.NoErrorf(t, err, "Lookup(parent=%d, name=%q) 不应失败", ek.ParentIno, ek.Name)
		assertFormalInodeMatchesEntity(t, expectedChild, got,
			fmt.Sprintf("Lookup(parent=%d, name=%q)", ek.ParentIno, ek.Name))
	}

	// 2. 核对所有存活目录的 "."、".."、不存在名称、空名称与非法名称
	activeDirs := env.Model.ActiveDirs()
	for idx, dir := range activeDirs {
		// 2.1 Lookup(".") 必须返回当前目录自身
		gotDot, err := env.Mgr.Lookup(ctx, dir.Ino, []byte("."))
		require.NoErrorf(t, err, "存活目录 ino=%d Lookup('.') 不应失败", dir.Ino)
		assertFormalInodeMatchesEntity(t, dir, gotDot, fmt.Sprintf("Lookup(ino=%d, '.')", dir.Ino))

		// 2.2 Lookup("..") 必须返回父目录（根目录的 ParentIno 为自身 RootIno）
		expectedParent, ok := env.Model.Inodes[dir.ParentIno]
		if ok && !expectedParent.Deleted {
			gotDotDot, err := env.Mgr.Lookup(ctx, dir.Ino, []byte(".."))
			require.NoErrorf(t, err, "存活目录 ino=%d Lookup('..') 不应失败", dir.Ino)
			assertFormalInodeMatchesEntity(t, expectedParent, gotDotDot, fmt.Sprintf("Lookup(ino=%d, '..')", dir.Ino))
		}

		// 对首个存活目录及抽样目录执行异常名称探测（控制每步 RPC 开销同时保证 100% 覆盖）
		if idx == 0 {
			// 不存在的目录项名称 -> ErrNotFound
			_, err := env.Mgr.Lookup(ctx, dir.Ino, []byte("__formal_nonexistent_entry__"))
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"Lookup 不存在名称应返回 ErrNotFound, 实际: %v", err)

			// 空名称 "" -> ErrNotFound
			_, err = env.Mgr.Lookup(ctx, dir.Ino, []byte(""))
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"Lookup 空名称应返回 ErrNotFound, 实际: %v", err)

			// 含 '/' 或 '\x00' 非法名称 -> ErrInvalidArgument
			_, err = env.Mgr.Lookup(ctx, dir.Ino, []byte("invalid/name"))
			require.Truef(t, errors.Is(err, meta.ErrInvalidArgument),
				"Lookup 含 '/' 名称应返回 ErrInvalidArgument, 实际: %v", err)

			_, err = env.Mgr.Lookup(ctx, dir.Ino, []byte("invalid\x00name"))
			require.Truef(t, errors.Is(err, meta.ErrInvalidArgument),
				"Lookup 含 '\\x00' 名称应返回 ErrInvalidArgument, 实际: %v", err)

			// 超长名称 (>255) -> ErrNameTooLong
			_, err = env.Mgr.Lookup(ctx, dir.Ino, []byte(strings.Repeat("z", meta.MaxFileNameBytes+1)))
			require.Truef(t, errors.Is(err, meta.ErrNameTooLong),
				"Lookup 超长名称应返回 ErrNameTooLong, 实际: %v", err)
		}
	}

	// 3. 核对孤儿目录 (rmdir-whilst-open, nlink == 0)：查找 "." 或普通名称返回 ErrNotFound，查找 ".." 仍能解析原父目录
	for _, orphanDir := range findOrphanDirsInModel(env.Model) {
		_, err := env.Mgr.Lookup(ctx, orphanDir.Ino, []byte("."))
		require.Truef(t, errors.Is(err, meta.ErrNotFound),
			"孤儿目录 ino=%d Lookup('.') 应返回 ErrNotFound, 实际: %v", orphanDir.Ino, err)

		_, err = env.Mgr.Lookup(ctx, orphanDir.Ino, []byte("any_child"))
		require.Truef(t, errors.Is(err, meta.ErrNotFound),
			"孤儿目录 ino=%d Lookup('any_child') 应返回 ErrNotFound, 实际: %v", orphanDir.Ino, err)

		if parent, ok := env.Model.Inodes[orphanDir.ParentIno]; ok && !parent.Deleted {
			gotParent, err := env.Mgr.Lookup(ctx, orphanDir.Ino, []byte(".."))
			require.NoErrorf(t, err, "孤儿目录 ino=%d Lookup('..') 应返回原父目录", orphanDir.Ino)
			assertFormalInodeMatchesEntity(t, parent, gotParent, fmt.Sprintf("孤儿目录Lookup(ino=%d, '..')", orphanDir.Ino))
		}
	}

	// 4. 核对非目录父节点调用 Lookup 返回 ErrNotDir
	nonDirs := env.Model.ActiveNonDirs()
	if len(nonDirs) > 0 {
		sampleNonDir := nonDirs[env.ProbeRNG().IntN(len(nonDirs))]
		// 注意：仅当该非目录节点仍存在于底表（nlink > 0 或持有有效句柄）时探测
		if sampleNonDir.Nlink > 0 || env.Model.HasValidHandlesForIno(sampleNonDir.Ino) {
			_, err := env.Mgr.Lookup(ctx, sampleNonDir.Ino, []byte("probe"))
			require.Truef(t, errors.Is(err, meta.ErrNotDir),
				"非目录父节点 ino=%d 调用 Lookup 应返回 ErrNotDir, 实际: %v", sampleNonDir.Ino, err)
		}
	}

	// 5. 核对不存在的父节点与 parentIno == 0
	_, err := env.Mgr.Lookup(ctx, 0, []byte("probe"))
	require.True(t, errors.Is(err, meta.ErrInvalidArgument), "Lookup(parentIno=0) 应返回 ErrInvalidArgument")

	_, err = env.Mgr.Lookup(ctx, uint64(math.MaxInt64-777777), []byte("probe"))
	require.True(t, errors.Is(err, meta.ErrNotFound), "Lookup(不存在父节点) 应返回 ErrNotFound")
}

// formalProbeGetAttr 在每步写操作后全面核对存活节点、孤儿节点与已物理删除节点的 GetAttr 属性响应。
func formalProbeGetAttr(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	// 1. 核对所有未物理删除的 Inode（含根目录、子目录、常规文件、符号链接及孤儿节点）
	for _, inode := range env.Model.ActiveInodes() {
		got, err := env.Mgr.GetAttr(ctx, inode.Ino)
		if inode.Nlink == 0 && !env.Model.HasValidHandlesForIno(inode.Ino) {
			// 若孤儿节点的会话已超期失活（尚未被 RenewSession 物理清扫），GetAttr 必须拦截并返回 ErrNotFound
			require.Errorf(t, err, "无有效句柄的孤儿节点 ino=%d 调用 GetAttr 应返回 ErrNotFound", inode.Ino)
			require.Truef(t, errors.Is(err, meta.ErrNotFound), "预期 ErrNotFound, 实际: %v", err)
			require.Nil(t, got)
			continue
		}

		require.NoErrorf(t, err, "存活或持有效句柄节点 ino=%d 调用 GetAttr 不应失败", inode.Ino)
		assertFormalInodeMatchesEntity(t, inode, got, fmt.Sprintf("GetAttr(ino=%d)", inode.Ino))
	}

	// 2. 核对所有已物理删除的 Inode 均返回 ErrNotFound
	for _, ino := range env.Model.AllCreatedInos {
		if env.Model.DeletedInos[ino] {
			got, err := env.Mgr.GetAttr(ctx, ino)
			require.Errorf(t, err, "已物理删除节点 ino=%d 调用 GetAttr 应返回 ErrNotFound", ino)
			require.Truef(t, errors.Is(err, meta.ErrNotFound), "预期 ErrNotFound, 实际: %v", err)
			require.Nil(t, got)
		}
	}

	// 3. 核对 ino == 0 与不存在的 Ino
	_, err := env.Mgr.GetAttr(ctx, 0)
	require.True(t, errors.Is(err, meta.ErrInvalidArgument), "GetAttr(ino=0) 应返回 ErrInvalidArgument")

	_, err = env.Mgr.GetAttr(ctx, uint64(math.MaxInt64-888888))
	require.True(t, errors.Is(err, meta.ErrNotFound), "GetAttr(不存在ino) 应返回 ErrNotFound")
}

// ============================================================================
// 4. T2 底表数学不变量审计：INV-1 (目录树无环与父子双向一致) 与 INV-2 (目录硬链接守恒)
// ============================================================================

type dbAuditInodeRow struct {
	Ino       uint64
	Mode      uint32
	Size      int64
	Nlink     int32
	UID       uint32
	GID       uint32
	Rdev      int64
	Flags     uint32
	ParentIno uint64
	RootIno   uint64
}

func (r dbAuditInodeRow) IsDir() bool {
	return (r.Mode & meta.S_IFMT) == meta.S_IFDIR
}

type dbAuditEntryRow struct {
	ParentIno uint64
	Name      string
	ChildIno  uint64
}

// loadFormalAuditTopologySnapshot 从 PostgreSQL 底表加载当前测试关联的全部 inodes 与 entries 快照。
func loadFormalAuditTopologySnapshot(ctx context.Context, t *testing.T, env *formalMainEnv) (map[uint64]dbAuditInodeRow, []dbAuditEntryRow) {
	t.Helper()
	inodesByIno := make(map[uint64]dbAuditInodeRow)
	var entries []dbAuditEntryRow

	if len(env.Model.AllCreatedRootInos) == 0 {
		return inodesByIno, entries
	}

	rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
	for _, r := range env.Model.AllCreatedRootInos {
		rootInos = append(rootInos, int64(r))
	}

	inodeRows, err := env.Pool.Query(ctx, `
		SELECT ino, mode, size, nlink, uid, gid, rdev, flags, parent_ino, root_ino
		FROM inodes
		WHERE root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "底表拓扑审计: 查询 inodes 失败")
	defer inodeRows.Close()

	for inodeRows.Next() {
		var ino, size, rdev, parentIno, rootIno int64
		var mode, nlink, uid, gid, flags int32
		err := inodeRows.Scan(&ino, &mode, &size, &nlink, &uid, &gid, &rdev, &flags, &parentIno, &rootIno)
		require.NoError(t, err, "底表拓扑审计: 扫描 inodes 行失败")
		inodesByIno[uint64(ino)] = dbAuditInodeRow{
			Ino:       uint64(ino),
			Mode:      uint32(mode),
			Size:      size,
			Nlink:     nlink,
			UID:       uint32(uid),
			GID:       uint32(gid),
			Rdev:      rdev,
			Flags:     uint32(flags),
			ParentIno: uint64(parentIno),
			RootIno:   uint64(rootIno),
		}
	}
	require.NoError(t, inodeRows.Err())

	entryRows, err := env.Pool.Query(ctx, `
		SELECT e.parent_ino, e.name, e.ino
		FROM entries e
		JOIN inodes p ON p.ino = e.parent_ino
		WHERE p.root_ino = ANY($1)
		UNION
		SELECT e.parent_ino, e.name, e.ino
		FROM entries e
		JOIN inodes c ON c.ino = e.ino
		WHERE c.root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "底表拓扑审计: 查询 entries 失败")
	defer entryRows.Close()

	for entryRows.Next() {
		var pIno, cIno int64
		var nameBytes []byte
		err := entryRows.Scan(&pIno, &nameBytes, &cIno)
		require.NoError(t, err, "底表拓扑审计: 扫描 entries 行失败")
		entries = append(entries, dbAuditEntryRow{
			ParentIno: uint64(pIno),
			Name:      string(nameBytes),
			ChildIno:  uint64(cIno),
		})
	}
	require.NoError(t, entryRows.Err())

	return inodesByIno, entries
}

// formalAuditINV1DirTopology 强制审计底表数学不变量 INV-1（目录树无环与父子双向一致）：
// 1. 根目录 (ino == root_ino) 满足 inodes.parent_ino == root_ino，且在 entries 表中不存在 entries.ino == root_ino 的记录；
// 2. 任意非根存活目录 (ino != root_ino && nlink > 0) 在 entries 表中恰有唯一父目录记录，满足 entries.parent_ino == inodes.parent_ino，
//    且沿 entries 向上回溯必在有限步内唯一到达其所属 Disk 的 root_ino，路径无环；
// 3. 底表 entries 与目录 Inode 拓扑与内存参考模型 100% 一致。
func formalAuditINV1DirTopology(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	dbInodes, dbEntries := loadFormalAuditTopologySnapshot(ctx, t, env)

	// 构建 incomingEntriesByChild: child_ino -> []dbAuditEntryRow
	incomingByChild := make(map[uint64][]dbAuditEntryRow)
	dbEntryMap := make(map[formalEntryKey]uint64, len(dbEntries))
	for _, e := range dbEntries {
		incomingByChild[e.ChildIno] = append(incomingByChild[e.ChildIno], e)
		dbEntryMap[formalEntryKey{ParentIno: e.ParentIno, Name: e.Name}] = e.ChildIno
	}

	// 1. 核对所有存活 Disk 的根目录满足 INV-1
	for _, rootIno := range env.Model.AllCreatedRootInos {
		if env.Model.DeletedRootInos[rootIno] {
			_, exists := dbInodes[rootIno]
			require.Falsef(t, exists, "INV-1 违例: 已删除磁盘 root_ino=%d 仍在 inodes 底表中存在", rootIno)
			continue
		}

		rootRow, exists := dbInodes[rootIno]
		require.Truef(t, exists, "INV-1 违例: 存活磁盘根目录 root_ino=%d 在 inodes 底表中缺失", rootIno)
		require.Truef(t, rootRow.IsDir(), "INV-1 违例: root_ino=%d 不是目录 (mode=0o%o)", rootIno, rootRow.Mode)
		require.Equalf(t, rootIno, rootRow.ParentIno,
			"INV-1 违例: 根目录 root_ino=%d 的 inodes.parent_ino (%d) != root_ino (%d)",
			rootIno, rootRow.ParentIno, rootIno)
		require.Equalf(t, rootIno, rootRow.RootIno,
			"INV-1 违例: 根目录 root_ino=%d 的 inodes.root_ino (%d) != root_ino (%d)",
			rootIno, rootRow.RootIno, rootIno)
		require.Emptyf(t, incomingByChild[rootIno],
			"INV-1 违例: entries 表中存在指向根目录 root_ino=%d 的记录: %+v",
			rootIno, incomingByChild[rootIno])
	}

	// 2. 核对所有非根目录满足 INV-1（唯一父 entry、entries.parent_ino == inodes.parent_ino、向上回溯有限步到达所属 root_ino 且无环）
	maxAllowedSteps := len(dbInodes) + 1
	for ino, row := range dbInodes {
		if !row.IsDir() {
			// 非目录节点的 parent_ino 恒为 0
			require.Zerof(t, row.ParentIno, "INV-1 违例: 非目录节点 ino=%d 的 inodes.parent_ino (%d) 不为 0", ino, row.ParentIno)
			continue
		}

		if ino == row.RootIno {
			continue
		}

		if row.Nlink == 0 {
			// 孤儿目录 (rmdir-whilst-open) 在 entries 表中绝不能有指向它的父目录项
			require.Emptyf(t, incomingByChild[ino],
				"INV-1 违例: 孤儿目录 ino=%d (nlink=0) 在 entries 表中仍存在父记录: %+v",
				ino, incomingByChild[ino])
			continue
		}

		// 非根存活目录 (ino != root_ino && nlink > 0)
		incoming := incomingByChild[ino]
		require.Lenf(t, incoming, 1,
			"INV-1 违例: 非根存活目录 ino=%d 在 entries 表中的父记录数必须唯一为 1, 实际为 %d (%+v)",
			ino, len(incoming), incoming)

		parentEntry := incoming[0]
		require.Equalf(t, row.ParentIno, parentEntry.ParentIno,
			"INV-1 违例: 存活目录 ino=%d 的 entries.parent_ino (%d) != inodes.parent_ino (%d)",
			ino, parentEntry.ParentIno, row.ParentIno)

		// 沿 entries 向上回溯至所属 Disk 的 root_ino，断言有限步可达且无环
		currIno := ino
		visited := make(map[uint64]bool)
		steps := 0
		for currIno != row.RootIno {
			require.Falsef(t, visited[currIno],
				"INV-1 违例: 从目录 ino=%d 向上回溯在节点 %d 处检测到目录环", ino, currIno)
			visited[currIno] = true
			steps++
			require.LessOrEqualf(t, steps, maxAllowedSteps,
				"INV-1 违例: 从目录 ino=%d 向上回溯超过最大步数 %d 仍未到达 root_ino=%d",
				ino, maxAllowedSteps, row.RootIno)

			currRow, ok := dbInodes[currIno]
			require.Truef(t, ok, "INV-1 违例: 回溯路径节点 ino=%d 在 inodes 底表中不存在", currIno)
			require.Truef(t, currRow.IsDir() && currRow.Nlink > 0,
				"INV-1 违例: 回溯路径节点 ino=%d 不是存活目录 (mode=0o%o, nlink=%d)",
				currIno, currRow.Mode, currRow.Nlink)
			require.Equalf(t, row.RootIno, currRow.RootIno,
				"INV-1 违例: 回溯路径节点 ino=%d 的 root_ino (%d) 与起点 root_ino (%d) 不一致",
				currIno, currRow.RootIno, row.RootIno)

			inc := incomingByChild[currIno]
			require.Lenf(t, inc, 1,
				"INV-1 违例: 回溯路径节点 ino=%d 在 entries 中父记录数不唯一: %+v", currIno, inc)
			require.Equalf(t, currRow.ParentIno, inc[0].ParentIno,
				"INV-1 违例: 回溯路径节点 ino=%d entries.parent_ino (%d) != inodes.parent_ino (%d)",
				currIno, inc[0].ParentIno, currRow.ParentIno)

			currIno = inc[0].ParentIno
		}

		// 确认终点确实是存活的所属根目录
		rootRow, ok := dbInodes[currIno]
		require.Truef(t, ok && rootRow.IsDir() && rootRow.Nlink >= 2,
			"INV-1 违例: 目录 ino=%d 回溯到达的 root_ino=%d 状态异常: %+v", ino, currIno, rootRow)
	}

	// 3. 核对底表 entries 与内存参考模型 Entries 完全一致
	require.Equalf(t, len(env.Model.Entries), len(dbEntryMap),
		"INV-1 违例: 底表 entries 记录数 (%d) 与内存参考模型 (%d) 不一致",
		len(dbEntryMap), len(env.Model.Entries))
	for ek, expectedChildIno := range env.Model.Entries {
		actualChildIno, exists := dbEntryMap[ek]
		require.Truef(t, exists, "INV-1 违例: 内存模型目录项 (%d, %q) 在底表 entries 中缺失", ek.ParentIno, ek.Name)
		require.Equalf(t, expectedChildIno, actualChildIno,
			"INV-1 违例: 目录项 (%d, %q) 目标 ino 不一致: 模型=%d, 底表=%d",
			ek.ParentIno, ek.Name, expectedChildIno, actualChildIno)
	}
}

// formalAuditINV2DirNlink 强制审计底表数学不变量 目录 INV-2（目录硬链接计数守恒）：
// 1. 任意存活目录（含 root_ino）满足 inodes.nlink == 2 + count(子目录 entries)；
// 2. 若为 nlink == 0 的已移除孤儿目录（rmdir-whilst-open），则名下不存在任何子 entries；
// 3. 底表中每个目录的 nlink、parent_ino、root_ino、mode、uid、gid 与内存参考模型完全一致。
func formalAuditINV2DirNlink(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	dbInodes, dbEntries := loadFormalAuditTopologySnapshot(ctx, t, env)

	// 统计每个父目录名下的总子项数与子目录项数
	totalChildEntries := make(map[uint64]int32)
	subDirEntries := make(map[uint64]int32)
	for _, e := range dbEntries {
		totalChildEntries[e.ParentIno]++
		if childRow, ok := dbInodes[e.ChildIno]; ok && childRow.IsDir() {
			subDirEntries[e.ParentIno]++
		}
	}

	for ino, row := range dbInodes {
		if !row.IsDir() {
			continue
		}

		if row.Nlink > 0 {
			expectedNlink := 2 + subDirEntries[ino]
			require.Equalf(t, expectedNlink, row.Nlink,
				"INV-2 违例: 存活目录 ino=%d (root_ino=%d) 的 inodes.nlink (%d) != 2 + 子目录数 (%d)",
				ino, row.RootIno, row.Nlink, subDirEntries[ino])
		} else {
			require.Zerof(t, row.Nlink, "INV-2 违例: 目录 ino=%d 的 nlink (%d) < 0", ino, row.Nlink)
			require.Zerof(t, totalChildEntries[ino],
				"INV-2 违例: 孤儿目录 ino=%d (nlink=0) 名下仍残留 %d 条子 entries",
				ino, totalChildEntries[ino])
		}

		// 与内存参考模型中的目录节点核对
		modelDir, ok := env.Model.Inodes[ino]
		require.Truef(t, ok && !modelDir.Deleted,
			"INV-2 违例: 底表存在目录 ino=%d，但在内存参考模型中不存在或已标记删除", ino)
		require.Equalf(t, int32(modelDir.Nlink), row.Nlink,
			"INV-2 违例: 目录 ino=%d 的底表 nlink (%d) 与内存模型 (%d) 不一致",
			ino, row.Nlink, modelDir.Nlink)
		require.Equalf(t, modelDir.ParentIno, row.ParentIno,
			"INV-2 违例: 目录 ino=%d 的底表 parent_ino (%d) 与内存模型 (%d) 不一致",
			ino, row.ParentIno, modelDir.ParentIno)
		require.Equalf(t, modelDir.RootIno, row.RootIno,
			"INV-2 违例: 目录 ino=%d 的底表 root_ino (%d) 与内存模型 (%d) 不一致",
			ino, row.RootIno, modelDir.RootIno)
		require.Equalf(t, modelDir.Mode, row.Mode,
			"INV-2 违例: 目录 ino=%d 的底表 mode (0o%o) 与内存模型 (0o%o) 不一致",
			ino, row.Mode, modelDir.Mode)
		require.Equalf(t, modelDir.UID, row.UID,
			"INV-2 违例: 目录 ino=%d 的底表 uid (%d) 与内存模型 (%d) 不一致",
			ino, row.UID, modelDir.UID)
		require.Equalf(t, modelDir.GID, row.GID,
			"INV-2 违例: 目录 ino=%d 的底表 gid (%d) 与内存模型 (%d) 不一致",
			ino, row.GID, modelDir.GID)
	}

	// 反向确认内存模型中的所有未删除目录在底表中均存在，已物理删除节点在底表中均不存在
	for _, ino := range env.Model.AllCreatedInos {
		if env.Model.DeletedInos[ino] {
			_, exists := dbInodes[ino]
			require.Falsef(t, exists, "INV-2 违例: 已物理删除节点 ino=%d 仍在底表 inodes 中残留", ino)
			continue
		}
		if mInode, ok := env.Model.Inodes[ino]; ok && mInode.IsDir() {
			_, exists := dbInodes[ino]
			require.Truef(t, exists, "INV-2 违例: 内存模型存活目录 ino=%d 在底表 inodes 中缺失", ino)
		}
	}
}

// ============================================================================
// 5. T2 专属测试入口：确定性边界矩阵验证 + 多轮随机状态机对拍
// ============================================================================

// TestFormalMain_T2_DirTopology 验证 T2 主状态机 Mkdir/Rmdir 目录树拓扑对拍、Lookup/GetAttr 探针及底表 INV-1、INV-2 审计。
func TestFormalMain_T2_DirTopology(t *testing.T) {
	t.Run("确定性边界_SGID继承_Umask_EMLINK_DotDot_ENOTEMPTY_ENOTDIR_孤儿目录全链路验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 20,
			Seed:       0x20260416,
			Rng:        newFormalRNG(0x20260416),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t2-det",
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
			formalAuditINV1DirTopology(ctx, t, env)
			formalAuditINV2DirNlink(ctx, t, env)
			formalAuditINV4DiskQuota(ctx, t, env)
		}

		// 1. 创建初始测试虚拟磁盘
		formalCreateValidDiskStep(ctx, t, env)
		disk := env.Model.ActiveDisks()[0]
		rootIno := disk.RootIno
		rootDir := env.Model.Inodes[rootIno]
		runAllProbesAndAuditors()

		// 2. 验证 Umask 权限掩码与粘滞位 S_ISVTX (0o1000)：
		// S_IFDIR | ((0o1777 & 0o777) &^ (0o027 & 0o777)) | (0o1777 & 0o1000) == S_IFDIR | 0o1750
		umaskDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("umask_sticky_dir"),
			Mode:      0o1777,
			Umask:     0o027,
			UID:       1001,
			GID:       1002,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFDIR|0o1750), umaskDir.Attr.Mode)
		require.Equal(t, uint32(1001), umaskDir.Attr.UID)
		require.Equal(t, uint32(1002), umaskDir.Attr.GID)
		require.Equal(t, uint32(2), umaskDir.Attr.Nlink)
		require.Equal(t, rootIno, umaskDir.Attr.ParentIno)

		env.Model.RecordNewInode(&formalInode{
			Ino:        umaskDir.Ino,
			Generation: umaskDir.Generation,
			RootIno:    rootIno,
			ParentIno:  rootIno,
			Mode:       umaskDir.Attr.Mode,
			UID:        1001,
			GID:        1002,
			Size:       meta.DefaultDirSize,
			Nlink:      2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "umask_sticky_dir"}] = umaskDir.Ino
		rootDir.Nlink++
		disk.UsedInodes++
		runAllProbesAndAuditors()

		// 3. 验证父目录 S_ISGID 属组继承与模式位强制传播
		sgidParent, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sgid_parent"),
			Mode:      meta.S_ISGID | 0o755,
			Umask:     0o022,
			UID:       1000,
			GID:       2000,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFDIR|meta.S_ISGID|0o755), sgidParent.Attr.Mode)
		require.Equal(t, uint32(2000), sgidParent.Attr.GID)

		env.Model.RecordNewInode(&formalInode{
			Ino:        sgidParent.Ino,
			Generation: sgidParent.Generation,
			RootIno:    rootIno,
			ParentIno:  rootIno,
			Mode:       sgidParent.Attr.Mode,
			UID:        1000,
			GID:        2000,
			Size:       meta.DefaultDirSize,
			Nlink:      2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sgid_parent"}] = sgidParent.Ino
		rootDir.Nlink++
		disk.UsedInodes++

		// 在 sgid_parent 下创建子目录（入参 Mode=0o777 不带 S_ISGID，GID=9999），验证自动继承 GID=2000 且带 S_ISGID
		sgidChild, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: sgidParent.Ino,
			Name:      []byte("sgid_child"),
			Mode:      0o777,
			Umask:     0o027,
			UID:       1005,
			GID:       9999,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(2000), sgidChild.Attr.GID, "子目录必须继承 S_ISGID 父目录的 GID")
		require.Equal(t, uint32(meta.S_IFDIR|meta.S_ISGID|0o750), sgidChild.Attr.Mode, "子目录必须自动携带 S_ISGID 位并应用 umask")

		env.Model.RecordNewInode(&formalInode{
			Ino:        sgidChild.Ino,
			Generation: sgidChild.Generation,
			RootIno:    rootIno,
			ParentIno:  sgidParent.Ino,
			Mode:       sgidChild.Attr.Mode,
			UID:        1005,
			GID:        2000,
			Size:       meta.DefaultDirSize,
			Nlink:      2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: sgidParent.Ino, Name: "sgid_child"}] = sgidChild.Ino
		env.Model.Inodes[sgidParent.Ino].Nlink++
		disk.UsedInodes++
		runAllProbesAndAuditors()

		// 4. 验证同名冲突 (ErrAlreadyExists) 与特殊/非法名称校验
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte("sgid_parent"), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte("."), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte(".."), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte(""), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte("a/b"), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte("a\x00b"), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: rootIno, Name: []byte(strings.Repeat("x", 256)), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrNameTooLong)
		runAllProbesAndAuditors()

		// 5. 创建一个常规文件用于验证父节点非目录 (ErrNotDir) 与 Rmdir 目标非目录 (ErrNotDir)
		regNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: rootIno,
			Name:      []byte("regular_file"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino:        regNode.Ino,
			Generation: regNode.Generation,
			RootIno:    rootIno,
			ParentIno:  0,
			Mode:       regNode.Attr.Mode,
			UID:        1000,
			GID:        1000,
			Size:       0,
			Nlink:      1,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "regular_file"}] = regNode.Ino
		disk.UsedInodes++

		// 父节点为常规文件时 Mkdir、Rmdir、Lookup 均返回 ErrNotDir
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: regNode.Ino, Name: []byte("sub"), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrNotDir)
		err = mgr.Rmdir(ctx, regNode.Ino, []byte("sub"))
		require.ErrorIs(t, err, meta.ErrNotDir)
		_, err = mgr.Lookup(ctx, regNode.Ino, []byte("sub"))
		require.ErrorIs(t, err, meta.ErrNotDir)

		// Rmdir 目标为常规文件时返回 ErrNotDir
		err = mgr.Rmdir(ctx, rootIno, []byte("regular_file"))
		require.ErrorIs(t, err, meta.ErrNotDir)

		// 7. 验证 Rmdir 特殊名称 ("." -> ErrInvalidArgument, ".." -> ErrNotEmpty, 非空目录 -> ErrNotEmpty)
		err = mgr.Rmdir(ctx, rootIno, []byte("."))
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		err = mgr.Rmdir(ctx, rootIno, []byte(".."))
		require.ErrorIs(t, err, meta.ErrNotEmpty)
		err = mgr.Rmdir(ctx, rootIno, []byte("sgid_parent"))
		require.ErrorIs(t, err, meta.ErrNotEmpty, "包含子目录 sgid_child 的非空目录严禁被 Rmdir 删除")
		runAllProbesAndAuditors()

		// 8. 验证 rmdir-whilst-open（目录持有活跃 OpenDir 句柄时被 Rmdir 转为 nlink=0 孤儿目录，关闭句柄后物理回收）
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   umaskDir.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh:    openDirRes.Fh,
			Ino:   umaskDir.Ino,
			Flags: openDirRes.OpenFlags,
			IsDir: true,
		})

		// 执行 Rmdir(rootIno, "umask_sticky_dir")：因持有活跃句柄，转为 nlink=0 的孤儿目录，暂不扣减 used_inodes
		err = mgr.Rmdir(ctx, rootIno, []byte("umask_sticky_dir"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: rootIno, Name: "umask_sticky_dir"})
		rootDir.Nlink--
		env.Model.Inodes[umaskDir.Ino].Nlink = 0
		reclaimed := env.Model.ReclaimOrphanInModel(umaskDir.Ino)
		require.False(t, reclaimed, "持有活跃目录句柄时不应立即物理回收")

		// 在孤儿目录下尝试 Mkdir 必须返回 ErrNotFound
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: umaskDir.Ino, Name: []byte("sub"), Mode: 0o755})
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 此时运行全量探针与底表 INV-1、INV-2、INV-4 审计（涵盖 nlink=0 孤儿目录分支！）
		runAllProbesAndAuditors()

		// 释放目录句柄 ReleaseDir，触发孤儿目录物理删除与 used_inodes - 1
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: umaskDir.Ino,
			Fh:  openDirRes.Fh,
		})
		require.NoError(t, err)
		env.Model.Handles[openDirRes.Fh].Closed = true
		env.Model.ClosedHandles[openDirRes.Fh] = env.Model.Handles[openDirRes.Fh]
		delete(env.Model.Handles, openDirRes.Fh)
		reclaimedAfterRelease := env.Model.ReclaimOrphanInModel(umaskDir.Ino)
		require.True(t, reclaimedAfterRelease, "最后一个目录句柄关闭后必须完成孤儿物理回收")
		runAllProbesAndAuditors()

		// 9. 依次移除空子目录 sgid_child 与 sgid_parent，核对 nlink 与配额回退
		err = mgr.Rmdir(ctx, sgidParent.Ino, []byte("sgid_child"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: sgidParent.Ino, Name: "sgid_child"})
		env.Model.Inodes[sgidParent.Ino].Nlink--
		env.Model.Inodes[sgidChild.Ino].Nlink = 0
		require.True(t, env.Model.ReclaimOrphanInModel(sgidChild.Ino))
		runAllProbesAndAuditors()

		err = mgr.Rmdir(ctx, rootIno, []byte("sgid_parent"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: rootIno, Name: "sgid_parent"})
		rootDir.Nlink--
		env.Model.Inodes[sgidParent.Ino].Nlink = 0
		require.True(t, env.Model.ReclaimOrphanInModel(sgidParent.Ino))
		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_Mkdir与Rmdir目录树拓扑对拍与底表INV-1_INV-2_INV-4全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 10, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 10, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 10, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 40, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 30, Fn: formalActionRmdir},
			},
			Probes: []formalNamedProbe{
				{Name: "GetDisk", Fn: formalProbeGetDisk},
				{Name: "StatFS", Fn: formalProbeStatFS},
				{Name: "Lookup", Fn: formalProbeLookup},
				{Name: "GetAttr", Fn: formalProbeGetAttr},
			},
			Auditors: []formalNamedAuditor{
				{Name: "INV-1_DirTopology", Fn: formalAuditINV1DirTopology},
				{Name: "INV-2_DirNlink", Fn: formalAuditINV2DirNlink},
				{Name: "INV-4_DiskQuota", Fn: formalAuditINV4DiskQuota},
			},
		})
	})
}
