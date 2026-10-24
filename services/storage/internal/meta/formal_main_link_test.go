package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T4 核心硬链接动作：Link、Unlink
	registerFormalAction("Link", 25, formalActionLink)
	registerFormalAction("Unlink", 25, formalActionUnlink)

	// 注册 T4 底表数学不变量审计器：非目录 INV-2 (nlink == count(entries)) 与 INV-5 (跨盘拓扑隔离)
	registerFormalAuditor("INV-2_NonDirNlink", formalAuditINV2NonDirNlink)
	registerFormalAuditor("INV-5_CrossDiskIsolation", formalAuditINV5CrossDiskIsolation)
}

// ============================================================================
// 1. T4 参考模型推导函数：Link 与 Unlink
// ============================================================================

// modelUniqueSortedUint64 对无符号 64 位整数切片升序排列并去重，对应被测代码中的 uniqueSortedInos。
func modelUniqueSortedUint64(vals ...uint64) []uint64 {
	if len(vals) == 0 {
		return nil
	}
	cp := append([]uint64(nil), vals...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	out := cp[:1]
	for _, v := range cp[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}

// formalPredictLink 根据内存参考模型预测 Link 的源节点、是否触发匿名 TmpFile 首次具名化以及预期错误码。
// 严格对齐 manager_dir_topology.go 中 Link 的静态校验、"."/".." 分支、按全局 ino 升序加锁校验、跨盘 EXDEV 与重名 EEXIST 顺序。
func formalPredictLink(
	m *formalMainModel,
	params meta.LinkParams,
) (srcNode *formalInode, isTmpFileMaterializing bool, expectedErr error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return nil, false, meta.ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return nil, false, meta.ErrInvalidArgument
	}
	if err := modelValidateEntryName(params.NewName); err != nil {
		return nil, false, err
	}

	if modelIsDotOrDotDot(params.NewName) {
		parent, exists := m.Inodes[params.NewParentIno]
		if !exists || parent.Deleted {
			return nil, false, meta.ErrNotFound
		}
		if !parent.IsDir() {
			return nil, false, meta.ErrNotDir
		}
		if parent.Nlink == 0 {
			return nil, false, meta.ErrNotFound
		}
		return nil, false, meta.ErrAlreadyExists
	}

	var parent *formalInode
	var src *formalInode
	for _, ino := range modelUniqueSortedUint64(params.NewParentIno, params.Ino) {
		row, exists := m.Inodes[ino]
		if !exists || row.Deleted {
			return nil, false, meta.ErrNotFound
		}
		if ino == params.NewParentIno {
			if !row.IsDir() {
				return nil, false, meta.ErrNotDir
			}
			if row.Nlink == 0 {
				return nil, false, meta.ErrNotFound
			}
			parent = row
		}
		if ino == params.Ino {
			src = row
			// POSIX EPERM：禁止对目录创建硬链接（在跨盘 EXDEV 判定之前）
			if row.IsDir() {
				return nil, false, meta.ErrPerm
			}
			if row.Nlink >= meta.MaxNlink {
				return nil, false, meta.ErrTooManyLinks
			}
			if row.Nlink == 0 {
				if !row.IsReg() || !m.HasLinkableTmpFileHandleForIno(row.Ino) {
					return nil, false, meta.ErrNotFound
				}
				isTmpFileMaterializing = true
			}
		}
	}

	if parent.RootIno != src.RootIno {
		return nil, false, meta.ErrCrossDevice
	}

	if _, dup := m.Entries[formalEntryKey{ParentIno: params.NewParentIno, Name: string(params.NewName)}]; dup {
		return nil, false, meta.ErrAlreadyExists
	}

	return src, isTmpFileMaterializing, nil
}

// formalPredictUnlink 根据内存参考模型预测 Unlink 的目标子节点与预期错误码。
// 严格对齐 manager_dir_topology.go 中 Unlink 的静态校验、空名与 "."/".." 校验、预读目录项、按全局 ino 升序加锁校验及目录 EISDIR 拦截。
func formalPredictUnlink(
	m *formalMainModel,
	parentIno uint64,
	name []byte,
) (targetChild *formalInode, expectedErr error) {
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
		return nil, meta.ErrInvalidArgument
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

	for _, ino := range modelUniqueSortedUint64(parentIno, childIno) {
		row, exists := m.Inodes[ino]
		if !exists || row.Deleted {
			return nil, meta.ErrNotFound
		}
		if ino == parentIno {
			if !row.IsDir() {
				return nil, meta.ErrNotDir
			}
			if row.Nlink == 0 {
				return nil, meta.ErrNotFound
			}
		}
	}

	child := m.Inodes[childIno]
	if child.IsDir() {
		return nil, meta.ErrIsDir
	}

	return child, nil
}

// ============================================================================
// 2. T4 动作实现：Link 与 Unlink 随机操作生成与逐操作对拍
// ============================================================================

// formalActionLink 在状态机中随机生成同目录/跨目录硬链接创建、TmpFile 一次性具名化、
// 目录源节点 EPERM 拦截（含跨盘目录优先返回 EPERM）、非目录跨盘 EXDEV 拦截、同名/Dot EEXIST 及各类错误码请求并与参考模型对拍。
func formalActionLink(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 收集可合法 Link 的源节点：nlink > 0 的非目录节点，或 nlink == 0 但持有可具名化 O_TMPFILE 句柄的常规文件
	var linkableSrcs []*formalInode
	var unlinkedOrphans []*formalInode
	for _, node := range env.Model.ActiveNonDirs() {
		if node.Nlink > 0 {
			linkableSrcs = append(linkableSrcs, node)
		} else if node.IsReg() && env.Model.HasLinkableTmpFileHandleForIno(node.Ino) {
			linkableSrcs = append(linkableSrcs, node)
		} else {
			unlinkedOrphans = append(unlinkedOrphans, node)
		}
	}

	branch := env.Rng.IntN(100)
	var params meta.LinkParams
	var caseTag string

	switch {
	case branch < 48 && len(linkableSrcs) > 0:
		// 正常创建同目录或跨父目录硬链接（或 TmpFile 首次具名化）
		src := linkableSrcs[env.Rng.IntN(len(linkableSrcs))]
		params.Ino = src.Ino
		params.NewName = []byte(env.UniqueEntryName("hl"))

		// 筛选同一虚拟磁盘 (RootIno 相同) 的存活目录
		var sameDiskDirs []*formalInode
		for _, d := range activeDirs {
			if d.RootIno == src.RootIno {
				sameDiskDirs = append(sameDiskDirs, d)
			}
		}
		if len(sameDiskDirs) > 0 {
			targetDir := sameDiskDirs[env.Rng.IntN(len(sameDiskDirs))]
			params.NewParentIno = targetDir.Ino
			if src.Nlink == 0 {
				caseTag = "TmpFile首次Link具名化"
			} else if len(sameDiskDirs) > 1 {
				caseTag = "同盘(含跨子目录)创建硬链接"
			} else {
				caseTag = "同目录创建硬链接"
			}
		} else {
			params.NewParentIno = activeDirs[0].Ino
			caseTag = "创建硬链接"
		}

	case branch < 60:
		// 对目录源节点调用 Link -> 优先返回 ErrPerm (EPERM)，即使跨盘也先返回 ErrPerm
		srcDir := activeDirs[env.Rng.IntN(len(activeDirs))]
		dstDir := activeDirs[env.Rng.IntN(len(activeDirs))]
		// 若存在跨盘目录，按概率故意挑选异盘目录以验证 EPERM 优先于 EXDEV
		for _, d := range activeDirs {
			if d.RootIno != srcDir.RootIno && env.Rng.IntN(2) == 0 {
				dstDir = d
				break
			}
		}
		params.Ino = srcDir.Ino
		params.NewParentIno = dstDir.Ino
		params.NewName = []byte(env.UniqueEntryName("dir_link_forbidden"))
		if srcDir.RootIno != dstDir.RootIno {
			caseTag = "跨盘目录源节点拦截(ErrPerm优先于EXDEV)"
		} else {
			caseTag = "目录源节点拦截(ErrPerm)"
		}

	case branch < 72 && len(linkableSrcs) > 0:
		// 跨虚拟磁盘 Link 拦截 -> ErrCrossDevice (EXDEV)
		var crossSrc *formalInode
		var crossParent *formalInode
		for _, s := range linkableSrcs {
			for _, d := range activeDirs {
				if d.RootIno != s.RootIno {
					crossSrc = s
					crossParent = d
					break
				}
			}
			if crossSrc != nil {
				break
			}
		}
		if crossSrc != nil && crossParent != nil {
			caseTag = "跨盘非目录硬链接拦截(ErrCrossDevice)"
			params.Ino = crossSrc.Ino
			params.NewParentIno = crossParent.Ino
			params.NewName = []byte(env.UniqueEntryName("cross_disk_link"))
		} else {
			// 若当前仅有单盘，退化为 "." / ".." 拦截测试
			caseTag = "特殊名称(.)冲突(ErrAlreadyExists)"
			params.Ino = linkableSrcs[0].Ino
			params.NewParentIno = activeDirs[0].Ino
			params.NewName = []byte(".")
		}

	case branch < 81:
		// 目标名称已存在或 "." / ".." (ErrAlreadyExists)
		entries := env.Model.SortedEntries()
		if len(entries) > 0 && len(linkableSrcs) > 0 && env.Rng.IntN(2) == 0 {
			chosen := entries[env.Rng.IntN(len(entries))]
			// 寻找与 chosen.ParentIno 同盘的可链接非目录源节点，确保不触发 EXDEV 而是精准命中 ErrAlreadyExists
			parentDir := env.Model.Inodes[chosen.ParentIno]
			var sameDiskSrc *formalInode
			if parentDir != nil {
				for _, s := range linkableSrcs {
					if s.RootIno == parentDir.RootIno {
						sameDiskSrc = s
						break
					}
				}
			}
			if sameDiskSrc != nil {
				caseTag = "目标目录项已存在(ErrAlreadyExists)"
				params.Ino = sameDiskSrc.Ino
				params.NewParentIno = chosen.ParentIno
				params.NewName = []byte(chosen.Name)
			} else {
				caseTag = "特殊名称(..)(ErrAlreadyExists)"
				params.Ino = linkableSrcs[0].Ino
				params.NewParentIno = activeDirs[0].Ino
				params.NewName = []byte("..")
			}
		} else {
			parent := activeDirs[env.Rng.IntN(len(activeDirs))]
			params.Ino = parent.Ino
			if len(linkableSrcs) > 0 {
				params.Ino = linkableSrcs[0].Ino
			}
			params.NewParentIno = parent.Ino
			if env.Rng.IntN(2) == 0 {
				caseTag = "特殊名称(.)(ErrAlreadyExists)"
				params.NewName = []byte(".")
			} else {
				caseTag = "特殊名称(..)(ErrAlreadyExists)"
				params.NewName = []byte("..")
			}
		}

	case branch < 89:
		// 源节点为普通 nlink==0 孤儿节点、已物理删除节点或不存在节点 -> ErrNotFound
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.NewParentIno = parent.Ino
		params.NewName = []byte(env.UniqueEntryName("orphan_or_missing_src"))
		if len(unlinkedOrphans) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "普通nlink=0孤儿源节点禁止复活(ErrNotFound)"
			params.Ino = unlinkedOrphans[env.Rng.IntN(len(unlinkedOrphans))].Ino
		} else if delIno, ok := env.Model.FirstDeletedIno(); ok && env.Rng.IntN(2) == 0 {
			caseTag = "已物理删除源节点(ErrNotFound)"
			params.Ino = delIno
		} else {
			caseTag = "不存在源节点(ErrNotFound)"
			params.Ino = uint64(math.MaxInt64 - 135791)
		}

	case branch < 95:
		// 目标父节点非目录 (ErrNotDir) 或孤儿父目录/不存在父目录 (ErrNotFound)
		srcIno := activeDirs[0].Ino
		if len(linkableSrcs) > 0 {
			srcIno = linkableSrcs[env.Rng.IntN(len(linkableSrcs))].Ino
		}
		params.Ino = srcIno
		params.NewName = []byte(env.UniqueEntryName("bad_parent_link"))

		orphanDirs := findOrphanDirsInModel(env.Model)
		if len(orphanDirs) > 0 && env.Rng.IntN(3) == 0 {
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.NewParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		} else if len(linkableSrcs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "父节点非目录(ErrNotDir)"
			params.NewParentIno = linkableSrcs[env.Rng.IntN(len(linkableSrcs))].Ino
		} else {
			caseTag = "不存在父目录(ErrNotFound)"
			params.NewParentIno = uint64(math.MaxInt64 - 246802)
		}

	default:
		// 非法静态入参：Ino==0、NewParentIno==0、空名称、含 '/'、含 '\x00'、超长名称 (>255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		srcIno := parent.Ino
		if len(linkableSrcs) > 0 {
			srcIno = linkableSrcs[0].Ino
		}
		params.Ino = srcIno
		params.NewParentIno = parent.Ino
		params.NewName = []byte(env.UniqueEntryName("bad_arg"))

		switch env.Rng.IntN(6) {
		case 0:
			caseTag = "非法Ino=0(ErrInvalidArgument)"
			params.Ino = 0
		case 1:
			caseTag = "非法NewParentIno=0(ErrInvalidArgument)"
			params.NewParentIno = 0
		case 2:
			caseTag = "空名称(ErrInvalidArgument)"
			params.NewName = []byte("")
		case 3:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			params.NewName = []byte("bad/link")
		case 4:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			params.NewName = []byte("bad\x00link")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			params.NewName = []byte(strings.Repeat("l", meta.MaxFileNameBytes+1+env.Rng.IntN(16)))
		}
	}

	srcNode, isTmpFileMaterializing, expectedErr := formalPredictLink(env.Model, params)
	got, err := env.Mgr.Link(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Link[%s] (ino=%d, newParent=%d, newName=%q) 预期错误 %v，但实际成功: %+v",
			caseTag, params.Ino, params.NewParentIno, string(params.NewName), expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Link[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Link[%s](ino=%d, new_parent=%d, new_name=%q) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.NewParentIno, string(params.NewName), expectedErr)
	}

	require.NoErrorf(t, err, "Link[%s] (ino=%d, newParent=%d, newName=%q) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.NewParentIno, string(params.NewName), err)
	require.NotNil(t, got)
	require.NotNil(t, srcNode)

	// 同步更新内存参考模型：
	// 1. 新增目录项映射；
	// 2. 递增源节点 nlink + 1（非目录节点的 ParentIno 恒为 0，父目录 nlink 与磁盘 used_inodes / used_bytes 均保持不变）；
	// 3. 若为匿名 TmpFile 首次具名化，清除该 Inode 关联句柄的 O_TMPFILE 标志位。
	env.Model.Entries[formalEntryKey{ParentIno: params.NewParentIno, Name: string(params.NewName)}] = params.Ino
	srcNode.Nlink++
	if isTmpFileMaterializing {
		srcNode.TmpFileLinked = true
		env.Model.ClearTmpFileFlagForIno(params.Ino)
	}

	assertFormalInodeMatchesEntity(t, srcNode, got, fmt.Sprintf("Link[%s]返回值", caseTag))

	return fmt.Sprintf("Link[%s](ino=%d, new_parent=%d, new_name=%q) -> 成功 (nlink=%d, tmpfile具名化=%v)",
		caseTag, params.Ino, params.NewParentIno, string(params.NewName), srcNode.Nlink, isTmpFileMaterializing)
}

// formalActionUnlink 在状态机中随机生成多硬链接解链 (nlink > 0 保留)、单链接解链物理删除与配额退还、
// unlink-whilst-open 孤儿保留、目录 EISDIR 拦截、"."/".." EINVAL 拦截及各类错误码请求并与参考模型对拍。
func formalActionUnlink(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 分类梳理当前所有目录项：多硬链接非目录项 (nlink > 1)、单硬链接非目录项 (nlink == 1)、子目录项 (IsDir)
	var multiLinkEntries []formalEntryKey
	var singleLinkEntries []formalEntryKey
	var dirEntries []formalEntryKey

	for _, ek := range env.Model.SortedEntries() {
		childIno := env.Model.Entries[ek]
		child, ok := env.Model.Inodes[childIno]
		if !ok || child.Deleted {
			continue
		}
		if child.IsDir() {
			dirEntries = append(dirEntries, ek)
		} else if child.Nlink > 1 {
			multiLinkEntries = append(multiLinkEntries, ek)
		} else {
			singleLinkEntries = append(singleLinkEntries, ek)
		}
	}

	branch := env.Rng.IntN(100)
	var parentIno uint64
	var name []byte
	var caseTag string

	switch {
	case branch < 28 && len(multiLinkEntries) > 0:
		// 解链多硬链接节点：解链后 nlink > 0，节点保留且不退还任何配额
		chosen := multiLinkEntries[env.Rng.IntN(len(multiLinkEntries))]
		caseTag = "多硬链接解链(nlink>0保留节点且配额不变)"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 52 && len(singleLinkEntries) > 0:
		// 解链单硬链接节点：解链后 nlink == 0（无句柄则当场物理删除并退还配额，有句柄则转为孤儿节点）
		chosen := singleLinkEntries[env.Rng.IntN(len(singleLinkEntries))]
		caseTag = "最后一条硬链接解链(nlink->0)"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 66 && len(dirEntries) > 0:
		// 对目录项调用 Unlink -> 必须返回 ErrIsDir (EISDIR)
		chosen := dirEntries[env.Rng.IntN(len(dirEntries))]
		caseTag = "对子目录调用Unlink拦截(ErrIsDir)"
		parentIno = chosen.ParentIno
		name = []byte(chosen.Name)

	case branch < 76:
		// 特殊名称 "." / ".." (ErrInvalidArgument) 或空名称 "" (ErrNotFound)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		parentIno = parent.Ino
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "特殊名称(.)(ErrInvalidArgument)"
			name = []byte(".")
		case 1:
			caseTag = "特殊名称(..)(ErrInvalidArgument)"
			name = []byte("..")
		default:
			caseTag = "空名称(ErrNotFound)"
			name = []byte("")
		}

	case branch < 85:
		// 不存在的目录项名称 (ErrNotFound)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		caseTag = "不存在的名称(ErrNotFound)"
		parentIno = parent.Ino
		name = []byte(env.UniqueEntryName("nonexistent_unlink"))

	case branch < 93:
		// 父节点非目录 (ErrNotDir) 或父节点不存在/孤儿父目录 (ErrNotFound)
		nonDirs := env.Model.ActiveNonDirs()
		orphanDirs := findOrphanDirsInModel(env.Model)
		if len(orphanDirs) > 0 && env.Rng.IntN(3) == 0 {
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			parentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
			name = []byte("child")
		} else if len(nonDirs) > 0 && env.Rng.IntN(2) == 0 {
			target := nonDirs[env.Rng.IntN(len(nonDirs))]
			if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
				caseTag = "父节点非目录(ErrNotDir)"
				parentIno = target.Ino
				name = []byte("child")
			} else {
				caseTag = "父节点不存在(ErrNotFound)"
				parentIno = uint64(math.MaxInt64 - 357913)
				name = []byte("child")
			}
		} else {
			caseTag = "父节点不存在(ErrNotFound)"
			parentIno = uint64(math.MaxInt64 - 357913)
			name = []byte("child")
		}

	default:
		// 非法静态入参：parentIno == 0、含 '/'、含 '\x00'、超长名称 (>255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			parentIno = 0
			name = []byte("file")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			parentIno = parent.Ino
			name = []byte("bad/unlink")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			parentIno = parent.Ino
			name = []byte("bad\x00unlink")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			parentIno = parent.Ino
			name = []byte(strings.Repeat("u", meta.MaxFileNameBytes+1+env.Rng.IntN(16)))
		}
	}

	targetChild, expectedErr := formalPredictUnlink(env.Model, parentIno, name)
	err := env.Mgr.Unlink(ctx, parentIno, name)

	if expectedErr != nil {
		require.Errorf(t, err, "Unlink[%s] (parent=%d, name=%q) 预期错误 %v，但实际成功",
			caseTag, parentIno, string(name), expectedErr)
		require.Truef(t, errors.Is(err, expectedErr),
			"Unlink[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		return fmt.Sprintf("Unlink[%s](parent=%d, name=%q) -> 预期拒绝 (%v)",
			caseTag, parentIno, string(name), expectedErr)
	}

	require.NoErrorf(t, err, "Unlink[%s] (parent=%d, name=%q) 预期成功，但返回错误: %v",
		caseTag, parentIno, string(name), err)
	require.NotNil(t, targetChild)
	require.GreaterOrEqual(t, targetChild.Nlink, uint32(1), "Unlink 前目标非目录节点 nlink 必须 >= 1")

	// 同步更新内存参考模型：
	// 1. 删除 entries 映射；
	// 2. 递减目标节点 nlink - 1；
	// 3. 若递减后 nlink == 0，调用 ReclaimOrphanInModel（无有效句柄时当场物理删除并退还 used_inodes 及 S_IFREG 的 used_bytes；若有有效句柄则保留为孤儿节点）。
	delete(env.Model.Entries, formalEntryKey{ParentIno: parentIno, Name: string(name)})
	targetChild.Nlink--
	reclaimed := false
	if targetChild.Nlink == 0 {
		reclaimed = env.Model.ReclaimOrphanInModel(targetChild.Ino)
	}

	return fmt.Sprintf("Unlink[%s](parent=%d, name=%q, child_ino=%d) -> 成功 (剩余nlink=%d, 物理回收=%v)",
		caseTag, parentIno, string(name), targetChild.Ino, targetChild.Nlink, reclaimed)
}

// ============================================================================
// 3. T4 底表数学不变量审计：非目录 INV-2 与跨盘拓扑隔离 INV-5
// ============================================================================

// formalAuditINV2NonDirNlink 强制审计底表数学不变量 非目录 INV-2（非目录硬链接计数守恒）：
// 1. 扫描底表 inodes 与 entries，断言任意非目录 Inode ((mode & S_IFMT) != S_IFDIR) 恒满足：
//    inodes.nlink == count(entries WHERE ino = i.ino)；
// 2. 核对底表中每个非目录 Inode 的 nlink、parent_ino == 0、root_ino 与内存参考模型 100% 一致；
// 3. 核对已物理回收的非目录节点在底表 inodes 与 entries 中零残留。
func formalAuditINV2NonDirNlink(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	dbInodes, dbEntries := loadFormalAuditTopologySnapshot(ctx, t, env)

	// 统计底表 entries 中指向每个 child_ino 的硬链接条目数
	dbEntryCountByIno := make(map[uint64]int32)
	for _, e := range dbEntries {
		dbEntryCountByIno[e.ChildIno]++
	}

	// 统计内存参考模型 Entries 中指向每个 child_ino 的硬链接条目数
	modelEntryCountByIno := make(map[uint64]uint32)
	for _, childIno := range env.Model.Entries {
		modelEntryCountByIno[childIno]++
	}

	for ino, row := range dbInodes {
		if row.IsDir() {
			continue
		}

		actualEntries := dbEntryCountByIno[ino]
		require.GreaterOrEqualf(t, row.Nlink, int32(0),
			"非目录 INV-2 违例: 非目录节点 ino=%d (mode=0o%o) 的 nlink (%d) < 0",
			ino, row.Mode, row.Nlink)
		require.Equalf(t, actualEntries, row.Nlink,
			"非目录 INV-2 违例: 非目录节点 ino=%d (mode=0o%o, root_ino=%d) 的 inodes.nlink (%d) != count(entries WHERE ino=%d) (%d)",
			ino, row.Mode, row.RootIno, row.Nlink, ino, actualEntries)
		require.Zerof(t, row.ParentIno,
			"非目录 INV-2 违例: 非目录节点 ino=%d 的 inodes.parent_ino (%d) 必须恒为 0",
			ino, row.ParentIno)

		modelNode, exists := env.Model.Inodes[ino]
		require.Truef(t, exists && !modelNode.Deleted,
			"非目录 INV-2 违例: 底表存在非目录节点 ino=%d，但内存模型中缺失或已标记删除", ino)
		require.Equalf(t, int32(modelNode.Nlink), row.Nlink,
			"非目录 INV-2 违例: 非目录节点 ino=%d 底表 nlink (%d) != 内存模型 Nlink (%d)",
			ino, row.Nlink, modelNode.Nlink)
		require.Equalf(t, modelEntryCountByIno[ino], modelNode.Nlink,
			"非目录 INV-2 违例: 非目录节点 ino=%d 内存模型 Nlink (%d) != 内存模型 Entries 计数 (%d)",
			ino, modelNode.Nlink, modelEntryCountByIno[ino])
	}

	// 反向核验：内存模型中所有未删除非目录节点均在底表中存在，所有已删除节点在底表 inodes 与 entries 中均零残留
	for _, ino := range env.Model.AllCreatedInos {
		if env.Model.DeletedInos[ino] {
			_, inDB := dbInodes[ino]
			require.Falsef(t, inDB, "非目录 INV-2 违例: 已物理删除节点 ino=%d 仍在底表 inodes 中残留", ino)
			require.Zerof(t, dbEntryCountByIno[ino],
				"非目录 INV-2 违例: 已物理删除节点 ino=%d 仍在底表 entries 中残留 %d 条记录",
				ino, dbEntryCountByIno[ino])
			continue
		}
		if mNode, ok := env.Model.Inodes[ino]; ok && !mNode.IsDir() {
			_, inDB := dbInodes[ino]
			require.Truef(t, inDB, "非目录 INV-2 违例: 内存模型未删除非目录节点 ino=%d 在底表 inodes 中缺失", ino)
		}
	}
}

// formalAuditINV5CrossDiskIsolation 强制审计底表数学不变量 INV-5（跨盘拓扑隔离）：
// 直接通过 SQL 扫描 entries JOIN 父子 inodes，断言 entries 表中任意绑定关系 (parent_ino, child_ino) 恒满足：
// 1. parent.root_ino == child.root_ino（严禁任何跨虚拟磁盘的目录项绑定）；
// 2. 父节点必为存活目录 ((parent.mode & S_IFMT) == S_IFDIR && parent.nlink > 0)；
// 3. 子节点的 nlink > 0（entries 中绑定的任何节点绝不可能处于 nlink == 0 孤儿态）。
func formalAuditINV5CrossDiskIsolation(ctx context.Context, t *testing.T, env *formalMainEnv) {
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
			e.parent_ino,
			e.name,
			e.ino,
			p.root_ino AS parent_root_ino,
			p.mode     AS parent_mode,
			p.nlink    AS parent_nlink,
			c.root_ino AS child_root_ino,
			c.mode     AS child_mode,
			c.nlink    AS child_nlink
		FROM entries e
		JOIN inodes p ON p.ino = e.parent_ino
		JOIN inodes c ON c.ino = e.ino
		WHERE p.root_ino = ANY($1) OR c.root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "INV-5 审计: 查询 entries JOIN 父子 inodes 失败")
	defer rows.Close()

	for rows.Next() {
		var parentIno, childIno, parentRootIno, childRootIno int64
		var parentMode, parentNlink, childMode, childNlink int32
		var nameBytes []byte

		err := rows.Scan(
			&parentIno,
			&nameBytes,
			&childIno,
			&parentRootIno,
			&parentMode,
			&parentNlink,
			&childRootIno,
			&childMode,
			&childNlink,
		)
		require.NoError(t, err, "INV-5 审计: 扫描行失败")

		nameStr := string(nameBytes)
		require.Equalf(t, parentRootIno, childRootIno,
			"INV-5 违例: 目录项 (parent_ino=%d, name=%q, child_ino=%d) 发生跨盘绑定: parent.root_ino=%d != child.root_ino=%d",
			parentIno, nameStr, childIno, parentRootIno, childRootIno)
		require.Equalf(t, uint32(meta.S_IFDIR), uint32(parentMode)&meta.S_IFMT,
			"INV-5 违例: 目录项 (parent_ino=%d, name=%q) 的父节点不是目录 (mode=0o%o)",
			parentIno, nameStr, parentMode)
		require.Greaterf(t, parentNlink, int32(0),
			"INV-5 违例: 目录项 (parent_ino=%d, name=%q) 的父目录处于孤儿状态 (nlink=%d)",
			parentIno, nameStr, parentNlink)
		require.Greaterf(t, childNlink, int32(0),
			"INV-5 违例: 目录项 (parent_ino=%d, name=%q, child_ino=%d) 的子节点 nlink (%d) <= 0",
			parentIno, nameStr, childIno, childNlink)
	}
	require.NoError(t, rows.Err())
}

// ============================================================================
// 4. T4 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T4_LinkUnlink 验证 T4 主状态机 Link/Unlink 硬链接增减对拍与底表非目录 INV-2、INV-5 审计。
func TestFormalMain_T4_LinkUnlink(t *testing.T) {
	t.Run("确定性边界_同目录与跨目录硬链接_多链接渐进Unlink_Symlink与Reg配额退还隔离_unlink-whilst-open_TmpFile一次性具名化_EMLINK_EPERM_EXDEV_EISDIR全矩阵验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 40,
			Seed:       0x20260421,
			Rng:        newFormalRNG(0x20260421),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t4-det",
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
			formalAuditINV1DirTopology(ctx, t, env)
			formalAuditINV2DirNlink(ctx, t, env)
			formalAuditINV2NonDirNlink(ctx, t, env)
			formalAuditINV4DiskQuota(ctx, t, env)
			formalAuditNodeAndSymlinkQuota(ctx, t, env)
			formalAuditINV5CrossDiskIsolation(ctx, t, env)
		}

		// 1. 创建 2 块独立虚拟磁盘 disk1 与 disk2，构造跨目录与跨盘测试拓扑
		formalCreateValidDiskStep(ctx, t, env)
		formalCreateValidDiskStep(ctx, t, env)
		disks := env.Model.ActiveDisks()
		require.Len(t, disks, 2)
		disk1, disk2 := disks[0], disks[1]
		root1, root2 := disk1.RootIno, disk2.RootIno
		rootDir1 := env.Model.Inodes[root1]
		rootDir2 := env.Model.Inodes[root2]

		// 在 disk1 下创建子目录 dirA 与 dirB，在 disk2 下创建子目录 dirDisk2
		dirA, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: root1, Name: []byte("dirA"), Mode: 0o755})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: dirA.Ino, Generation: dirA.Generation, RootIno: root1, ParentIno: root1,
			Mode: dirA.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirA"}] = dirA.Ino
		rootDir1.Nlink++
		disk1.UsedInodes++

		dirB, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: root1, Name: []byte("dirB"), Mode: 0o755})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: dirB.Ino, Generation: dirB.Generation, RootIno: root1, ParentIno: root1,
			Mode: dirB.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirB"}] = dirB.Ino
		rootDir1.Nlink++
		disk1.UsedInodes++

		dirDisk2, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: root2, Name: []byte("dirDisk2"), Mode: 0o755})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: dirDisk2.Ino, Generation: dirDisk2.Generation, RootIno: root2, ParentIno: root2,
			Mode: dirDisk2.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: root2, Name: "dirDisk2"}] = dirDisk2.Ino
		rootDir2.Nlink++
		disk2.UsedInodes++
		runAllProbesAndAuditors()

		// 2. 在 disk1/dirA 下创建非零大小的常规文件 reg1 (size=4096)、符号链接 sym1 (size=256) 与 FIFO 节点 fifo1
		reg1Node, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: dirA.Ino, Name: []byte("reg1"), Mode: meta.S_IFREG | 0o644, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		regSize := uint64(4096)
		reg1Node, err = mgr.SetAttr(ctx, reg1Node.Ino, meta.SetAttrInput{Size: &regSize})
		require.NoError(t, err)
		reg1Model := &formalInode{
			Ino: reg1Node.Ino, Generation: reg1Node.Generation, RootIno: root1, ParentIno: 0,
			Mode: reg1Node.Attr.Mode, UID: 1001, GID: 1002, Size: regSize, Nlink: 1,
		}
		env.Model.RecordNewInode(reg1Model)
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "reg1"}] = reg1Node.Ino
		disk1.UsedInodes++
		disk1.UsedBytes += regSize

		symTarget := []byte(strings.Repeat("s", 256))
		sym1Node, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: dirA.Ino, Name: []byte("sym1"), Target: symTarget, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		sym1Model := &formalInode{
			Ino: sym1Node.Ino, Generation: sym1Node.Generation, RootIno: root1, ParentIno: 0,
			Mode: sym1Node.Attr.Mode, UID: 1001, GID: 1002, Size: 256, Nlink: 1, SymlinkTarget: symTarget,
		}
		env.Model.RecordNewInode(sym1Model)
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "sym1"}] = sym1Node.Ino
		disk1.UsedInodes++

		fifo1Node, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: dirA.Ino, Name: []byte("fifo1"), Mode: meta.S_IFIFO | 0o640, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		fifo1Model := &formalInode{
			Ino: fifo1Node.Ino, Generation: fifo1Node.Generation, RootIno: root1, ParentIno: 0,
			Mode: fifo1Node.Attr.Mode, UID: 1001, GID: 1002, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fifo1Model)
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "fifo1"}] = fifo1Node.Ino
		disk1.UsedInodes++
		runAllProbesAndAuditors()

		// 3. 验证同目录与跨父目录硬链接创建（nlink + 1，ParentIno 恒为 0，used_inodes 与 used_bytes 均不变）
		usedInodesBeforeLink := disk1.UsedInodes
		usedBytesBeforeLink := disk1.UsedBytes

		// 3.1 reg1: 同目录 dirA/reg1_same (nlink=2) -> 跨目录 dirB/reg1_cross (nlink=3) -> 根目录 root1/reg1_root (nlink=4)
		gotLink, err := mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: dirA.Ino, NewName: []byte("reg1_same")})
		require.NoError(t, err)
		require.Equal(t, uint32(2), gotLink.Attr.Nlink)
		require.Equal(t, uint64(0), gotLink.Attr.ParentIno)
		reg1Model.Nlink = 2
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "reg1_same"}] = reg1Model.Ino

		gotLink, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: dirB.Ino, NewName: []byte("reg1_cross")})
		require.NoError(t, err)
		require.Equal(t, uint32(3), gotLink.Attr.Nlink)
		require.Equal(t, uint64(0), gotLink.Attr.ParentIno)
		reg1Model.Nlink = 3
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "reg1_cross"}] = reg1Model.Ino

		gotLink, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: root1, NewName: []byte("reg1_root")})
		require.NoError(t, err)
		require.Equal(t, uint32(4), gotLink.Attr.Nlink)
		reg1Model.Nlink = 4
		env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "reg1_root"}] = reg1Model.Ino

		// 3.2 sym1 与 fifo1 跨父目录创建硬链接 (nlink: 1 -> 2)
		gotSymLink, err := mgr.Link(ctx, meta.LinkParams{Ino: sym1Model.Ino, NewParentIno: dirB.Ino, NewName: []byte("sym1_cross")})
		require.NoError(t, err)
		require.Equal(t, uint32(2), gotSymLink.Attr.Nlink)
		sym1Model.Nlink = 2
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "sym1_cross"}] = sym1Model.Ino

		gotFifoLink, err := mgr.Link(ctx, meta.LinkParams{Ino: fifo1Model.Ino, NewParentIno: dirB.Ino, NewName: []byte("fifo1_cross")})
		require.NoError(t, err)
		require.Equal(t, uint32(2), gotFifoLink.Attr.Nlink)
		fifo1Model.Nlink = 2
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "fifo1_cross"}] = fifo1Model.Ino

		require.Equal(t, usedInodesBeforeLink, disk1.UsedInodes, "Link 严禁改变 used_inodes")
		require.Equal(t, usedBytesBeforeLink, disk1.UsedBytes, "Link 严禁改变 used_bytes")
		runAllProbesAndAuditors()

		// 4. 验证 Link 错误码边界与校验优先级
		// 4.1 对目录源节点调用 Link 优先返回 ErrPerm (EPERM)，即使跨 Disk 也是先判目录返回 ErrPerm
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: dirA.Ino, NewParentIno: dirB.Ino, NewName: []byte("dir_hardlink")})
		require.ErrorIs(t, err, meta.ErrPerm, "同盘目录硬链接必须返回 ErrPerm")
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: dirA.Ino, NewParentIno: dirDisk2.Ino, NewName: []byte("cross_dir_hardlink")})
		require.ErrorIs(t, err, meta.ErrPerm, "跨盘目录硬链接必须优先返回 ErrPerm 而非 ErrCrossDevice")

		// 4.2 对非目录源节点发起跨 Disk 的 Link 返回 ErrCrossDevice (EXDEV)
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: root2, NewName: []byte("cross_reg")})
		require.ErrorIs(t, err, meta.ErrCrossDevice, "跨盘常规文件 Link 必须返回 ErrCrossDevice")
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: sym1Model.Ino, NewParentIno: dirDisk2.Ino, NewName: []byte("cross_sym")})
		require.ErrorIs(t, err, meta.ErrCrossDevice, "跨盘符号链接 Link 必须返回 ErrCrossDevice")

		// 4.3 目标名称已存在或 "." / ".." 返回 ErrAlreadyExists (EEXIST)
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: dirA.Ino, NewName: []byte("reg1")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: dirA.Ino, NewName: []byte(".")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: dirA.Ino, NewName: []byte("..")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)

		// 4.4 非目录父节点返回 ErrNotDir，不存在父节点返回 ErrNotFound
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: fifo1Model.Ino, NewParentIno: reg1Model.Ino, NewName: []byte("under_file")})
		require.ErrorIs(t, err, meta.ErrNotDir)
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: fifo1Model.Ino, NewParentIno: uint64(math.MaxInt64 - 111), NewName: []byte("ghost_parent")})
		require.ErrorIs(t, err, meta.ErrNotFound)
		runAllProbesAndAuditors()

		// 5. 验证匿名 TmpFile 一次性 Link 具名化、清除 O_TMPFILE 标志位、防止解链后二次复活 (兼容 T8)
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: dirA.Ino, Mode: 0o640, Flags: meta.O_RDWR, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(0), tmpRes.Inode.Attr.Nlink)
		tmpSize := uint64(2048)
		_, err = mgr.SetAttr(ctx, tmpRes.Inode.Ino, meta.SetAttrInput{Size: &tmpSize})
		require.NoError(t, err)

		tmpModel := &formalInode{
			Ino: tmpRes.Inode.Ino, Generation: tmpRes.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: tmpRes.Inode.Attr.Mode, UID: 1001, GID: 1002, Size: tmpSize, Nlink: 0, IsTmpFile: true,
		}
		env.Model.RecordNewInode(tmpModel)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: tmpRes.Fh, Ino: tmpModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE,
		})
		disk1.UsedInodes++
		disk1.UsedBytes += tmpSize

		// 5.1 跨盘具名化 TmpFile 必须返回 ErrCrossDevice
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: tmpModel.Ino, NewParentIno: root2, NewName: []byte("tmp_cross")})
		require.ErrorIs(t, err, meta.ErrCrossDevice)

		// 5.2 同盘首次 Link 具名化成功：nlink 0 -> 1，清除句柄 O_TMPFILE 位，配额不变
		linkedTmp, err := mgr.Link(ctx, meta.LinkParams{Ino: tmpModel.Ino, NewParentIno: dirB.Ino, NewName: []byte("tmp_named")})
		require.NoError(t, err)
		require.Equal(t, uint32(1), linkedTmp.Attr.Nlink)
		tmpModel.Nlink = 1
		tmpModel.TmpFileLinked = true
		env.Model.ClearTmpFileFlagForIno(tmpModel.Ino)
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "tmp_named"}] = tmpModel.Ino

		// 5.3 在仍持有打开句柄时对其执行 Unlink：nlink 1 -> 0，成为 unlink-whilst-open 孤儿节点，暂不退还配额
		err = mgr.Unlink(ctx, dirB.Ino, []byte("tmp_named"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirB.Ino, Name: "tmp_named"})
		tmpModel.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(tmpModel.Ino), "持有打开句柄时不应物理删除")

		// 5.4 尝试二次 Link 复活该已具名化又被 Unlink 的节点 -> 必须返回 ErrNotFound
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: tmpModel.Ino, NewParentIno: dirB.Ino, NewName: []byte("tmp_resurrect")})
		require.ErrorIs(t, err, meta.ErrNotFound, "已具名化后被 Unlink 至 nlink=0 的节点严禁二次 Link 复活")

		// 5.5 释放句柄 Release -> 触发物理删除并退还 used_inodes - 1 与 used_bytes - 2048
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: tmpModel.Ino, Fh: tmpRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[tmpRes.Fh].Closed = true
		env.Model.ClosedHandles[tmpRes.Fh] = env.Model.Handles[tmpRes.Fh]
		delete(env.Model.Handles, tmpRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(tmpModel.Ino))
		runAllProbesAndAuditors()

		// 5.6 验证携带 O_EXCL 的 TmpFile 严禁通过 Link 具名化 (返回 ErrNotFound)
		tmpExclRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: dirA.Ino, Mode: 0o600, Flags: meta.O_RDWR | meta.O_EXCL, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		tmpExclModel := &formalInode{
			Ino: tmpExclRes.Inode.Ino, Generation: tmpExclRes.Inode.Generation, RootIno: root1, ParentIno: 0,
			Mode: tmpExclRes.Inode.Attr.Mode, UID: 1001, GID: 1002, Size: 0, Nlink: 0, IsTmpFile: true, TmpFileExcl: true,
		}
		env.Model.RecordNewInode(tmpExclModel)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: tmpExclRes.Fh, Ino: tmpExclModel.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE | meta.O_EXCL,
		})
		disk1.UsedInodes++
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: tmpExclModel.Ino, NewParentIno: dirA.Ino, NewName: []byte("excl_forbidden")})
		require.ErrorIs(t, err, meta.ErrNotFound, "携带 O_EXCL 的 TmpFile 尝试 Link 必须返回 ErrNotFound")
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: tmpExclModel.Ino, Fh: tmpExclRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[tmpExclRes.Fh].Closed = true
		env.Model.ClosedHandles[tmpExclRes.Fh] = env.Model.Handles[tmpExclRes.Fh]
		delete(env.Model.Handles, tmpExclRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(tmpExclModel.Ino))
		runAllProbesAndAuditors()

		// 6. 验证多硬链接渐进 Unlink、符号链接解链不减 used_bytes 与常规文件解链配额退还
		// 6.1 解链 sym1 (nlink: 2 -> 1 -> 0)：验证 nlink=1 时保留，nlink=0 时物理删除且仅减 used_inodes 不减 used_bytes
		err = mgr.Unlink(ctx, dirA.Ino, []byte("sym1"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirA.Ino, Name: "sym1"})
		sym1Model.Nlink = 1

		usedBytesBeforeSymDelete := disk1.UsedBytes
		err = mgr.Unlink(ctx, dirB.Ino, []byte("sym1_cross"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirB.Ino, Name: "sym1_cross"})
		sym1Model.Nlink = 0
		require.True(t, env.Model.ReclaimOrphanInModel(sym1Model.Ino))
		require.Equal(t, usedBytesBeforeSymDelete, disk1.UsedBytes, "S_IFLNK 物理删除绝不能扣减 used_bytes")
		runAllProbesAndAuditors()

		// 6.2 解链 fifo1 (nlink: 2 -> 1 -> 0)
		err = mgr.Unlink(ctx, dirA.Ino, []byte("fifo1"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirA.Ino, Name: "fifo1"})
		fifo1Model.Nlink = 1

		err = mgr.Unlink(ctx, dirB.Ino, []byte("fifo1_cross"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirB.Ino, Name: "fifo1_cross"})
		fifo1Model.Nlink = 0
		require.True(t, env.Model.ReclaimOrphanInModel(fifo1Model.Ino))
		runAllProbesAndAuditors()

		// 6.3 解链 reg1 (nlink: 4 -> 3 -> 2 -> 1)：当 nlink > 0 时保留节点且不退还 4096 字节配额
		for _, step := range []struct {
			Parent uint64
			Name   string
			Expect uint32
		}{
			{dirA.Ino, "reg1", 3},
			{dirA.Ino, "reg1_same", 2},
			{dirB.Ino, "reg1_cross", 1},
		} {
			err = mgr.Unlink(ctx, step.Parent, []byte(step.Name))
			require.NoError(t, err)
			delete(env.Model.Entries, formalEntryKey{ParentIno: step.Parent, Name: step.Name})
			reg1Model.Nlink = step.Expect
			require.Equal(t, uint64(4096), disk1.UsedBytes, "nlink > 0 时绝不退还常规文件配额")
		}

		// 6.4 验证 unlink-whilst-open (兼容 T6)：先 Open(reg1) 持有有效 FileHandle，再 Unlink 最后一条硬链接 reg1_root
		openRegRes, err := mgr.Open(ctx, meta.OpenParams{Ino: reg1Model.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: openRegRes.Fh, Ino: reg1Model.Ino, Flags: meta.O_RDONLY,
		})

		usedInodesBeforeLastUnlink := disk1.UsedInodes
		err = mgr.Unlink(ctx, root1, []byte("reg1_root"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "reg1_root"})
		reg1Model.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(reg1Model.Ino), "持有有效 FileHandle 时 Unlink 至 nlink=0 必须保留为 Orphan Inode")
		require.Equal(t, usedInodesBeforeLastUnlink, disk1.UsedInodes, "Orphan Inode 存活期间暂不退还 used_inodes")
		require.Equal(t, uint64(4096), disk1.UsedBytes, "Orphan Inode 存活期间暂不退还 used_bytes")
		runAllProbesAndAuditors()

		// 尝试对普通 unlink-whilst-open 孤儿节点调用 Link 必须返回 ErrNotFound
		_, err = mgr.Link(ctx, meta.LinkParams{Ino: reg1Model.Ino, NewParentIno: root1, NewName: []byte("resurrect_orphan")})
		require.ErrorIs(t, err, meta.ErrNotFound, "普通 nlink=0 孤儿节点调用 Link 必须返回 ErrNotFound")

		// 关闭 reg1 句柄 Release -> 当场物理删除孤儿节点并退还 used_inodes - 1 与 used_bytes - 4096
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: reg1Model.Ino, Fh: openRegRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[openRegRes.Fh].Closed = true
		env.Model.ClosedHandles[openRegRes.Fh] = env.Model.Handles[openRegRes.Fh]
		delete(env.Model.Handles, openRegRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(reg1Model.Ino))
		require.Equal(t, uint64(0), disk1.UsedBytes, "常规文件物理删除后 used_bytes 必须精确归零")
		runAllProbesAndAuditors()

		// 6.5 再创建一个单硬链接常规文件 reg2 (size=1024)，验证无打开句柄时 Unlink 直接当场物理删除并退还配额
		reg2Node, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: dirA.Ino, Name: []byte("reg2_direct_unlink"), Mode: meta.S_IFREG | 0o644,
		})
		require.NoError(t, err)
		reg2Size := uint64(1024)
		reg2Node, err = mgr.SetAttr(ctx, reg2Node.Ino, meta.SetAttrInput{Size: &reg2Size})
		require.NoError(t, err)
		reg2Model := &formalInode{
			Ino: reg2Node.Ino, Generation: reg2Node.Generation, RootIno: root1, ParentIno: 0,
			Mode: reg2Node.Attr.Mode, Size: reg2Size, Nlink: 1,
		}
		env.Model.RecordNewInode(reg2Model)
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "reg2_direct_unlink"}] = reg2Node.Ino
		disk1.UsedInodes++
		disk1.UsedBytes += reg2Size
		runAllProbesAndAuditors()

		err = mgr.Unlink(ctx, dirA.Ino, []byte("reg2_direct_unlink"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: dirA.Ino, Name: "reg2_direct_unlink"})
		reg2Model.Nlink = 0
		require.True(t, env.Model.ReclaimOrphanInModel(reg2Model.Ino), "无句柄单链接常规文件 Unlink 后必须当场物理回收")
		require.Equal(t, uint64(0), disk1.UsedBytes)
		runAllProbesAndAuditors()

		// 7. 验证 Unlink 对目录返回 ErrIsDir (EISDIR)，对 "."/".." 返回 ErrInvalidArgument，对不存在名称及孤儿父目录返回 ErrNotFound
		err = mgr.Unlink(ctx, root1, []byte("dirA"))
		require.ErrorIs(t, err, meta.ErrIsDir, "对子目录调用 Unlink 必须返回 ErrIsDir")
		err = mgr.Unlink(ctx, root1, []byte("."))
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		err = mgr.Unlink(ctx, root1, []byte(".."))
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		err = mgr.Unlink(ctx, root1, []byte(""))
		require.ErrorIs(t, err, meta.ErrNotFound)
		err = mgr.Unlink(ctx, root1, []byte("no_such_entry"))
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 孤儿父目录 (nlink == 0) 调用 Link 与 Unlink 均返回 ErrNotFound
		openDirB, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: dirB.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{Fh: openDirB.Fh, Ino: dirB.Ino, Flags: openDirB.OpenFlags, IsDir: true})
		err = mgr.Rmdir(ctx, root1, []byte("dirB"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: root1, Name: "dirB"})
		rootDir1.Nlink--
		env.Model.Inodes[dirB.Ino].Nlink = 0

		tempFileInA, err := mgr.Mknod(ctx, meta.MknodParams{ParentIno: dirA.Ino, Name: []byte("f_in_a"), Mode: meta.S_IFREG | 0o644})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino: tempFileInA.Ino, Generation: tempFileInA.Generation, RootIno: root1, ParentIno: 0,
			Mode: tempFileInA.Attr.Mode, Nlink: 1,
		})
		env.Model.Entries[formalEntryKey{ParentIno: dirA.Ino, Name: "f_in_a"}] = tempFileInA.Ino
		disk1.UsedInodes++

		_, err = mgr.Link(ctx, meta.LinkParams{Ino: tempFileInA.Ino, NewParentIno: dirB.Ino, NewName: []byte("in_orphan_dir")})
		require.ErrorIs(t, err, meta.ErrNotFound, "孤儿父目录下调用 Link 必须返回 ErrNotFound")
		err = mgr.Unlink(ctx, dirB.Ino, []byte("any"))
		require.ErrorIs(t, err, meta.ErrNotFound, "孤儿父目录下调用 Unlink 必须返回 ErrNotFound")

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: dirB.Ino, Fh: openDirB.Fh})
		require.NoError(t, err)
		env.Model.Handles[openDirB.Fh].Closed = true
		env.Model.ClosedHandles[openDirB.Fh] = env.Model.Handles[openDirB.Fh]
		delete(env.Model.Handles, openDirB.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(dirB.Ino))
		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_Link与Unlink多硬链接增减_跨盘EXDEV拦截与底表INV-2_INV-5全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			InitHook: func(ctx context.Context, t *testing.T, env *formalMainEnv) {
				// 初始创建 2 块虚拟磁盘，确保从第 1 步起即具备跨盘 EXDEV 与同盘多子目录碰撞条件
				env.StepIndex = 0
				d1 := formalCreateValidDiskStep(ctx, t, env)
				d2 := formalCreateValidDiskStep(ctx, t, env)
				env.AppendTrace(0, fmt.Sprintf("[初始双盘] %s; %s", d1, d2))
			},
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 8, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 8, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 12, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 15, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 10, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 18, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 15, Fn: formalActionSymlink},
				{Name: "Link", Weight: 30, Fn: formalActionLink},
				{Name: "Unlink", Weight: 25, Fn: formalActionUnlink},
			},
			Probes: []formalNamedProbe{
				{Name: "GetDisk", Fn: formalProbeGetDisk},
				{Name: "StatFS", Fn: formalProbeStatFS},
				{Name: "Lookup", Fn: formalProbeLookup},
				{Name: "GetAttr", Fn: formalProbeGetAttr},
				{Name: "Readlink", Fn: formalProbeReadlink},
			},
			Auditors: []formalNamedAuditor{
				{Name: "INV-1_DirTopology", Fn: formalAuditINV1DirTopology},
				{Name: "INV-2_DirNlink", Fn: formalAuditINV2DirNlink},
				{Name: "INV-2_NonDirNlink", Fn: formalAuditINV2NonDirNlink},
				{Name: "INV-4_DiskQuota", Fn: formalAuditINV4DiskQuota},
				{Name: "INV-4_NodeAndSymlinkQuota", Fn: formalAuditNodeAndSymlinkQuota},
				{Name: "INV-5_CrossDiskIsolation", Fn: formalAuditINV5CrossDiskIsolation},
			},
		})
	})
}
