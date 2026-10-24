package meta_test

import (
	"bytes"
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
	// 注册 T7 核心目录流动作：OpenDir、ReleaseDir
	registerFormalAction("OpenDir", 25, formalActionOpenDir)
	registerFormalAction("ReleaseDir", 22, formalActionReleaseDir)

	// 注册 T7 核心目录流只读查账探针：ReadDir、ReadDirPlus、FsyncDir
	registerFormalProbe("ReadDir", formalProbeReadDir)
	registerFormalProbe("ReadDirPlus", formalProbeReadDirPlus)
	registerFormalProbe("FsyncDir", formalProbeFsyncDir)
}

// ============================================================================
// 1. T7 参考模型预测器与目录流合成辅助函数
// ============================================================================

// formalPredictOpenDir 根据内存参考模型预测 OpenDir 的目标目录节点与预期错误码。
// 严格对齐 manager_create_open.go 中 OpenDir 的静态校验、目录类型检查及孤儿目录句柄存续检查。
func formalPredictOpenDir(
	m *formalMainModel,
	params meta.OpenDirParams,
) (targetDir *formalInode, expectedErr error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		(params.Flags&meta.O_ACCMODE) != meta.O_RDONLY ||
		(params.Flags&meta.O_TRUNC) != 0 {
		return nil, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return nil, err
	}

	inode, exists := m.Inodes[params.Ino]
	if !exists || inode.Deleted {
		return nil, meta.ErrNotFound
	}
	if !inode.IsDir() {
		return nil, meta.ErrNotDir
	}

	// rmdir-whilst-open 孤儿目录重打开检查：nlink == 0 时必须仍存在至少一条有效句柄，否则返回 ErrNotFound
	if inode.Nlink == 0 {
		if !m.HasValidHandlesForIno(inode.Ino) {
			return nil, meta.ErrNotFound
		}
	}

	return inode, nil
}

// formalPredictReleaseDir 根据内存参考模型预测 ReleaseDir 的目标目录节点、目标目录句柄与预期错误码。
// 严格对齐 manager_handle_session.go 中 ReleaseDir 的静态校验、非目录拦截、会话续期与句柄匹配删除逻辑。
func formalPredictReleaseDir(
	m *formalMainModel,
	params meta.ReleaseDirParams,
) (targetDir *formalInode, targetHandle *formalHandle, expectedErr error) {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh == 0 || params.Fh > math.MaxInt64 {
		return nil, nil, meta.ErrInvalidArgument
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return nil, nil, err
	}

	inode, exists := m.Inodes[params.Ino]
	if !exists || inode.Deleted {
		return nil, nil, meta.ErrNotFound
	}
	if !inode.IsDir() {
		return nil, nil, meta.ErrNotDir
	}

	h, hExists := m.Handles[params.Fh]
	if !hExists || h.Closed || h.Ino != params.Ino || !h.IsDir {
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

// formalExpectedDirEntryItem 表示内存参考模型推导出的单条目录项 (DirEntry) 期望值。
type formalExpectedDirEntryItem struct {
	Ino        uint64
	Name       []byte
	Mode       uint32
	NextOffset uint64
	Node       *formalInode
}

// formalBuildExpectedDirEntries 根据内存参考模型构建指定目录在 Offset=0 全量读取时的期望条目序列。
// - 若为孤儿目录 (dir.Nlink == 0)，返回空切片；
// - 若为存活目录 (dir.Nlink > 0)：
//   - 第 0 项 (NextOffset = 1): "." -> dir 自身；
//   - 第 1 项 (NextOffset = 2): ".." -> dir.ParentIno 对应的父目录（根目录为自身）；
//   - 第 2+i 项 (NextOffset = i+3): 按原始字节字典序升序 (bytes.Compare) 排列的子目录项。
func formalBuildExpectedDirEntries(m *formalMainModel, dir *formalInode) []formalExpectedDirEntryItem {
	if dir == nil || dir.Deleted || dir.Nlink == 0 {
		return []formalExpectedDirEntryItem{}
	}

	parentNode, ok := m.Inodes[dir.ParentIno]
	if !ok || parentNode.Deleted {
		parentNode = dir
	}

	childKeys := m.ChildEntriesOf(dir.Ino)
	items := make([]formalExpectedDirEntryItem, 0, 2+len(childKeys))

	items = append(items, formalExpectedDirEntryItem{
		Ino:        dir.Ino,
		Name:       []byte("."),
		Mode:       dir.Mode,
		NextOffset: 1,
		Node:       dir,
	})
	items = append(items, formalExpectedDirEntryItem{
		Ino:        parentNode.Ino,
		Name:       []byte(".."),
		Mode:       parentNode.Mode,
		NextOffset: 2,
		Node:       parentNode,
	})

	for i, ek := range childKeys {
		childIno := m.Entries[ek]
		childNode := m.Inodes[childIno]
		items = append(items, formalExpectedDirEntryItem{
			Ino:        childNode.Ino,
			Name:       []byte(ek.Name),
			Mode:       childNode.Mode,
			NextOffset: uint64(i + 3),
			Node:       childNode,
		})
	}

	return items
}

// ============================================================================
// 2. T7 动作实现：OpenDir 与 ReleaseDir 随机操作生成与逐操作对拍
// ============================================================================

// formalActionOpenDir 在状态机中随机生成存活目录打开、rmdir-whilst-open 活跃孤儿目录重打开、
// 以及非目录节点拦截 (ErrNotDir)、非法标志位拦截 (ErrInvalidArgument)、无句柄孤儿/不存在节点拦截 (ErrNotFound) 的 OpenDir 请求并与参考模型对拍。
func formalActionOpenDir(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 梳理孤儿目录：仍持有有效句柄的孤儿目录 vs 已失去全部有效句柄的孤儿目录
	var openableOrphanDirs []*formalInode
	var deadOrphanDirs []*formalInode
	for _, od := range findOrphanDirsInModel(env.Model) {
		if env.Model.HasValidHandlesForIno(od.Ino) {
			openableOrphanDirs = append(openableOrphanDirs, od)
		} else {
			deadOrphanDirs = append(deadOrphanDirs, od)
		}
	}

	nonDirs := env.Model.ActiveNonDirs()

	var params meta.OpenDirParams
	params.Flags = meta.O_RDONLY
	params.SessionID = pickOptionalFormalSessionID(env)

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 18 && len(openableOrphanDirs) > 0:
		// 重点场景 1：重打开 nlink == 0 但仍持有有效句柄的孤儿目录 (rmdir-whilst-open)
		target := openableOrphanDirs[env.Rng.IntN(len(openableOrphanDirs))]
		caseTag = "重打开nlink=0活跃孤儿目录(新增并发目录句柄)"
		params.Ino = target.Ino
		params.Flags = meta.O_RDONLY

	case branch < 62:
		// 重点场景 2：正常打开存活目录（按概率优先挑选空子目录以促成后续 Rmdir 形成 rmdir-whilst-open，或挑选多子项目录以丰富 ReadDir 分页）
		chosen := activeDirs[env.Rng.IntN(len(activeDirs))]
		if env.Rng.IntN(2) == 0 {
			// 优先挑选非根空子目录（一旦后续被 Rmdir 或 Rename 覆盖即变为活跃孤儿目录）
			for _, d := range activeDirs {
				if d.Ino != d.RootIno && len(env.Model.ChildEntriesOf(d.Ino)) == 0 {
					chosen = d
					break
				}
			}
		} else {
			// 优先挑选子目录项最多的目录以高频锻炼 ReadDir/ReadDirPlus 字典序与分页
			maxChildren := -1
			for _, d := range activeDirs {
				cnt := len(env.Model.ChildEntriesOf(d.Ino))
				if cnt > maxChildren {
					maxChildren = cnt
					chosen = d
				}
			}
		}
		caseTag = "正常只读打开存活目录"
		params.Ino = chosen.Ino
		params.Flags = meta.O_RDONLY

	case branch < 74 && len(nonDirs) > 0:
		// 重点场景 3：对非目录节点（常规文件、符号链接、特殊设备）调用 OpenDir -> ErrNotDir
		target := nonDirs[env.Rng.IntN(len(nonDirs))]
		if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
			caseTag = "对非目录节点调用OpenDir拦截(ErrNotDir)"
			params.Ino = target.Ino
			params.Flags = meta.O_RDONLY
		} else {
			caseTag = "无有效句柄孤儿节点调用OpenDir(ErrNotFound)"
			params.Ino = target.Ino
			params.Flags = meta.O_RDONLY
		}

	case branch < 86:
		// 重点场景 4：非法打开标志位（非只读 O_WRONLY/O_RDWR 或携带 O_TRUNC/O_CREAT/O_EXCL/O_APPEND/O_TMPFILE） -> ErrInvalidArgument
		dir := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.Ino = dir.Ino
		badFlagsPool := []struct {
			tag   string
			flags uint32
		}{
			{tag: "非法Flags含O_WRONLY(ErrInvalidArgument)", flags: meta.O_WRONLY},
			{tag: "非法Flags含O_RDWR(ErrInvalidArgument)", flags: meta.O_RDWR},
			{tag: "非法Flags含O_RDONLY|O_TRUNC(ErrInvalidArgument)", flags: meta.O_RDONLY | meta.O_TRUNC},
			{tag: "非法Flags含O_WRONLY|O_CREAT|O_EXCL(ErrInvalidArgument)", flags: meta.O_WRONLY | meta.O_CREAT | meta.O_EXCL},
			{tag: "非法Flags含O_RDWR|O_APPEND(ErrInvalidArgument)", flags: meta.O_RDWR | meta.O_APPEND},
			{tag: "非法Flags含O_RDWR|O_TMPFILE(ErrInvalidArgument)", flags: meta.O_RDWR | meta.O_TMPFILE},
		}
		picked := badFlagsPool[env.Rng.IntN(len(badFlagsPool))]
		caseTag = picked.tag
		params.Flags = picked.flags

	case branch < 94:
		// 重点场景 5：已物理删除目录、无有效句柄的 nlink==0 孤儿目录、或不存在的 Ino -> ErrNotFound
		params.Flags = meta.O_RDONLY
		if len(deadOrphanDirs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "无有效句柄nlink=0孤儿目录(ErrNotFound)"
			params.Ino = deadOrphanDirs[env.Rng.IntN(len(deadOrphanDirs))].Ino
		} else if len(env.Model.DeletedInos) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "已物理删除节点(ErrNotFound)"
			for ino := range env.Model.DeletedInos {
				params.Ino = ino
				break
			}
		} else {
			caseTag = "不存在的Ino(ErrNotFound)"
			params.Ino = uint64(math.MaxInt64 - 424001)
		}

	default:
		// 重点场景 6：静态非法参数 Ino == 0、Ino > math.MaxInt64、SessionID == 0
		params.Flags = meta.O_RDONLY
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "非法Ino=0(ErrInvalidArgument)"
			params.Ino = 0
		case 1:
			caseTag = "非法Ino超MaxInt64(ErrInvalidArgument)"
			params.Ino = uint64(math.MaxInt64) + 7
		default:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			params.Ino = activeDirs[0].Ino
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		}
	}

	targetDir, expectedErr := formalPredictOpenDir(env.Model, params)
	got, err := env.Mgr.OpenDir(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "OpenDir[%s] (ino=%d, flags=0x%x, sid=%s) 预期错误 %v，但实际成功: %+v",
			caseTag, params.Ino, params.Flags, modelFormatSessionID(params.SessionID), expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"OpenDir[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("OpenDir[%s](ino=%d, flags=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.Flags, expectedErr)
	}

	require.NoErrorf(t, err, "OpenDir[%s] (ino=%d, flags=0x%x) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.Flags, err)
	require.NotNil(t, got)
	require.Greater(t, got.Fh, uint64(0), "OpenDir 分配的目录句柄 Fh 必须 > 0")
	require.NotNil(t, targetDir)

	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)
	env.Model.RecordNewHandle(&formalHandle{
		Fh:        got.Fh,
		Ino:       targetDir.Ino,
		Flags:     params.Flags,
		SessionID: modelCloneSessionID(params.SessionID),
		IsDir:     true,
		Closed:    false,
	})

	return fmt.Sprintf("OpenDir[%s](ino=%d, nlink=%d, flags=0x%x, sid=%s) -> 成功 (fh=%d)",
		caseTag, targetDir.Ino, targetDir.Nlink, params.Flags, modelFormatSessionID(params.SessionID), got.Fh)
}

// formalActionReleaseDir 在状态机中随机生成正常关闭目录句柄、触发 rmdir-whilst-open 孤儿目录最终物理删除与 used_inodes 扣减、
// 以及文件句柄混用/跨目录句柄/已关闭句柄/会话不匹配/非目录节点/非法入参拦截的 ReleaseDir 请求并与参考模型对拍。
func formalActionReleaseDir(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	activeDirHandles := env.Model.ActiveDirHandles()
	activeFileHandles := env.Model.ActiveFileHandles()
	nonDirs := env.Model.ActiveNonDirs()

	var params meta.ReleaseDirParams
	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 48 && len(activeDirHandles) > 0:
		// 正常释放活跃目录句柄（优先按概率挑选 nlink == 0 孤儿目录句柄以高频检验孤儿目录延迟物理回收与配额退还）
		chosen := activeDirHandles[env.Rng.IntN(len(activeDirHandles))]
		if env.Rng.IntN(2) == 0 {
			for _, h := range activeDirHandles {
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
			caseTag = "正常释放孤儿目录句柄(检验nlink=0物理回收与used_inodes扣减)"
		} else {
			caseTag = "正常释放存活目录句柄"
		}

	case branch < 60 && len(env.Model.ClosedHandles) > 0:
		// 对已关闭的目录句柄再次调用 ReleaseDir
		var closedDirHandles []*formalHandle
		for _, fh := range env.Model.AllCreatedFhs {
			if ch, ok := env.Model.ClosedHandles[fh]; ok && ch.IsDir {
				closedDirHandles = append(closedDirHandles, ch)
			}
		}
		if len(closedDirHandles) > 0 {
			chosen := closedDirHandles[env.Rng.IntN(len(closedDirHandles))]
			caseTag = "重复释放已关闭目录句柄"
			params.Ino = chosen.Ino
			params.Fh = chosen.Fh
			params.SessionID = modelCloneSessionID(chosen.SessionID)
		} else {
			caseTag = "释放不存在目录句柄Fh(ErrBadFileHandle)"
			params.Ino = activeDirs[0].Ino
			params.Fh = uint64(math.MaxInt64 - 424111)
		}

	case branch < 72:
		// 句柄不匹配：向目录节点传入常规文件的 Fh、跨目录 Ino 混用其他目录的 Fh、或不存在的 Fh -> ErrBadFileHandle
		targetDir := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.Ino = targetDir.Ino
		if len(activeFileHandles) > 0 && env.Rng.IntN(3) == 0 {
			fileH := activeFileHandles[env.Rng.IntN(len(activeFileHandles))]
			caseTag = "向目录传入常规文件句柄Fh调用ReleaseDir(ErrBadFileHandle)"
			params.Fh = fileH.Fh
			params.SessionID = modelCloneSessionID(fileH.SessionID)
		} else if len(activeDirHandles) > 0 && env.Rng.IntN(2) == 0 {
			var otherDirH *formalHandle
			for _, h := range activeDirHandles {
				if h.Ino != targetDir.Ino {
					otherDirH = h
					break
				}
			}
			if otherDirH != nil {
				caseTag = "跨目录Ino混用目录句柄Fh(ErrBadFileHandle)"
				params.Fh = otherDirH.Fh
				params.SessionID = modelCloneSessionID(otherDirH.SessionID)
			} else {
				caseTag = "不存在的目录句柄Fh(ErrBadFileHandle)"
				params.Fh = uint64(math.MaxInt64 - 424222)
			}
		} else {
			caseTag = "不存在的目录句柄Fh(ErrBadFileHandle)"
			params.Fh = uint64(math.MaxInt64 - 424222)
		}

	case branch < 80 && len(activeDirHandles) > 0 && env.LeaseTTL >= 5*time.Second:
		// SessionID 不匹配 -> ErrBadFileHandle
		chosen := activeDirHandles[env.Rng.IntN(len(activeDirHandles))]
		params.Ino = chosen.Ino
		params.Fh = chosen.Fh
		if chosen.SessionID == nil {
			wrongSID := uint64(1009)
			params.SessionID = &wrongSID
			caseTag = "无会话目录句柄传入非空SessionID不匹配(ErrBadFileHandle)"
		} else {
			if env.Rng.IntN(2) == 0 {
				params.SessionID = nil
				caseTag = "绑定会话目录句柄传入nil-SessionID不匹配(ErrBadFileHandle)"
			} else {
				wrongSID := *chosen.SessionID + 200
				params.SessionID = &wrongSID
				caseTag = "绑定会话目录句柄传入异构SessionID不匹配(ErrBadFileHandle)"
			}
		}

	case branch < 88 && len(nonDirs) > 0:
		// 对非目录节点调用 ReleaseDir -> ErrNotDir
		target := nonDirs[env.Rng.IntN(len(nonDirs))]
		if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
			caseTag = "对非目录节点调用ReleaseDir拦截(ErrNotDir)"
			params.Ino = target.Ino
			params.Fh = 1
			if len(activeFileHandles) > 0 {
				params.Fh = activeFileHandles[0].Fh
			}
		} else {
			caseTag = "不存在或已删除Ino调用ReleaseDir(ErrNotFound)"
			params.Ino = uint64(math.MaxInt64 - 424333)
			params.Fh = 1
		}

	case branch < 94:
		// 对不存在或已物理回收的 Ino 调用 ReleaseDir -> ErrNotFound
		caseTag = "不存在或已删除Ino调用ReleaseDir(ErrNotFound)"
		params.Fh = 1
		if len(env.Model.DeletedInos) > 0 && env.Rng.IntN(2) == 0 {
			for ino := range env.Model.DeletedInos {
				params.Ino = ino
				break
			}
		} else {
			params.Ino = uint64(math.MaxInt64 - 424333)
		}

	default:
		// 静态非法入参：Ino==0、Fh==0、Ino>MaxInt64、SessionID==0
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
			caseTag = "非法Fh超MaxInt64(ErrInvalidArgument)"
			params.Fh = uint64(math.MaxInt64) + 9
		default:
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
		}
	}

	targetDir, targetHandle, expectedErr := formalPredictReleaseDir(env.Model, params)
	err := env.Mgr.ReleaseDir(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "ReleaseDir[%s] (ino=%d, fh=%d, sid=%s) 预期错误 %v，但实际成功",
			caseTag, params.Ino, params.Fh, modelFormatSessionID(params.SessionID), expectedErr)
		require.Truef(t, errors.Is(err, expectedErr),
			"ReleaseDir[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		return fmt.Sprintf("ReleaseDir[%s](ino=%d, fh=%d, sid=%s) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.Fh, modelFormatSessionID(params.SessionID), expectedErr)
	}

	require.NoErrorf(t, err, "ReleaseDir[%s] (ino=%d, fh=%d) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.Fh, err)
	require.NotNil(t, targetDir)
	require.NotNil(t, targetHandle)

	// 同步更新内存参考模型：
	// 1. 同步可选会话续期；
	// 2. 将目录句柄标记为已关闭并移入 ClosedHandles；
	// 3. 若目标目录 nlink == 0，调用 ReclaimOrphanInModel（当且仅当再无任何有效句柄时物理删除孤儿目录并扣减 used_inodes - 1）。
	modelSyncSessionRenewal(env.Model, params.SessionID, env.LeaseTTL)
	targetHandle.Closed = true
	env.Model.ClosedHandles[targetHandle.Fh] = targetHandle
	delete(env.Model.Handles, targetHandle.Fh)

	reclaimed := false
	if targetDir.Nlink == 0 {
		reclaimed = env.Model.ReclaimOrphanInModel(targetDir.Ino)
	}

	return fmt.Sprintf("ReleaseDir[%s](ino=%d, fh=%d, nlink=%d, sid=%s) -> 成功 (孤儿目录物理回收=%v)",
		caseTag, targetDir.Ino, targetHandle.Fh, targetDir.Nlink, modelFormatSessionID(params.SessionID), reclaimed)
}

// ============================================================================
// 3. T7 只读查账探针：ReadDir、ReadDirPlus 与 FsyncDir
// ============================================================================

// assertFormalReadDirEntriesMatches 核对给定有效目录句柄下的 ReadDir 全量读取与分页迭代读取结果。
func assertFormalReadDirEntriesMatches(
	ctx context.Context,
	t *testing.T,
	mgr meta.Manager,
	m *formalMainModel,
	dir *formalInode,
	fh uint64,
	sessionID *uint64,
	verifyPagination bool,
) {
	t.Helper()

	expected := formalBuildExpectedDirEntries(m, dir)

	// 1. 全量读取 (Offset = 0, Limit = MaxReadDirLimit)
	gotAll, err := mgr.ReadDir(ctx, meta.ReadDirParams{
		Ino:       dir.Ino,
		Fh:        fh,
		Offset:    0,
		Limit:     meta.MaxReadDirLimit,
		SessionID: sessionID,
	})
	require.NoErrorf(t, err, "ReadDir 全量读取 (ino=%d, fh=%d, nlink=%d) 不应失败", dir.Ino, fh, dir.Nlink)

	// 孤儿目录 (nlink == 0) 必须返回空列表 (len == 0)
	if dir.Nlink == 0 {
		require.Emptyf(t, gotAll, "孤儿目录 (ino=%d, nlink=0) ReadDir 必须返回空列表", dir.Ino)
		gotFrom1, err1 := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       dir.Ino,
			Fh:        fh,
			Offset:    1,
			Limit:     10,
			SessionID: sessionID,
		})
		require.NoError(t, err1)
		require.Empty(t, gotFrom1, "孤儿目录 Offset=1 ReadDir 必须返回空列表")
		return
	}

	// 存活目录 (nlink > 0) 核对 "."、".." 与按字节序升序排列的子项列表
	require.Lenf(t, gotAll, len(expected),
		"存活目录 ino=%d ReadDir 条目数不匹配: 预期 %d, 实际 %d", dir.Ino, len(expected), len(gotAll))

	// 显式核对第 0 项为 "." (NextOffset=1, Ino==dir.Ino, Type==S_IFDIR)
	require.Equal(t, []byte("."), gotAll[0].Name, "ReadDir 第 0 项必须为 '.'")
	require.Equal(t, dir.Ino, gotAll[0].Ino, "ReadDir '.' 的 Ino 必须等于目录自身 Ino")
	require.Equal(t, uint32(meta.S_IFDIR), gotAll[0].Mode&meta.S_IFMT, "ReadDir '.' 的类型必须为 S_IFDIR")
	require.Equal(t, dir.Mode, gotAll[0].Mode, "ReadDir '.' 的 Mode 必须等于目录自身 Mode")
	require.Equal(t, uint64(1), gotAll[0].NextOffset, "ReadDir '.' 的 NextOffset 必须为 1")

	// 显式核对第 1 项为 ".." (NextOffset=2, Ino==dir.ParentIno, Type==S_IFDIR)
	require.Equal(t, []byte(".."), gotAll[1].Name, "ReadDir 第 1 项必须为 '..'")
	require.Equal(t, dir.ParentIno, gotAll[1].Ino, "ReadDir '..' 的 Ino 必须等于目录的 ParentIno")
	require.Equal(t, uint32(meta.S_IFDIR), gotAll[1].Mode&meta.S_IFMT, "ReadDir '..' 的类型必须为 S_IFDIR")
	require.Equal(t, expected[1].Mode, gotAll[1].Mode, "ReadDir '..' 的 Mode 必须等于父目录 Mode")
	require.Equal(t, uint64(2), gotAll[1].NextOffset, "ReadDir '..' 的 NextOffset 必须为 2")

	// 显式核对 Offset >= 3 的子目录项按名称原始字节字典序升序 (bytes.Compare) 排列
	for i := 2; i < len(gotAll); i++ {
		if i > 2 {
			require.Truef(t, bytes.Compare(gotAll[i-1].Name, gotAll[i].Name) < 0,
				"ReadDir 子项未按字节序升序排列: %q >= %q", string(gotAll[i-1].Name), string(gotAll[i].Name))
		}
		require.Equal(t, expected[i].Name, gotAll[i].Name, "ReadDir 第 %d 项 Name 不一致", i)
		require.Equal(t, expected[i].Ino, gotAll[i].Ino, "ReadDir 第 %d 项 (%q) Ino 不一致", i, string(expected[i].Name))
		require.Equal(t, expected[i].Mode, gotAll[i].Mode, "ReadDir 第 %d 项 (%q) Mode 不一致", i, string(expected[i].Name))
		require.Equal(t, expected[i].NextOffset, gotAll[i].NextOffset, "ReadDir 第 %d 项 (%q) NextOffset 不一致", i, string(expected[i].Name))
	}

	if !verifyPagination {
		return
	}

	// 2. 分页迭代读取核对 (limit = 1 与 limit = 2)
	for _, pageLimit := range []uint32{1, 2} {
		var collected []meta.DirEntry
		var cursor uint64
		for step := 0; step <= len(expected)+2; step++ {
			page, pageErr := mgr.ReadDir(ctx, meta.ReadDirParams{
				Ino:       dir.Ino,
				Fh:        fh,
				Offset:    cursor,
				Limit:     pageLimit,
				SessionID: sessionID,
			})
			require.NoErrorf(t, pageErr, "ReadDir 分页读取 (ino=%d, limit=%d, offset=%d) 不应失败", dir.Ino, pageLimit, cursor)
			require.LessOrEqualf(t, len(page), int(pageLimit), "ReadDir 分页返回条数不应超过 limit=%d", pageLimit)
			if len(page) == 0 {
				break
			}
			collected = append(collected, page...)
			cursor = page[len(page)-1].NextOffset
		}
		require.Equalf(t, gotAll, collected,
			"ReadDir 分页迭代 (limit=%d) 拼接结果与全量读取结果不一致 (ino=%d)", pageLimit, dir.Ino)
	}

	// 3. 越界 Offset 读取必须返回空切片
	beyond, err := mgr.ReadDir(ctx, meta.ReadDirParams{
		Ino:       dir.Ino,
		Fh:        fh,
		Offset:    uint64(len(expected) + 10),
		Limit:     meta.MaxReadDirLimit,
		SessionID: sessionID,
	})
	require.NoError(t, err)
	require.Empty(t, beyond, "越界 Offset 调用 ReadDir 应返回空列表")
}

// assertFormalReadDirPlusEntriesMatches 核对给定有效目录句柄下的 ReadDirPlus 全量读取、每个子项完整 Inode 属性与分页迭代读取结果。
func assertFormalReadDirPlusEntriesMatches(
	ctx context.Context,
	t *testing.T,
	mgr meta.Manager,
	m *formalMainModel,
	dir *formalInode,
	fh uint64,
	sessionID *uint64,
	verifyPagination bool,
) {
	t.Helper()

	expected := formalBuildExpectedDirEntries(m, dir)

	// 1. 全量读取 (Offset = 0, Limit = MaxReadDirLimit)
	gotAll, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
		Ino:       dir.Ino,
		Fh:        fh,
		Offset:    0,
		Limit:     meta.MaxReadDirLimit,
		SessionID: sessionID,
	})
	require.NoErrorf(t, err, "ReadDirPlus 全量读取 (ino=%d, fh=%d, nlink=%d) 不应失败", dir.Ino, fh, dir.Nlink)

	// 孤儿目录 (nlink == 0) 必须返回空列表 (len == 0)
	if dir.Nlink == 0 {
		require.Emptyf(t, gotAll, "孤儿目录 (ino=%d, nlink=0) ReadDirPlus 必须返回空列表", dir.Ino)
		gotFrom1, err1 := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       dir.Ino,
			Fh:        fh,
			Offset:    1,
			Limit:     10,
			SessionID: sessionID,
		})
		require.NoError(t, err1)
		require.Empty(t, gotFrom1, "孤儿目录 Offset=1 ReadDirPlus 必须返回空列表")
		return
	}

	require.Lenf(t, gotAll, len(expected),
		"存活目录 ino=%d ReadDirPlus 条目数不匹配: 预期 %d, 实际 %d", dir.Ino, len(expected), len(gotAll))

	// 核对第 0 项 "."
	require.Equal(t, []byte("."), gotAll[0].Name, "ReadDirPlus 第 0 项必须为 '.'")
	require.Equal(t, dir.Ino, gotAll[0].Ino, "ReadDirPlus '.' 的 Ino 必须等于目录自身 Ino")
	require.Equal(t, uint64(1), gotAll[0].NextOffset, "ReadDirPlus '.' 的 NextOffset 必须为 1")
	require.Equal(t, float64(0), gotAll[0].EntryTimeoutSeconds, "ReadDirPlus '.' 的 EntryTimeoutSeconds 必须为 0")
	assertFormalInodeMatchesEntity(t, dir, gotAll[0].Inode, fmt.Sprintf("ReadDirPlus(ino=%d)['.']", dir.Ino))

	// 核对第 1 项 ".."
	require.Equal(t, []byte(".."), gotAll[1].Name, "ReadDirPlus 第 1 项必须为 '..'")
	require.Equal(t, dir.ParentIno, gotAll[1].Ino, "ReadDirPlus '..' 的 Ino 必须等于目录的 ParentIno")
	require.Equal(t, uint64(2), gotAll[1].NextOffset, "ReadDirPlus '..' 的 NextOffset 必须为 2")
	require.Equal(t, float64(0), gotAll[1].EntryTimeoutSeconds, "ReadDirPlus '..' 的 EntryTimeoutSeconds 必须为 0")
	assertFormalInodeMatchesEntity(t, expected[1].Node, gotAll[1].Inode, fmt.Sprintf("ReadDirPlus(ino=%d)['..']", dir.Ino))

	// 核对 Offset >= 3 的子目录项按字节序升序排列且每个子项 Attr 与参考模型完全一致
	for i := 2; i < len(gotAll); i++ {
		if i > 2 {
			require.Truef(t, bytes.Compare(gotAll[i-1].Name, gotAll[i].Name) < 0,
				"ReadDirPlus 子项未按字节序升序排列: %q >= %q", string(gotAll[i-1].Name), string(gotAll[i].Name))
		}
		require.Equal(t, expected[i].Name, gotAll[i].Name, "ReadDirPlus 第 %d 项 Name 不一致", i)
		require.Equal(t, expected[i].Ino, gotAll[i].Ino, "ReadDirPlus 第 %d 项 (%q) Ino 不一致", i, string(expected[i].Name))
		require.Equal(t, expected[i].NextOffset, gotAll[i].NextOffset, "ReadDirPlus 第 %d 项 (%q) NextOffset 不一致", i, string(expected[i].Name))
		assertFormalInodeMatchesEntity(t, expected[i].Node, gotAll[i].Inode,
			fmt.Sprintf("ReadDirPlus(dir=%d)[%q]", dir.Ino, string(expected[i].Name)))
	}

	if !verifyPagination {
		return
	}

	// 2. 分页迭代读取核对 (limit = 1 与 limit = 2)
	for _, pageLimit := range []uint32{1, 2} {
		var collected []meta.DirEntryPlus
		var cursor uint64
		for step := 0; step <= len(expected)+2; step++ {
			page, pageErr := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
				Ino:       dir.Ino,
				Fh:        fh,
				Offset:    cursor,
				Limit:     pageLimit,
				SessionID: sessionID,
			})
			require.NoErrorf(t, pageErr, "ReadDirPlus 分页读取 (ino=%d, limit=%d, offset=%d) 不应失败", dir.Ino, pageLimit, cursor)
			require.LessOrEqualf(t, len(page), int(pageLimit), "ReadDirPlus 分页返回条数不应超过 limit=%d", pageLimit)
			if len(page) == 0 {
				break
			}
			collected = append(collected, page...)
			cursor = page[len(page)-1].NextOffset
		}
		require.Lenf(t, collected, len(gotAll),
			"ReadDirPlus 分页迭代 (limit=%d) 条目总数与全量读取不一致 (ino=%d)", pageLimit, dir.Ino)
		for i := range gotAll {
			require.Equal(t, gotAll[i].Ino, collected[i].Ino)
			require.Equal(t, gotAll[i].Name, collected[i].Name)
			require.Equal(t, gotAll[i].NextOffset, collected[i].NextOffset)
			assertFormalInodeMatchesEntity(t, expected[i].Node, collected[i].Inode,
				fmt.Sprintf("ReadDirPlus分页(limit=%d, idx=%d)", pageLimit, i))
		}
	}
}

// formalProbeReadDir 在每步写操作后全面核对只读 ReadDir：
// 1. 所有有效目录句柄（含存活目录与 nlink == 0 的 rmdir-whilst-open 孤儿目录）的全量与分页迭代读取；
// 2. 对非目录节点 + 文件句柄调用 ReadDir 返回 ErrNotDir，对目录节点 + 文件句柄/已关闭句柄/跨目录句柄/不匹配 SessionID 返回 ErrBadFileHandle；
// 3. 对 offset < 0 (uint64 越界 > math.MaxInt64) 或 limit == 0 / limit > MaxReadDirLimit 等非法参数返回 ErrInvalidArgument。
func formalProbeReadDir(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	activeDirHandles := env.Model.ActiveDirHandles()
	activeFileHandles := env.Model.ActiveFileHandles()

	// 1. 遍历所有有效目录句柄核对 ReadDir（前 2 个有效目录句柄及所有孤儿目录句柄额外跑完整 limit=1,2 分页遍历）
	for idx, h := range activeDirHandles {
		dir, ok := env.Model.Inodes[h.Ino]
		require.Truef(t, ok && !dir.Deleted && dir.IsDir(),
			"ReadDir 探针: 活跃目录句柄 fh=%d 关联的目录 ino=%d 在参考模型中不存在或非目录", h.Fh, h.Ino)

		verifyPagination := (idx == 0 && env.StepIndex%4 == 0) || dir.Nlink == 0
		assertFormalReadDirEntriesMatches(ctx, t, env.Mgr, env.Model, dir, h.Fh, h.SessionID, verifyPagination)

		// 抽样核对 SessionID 不匹配时返回 ErrBadFileHandle
		if idx == 0 {
			var mismatchedSID *uint64
			if h.SessionID == nil {
				bogus := uint64(888777)
				mismatchedSID = &bogus
			}
			_, errMismatch := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
				Ino:       h.Ino,
				Fh:        h.Fh,
				Offset:    0,
				Limit:     10,
				SessionID: mismatchedSID,
			})
			require.ErrorIs(t, errMismatch, meta.ErrBadFileHandle, "ReadDir 传入不匹配 SessionID 应返回 ErrBadFileHandle")
		}
	}

	// 2. 核对已释放的目录句柄 (ClosedHandles)
	for _, fh := range env.Model.AllCreatedFhs {
		ch, isClosed := env.Model.ClosedHandles[fh]
		if !isClosed || !ch.IsDir {
			continue
		}
		_, err := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       ch.Ino,
			Fh:        ch.Fh,
			Offset:    0,
			Limit:     10,
			SessionID: ch.SessionID,
		})
		if env.Model.DeletedInos[ch.Ino] {
			require.ErrorIs(t, err, meta.ErrNotFound,
				"已物理删除孤儿目录的已关闭句柄调用 ReadDir 应返回 ErrNotFound")
		} else if inode, ok := env.Model.Inodes[ch.Ino]; ok && !inode.Deleted && inode.IsDir() {
			if inode.Nlink > 0 || env.Model.HasValidHandlesForIno(inode.Ino) {
				require.ErrorIs(t, err, meta.ErrBadFileHandle,
					"存活目录的已关闭句柄调用 ReadDir 应返回 ErrBadFileHandle")
			}
		}
	}

	// 3. 核对文件句柄调用 ReadDir：
	// - Ino 为非目录节点 + 文件句柄 Fh -> 先查 Inode 非目录返回 ErrNotDir；
	// - Ino 为目录节点 + 文件句柄 Fh -> Inode 是目录但 fh 不属于该目录返回 ErrBadFileHandle。
	if len(activeFileHandles) > 0 {
		fileH := activeFileHandles[0]
		_, errNotDir := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       fileH.Ino,
			Fh:        fileH.Fh,
			Offset:    0,
			Limit:     10,
			SessionID: fileH.SessionID,
		})
		require.ErrorIs(t, errNotDir, meta.ErrNotDir,
			"对非目录节点与文件句柄调用 ReadDir 必须返回 ErrNotDir")

		activeDirs := env.Model.ActiveDirs()
		if len(activeDirs) > 0 {
			_, errBadFh := env.Mgr.ReadDir(ctx, meta.ReadDirParams{
				Ino:       activeDirs[0].Ino,
				Fh:        fileH.Fh,
				Offset:    0,
				Limit:     10,
				SessionID: fileH.SessionID,
			})
			require.ErrorIs(t, errBadFh, meta.ErrBadFileHandle,
				"对目录 Ino 传入常规文件句柄 Fh 调用 ReadDir 必须返回 ErrBadFileHandle")
		}
	}

	// 4. 核对非法静态边界参数 (offset < 0 补码越界、limit == 0、limit > MaxReadDirLimit、ino == 0、fh == 0、SessionID == 0)
	zeroSID := uint64(0)
	require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: 0, Fh: 1}), meta.ErrInvalidArgument)
	_, err := env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 1, Offset: ^uint64(0), Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDir offset < 0 (^uint64(0)) 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 1, Offset: uint64(math.MaxInt64) + 1, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDir offset > math.MaxInt64 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 1, Offset: 0, Limit: 0})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDir limit == 0 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 1, Offset: 0, Limit: meta.MaxReadDirLimit + 1})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDir limit > MaxReadDirLimit 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 0, Fh: 1, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 0, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: 1, Fh: 1, Offset: 0, Limit: 10, SessionID: &zeroSID})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDir(ctx, meta.ReadDirParams{Ino: uint64(math.MaxInt64 - 424901), Fh: 1, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrNotFound)
}

// formalProbeReadDirPlus 在每步写操作后全面核对只读 ReadDirPlus：
// 1. 所有有效目录句柄（含存活目录与 nlink == 0 的 rmdir-whilst-open 孤儿目录）的全量读取、子项完整 Inode Attr 对拍与分页迭代读取；
// 2. 对非目录节点 + 文件句柄返回 ErrNotDir，对目录节点 + 文件句柄/已关闭句柄/跨目录句柄/不匹配 SessionID 返回 ErrBadFileHandle；
// 3. 对 offset < 0 (uint64 越界 > math.MaxInt64) 或 limit == 0 / limit > MaxReadDirLimit 等非法参数返回 ErrInvalidArgument。
func formalProbeReadDirPlus(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	activeDirHandles := env.Model.ActiveDirHandles()
	activeFileHandles := env.Model.ActiveFileHandles()

	// 1. 遍历所有有效目录句柄核对 ReadDirPlus
	for idx, h := range activeDirHandles {
		dir, ok := env.Model.Inodes[h.Ino]
		require.Truef(t, ok && !dir.Deleted && dir.IsDir(),
			"ReadDirPlus 探针: 活跃目录句柄 fh=%d 关联的目录 ino=%d 在参考模型中不存在或非目录", h.Fh, h.Ino)

		verifyPagination := (idx == 0 && env.StepIndex%4 == 0) || dir.Nlink == 0
		assertFormalReadDirPlusEntriesMatches(ctx, t, env.Mgr, env.Model, dir, h.Fh, h.SessionID, verifyPagination)

		if idx == 0 {
			var mismatchedSID *uint64
			if h.SessionID == nil {
				bogus := uint64(888778)
				mismatchedSID = &bogus
			}
			_, errMismatch := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
				Ino:       h.Ino,
				Fh:        h.Fh,
				Offset:    0,
				Limit:     10,
				SessionID: mismatchedSID,
			})
			require.ErrorIs(t, errMismatch, meta.ErrBadFileHandle, "ReadDirPlus 传入不匹配 SessionID 应返回 ErrBadFileHandle")
		}
	}

	// 2. 核对已释放的目录句柄 (ClosedHandles)
	for _, fh := range env.Model.AllCreatedFhs {
		ch, isClosed := env.Model.ClosedHandles[fh]
		if !isClosed || !ch.IsDir {
			continue
		}
		_, err := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       ch.Ino,
			Fh:        ch.Fh,
			Offset:    0,
			Limit:     10,
			SessionID: ch.SessionID,
		})
		if env.Model.DeletedInos[ch.Ino] {
			require.ErrorIs(t, err, meta.ErrNotFound,
				"已物理删除孤儿目录的已关闭句柄调用 ReadDirPlus 应返回 ErrNotFound")
		} else if inode, ok := env.Model.Inodes[ch.Ino]; ok && !inode.Deleted && inode.IsDir() {
			if inode.Nlink > 0 || env.Model.HasValidHandlesForIno(inode.Ino) {
				require.ErrorIs(t, err, meta.ErrBadFileHandle,
					"存活目录的已关闭句柄调用 ReadDirPlus 应返回 ErrBadFileHandle")
			}
		}
	}

	// 3. 核对文件句柄调用 ReadDirPlus
	if len(activeFileHandles) > 0 {
		fileH := activeFileHandles[0]
		_, errNotDir := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
			Ino:       fileH.Ino,
			Fh:        fileH.Fh,
			Offset:    0,
			Limit:     10,
			SessionID: fileH.SessionID,
		})
		require.ErrorIs(t, errNotDir, meta.ErrNotDir,
			"对非目录节点与文件句柄调用 ReadDirPlus 必须返回 ErrNotDir")

		activeDirs := env.Model.ActiveDirs()
		if len(activeDirs) > 0 {
			_, errBadFh := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
				Ino:       activeDirs[0].Ino,
				Fh:        fileH.Fh,
				Offset:    0,
				Limit:     10,
				SessionID: fileH.SessionID,
			})
			require.ErrorIs(t, errBadFh, meta.ErrBadFileHandle,
				"对目录 Ino 传入常规文件句柄 Fh 调用 ReadDirPlus 必须返回 ErrBadFileHandle")
		}
	}

	// 4. 核对非法静态边界参数
	zeroSID := uint64(0)
	_, err := env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 1, Offset: ^uint64(0), Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDirPlus offset < 0 (^uint64(0)) 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 1, Offset: uint64(math.MaxInt64) + 1, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDirPlus offset > math.MaxInt64 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 1, Offset: 0, Limit: 0})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDirPlus limit == 0 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 1, Offset: 0, Limit: meta.MaxReadDirLimit + 1})
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "ReadDirPlus limit > MaxReadDirLimit 必须返回 ErrInvalidArgument")
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 0, Fh: 1, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 0, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 1, Fh: 1, Offset: 0, Limit: 10, SessionID: &zeroSID})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = env.Mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: uint64(math.MaxInt64 - 424902), Fh: 1, Offset: 0, Limit: 10})
	require.ErrorIs(t, err, meta.ErrNotFound)
}

// formalProbeFsyncDir 在每步写操作后全面核对只读 FsyncDir：
// 1. 所有有效目录句柄（含存活目录与 nlink == 0 的 rmdir-whilst-open 孤儿目录）调用 FsyncDir(Datasync=false/true) 均幂等返回 nil；
// 2. 对非目录节点 + 文件句柄返回 ErrNotDir，对目录节点 + 文件句柄/已关闭句柄/跨目录句柄/不匹配 SessionID 返回 ErrBadFileHandle；
// 3. 对已物理删除节点或不存在节点返回 ErrNotFound，对非法 0 参数返回 ErrInvalidArgument。
func formalProbeFsyncDir(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	activeDirHandles := env.Model.ActiveDirHandles()
	activeFileHandles := env.Model.ActiveFileHandles()

	// 1. 核对所有有效目录句柄（含 nlink == 0 活跃孤儿目录句柄）
	for idx, h := range activeDirHandles {
		err := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       h.Ino,
			Fh:        h.Fh,
			Datasync:  idx%2 == 0,
			SessionID: h.SessionID,
		})
		require.NoErrorf(t, err, "有效目录句柄 (fh=%d, ino=%d, sid=%s) 调用 FsyncDir 不应失败",
			h.Fh, h.Ino, modelFormatSessionID(h.SessionID))

		if idx == 0 {
			var mismatchedSID *uint64
			if h.SessionID == nil {
				bogus := uint64(888779)
				mismatchedSID = &bogus
			}
			errMismatch := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
				Ino:       h.Ino,
				Fh:        h.Fh,
				Datasync:  true,
				SessionID: mismatchedSID,
			})
			require.ErrorIs(t, errMismatch, meta.ErrBadFileHandle,
				"FsyncDir 传入不匹配 SessionID 应返回 ErrBadFileHandle")
		}
	}

	// 2. 核对已释放的目录句柄 (ClosedHandles)
	for _, fh := range env.Model.AllCreatedFhs {
		ch, isClosed := env.Model.ClosedHandles[fh]
		if !isClosed || !ch.IsDir {
			continue
		}
		err := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       ch.Ino,
			Fh:        ch.Fh,
			Datasync:  false,
			SessionID: ch.SessionID,
		})
		if env.Model.DeletedInos[ch.Ino] {
			require.ErrorIs(t, err, meta.ErrNotFound,
				"已物理删除孤儿目录的已关闭句柄调用 FsyncDir 应返回 ErrNotFound")
		} else if inode, ok := env.Model.Inodes[ch.Ino]; ok && !inode.Deleted && inode.IsDir() {
			if inode.Nlink > 0 || env.Model.HasValidHandlesForIno(inode.Ino) {
				require.ErrorIs(t, err, meta.ErrBadFileHandle,
					"存活目录的已关闭句柄调用 FsyncDir 应返回 ErrBadFileHandle")
			}
		}
	}

	// 3. 核对文件句柄调用 FsyncDir
	if len(activeFileHandles) > 0 {
		fileH := activeFileHandles[0]
		errNotDir := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:       fileH.Ino,
			Fh:        fileH.Fh,
			Datasync:  true,
			SessionID: fileH.SessionID,
		})
		require.ErrorIs(t, errNotDir, meta.ErrNotDir,
			"对非目录节点与文件句柄调用 FsyncDir 必须返回 ErrNotDir")

		activeDirs := env.Model.ActiveDirs()
		if len(activeDirs) > 0 {
			errBadFh := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
				Ino:       activeDirs[0].Ino,
				Fh:        fileH.Fh,
				Datasync:  false,
				SessionID: fileH.SessionID,
			})
			require.ErrorIs(t, errBadFh, meta.ErrBadFileHandle,
				"对目录 Ino 传入常规文件句柄 Fh 调用 FsyncDir 必须返回 ErrBadFileHandle")
		}
	}

	// 4. 核对不存在目录句柄、跨目录句柄与非法参数
	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) > 0 {
		errBogusFh := env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{
			Ino:      activeDirs[0].Ino,
			Fh:       uint64(math.MaxInt64 - 424888),
			Datasync: false,
		})
		require.ErrorIs(t, errBogusFh, meta.ErrBadFileHandle)
	}

	zeroSID := uint64(0)
	require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: 0, Fh: 1}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: 1, Fh: 0}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: 1, Fh: 1, SessionID: &zeroSID}), meta.ErrInvalidArgument)
	require.ErrorIs(t, env.Mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: uint64(math.MaxInt64 - 424903), Fh: 1}), meta.ErrNotFound)
}

// ============================================================================
// 4. T7 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T7_OpenDirReadDir 验证 T7 主状态机 OpenDir/ReadDir/ReadDirPlus/FsyncDir/ReleaseDir 目录遍历合成、
// 字典序分页游标、rmdir-whilst-open 与 Rename 覆盖打开目录的孤儿延迟回收及底表 INV-1 ~ INV-5 闭环。
func TestFormalMain_T7_OpenDirReadDir(t *testing.T) {
	t.Run("确定性边界_OpenDir与ReleaseDir全参数矩阵_ReadDir与ReadDirPlus字典序分页对拍_rmdir-whilst-open与Rename覆盖打开目录孤儿回收_INV-2至INV-4闭环验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 50,
			Seed:       0x20260424,
			Rng:        newFormalRNG(0x20260424),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t7-det",
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
		}

		// 1. 创建初始测试磁盘，并在根目录下创建测试主目录 work_dir
		formalCreateValidDiskStep(ctx, t, env)
		disk := env.Model.ActiveDisks()[0]
		rootIno := disk.RootIno
		rootDir := env.Model.Inodes[rootIno]

		workDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("work_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		workDirModel := &formalInode{
			Ino: workDir.Ino, Generation: workDir.Generation, RootIno: rootIno, ParentIno: rootIno,
			Mode: workDir.Attr.Mode, UID: 1000, GID: 1000, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(workDirModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "work_dir"}] = workDirModel.Ino
		rootDir.Nlink++
		disk.UsedInodes++

		// 2. 在 work_dir 下乱序创建 5 个异构类型子项，验证 ReadDir / ReadDirPlus 原始字节序升序 (bytes.Compare) 与属性对拍
		// 预期字典序： "01_fifo" < "A_chr" < "_lnk" < "a_sub" < "z_reg"
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: workDirModel.Ino, Name: []byte("a_sub"), Mode: 0o750, UID: 1001, GID: 1002,
		})
		require.NoError(t, err)
		subDirModel := &formalInode{
			Ino: subDir.Ino, Generation: subDir.Generation, RootIno: rootIno, ParentIno: workDirModel.Ino,
			Mode: subDir.Attr.Mode, UID: 1001, GID: 1002, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(subDirModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "a_sub"}] = subDirModel.Ino
		workDirModel.Nlink++
		disk.UsedInodes++

		regRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: workDirModel.Ino, Name: []byte("z_reg"), Mode: 0o644, Flags: meta.O_CREAT | meta.O_RDWR, UID: 1003, GID: 1004,
		})
		require.NoError(t, err)
		regModel := &formalInode{
			Ino: regRes.Inode.Ino, Generation: regRes.Inode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: regRes.Inode.Attr.Mode, UID: 1003, GID: 1004, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(regModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "z_reg"}] = regModel.Ino
		disk.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: regRes.Fh, Ino: regModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR, IsDir: false,
		})
		// 为 z_reg 设置非零大小 2048 字节以核对 ReadDirPlus 子项 Size 与 Blocks
		regSize := uint64(2048)
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Size: &regSize})
		require.NoError(t, err)
		regModel.Size = regSize
		disk.UsedBytes += regSize

		fifoNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: workDirModel.Ino, Name: []byte("01_fifo"), Mode: meta.S_IFIFO | 0o600, UID: 1005, GID: 1006,
		})
		require.NoError(t, err)
		fifoModel := &formalInode{
			Ino: fifoNode.Ino, Generation: fifoNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: fifoNode.Attr.Mode, UID: 1005, GID: 1006, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fifoModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "01_fifo"}] = fifoModel.Ino
		disk.UsedInodes++

		chrNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: workDirModel.Ino, Name: []byte("A_chr"), Mode: meta.S_IFCHR | 0o660, Rdev: 0x0103, UID: 0, GID: 5,
		})
		require.NoError(t, err)
		chrModel := &formalInode{
			Ino: chrNode.Ino, Generation: chrNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: chrNode.Attr.Mode, UID: 0, GID: 5, Rdev: 0x0103, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(chrModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "A_chr"}] = chrModel.Ino
		disk.UsedInodes++

		symNode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: workDirModel.Ino, Name: []byte("_lnk"), Target: []byte("../z_reg"), UID: 1000, GID: 1000,
		})
		require.NoError(t, err)
		symModel := &formalInode{
			Ino: symNode.Ino, Generation: symNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: symNode.Attr.Mode, UID: 1000, GID: 1000, Size: uint64(len("../z_reg")), Nlink: 1, SymlinkTarget: []byte("../z_reg"),
		}
		env.Model.RecordNewInode(symModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "_lnk"}] = symModel.Ino
		disk.UsedInodes++

		// 3. 验证 OpenDir 错误码边界矩阵与正常打开
		for _, badFlags := range []uint32{
			meta.O_WRONLY,
			meta.O_RDWR,
			meta.O_RDONLY | meta.O_TRUNC,
			meta.O_WRONLY | meta.O_CREAT,
			meta.O_WRONLY | meta.O_EXCL,
			meta.O_RDWR | meta.O_APPEND,
			meta.O_RDWR | meta.O_TMPFILE,
		} {
			_, err = mgr.OpenDir(ctx, meta.OpenDirParams{Ino: workDirModel.Ino, Flags: badFlags})
			require.ErrorIs(t, err, meta.ErrInvalidArgument, "OpenDir 非法 flags=0x%x 必须返回 ErrInvalidArgument", badFlags)
		}
		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{Ino: regModel.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrNotDir, "对常规文件调用 OpenDir 必须返回 ErrNotDir")
		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{Ino: symModel.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrNotDir, "对符号链接调用 OpenDir 必须返回 ErrNotDir")

		// 正常打开根目录与 work_dir
		rootDirHandle, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: rootIno, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: rootDirHandle.Fh, Ino: rootIno, Flags: meta.O_RDONLY, IsDir: true,
		})

		workDirHandle, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: workDirModel.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: workDirHandle.Fh, Ino: workDirModel.Ino, Flags: meta.O_RDONLY, IsDir: true,
		})

		// 运行探针与审计器，自动核对根目录与 work_dir 的 ReadDir / ReadDirPlus 全量与 limit=1,2 分页读取
		runAllProbesAndAuditors()

		// 额外验证 limit=3 分页迭代读取 work_dir（共 7 项：".", "..", "01_fifo", "A_chr", "_lnk", "a_sub", "z_reg"）
		var page3Names []string
		var cursor uint64
		for {
			page, pageErr := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
				Ino:    workDirModel.Ino,
				Fh:     workDirHandle.Fh,
				Offset: cursor,
				Limit:  3,
			})
			require.NoError(t, pageErr)
			if len(page) == 0 {
				break
			}
			for _, item := range page {
				page3Names = append(page3Names, string(item.Name))
			}
			cursor = page[len(page)-1].NextOffset
		}
		require.Equal(t, []string{".", "..", "01_fifo", "A_chr", "_lnk", "a_sub", "z_reg"}, page3Names)

		// 4. 验证场景 A：rmdir-whilst-open（目录持有活跃 OpenDir 句柄期间被 Rmdir，延迟物理回收与配额扣减闭环）
		sidOrphan := uint64(42401)
		subFh1, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: subDirModel.Ino, Flags: meta.O_RDONLY, SessionID: nil})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: subFh1.Fh, Ino: subDirModel.Ino, Flags: meta.O_RDONLY, SessionID: nil, IsDir: true,
		})

		subFh2, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: subDirModel.Ino, Flags: meta.O_RDONLY, SessionID: &sidOrphan})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &sidOrphan, env.LeaseTTL)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: subFh2.Fh, Ino: subDirModel.Ino, Flags: meta.O_RDONLY, SessionID: modelCloneSessionID(&sidOrphan), IsDir: true,
		})

		usedInodesBeforeRmdir := disk.UsedInodes
		// 执行 Rmdir 移除 a_sub：因其持有 subFh1 与 subFh2，nlink 从 2 变为 0，转为孤儿目录，used_inodes 暂不减！
		err = mgr.Rmdir(ctx, workDirModel.Ino, []byte("a_sub"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: workDirModel.Ino, Name: "a_sub"})
		workDirModel.Nlink--
		subDirModel.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(subDirModel.Ino))
		require.Equal(t, usedInodesBeforeRmdir, disk.UsedInodes, "rmdir-whilst-open 期间 used_inodes 暂不扣减")

		// 4.1 孤儿目录 subDirModel 仍持有有效句柄时，允许再次 OpenDir 分配第 3 个句柄 subFh3
		subFh3, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: subDirModel.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err, "nlink==0 但仍持有效句柄的孤儿目录必须允许再次 OpenDir")
		env.Model.RecordNewHandle(&formalHandle{
			Fh: subFh3.Fh, Ino: subDirModel.Ino, Flags: meta.O_RDONLY, IsDir: true,
		})

		// 4.2 对孤儿目录调用 ReadDir / ReadDirPlus 必须返回空列表 (len == 0)，FsyncDir 幂等返回 nil
		orphanEntries, err := mgr.ReadDir(ctx, meta.ReadDirParams{Ino: subDirModel.Ino, Fh: subFh1.Fh, Offset: 0, Limit: 100})
		require.NoError(t, err)
		require.Empty(t, orphanEntries)
		orphanPlusEntries, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: subDirModel.Ino, Fh: subFh1.Fh, Offset: 0, Limit: 100})
		require.NoError(t, err)
		require.Empty(t, orphanPlusEntries)
		require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: subDirModel.Ino, Fh: subFh1.Fh, Datasync: true}))
		runAllProbesAndAuditors()

		// 4.3 依次关闭 subFh1 与 subFh2（此时仍剩 subFh3，孤儿目录不回收，used_inodes 不减）
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: subDirModel.Ino, Fh: subFh1.Fh}))
		env.Model.Handles[subFh1.Fh].Closed = true
		env.Model.ClosedHandles[subFh1.Fh] = env.Model.Handles[subFh1.Fh]
		delete(env.Model.Handles, subFh1.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(subDirModel.Ino))

		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: subDirModel.Ino, Fh: subFh2.Fh, SessionID: &sidOrphan}))
		env.Model.Handles[subFh2.Fh].Closed = true
		env.Model.ClosedHandles[subFh2.Fh] = env.Model.Handles[subFh2.Fh]
		delete(env.Model.Handles, subFh2.Fh)
		require.False(t, env.Model.ReclaimOrphanInModel(subDirModel.Ino))
		require.Equal(t, usedInodesBeforeRmdir, disk.UsedInodes)

		// 4.4 关闭孤儿目录最后一个句柄 subFh3 -> 在同一事务内物理删除孤儿目录且 used_inodes - 1！
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: subDirModel.Ino, Fh: subFh3.Fh}))
		env.Model.Handles[subFh3.Fh].Closed = true
		env.Model.ClosedHandles[subFh3.Fh] = env.Model.Handles[subFh3.Fh]
		delete(env.Model.Handles, subFh3.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(subDirModel.Ino), "最后一个目录句柄关闭后必须物理删除孤儿目录")
		require.Equal(t, usedInodesBeforeRmdir-1, disk.UsedInodes, "孤儿目录物理回收后 used_inodes 必须 -1")
		runAllProbesAndAuditors()

		// 物理删除后再次 OpenDir(subDirModel.Ino) 必须返回 ErrNotFound
		_, err = mgr.OpenDir(ctx, meta.OpenDirParams{Ino: subDirModel.Ino, Flags: meta.O_RDONLY})
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 5. 验证场景 B：Rename 覆盖持有活跃 OpenDir 句柄的目标空目录，转为 nlink == 0 孤儿目录并在 ReleaseDir 时延迟物理回收
		srcDir, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: workDirModel.Ino, Name: []byte("rename_src_dir"), Mode: 0o755})
		require.NoError(t, err)
		srcDirModel := &formalInode{
			Ino: srcDir.Ino, Generation: srcDir.Generation, RootIno: rootIno, ParentIno: workDirModel.Ino,
			Mode: srcDir.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(srcDirModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "rename_src_dir"}] = srcDirModel.Ino
		workDirModel.Nlink++
		disk.UsedInodes++

		victimDir, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: workDirModel.Ino, Name: []byte("rename_victim_dir"), Mode: 0o755})
		require.NoError(t, err)
		victimDirModel := &formalInode{
			Ino: victimDir.Ino, Generation: victimDir.Generation, RootIno: rootIno, ParentIno: workDirModel.Ino,
			Mode: victimDir.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(victimDirModel)
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "rename_victim_dir"}] = victimDirModel.Ino
		workDirModel.Nlink++
		disk.UsedInodes++

		// 打开 victimDir 的目录句柄
		victimFh, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: victimDirModel.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{
			Fh: victimFh.Fh, Ino: victimDirModel.Ino, Flags: meta.O_RDONLY, IsDir: true,
		})

		usedInodesBeforeRenameOverwrite := disk.UsedInodes
		// 执行 Rename 将 rename_src_dir 覆盖到 rename_victim_dir
		err = mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: workDirModel.Ino,
			OldName:      []byte("rename_src_dir"),
			NewParentIno: workDirModel.Ino,
			NewName:      []byte("rename_victim_dir"),
			Flags:        0,
		})
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: workDirModel.Ino, Name: "rename_src_dir"})
		env.Model.Entries[formalEntryKey{ParentIno: workDirModel.Ino, Name: "rename_victim_dir"}] = srcDirModel.Ino
		workDirModel.Nlink--
		victimDirModel.Nlink = 0
		require.False(t, env.Model.ReclaimOrphanInModel(victimDirModel.Ino), "被 Rename 覆盖但仍持有 OpenDir 句柄的目录不得立即物理删除")
		require.Equal(t, usedInodesBeforeRenameOverwrite, disk.UsedInodes, "被覆盖孤儿目录未关闭句柄前 used_inodes 不变")
		runAllProbesAndAuditors()

		// 关闭被 Rename 覆盖的孤儿目录句柄 victimFh -> 立即物理删除并扣减 used_inodes - 1
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: victimDirModel.Ino, Fh: victimFh.Fh}))
		env.Model.Handles[victimFh.Fh].Closed = true
		env.Model.ClosedHandles[victimFh.Fh] = env.Model.Handles[victimFh.Fh]
		delete(env.Model.Handles, victimFh.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(victimDirModel.Ino))
		require.Equal(t, usedInodesBeforeRenameOverwrite-1, disk.UsedInodes)

		// 清理剩余打开句柄
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: regModel.Ino, Fh: regRes.Fh}))
		env.Model.Handles[regRes.Fh].Closed = true
		env.Model.ClosedHandles[regRes.Fh] = env.Model.Handles[regRes.Fh]
		delete(env.Model.Handles, regRes.Fh)

		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: workDirModel.Ino, Fh: workDirHandle.Fh}))
		env.Model.Handles[workDirHandle.Fh].Closed = true
		env.Model.ClosedHandles[workDirHandle.Fh] = env.Model.Handles[workDirHandle.Fh]
		delete(env.Model.Handles, workDirHandle.Fh)

		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: rootIno, Fh: rootDirHandle.Fh}))
		env.Model.Handles[rootDirHandle.Fh].Closed = true
		env.Model.ClosedHandles[rootDirHandle.Fh] = env.Model.Handles[rootDirHandle.Fh]
		delete(env.Model.Handles, rootDirHandle.Fh)

		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_OpenDir与ReleaseDir目录流生命周期_rmdir-whilst-open与Rename覆盖延迟回收_ReadDir与ReadDirPlus及FsyncDir探针与底表INV-1至INV-5全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 8, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 8, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 10, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 22, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 20, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 10, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 8, Fn: formalActionSymlink},
				{Name: "Link", Weight: 10, Fn: formalActionLink},
				{Name: "Unlink", Weight: 12, Fn: formalActionUnlink},
				{Name: "Rename", Weight: 18, Fn: formalActionRename},
				{Name: "Create", Weight: 15, Fn: formalActionCreate},
				{Name: "Open", Weight: 12, Fn: formalActionOpen},
				{Name: "Release", Weight: 12, Fn: formalActionRelease},
				{Name: "OpenDir", Weight: 30, Fn: formalActionOpenDir},
				{Name: "ReleaseDir", Weight: 24, Fn: formalActionReleaseDir},
			},
			Probes: []formalNamedProbe{
				{Name: "GetDisk", Fn: formalProbeGetDisk},
				{Name: "StatFS", Fn: formalProbeStatFS},
				{Name: "Lookup", Fn: formalProbeLookup},
				{Name: "GetAttr", Fn: formalProbeGetAttr},
				{Name: "Readlink", Fn: formalProbeReadlink},
				{Name: "Flush", Fn: formalProbeFlush},
				{Name: "Fsync", Fn: formalProbeFsync},
				{Name: "ReadDir", Fn: formalProbeReadDir},
				{Name: "ReadDirPlus", Fn: formalProbeReadDirPlus},
				{Name: "FsyncDir", Fn: formalProbeFsyncDir},
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
