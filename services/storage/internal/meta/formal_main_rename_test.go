package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func init() {
	// 注册 T5 核心重命名动作：Rename（涵盖 flags=0 传统覆盖、RenameNoReplace、RenameExchange 全模式）
	registerFormalAction("Rename", 35, formalActionRename)
}

// ============================================================================
// 1. T5 参考模型推导与状态转移函数：Rename 全模式 (0, RenameNoReplace, RenameExchange)
// ============================================================================

// modelLockAndValidateParents 严格对齐 manager.go 中 lockAndValidateParents 的按全局 ino 升序加锁与校验顺序。
func modelLockAndValidateParents(
	m *formalMainModel,
	oldParentIno, newParentIno uint64,
) (oldParent *formalInode, newParent *formalInode, err error) {
	for _, ino := range modelUniqueSortedUint64(oldParentIno, newParentIno) {
		row, exists := m.Inodes[ino]
		if !exists || row.Deleted {
			return nil, nil, meta.ErrNotFound
		}
		if !row.IsDir() {
			return nil, nil, meta.ErrNotDir
		}
		if row.Nlink == 0 {
			return nil, nil, meta.ErrNotFound
		}
		if ino == oldParentIno {
			oldParent = row
		}
		if ino == newParentIno {
			newParent = row
		}
	}
	return oldParent, newParent, nil
}

// formalRenamePlan 保存内存参考模型对单次 Rename 操作的预测结果与状态转移要素。
type formalRenamePlan struct {
	NoOp        bool   // 是否触发同路径或同 Inode 硬链接无副作用短路
	OldIno      uint64 // 源目录项指向的 Ino
	HasNewEntry bool   // 目标目录项是否已存在
	NewIno      uint64 // 目标目录项指向的 Ino（当 HasNewEntry 为真时有效）
	OldIsDir    bool   // 源节点是否为目录
	NewIsDir    bool   // 目标节点是否为目录
	ExpectedErr error  // 预期返回的领域错误码
}

// formalPredictRename 根据内存参考模型预测 Rename 的执行结果与错误码。
// 严格对齐 manager_dir_topology.go 中 Rename 的静态校验、特殊名称校验、预读目录项、
// 同路径短路、按全局 ino 升序加锁、跨盘 EXDEV、RenameNoReplace、同 Inode 短路、
// 正反向祖先防环检测、EMLINK 上限以及覆盖兼容矩阵顺序。
func formalPredictRename(m *formalMainModel, params meta.RenameParams) formalRenamePlan {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
	}
	if (params.Flags &^ (meta.RenameNoReplace | meta.RenameExchange | meta.RenameWhiteout)) != 0 {
		return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
	}
	if (params.Flags & (meta.RenameNoReplace | meta.RenameExchange)) == (meta.RenameNoReplace | meta.RenameExchange) {
		return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
	}
	if (params.Flags & meta.RenameWhiteout) != 0 {
		return formalRenamePlan{ExpectedErr: meta.ErrNotSupported}
	}
	if err := modelValidateEntryNameAllowEmpty(params.OldName); err != nil {
		return formalRenamePlan{ExpectedErr: err}
	}
	if err := modelValidateEntryName(params.NewName); err != nil {
		return formalRenamePlan{ExpectedErr: err}
	}

	// 1. 空 OldName 或 "." / ".." 特殊名称校验（先按升序锁并校验双父目录）
	if len(params.OldName) == 0 || modelIsDotOrDotDot(params.OldName) || modelIsDotOrDotDot(params.NewName) {
		if _, _, err := modelLockAndValidateParents(m, params.OldParentIno, params.NewParentIno); err != nil {
			return formalRenamePlan{ExpectedErr: err}
		}
		if len(params.OldName) == 0 {
			return formalRenamePlan{ExpectedErr: meta.ErrNotFound}
		}
		return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
	}

	// 2. 预读源目录项
	oldIno, hasOldEntry := m.Entries[formalEntryKey{ParentIno: params.OldParentIno, Name: string(params.OldName)}]
	if !hasOldEntry {
		if _, _, err := modelLockAndValidateParents(m, params.OldParentIno, params.NewParentIno); err != nil {
			return formalRenamePlan{ExpectedErr: err}
		}
		return formalRenamePlan{ExpectedErr: meta.ErrNotFound}
	}

	// 3. 同路径短路 (OldParentIno == NewParentIno && OldName == NewName)
	if params.OldParentIno == params.NewParentIno && bytes.Equal(params.OldName, params.NewName) {
		if _, _, err := modelLockAndValidateParents(m, params.OldParentIno, params.NewParentIno); err != nil {
			return formalRenamePlan{ExpectedErr: err}
		}
		if params.Flags == meta.RenameNoReplace {
			return formalRenamePlan{ExpectedErr: meta.ErrAlreadyExists}
		}
		return formalRenamePlan{
			NoOp:   true,
			OldIno: oldIno,
		}
	}

	// 4. 预读目标目录项
	newIno, hasNewEntry := m.Entries[formalEntryKey{ParentIno: params.NewParentIno, Name: string(params.NewName)}]
	if !hasNewEntry && params.Flags == meta.RenameExchange {
		if _, _, err := modelLockAndValidateParents(m, params.OldParentIno, params.NewParentIno); err != nil {
			return formalRenamePlan{ExpectedErr: err}
		}
		return formalRenamePlan{ExpectedErr: meta.ErrNotFound}
	}

	// 5. 按全局 ino 升序锁定并校验双父目录、源节点与目标节点
	inosToLock := []uint64{params.OldParentIno, params.NewParentIno, oldIno}
	if hasNewEntry && oldIno != newIno {
		inosToLock = append(inosToLock, newIno)
	}
	for _, ino := range modelUniqueSortedUint64(inosToLock...) {
		row, exists := m.Inodes[ino]
		if !exists || row.Deleted {
			return formalRenamePlan{ExpectedErr: meta.ErrNotFound}
		}
		if ino == params.OldParentIno || ino == params.NewParentIno {
			if !row.IsDir() {
				return formalRenamePlan{ExpectedErr: meta.ErrNotDir}
			}
			if row.Nlink == 0 {
				return formalRenamePlan{ExpectedErr: meta.ErrNotFound}
			}
		}
	}

	oldParent := m.Inodes[params.OldParentIno]
	newParent := m.Inodes[params.NewParentIno]
	oldInode := m.Inodes[oldIno]
	var newInode *formalInode
	if hasNewEntry {
		newInode = m.Inodes[newIno]
	}

	// 6. 跨虚拟磁盘拓扑隔离校验 (EXDEV)
	if oldParent.RootIno != newParent.RootIno || oldInode.RootIno != oldParent.RootIno {
		return formalRenamePlan{ExpectedErr: meta.ErrCrossDevice}
	}
	if hasNewEntry && oldIno != newIno && newInode.RootIno != oldParent.RootIno {
		return formalRenamePlan{ExpectedErr: meta.ErrCrossDevice}
	}

	// 7. RenameNoReplace：目标已存在（含同 Inode 硬链接）一律返回 ErrAlreadyExists
	if params.Flags == meta.RenameNoReplace && hasNewEntry {
		return formalRenamePlan{ExpectedErr: meta.ErrAlreadyExists}
	}

	// 8. 同节点硬链接短路 (POSIX No-op / RenameExchange 同节点短路)
	if hasNewEntry && oldIno == newIno {
		return formalRenamePlan{
			NoOp:        true,
			OldIno:      oldIno,
			HasNewEntry: true,
			NewIno:      newIno,
		}
	}

	oldIsDir := oldInode.IsDir()
	newIsDir := hasNewEntry && newInode.IsDir()

	// 9. 正向祖先环检测：当源节点为目录且跨父目录移动或交换时，NewParentIno 不能是 oldIno 自身或其子孙目录
	if oldIsDir && params.OldParentIno != params.NewParentIno {
		if m.IsAncestorDir(oldIno, params.NewParentIno) {
			return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
		}
	}

	// 10. RenameExchange 双向原子交换分支
	if params.Flags == meta.RenameExchange {
		// 反向祖先环检测：当目标节点为目录且跨父目录交换时，OldParentIno 不能是 newIno 自身或其子孙目录
		if newIsDir && params.OldParentIno != params.NewParentIno {
			if m.IsAncestorDir(newIno, params.OldParentIno) {
				return formalRenamePlan{ExpectedErr: meta.ErrInvalidArgument}
			}
		}

		// 跨父目录交换“一目录一非目录”时，移入目录的目标父目录 nlink 上限校验
		if params.OldParentIno != params.NewParentIno {
			if oldIsDir && !newIsDir && newParent.Nlink >= meta.MaxNlink {
				return formalRenamePlan{ExpectedErr: meta.ErrTooManyLinks}
			}
			if !oldIsDir && newIsDir && oldParent.Nlink >= meta.MaxNlink {
				return formalRenamePlan{ExpectedErr: meta.ErrTooManyLinks}
			}
		}

		return formalRenamePlan{
			OldIno:      oldIno,
			HasNewEntry: true,
			NewIno:      newIno,
			OldIsDir:    oldIsDir,
			NewIsDir:    newIsDir,
		}
	}

	// 11. 传统覆盖模式 (flags == 0) 与 RenameNoReplace (此时 hasNewEntry == false) 分支
	if hasNewEntry {
		if oldIsDir && !newIsDir {
			return formalRenamePlan{ExpectedErr: meta.ErrNotDir}
		}
		if !oldIsDir && newIsDir {
			return formalRenamePlan{ExpectedErr: meta.ErrIsDir}
		}
		if oldIsDir && newIsDir {
			if len(m.ChildEntriesOf(newIno)) > 0 {
				return formalRenamePlan{ExpectedErr: meta.ErrNotEmpty}
			}
		}
	}

	if oldIsDir && params.OldParentIno != params.NewParentIno && !hasNewEntry && newParent.Nlink >= meta.MaxNlink {
		return formalRenamePlan{ExpectedErr: meta.ErrTooManyLinks}
	}

	return formalRenamePlan{
		OldIno:      oldIno,
		HasNewEntry: hasNewEntry,
		NewIno:      newIno,
		OldIsDir:    oldIsDir,
		NewIsDir:    newIsDir,
	}
}

// formalApplyRenameToModel 在 Rename 成功执行后，按 plan 同步更新内存参考模型的 Entries、Inodes 与 Disk 配额账本。
func formalApplyRenameToModel(m *formalMainModel, params meta.RenameParams, plan formalRenamePlan) (reclaimedTarget bool) {
	if plan.ExpectedErr != nil || plan.NoOp {
		return false
	}

	oldKey := formalEntryKey{ParentIno: params.OldParentIno, Name: string(params.OldName)}
	newKey := formalEntryKey{ParentIno: params.NewParentIno, Name: string(params.NewName)}
	oldParent := m.Inodes[params.OldParentIno]
	newParent := m.Inodes[params.NewParentIno]
	oldInode := m.Inodes[plan.OldIno]

	if params.Flags == meta.RenameExchange {
		newInode := m.Inodes[plan.NewIno]

		// 1. 原地互换两个目录项绑定的 Ino
		m.Entries[oldKey] = plan.NewIno
		m.Entries[newKey] = plan.OldIno

		// 2. 跨父目录交换目录时同步更新被移动目录的 ParentIno
		if plan.OldIsDir && params.OldParentIno != params.NewParentIno {
			oldInode.ParentIno = params.NewParentIno
		}
		if plan.NewIsDir && params.OldParentIno != params.NewParentIno {
			newInode.ParentIno = params.OldParentIno
		}

		// 3. 跨父目录交换“一目录一非目录”时同步增减双方父目录的 Nlink
		if params.OldParentIno != params.NewParentIno {
			if plan.OldIsDir && !plan.NewIsDir {
				oldParent.Nlink--
				newParent.Nlink++
			} else if !plan.OldIsDir && plan.NewIsDir {
				oldParent.Nlink++
				newParent.Nlink--
			}
		}
		return false
	}

	// 标准覆盖模式 (flags == 0) 或 RenameNoReplace (目标不存在)
	if plan.HasNewEntry {
		newInode := m.Inodes[plan.NewIno]
		if plan.NewIsDir {
			// 被覆盖目标为空目录：nlink 直接归零并尝试物理回收
			newInode.Nlink = 0
			reclaimedTarget = m.ReclaimOrphanInModel(plan.NewIno)
		} else {
			// 被覆盖目标为非目录：nlink 递减 1，若归零则尝试物理回收并退还配额
			newInode.Nlink--
			if newInode.Nlink == 0 {
				reclaimedTarget = m.ReclaimOrphanInModel(plan.NewIno)
			}
		}
	}

	// 移动目录项至新位置
	delete(m.Entries, oldKey)
	m.Entries[newKey] = plan.OldIno

	if plan.OldIsDir && params.OldParentIno != params.NewParentIno {
		oldInode.ParentIno = params.NewParentIno
	}

	// 维护父目录 Nlink
	if plan.OldIsDir {
		if params.OldParentIno == params.NewParentIno {
			if plan.HasNewEntry {
				// 同父目录下目录覆盖空目录：父目录子目录数减 1
				oldParent.Nlink--
			}
		} else {
			// 跨父目录移动子目录：原父目录 nlink 减 1；若未覆盖空目录则新父目录 nlink 加 1
			oldParent.Nlink--
			if !plan.HasNewEntry {
				newParent.Nlink++
			}
		}
	}

	return reclaimedTarget
}

// ============================================================================
// 2. T5 动作实现：Rename 全模式随机操作生成与逐操作对拍
// ============================================================================

type formalRenameEntryCandidate struct {
	Key     formalEntryKey
	Ino     uint64
	Node    *formalInode
	RootIno uint64
}

// formalActionRename 在状态机中随机生成覆盖传统模式 (0)、RenameNoReplace、RenameExchange、
// 同路径与同 Inode 硬链接短路、覆盖回收配额、正反向祖先防环检测、跨盘 EXDEV 及各类异常边界的 Rename 请求并与参考模型对拍。
func formalActionRename(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	// 梳理当前所有存活目录项，按所属虚拟磁盘 RootIno 及节点类型分类
	var allCandidates []formalRenameEntryCandidate
	var dirCandidates []formalRenameEntryCandidate
	var emptyDirCandidates []formalRenameEntryCandidate
	var nonEmptyDirCandidates []formalRenameEntryCandidate
	var nonDirCandidates []formalRenameEntryCandidate
	entriesByIno := make(map[uint64][]formalRenameEntryCandidate)

	for _, ek := range env.Model.SortedEntries() {
		ino := env.Model.Entries[ek]
		node, ok := env.Model.Inodes[ino]
		if !ok || node.Deleted {
			continue
		}
		cand := formalRenameEntryCandidate{
			Key:     ek,
			Ino:     ino,
			Node:    node,
			RootIno: node.RootIno,
		}
		allCandidates = append(allCandidates, cand)
		entriesByIno[ino] = append(entriesByIno[ino], cand)
		if node.IsDir() {
			dirCandidates = append(dirCandidates, cand)
			if len(env.Model.ChildEntriesOf(ino)) == 0 {
				emptyDirCandidates = append(emptyDirCandidates, cand)
			} else {
				nonEmptyDirCandidates = append(nonEmptyDirCandidates, cand)
			}
		} else {
			nonDirCandidates = append(nonDirCandidates, cand)
		}
	}

	// 收集同一 Inode 拥有多条硬链接的目录项对 (src_ino == dst_ino)
	type entryPair struct {
		Src formalRenameEntryCandidate
		Dst formalRenameEntryCandidate
	}
	var sameInoPairs []entryPair
	for _, list := range entriesByIno {
		if len(list) >= 2 {
			for i := 0; i < len(list); i++ {
				for j := i + 1; j < len(list); j++ {
					sameInoPairs = append(sameInoPairs, entryPair{Src: list[i], Dst: list[j]})
				}
			}
		}
	}

	// 收集正向祖先防环候选：(目录项 dirCand, 作为 dirCand.Ino 自身或子孙的存活目录 targetDir)
	type ancestorLoopCandidate struct {
		DirEntry  formalRenameEntryCandidate
		TargetDir *formalInode
	}
	var forwardLoopCandidates []ancestorLoopCandidate
	for _, dc := range dirCandidates {
		for _, d := range activeDirs {
			if d.RootIno == dc.RootIno && d.Ino != dc.Key.ParentIno && env.Model.IsAncestorDir(dc.Ino, d.Ino) {
				forwardLoopCandidates = append(forwardLoopCandidates, ancestorLoopCandidate{
					DirEntry:  dc,
					TargetDir: d,
				})
			}
		}
	}

	// 收集反向祖先防环候选 (RenameExchange)：Src 的父目录是 Dst 目录自身或其后代
	var reverseLoopPairs []entryPair
	for _, src := range allCandidates {
		for _, dstDir := range dirCandidates {
			if src.RootIno == dstDir.RootIno &&
				src.Key.ParentIno != dstDir.Key.ParentIno &&
				src.Ino != dstDir.Ino &&
				env.Model.IsAncestorDir(dstDir.Ino, src.Key.ParentIno) {
				reverseLoopPairs = append(reverseLoopPairs, entryPair{Src: src, Dst: dstDir})
			}
		}
	}

	// 若当前尚无任何目录项，先通过 Mknod 或 Mkdir 创建基础节点后再执行 Rename
	if len(allCandidates) == 0 {
		return formalActionMknod(ctx, t, env)
	}

	branch := env.Rng.IntN(100)
	var params meta.RenameParams
	var caseTag string

	switch {
	case branch < 16:
		// 1. 传统模式 (flags = 0) 移动非目录或子目录至新名称（目标不存在，同目录或同盘跨父目录且不成环）
		src := allCandidates[env.Rng.IntN(len(allCandidates))]
		var validTargetDirs []*formalInode
		for _, d := range activeDirs {
			if d.RootIno == src.RootIno {
				if src.Node.IsDir() && d.Ino != src.Key.ParentIno && env.Model.IsAncestorDir(src.Ino, d.Ino) {
					continue
				}
				validTargetDirs = append(validTargetDirs, d)
			}
		}
		targetDir := validTargetDirs[env.Rng.IntN(len(validTargetDirs))]
		params = meta.RenameParams{
			OldParentIno: src.Key.ParentIno,
			OldName:      []byte(src.Key.Name),
			NewParentIno: targetDir.Ino,
			NewName:      []byte(env.UniqueEntryName("ren")),
			Flags:        0,
		}
		if src.Node.IsDir() {
			if src.Key.ParentIno == targetDir.Ino {
				caseTag = "传统模式_同目录重命名子目录"
			} else {
				caseTag = "传统模式_跨父目录移动子目录(更新parent_ino与双父nlink)"
			}
		} else {
			if src.Key.ParentIno == targetDir.Ino {
				caseTag = "传统模式_同目录重命名非目录"
			} else {
				caseTag = "传统模式_跨父目录移动非目录"
			}
		}

	case branch < 30:
		// 2. 传统模式 (flags = 0) 覆盖已有同类型目标（非目录覆盖非目录，或目录覆盖空目录）
		var overwritePairs []entryPair
		if env.Rng.IntN(2) == 0 && len(emptyDirCandidates) > 0 && len(dirCandidates) > 0 {
			for _, src := range dirCandidates {
				for _, dst := range emptyDirCandidates {
					if src.RootIno == dst.RootIno && src.Ino != dst.Ino {
						if src.Key.ParentIno != dst.Key.ParentIno && env.Model.IsAncestorDir(src.Ino, dst.Key.ParentIno) {
							continue
						}
						overwritePairs = append(overwritePairs, entryPair{Src: src, Dst: dst})
					}
				}
			}
		}
		if len(overwritePairs) == 0 && len(nonDirCandidates) >= 2 {
			for _, src := range nonDirCandidates {
				for _, dst := range nonDirCandidates {
					if src.RootIno == dst.RootIno && src.Ino != dst.Ino {
						overwritePairs = append(overwritePairs, entryPair{Src: src, Dst: dst})
					}
				}
			}
		}
		if len(overwritePairs) > 0 {
			chosen := overwritePairs[env.Rng.IntN(len(overwritePairs))]
			params = meta.RenameParams{
				OldParentIno: chosen.Src.Key.ParentIno,
				OldName:      []byte(chosen.Src.Key.Name),
				NewParentIno: chosen.Dst.Key.ParentIno,
				NewName:      []byte(chosen.Dst.Key.Name),
				Flags:        0,
			}
			if chosen.Src.Node.IsDir() {
				caseTag = "传统模式_目录覆盖空目标目录(回收目标与修正父nlink)"
			} else {
				caseTag = "传统模式_非目录覆盖非目录(递减目标nlink与配额退还)"
			}
		} else {
			src := allCandidates[env.Rng.IntN(len(allCandidates))]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: src.Key.ParentIno,
				NewName:      []byte(env.UniqueEntryName("ren_fallback")),
				Flags:        0,
			}
			caseTag = "传统模式_同目录重命名"
		}

	case branch < 40:
		// 3. 同路径短路 (OldParentIno == NewParentIno && OldName == NewName) 或同 Inode 不同硬链接短路 (src_ino == dst_ino)
		flagChoices := []uint32{0, meta.RenameNoReplace, meta.RenameExchange}
		chosenFlag := flagChoices[env.Rng.IntN(len(flagChoices))]
		if len(sameInoPairs) > 0 && env.Rng.IntN(2) == 0 {
			pair := sameInoPairs[env.Rng.IntN(len(sameInoPairs))]
			params = meta.RenameParams{
				OldParentIno: pair.Src.Key.ParentIno,
				OldName:      []byte(pair.Src.Key.Name),
				NewParentIno: pair.Dst.Key.ParentIno,
				NewName:      []byte(pair.Dst.Key.Name),
				Flags:        chosenFlag,
			}
			caseTag = fmt.Sprintf("同Inode不同硬链接短路(flags=0x%x)", chosenFlag)
		} else {
			src := allCandidates[env.Rng.IntN(len(allCandidates))]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: src.Key.ParentIno,
				NewName:      []byte(src.Key.Name),
				Flags:        chosenFlag,
			}
			caseTag = fmt.Sprintf("同路径短路(flags=0x%x)", chosenFlag)
		}

	case branch < 52:
		// 4. RenameNoReplace 模式：目标不存在时成功，目标已存在时返回 ErrAlreadyExists
		src := allCandidates[env.Rng.IntN(len(allCandidates))]
		if env.Rng.IntN(2) == 0 {
			// 目标不存在 -> 成功移动
			var validTargetDirs []*formalInode
			for _, d := range activeDirs {
				if d.RootIno == src.RootIno {
					if src.Node.IsDir() && d.Ino != src.Key.ParentIno && env.Model.IsAncestorDir(src.Ino, d.Ino) {
						continue
					}
					validTargetDirs = append(validTargetDirs, d)
				}
			}
			targetDir := validTargetDirs[env.Rng.IntN(len(validTargetDirs))]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: targetDir.Ino,
				NewName:      []byte(env.UniqueEntryName("noreplace_ok")),
				Flags:        meta.RenameNoReplace,
			}
			caseTag = "RenameNoReplace_目标不存在正常重命名"
		} else {
			// 目标已存在（同盘任意已存在目录项） -> ErrAlreadyExists
			var sameDiskDsts []formalRenameEntryCandidate
			for _, c := range allCandidates {
				if c.RootIno == src.RootIno {
					sameDiskDsts = append(sameDiskDsts, c)
				}
			}
			dst := sameDiskDsts[env.Rng.IntN(len(sameDiskDsts))]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: dst.Key.ParentIno,
				NewName:      []byte(dst.Key.Name),
				Flags:        meta.RenameNoReplace,
			}
			caseTag = "RenameNoReplace_目标已存在拦截(ErrAlreadyExists)"
		}

	case branch < 68:
		// 5. RenameExchange 双向原子交换模式：同类型交换、跨父目录“一目录一非目录”交换、或目标不存在报错
		if env.Rng.IntN(5) == 0 {
			// 目标不存在 -> ErrNotFound
			src := allCandidates[env.Rng.IntN(len(allCandidates))]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: src.Key.ParentIno,
				NewName:      []byte(env.UniqueEntryName("exchange_missing_dst")),
				Flags:        meta.RenameExchange,
			}
			caseTag = "RenameExchange_目标不存在拦截(ErrNotFound)"
		} else {
			var validExchangePairs []entryPair
			var crossTypeCrossParentPairs []entryPair
			for i := 0; i < len(allCandidates); i++ {
				for j := 0; j < len(allCandidates); j++ {
					if i == j {
						continue
					}
					s := allCandidates[i]
					d := allCandidates[j]
					if s.RootIno != d.RootIno || s.Ino == d.Ino {
						continue
					}
					// 排除正反向祖先环
					if s.Node.IsDir() && s.Key.ParentIno != d.Key.ParentIno && env.Model.IsAncestorDir(s.Ino, d.Key.ParentIno) {
						continue
					}
					if d.Node.IsDir() && s.Key.ParentIno != d.Key.ParentIno && env.Model.IsAncestorDir(d.Ino, s.Key.ParentIno) {
						continue
					}
					validExchangePairs = append(validExchangePairs, entryPair{Src: s, Dst: d})
					if s.Key.ParentIno != d.Key.ParentIno && s.Node.IsDir() != d.Node.IsDir() {
						crossTypeCrossParentPairs = append(crossTypeCrossParentPairs, entryPair{Src: s, Dst: d})
					}
				}
			}
			if len(crossTypeCrossParentPairs) > 0 && env.Rng.IntN(2) == 0 {
				chosen := crossTypeCrossParentPairs[env.Rng.IntN(len(crossTypeCrossParentPairs))]
				params = meta.RenameParams{
					OldParentIno: chosen.Src.Key.ParentIno,
					OldName:      []byte(chosen.Src.Key.Name),
					NewParentIno: chosen.Dst.Key.ParentIno,
					NewName:      []byte(chosen.Dst.Key.Name),
					Flags:        meta.RenameExchange,
				}
				caseTag = "RenameExchange_跨父目录交换目录与非目录(更新parent_ino与双父nlink)"
			} else if len(validExchangePairs) > 0 {
				chosen := validExchangePairs[env.Rng.IntN(len(validExchangePairs))]
				params = meta.RenameParams{
					OldParentIno: chosen.Src.Key.ParentIno,
					OldName:      []byte(chosen.Src.Key.Name),
					NewParentIno: chosen.Dst.Key.ParentIno,
					NewName:      []byte(chosen.Dst.Key.Name),
					Flags:        meta.RenameExchange,
				}
				caseTag = "RenameExchange_双向原子交换两个目录项"
			} else {
				src := allCandidates[0]
				params = meta.RenameParams{
					OldParentIno: src.Key.ParentIno,
					OldName:      []byte(src.Key.Name),
					NewParentIno: src.Key.ParentIno,
					NewName:      []byte(src.Key.Name),
					Flags:        meta.RenameExchange,
				}
				caseTag = "RenameExchange_同路径无副作用交换"
			}
		}

	case branch < 77:
		// 6. 正向与反向祖先环检测 (ErrInvalidArgument)
		if len(reverseLoopPairs) > 0 && env.Rng.IntN(2) == 0 {
			pair := reverseLoopPairs[env.Rng.IntN(len(reverseLoopPairs))]
			params = meta.RenameParams{
				OldParentIno: pair.Src.Key.ParentIno,
				OldName:      []byte(pair.Src.Key.Name),
				NewParentIno: pair.Dst.Key.ParentIno,
				NewName:      []byte(pair.Dst.Key.Name),
				Flags:        meta.RenameExchange,
			}
			caseTag = "RenameExchange_反向祖先环检测拦截(ErrInvalidArgument)"
		} else if len(forwardLoopCandidates) > 0 {
			cand := forwardLoopCandidates[env.Rng.IntN(len(forwardLoopCandidates))]
			params = meta.RenameParams{
				OldParentIno: cand.DirEntry.Key.ParentIno,
				OldName:      []byte(cand.DirEntry.Key.Name),
				NewParentIno: cand.TargetDir.Ino,
				NewName:      []byte(env.UniqueEntryName("loop_sub")),
				Flags:        0,
			}
			caseTag = "正向祖先环检测拦截(ErrInvalidArgument)"
		} else if len(dirCandidates) > 0 {
			// 直接将目录移动到自身名下 (NewParentIno == oldIno)
			dc := dirCandidates[env.Rng.IntN(len(dirCandidates))]
			params = meta.RenameParams{
				OldParentIno: dc.Key.ParentIno,
				OldName:      []byte(dc.Key.Name),
				NewParentIno: dc.Ino,
				NewName:      []byte("into_self"),
				Flags:        0,
			}
			caseTag = "目录移动至自身名下防环拦截(ErrInvalidArgument)"
		} else {
			src := allCandidates[0]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte("."),
				NewParentIno: src.Key.ParentIno,
				NewName:      []byte("dot_rename"),
				Flags:        0,
			}
			caseTag = "特殊名称(.)拦截(ErrInvalidArgument)"
		}

	case branch < 85:
		// 7. 跨虚拟磁盘 EXDEV 拦截（涵盖 flags=0, RenameNoReplace, RenameExchange）
		var crossSrc formalRenameEntryCandidate
		var crossTargetDir *formalInode
		var crossDstEntry *formalRenameEntryCandidate
		foundCross := false
		for _, s := range allCandidates {
			for _, d := range activeDirs {
				if d.RootIno != s.RootIno {
					crossSrc = s
					crossTargetDir = d
					foundCross = true
					break
				}
			}
			if foundCross {
				break
			}
		}
		if foundCross {
			for idx := range allCandidates {
				if allCandidates[idx].RootIno == crossTargetDir.RootIno {
					crossDstEntry = &allCandidates[idx]
					break
				}
			}
			modeRoll := env.Rng.IntN(3)
			if modeRoll == 0 && crossDstEntry != nil {
				params = meta.RenameParams{
					OldParentIno: crossSrc.Key.ParentIno,
					OldName:      []byte(crossSrc.Key.Name),
					NewParentIno: crossDstEntry.Key.ParentIno,
					NewName:      []byte(crossDstEntry.Key.Name),
					Flags:        meta.RenameExchange,
				}
				caseTag = "跨盘RenameExchange拦截(ErrCrossDevice)"
			} else if modeRoll == 1 {
				params = meta.RenameParams{
					OldParentIno: crossSrc.Key.ParentIno,
					OldName:      []byte(crossSrc.Key.Name),
					NewParentIno: crossTargetDir.Ino,
					NewName:      []byte(env.UniqueEntryName("cross_noreplace")),
					Flags:        meta.RenameNoReplace,
				}
				caseTag = "跨盘RenameNoReplace拦截(ErrCrossDevice)"
			} else {
				params = meta.RenameParams{
					OldParentIno: crossSrc.Key.ParentIno,
					OldName:      []byte(crossSrc.Key.Name),
					NewParentIno: crossTargetDir.Ino,
					NewName:      []byte(env.UniqueEntryName("cross_std")),
					Flags:        0,
				}
				caseTag = "跨盘传统Rename拦截(ErrCrossDevice)"
			}
		} else {
			src := allCandidates[0]
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: src.Key.ParentIno,
				NewName:      []byte(".."),
				Flags:        0,
			}
			caseTag = "特殊名称(..)拦截(ErrInvalidArgument)"
		}

	case branch < 92:
		// 8. 覆盖类型不匹配 (ErrNotDir / ErrIsDir) 与覆盖非空目标目录 (ErrNotEmpty)
		var mismatchDone bool
		roll := env.Rng.IntN(3)
		if roll == 0 && len(dirCandidates) > 0 && len(nonEmptyDirCandidates) > 0 {
			for _, s := range dirCandidates {
				for _, d := range nonEmptyDirCandidates {
					if s.RootIno == d.RootIno && s.Ino != d.Ino {
						if s.Key.ParentIno != d.Key.ParentIno && env.Model.IsAncestorDir(s.Ino, d.Key.ParentIno) {
							continue
						}
						params = meta.RenameParams{
							OldParentIno: s.Key.ParentIno,
							OldName:      []byte(s.Key.Name),
							NewParentIno: d.Key.ParentIno,
							NewName:      []byte(d.Key.Name),
							Flags:        0,
						}
						caseTag = "目录覆盖非空目标目录拦截(ErrNotEmpty)"
						mismatchDone = true
						break
					}
				}
				if mismatchDone {
					break
				}
			}
		}
		if !mismatchDone && len(dirCandidates) > 0 && len(nonDirCandidates) > 0 {
			for _, d := range dirCandidates {
				for _, f := range nonDirCandidates {
					if d.RootIno == f.RootIno {
						if env.Rng.IntN(2) == 0 {
							// 目录覆盖非目录（确保不先触发正向祖先环）
							if d.Key.ParentIno != f.Key.ParentIno && env.Model.IsAncestorDir(d.Ino, f.Key.ParentIno) {
								continue
							}
							params = meta.RenameParams{
								OldParentIno: d.Key.ParentIno,
								OldName:      []byte(d.Key.Name),
								NewParentIno: f.Key.ParentIno,
								NewName:      []byte(f.Key.Name),
								Flags:        0,
							}
							caseTag = "源为目录覆盖非目录目标拦截(ErrNotDir)"
						} else {
							params = meta.RenameParams{
								OldParentIno: f.Key.ParentIno,
								OldName:      []byte(f.Key.Name),
								NewParentIno: d.Key.ParentIno,
								NewName:      []byte(d.Key.Name),
								Flags:        0,
							}
							caseTag = "源为非目录覆盖目录目标拦截(ErrIsDir)"
						}
						mismatchDone = true
						break
					}
				}
				if mismatchDone {
					break
				}
			}
		}
		if !mismatchDone {
			parent := activeDirs[env.Rng.IntN(len(activeDirs))]
			params = meta.RenameParams{
				OldParentIno: parent.Ino,
				OldName:      []byte(env.UniqueEntryName("missing_src")),
				NewParentIno: parent.Ino,
				NewName:      []byte(env.UniqueEntryName("dst")),
				Flags:        0,
			}
			caseTag = "源目录项不存在拦截(ErrNotFound)"
		}

	default:
		// 9. 非法 Flags、"." / ".."、空名称、超长名称、非目录父节点或不存在父节点
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		src := allCandidates[env.Rng.IntN(len(allCandidates))]
		switch env.Rng.IntN(8) {
		case 0:
			caseTag = "非法Flags同时置位NOREPLACE与EXCHANGE(ErrInvalidArgument)"
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: parent.Ino,
				NewName:      []byte("both_flags"),
				Flags:        meta.RenameNoReplace | meta.RenameExchange,
			}
		case 1:
			caseTag = "不支持的RenameWhiteout标志(ErrNotSupported)"
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: parent.Ino,
				NewName:      []byte("whiteout_flag"),
				Flags:        meta.RenameWhiteout,
			}
		case 2:
			caseTag = "未知Flags位(ErrInvalidArgument)"
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: parent.Ino,
				NewName:      []byte("unknown_flag"),
				Flags:        0x80,
			}
		case 3:
			caseTag = "空OldName(ErrNotFound)"
			params = meta.RenameParams{
				OldParentIno: parent.Ino,
				OldName:      []byte(""),
				NewParentIno: parent.Ino,
				NewName:      []byte("valid_new"),
				Flags:        0,
			}
		case 4:
			caseTag = "空NewName(ErrInvalidArgument)"
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: parent.Ino,
				NewName:      []byte(""),
				Flags:        0,
			}
		case 5:
			caseTag = "超长NewName>255(ErrNameTooLong)"
			params = meta.RenameParams{
				OldParentIno: src.Key.ParentIno,
				OldName:      []byte(src.Key.Name),
				NewParentIno: parent.Ino,
				NewName:      []byte(strings.Repeat("r", meta.MaxFileNameBytes+1)),
				Flags:        0,
			}
		case 6:
			if len(nonDirCandidates) > 0 {
				caseTag = "目标父节点为非目录(ErrNotDir)"
				params = meta.RenameParams{
					OldParentIno: src.Key.ParentIno,
					OldName:      []byte(src.Key.Name),
					NewParentIno: nonDirCandidates[0].Ino,
					NewName:      []byte("under_file"),
					Flags:        0,
				}
			} else {
				caseTag = "目标父节点不存在(ErrNotFound)"
				params = meta.RenameParams{
					OldParentIno: src.Key.ParentIno,
					OldName:      []byte(src.Key.Name),
					NewParentIno: uint64(math.MaxInt64 - 422422),
					NewName:      []byte("ghost_dst"),
					Flags:        0,
				}
			}
		default:
			caseTag = "OldName为(..)(ErrInvalidArgument)"
			params = meta.RenameParams{
				OldParentIno: parent.Ino,
				OldName:      []byte(".."),
				NewParentIno: parent.Ino,
				NewName:      []byte("valid_new"),
				Flags:        0,
			}
		}
	}

	plan := formalPredictRename(env.Model, params)
	err := env.Mgr.Rename(ctx, params)

	if plan.ExpectedErr != nil {
		require.Errorf(t, err, "Rename[%s] (old=%d/%q -> new=%d/%q, flags=0x%x) 预期错误 %v，但实际成功",
			caseTag, params.OldParentIno, string(params.OldName), params.NewParentIno, string(params.NewName), params.Flags, plan.ExpectedErr)
		require.Truef(t, errors.Is(err, plan.ExpectedErr),
			"Rename[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, plan.ExpectedErr, err)
		return fmt.Sprintf("Rename[%s](%d/%q -> %d/%q, flags=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.OldParentIno, string(params.OldName), params.NewParentIno, string(params.NewName), params.Flags, plan.ExpectedErr)
	}

	require.NoErrorf(t, err, "Rename[%s] (old=%d/%q -> new=%d/%q, flags=0x%x) 预期成功，但返回错误: %v",
		caseTag, params.OldParentIno, string(params.OldName), params.NewParentIno, string(params.NewName), params.Flags, err)

	reclaimed := formalApplyRenameToModel(env.Model, params, plan)

	return fmt.Sprintf("Rename[%s](%d/%q[ino=%d] -> %d/%q[has_dst=%v,dst_ino=%d], flags=0x%x) -> 成功 (noop=%v, 物理回收目标=%v)",
		caseTag, params.OldParentIno, string(params.OldName), plan.OldIno,
		params.NewParentIno, string(params.NewName), plan.HasNewEntry, plan.NewIno,
		params.Flags, plan.NoOp, reclaimed)
}

// ============================================================================
// 3. T5 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T5_Rename 验证 T5 主状态机 Rename 全模式（覆盖、NOREPLACE、EXCHANGE）对拍、正反向双向防环检测、跨盘 EXDEV 与配额退还审计。
func TestFormalMain_T5_Rename(t *testing.T) {
	t.Run("确定性边界_传统覆盖全矩阵_同路径与同Inode硬链接短路_NOREPLACE_EXCHANGE双向交换_正反向祖先防环_跨盘EXDEV_EMLINK_孤儿保留与配额退还全验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 60,
			Seed:       0x20260422,
			Rng:        newFormalRNG(0x20260422),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t5-det",
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

		// 辅助闭包：执行一步 Rename，先通过 formalPredictRename 对拍错误码，成功后通过 formalApplyRenameToModel 更新模型并运行全量审计
		execCheckedRename := func(params meta.RenameParams, expectedErr error, desc string) {
			t.Helper()
			plan := formalPredictRename(env.Model, params)
			require.Equalf(t, expectedErr, plan.ExpectedErr, "%s: 参考模型预测错误码与用例预期不一致", desc)
			err := mgr.Rename(ctx, params)
			if expectedErr != nil {
				require.Errorf(t, err, "%s: 预期返回错误 %v，但实际成功", desc, expectedErr)
				require.ErrorIsf(t, err, expectedErr, "%s: 错误码不匹配", desc)
				return
			}
			require.NoErrorf(t, err, "%s: 预期成功，但返回错误: %v", desc, err)
			formalApplyRenameToModel(env.Model, params, plan)
			runAllProbesAndAuditors()
		}

		// 辅助闭包：创建子目录并登记到内存模型
		mustMkdir := func(parentIno uint64, name string) *formalInode {
			t.Helper()
			parent := env.Model.Inodes[parentIno]
			require.NotNil(t, parent)
			got, err := mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: parentIno, Name: []byte(name), Mode: 0o755})
			require.NoError(t, err)
			node := &formalInode{
				Ino: got.Ino, Generation: got.Generation, RootIno: parent.RootIno, ParentIno: parentIno,
				Mode: got.Attr.Mode, Size: meta.DefaultDirSize, Nlink: 2,
			}
			env.Model.RecordNewInode(node)
			env.Model.Entries[formalEntryKey{ParentIno: parentIno, Name: name}] = got.Ino
			parent.Nlink++
			env.Model.DiskByRootIno[parent.RootIno].UsedInodes++
			return node
		}

		// 辅助闭包：创建指定大小的常规文件并登记到内存模型
		mustCreateSizedReg := func(parentIno uint64, name string, size uint64) *formalInode {
			t.Helper()
			parent := env.Model.Inodes[parentIno]
			require.NotNil(t, parent)
			got, err := mgr.Mknod(ctx, meta.MknodParams{ParentIno: parentIno, Name: []byte(name), Mode: meta.S_IFREG | 0o644})
			require.NoError(t, err)
			if size > 0 {
				got, err = mgr.SetAttr(ctx, got.Ino, meta.SetAttrInput{Size: &size})
				require.NoError(t, err)
			}
			node := &formalInode{
				Ino: got.Ino, Generation: got.Generation, RootIno: parent.RootIno, ParentIno: 0,
				Mode: got.Attr.Mode, Size: size, Nlink: 1,
			}
			env.Model.RecordNewInode(node)
			env.Model.Entries[formalEntryKey{ParentIno: parentIno, Name: name}] = got.Ino
			disk := env.Model.DiskByRootIno[parent.RootIno]
			disk.UsedInodes++
			disk.UsedBytes += size
			return node
		}

		// 1. 创建 2 块独立虚拟磁盘 disk1 与 disk2，构建多级目录树拓扑
		formalCreateValidDiskStep(ctx, t, env)
		formalCreateValidDiskStep(ctx, t, env)
		disks := env.Model.ActiveDisks()
		require.Len(t, disks, 2)
		disk1, disk2 := disks[0], disks[1]
		root1, root2 := disk1.RootIno, disk2.RootIno

		dirA := mustMkdir(root1, "dirA")
		subA1 := mustMkdir(dirA.Ino, "subA1")
		subA2 := mustMkdir(subA1.Ino, "subA2")
		dirB := mustMkdir(root1, "dirB")
		dirC := mustMkdir(root1, "dirC")
		dirDisk2 := mustMkdir(root2, "dirDisk2")
		runAllProbesAndAuditors()

		// 2. 验证静态入参校验、非法 Flags、"."/".." 与空名称拦截
		execCheckedRename(meta.RenameParams{
			OldParentIno: 0, OldName: []byte("a"), NewParentIno: root1, NewName: []byte("b"),
		}, meta.ErrInvalidArgument, "OldParentIno=0")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("a"), NewParentIno: 0, NewName: []byte("b"),
		}, meta.ErrInvalidArgument, "NewParentIno=0")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("dirA_new"),
			Flags: meta.RenameNoReplace | meta.RenameExchange,
		}, meta.ErrInvalidArgument, "RenameNoReplace|RenameExchange同时置位")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("dirA_new"),
			Flags: meta.RenameWhiteout,
		}, meta.ErrNotSupported, "RenameWhiteout不支持")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("dirA_new"),
			Flags: 0x80,
		}, meta.ErrInvalidArgument, "未知Flags位")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte(""), NewParentIno: root1, NewName: []byte("b"),
		}, meta.ErrNotFound, "OldName为空返回ErrNotFound")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte(""),
		}, meta.ErrInvalidArgument, "NewName为空返回ErrInvalidArgument")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("."), NewParentIno: root1, NewName: []byte("b"),
		}, meta.ErrInvalidArgument, "OldName为.")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte(".."), NewParentIno: root1, NewName: []byte("b"),
		}, meta.ErrInvalidArgument, "OldName为..")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("."),
		}, meta.ErrInvalidArgument, "NewName为.")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte(".."),
		}, meta.ErrInvalidArgument, "NewName为..")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("no_such_src"), NewParentIno: root1, NewName: []byte("b"),
		}, meta.ErrNotFound, "源目录项不存在")
		runAllProbesAndAuditors()

		// 3. 验证同路径 (parent_ino == new_parent_ino && name == new_name) 与同 Inode 不同硬链接 (src_ino == dst_ino) 短路语义
		regHL := mustCreateSizedReg(dirA.Ino, "reg_hl", 4096)
		_, err := mgr.Link(ctx, meta.LinkParams{Ino: regHL.Ino, NewParentIno: dirB.Ino, NewName: []byte("reg_hl_alias")})
		require.NoError(t, err)
		regHL.Nlink = 2
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "reg_hl_alias"}] = regHL.Ino
		runAllProbesAndAuditors()

		// 3.1 同路径：flags=0 与 RenameExchange 无副作用成功；flags=RenameNoReplace 返回 ErrAlreadyExists
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirA.Ino, NewName: []byte("reg_hl"), Flags: 0,
		}, nil, "同路径_flags=0短路成功")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirA.Ino, NewName: []byte("reg_hl"), Flags: meta.RenameExchange,
		}, nil, "同路径_RenameExchange短路成功")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirA.Ino, NewName: []byte("reg_hl"), Flags: meta.RenameNoReplace,
		}, meta.ErrAlreadyExists, "同路径_RenameNoReplace必须返回ErrAlreadyExists")

		// 3.2 同 Inode 不同硬链接：flags=RenameNoReplace 返回 ErrAlreadyExists；flags=0 与 RenameExchange 无副作用成功且不删源硬链接
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirB.Ino, NewName: []byte("reg_hl_alias"), Flags: meta.RenameNoReplace,
		}, meta.ErrAlreadyExists, "同Inode硬链接_RenameNoReplace必须返回ErrAlreadyExists")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirB.Ino, NewName: []byte("reg_hl_alias"), Flags: 0,
		}, nil, "同Inode硬链接_flags=0无副作用短路")
		require.Equal(t, uint32(2), regHL.Nlink, "同Inode硬链接Rename绝不能减少nlink或删除源条目")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: dirB.Ino, NewName: []byte("reg_hl_alias"), Flags: meta.RenameExchange,
		}, nil, "同Inode硬链接_RenameExchange无副作用短路")

		// 4. 验证覆盖类型不匹配 (ErrNotDir / ErrIsDir) 与覆盖非空目标目录拦截 (ErrNotEmpty)
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirC"), NewParentIno: dirA.Ino, NewName: []byte("reg_hl"), Flags: 0,
		}, meta.ErrNotDir, "源为目录覆盖非目录返回ErrNotDir")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"), NewParentIno: root1, NewName: []byte("dirC"), Flags: 0,
		}, meta.ErrIsDir, "源为非目录覆盖目录返回ErrIsDir")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirC"), NewParentIno: root1, NewName: []byte("dirA"), Flags: 0,
		}, meta.ErrNotEmpty, "源为目录覆盖非空目标目录dirA返回ErrNotEmpty")

		// 5. 验证跨父目录移动子目录、覆盖空目标目录（同父与跨父 nlink 变化、parent_ino 更新、MaxNlink 拦截、rmdir-whilst-open 孤儿目录覆盖）
		movingDir := mustMkdir(dirB.Ino, "moving_dir")
		runAllProbesAndAuditors()

		// 5.1 跨父目录移动子目录至未覆盖新位置时，若 newParent.nlink >= MaxNlink 返回 ErrTooManyLinks
		_, err = pool.Exec(ctx, `UPDATE inodes SET nlink = $1 WHERE ino = $2`, int32(meta.MaxNlink), int64(dirC.Ino))
		require.NoError(t, err)
		env.Model.Inodes[dirC.Ino].Nlink = meta.MaxNlink
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("moving_dir"), NewParentIno: dirC.Ino, NewName: []byte("moved_dir"), Flags: 0,
		}, meta.ErrTooManyLinks, "跨父目录移动子目录当newParent达MaxNlink时返回ErrTooManyLinks")
		_, err = pool.Exec(ctx, `UPDATE inodes SET nlink = 2 WHERE ino = $1`, int64(dirC.Ino))
		require.NoError(t, err)
		env.Model.Inodes[dirC.Ino].Nlink = 2
		runAllProbesAndAuditors()

		// 5.2 正常跨父目录移动子目录 dirB/moving_dir -> dirC/moved_dir（dirB.nlink - 1, dirC.nlink + 1, movingDir.ParentIno = dirC.Ino）
		dirBNlinkBefore := dirB.Nlink
		dirCNlinkBefore := dirC.Nlink
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("moving_dir"), NewParentIno: dirC.Ino, NewName: []byte("moved_dir"), Flags: 0,
		}, nil, "跨父目录移动子目录至未覆盖新位置")
		require.Equal(t, dirBNlinkBefore-1, dirB.Nlink)
		require.Equal(t, dirCNlinkBefore+1, dirC.Nlink)
		require.Equal(t, dirC.Ino, movingDir.ParentIno)

		// 5.3 同父目录下覆盖空目标目录：dirC/moved_dir -> dirC/empty_same（dirC.nlink - 1，被覆盖空目录物理删除退 used_inodes - 1）
		emptySame := mustMkdir(dirC.Ino, "empty_same")
		usedInodesBeforeSameOverwrite := disk1.UsedInodes
		dirCNlinkBeforeSameOverwrite := dirC.Nlink
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("moved_dir"), NewParentIno: dirC.Ino, NewName: []byte("empty_same"), Flags: 0,
		}, nil, "同父目录下覆盖空目标目录")
		require.Equal(t, dirCNlinkBeforeSameOverwrite-1, dirC.Nlink)
		require.Equal(t, usedInodesBeforeSameOverwrite-1, disk1.UsedInodes)
		require.True(t, env.Model.DeletedInos[emptySame.Ino])

		// 5.4 跨父目录下覆盖空目标目录（无句柄当场物理回收）：dirC/empty_same -> dirB/empty_cross
		// 验证 dirC.nlink - 1，dirB.nlink 不变（+1 与 -1 相抵），movingDir.ParentIno = dirB.Ino
		emptyCross := mustMkdir(dirB.Ino, "empty_cross")
		dirCNlinkBeforeCross := dirC.Nlink
		dirBNlinkBeforeCross := dirB.Nlink
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("empty_same"), NewParentIno: dirB.Ino, NewName: []byte("empty_cross"), Flags: 0,
		}, nil, "跨父目录覆盖空目标目录(无句柄当场物理删除)")
		require.Equal(t, dirCNlinkBeforeCross-1, dirC.Nlink)
		require.Equal(t, dirBNlinkBeforeCross, dirB.Nlink, "跨父目录覆盖空目标目录时newParent.nlink必须保持不变")
		require.Equal(t, dirB.Ino, movingDir.ParentIno)
		require.True(t, env.Model.DeletedInos[emptyCross.Ino])

		// 5.5 跨父目录下覆盖持有活跃 OpenDir 句柄的空目标目录：被覆盖空目录转为 nlink=0 孤儿目录，ReleaseDir 后物理回收
		emptyOpenTarget := mustMkdir(dirC.Ino, "empty_open_target")
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: emptyOpenTarget.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{Fh: openDirRes.Fh, Ino: emptyOpenTarget.Ino, Flags: openDirRes.OpenFlags, IsDir: true})
		usedInodesBeforeOpenDirOverwrite := disk1.UsedInodes

		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("empty_cross"), NewParentIno: dirC.Ino, NewName: []byte("empty_open_target"), Flags: 0,
		}, nil, "跨父目录覆盖持有活跃句柄的空目标目录")
		require.Equal(t, uint32(0), emptyOpenTarget.Nlink, "被覆盖且持有句柄的空目录nlink必须置0")
		require.False(t, env.Model.DeletedInos[emptyOpenTarget.Ino], "持有句柄的孤儿目录暂不物理删除")
		require.Equal(t, usedInodesBeforeOpenDirOverwrite, disk1.UsedInodes, "孤儿目录存活期间暂不退还used_inodes")

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: emptyOpenTarget.Ino, Fh: openDirRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[openDirRes.Fh].Closed = true
		env.Model.ClosedHandles[openDirRes.Fh] = env.Model.Handles[openDirRes.Fh]
		delete(env.Model.Handles, openDirRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(emptyOpenTarget.Ino))
		require.Equal(t, usedInodesBeforeOpenDirOverwrite-1, disk1.UsedInodes)
		runAllProbesAndAuditors()

		// 6. 验证传统覆盖模式 (flags = 0) 覆盖非目录目标：
		// (a) 覆盖多硬链接目标 reg_hl_alias (nlink: 2 -> 1)，不物理删除且不退配额；
		// (b) 覆盖无句柄单硬链接常规文件 victim_closed (size=8192)，当场物理删除并退还 used_inodes - 1 与 used_bytes - 8192；
		// (c) 覆盖持有活跃 FileHandle 的单硬链接常规文件 victim_open (size=16384)，保留为 nlink=0 孤儿节点，Release 后退配额；
		// (d) 覆盖单硬链接符号链接 victim_sym (size=512)，当场物理删除，仅退 used_inodes - 1，used_bytes 不变。
		moverFile := mustCreateSizedReg(dirC.Ino, "mover_file", 1024)
		victimClosed := mustCreateSizedReg(dirB.Ino, "victim_closed", 8192)
		victimOpen := mustCreateSizedReg(dirB.Ino, "victim_open", 16384)
		symNode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: dirB.Ino, Name: []byte("victim_sym"), Target: []byte(strings.Repeat("x", 512)),
		})
		require.NoError(t, err)
		victimSym := &formalInode{
			Ino: symNode.Ino, Generation: symNode.Generation, RootIno: root1, ParentIno: 0,
			Mode: symNode.Attr.Mode, Size: 512, Nlink: 1, SymlinkTarget: []byte(strings.Repeat("x", 512)),
		}
		env.Model.RecordNewInode(victimSym)
		env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "victim_sym"}] = victimSym.Ino
		disk1.UsedInodes++
		runAllProbesAndAuditors()

		// 6.1 覆盖多硬链接目标 dirB/reg_hl_alias (regHL.Nlink: 2 -> 1)
		usedBytesBeforeMultiOverwrite := disk1.UsedBytes
		usedInodesBeforeMultiOverwrite := disk1.UsedInodes
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("mover_file"), NewParentIno: dirB.Ino, NewName: []byte("reg_hl_alias"), Flags: 0,
		}, nil, "覆盖多硬链接目标(nlink: 2->1)")
		require.Equal(t, uint32(1), regHL.Nlink)
		require.Equal(t, usedBytesBeforeMultiOverwrite, disk1.UsedBytes)
		require.Equal(t, usedInodesBeforeMultiOverwrite, disk1.UsedInodes)

		// 6.2 覆盖无句柄单硬链接常规文件 dirB/victim_closed (size=8192) -> 当场物理删除并退还 8192 字节与 1 个 inode
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("reg_hl_alias"), NewParentIno: dirB.Ino, NewName: []byte("victim_closed"), Flags: 0,
		}, nil, "覆盖无句柄单硬链接常规文件(当场物理删除并退配额)")
		require.True(t, env.Model.DeletedInos[victimClosed.Ino])
		require.Equal(t, usedBytesBeforeMultiOverwrite-8192, disk1.UsedBytes)
		require.Equal(t, usedInodesBeforeMultiOverwrite-1, disk1.UsedInodes)

		// 6.3 覆盖持有有效 FileHandle 的单硬链接常规文件 dirB/victim_open (size=16384) -> 转为 Orphan Inode 延迟退配额
		openVictimRes, err := mgr.Open(ctx, meta.OpenParams{Ino: victimOpen.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{Fh: openVictimRes.Fh, Ino: victimOpen.Ino, Flags: meta.O_RDONLY})
		usedBytesBeforeOpenVictim := disk1.UsedBytes
		usedInodesBeforeOpenVictim := disk1.UsedInodes

		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("victim_closed"), NewParentIno: dirB.Ino, NewName: []byte("victim_open"), Flags: 0,
		}, nil, "覆盖持有有效FileHandle的目标文件(保留为Orphan)")
		require.Equal(t, uint32(0), victimOpen.Nlink)
		require.False(t, env.Model.DeletedInos[victimOpen.Ino], "持有FileHandle的被覆盖目标必须保留为Orphan Inode")
		require.Equal(t, usedBytesBeforeOpenVictim, disk1.UsedBytes, "Orphan存活期间暂不退还used_bytes")
		require.Equal(t, usedInodesBeforeOpenVictim, disk1.UsedInodes, "Orphan存活期间暂不退还used_inodes")

		err = mgr.Release(ctx, meta.ReleaseParams{Ino: victimOpen.Ino, Fh: openVictimRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[openVictimRes.Fh].Closed = true
		env.Model.ClosedHandles[openVictimRes.Fh] = env.Model.Handles[openVictimRes.Fh]
		delete(env.Model.Handles, openVictimRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(victimOpen.Ino))
		require.Equal(t, usedBytesBeforeOpenVictim-16384, disk1.UsedBytes)
		require.Equal(t, usedInodesBeforeOpenVictim-1, disk1.UsedInodes)
		runAllProbesAndAuditors()

		// 6.4 覆盖单硬链接符号链接 dirB/victim_sym (size=512) -> 物理删除，used_inodes - 1，used_bytes 不变
		usedBytesBeforeSymVictim := disk1.UsedBytes
		usedInodesBeforeSymVictim := disk1.UsedInodes
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("victim_open"), NewParentIno: dirB.Ino, NewName: []byte("victim_sym"), Flags: 0,
		}, nil, "覆盖符号链接目标(used_bytes不变)")
		require.True(t, env.Model.DeletedInos[victimSym.Ino])
		require.Equal(t, usedBytesBeforeSymVictim, disk1.UsedBytes, "覆盖删除符号链接绝不扣减used_bytes")
		require.Equal(t, usedInodesBeforeSymVictim-1, disk1.UsedInodes)
		require.Equal(t, moverFile.Ino, env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "victim_sym"}])

		// 7. 验证 RenameNoReplace 模式：目标已存在返回 ErrAlreadyExists，目标不存在正常移动
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("victim_sym"), NewParentIno: dirA.Ino, NewName: []byte("reg_hl"), Flags: meta.RenameNoReplace,
		}, meta.ErrAlreadyExists, "RenameNoReplace_目标已存在返回ErrAlreadyExists")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("victim_sym"), NewParentIno: dirB.Ino, NewName: []byte("mover_noreplace_done"), Flags: meta.RenameNoReplace,
		}, nil, "RenameNoReplace_目标不存在成功重命名")

		// 8. 验证 RenameExchange 双向原子交换模式全场景
		// 8.1 源或目标任一不存在返回 ErrNotFound
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("mover_noreplace_done"), NewParentIno: dirB.Ino, NewName: []byte("nonexistent_dst"), Flags: meta.RenameExchange,
		}, meta.ErrNotFound, "RenameExchange_目标不存在返回ErrNotFound")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("nonexistent_src"), NewParentIno: dirB.Ino, NewName: []byte("mover_noreplace_done"), Flags: meta.RenameExchange,
		}, meta.ErrNotFound, "RenameExchange_源不存在返回ErrNotFound")

		// 8.2 交换两个非空目录 (root1/dirA 与 root1/dirC)：不报 ErrNotEmpty，双方 nlink 与子目录树保持完整
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("dirC"), Flags: meta.RenameExchange,
		}, nil, "RenameExchange_交换两个非空目录")
		require.Equal(t, dirC.Ino, env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirA"}])
		require.Equal(t, dirA.Ino, env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirC"}])
		// 再次交换还原名称绑定
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: root1, NewName: []byte("dirC"), Flags: meta.RenameExchange,
		}, nil, "RenameExchange_二次交换还原两个非空目录")
		require.Equal(t, dirA.Ino, env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirA"}])
		require.Equal(t, dirC.Ino, env.Model.Entries[formalEntryKey{ParentIno: root1, Name: "dirC"}])

		// 8.3 跨父目录交换“一目录 (dirC/empty_open_target -> movingDir) 一非目录 (dirB/mover_noreplace_done -> moverFile)”：
		// 先验证移入目录方的父目录 nlink >= MaxNlink 时返回 ErrTooManyLinks（正反双向）
		_, err = pool.Exec(ctx, `UPDATE inodes SET nlink = $1 WHERE ino = $2`, int32(meta.MaxNlink), int64(dirB.Ino))
		require.NoError(t, err)
		env.Model.Inodes[dirB.Ino].Nlink = meta.MaxNlink
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("empty_open_target"),
			NewParentIno: dirB.Ino, NewName: []byte("mover_noreplace_done"),
			Flags: meta.RenameExchange,
		}, meta.ErrTooManyLinks, "RenameExchange_源为目录目标为非目录且newParent达MaxNlink返回ErrTooManyLinks")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirB.Ino, OldName: []byte("mover_noreplace_done"),
			NewParentIno: dirC.Ino, NewName: []byte("empty_open_target"),
			Flags: meta.RenameExchange,
		}, meta.ErrTooManyLinks, "RenameExchange_源为非目录目标为目录且oldParent达MaxNlink返回ErrTooManyLinks")
		_, err = pool.Exec(ctx, `UPDATE inodes SET nlink = 2 WHERE ino = $1`, int64(dirB.Ino))
		require.NoError(t, err)
		env.Model.Inodes[dirB.Ino].Nlink = 2
		runAllProbesAndAuditors()

		// 正常执行跨父目录“一目录一非目录”交换：dirC (移出目录) nlink: 3 -> 2，dirB (移入目录) nlink: 2 -> 3，movingDir.ParentIno = dirB.Ino
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("empty_open_target"),
			NewParentIno: dirB.Ino, NewName: []byte("mover_noreplace_done"),
			Flags: meta.RenameExchange,
		}, nil, "RenameExchange_跨父目录交换目录与常规文件(正向)")
		require.Equal(t, uint32(2), dirC.Nlink)
		require.Equal(t, uint32(3), dirB.Nlink)
		require.Equal(t, dirB.Ino, movingDir.ParentIno)
		require.Equal(t, moverFile.Ino, env.Model.Entries[formalEntryKey{ParentIno: dirC.Ino, Name: "empty_open_target"}])
		require.Equal(t, movingDir.Ino, env.Model.Entries[formalEntryKey{ParentIno: dirB.Ino, Name: "mover_noreplace_done"}])

		// 反向再交换一次：dirC (移入目录) nlink: 2 -> 3，dirB (移出目录) nlink: 3 -> 2，movingDir.ParentIno = dirC.Ino
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirC.Ino, OldName: []byte("empty_open_target"),
			NewParentIno: dirB.Ino, NewName: []byte("mover_noreplace_done"),
			Flags: meta.RenameExchange,
		}, nil, "RenameExchange_跨父目录交换常规文件与目录(反向还原)")
		require.Equal(t, uint32(3), dirC.Nlink)
		require.Equal(t, uint32(2), dirB.Nlink)
		require.Equal(t, dirC.Ino, movingDir.ParentIno)

		// 9. 验证单向与双向祖先防环检测 (ErrInvalidArgument)
		// 9.1 正向防环：将 dirA 移动到 dirA 自身下 (NewParentIno == dirA.Ino) 或子孙目录 subA1 / subA2 下
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: dirA.Ino, NewName: []byte("self_child"), Flags: 0,
		}, meta.ErrInvalidArgument, "正向防环_目录移动到自身名下")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: subA1.Ino, NewName: []byte("into_subA1"), Flags: meta.RenameNoReplace,
		}, meta.ErrInvalidArgument, "正向防环_目录移动到直接子目录subA1下")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"), NewParentIno: subA2.Ino, NewName: []byte("into_subA2"), Flags: 0,
		}, meta.ErrInvalidArgument, "正向防环_目录移动到深层后代subA2下")

		// 9.2 双向防环 (RenameExchange)：在深层目录 subA2 下创建节点 leafInSubA2，尝试与祖先目录 (root1, "dirA") 或 (dirA.Ino, "subA1") 交换
		leafInSubA2 := mustCreateSizedReg(subA2.Ino, "leaf_in_subA2", 512)
		runAllProbesAndAuditors()
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirA"),
			NewParentIno: subA2.Ino, NewName: []byte("leaf_in_subA2"),
			Flags: meta.RenameExchange,
		}, meta.ErrInvalidArgument, "RenameExchange_正向防环_祖先dirA与后代subA2下节点交换")
		execCheckedRename(meta.RenameParams{
			OldParentIno: subA2.Ino, OldName: []byte("leaf_in_subA2"),
			NewParentIno: root1, NewName: []byte("dirA"),
			Flags: meta.RenameExchange,
		}, meta.ErrInvalidArgument, "RenameExchange_反向防环_后代subA2下节点与祖先dirA交换")
		execCheckedRename(meta.RenameParams{
			OldParentIno: subA2.Ino, OldName: []byte("leaf_in_subA2"),
			NewParentIno: dirA.Ino, NewName: []byte("subA1"),
			Flags: meta.RenameExchange,
		}, meta.ErrInvalidArgument, "RenameExchange_反向防环_后代subA2下节点与祖先subA1交换")
		execCheckedRename(meta.RenameParams{
			OldParentIno: subA1.Ino, OldName: []byte("subA2"),
			NewParentIno: root1, NewName: []byte("dirA"),
			Flags: meta.RenameExchange,
		}, meta.ErrInvalidArgument, "RenameExchange_反向防环_后代目录subA2与祖先目录dirA交换")
		_ = leafInSubA2

		// 10. 验证跨虚拟磁盘 EXDEV 拦截（涵盖 flags=0, RenameNoReplace, RenameExchange）
		fileOnDisk2 := mustCreateSizedReg(dirDisk2.Ino, "file_on_disk2", 2048)
		runAllProbesAndAuditors()
		_ = fileOnDisk2

		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"),
			NewParentIno: dirDisk2.Ino, NewName: []byte("cross_target"),
			Flags: 0,
		}, meta.ErrCrossDevice, "跨盘Rename_flags=0目标不存在返回ErrCrossDevice")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"),
			NewParentIno: dirDisk2.Ino, NewName: []byte("file_on_disk2"),
			Flags: 0,
		}, meta.ErrCrossDevice, "跨盘Rename_flags=0覆盖目标返回ErrCrossDevice")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"),
			NewParentIno: dirDisk2.Ino, NewName: []byte("file_on_disk2"),
			Flags: meta.RenameNoReplace,
		}, meta.ErrCrossDevice, "跨盘Rename_RenameNoReplace优先返回ErrCrossDevice")
		execCheckedRename(meta.RenameParams{
			OldParentIno: dirA.Ino, OldName: []byte("reg_hl"),
			NewParentIno: dirDisk2.Ino, NewName: []byte("file_on_disk2"),
			Flags: meta.RenameExchange,
		}, meta.ErrCrossDevice, "跨盘Rename_RenameExchange返回ErrCrossDevice")
		execCheckedRename(meta.RenameParams{
			OldParentIno: root1, OldName: []byte("dirB"),
			NewParentIno: root2, NewName: []byte("dirDisk2"),
			Flags: meta.RenameExchange,
		}, meta.ErrCrossDevice, "跨盘Rename_RenameExchange交换两个目录返回ErrCrossDevice")
	})

	t.Run("随机状态机_Rename全模式覆盖_NOREPLACE_EXCHANGE_双向防环_跨盘EXDEV与底表INV-1_INV-2_INV-4_INV-5全量审计", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			InitHook: func(ctx context.Context, t *testing.T, env *formalMainEnv) {
				// 初始创建 2 块虚拟磁盘，确保从第 1 步起即具备同盘多目录与跨盘 EXDEV 碰撞条件
				env.StepIndex = 0
				d1 := formalCreateValidDiskStep(ctx, t, env)
				d2 := formalCreateValidDiskStep(ctx, t, env)
				env.AppendTrace(0, fmt.Sprintf("[初始双盘] %s; %s", d1, d2))
			},
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 6, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 6, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 12, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 18, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 8, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 16, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 12, Fn: formalActionSymlink},
				{Name: "Link", Weight: 18, Fn: formalActionLink},
				{Name: "Unlink", Weight: 10, Fn: formalActionUnlink},
				{Name: "Rename", Weight: 45, Fn: formalActionRename},
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
