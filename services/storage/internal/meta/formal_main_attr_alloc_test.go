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
	// 注册 T9 核心属性修改与空间预分配动作：SetAttr、Fallocate
	registerFormalAction("SetAttr", 28, formalActionSetAttr)
	registerFormalAction("Fallocate", 24, formalActionFallocate)
}

// ============================================================================
// 1. T9 参考模型预测器：SetAttr 与 Fallocate
// ============================================================================

// formalSetAttrPrediction 封装参考模型对 SetAttr 执行结果的预测。
type formalSetAttrPrediction struct {
	TargetNode  *formalInode
	TargetMode  uint32
	TargetUID   uint32
	TargetGID   uint32
	TargetSize  uint64
	TargetFlags uint32
	ExpectedErr error
}

// formalPredictSetAttr 根据内存参考模型预测 SetAttr 的目标属性变更与预期错误码。
// 严格对齐 manager_setattr_fallocate.go 中 SetAttr 的静态校验、写事务校验优先级、特权位剥离与配额检查规则。
func formalPredictSetAttr(
	m *formalMainModel,
	ino uint64,
	input meta.SetAttrInput,
) formalSetAttrPrediction {
	// 1. 静态入参边界校验
	if ino == 0 || ino > math.MaxInt64 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.Mode == nil && input.UID == nil && input.GID == nil && input.Size == nil &&
		input.Atime == nil && input.Mtime == nil && !input.AtimeNow && !input.MtimeNow &&
		!input.CtimeNow && input.Flags == nil {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.Fh != nil && (*input.Fh == 0 || *input.Fh > math.MaxInt64) {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if err := modelValidateOptionalSessionID(input.SessionID); err != nil {
		return formalSetAttrPrediction{ExpectedErr: err}
	}
	if (input.Atime != nil && input.AtimeNow) || (input.Mtime != nil && input.MtimeNow) {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.Mode != nil && *input.Mode > 0o177777 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.UID != nil && *input.UID > math.MaxInt32 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.GID != nil && *input.GID > math.MaxInt32 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.Size != nil && *input.Size > math.MaxInt64 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if input.Flags != nil && *input.Flags > math.MaxInt32 {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
	}

	// 2. 写事务内目标节点存在性校验
	inode, exists := m.Inodes[ino]
	if !exists || inode.Deleted {
		return formalSetAttrPrediction{ExpectedErr: meta.ErrNotFound}
	}

	fileType := inode.FileType()

	// 3. POSIX 文件类型与 Size 约束
	if input.Size != nil {
		if fileType == meta.S_IFDIR {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrIsDir}
		}
		if fileType != meta.S_IFREG {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
		}
	}

	// 4. 可选文件句柄归属、会话有效性与写权限校验
	if input.Fh != nil {
		h, hExists := m.Handles[*input.Fh]
		if !hExists || h.Closed || h.Ino != ino {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrBadFileHandle}
		}
		if !modelSessionIDEqual(h.SessionID, input.SessionID) {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrBadFileHandle}
		}
		if !m.IsSessionValid(h.SessionID) {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrBadFileHandle}
		}
		if input.Size != nil {
			accMode := h.Flags & meta.O_ACCMODE
			if accMode != meta.O_WRONLY && accMode != meta.O_RDWR {
				return formalSetAttrPrediction{ExpectedErr: meta.ErrBadFileHandle}
			}
		}
	}

	// 5. Mode 更新与特权位 (S_ISUID / S_ISGID) 剥离规则
	targetMode := inode.Mode
	if input.Mode != nil {
		if fileType == meta.S_IFLNK {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
		}
		inputType := *input.Mode & meta.S_IFMT
		if inputType != 0 && inputType != fileType {
			return formalSetAttrPrediction{ExpectedErr: meta.ErrInvalidArgument}
		}
		targetMode = fileType | (*input.Mode & meta.PermMask)
	} else if fileType == meta.S_IFREG && (input.UID != nil || input.GID != nil || input.Size != nil) {
		targetMode &^= (meta.S_ISUID | meta.S_ISGID)
	}

	targetUID := inode.UID
	if input.UID != nil {
		targetUID = *input.UID
	}

	targetGID := inode.GID
	if input.GID != nil {
		targetGID = *input.GID
	}

	targetSize := inode.Size
	if input.Size != nil {
		targetSize = *input.Size
	}

	targetFlags := inode.Flags
	if input.Flags != nil {
		targetFlags = *input.Flags
	}

	// 6. 常规文件扩容配额上限校验 (超限返回 ErrNoSpace 并回滚整个事务)
	if input.Size != nil && fileType == meta.S_IFREG && inode.RootIno > 0 {
		oldSize := inode.Size
		if targetSize > oldSize {
			deltaBytes := targetSize - oldSize
			if disk, ok := m.DiskByRootIno[inode.RootIno]; ok && !disk.Deleted {
				if disk.UsedBytes > disk.MaxBytes || deltaBytes > disk.MaxBytes-disk.UsedBytes {
					return formalSetAttrPrediction{ExpectedErr: meta.ErrNoSpace}
				}
			}
		}
	}

	return formalSetAttrPrediction{
		TargetNode:  inode,
		TargetMode:  targetMode,
		TargetUID:   targetUID,
		TargetGID:   targetGID,
		TargetSize:  targetSize,
		TargetFlags: targetFlags,
		ExpectedErr: nil,
	}
}

// formalFallocatePrediction 封装参考模型对 Fallocate 执行结果的预测。
type formalFallocatePrediction struct {
	TargetNode  *formalInode
	TargetMode  uint32
	TargetSize  uint64
	ExpectedErr error
}

// formalPredictFallocate 根据内存参考模型预测 Fallocate 的目标 Mode、Size 与预期错误码。
// 严格对齐 manager_setattr_fallocate.go 中 Fallocate 的静态校验、模式校验、句柄写权限校验与配额检查规则。
func formalPredictFallocate(
	m *formalMainModel,
	params meta.FallocateParams,
) formalFallocatePrediction {
	// 1. 静态入参边界与整型溢出校验
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh == 0 || params.Fh > math.MaxInt64 ||
		params.Length == 0 || params.Length > math.MaxInt64 ||
		params.Offset > math.MaxInt64 {
		return formalFallocatePrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if params.Offset > uint64(math.MaxInt64)-params.Length {
		return formalFallocatePrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if err := modelValidateOptionalSessionID(params.SessionID); err != nil {
		return formalFallocatePrediction{ExpectedErr: err}
	}

	// 2. 模式位静态校验
	if params.Mode == meta.FallocFlPunchHole {
		return formalFallocatePrediction{ExpectedErr: meta.ErrInvalidArgument}
	}
	if params.Mode != 0 &&
		params.Mode != meta.FallocFlKeepSize &&
		params.Mode != (meta.FallocFlPunchHole|meta.FallocFlKeepSize) {
		return formalFallocatePrediction{ExpectedErr: meta.ErrNotSupported}
	}

	// 3. 写事务内目标节点存在性与文件类型校验
	inode, exists := m.Inodes[params.Ino]
	if !exists || inode.Deleted {
		return formalFallocatePrediction{ExpectedErr: meta.ErrNotFound}
	}

	fileType := inode.FileType()
	if fileType == meta.S_IFDIR {
		return formalFallocatePrediction{ExpectedErr: meta.ErrIsDir}
	}
	if fileType != meta.S_IFREG {
		return formalFallocatePrediction{ExpectedErr: meta.ErrNotSupported}
	}

	// 4. 文件句柄归属、会话有效性与写权限校验
	h, hExists := m.Handles[params.Fh]
	if !hExists || h.Closed || h.Ino != params.Ino {
		return formalFallocatePrediction{ExpectedErr: meta.ErrBadFileHandle}
	}
	if !modelSessionIDEqual(h.SessionID, params.SessionID) {
		return formalFallocatePrediction{ExpectedErr: meta.ErrBadFileHandle}
	}
	if !m.IsSessionValid(h.SessionID) {
		return formalFallocatePrediction{ExpectedErr: meta.ErrBadFileHandle}
	}
	accMode := h.Flags & meta.O_ACCMODE
	if accMode != meta.O_WRONLY && accMode != meta.O_RDWR {
		return formalFallocatePrediction{ExpectedErr: meta.ErrBadFileHandle}
	}

	// 5. 三种合法模式的状态转移与配额上限预测
	targetMode := inode.Mode
	targetSize := inode.Size

	switch params.Mode {
	case 0:
		reqEnd := params.Offset + params.Length
		if reqEnd > inode.Size {
			deltaBytes := reqEnd - inode.Size
			if inode.RootIno > 0 {
				if disk, ok := m.DiskByRootIno[inode.RootIno]; ok && !disk.Deleted {
					if disk.UsedBytes > disk.MaxBytes || deltaBytes > disk.MaxBytes-disk.UsedBytes {
						return formalFallocatePrediction{ExpectedErr: meta.ErrNoSpace}
					}
				}
			}
			targetSize = reqEnd
		}
	case meta.FallocFlKeepSize:
		// 保持 Size 与 Mode 不变
	case meta.FallocFlPunchHole | meta.FallocFlKeepSize:
		// 打洞保持 Size 不变，但剥离 S_ISUID | S_ISGID 特权位
		targetMode = inode.Mode &^ (meta.S_ISUID | meta.S_ISGID)
	}

	return formalFallocatePrediction{
		TargetNode:  inode,
		TargetMode:  targetMode,
		TargetSize:  targetSize,
		ExpectedErr: nil,
	}
}

// applyFormalSetAttrToModel 在 SetAttr 执行成功后同步更新内存参考模型的 Inode 属性、磁盘配额与会话租约。
func applyFormalSetAttrToModel(
	m *formalMainModel,
	pred formalSetAttrPrediction,
	input meta.SetAttrInput,
	leaseTTL time.Duration,
) {
	modelSyncSessionRenewal(m, input.SessionID, leaseTTL)
	inode := pred.TargetNode
	if inode == nil || inode.Deleted {
		return
	}

	oldSize := inode.Size
	inode.Mode = pred.TargetMode
	inode.UID = pred.TargetUID
	inode.GID = pred.TargetGID
	inode.Size = pred.TargetSize
	inode.Flags = pred.TargetFlags

	if input.Size != nil && inode.IsReg() && inode.RootIno > 0 {
		if disk, ok := m.DiskByRootIno[inode.RootIno]; ok && !disk.Deleted {
			if pred.TargetSize > oldSize {
				disk.UsedBytes += pred.TargetSize - oldSize
			} else if pred.TargetSize < oldSize {
				diff := oldSize - pred.TargetSize
				if disk.UsedBytes >= diff {
					disk.UsedBytes -= diff
				} else {
					disk.UsedBytes = 0
				}
			}
		}
	}
}

// applyFormalFallocateToModel 在 Fallocate 执行成功后同步更新内存参考模型的 Inode 属性、磁盘配额与会话租约。
func applyFormalFallocateToModel(
	m *formalMainModel,
	pred formalFallocatePrediction,
	params meta.FallocateParams,
	leaseTTL time.Duration,
) {
	modelSyncSessionRenewal(m, params.SessionID, leaseTTL)
	inode := pred.TargetNode
	if inode == nil || inode.Deleted {
		return
	}

	oldSize := inode.Size
	inode.Mode = pred.TargetMode
	inode.Size = pred.TargetSize

	if inode.IsReg() && inode.RootIno > 0 && pred.TargetSize > oldSize {
		if disk, ok := m.DiskByRootIno[inode.RootIno]; ok && !disk.Deleted {
			disk.UsedBytes += pred.TargetSize - oldSize
		}
	}
}

// ============================================================================
// 2. T9 动作实现：SetAttr 与 Fallocate 随机操作生成与逐操作对拍
// ============================================================================

// formalActionSetAttr 在状态机中随机生成部分属性掩码更新、特权位 (S_ISUID/S_ISGID) 自动剥离或显式保留、
// 常规文件与孤儿常规文件 (nlink==0) 扩缩容、超配额 ENOSPC 原子回滚、可选 Fh 写权限校验以及各类非法参数拦截请求并与参考模型对拍。
func formalActionSetAttr(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	regFiles := env.Model.ActiveRegFiles()
	symlinks := env.Model.ActiveSymlinks()
	activeFileHandles := env.Model.ActiveFileHandles()

	var orphanRegFiles []*formalInode
	var specialNodes []*formalInode
	for _, n := range env.Model.ActiveNonDirs() {
		if n.IsReg() && n.Nlink == 0 && env.Model.HasValidHandlesForIno(n.Ino) {
			orphanRegFiles = append(orphanRegFiles, n)
		} else if !n.IsReg() && !n.IsLnk() {
			specialNodes = append(specialNodes, n)
		}
	}

	var writableRegHandles []*formalHandle
	var readOnlyRegHandles []*formalHandle
	for _, h := range activeFileHandles {
		inode, ok := env.Model.Inodes[h.Ino]
		if !ok || inode.Deleted || !inode.IsReg() {
			continue
		}
		acc := h.Flags & meta.O_ACCMODE
		if acc == meta.O_WRONLY || acc == meta.O_RDWR {
			writableRegHandles = append(writableRegHandles, h)
		} else {
			readOnlyRegHandles = append(readOnlyRegHandles, h)
		}
	}

	uids := []uint32{0, 1000, 1001, 2002, 65534}
	gids := []uint32{0, 1000, 2000, 3003, 65534}
	flagsPool := []uint32{0, meta.FlagImmutable, meta.FlagAppend, meta.FlagImmutable | meta.FlagAppend}
	fixedTime := time.Unix(1710000000+int64(env.NextSeq()), 123456000).UTC()

	var targetIno uint64
	var input meta.SetAttrInput
	var caseTag string

	branch := env.Rng.IntN(100)

	switch {
	case branch < 14 && len(orphanRegFiles) > 0:
		// 重点场景 1：对 nlink == 0 的活跃孤儿常规文件执行 SetAttr(Size) 扩缩容
		orphan := orphanRegFiles[env.Rng.IntN(len(orphanRegFiles))]
		targetIno = orphan.Ino
		disk := env.Model.DiskByRootIno[orphan.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		var newSize uint64
		if avail >= 512 && (orphan.Size == 0 || env.Rng.IntN(2) == 0) {
			delta := uint64(env.Rng.IntN(4096) + 256)
			if delta > avail {
				delta = avail
			}
			newSize = orphan.Size + delta
			caseTag = "活跃孤儿常规文件(nlink=0)扩容"
		} else if orphan.Size > 0 {
			newSize = orphan.Size / 2
			caseTag = "活跃孤儿常规文件(nlink=0)缩容"
		} else {
			newSize = 0
			caseTag = "活跃孤儿常规文件(nlink=0)保持Size=0"
		}
		input.Size = &newSize
		// 若该孤儿存在可写句柄，按概率绑定 Fh 与 SessionID
		for _, h := range writableRegHandles {
			if h.Ino == orphan.Ino {
				if env.Rng.IntN(2) == 0 {
					fhCopy := h.Fh
					input.Fh = &fhCopy
					input.SessionID = modelCloneSessionID(h.SessionID)
				}
				break
			}
		}

	case branch < 32 && len(regFiles) > 0:
		// 重点场景 2：常规文件合法扩缩容与配额实时联动（含缩容至 0、扩容、同大小幂等）
		target := regFiles[env.Rng.IntN(len(regFiles))]
		targetIno = target.Ino
		disk := env.Model.DiskByRootIno[target.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}

		var newSize uint64
		switch env.Rng.IntN(4) {
		case 0:
			if avail > 0 {
				maxDelta := avail
				if maxDelta > 16384 {
					maxDelta = 16384
				}
				newSize = target.Size + uint64(env.Rng.Uint64N(maxDelta)) + 1
				caseTag = "常规文件合法扩容"
			} else {
				newSize = target.Size
				caseTag = "常规文件同Size幂等更新"
			}
		case 1:
			if target.Size > 0 {
				newSize = uint64(env.Rng.Uint64N(target.Size))
				caseTag = "常规文件合法缩容"
			} else {
				newSize = 0
				caseTag = "常规文件Size=0幂等更新"
			}
		case 2:
			newSize = 0
			caseTag = "常规文件截断Size=0"
		default:
			if avail >= 1024 {
				newSize = target.Size + 1024
				caseTag = "常规文件扩容+时间戳联动"
				input.AtimeNow = true
			} else {
				newSize = 0
				caseTag = "常规文件截断Size=0"
			}
		}
		input.Size = &newSize

	case branch < 44 && len(regFiles) > 0:
		// 重点场景 3：特权位 (S_ISUID | S_ISGID) 注入与自动剥离验证
		target := regFiles[env.Rng.IntN(len(regFiles))]
		targetIno = target.Ino
		if (target.Mode & (meta.S_ISUID | meta.S_ISGID)) == 0 {
			// 先为该常规文件显式注入 S_ISUID | S_ISGID | S_ISVTX 特权位
			privMode := uint32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | meta.S_ISVTX | 0o755)
			if env.Rng.IntN(2) == 0 {
				privMode = meta.S_ISUID | meta.S_ISGID | 0o755 // 不带 S_IFMT 仅传权限与特权位
			}
			input.Mode = &privMode
			caseTag = "常规文件显式设置S_ISUID|S_ISGID特权位"
		} else {
			// 目标已含 S_ISUID/S_ISGID：不传 Mode，修改 UID/GID/Size 触发特权位自动剥离，或显式传 Mode 保留特权位
			switch env.Rng.IntN(4) {
			case 0:
				newUID := uids[env.Rng.IntN(len(uids))]
				input.UID = &newUID
				caseTag = "常规文件修改UID自动剥离S_ISUID|S_ISGID"
			case 1:
				newGID := gids[env.Rng.IntN(len(gids))]
				input.GID = &newGID
				caseTag = "常规文件修改GID自动剥离S_ISUID|S_ISGID"
			case 2:
				newSize := target.Size
				input.Size = &newSize
				caseTag = "常规文件修改Size自动剥离S_ISUID|S_ISGID"
			default:
				keepPriv := uint32(meta.S_ISUID | meta.S_ISGID | 0o750)
				newUID := uids[env.Rng.IntN(len(uids))]
				input.Mode = &keepPriv
				input.UID = &newUID
				caseTag = "常规文件同时传Mode与UID显式保留特权位"
			}
		}

	case branch < 54 && len(regFiles) > 0:
		// 重点场景 4：超配额扩容触发 ErrNoSpace (ENOSPC) 原子回滚验证（同时附带 Mode/UID/GID/Flags 修改，验证全部回滚）
		target := regFiles[env.Rng.IntN(len(regFiles))]
		targetIno = target.Ino
		disk := env.Model.DiskByRootIno[target.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		overflowSize := target.Size + avail + uint64(env.Rng.IntN(8192)+1)
		if overflowSize > math.MaxInt64 {
			overflowSize = math.MaxInt64
		}
		newMode := uint32(0o700)
		newUID := uint32(9999)
		newGID := uint32(8888)
		newFlags := meta.FlagImmutable
		input.Size = &overflowSize
		input.Mode = &newMode
		input.UID = &newUID
		input.GID = &newGID
		input.Flags = &newFlags
		caseTag = "超配额扩容触发ErrNoSpace事务原子回滚(含Mode/UID/GID/Flags)"

	case branch < 66:
		// 重点场景 5：多字段掩码组合更新（Mode/UID/GID/Atime/Mtime/AtimeNow/MtimeNow/CtimeNow/Flags）及目录属性更新
		if env.Rng.IntN(3) == 0 || len(regFiles) == 0 {
			dir := activeDirs[env.Rng.IntN(len(activeDirs))]
			targetIno = dir.Ino
			switch env.Rng.IntN(3) {
			case 0:
				newUID := uids[env.Rng.IntN(len(uids))]
				newGID := gids[env.Rng.IntN(len(gids))]
				input.UID = &newUID
				input.GID = &newGID
				caseTag = "目录修改UID与GID(保留目录原S_ISGID位)"
			case 1:
				dirMode := uint32(meta.S_IFDIR | meta.S_ISGID | 0o775)
				input.Mode = &dirMode
				input.AtimeNow = true
				input.MtimeNow = true
				caseTag = "目录修改Mode(S_ISGID|0o775)与AtimeNow|MtimeNow"
			default:
				input.CtimeNow = true
				caseTag = "目录仅设置CtimeNow幂等返回当前属性"
			}
		} else {
			target := regFiles[env.Rng.IntN(len(regFiles))]
			targetIno = target.Ino
			switch env.Rng.IntN(4) {
			case 0:
				tAtime := fixedTime
				tMtime := fixedTime.Add(10 * time.Second)
				fVal := flagsPool[env.Rng.IntN(len(flagsPool))]
				input.Atime = &tAtime
				input.Mtime = &tMtime
				input.Flags = &fVal
				caseTag = "常规文件显式Atime+Mtime+Flags更新"
			case 1:
				input.AtimeNow = true
				input.MtimeNow = true
				input.CtimeNow = true
				caseTag = "常规文件AtimeNow+MtimeNow+CtimeNow更新"
			case 2:
				input.CtimeNow = true
				caseTag = "常规文件仅CtimeNow幂等返回当前属性"
			default:
				newMode := uint32(0o640)
				newUID := uids[env.Rng.IntN(len(uids))]
				newGID := gids[env.Rng.IntN(len(gids))]
				input.Mode = &newMode
				input.UID = &newUID
				input.GID = &newGID
				caseTag = "常规文件Mode+UID+GID组合更新"
			}
		}

	case branch < 76 && (len(writableRegHandles) > 0 || len(readOnlyRegHandles) > 0):
		// 重点场景 6：携带关联 Fh 的 SetAttr（合法写句柄 ftruncate vs 只读句柄 O_RDONLY 修改 Size 触发 ErrBadFileHandle）
		if len(writableRegHandles) > 0 && (len(readOnlyRegHandles) == 0 || env.Rng.IntN(2) == 0) {
			h := writableRegHandles[env.Rng.IntN(len(writableRegHandles))]
			targetIno = h.Ino
			fhCopy := h.Fh
			input.Fh = &fhCopy
			input.SessionID = modelCloneSessionID(h.SessionID)
			inode := env.Model.Inodes[h.Ino]
			disk := env.Model.DiskByRootIno[inode.RootIno]
			avail := uint64(0)
			if disk != nil && disk.MaxBytes > disk.UsedBytes {
				avail = disk.MaxBytes - disk.UsedBytes
			}
			newSize := inode.Size
			if avail >= 256 {
				newSize = inode.Size + 256
			}
			input.Size = &newSize
			caseTag = "携带有效写句柄Fh执行ftruncate扩容"
		} else {
			h := readOnlyRegHandles[env.Rng.IntN(len(readOnlyRegHandles))]
			targetIno = h.Ino
			fhCopy := h.Fh
			input.Fh = &fhCopy
			input.SessionID = modelCloneSessionID(h.SessionID)
			if env.Rng.IntN(2) == 0 {
				badSize := uint64(128)
				input.Size = &badSize
				caseTag = "携带只读句柄O_RDONLY修改Size拦截(ErrBadFileHandle)"
			} else {
				newUID := uint32(1001)
				input.UID = &newUID
				caseTag = "携带只读句柄O_RDONLY仅修改UID成功"
			}
		}

	case branch < 85:
		// 重点场景 7：类型限制拦截（对目录设置 Size -> ErrIsDir；对 S_IFLNK/特殊节点设置 Size -> ErrInvalidArgument；对 S_IFLNK 设置 Mode -> ErrInvalidArgument）
		switch {
		case len(symlinks) > 0 && env.Rng.IntN(3) == 0:
			sym := symlinks[env.Rng.IntN(len(symlinks))]
			targetIno = sym.Ino
			if env.Rng.IntN(2) == 0 {
				sz := uint64(16)
				input.Size = &sz
				caseTag = "对符号链接S_IFLNK设置Size拦截(ErrInvalidArgument)"
			} else {
				m := uint32(0o777)
				input.Mode = &m
				caseTag = "对符号链接S_IFLNK设置Mode拦截(ErrInvalidArgument)"
			}
		case len(specialNodes) > 0 && env.Rng.IntN(2) == 0:
			sp := specialNodes[env.Rng.IntN(len(specialNodes))]
			targetIno = sp.Ino
			sz := uint64(64)
			input.Size = &sz
			caseTag = "对特殊设备/FIFO/SOCK设置Size拦截(ErrInvalidArgument)"
		default:
			dir := activeDirs[env.Rng.IntN(len(activeDirs))]
			targetIno = dir.Ino
			sz := uint64(4096)
			input.Size = &sz
			caseTag = "对目录S_IFDIR设置Size拦截(ErrIsDir)"
		}

	case branch < 93:
		// 重点场景 8：句柄不匹配或无效句柄（不存在 Fh、跨节点 Fh、已关闭 Fh、SessionID 不匹配）-> ErrBadFileHandle
		targetIno = activeDirs[0].Ino
		if len(regFiles) > 0 {
			targetIno = regFiles[env.Rng.IntN(len(regFiles))].Ino
		}
		newUID := uint32(1000)
		input.UID = &newUID

		if len(env.Model.ClosedHandles) > 0 && env.Rng.IntN(3) == 0 {
			for _, fh := range env.Model.SortedFhs() {
				if ch, ok := env.Model.ClosedHandles[fh]; ok && ch.Ino == targetIno {
					fhCopy := fh
					input.Fh = &fhCopy
					input.SessionID = modelCloneSessionID(ch.SessionID)
					caseTag = "传入已关闭Fh调用SetAttr拦截(ErrBadFileHandle)"
					break
				}
			}
		}
		if input.Fh == nil && len(activeFileHandles) > 0 && env.Rng.IntN(2) == 0 {
			h := activeFileHandles[env.Rng.IntN(len(activeFileHandles))]
			fhCopy := h.Fh
			input.Fh = &fhCopy
			wrongSID := uint64(777888)
			input.SessionID = &wrongSID
			targetIno = h.Ino
			caseTag = "传入SessionID不匹配的Fh调用SetAttr拦截(ErrBadFileHandle)"
		}
		if input.Fh == nil {
			bogusFh := uint64(math.MaxInt64 - 444555)
			input.Fh = &bogusFh
			caseTag = "传入不存在Fh调用SetAttr拦截(ErrBadFileHandle)"
		}

	default:
		// 重点场景 9：非法掩码/边界入参拦截（全空入参、Atime+AtimeNow互斥、Mtime+MtimeNow互斥、Mode类型冲突或越界、UID/GID/Flags/Size越界、Ino=0、不存在Ino）
		targetIno = activeDirs[0].Ino
		if len(regFiles) > 0 {
			targetIno = regFiles[0].Ino
		}
		switch env.Rng.IntN(10) {
		case 0:
			caseTag = "全空SetAttrInput无任何更新项(ErrInvalidArgument)"
		case 1:
			tNow := fixedTime
			input.Atime = &tNow
			input.AtimeNow = true
			caseTag = "Atime与AtimeNow同时置位互斥(ErrInvalidArgument)"
		case 2:
			tNow := fixedTime
			input.Mtime = &tNow
			input.MtimeNow = true
			caseTag = "Mtime与MtimeNow同时置位互斥(ErrInvalidArgument)"
		case 3:
			badMode := uint32(0o200000)
			input.Mode = &badMode
			caseTag = "Mode超过0o177777越界(ErrInvalidArgument)"
		case 4:
			targetIno = activeDirs[0].Ino
			mismatchMode := uint32(meta.S_IFREG | 0o644)
			input.Mode = &mismatchMode
			caseTag = "目录传入S_IFREG类型冲突Mode(ErrInvalidArgument)"
		case 5:
			badUID := uint32(math.MaxInt32) + 1
			input.UID = &badUID
			caseTag = "UID超过MaxInt32越界(ErrInvalidArgument)"
		case 6:
			badGID := uint32(math.MaxInt32) + 1
			input.GID = &badGID
			caseTag = "GID超过MaxInt32越界(ErrInvalidArgument)"
		case 7:
			badSize := uint64(math.MaxInt64) + 1
			input.Size = &badSize
			caseTag = "Size超过MaxInt64越界(ErrInvalidArgument)"
		case 8:
			targetIno = 0
			input.CtimeNow = true
			caseTag = "非法Ino=0(ErrInvalidArgument)"
		default:
			targetIno = uint64(math.MaxInt64 - 654321)
			input.CtimeNow = true
			caseTag = "不存在Ino(ErrNotFound)"
		}
	}

	pred := formalPredictSetAttr(env.Model, targetIno, input)
	got, err := env.Mgr.SetAttr(ctx, targetIno, input)

	if pred.ExpectedErr != nil {
		require.Errorf(t, err, "SetAttr[%s] (ino=%d) 预期错误 %v，但实际成功: %+v",
			caseTag, targetIno, pred.ExpectedErr, got)
		require.Truef(t, errors.Is(err, pred.ExpectedErr),
			"SetAttr[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, pred.ExpectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("SetAttr[%s](ino=%d) -> 预期拒绝 (%v)", caseTag, targetIno, pred.ExpectedErr)
	}

	require.NoErrorf(t, err, "SetAttr[%s] (ino=%d) 预期成功，但返回错误: %v", caseTag, targetIno, err)
	require.NotNil(t, got)

	oldSize := pred.TargetNode.Size
	oldMode := pred.TargetNode.Mode

	applyFormalSetAttrToModel(env.Model, pred, input, env.LeaseTTL)
	assertFormalInodeMatchesEntity(t, pred.TargetNode, got, fmt.Sprintf("SetAttr[%s]返回值", caseTag))

	if input.Atime != nil {
		require.True(t, got.Attr.Atime.Equal(*input.Atime), "SetAttr 显式 Atime 未生效")
	}
	if input.Mtime != nil {
		require.True(t, got.Attr.Mtime.Equal(*input.Mtime), "SetAttr 显式 Mtime 未生效")
	}

	return fmt.Sprintf("SetAttr[%s](ino=%d, nlink=%d) -> 成功 (mode:0o%o->0o%o, size:%d->%d, uid=%d, gid=%d, flags=0x%x)",
		caseTag, targetIno, pred.TargetNode.Nlink, oldMode, pred.TargetNode.Mode,
		oldSize, pred.TargetNode.Size, pred.TargetNode.UID, pred.TargetNode.GID, pred.TargetNode.Flags)
}

// formalActionFallocate 在状态机中随机生成三种合法模式 (0、FallocFlKeepSize、FallocFlPunchHole|FallocFlKeepSize)、
// 常规文件与孤儿常规文件 (nlink==0) 预分配扩容、超配额 ENOSPC 拦截、打洞剥离 S_ISUID/S_ISGID 特权位、
// 以及非法模式/整型溢出/只读句柄/目录/特殊节点拦截请求并与参考模型对拍。
func formalActionFallocate(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	regFiles := env.Model.ActiveRegFiles()
	activeFileHandles := env.Model.ActiveFileHandles()

	var writableRegHandles []*formalHandle
	var readOnlyRegHandles []*formalHandle
	var writableOrphanRegHandles []*formalHandle

	for _, h := range activeFileHandles {
		inode, ok := env.Model.Inodes[h.Ino]
		if !ok || inode.Deleted || !inode.IsReg() {
			continue
		}
		acc := h.Flags & meta.O_ACCMODE
		if acc == meta.O_WRONLY || acc == meta.O_RDWR {
			writableRegHandles = append(writableRegHandles, h)
			if inode.Nlink == 0 {
				writableOrphanRegHandles = append(writableOrphanRegHandles, h)
			}
		} else {
			readOnlyRegHandles = append(readOnlyRegHandles, h)
		}
	}

	// 若当前尚无常规文件可写句柄，且存在存活常规文件，按 60% 概率先打开一个 O_RDWR 写句柄以提升 Fallocate 有效碰撞密度
	if len(writableRegHandles) == 0 && len(regFiles) > 0 && env.Rng.IntN(100) < 60 {
		target := regFiles[env.Rng.IntN(len(regFiles))]
		if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
			openRes, openErr := env.Mgr.Open(ctx, meta.OpenParams{
				Ino:   target.Ino,
				Flags: meta.O_RDWR,
			})
			require.NoError(t, openErr, "Fallocate 前置打开常规文件可写句柄不应失败")
			newH := &formalHandle{
				Fh:        openRes.Fh,
				Ino:       target.Ino,
				Flags:     meta.O_RDWR,
				SessionID: nil,
				IsDir:     false,
				Closed:    false,
			}
			env.Model.RecordNewHandle(newH)
			writableRegHandles = append(writableRegHandles, newH)
			if target.Nlink == 0 {
				writableOrphanRegHandles = append(writableOrphanRegHandles, newH)
			}
		}
	}

	var params meta.FallocateParams
	var caseTag string
	branch := env.Rng.IntN(100)

	switch {
	case branch < 15 && len(writableOrphanRegHandles) > 0:
		// 重点场景 1：对 nlink == 0 的活跃孤儿常规文件执行 Fallocate(mode=0) 扩容或打洞
		h := writableOrphanRegHandles[env.Rng.IntN(len(writableOrphanRegHandles))]
		inode := env.Model.Inodes[h.Ino]
		params.Ino = h.Ino
		params.Fh = h.Fh
		params.SessionID = modelCloneSessionID(h.SessionID)
		disk := env.Model.DiskByRootIno[inode.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		if avail >= 512 && env.Rng.IntN(3) != 0 {
			delta := uint64(env.Rng.IntN(2048) + 256)
			if delta > avail {
				delta = avail
			}
			params.Offset = inode.Size
			params.Length = delta
			params.Mode = 0
			caseTag = "活跃孤儿文件(nlink=0)Fallocate(mode=0)扩容"
		} else {
			params.Offset = 0
			params.Length = 512
			params.Mode = meta.FallocFlPunchHole | meta.FallocFlKeepSize
			caseTag = "活跃孤儿文件(nlink=0)Fallocate打洞"
		}

	case branch < 38 && len(writableRegHandles) > 0:
		// 重点场景 2：Mode == 0 默认预分配（覆盖 Offset+Length > Size 扩容 与 Offset+Length <= Size 大小不变）
		h := writableRegHandles[env.Rng.IntN(len(writableRegHandles))]
		inode := env.Model.Inodes[h.Ino]
		params.Ino = h.Ino
		params.Fh = h.Fh
		params.SessionID = modelCloneSessionID(h.SessionID)
		params.Mode = 0

		disk := env.Model.DiskByRootIno[inode.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}

		if inode.Size >= 64 && env.Rng.IntN(3) == 0 {
			// 在现有文件大小范围内预分配：不增加 Size 和 UsedBytes
			params.Offset = 0
			params.Length = inode.Size / 2
			if params.Length == 0 {
				params.Length = 1
			}
			caseTag = "Fallocate(mode=0)范围<=Size不改变大小"
		} else if avail >= 256 {
			maxExtend := avail
			if maxExtend > 8192 {
				maxExtend = 8192
			}
			extend := uint64(env.Rng.Uint64N(maxExtend)) + 1
			if inode.Size > 128 && env.Rng.IntN(2) == 0 {
				params.Offset = inode.Size - 64
				params.Length = 64 + extend
			} else {
				params.Offset = inode.Size
				params.Length = extend
			}
			caseTag = "Fallocate(mode=0)超出原Size合法扩容"
		} else {
			params.Offset = 0
			params.Length = 1
			if inode.Size == 0 {
				caseTag = "Fallocate(mode=0)配额已满尝试扩容1字节(ErrNoSpace)"
			} else {
				caseTag = "Fallocate(mode=0)范围<=Size保持不变"
			}
		}

	case branch < 50 && len(writableRegHandles) > 0:
		// 重点场景 3：Mode == FallocFlKeepSize（即使 Offset+Length > Size 也不修改 Size 与 UsedBytes）
		h := writableRegHandles[env.Rng.IntN(len(writableRegHandles))]
		inode := env.Model.Inodes[h.Ino]
		params.Ino = h.Ino
		params.Fh = h.Fh
		params.SessionID = modelCloneSessionID(h.SessionID)
		params.Mode = meta.FallocFlKeepSize
		params.Offset = inode.Size + uint64(env.Rng.IntN(4096))
		params.Length = uint64(env.Rng.IntN(8192) + 512)
		caseTag = "Fallocate(FallocFlKeepSize)保持Size与UsedBytes不变"

	case branch < 62 && len(writableRegHandles) > 0:
		// 重点场景 4：Mode == FallocFlPunchHole | FallocFlKeepSize（打洞：不修改 Size 与 UsedBytes，但剥离 S_ISUID | S_ISGID）
		h := writableRegHandles[env.Rng.IntN(len(writableRegHandles))]
		inode := env.Model.Inodes[h.Ino]
		// 若该文件尚未带特权位，先按概率赋予 S_ISUID | S_ISGID 以高频验证打洞剥离特权位
		if (inode.Mode&(meta.S_ISUID|meta.S_ISGID)) == 0 && env.Rng.IntN(2) == 0 {
			privMode := inode.Mode | meta.S_ISUID | meta.S_ISGID
			_, err := env.Mgr.SetAttr(ctx, inode.Ino, meta.SetAttrInput{Mode: &privMode})
			require.NoError(t, err)
			inode.Mode = privMode
		}
		params.Ino = h.Ino
		params.Fh = h.Fh
		params.SessionID = modelCloneSessionID(h.SessionID)
		params.Mode = meta.FallocFlPunchHole | meta.FallocFlKeepSize
		params.Offset = uint64(env.Rng.IntN(1024))
		params.Length = uint64(env.Rng.IntN(4096) + 128)
		caseTag = "Fallocate(FallocFlPunchHole|FallocFlKeepSize)打洞剥离特权位且保持Size不变"

	case branch < 70 && len(writableRegHandles) > 0:
		// 重点场景 5：Mode == 0 超出磁盘配额上限触发 ErrNoSpace (ENOSPC)
		h := writableRegHandles[env.Rng.IntN(len(writableRegHandles))]
		inode := env.Model.Inodes[h.Ino]
		disk := env.Model.DiskByRootIno[inode.RootIno]
		avail := uint64(0)
		if disk != nil && disk.MaxBytes > disk.UsedBytes {
			avail = disk.MaxBytes - disk.UsedBytes
		}
		params.Ino = h.Ino
		params.Fh = h.Fh
		params.SessionID = modelCloneSessionID(h.SessionID)
		params.Mode = 0
		params.Offset = inode.Size
		params.Length = avail + uint64(env.Rng.IntN(4096)+1)
		caseTag = "Fallocate(mode=0)超出剩余配额拦截(ErrNoSpace)"

	case branch < 78:
		// 重点场景 6：非法 Mode 拦截（单独 FallocFlPunchHole -> ErrInvalidArgument；Collapse/Zero/Insert/未知位 -> ErrNotSupported）
		params.Ino = activeDirs[0].Ino
		params.Fh = 1
		if len(writableRegHandles) > 0 {
			h := writableRegHandles[0]
			params.Ino = h.Ino
			params.Fh = h.Fh
			params.SessionID = modelCloneSessionID(h.SessionID)
		}
		params.Offset = 0
		params.Length = 1024
		switch env.Rng.IntN(5) {
		case 0:
			params.Mode = meta.FallocFlPunchHole
			caseTag = "单独FallocFlPunchHole未带KeepSize(ErrInvalidArgument)"
		case 1:
			params.Mode = meta.FallocFlCollapseRange
			caseTag = "不支持模式FallocFlCollapseRange(ErrNotSupported)"
		case 2:
			params.Mode = meta.FallocFlZeroRange
			caseTag = "不支持模式FallocFlZeroRange(ErrNotSupported)"
		case 3:
			params.Mode = meta.FallocFlInsertRange
			caseTag = "不支持模式FallocFlInsertRange(ErrNotSupported)"
		default:
			params.Mode = 0x80
			caseTag = "未知Mode标志位0x80(ErrNotSupported)"
		}

	case branch < 86:
		// 重点场景 7：句柄或节点类型错误（只读句柄 O_RDONLY -> ErrBadFileHandle；目录 -> ErrIsDir；特殊节点/符号链接 -> ErrNotSupported）
		params.Offset = 0
		params.Length = 512
		params.Mode = 0
		if len(readOnlyRegHandles) > 0 && env.Rng.IntN(2) == 0 {
			roH := readOnlyRegHandles[env.Rng.IntN(len(readOnlyRegHandles))]
			params.Ino = roH.Ino
			params.Fh = roH.Fh
			params.SessionID = modelCloneSessionID(roH.SessionID)
			caseTag = "只读句柄O_RDONLY调用Fallocate拦截(ErrBadFileHandle)"
		} else if env.Rng.IntN(2) == 0 {
			dir := activeDirs[env.Rng.IntN(len(activeDirs))]
			params.Ino = dir.Ino
			params.Fh = 1
			caseTag = "目录节点调用Fallocate拦截(ErrIsDir)"
		} else {
			// 查找非目录且非常规文件节点（如 S_IFLNK / S_IFIFO / S_IFCHR 等）
			var nonRegNode *formalInode
			for _, n := range env.Model.ActiveNonDirs() {
				if !n.IsReg() {
					nonRegNode = n
					break
				}
			}
			if nonRegNode != nil {
				params.Ino = nonRegNode.Ino
				params.Fh = 1
				caseTag = "非常规文件调用Fallocate拦截(ErrNotSupported)"
			} else {
				params.Ino = activeDirs[0].Ino
				params.Fh = 1
				caseTag = "目录节点调用Fallocate拦截(ErrIsDir)"
			}
		}

	case branch < 93:
		// 重点场景 8：不存在/已关闭/跨节点句柄或不存在 Ino
		params.Offset = 0
		params.Length = 256
		params.Mode = 0
		if len(regFiles) > 0 && env.Rng.IntN(2) == 0 {
			params.Ino = regFiles[env.Rng.IntN(len(regFiles))].Ino
			params.Fh = uint64(math.MaxInt64 - 333111)
			caseTag = "不存在Fh调用Fallocate拦截(ErrBadFileHandle)"
		} else {
			params.Ino = uint64(math.MaxInt64 - 888999)
			params.Fh = 1
			caseTag = "不存在Ino调用Fallocate拦截(ErrNotFound)"
		}

	default:
		// 重点场景 9：静态边界与整型溢出防御（Length=0、Offset>MaxInt64、Offset+Length>MaxInt64、Ino=0、Fh=0、SessionID=0）
		params.Ino = activeDirs[0].Ino
		params.Fh = 1
		params.Offset = 0
		params.Length = 1024
		params.Mode = 0
		switch env.Rng.IntN(6) {
		case 0:
			params.Length = 0
			caseTag = "非法Length=0(ErrInvalidArgument)"
		case 1:
			params.Offset = uint64(math.MaxInt64) - 100
			params.Length = 200
			caseTag = "Offset+Length超MaxInt64溢出(ErrInvalidArgument)"
		case 2:
			params.Offset = uint64(math.MaxInt64) + 1
			params.Length = 1
			caseTag = "非法Offset超MaxInt64(ErrInvalidArgument)"
		case 3:
			params.Ino = 0
			caseTag = "非法Ino=0(ErrInvalidArgument)"
		case 4:
			params.Fh = 0
			caseTag = "非法Fh=0(ErrInvalidArgument)"
		default:
			zeroSID := uint64(0)
			params.SessionID = &zeroSID
			caseTag = "非法SessionID=0(ErrInvalidArgument)"
		}
	}

	pred := formalPredictFallocate(env.Model, params)
	err := env.Mgr.Fallocate(ctx, params)

	if pred.ExpectedErr != nil {
		require.Errorf(t, err, "Fallocate[%s] (ino=%d, fh=%d, offset=%d, len=%d, mode=0x%x) 预期错误 %v，但实际成功",
			caseTag, params.Ino, params.Fh, params.Offset, params.Length, params.Mode, pred.ExpectedErr)
		require.Truef(t, errors.Is(err, pred.ExpectedErr),
			"Fallocate[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, pred.ExpectedErr, err)
		return fmt.Sprintf("Fallocate[%s](ino=%d, fh=%d, off=%d, len=%d, mode=0x%x) -> 预期拒绝 (%v)",
			caseTag, params.Ino, params.Fh, params.Offset, params.Length, params.Mode, pred.ExpectedErr)
	}

	require.NoErrorf(t, err, "Fallocate[%s] (ino=%d, fh=%d, off=%d, len=%d, mode=0x%x) 预期成功，但返回错误: %v",
		caseTag, params.Ino, params.Fh, params.Offset, params.Length, params.Mode, err)
	require.NotNil(t, pred.TargetNode)

	oldSize := pred.TargetNode.Size
	oldMode := pred.TargetNode.Mode

	applyFormalFallocateToModel(env.Model, pred, params, env.LeaseTTL)

	return fmt.Sprintf("Fallocate[%s](ino=%d, fh=%d, nlink=%d, off=%d, len=%d, mode=0x%x) -> 成功 (size:%d->%d, mode:0o%o->0o%o)",
		caseTag, params.Ino, params.Fh, pred.TargetNode.Nlink, params.Offset, params.Length, params.Mode,
		oldSize, pred.TargetNode.Size, oldMode, pred.TargetNode.Mode)
}

// ============================================================================
// 3. T9 专属测试入口：确定性全边界矩阵、ENOSPC 原子回滚、孤儿扩缩容回收与随机状态机对拍
// ============================================================================

// TestFormalMain_T9_SetAttrFallocate 验证 T9 主状态机 SetAttr/Fallocate 文件大小扩缩容、
// 特权位剥离、打洞预分配、活跃孤儿常规文件扩缩容延迟回收与配额上限拦截对拍。
func TestFormalMain_T9_SetAttrFallocate(t *testing.T) {
	t.Run("确定性全边界矩阵_SetAttr掩码部分更新_特权位剥离_Fh写权限校验_Fallocate三种合法模式与非法模式拦截", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 30,
			Seed:       0x20260426,
			Rng:        newFormalRNG(0x20260426),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t9-det-matrix",
			LeaseTTL:   meta.DefaultSessionLeaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		runProbesAndAuditors := func() {
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

		// 1. 创建容量为 64KB (65536 字节) 的虚拟磁盘
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     env.UniqueDiskName("t9-matrix"),
			MaxBytes: 65536,
		})
		require.NoError(t, err)
		modelDisk := env.Model.RecordNewDisk(disk)
		rootIno := disk.RootIno
		rootDir := env.Model.Inodes[rootIno]

		// 2. 创建子目录 sub_dir（带 S_ISGID | 0o775）、常规文件 reg_file、符号链接 sym_link、命名管道 fifo_node
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sub_dir"),
			Mode:      meta.S_ISGID | 0o775,
			UID:       100,
			GID:       200,
		})
		require.NoError(t, err)
		subDirModel := &formalInode{
			Ino: subDir.Ino, Generation: subDir.Generation, RootIno: rootIno, ParentIno: rootIno,
			Mode: subDir.Attr.Mode, UID: 100, GID: 200, Size: meta.DefaultDirSize, Nlink: 2,
		}
		env.Model.RecordNewInode(subDirModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sub_dir"}] = subDir.Ino
		rootDir.Nlink++
		modelDisk.UsedInodes++

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("reg_file"),
			Mode:      meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | meta.S_ISVTX | 0o755,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		regModel := &formalInode{
			Ino: createRes.Inode.Ino, Generation: createRes.Inode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: createRes.Inode.Attr.Mode, UID: 1000, GID: 1000, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(regModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "reg_file"}] = regModel.Ino
		modelDisk.UsedInodes++
		rwFh := createRes.Fh
		env.Model.RecordNewHandle(&formalHandle{
			Fh: rwFh, Ino: regModel.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
		})

		// 同时为 reg_file 打开一个只读句柄 roFh (绑定 SessionID = 5001)
		sid5001 := uint64(5001)
		roOpenRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       regModel.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sid5001,
		})
		require.NoError(t, err)
		modelSyncSessionRenewal(env.Model, &sid5001, env.LeaseTTL)
		roFh := roOpenRes.Fh
		env.Model.RecordNewHandle(&formalHandle{
			Fh: roFh, Ino: regModel.Ino, Flags: meta.O_RDONLY, SessionID: modelCloneSessionID(&sid5001),
		})

		symNode, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno, Name: []byte("sym_link"), Target: []byte("/etc/hosts"),
		})
		require.NoError(t, err)
		symModel := &formalInode{
			Ino: symNode.Ino, Generation: symNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: symNode.Attr.Mode, Size: uint64(len("/etc/hosts")), Nlink: 1, SymlinkTarget: []byte("/etc/hosts"),
		}
		env.Model.RecordNewInode(symModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sym_link"}] = symModel.Ino
		modelDisk.UsedInodes++

		fifoNode, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: rootIno, Name: []byte("fifo_node"), Mode: meta.S_IFIFO | 0o640,
		})
		require.NoError(t, err)
		fifoModel := &formalInode{
			Ino: fifoNode.Ino, Generation: fifoNode.Generation, RootIno: rootIno, ParentIno: 0,
			Mode: fifoNode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(fifoModel)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "fifo_node"}] = fifoModel.Ino
		modelDisk.UsedInodes++
		runProbesAndAuditors()

		// 3. 验证 SetAttr 特权位 (S_ISUID / S_ISGID) 剥离与保留矩阵
		// 3.1 当前 reg_file Mode 为 S_IFREG | S_ISUID | S_ISGID | S_ISVTX | 0o755；
		// 不传 Mode 仅修改 UID -> 必须自动剥离 S_ISUID | S_ISGID，保留 S_ISVTX | 0o755
		newUID := uint32(1002)
		updated, err := mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{UID: &newUID})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFREG|meta.S_ISVTX|0o755), updated.Attr.Mode, "常规文件修改 UID 必须剥离 S_ISUID|S_ISGID 并保留 S_ISVTX")
		require.Equal(t, uint32(1002), updated.Attr.UID)
		regModel.Mode = updated.Attr.Mode
		regModel.UID = 1002

		// 3.2 重新恢复 S_ISUID | S_ISGID，随后不传 Mode 仅修改 GID -> 必须自动剥离 S_ISUID | S_ISGID
		restorePriv := uint32(meta.S_ISUID | meta.S_ISGID | 0o755)
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mode: &restorePriv})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), updated.Attr.Mode)
		regModel.Mode = updated.Attr.Mode

		newGID := uint32(2002)
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{GID: &newGID})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFREG|0o755), updated.Attr.Mode, "常规文件修改 GID 必须剥离 S_ISUID|S_ISGID")
		regModel.Mode = updated.Attr.Mode
		regModel.GID = 2002

		// 3.3 重新恢复 S_ISUID | S_ISGID，随后不传 Mode 修改 Size (扩容至 4096) -> 必须自动剥离 S_ISUID | S_ISGID 并增加 UsedBytes
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mode: &restorePriv})
		require.NoError(t, err)
		regModel.Mode = updated.Attr.Mode

		size4096 := uint64(4096)
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Size: &size4096, Fh: &rwFh})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFREG|0o755), updated.Attr.Mode, "常规文件修改 Size 必须剥离 S_ISUID|S_ISGID")
		require.Equal(t, uint64(4096), updated.Attr.Size)
		regModel.Mode = updated.Attr.Mode
		regModel.Size = 4096
		modelDisk.UsedBytes = 4096

		// 3.4 同时传入 Mode (含 S_ISUID|S_ISGID) 与 Size (缩容至 2048) -> 显式传入的 Mode 优先，保留 S_ISUID|S_ISGID
		size2048 := uint64(2048)
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mode: &restorePriv, Size: &size2048})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), updated.Attr.Mode)
		require.Equal(t, uint64(2048), updated.Attr.Size)
		regModel.Mode = updated.Attr.Mode
		regModel.Size = 2048
		modelDisk.UsedBytes = 2048

		// 3.5 对带 S_ISGID 的目录 sub_dir 修改 UID 与 GID -> 保留目录的 S_ISGID 位
		dirUID := uint32(300)
		dirGID := uint32(400)
		updatedDir, err := mgr.SetAttr(ctx, subDirModel.Ino, meta.SetAttrInput{UID: &dirUID, GID: &dirGID})
		require.NoError(t, err)
		require.Equal(t, uint32(meta.S_IFDIR|meta.S_ISGID|0o775), updatedDir.Attr.Mode, "目录修改 UID/GID 必须保留 S_ISGID")
		subDirModel.UID = 300
		subDirModel.GID = 400

		// 3.6 验证 Atime、Mtime、Flags 与仅 CtimeNow 幂等返回当前属性
		customAtime := time.Unix(1700000100, 0).UTC()
		customMtime := time.Unix(1700000200, 0).UTC()
		customFlags := meta.FlagImmutable | meta.FlagAppend
		updated, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{
			Atime: &customAtime,
			Mtime: &customMtime,
			Flags: &customFlags,
		})
		require.NoError(t, err)
		require.True(t, updated.Attr.Atime.Equal(customAtime))
		require.True(t, updated.Attr.Mtime.Equal(customMtime))
		require.Equal(t, customFlags, updated.Attr.Flags)
		regModel.Flags = customFlags

		// 仅设置 CtimeNow: true 幂等返回当前属性并刷新 ctime 为当前事务时间
		beforeCallTime := time.Now().UTC().Add(-100 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		beforeCtime := updated.Attr.Ctime
		updatedIdem, err := mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{CtimeNow: true})
		require.NoError(t, err)
		assertFormalInodeMatchesEntity(t, regModel, updatedIdem, "仅CtimeNow幂等更新")
		require.True(t, updatedIdem.Attr.Ctime.After(beforeCtime), "CtimeNow 必须严格推进 ctime (before=%v, after=%v)", beforeCtime, updatedIdem.Attr.Ctime)
		require.True(t, updatedIdem.Attr.Ctime.After(beforeCallTime), "CtimeNow 必须刷新为当前时间")
		var dbCtime time.Time
		err = pool.QueryRow(ctx, `SELECT ctime FROM inodes WHERE ino = $1`, int64(regModel.Ino)).Scan(&dbCtime)
		require.NoError(t, err)
		require.True(t, updatedIdem.Attr.Ctime.Equal(dbCtime), "返回的 Ctime 必须与 DB 中的 ctime 一致")

		// 补充 AtimeNow: true 与 MtimeNow: true 有效刷新时间戳断言（从过去时间 1700000100/1700000200 刷新到当前时间）
		beforeAtime := updatedIdem.Attr.Atime
		beforeMtime := updatedIdem.Attr.Mtime
		updatedNow, err := mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{AtimeNow: true, MtimeNow: true})
		require.NoError(t, err)
		require.True(t, updatedNow.Attr.Atime.After(beforeAtime), "AtimeNow 必须从历史时间严格推进至当前时间 (before=%v, after=%v)", beforeAtime, updatedNow.Attr.Atime)
		require.True(t, updatedNow.Attr.Mtime.After(beforeMtime), "MtimeNow 必须从历史时间严格推进至当前时间 (before=%v, after=%v)", beforeMtime, updatedNow.Attr.Mtime)
		require.True(t, updatedNow.Attr.Atime.After(beforeCallTime), "AtimeNow 必须刷新为当前时间")
		require.True(t, updatedNow.Attr.Mtime.After(beforeCallTime), "MtimeNow 必须刷新为当前时间")
		var dbAtime, dbMtime time.Time
		err = pool.QueryRow(ctx, `SELECT atime, mtime FROM inodes WHERE ino = $1`, int64(regModel.Ino)).Scan(&dbAtime, &dbMtime)
		require.NoError(t, err)
		require.True(t, updatedNow.Attr.Atime.Equal(dbAtime), "返回的 Atime 必须与 DB 一致")
		require.True(t, updatedNow.Attr.Mtime.Equal(dbMtime), "返回的 Mtime 必须与 DB 一致")

		runProbesAndAuditors()

		// 4. 验证 SetAttr 非法掩码、类型限制与 Fh 校验拦截
		// 全空 SetAttrInput -> ErrInvalidArgument
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// Atime 与 AtimeNow 同时置位 / Mtime 与 MtimeNow 同时置位 -> ErrInvalidArgument
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Atime: &customAtime, AtimeNow: true})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mtime: &customMtime, MtimeNow: true})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// Mode 越界或类型位冲突 -> ErrInvalidArgument
		badModeOverflow := uint32(0o200000)
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mode: &badModeOverflow})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		badModeDirOnReg := uint32(meta.S_IFDIR | 0o755)
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Mode: &badModeDirOnReg})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 对符号链接设置 Mode -> ErrInvalidArgument
		symMode := uint32(0o755)
		_, err = mgr.SetAttr(ctx, symModel.Ino, meta.SetAttrInput{Mode: &symMode})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 对目录设置 Size -> ErrIsDir；对符号链接或 FIFO 设置 Size -> ErrInvalidArgument
		_, err = mgr.SetAttr(ctx, subDirModel.Ino, meta.SetAttrInput{Size: &size4096})
		require.ErrorIs(t, err, meta.ErrIsDir)
		_, err = mgr.SetAttr(ctx, symModel.Ino, meta.SetAttrInput{Size: &size4096})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = mgr.SetAttr(ctx, fifoModel.Ino, meta.SetAttrInput{Size: &size4096})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 只读句柄 roFh 修改 Size -> ErrBadFileHandle；SessionID 不匹配 -> ErrBadFileHandle
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{Size: &size4096, Fh: &roFh, SessionID: &sid5001})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "只读句柄修改 Size 必须返回 ErrBadFileHandle")
		_, err = mgr.SetAttr(ctx, regModel.Ino, meta.SetAttrInput{CtimeNow: true, Fh: &roFh, SessionID: nil})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "句柄 SessionID 不匹配必须返回 ErrBadFileHandle")

		// 5. 验证 Fallocate 三种合法模式与非法模式/参数边界
		// 5.1 Mode == 0 且 Offset(2048) + Length(6144) = 8192 > 当前 Size(2048)：
		// 预期：Size 扩容至 8192，UsedBytes 从 2048 增至 8192，且保留 S_ISUID|S_ISGID 特权位！
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    regModel.Ino,
			Fh:     rwFh,
			Offset: 2048,
			Length: 6144,
			Mode:   0,
		})
		require.NoError(t, err)
		regModel.Size = 8192
		modelDisk.UsedBytes = 8192
		runProbesAndAuditors()

		// 5.2 Mode == 0 且 Offset(1024) + Length(2048) = 3072 <= 当前 Size(8192)：
		// 预期：Size 与 UsedBytes 保持 8192 不变
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    regModel.Ino,
			Fh:     rwFh,
			Offset: 1024,
			Length: 2048,
			Mode:   0,
		})
		require.NoError(t, err)
		runProbesAndAuditors()

		// 5.3 Mode == FallocFlKeepSize 且 Offset(8192) + Length(16384) = 24576 > 当前 Size(8192)：
		// 预期：不修改 Size (仍为 8192)，不增加 UsedBytes (仍为 8192)，不剥离特权位
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    regModel.Ino,
			Fh:     rwFh,
			Offset: 8192,
			Length: 16384,
			Mode:   meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		runProbesAndAuditors()

		// 5.4 Mode == FallocFlPunchHole | FallocFlKeepSize（打洞）：
		// 预期：Size 与 UsedBytes 保持 8192 不变，但剥离常规文件的 S_ISUID | S_ISGID 特权位！
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    regModel.Ino,
			Fh:     rwFh,
			Offset: 1024,
			Length: 4096,
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		regModel.Mode &^= (meta.S_ISUID | meta.S_ISGID)
		runProbesAndAuditors()

		// 5.5 验证 Fallocate 错误码拦截矩阵
		// 单独 FallocFlPunchHole -> ErrInvalidArgument
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: regModel.Ino, Fh: rwFh, Offset: 0, Length: 1024, Mode: meta.FallocFlPunchHole,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 其他不支持模式 -> ErrNotSupported
		for _, unsupMode := range []uint32{
			meta.FallocFlCollapseRange,
			meta.FallocFlZeroRange,
			meta.FallocFlInsertRange,
			0x40,
		} {
			err = mgr.Fallocate(ctx, meta.FallocateParams{
				Ino: regModel.Ino, Fh: rwFh, Offset: 0, Length: 1024, Mode: unsupMode,
			})
			require.ErrorIs(t, err, meta.ErrNotSupported)
		}

		// Length == 0 或 Offset + Length 溢出 -> ErrInvalidArgument
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: regModel.Ino, Fh: rwFh, Offset: 0, Length: 0, Mode: 0,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: regModel.Ino, Fh: rwFh, Offset: uint64(math.MaxInt64) - 10, Length: 20, Mode: 0,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		// 只读句柄 roFh 调用 Fallocate -> ErrBadFileHandle
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: regModel.Ino, Fh: roFh, Offset: 0, Length: 1024, Mode: 0, SessionID: &sid5001,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// 目录调用 Fallocate -> ErrIsDir；非常规文件调用 Fallocate -> ErrNotSupported
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: subDirModel.Ino, Fh: rwFh, Offset: 0, Length: 1024, Mode: 0,
		})
		require.ErrorIs(t, err, meta.ErrIsDir)
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino: fifoModel.Ino, Fh: rwFh, Offset: 0, Length: 1024, Mode: 0,
		})
		require.ErrorIs(t, err, meta.ErrNotSupported)
		runProbesAndAuditors()
	})

	t.Run("确定性验证_超配额ENOSPC原子回滚_SetAttr同事务属性与Fallocate扩容回滚", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 10,
			Seed:       0x20260427,
			Rng:        newFormalRNG(0x20260427),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t9-enospc",
			LeaseTTL:   meta.DefaultSessionLeaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		// 1. 创建容量上限 MaxBytes = 10000 字节的磁盘
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     env.UniqueDiskName("quota-10k"),
			MaxBytes: 10000,
		})
		require.NoError(t, err)
		modelDisk := env.Model.RecordNewDisk(disk)
		rootIno := disk.RootIno

		// 2. 创建文件 f1 并通过 SetAttr 扩容至 6000 字节（剩余可用配额 = 4000 字节）
		cRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("f1"),
			Mode:      meta.S_IFREG | meta.S_ISUID | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			UID:       100,
			GID:       200,
		})
		require.NoError(t, err)
		f1Model := &formalInode{
			Ino: cRes.Inode.Ino, Generation: cRes.Inode.Generation, RootIno: rootIno,
			Mode: cRes.Inode.Attr.Mode, UID: 100, GID: 200, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(f1Model)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "f1"}] = f1Model.Ino
		modelDisk.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: cRes.Fh, Ino: f1Model.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
		})

		size6000 := uint64(6000)
		keepMode := f1Model.Mode
		_, err = mgr.SetAttr(ctx, f1Model.Ino, meta.SetAttrInput{Size: &size6000, Mode: &keepMode})
		require.NoError(t, err)
		f1Model.Size = 6000
		modelDisk.UsedBytes = 6000
		formalAuditINV4DiskQuota(ctx, t, env)

		// 3. 发起 SetAttr 将 Size 设为 10001（超限 1 字节），同时请求修改 Mode=0o777, UID=999, GID=888, Flags=FlagImmutable：
		// 预期返回 ErrNoSpace，且整个事务回滚，f1 的 Mode、UID、GID、Flags、Size 以及磁盘 UsedBytes 保持 6000 丝毫不变！
		size10001 := uint64(10001)
		newMode := uint32(0o777)
		newUID := uint32(999)
		newGID := uint32(888)
		newFlags := meta.FlagImmutable
		_, err = mgr.SetAttr(ctx, f1Model.Ino, meta.SetAttrInput{
			Size:  &size10001,
			Mode:  &newMode,
			UID:   &newUID,
			GID:   &newGID,
			Flags: &newFlags,
		})
		require.ErrorIs(t, err, meta.ErrNoSpace, "超出磁盘配额上限 1 字节必须返回 ErrNoSpace")
		formalProbeGetAttr(ctx, t, env)
		formalProbeGetDisk(ctx, t, env)
		formalProbeStatFS(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 4. 发起 Fallocate(mode=0) 请求 Offset=6000, Length=4001（目标 10001 字节，超限 1 字节）：
		// 预期返回 ErrNoSpace，且文件大小与磁盘已用配额保持 6000 不变
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    f1Model.Ino,
			Fh:     cRes.Fh,
			Offset: 6000,
			Length: 4001,
			Mode:   0,
		})
		require.ErrorIs(t, err, meta.ErrNoSpace, "Fallocate(mode=0) 超出配额上限必须返回 ErrNoSpace")
		formalProbeGetAttr(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 5. 发起 Fallocate(mode=0) 恰好用满剩余 4000 字节配额（Offset=6000, Length=4000 -> Size=10000）：必须成功！
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    f1Model.Ino,
			Fh:     cRes.Fh,
			Offset: 6000,
			Length: 4000,
			Mode:   0,
		})
		require.NoError(t, err)
		f1Model.Size = 10000
		modelDisk.UsedBytes = 10000
		formalProbeGetAttr(ctx, t, env)
		formalProbeStatFS(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)
	})

	t.Run("确定性验证_活跃孤儿常规文件nlink=0扩缩容与Release延迟回收配额闭环", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 20,
			Seed:       0x20260428,
			Rng:        newFormalRNG(0x20260428),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t9-orphan-quota",
			LeaseTTL:   meta.DefaultSessionLeaseTTL,
		}
		t.Cleanup(func() {
			env.cleanupDatabase()
		})

		ctx := context.Background()

		// 1. 创建容量 65536 字节的磁盘
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     env.UniqueDiskName("orphan-alloc"),
			MaxBytes: 65536,
		})
		require.NoError(t, err)
		modelDisk := env.Model.RecordNewDisk(disk)
		rootIno := disk.RootIno

		// 2. 场景 A：具名常规文件打开后 Unlink 成为 nlink == 0 孤儿 -> SetAttr 扩容 -> Fallocate(mode=0) 扩容 -> SetAttr 缩容 -> Release 精确退还最新配额
		cRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: rootIno,
			Name:      []byte("unlinked_open_file"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.NoError(t, err)
		orphanA := &formalInode{
			Ino: cRes.Inode.Ino, Generation: cRes.Inode.Generation, RootIno: rootIno,
			Mode: cRes.Inode.Attr.Mode, Size: 0, Nlink: 1,
		}
		env.Model.RecordNewInode(orphanA)
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "unlinked_open_file"}] = orphanA.Ino
		modelDisk.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: cRes.Fh, Ino: orphanA.Ino, Flags: meta.O_CREAT | meta.O_RDWR,
		})

		// 初始写入 2000 字节
		sz2000 := uint64(2000)
		_, err = mgr.SetAttr(ctx, orphanA.Ino, meta.SetAttrInput{Size: &sz2000, Fh: &cRes.Fh})
		require.NoError(t, err)
		orphanA.Size = 2000
		modelDisk.UsedBytes = 2000

		// Unlink 使其变为 nlink == 0 的活跃孤儿文件（此时句柄未关，2000 字节配额仍占用）
		err = mgr.Unlink(ctx, rootIno, []byte("unlinked_open_file"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: rootIno, Name: "unlinked_open_file"})
		orphanA.Nlink = 0
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 在 nlink == 0 孤儿状态下通过 SetAttr 扩容至 5000 字节
		sz5000 := uint64(5000)
		_, err = mgr.SetAttr(ctx, orphanA.Ino, meta.SetAttrInput{Size: &sz5000, Fh: &cRes.Fh})
		require.NoError(t, err)
		orphanA.Size = 5000
		modelDisk.UsedBytes = 5000
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 在 nlink == 0 孤儿状态下通过 Fallocate(mode=0) 进一步扩容至 9000 字节
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    orphanA.Ino,
			Fh:     cRes.Fh,
			Offset: 5000,
			Length: 4000,
			Mode:   0,
		})
		require.NoError(t, err)
		orphanA.Size = 9000
		modelDisk.UsedBytes = 9000
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 在 nlink == 0 孤儿状态下通过 SetAttr 缩容至 3500 字节
		sz3500 := uint64(3500)
		_, err = mgr.SetAttr(ctx, orphanA.Ino, meta.SetAttrInput{Size: &sz3500, Fh: &cRes.Fh})
		require.NoError(t, err)
		orphanA.Size = 3500
		modelDisk.UsedBytes = 3500
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 3. 场景 B：TmpFile 创建未具名化的 nlink == 0 临时文件 -> Fallocate(mode=0) 扩容至 4500 字节
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: rootIno,
			Mode:      meta.S_IFREG | 0o600,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		orphanB := &formalInode{
			Ino: tmpRes.Inode.Ino, Generation: tmpRes.Inode.Generation, RootIno: rootIno,
			Mode: tmpRes.Inode.Attr.Mode, Size: 0, Nlink: 0, IsTmpFile: true,
		}
		env.Model.RecordNewInode(orphanB)
		modelDisk.UsedInodes++
		env.Model.RecordNewHandle(&formalHandle{
			Fh: tmpRes.Fh, Ino: orphanB.Ino, Flags: meta.O_RDWR | meta.O_TMPFILE,
		})

		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    orphanB.Ino,
			Fh:     tmpRes.Fh,
			Offset: 0,
			Length: 4500,
			Mode:   0,
		})
		require.NoError(t, err)
		orphanB.Size = 4500
		modelDisk.UsedBytes += 4500 // 3500 + 4500 = 8000
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 4. 关闭 orphanA 的句柄 -> 物理删除 orphanA 并退还最新 3500 字节，剩余 UsedBytes = 4500
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: orphanA.Ino, Fh: cRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[cRes.Fh].Closed = true
		env.Model.ClosedHandles[cRes.Fh] = env.Model.Handles[cRes.Fh]
		delete(env.Model.Handles, cRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphanA.Ino))
		require.Equal(t, uint64(4500), modelDisk.UsedBytes)
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)

		// 5. 关闭 orphanB 的句柄 -> 物理删除 orphanB 并退还最新 4500 字节，磁盘 UsedBytes 完全归零，UsedInodes 回到 1 (仅剩根目录)
		err = mgr.Release(ctx, meta.ReleaseParams{Ino: orphanB.Ino, Fh: tmpRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[tmpRes.Fh].Closed = true
		env.Model.ClosedHandles[tmpRes.Fh] = env.Model.Handles[tmpRes.Fh]
		delete(env.Model.Handles, tmpRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphanB.Ino))
		require.Equal(t, uint64(0), modelDisk.UsedBytes)
		require.Equal(t, uint64(1), modelDisk.UsedInodes)
		formalProbeStatFS(ctx, t, env)
		formalAuditINV3OrphanAndHandles(ctx, t, env)
		formalAuditINV4DiskQuota(ctx, t, env)
	})

	t.Run("随机状态机_SetAttr与Fallocate文件扩缩容_打洞预分配_孤儿扩容回收与配额上限拦截对拍", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 8, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 10, Fn: formalActionUpdateDisk},
				{Name: "Mkdir", Weight: 10, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 6, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 10, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 8, Fn: formalActionSymlink},
				{Name: "Link", Weight: 12, Fn: formalActionLink},
				{Name: "Unlink", Weight: 20, Fn: formalActionUnlink},
				{Name: "Rename", Weight: 14, Fn: formalActionRename},
				{Name: "Create", Weight: 26, Fn: formalActionCreate},
				{Name: "Open", Weight: 24, Fn: formalActionOpen},
				{Name: "Release", Weight: 22, Fn: formalActionRelease},
				{Name: "SetAttr", Weight: 36, Fn: formalActionSetAttr},
				{Name: "Fallocate", Weight: 32, Fn: formalActionFallocate},
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
