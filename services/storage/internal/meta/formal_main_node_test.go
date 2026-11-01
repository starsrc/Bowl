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
	// 注册 T3 核心节点创建动作：Mknod、Symlink
	registerFormalAction("Mknod", 25, formalActionMknod)
	registerFormalAction("Symlink", 25, formalActionSymlink)

	// 注册 T3 核心只读查账探针：Readlink
	registerFormalProbe("Readlink", formalProbeReadlink)

	// 注册 T3 底表节点属性与符号链接 INV-4 配额边界审计器
	registerFormalAuditor("INV-4_NodeAndSymlinkQuota", formalAuditNodeAndSymlinkQuota)
}

// ============================================================================
// 1. T3 参考模型推导函数：Mknod 与 Symlink
// ============================================================================

// modelIsValidMknodFileType 判断 mode & S_IFMT 是否属于 Mknod 允许的 5 种文件类型。
func modelIsValidMknodFileType(modeType uint32) bool {
	switch modeType {
	case meta.S_IFREG, meta.S_IFCHR, meta.S_IFBLK, meta.S_IFIFO, meta.S_IFSOCK:
		return true
	default:
		return false
	}
}

// formalPredictMknod 根据内存参考模型预测 Mknod 的目标 Mode、GID、Rdev 与预期错误码。
func formalPredictMknod(
	m *formalMainModel,
	params meta.MknodParams,
) (targetMode uint32, targetGID uint32, targetRdev uint32, expectedErr error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return 0, 0, 0, meta.ErrInvalidArgument
	}
	if err := modelValidateEntryName(params.Name); err != nil {
		return 0, 0, 0, err
	}
	if params.Mode > 0o177777 {
		return 0, 0, 0, meta.ErrInvalidArgument
	}

	modeType := params.Mode & meta.S_IFMT
	if !modelIsValidMknodFileType(modeType) {
		return 0, 0, 0, meta.ErrInvalidArgument
	}

	parent, err := m.validateParentDirForCreate(params.ParentIno, params.Name)
	if err != nil {
		return 0, 0, 0, err
	}

	if modeType == meta.S_IFCHR || modeType == meta.S_IFBLK {
		targetRdev = params.Rdev
	}

	targetGID = params.GID
	if (parent.Mode & meta.S_ISGID) != 0 {
		targetGID = parent.GID
	}

	// 特权位 (0o7000) 与权限位 (0o777) 经 Umask 掩码计算（非目录不因父目录 S_ISGID 而自动置位 S_ISGID）
	targetMode = modeType | ((params.Mode & meta.PermMask) &^ (params.Umask & meta.PermMask))
	return targetMode, targetGID, targetRdev, nil
}

// formalPredictSymlink 根据内存参考模型预测 Symlink 的目标 Mode、GID 与预期错误码。
func formalPredictSymlink(
	m *formalMainModel,
	params meta.SymlinkParams,
) (targetMode uint32, targetGID uint32, expectedErr error) {
	if params.ParentIno == 0 || params.ParentIno > math.MaxInt64 {
		return 0, 0, meta.ErrInvalidArgument
	}
	if err := modelValidateEntryName(params.Name); err != nil {
		return 0, 0, err
	}
	if len(params.Target) == 0 || bytes.IndexByte(params.Target, 0) >= 0 {
		return 0, 0, meta.ErrInvalidArgument
	}
	if len(params.Target) > meta.MaxSymlinkTargetBytes {
		return 0, 0, meta.ErrNameTooLong
	}

	parent, err := m.validateParentDirForCreate(params.ParentIno, params.Name)
	if err != nil {
		return 0, 0, err
	}

	targetGID = params.GID
	if (parent.Mode & meta.S_ISGID) != 0 {
		targetGID = parent.GID
	}
	targetMode = meta.S_IFLNK | 0o777
	return targetMode, targetGID, nil
}

// ============================================================================
// 2. T3 动作实现：Mknod 与 Symlink 随机操作生成与逐操作对拍
// ============================================================================

// formalActionMknod 在状态机中随机生成覆盖 5 种合法文件类型、rdev 保存、umask/特权位计算、
// 父目录 S_ISGID 属组继承及各类异常边界的 Mknod 请求并与参考模型对拍。
func formalActionMknod(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	validTypes := []struct {
		TypeBit uint32
		Tag     string
		Prefix  string
	}{
		{meta.S_IFREG, "S_IFREG", "reg"},
		{meta.S_IFCHR, "S_IFCHR", "chr"},
		{meta.S_IFBLK, "S_IFBLK", "blk"},
		{meta.S_IFIFO, "S_IFIFO", "fifo"},
		{meta.S_IFSOCK, "S_IFSOCK", "sock"},
	}
	uids := []uint32{0, 1000, 1001, 2002}
	gids := []uint32{0, 1000, 2000, 3003}
	umasks := []uint32{0, 0o022, 0o027, 0o077}
	rdevs := []uint32{0, 0x0103, 0x0801, 0xfe00, 12345}
	specialBits := []uint32{0, meta.S_ISUID, meta.S_ISGID, meta.S_ISVTX, meta.S_ISUID | meta.S_ISVTX}

	var params meta.MknodParams
	params.UID = uids[env.Rng.IntN(len(uids))]
	params.GID = gids[env.Rng.IntN(len(gids))]
	params.Umask = umasks[env.Rng.IntN(len(umasks))]
	params.Rdev = rdevs[env.Rng.IntN(len(rdevs))]

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 48:
		// 正常创建节点：覆盖 5 种合法类型、rdev 保留/清零、特权位与父目录 S_ISGID 属组继承
		chosenType := validTypes[env.Rng.IntN(len(validTypes))]
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]

		// 按一定概率优先选择带 S_ISGID 的父目录以高频验证非目录属组继承
		if env.Rng.IntN(3) == 0 {
			for _, d := range activeDirs {
				if (d.Mode & meta.S_ISGID) != 0 {
					parent = d
					break
				}
			}
		}

		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName(chosenType.Prefix))
		spec := specialBits[env.Rng.IntN(len(specialBits))]
		perm := uint32(0o666)
		if chosenType.TypeBit == meta.S_IFREG && env.Rng.IntN(2) == 0 {
			perm = 0o755
		}
		params.Mode = chosenType.TypeBit | spec | perm
		// 为验证非设备类型强制置 Rdev=0，给所有类型高概率传入非零 Rdev
		if params.Rdev == 0 {
			params.Rdev = 0x0801
		}
		caseTag = fmt.Sprintf("正常创建(%s,spec=0o%o)", chosenType.Tag, spec)

	case branch < 58:
		// 同名目录项冲突 (ErrAlreadyExists)
		entries := env.Model.SortedEntries()
		if len(entries) > 0 {
			chosen := entries[env.Rng.IntN(len(entries))]
			caseTag = "同名冲突(ErrAlreadyExists)"
			params.ParentIno = chosen.ParentIno
			params.Name = []byte(chosen.Name)
			params.Mode = meta.S_IFREG | 0o644
		} else {
			caseTag = "特殊名称(.)冲突"
			params.ParentIno = activeDirs[0].Ino
			params.Name = []byte(".")
			params.Mode = meta.S_IFREG | 0o644
		}

	case branch < 65:
		// 特殊名称 "." 或 ".." (ErrAlreadyExists)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = meta.S_IFREG | 0o644
		if env.Rng.IntN(2) == 0 {
			caseTag = "特殊名称(.)(ErrAlreadyExists)"
			params.Name = []byte(".")
		} else {
			caseTag = "特殊名称(..)(ErrAlreadyExists)"
			params.Name = []byte("..")
		}

	case branch < 75:
		// 非法文件类型位：S_IFDIR、S_IFLNK、无文件类型位 (0)、未知类型位 0o170000 或 Mode > 0o177777
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName("bad_mode"))
		switch env.Rng.IntN(5) {
		case 0:
			caseTag = "非法类型S_IFDIR(ErrInvalidArgument)"
			params.Mode = meta.S_IFDIR | 0o755
		case 1:
			caseTag = "非法类型S_IFLNK(ErrInvalidArgument)"
			params.Mode = meta.S_IFLNK | 0o777
		case 2:
			caseTag = "缺失文件类型位0(ErrInvalidArgument)"
			params.Mode = 0o644
		case 3:
			caseTag = "未知文件类型位S_IFMT(ErrInvalidArgument)"
			params.Mode = meta.S_IFMT | 0o644
		default:
			caseTag = "Mode超0o177777(ErrInvalidArgument)"
			params.Mode = 0o200000 | meta.S_IFREG | 0o644
		}

	case branch < 83:
		// 非法名称：空名称、含 '/'、含 '\x00' 或超长 (>255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Mode = meta.S_IFREG | 0o644
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "空名称(ErrInvalidArgument)"
			params.Name = []byte("")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			params.Name = []byte("bad/node")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			params.Name = []byte("bad\x00node")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			params.Name = []byte(strings.Repeat("m", meta.MaxFileNameBytes+1+env.Rng.IntN(16)))
		}

	case branch < 91:
		// 父节点为非目录 (ErrNotDir)
		nonDirs := env.Model.ActiveNonDirs()
		if len(nonDirs) > 0 {
			target := nonDirs[env.Rng.IntN(len(nonDirs))]
			if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
				caseTag = "父节点非目录(ErrNotDir)"
				params.ParentIno = target.Ino
				params.Name = []byte(env.UniqueEntryName("under_nondir"))
				params.Mode = meta.S_IFREG | 0o644
			} else {
				caseTag = "不存在父节点(ErrNotFound)"
				params.ParentIno = uint64(math.MaxInt64 - 112233)
				params.Name = []byte(env.UniqueEntryName("ghost"))
				params.Mode = meta.S_IFREG | 0o644
			}
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 112233)
			params.Name = []byte(env.UniqueEntryName("ghost"))
			params.Mode = meta.S_IFREG | 0o644
		}

	default:
		// 孤儿父目录 (nlink==0)、已删除父节点、不存在父节点或非法 ParentIno=0
		orphanDirs := findOrphanDirsInModel(env.Model)
		params.Name = []byte(env.UniqueEntryName("err_parent"))
		params.Mode = meta.S_IFCHR | 0o600
		if len(orphanDirs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.ParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		} else if env.Rng.IntN(3) == 0 {
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			params.ParentIno = 0
		} else if delIno, ok := env.Model.FirstDeletedIno(); ok && env.Rng.IntN(2) == 0 {
			caseTag = "已删除父节点(ErrNotFound)"
			params.ParentIno = delIno
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 445566)
		}
	}

	expectedMode, expectedGID, expectedRdev, expectedErr := formalPredictMknod(env.Model, params)
	got, err := env.Mgr.Mknod(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Mknod[%s] (parent=%d, name=%q, mode=0o%o) 预期错误 %v，但实际成功: %+v",
			caseTag, params.ParentIno, string(params.Name), params.Mode, expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Mknod[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Mknod[%s](parent=%d, name=%q, mode=0o%o) -> 预期拒绝 (%v)",
			caseTag, params.ParentIno, string(params.Name), params.Mode, expectedErr)
	}

	require.NoErrorf(t, err, "Mknod[%s] (parent=%d, name=%q, mode=0o%o) 预期成功，但返回错误: %v",
		caseTag, params.ParentIno, string(params.Name), params.Mode, err)
	require.NotNil(t, got)

	parentModel := env.Model.Inodes[params.ParentIno]
	require.NotNil(t, parentModel)

	newNodeModel := &formalInode{
		Ino:        got.Ino,
		Generation: got.Generation,
		RootIno:    parentModel.RootIno,
		ParentIno:  0,
		Mode:       expectedMode,
		UID:        params.UID,
		GID:        expectedGID,
		Rdev:       expectedRdev,
		Size:       0,
		Nlink:      1,
		Flags:      0,
	}
	env.Model.RecordNewInode(newNodeModel)
	env.Model.Entries[formalEntryKey{ParentIno: params.ParentIno, Name: string(params.Name)}] = got.Ino

	// Mknod 仅使所属磁盘 used_inodes + 1，used_bytes 与父目录 nlink 均保持不变
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted {
		disk.UsedInodes++
	}

	assertFormalInodeMatchesEntity(t, newNodeModel, got, fmt.Sprintf("Mknod[%s]返回值", caseTag))

	return fmt.Sprintf("Mknod[%s](parent=%d, name=%q, mode=0o%o, umask=0o%o, req_rdev=0x%x) -> 成功 (ino=%d, final_mode=0o%o, gid=%d, rdev=0x%x)",
		caseTag, params.ParentIno, string(params.Name), params.Mode, params.Umask, params.Rdev,
		got.Ino, got.Attr.Mode, got.Attr.GID, got.Attr.Rdev)
}

// formalActionSymlink 在状态机中随机生成覆盖合法目标路径 (1..4095)、非法目标路径 (空、超长、含 \x00)、
// 父目录 S_ISGID 属组继承、固定模式 S_IFLNK|0o777 及配额守恒 (size > 0 但 used_bytes 不变) 的 Symlink 请求并与参考模型对拍。
func formalActionSymlink(ctx context.Context, t *testing.T, env *formalMainEnv) string {
	t.Helper()

	activeDirs := env.Model.ActiveDirs()
	if len(activeDirs) == 0 {
		return formalCreateValidDiskStep(ctx, t, env)
	}

	uids := []uint32{0, 1000, 1001, 2002}
	gids := []uint32{0, 1000, 2000, 3003}

	var params meta.SymlinkParams
	params.UID = uids[env.Rng.IntN(len(uids))]
	params.GID = gids[env.Rng.IntN(len(gids))]

	branch := env.Rng.IntN(100)
	var caseTag string

	switch {
	case branch < 48:
		// 正常创建符号链接：覆盖不同长度档位 (1 字节、常规路径、跨 512 块边界、最大 4095 字节)
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
		params.Name = []byte(env.UniqueEntryName("sym"))

		switch env.Rng.IntN(5) {
		case 0:
			caseTag = "正常创建(1字节最短目标)"
			params.Target = []byte("x")
		case 1:
			caseTag = "正常创建(512字节单块边界)"
			params.Target = []byte(strings.Repeat("a", 512))
		case 2:
			caseTag = "正常创建(513字节跨双块边界)"
			params.Target = []byte(strings.Repeat("b", 513))
		case 3:
			caseTag = "正常创建(4095字节最大合法目标)"
			params.Target = []byte(strings.Repeat("L", meta.MaxSymlinkTargetBytes))
		default:
			caseTag = "正常创建(常规相对或绝对路径)"
			params.Target = []byte(fmt.Sprintf("/mnt/disk/target_%d/file.dat", env.NextSeq()))
		}

	case branch < 58:
		// 同名目录项冲突 (ErrAlreadyExists)
		entries := env.Model.SortedEntries()
		if len(entries) > 0 {
			chosen := entries[env.Rng.IntN(len(entries))]
			caseTag = "同名冲突(ErrAlreadyExists)"
			params.ParentIno = chosen.ParentIno
			params.Name = []byte(chosen.Name)
			params.Target = []byte("/valid/target")
		} else {
			caseTag = "特殊名称(.)冲突"
			params.ParentIno = activeDirs[0].Ino
			params.Name = []byte(".")
			params.Target = []byte("/valid/target")
		}

	case branch < 65:
		// 特殊名称 "." 或 ".." (ErrAlreadyExists)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Target = []byte("../target")
		if env.Rng.IntN(2) == 0 {
			caseTag = "特殊名称(.)(ErrAlreadyExists)"
			params.Name = []byte(".")
		} else {
			caseTag = "特殊名称(..)(ErrAlreadyExists)"
			params.Name = []byte("..")
		}

	case branch < 77:
		// 非法目标路径：空目标、含 '\x00' 或超长 (> MaxSymlinkTargetBytes = 4095)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Name = []byte(env.UniqueEntryName("bad_target_sym"))
		switch env.Rng.IntN(3) {
		case 0:
			caseTag = "空目标路径(ErrInvalidArgument)"
			params.Target = []byte("")
		case 1:
			caseTag = "目标含空字符(ErrInvalidArgument)"
			params.Target = []byte("/path/with\x00null")
		default:
			caseTag = "超长目标路径>=4096(ErrNameTooLong)"
			params.Target = []byte(strings.Repeat("t", meta.MaxSymlinkTargetBytes+1+env.Rng.IntN(32)))
		}

	case branch < 85:
		// 非法目录项名称：空名、含 '/'、含 '\x00' 或超长 (> 255)
		parent := activeDirs[env.Rng.IntN(len(activeDirs))]
		params.ParentIno = parent.Ino
		params.Target = []byte("/valid/target")
		switch env.Rng.IntN(4) {
		case 0:
			caseTag = "空名称(ErrInvalidArgument)"
			params.Name = []byte("")
		case 1:
			caseTag = "含斜杠名称(ErrInvalidArgument)"
			params.Name = []byte("bad/sym")
		case 2:
			caseTag = "含空字符名称(ErrInvalidArgument)"
			params.Name = []byte("bad\x00sym")
		default:
			caseTag = "超长名称>255(ErrNameTooLong)"
			params.Name = []byte(strings.Repeat("s", meta.MaxFileNameBytes+1+env.Rng.IntN(10)))
		}

	case branch < 93:
		// 父节点为非目录 (ErrNotDir)
		nonDirs := env.Model.ActiveNonDirs()
		if len(nonDirs) > 0 {
			target := nonDirs[env.Rng.IntN(len(nonDirs))]
			if target.Nlink > 0 || env.Model.HasValidHandlesForIno(target.Ino) {
				caseTag = "父节点非目录(ErrNotDir)"
				params.ParentIno = target.Ino
				params.Name = []byte(env.UniqueEntryName("sym_under_file"))
				params.Target = []byte("/valid/target")
			} else {
				caseTag = "不存在父节点(ErrNotFound)"
				params.ParentIno = uint64(math.MaxInt64 - 223344)
				params.Name = []byte(env.UniqueEntryName("ghost_sym"))
				params.Target = []byte("/valid/target")
			}
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 223344)
			params.Name = []byte(env.UniqueEntryName("ghost_sym"))
			params.Target = []byte("/valid/target")
		}

	default:
		// 孤儿父目录 (nlink==0)、已删除父节点、不存在父节点或非法 ParentIno=0
		orphanDirs := findOrphanDirsInModel(env.Model)
		params.Name = []byte(env.UniqueEntryName("sym_err_parent"))
		params.Target = []byte("/valid/target")
		if len(orphanDirs) > 0 && env.Rng.IntN(2) == 0 {
			caseTag = "孤儿父目录nlink=0(ErrNotFound)"
			params.ParentIno = orphanDirs[env.Rng.IntN(len(orphanDirs))].Ino
		} else if env.Rng.IntN(3) == 0 {
			caseTag = "非法ParentIno=0(ErrInvalidArgument)"
			params.ParentIno = 0
		} else {
			caseTag = "不存在父节点(ErrNotFound)"
			params.ParentIno = uint64(math.MaxInt64 - 556677)
		}
	}

	expectedMode, expectedGID, expectedErr := formalPredictSymlink(env.Model, params)
	got, err := env.Mgr.Symlink(ctx, params)

	if expectedErr != nil {
		require.Errorf(t, err, "Symlink[%s] (parent=%d, name=%q, target_len=%d) 预期错误 %v，但实际成功: %+v",
			caseTag, params.ParentIno, string(params.Name), len(params.Target), expectedErr, got)
		require.Truef(t, errors.Is(err, expectedErr),
			"Symlink[%s] 错误码不匹配: 预期 %v, 实际 %v", caseTag, expectedErr, err)
		require.Nil(t, got)
		return fmt.Sprintf("Symlink[%s](parent=%d, name=%q, target_len=%d) -> 预期拒绝 (%v)",
			caseTag, params.ParentIno, string(params.Name), len(params.Target), expectedErr)
	}

	require.NoErrorf(t, err, "Symlink[%s] (parent=%d, name=%q, target_len=%d) 预期成功，但返回错误: %v",
		caseTag, params.ParentIno, string(params.Name), len(params.Target), err)
	require.NotNil(t, got)

	parentModel := env.Model.Inodes[params.ParentIno]
	require.NotNil(t, parentModel)

	targetCopy := append([]byte(nil), params.Target...)
	targetLen := uint64(len(targetCopy))
	expectedBlocks := (targetLen + 511) / 512

	// 重点核验 Symlink 返回的 Size 与 Blocks
	require.Equal(t, uint32(meta.S_IFLNK|0o777), got.Attr.Mode, "Symlink 模式必须恒为 S_IFLNK|0o777")
	require.Equal(t, targetLen, got.Attr.Size, "Symlink Size 必须等于 len(target)")
	require.Equal(t, expectedBlocks, got.Attr.Blocks, "Symlink Blocks 必须等于 (len(target)+511)/512")

	newSymModel := &formalInode{
		Ino:           got.Ino,
		Generation:    got.Generation,
		RootIno:       parentModel.RootIno,
		ParentIno:     0,
		Mode:          expectedMode,
		UID:           params.UID,
		GID:           expectedGID,
		Rdev:          0,
		Size:          targetLen,
		Nlink:         1,
		Flags:         0,
		SymlinkTarget: targetCopy,
	}
	env.Model.RecordNewInode(newSymModel)
	env.Model.Entries[formalEntryKey{ParentIno: params.ParentIno, Name: string(params.Name)}] = got.Ino

	// 重点配额边界：Symlink 的 Size > 0 绝不计入所属 Disk 的 used_bytes，仅递增 used_inodes + 1
	var diskUsedBytes uint64
	if disk, ok := env.Model.DiskByRootIno[parentModel.RootIno]; ok && !disk.Deleted {
		disk.UsedInodes++
		diskUsedBytes = disk.UsedBytes
	}

	assertFormalInodeMatchesEntity(t, newSymModel, got, fmt.Sprintf("Symlink[%s]返回值", caseTag))

	return fmt.Sprintf("Symlink[%s](parent=%d, name=%q, target_len=%d) -> 成功 (ino=%d, size=%d, blocks=%d, gid=%d, disk_used_bytes=%d[不变])",
		caseTag, params.ParentIno, string(params.Name), len(params.Target),
		got.Ino, got.Attr.Size, got.Attr.Blocks, got.Attr.GID, diskUsedBytes)
}

// ============================================================================
// 3. T3 只读查账探针：Readlink
// ============================================================================

// formalProbeReadlink 在每步写操作后全面核对：
// 1. 所有存活符号链接返回的目标字节序列与内存参考模型 100% 一致；
// 2. 所有非符号链接节点（目录、常规文件、特殊设备节点 S_IFCHR/S_IFBLK/S_IFIFO/S_IFSOCK）调用 Readlink 返回 ErrInvalidArgument；
// 3. 所有已物理删除节点及不存在的 ino 调用 Readlink 返回 ErrNotFound；
// 4. 非法 ino == 0 调用 Readlink 返回 ErrInvalidArgument。
func formalProbeReadlink(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()

	// 1. 核对所有存活符号链接
	for _, sym := range env.Model.ActiveSymlinks() {
		gotTarget, err := env.Mgr.Readlink(ctx, sym.Ino)
		require.NoErrorf(t, err, "存活符号链接 ino=%d 调用 Readlink 不应失败", sym.Ino)
		require.Equalf(t, sym.SymlinkTarget, gotTarget,
			"Readlink(ino=%d) 返回的目标字节序列与内存参考模型不一致", sym.Ino)
		require.Equalf(t, int(sym.Size), len(gotTarget),
			"Readlink(ino=%d) 返回的目标字节长度 (%d) 与 Inode.Size (%d) 不一致",
			sym.Ino, len(gotTarget), sym.Size)
	}

	// 2. 核对所有非符号链接节点（目录、常规文件、特殊设备节点）调用 Readlink 返回 ErrInvalidArgument
	for _, inode := range env.Model.ActiveInodes() {
		if inode.IsLnk() {
			continue
		}
		// 若为已失去所有有效句柄但尚未经 RenewSession 清扫的孤儿节点，跳过非符号链接校验
		if inode.Nlink == 0 && !env.Model.HasValidHandlesForIno(inode.Ino) {
			continue
		}
		gotTarget, err := env.Mgr.Readlink(ctx, inode.Ino)
		require.Errorf(t, err, "非符号链接节点 ino=%d (mode=0o%o) 调用 Readlink 必须失败", inode.Ino, inode.Mode)
		require.Truef(t, errors.Is(err, meta.ErrInvalidArgument),
			"非符号链接节点 ino=%d (mode=0o%o) 调用 Readlink 应返回 ErrInvalidArgument, 实际: %v",
			inode.Ino, inode.Mode, err)
		require.Nil(t, gotTarget)
	}

	// 3. 核对所有已物理删除的 Inode 调用 Readlink 返回 ErrNotFound
	for _, ino := range env.Model.AllCreatedInos {
		if env.Model.DeletedInos[ino] {
			gotTarget, err := env.Mgr.Readlink(ctx, ino)
			require.Errorf(t, err, "已删除节点 ino=%d 调用 Readlink 应返回 ErrNotFound", ino)
			require.Truef(t, errors.Is(err, meta.ErrNotFound),
				"已删除节点 ino=%d 调用 Readlink 预期 ErrNotFound, 实际: %v", ino, err)
			require.Nil(t, gotTarget)
		}
	}

	// 4. 核对 ino == 0 与不存在的 Ino
	_, err := env.Mgr.Readlink(ctx, 0)
	require.True(t, errors.Is(err, meta.ErrInvalidArgument), "Readlink(ino=0) 应返回 ErrInvalidArgument")

	_, err = env.Mgr.Readlink(ctx, uint64(math.MaxInt64-765432))
	require.True(t, errors.Is(err, meta.ErrNotFound), "Readlink(不存在ino) 应返回 ErrNotFound")
}

// ============================================================================
// 4. T3 底表数学不变量审计：非目录节点属性与符号链接 INV-4 配额隔离审计
// ============================================================================

// formalAuditNodeAndSymlinkQuota 直接扫描 PostgreSQL 底表 inodes 与 disks，强制校验：
//  1. 所有非目录 Inode (S_IFREG, S_IFCHR, S_IFBLK, S_IFIFO, S_IFSOCK, S_IFLNK) 的底表字段
//     (mode, size, nlink, uid, gid, rdev, parent_ino=0, root_ino, symlink_target) 与内存参考模型 100% 一致；
//  2. 对所有 S_IFLNK 符号链接节点，底表 inodes.size == len(inodes.symlink_target) > 0；
//  3. 对所有存活 Disk，底表 disks.used_bytes 严格等于 S_IFREG 节点的 size 之和，绝不混入 S_IFLNK 节点的 size。
func formalAuditNodeAndSymlinkQuota(ctx context.Context, t *testing.T, env *formalMainEnv) {
	t.Helper()
	if len(env.Model.AllCreatedRootInos) == 0 {
		return
	}

	rootInos := make([]int64, 0, len(env.Model.AllCreatedRootInos))
	for _, r := range env.Model.AllCreatedRootInos {
		rootInos = append(rootInos, int64(r))
	}

	rows, err := env.Pool.Query(ctx, `
		SELECT ino, mode, size, nlink, uid, gid, rdev, parent_ino, root_ino, symlink_target
		FROM inodes
		WHERE root_ino = ANY($1)
	`, rootInos)
	require.NoError(t, err, "T3底表审计: 查询 inodes 失败")
	defer rows.Close()

	regBytesByRoot := make(map[uint64]int64)
	symlinkBytesByRoot := make(map[uint64]int64)
	inodeCountByRoot := make(map[uint64]int64)

	for rows.Next() {
		var ino, size, rdev, parentIno, rootIno int64
		var mode, nlink, uid, gid int32
		var symlinkTarget []byte
		err := rows.Scan(&ino, &mode, &size, &nlink, &uid, &gid, &rdev, &parentIno, &rootIno, &symlinkTarget)
		require.NoError(t, err, "T3底表审计: 扫描 inodes 行失败")

		uIno := uint64(ino)
		uRoot := uint64(rootIno)
		uMode := uint32(mode)
		fileType := uMode & meta.S_IFMT

		inodeCountByRoot[uRoot]++
		if fileType == meta.S_IFREG {
			regBytesByRoot[uRoot] += size
		} else if fileType == meta.S_IFLNK {
			symlinkBytesByRoot[uRoot] += size
		}

		if fileType == meta.S_IFDIR {
			continue
		}

		modelNode, exists := env.Model.Inodes[uIno]
		require.Truef(t, exists && !modelNode.Deleted,
			"T3底表审计违例: 底表存在非目录节点 ino=%d (mode=0o%o)，但内存模型中缺失或已标记删除", uIno, uMode)

		require.Zerof(t, parentIno, "T3底表审计违例: 非目录节点 ino=%d 的 parent_ino (%d) 必须为 0", uIno, parentIno)
		require.Equalf(t, modelNode.RootIno, uRoot, "T3底表审计违例: 节点 ino=%d root_ino 不一致", uIno)
		require.Equalf(t, modelNode.Mode, uMode, "T3底表审计违例: 节点 ino=%d mode 不一致 (model=0o%o, db=0o%o)", uIno, modelNode.Mode, uMode)
		require.Equalf(t, int64(modelNode.Size), size, "T3底表审计违例: 节点 ino=%d size 不一致", uIno)
		require.Equalf(t, int32(modelNode.Nlink), nlink, "T3底表审计违例: 节点 ino=%d nlink 不一致", uIno)
		require.Equalf(t, modelNode.UID, uint32(uid), "T3底表审计违例: 节点 ino=%d uid 不一致", uIno)
		require.Equalf(t, modelNode.GID, uint32(gid), "T3底表审计违例: 节点 ino=%d gid 不一致", uIno)
		require.Equalf(t, int64(modelNode.ExpectedAttrRdev()), rdev, "T3底表审计违例: 节点 ino=%d rdev 不一致", uIno)

		if fileType == meta.S_IFLNK {
			require.Greaterf(t, size, int64(0), "T3底表审计违例: 符号链接 ino=%d 底表 size 必须 > 0", uIno)
			require.Equalf(t, int64(len(symlinkTarget)), size,
				"T3底表审计违例: 符号链接 ino=%d 底表 size (%d) != len(symlink_target) (%d)",
				uIno, size, len(symlinkTarget))
			require.Equalf(t, modelNode.SymlinkTarget, symlinkTarget,
				"T3底表审计违例: 符号链接 ino=%d 底表 symlink_target 与内存模型不一致", uIno)
		} else {
			require.Emptyf(t, symlinkTarget,
				"T3底表审计违例: 非符号链接节点 ino=%d 的 symlink_target 应为空", uIno)
		}
	}
	require.NoError(t, rows.Err())

	// 核对每个存活 Disk 的配额账本严格隔离 S_IFLNK 的 size
	for _, disk := range env.Model.ActiveDisks() {
		gotDisk, err := env.Mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err, "T3底表审计: 查询磁盘配额失败")
		dbUsedBytes := int64(gotDisk.UsedBytes)
		dbUsedInodes := int64(gotDisk.UsedInodes)

		expectedRegBytes := regBytesByRoot[disk.RootIno]
		expectedInodes := inodeCountByRoot[disk.RootIno]
		symBytes := symlinkBytesByRoot[disk.RootIno]

		require.Equalf(t, expectedRegBytes, dbUsedBytes,
			"INV-4 符号链接配额违例: root_ino=%d used_bytes (%d) != S_IFREG 总字节 (%d) (当前盘上 S_IFLNK 总字节=%d)",
			disk.RootIno, dbUsedBytes, expectedRegBytes, symBytes)
		require.Equalf(t, expectedInodes, dbUsedInodes,
			"INV-4 节点配额违例: root_ino=%d used_inodes (%d) != inodes 总行数 (%d)",
			disk.RootIno, dbUsedInodes, expectedInodes)
	}
}

// ============================================================================
// 5. T3 专属测试入口：确定性全边界矩阵子测试 + 多轮随机状态机对拍子测试
// ============================================================================

// TestFormalMain_T3_MknodSymlink 验证 T3 主状态机 Mknod/Symlink 节点创建对拍、Readlink 探针与符号链接 INV-4 配额边界。
func TestFormalMain_T3_MknodSymlink(t *testing.T) {
	t.Run("确定性边界_Mknod五类节点_Rdev_Umask_SGID继承_Symlink配额守恒_Readlink与Rmdir非空全矩阵验证", func(t *testing.T) {
		pool := setupFormalPostgresPool(t)
		store := db.NewStore(pool)
		mgr := meta.New(store)

		env := &formalMainEnv{
			T:          t,
			TestName:   t.Name(),
			RunIndex:   0,
			TotalRuns:  1,
			TotalSteps: 30,
			Seed:       0x20260418,
			Rng:        newFormalRNG(0x20260418),
			Pool:       pool,
			Store:      store,
			Mgr:        mgr,
			Model:      newFormalMainModel(),
			NamePrefix: "formal-t3-det",
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
			formalAuditINV4DiskQuota(ctx, t, env)
			formalAuditNodeAndSymlinkQuota(ctx, t, env)
		}

		// 1. 创建初始测试磁盘
		formalCreateValidDiskStep(ctx, t, env)
		disk := env.Model.ActiveDisks()[0]
		rootIno := disk.RootIno
		rootDir := env.Model.Inodes[rootIno]
		runAllProbesAndAuditors()

		// 2. 验证 Mknod 全部 5 种合法文件类型、Rdev 保留/清零规则、Umask 与特权位 (0o7000) 计算
		mknodMatrix := []struct {
			Name         string
			Mode         uint32
			Umask        uint32
			ReqRdev      uint32
			ExpectedMode uint32
			ExpectedRdev uint32
		}{
			{
				Name:         "node_reg_suid",
				Mode:         meta.S_IFREG | meta.S_ISUID | 0o755,
				Umask:        0o022,
				ReqRdev:      0x1234, // 非设备类型必须强制清零 Rdev
				ExpectedMode: meta.S_IFREG | meta.S_ISUID | 0o755,
				ExpectedRdev: 0,
			},
			{
				Name:         "node_chr_dev",
				Mode:         meta.S_IFCHR | 0o666,
				Umask:        0o027,
				ReqRdev:      0x0103, // S_IFCHR 必须保留请求 Rdev
				ExpectedMode: meta.S_IFCHR | 0o640,
				ExpectedRdev: 0x0103,
			},
			{
				Name:         "node_blk_dev",
				Mode:         meta.S_IFBLK | meta.S_ISVTX | 0o660,
				Umask:        0o007,
				ReqRdev:      0x0801, // S_IFBLK 必须保留请求 Rdev
				ExpectedMode: meta.S_IFBLK | meta.S_ISVTX | 0o660,
				ExpectedRdev: 0x0801,
			},
			{
				Name:         "node_fifo",
				Mode:         meta.S_IFIFO | 0o666,
				Umask:        0o022,
				ReqRdev:      0x9999, // S_IFIFO 必须强制清零 Rdev
				ExpectedMode: meta.S_IFIFO | 0o644,
				ExpectedRdev: 0,
			},
			{
				Name:         "node_sock",
				Mode:         meta.S_IFSOCK | 0o777,
				Umask:        0o077,
				ReqRdev:      0xabcd, // S_IFSOCK 必须强制清零 Rdev
				ExpectedMode: meta.S_IFSOCK | 0o700,
				ExpectedRdev: 0,
			},
		}

		for _, tc := range mknodMatrix {
			got, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: rootIno,
				Name:      []byte(tc.Name),
				Mode:      tc.Mode,
				Umask:     tc.Umask,
				UID:       1001,
				GID:       1002,
				Rdev:      tc.ReqRdev,
			})
			require.NoErrorf(t, err, "Mknod(%s) 不应失败", tc.Name)
			require.Equal(t, tc.ExpectedMode, got.Attr.Mode, "Mknod(%s) Mode 不匹配", tc.Name)
			require.Equal(t, tc.ExpectedRdev, got.Attr.Rdev, "Mknod(%s) Rdev 不匹配", tc.Name)
			require.Equal(t, uint32(1), got.Attr.Nlink, "Mknod(%s) Nlink 必须为 1", tc.Name)
			require.Equal(t, uint64(0), got.Attr.Size, "Mknod(%s) Size 必须为 0", tc.Name)
			require.Equal(t, uint64(0), got.Attr.Blocks, "Mknod(%s) Blocks 必须为 0", tc.Name)
			require.Equal(t, uint64(0), got.Attr.ParentIno, "Mknod(%s) ParentIno 必须为 0", tc.Name)

			env.Model.RecordNewInode(&formalInode{
				Ino:        got.Ino,
				Generation: got.Generation,
				RootIno:    rootIno,
				ParentIno:  0,
				Mode:       tc.ExpectedMode,
				UID:        1001,
				GID:        1002,
				Rdev:       tc.ExpectedRdev,
				Size:       0,
				Nlink:      1,
			})
			env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: tc.Name}] = got.Ino
			disk.UsedInodes++
		}
		require.Equal(t, uint64(0), disk.UsedBytes, "Mknod 创建节点后磁盘 used_bytes 必须保持为 0")
		runAllProbesAndAuditors()

		// 3. 验证父目录 S_ISGID 属组继承（非目录节点继承 GID，但绝不自动置位 S_ISGID）
		sgidDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("sgid_dir_for_nodes"),
			Mode:      meta.S_ISGID | 0o755,
			UID:       1000,
			GID:       5000,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino:        sgidDir.Ino,
			Generation: sgidDir.Generation,
			RootIno:    rootIno,
			ParentIno:  rootIno,
			Mode:       sgidDir.Attr.Mode,
			UID:        1000,
			GID:        5000,
			Size:       meta.DefaultDirSize,
			Nlink:      2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "sgid_dir_for_nodes"}] = sgidDir.Ino
		rootDir.Nlink++
		disk.UsedInodes++

		// 3.1 在 sgid_dir_for_nodes 下执行 Mknod（传入 GID=9999，Mode=S_IFREG|0o664 不带 S_ISGID）
		sgidMknod, err := mgr.Mknod(ctx, meta.MknodParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("mknod_in_sgid"),
			Mode:      meta.S_IFREG | 0o664,
			Umask:     0o022,
			UID:       1005,
			GID:       9999,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(5000), sgidMknod.Attr.GID, "Mknod 必须继承 S_ISGID 父目录的 GID=5000")
		require.Equal(t, uint32(meta.S_IFREG|0o644), sgidMknod.Attr.Mode, "Mknod 非目录节点不应自动置位 S_ISGID")
		env.Model.RecordNewInode(&formalInode{
			Ino:        sgidMknod.Ino,
			Generation: sgidMknod.Generation,
			RootIno:    rootIno,
			ParentIno:  0,
			Mode:       sgidMknod.Attr.Mode,
			UID:        1005,
			GID:        5000,
			Size:       0,
			Nlink:      1,
		})
		env.Model.Entries[formalEntryKey{ParentIno: sgidDir.Ino, Name: "mknod_in_sgid"}] = sgidMknod.Ino
		disk.UsedInodes++

		// 3.2 在 sgid_dir_for_nodes 下执行 Symlink（传入 GID=8888，Target 长度 513 跨双块）
		target513 := []byte(strings.Repeat("k", 513))
		sgidSym, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("symlink_in_sgid"),
			Target:    target513,
			UID:       1006,
			GID:       8888,
		})
		require.NoError(t, err)
		require.Equal(t, uint32(5000), sgidSym.Attr.GID, "Symlink 必须继承 S_ISGID 父目录的 GID=5000")
		require.Equal(t, uint32(meta.S_IFLNK|0o777), sgidSym.Attr.Mode, "Symlink Mode 必须恒为 S_IFLNK|0o777")
		require.Equal(t, uint64(513), sgidSym.Attr.Size, "Symlink Size 必须等于 len(target)=513")
		require.Equal(t, uint64(2), sgidSym.Attr.Blocks, "513 字节 Symlink 的 Blocks 必须为 (513+511)/512 = 2")
		env.Model.RecordNewInode(&formalInode{
			Ino:           sgidSym.Ino,
			Generation:    sgidSym.Generation,
			RootIno:       rootIno,
			ParentIno:     0,
			Mode:          sgidSym.Attr.Mode,
			UID:           1006,
			GID:           5000,
			Size:          513,
			Nlink:         1,
			SymlinkTarget: target513,
		})
		env.Model.Entries[formalEntryKey{ParentIno: sgidDir.Ino, Name: "symlink_in_sgid"}] = sgidSym.Ino
		disk.UsedInodes++

		// 3.3 创建最大合法长度 (4095 字节) 与最短 (1 字节) 的 Symlink，核验 INV-4 配额边界
		targetMax := []byte(strings.Repeat("M", meta.MaxSymlinkTargetBytes))
		maxSym, err := mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno,
			Name:      []byte("symlink_max_4095"),
			Target:    targetMax,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(meta.MaxSymlinkTargetBytes), maxSym.Attr.Size)
		require.Equal(t, uint64(8), maxSym.Attr.Blocks)
		env.Model.RecordNewInode(&formalInode{
			Ino:           maxSym.Ino,
			Generation:    maxSym.Generation,
			RootIno:       rootIno,
			ParentIno:     0,
			Mode:          maxSym.Attr.Mode,
			UID:           1000,
			GID:           1000,
			Size:          uint64(meta.MaxSymlinkTargetBytes),
			Nlink:         1,
			SymlinkTarget: targetMax,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "symlink_max_4095"}] = maxSym.Ino
		disk.UsedInodes++

		// 此时盘上已有 513 + 4095 = 4608 字节的符号链接，但磁盘 used_bytes 必须依然为 0！
		require.Equal(t, uint64(0), disk.UsedBytes)
		runAllProbesAndAuditors()

		// 4. 验证 Mknod 与 Symlink 的全部错误码边界
		// 4.1 Mknod 非法 Mode 类型位
		for _, badMode := range []uint32{
			0o644,                   // 缺失类型位 (0)
			meta.S_IFDIR | 0o755,    // 目录类型
			meta.S_IFLNK | 0o777,    // 符号链接类型
			meta.S_IFMT | 0o644,     // 非法类型掩码 0o170000
			0o200000 | meta.S_IFREG, // 超出 0o177777
		} {
			_, err := mgr.Mknod(ctx, meta.MknodParams{
				ParentIno: rootIno,
				Name:      []byte("bad_type_node"),
				Mode:      badMode,
			})
			require.ErrorIsf(t, err, meta.ErrInvalidArgument, "Mknod(mode=0o%o) 应返回 ErrInvalidArgument", badMode)
		}

		// 4.2 Symlink 非法目标路径（空、含 '\x00'、超长 >= 4096）
		_, err = mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno,
			Name:      []byte("bad_sym_empty"),
			Target:    []byte(""),
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno,
			Name:      []byte("bad_sym_nul"),
			Target:    []byte("/path/with\x00nul"),
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)

		_, err = mgr.Symlink(ctx, meta.SymlinkParams{
			ParentIno: rootIno,
			Name:      []byte("bad_sym_4096"),
			Target:    []byte(strings.Repeat("x", meta.MaxSymlinkTargetBytes+1)),
		})
		require.ErrorIs(t, err, meta.ErrNameTooLong)

		// 4.3 同名冲突与 "." / ".." 拦截 (ErrAlreadyExists)
		_, err = mgr.Mknod(ctx, meta.MknodParams{ParentIno: rootIno, Name: []byte("node_reg_suid"), Mode: meta.S_IFREG | 0o644})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Mknod(ctx, meta.MknodParams{ParentIno: rootIno, Name: []byte("."), Mode: meta.S_IFREG | 0o644})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Mknod(ctx, meta.MknodParams{ParentIno: rootIno, Name: []byte(".."), Mode: meta.S_IFREG | 0o644})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)

		_, err = mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: rootIno, Name: []byte("symlink_max_4095"), Target: []byte("x")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: rootIno, Name: []byte("."), Target: []byte("x")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)
		_, err = mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: rootIno, Name: []byte(".."), Target: []byte("x")})
		require.ErrorIs(t, err, meta.ErrAlreadyExists)

		// 4.4 父节点非目录（以 Mknod 节点或 Symlink 节点作为 ParentIno） -> ErrNotDir
		_, err = mgr.Mknod(ctx, meta.MknodParams{ParentIno: sgidMknod.Ino, Name: []byte("sub"), Mode: meta.S_IFREG | 0o644})
		require.ErrorIs(t, err, meta.ErrNotDir)
		_, err = mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: sgidSym.Ino, Name: []byte("sub"), Target: []byte("x")})
		require.ErrorIs(t, err, meta.ErrNotDir)

		// 5. 验证目录中存在 Mknod / Symlink 子项时 Rmdir 必返回 ErrNotEmpty，对 Mknod / Symlink 自身调用 Rmdir 返回 ErrNotDir
		err = mgr.Rmdir(ctx, rootIno, []byte("sgid_dir_for_nodes"))
		require.ErrorIs(t, err, meta.ErrNotEmpty, "包含 Mknod/Symlink 子节点的目录调用 Rmdir 必须返回 ErrNotEmpty")
		err = mgr.Rmdir(ctx, sgidDir.Ino, []byte("mknod_in_sgid"))
		require.ErrorIs(t, err, meta.ErrNotDir, "对 Mknod 节点调用 Rmdir 必须返回 ErrNotDir")
		err = mgr.Rmdir(ctx, sgidDir.Ino, []byte("symlink_in_sgid"))
		require.ErrorIs(t, err, meta.ErrNotDir, "对 Symlink 节点调用 Rmdir 必须返回 ErrNotDir")

		// 6. 验证孤儿父目录 (nlink == 0) 拒绝 Mknod 与 Symlink (ErrNotFound)
		orphanTestDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: rootIno,
			Name:      []byte("orphan_parent_dir"),
			Mode:      0o755,
		})
		require.NoError(t, err)
		env.Model.RecordNewInode(&formalInode{
			Ino:        orphanTestDir.Ino,
			Generation: orphanTestDir.Generation,
			RootIno:    rootIno,
			ParentIno:  rootIno,
			Mode:       orphanTestDir.Attr.Mode,
			Size:       meta.DefaultDirSize,
			Nlink:      2,
		})
		env.Model.Entries[formalEntryKey{ParentIno: rootIno, Name: "orphan_parent_dir"}] = orphanTestDir.Ino
		rootDir.Nlink++
		disk.UsedInodes++

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: orphanTestDir.Ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		env.Model.RecordNewHandle(&formalHandle{Fh: openDirRes.Fh, Ino: orphanTestDir.Ino, Flags: openDirRes.OpenFlags, IsDir: true})

		err = mgr.Rmdir(ctx, rootIno, []byte("orphan_parent_dir"))
		require.NoError(t, err)
		delete(env.Model.Entries, formalEntryKey{ParentIno: rootIno, Name: "orphan_parent_dir"})
		rootDir.Nlink--
		env.Model.Inodes[orphanTestDir.Ino].Nlink = 0

		_, err = mgr.Mknod(ctx, meta.MknodParams{ParentIno: orphanTestDir.Ino, Name: []byte("f"), Mode: meta.S_IFREG | 0o644})
		require.ErrorIs(t, err, meta.ErrNotFound, "孤儿父目录下调用 Mknod 必须返回 ErrNotFound")
		_, err = mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: orphanTestDir.Ino, Name: []byte("s"), Target: []byte("x")})
		require.ErrorIs(t, err, meta.ErrNotFound, "孤儿父目录下调用 Symlink 必须返回 ErrNotFound")

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{Ino: orphanTestDir.Ino, Fh: openDirRes.Fh})
		require.NoError(t, err)
		env.Model.Handles[openDirRes.Fh].Closed = true
		env.Model.ClosedHandles[openDirRes.Fh] = env.Model.Handles[openDirRes.Fh]
		delete(env.Model.Handles, openDirRes.Fh)
		require.True(t, env.Model.ReclaimOrphanInModel(orphanTestDir.Ino))

		runAllProbesAndAuditors()
	})

	t.Run("随机状态机_Mknod与Symlink节点创建_Readlink探针_Rmdir非空拦截与INV-4配额边界对拍", func(t *testing.T) {
		runFormalMainStateMachine(t, formalMainRunConfig{
			Actions: []formalWeightedAction{
				{Name: "CreateDisk", Weight: 10, Fn: formalActionCreateDisk},
				{Name: "UpdateDisk", Weight: 10, Fn: formalActionUpdateDisk},
				{Name: "T1QuotaFileStep", Weight: 10, Fn: formalActionT1QuotaFileStep},
				{Name: "Mkdir", Weight: 20, Fn: formalActionMkdir},
				{Name: "Rmdir", Weight: 15, Fn: formalActionRmdir},
				{Name: "Mknod", Weight: 20, Fn: formalActionMknod},
				{Name: "Symlink", Weight: 20, Fn: formalActionSymlink},
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
				{Name: "INV-4_DiskQuota", Fn: formalAuditINV4DiskQuota},
				{Name: "INV-4_NodeAndSymlinkQuota", Fn: formalAuditNodeAndSymlinkQuota},
			},
		})
	})
}
