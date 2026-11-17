package store_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestGivenRegularAndHardlinkedFilesWhenUnlinkedThenReclaimAndLinksBehaveCorrectly(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "unlink-links"})

	// Given: 创建含数据与扩展属性的文件及其硬链接
	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("a.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	payload := bytes.Repeat([]byte{0x3A}, 4096)
	_, _, err = actor.Write(ctx, f.Ino, 0, payload)
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.tag"), []byte("v1"), 0))

	_, err = actor.Link(ctx, store.LinkReq{
		Ino:          f.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("b.bin"),
	})
	require.NoError(t, err)

	// When: 删除第一个链接 a.bin
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("a.bin"), false))

	// Then: 节点依然存活，Nlink=1，数据与扩展属性仍可读
	_, _, err = actor.Lookup(ctx, store.RootIno, []byte("a.bin"))
	require.ErrorIsf(t, err, store.ErrNotFound, "已删目录项 a.bin 不应再可查: got=%v", err)

	attrB, err := actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	require.Equalf(t, uint32(1), attrB.Nlink, "剩余硬链接 Nlink 应为 1: got=%d", attrB.Nlink)

	gotData, err := actor.Read(ctx, f.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, payload, gotData)

	// When: 删除最后一个链接 b.bin（openHandle=false）
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("b.bin"), false))

	// Then: 节点与逻辑容量立即回收，无孤儿残留
	_, err = actor.GetAttr(ctx, f.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "最后一个链接删除后 GetAttr 应返回 ErrNotFound: got=%v", err)

	hasOrphans, err := actor.HasOrphans()
	require.NoError(t, err)
	require.Falsef(t, hasOrphans, "openHandle=false 删除后不应残留孤儿")

	st, err := actor.StatFS(ctx)
	require.NoError(t, err)
	require.Zero(t, st.UsedBytes, "删除全部文件后 UsedBytes 应归零: got=%d", st.UsedBytes)
	require.Equalf(t, uint64(1), st.Files, "删除全部文件后 Files 应回到 1: got=%d", st.Files)
}

func TestGivenOpenHandleOrphanWhenPurgedOrCleanedThenResourcesReclaimed(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "unlink-orphan"})

	// Given: 两个常规文件均写入数据，随后以 openHandle=true 执行 Unlink 转为孤儿节点
	f1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("o1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	p1 := bytes.Repeat([]byte{0x11}, 2048)
	_, _, err = actor.Write(ctx, f1.Ino, 0, p1)
	require.NoError(t, err)

	f2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("o2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	p2 := bytes.Repeat([]byte{0x22}, 4096)
	_, _, err = actor.Write(ctx, f2.Ino, 0, p2)
	require.NoError(t, err)

	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("o1.bin"), true))
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("o2.bin"), true))

	hasOrphans, err := actor.HasOrphans()
	require.NoError(t, err)
	require.Truef(t, hasOrphans, "openHandle=true 删除后 HasOrphans 应为 true")

	// Then: 目录项不可见但孤儿节点仍可通过节点号读取
	_, _, err = actor.Lookup(ctx, store.RootIno, []byte("o1.bin"))
	require.ErrorIs(t, err, store.ErrNotFound)

	got1, err := actor.Read(ctx, f1.Ino, 0, 2048)
	require.NoError(t, err)
	require.Equal(t, p1, got1)

	// When: 先单独 PurgeOrphanInode(f1)，再 CleanOrphans 批量回收剩余孤儿 f2
	require.NoError(t, actor.PurgeOrphanInode(ctx, f1.Ino))
	_, err = actor.GetAttr(ctx, f1.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "PurgeOrphanInode 后 o1 应彻底消失: got=%v", err)

	hasOrphans, err = actor.HasOrphans()
	require.NoError(t, err)
	require.Truef(t, hasOrphans, "o2 尚未清理时 HasOrphans 仍应为 true")

	require.NoError(t, actor.CleanOrphans(ctx))
	_, err = actor.GetAttr(ctx, f2.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "CleanOrphans 后 o2 应彻底消失: got=%v", err)

	hasOrphans, err = actor.HasOrphans()
	require.NoError(t, err)
	require.Falsef(t, hasOrphans, "CleanOrphans 后 HasOrphans 应为 false")

	st, err := actor.StatFS(ctx)
	require.NoError(t, err)
	require.Zero(t, st.UsedBytes)
	require.Equalf(t, uint64(1), st.Files, "清理所有孤儿后 Files 应回到 1: got=%d", st.Files)
}

func TestGivenDirectoriesWhenRmdirCalledThenHandlesEmptyNonEmptyAndImmutable(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "rmdir-cases"})

	// Given: 非空目录、空目录、不可变目录与常规文件
	nonEmptyDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("nonempty"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	_, err = actor.Create(ctx, store.CreateReq{ParentIno: nonEmptyDir.Ino, Name: []byte("inside.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	emptyDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("empty"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, emptyDir.Ino, []byte("user.dir"), []byte("1"), 0))

	immutDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("immut"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	immutFlag := store.FlagImmutable
	_, err = actor.SetAttr(ctx, immutDir.Ino, store.SetAttrReq{Flags: &immutFlag})
	require.NoError(t, err)

	regFile, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("file.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// When & Then: 错误路径校验
	require.ErrorIs(t, actor.Rmdir(ctx, store.RootIno, []byte("nonempty")), store.ErrNotEmpty)
	require.ErrorIs(t, actor.Rmdir(ctx, store.RootIno, []byte("immut")), store.ErrPerm)
	require.ErrorIs(t, actor.Rmdir(ctx, store.RootIno, []byte("file.txt")), store.ErrNotDir)
	require.ErrorIs(t, actor.Rmdir(ctx, store.RootIno, []byte(".")), store.ErrInvalid)
	require.ErrorIs(t, actor.Rmdir(ctx, store.RootIno, []byte("..")), store.ErrNotEmpty)
	require.ErrorIs(t, actor.Unlink(ctx, store.RootIno, []byte("empty"), false), store.ErrIsDir)
	require.ErrorIs(t, actor.PurgeOrphanInode(ctx, regFile.Ino), store.ErrInvalid)

	// When: 成功删除空目录
	rootBefore, err := actor.GetAttr(ctx, store.RootIno)
	require.NoError(t, err)
	require.NoError(t, actor.Rmdir(ctx, store.RootIno, []byte("empty")))

	// Then: 空目录消失且父目录 Nlink 递减 1
	_, err = actor.GetAttr(ctx, emptyDir.Ino)
	require.ErrorIs(t, err, store.ErrNotFound)
	rootAfter, err := actor.GetAttr(ctx, store.RootIno)
	require.NoError(t, err)
	require.Equalf(t, rootBefore.Nlink-1, rootAfter.Nlink, "Rmdir 后父目录 Nlink 应减 1: want=%d got=%d", rootBefore.Nlink-1, rootAfter.Nlink)
}

func TestGivenFilesAndDirsWhenRenameOverwriteOrExchangeThenSemanticsPreserved(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "rename-ops"})

	// Given: 两个目录 dirA、dirB 与多个测试节点
	dirA, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("dirA"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	dirB, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("dirB"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)

	srcFile, err := actor.Create(ctx, store.CreateReq{ParentIno: dirA.Ino, Name: []byte("src.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, srcFile.Ino, 0, []byte("src-data"))
	require.NoError(t, err)

	dstFile, err := actor.Create(ctx, store.CreateReq{ParentIno: dirB.Ino, Name: []byte("dst.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, dstFile.Ino, 0, []byte("dst-data"))
	require.NoError(t, err)

	// When: 使用 RenameNoreplace 覆写已存在目标被拒绝
	err = actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("src.txt"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("dst.txt"),
		Flags:        store.RenameNoreplace,
	})
	require.ErrorIsf(t, err, store.ErrExist, "RenameNoreplace 遇已存在目标应返回 ErrExist: got=%v", err)

	// When: 使用 RenameExchange 双向交换跨目录的两个文件
	require.NoError(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("src.txt"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("dst.txt"),
		Flags:        store.RenameExchange,
	}))

	// Then: 两边目录项互换且文件内容完整跟随原 Ino
	eA, _, err := actor.Lookup(ctx, dirA.Ino, []byte("src.txt"))
	require.NoError(t, err)
	require.Equalf(t, dstFile.Ino, eA.Ino, "交换后 dirA/src.txt 应指向 dstFile.Ino")
	eB, _, err := actor.Lookup(ctx, dirB.Ino, []byte("dst.txt"))
	require.NoError(t, err)
	require.Equalf(t, srcFile.Ino, eB.Ino, "交换后 dirB/dst.txt 应指向 srcFile.Ino")

	// When: 默认覆写并设置 TargetOpenHandle=true，被覆写目标转为孤儿可继续读取
	require.NoError(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno:     dirA.Ino,
		OldName:          []byte("src.txt"),
		NewParentIno:     dirB.Ino,
		NewName:          []byte("dst.txt"),
		TargetOpenHandle: true,
	}))

	hasOrphans, err := actor.HasOrphans()
	require.NoError(t, err)
	require.Truef(t, hasOrphans, "TargetOpenHandle=true 覆写目标后应保留孤儿")
	orphanBytes, err := actor.Read(ctx, srcFile.Ino, 0, 8)
	require.NoError(t, err)
	require.Equal(t, []byte("src-data"), orphanBytes)

	// When: 跨父目录移动子目录，并验证移动后 ".." 指向新父目录且双亲 Nlink 正确维护
	movingDir, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: dirA.Ino, Name: []byte("mvdir"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	grandChild, err := actor.Mkdir(ctx, store.MkdirReq{ParentIno: movingDir.Ino, Name: []byte("leaf"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)

	// 环路防御：禁止将 movingDir 移入其自身子目录 grandChild 内
	err = actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("mvdir"),
		NewParentIno: grandChild.Ino,
		NewName:      []byte("cycle"),
	})
	require.ErrorIsf(t, err, store.ErrInvalid, "将目录移入其子孙目录应返回 ErrInvalid: got=%v", err)

	require.NoError(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("mvdir"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("moved"),
	}))

	dotDotEntry, _, err := actor.Lookup(ctx, movingDir.Ino, []byte(".."))
	require.NoError(t, err)
	require.Equalf(t, dirB.Ino, dotDotEntry.Ino, "跨目录移动后 '..' 必须指向新父目录 dirB: got=%d", dotDotEntry.Ino)

	dirAAfter, err := actor.GetAttr(ctx, dirA.Ino)
	require.NoError(t, err)
	dirBAfter, err := actor.GetAttr(ctx, dirB.Ino)
	require.NoError(t, err)
	require.Equalf(t, uint32(2), dirAAfter.Nlink, "移出子目录后 dirA Nlink 应回到 2: got=%d", dirAAfter.Nlink)
	require.Equalf(t, uint32(3), dirBAfter.Nlink, "移入子目录后 dirB Nlink 应升至 3: got=%d", dirBAfter.Nlink)

	// 不支持与冲突标志检查
	require.ErrorIs(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirB.Ino, OldName: []byte("moved"),
		NewParentIno: dirA.Ino, NewName: []byte("x"),
		Flags: store.RenameWhiteout,
	}), store.ErrNotSupported)
	require.ErrorIs(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirB.Ino, OldName: []byte("moved"),
		NewParentIno: dirA.Ino, NewName: []byte("x"),
		Flags: store.RenameNoreplace | store.RenameExchange,
	}), store.ErrInvalid)
	require.ErrorIs(t, actor.Rename(ctx, store.RenameReq{
		OldParentIno: dirB.Ino, OldName: []byte("moved"),
		NewParentIno: dirB.Ino, NewName: []byte("dst.txt"),
	}), store.ErrNotDir)
}

func TestGivenUnpromotedTmpfileOrphansWhenPurgedOrCleanedThenResourcesReclaimed(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "tmpfile-purge-clean"})

	// Given: unpromoted exclusive (0x02) and linkable (0x03) tmpfiles with data blocks and xattrs.
	excl, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: true,
	})
	require.NoErrorf(t, err, "CreateTmpfile exclusive failed")
	_, _, err = actor.Write(ctx, excl.Ino, 0, bytes.Repeat([]byte{0x21}, 4096))
	require.NoErrorf(t, err, "Write exclusive tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, excl.Ino, []byte("user.k1"), []byte("v1"), 0), "SetXattr exclusive failed")

	linkable, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o644,
		Exclusive: false,
	})
	require.NoErrorf(t, err, "CreateTmpfile linkable failed")
	_, _, err = actor.Write(ctx, linkable.Ino, 0, bytes.Repeat([]byte{0x31}, store.BlobSize+2048))
	require.NoErrorf(t, err, "Write linkable tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, linkable.Ino, []byte("user.k2"), []byte("v2"), 0), "SetXattr linkable failed")

	// When: purging the exclusive tmpfile first, then cleaning all remaining orphans.
	require.NoErrorf(t, actor.PurgeOrphanInode(ctx, excl.Ino), "PurgeOrphanInode on exclusive tmpfile failed")

	// Then: exclusive tmpfile is gone, linkable tmpfile remains until CleanOrphans reclaims everything.
	_, err = actor.GetAttr(ctx, excl.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "purged exclusive tmpfile must return ErrNotFound: got=%v", err)

	hasOrphansMid, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans mid check failed")
	require.Truef(t, hasOrphansMid, "HasOrphans must remain true while linkable tmpfile exists")

	stMid, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS mid check failed")
	require.Equalf(t, uint64(2), stMid.Files, "StatFS Files mid mismatch: want=2 got=%d", stMid.Files)
	require.Equalf(t, uint64(store.BlobSize+2048), stMid.UsedBytes, "StatFS UsedBytes mid mismatch: want=%d got=%d", store.BlobSize+2048, stMid.UsedBytes)

	require.NoErrorf(t, actor.CleanOrphans(ctx), "CleanOrphans failed")
	_, err = actor.GetAttr(ctx, linkable.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "cleaned linkable tmpfile must return ErrNotFound: got=%v", err)

	hasOrphansAfter, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans after CleanOrphans failed")
	require.Falsef(t, hasOrphansAfter, "HasOrphans must be false after CleanOrphans")

	stAfter, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS after CleanOrphans failed")
	require.Zerof(t, stAfter.UsedBytes, "StatFS UsedBytes must return to 0: got=%d", stAfter.UsedBytes)
	require.Equalf(t, uint64(1), stAfter.Files, "StatFS Files must return to 1: got=%d", stAfter.Files)
}

func TestGivenUnpromotedTmpfileOrphansWhenAdvanceEpochCalledThenOrphansReclaimed(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "tmpfile-advance-epoch"})

	// Given: unpromoted exclusive (0x02) and linkable (0x03) tmpfiles with data and xattrs.
	excl, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: true,
	})
	require.NoErrorf(t, err, "CreateTmpfile exclusive failed")
	_, _, err = actor.Write(ctx, excl.Ino, 0, bytes.Repeat([]byte{0x41}, 8192))
	require.NoErrorf(t, err, "Write exclusive tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, excl.Ino, []byte("user.tag"), []byte("excl"), 0), "SetXattr exclusive failed")

	linkable, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o644,
		Exclusive: false,
	})
	require.NoErrorf(t, err, "CreateTmpfile linkable failed")
	_, _, err = actor.Write(ctx, linkable.Ino, 0, bytes.Repeat([]byte{0x42}, 8192))
	require.NoErrorf(t, err, "Write linkable tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, linkable.Ino, []byte("user.tag"), []byte("link"), 0), "SetXattr linkable failed")

	// When: advancing the writer epoch.
	epoch, _, err := actor.AdvanceEpoch(ctx)
	require.NoErrorf(t, err, "AdvanceEpoch failed")
	require.Equalf(t, uint64(1), epoch, "epoch mismatch: want=1 got=%d", epoch)

	// Then: both tmpfiles and their accounting are reclaimed.
	_, err = actor.GetAttr(ctx, excl.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "exclusive tmpfile must be reclaimed on AdvanceEpoch: got=%v", err)
	_, err = actor.GetAttr(ctx, linkable.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "linkable tmpfile must be reclaimed on AdvanceEpoch: got=%v", err)

	hasOrphans, err := actor.HasOrphans()
	require.NoErrorf(t, err, "HasOrphans after AdvanceEpoch failed")
	require.Falsef(t, hasOrphans, "HasOrphans must be false after AdvanceEpoch")

	st, err := actor.StatFS(ctx)
	require.NoErrorf(t, err, "StatFS after AdvanceEpoch failed")
	require.Zerof(t, st.UsedBytes, "StatFS UsedBytes must return to 0: got=%d", st.UsedBytes)
	require.Equalf(t, uint64(1), st.Files, "StatFS Files must return to 1: got=%d", st.Files)
}

func TestGivenCheckpointWithUnpromotedTmpfileOrphansWhenSanitizedThenStatFSRestored(t *testing.T) {
	ctx := context.Background()
	actor := openTestActor(t, &store.Options{DiskID: "tmpfile-sanitize-src"})

	// Given: unpromoted exclusive (0x02) and linkable (0x03) tmpfiles with data blocks and xattrs.
	excl, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o600,
		Exclusive: true,
	})
	require.NoErrorf(t, err, "CreateTmpfile exclusive failed")
	_, _, err = actor.Write(ctx, excl.Ino, 0, bytes.Repeat([]byte{0x51}, 4096))
	require.NoErrorf(t, err, "Write exclusive tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, excl.Ino, []byte("user.tmp"), []byte("e"), 0), "SetXattr exclusive failed")

	linkable, err := actor.CreateTmpfile(ctx, store.CreateTmpfileReq{
		ParentIno: store.RootIno,
		Mode:      store.S_IFREG | 0o644,
		Exclusive: false,
	})
	require.NoErrorf(t, err, "CreateTmpfile linkable failed")
	_, _, err = actor.Write(ctx, linkable.Ino, 0, bytes.Repeat([]byte{0x52}, 4096))
	require.NoErrorf(t, err, "Write linkable tmpfile failed")
	require.NoErrorf(t, actor.SetXattr(ctx, linkable.Ino, []byte("user.tmp"), []byte("l"), 0), "SetXattr linkable failed")

	ckDir := filepath.Join(t.TempDir(), "ck")
	ckStats, err := actor.Checkpoint(ctx, ckDir)
	require.NoErrorf(t, err, "Checkpoint failed")
	require.Truef(t, ckStats.HasOrphans, "checkpoint must report HasOrphans=true")
	require.Equalf(t, uint64(3), ckStats.Inodes, "checkpoint Inodes mismatch: want=3 got=%d", ckStats.Inodes)
	require.Equalf(t, uint64(8192), ckStats.LogicalBytes, "checkpoint LogicalBytes mismatch: want=8192 got=%d", ckStats.LogicalBytes)

	// When: sanitizing the checkpoint directory.
	sanitized, err := store.SanitizeCheckpoint(ctx, store.SanitizeOptions{
		Dir:      ckDir,
		CommitID: "tmpfile-sanitize",
	})
	require.NoErrorf(t, err, "SanitizeCheckpoint failed")

	// Then: sanitized checkpoint reports UsedBytes=0 and Files=1, with all tmpfile orphans gone.
	require.Zerof(t, sanitized.UsedBytes, "sanitized UsedBytes must be 0: got=%d", sanitized.UsedBytes)
	require.Equalf(t, uint64(1), sanitized.Files, "sanitized Files must be 1: got=%d", sanitized.Files)

	ro := openTestActor(t, &store.Options{Dir: ckDir, DiskID: "tmpfile-sanitize-ro", ReadOnly: true})
	roOrphans, err := ro.HasOrphans()
	require.NoErrorf(t, err, "read-only HasOrphans on sanitized checkpoint failed")
	require.Falsef(t, roOrphans, "sanitized checkpoint must have HasOrphans=false")

	_, err = ro.GetAttr(ctx, excl.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "exclusive tmpfile must not exist in sanitized checkpoint: got=%v", err)
	_, err = ro.GetAttr(ctx, linkable.Ino)
	require.ErrorIsf(t, err, store.ErrNotFound, "linkable tmpfile must not exist in sanitized checkpoint: got=%v", err)
}
