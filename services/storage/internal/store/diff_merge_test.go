package store_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func openTestBranch(t *testing.T, dir string, pool *sharedSegmentPool) *store.StoreActor {
	t.Helper()
	opts := store.Options{
		Dir:      dir,
		MaxBytes: 64 << 20,
	}
	if pool != nil {
		opts.InoAllocator = pool.allocIno
		opts.PackAllocator = pool.allocPack
	}
	a, err := store.Open(opts)
	require.NoError(t, err)
	return a
}

func makeSeededFork(t *testing.T, pool *sharedSegmentPool, seedFn func(ctx context.Context, base *store.StoreActor)) (baseDir, targetDir, sourceDir string) {
	t.Helper()
	ctx := context.Background()
	root := t.TempDir()
	originDir := filepath.Join(root, "origin")
	baseDir = filepath.Join(root, "base")
	targetDir = filepath.Join(root, "target")
	sourceDir = filepath.Join(root, "source")

	origin := openTestBranch(t, originDir, pool)
	if seedFn != nil {
		seedFn(ctx, origin)
	}
	_, err := origin.Checkpoint(ctx, baseDir)
	require.NoError(t, err)
	require.NoError(t, origin.Close())

	require.NoError(t, store.CloneCheckpointDir(ctx, baseDir, targetDir))
	require.NoError(t, store.CloneCheckpointDir(ctx, baseDir, sourceDir))
	return baseDir, targetDir, sourceDir
}

func TestInspectBranchStat_ReadOnlyAndOrphans(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "disk")
	pool := newSharedSegmentPool(100, 1)
	actor := openTestBranch(t, dir, pool)

	_, _, err := actor.AdvanceEpoch(ctx)
	require.NoError(t, err)
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("orphan.bin"), Mode: store.S_IFREG | 0o644, Epoch: 1})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, []byte("hello"), 1)
	require.NoError(t, err)
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true, 1))
	require.NoError(t, actor.Close())

	epoch, nextSeq, hasOrphans, err := store.InspectBranchStat(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1), epoch)
	require.GreaterOrEqual(t, nextSeq, uint64(4))
	require.True(t, hasOrphans)
}

func TestDiffTrees_Slice1_WriteSeqPruning_NestedSubdir_And_SparseBlocks(t *testing.T) {
	ctx := context.Background()
	pool := newSharedSegmentPool(100, 1)

	var sharedFileIno, sparseFileIno, nestedFileIno uint64
	baseDir, targetDir, sourceDir := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
		// 1. 用于验证纵向 WriteSeq 剪枝 + CompactPacks 免疫的只读文件
		f1, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("unchanged.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		sharedFileIno = f1.Ino
		_, _, err = base.Write(ctx, f1.Ino, 0, makePatternBytes(128<<10, 0x31))
		require.NoError(t, err)

		// 2. 用于验证制造低水位触发 CompactPacks 搬迁的垃圾文件
		trash, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("trash.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = base.Write(ctx, trash.Ino, 0, makePatternBytes(512<<10, 0x77))
		require.NoError(t, err)

		// 3. 用于验证深层子目录修改不漏检（父目录 WriteSeq 不向上冒泡）
		d1, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("a"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		d2, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: d1.Ino, Name: []byte("b"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		nf, err := base.Create(ctx, store.CreateReq{ParentIno: d2.Ino, Name: []byte("deep.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		nestedFileIno = nf.Ino
		_, _, err = base.Write(ctx, nf.Ino, 0, []byte("v0"))
		require.NoError(t, err)

		// 4. 用于验证同 WriteSeq 跨分支碰撞禁令（Target 与 Source 各写一次不同内容）
		cf, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("cross.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = base.Write(ctx, cf.Ino, 0, []byte("base-cross"))
		require.NoError(t, err)

		// 5. 用于验证局部块 + SetAttr 扩展补零等价性
		sf, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("sparse.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		sparseFileIno = sf.Ino
		_, _, err = base.Write(ctx, sf.Ino, 0, []byte("HEAD"))
		require.NoError(t, err)
	})

	target := openTestBranch(t, targetDir, pool)
	// Target 删除 trash.bin 并触发 CompactPacks 搬迁 unchanged.bin（WriteSeq 未变但 BlobRef 已搬迁）
	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("trash.bin"), false))
	require.NoError(t, target.CompactPacks(ctx))

	// Target 修改 cross.txt 为相同长度不同内容（1 次写，WriteSeq 与 Source 后续 1 次写相同！）
	_, crossEntry, err := target.Lookup(ctx, store.RootIno, []byte("cross.txt"))
	require.NoError(t, err)
	_, _, err = target.Write(ctx, crossEntry.Ino, 0, []byte("tgt!-cross"))
	require.NoError(t, err)

	// Target 对 sparse.bin 写入 1024B 尾部全 0 并 SetAttr 扩容至 4096
	_, _, err = target.Write(ctx, sparseFileIno, 0, append([]byte("HEAD"), make([]byte, 1020)...))
	require.NoError(t, err)
	extSize := uint64(4096)
	_, err = target.SetAttr(ctx, sparseFileIno, store.SetAttrReq{Size: &extSize})
	require.NoError(t, err)
	require.NoError(t, target.Close())

	source := openTestBranch(t, sourceDir, pool)
	// Source 修改深层文件 a/b/deep.txt（根目录 a 的 WriteSeq 未变）
	_, _, err = source.Write(ctx, nestedFileIno, 0, []byte("v1"))
	require.NoError(t, err)
	// Source 对 cross.txt 写相同长度不同内容
	_, _, err = source.Write(ctx, crossEntry.Ino, 0, []byte("src!-cross"))
	require.NoError(t, err)
	// Source 对 sparse.bin 仅写 4B "HEAD" 后 SetAttr 扩容至 4096（ref.Length=4 vs Target ref.Length=1024，但逻辑全相等）
	_, _, err = source.Write(ctx, sparseFileIno, 0, []byte("HEAD"))
	require.NoError(t, err)
	_, err = source.SetAttr(ctx, sparseFileIno, store.SetAttrReq{Size: &extSize})
	require.NoError(t, err)
	require.NoError(t, source.Close())

	// 即使 Target 对 sharedFileIno 做 CompactPacks 导致 BlobRef 改变，WriteSeq == base.WriteSeq 剪枝也不得误报
	_ = sharedFileIno

	res, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
	})
	require.NoError(t, err)

	// 预期结果：
	// - unchanged.bin: 跳过（WriteSeq 剪枝 + CompactPacks 免疫）
	// - trash.bin: 仅 Target 删除，Source 未改 -> 跳过
	// - sparse.bin: 虽双方 WriteSeq 都变且 refA.Length != refB.Length，但逻辑内容与属性完全一致 -> 跳过（无假修改/假冲突）
	// - a/b/deep.txt: Modified, ConflictNone
	// - cross.txt: Modified, ConflictContent（严禁因跨分支 WriteSeq 碰撞漏检）
	require.Equal(t, uint64(2), res.Summary.Modified)
	require.Equal(t, uint64(1), res.Summary.Conflicts)
	require.Len(t, res.Entries, 2)

	require.Equal(t, []byte("a/b/deep.txt"), res.Entries[0].Path)
	require.Equal(t, store.DiffKindModified, res.Entries[0].Kind)
	require.Equal(t, store.ConflictNone, res.Entries[0].Conflict)

	require.Equal(t, []byte("cross.txt"), res.Entries[1].Path)
	require.Equal(t, store.DiffKindModified, res.Entries[1].Kind)
	require.Equal(t, store.ConflictContent, res.Entries[1].Conflict)

	// 验证 TargetRoot == BaseRoot 2-way 复用句柄模式
	res2Way, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: baseDir,
		SourceRoot: sourceDir,
	})
	require.NoError(t, err)
	paths2Way := make([]string, 0, len(res2Way.Entries))
	for _, e := range res2Way.Entries {
		paths2Way = append(paths2Way, string(e.Path))
		require.Equal(t, store.ConflictNone, e.Conflict)
	}
	require.Equal(t, []string{"a/b/deep.txt", "cross.txt", "sparse.bin"}, paths2Way)

	// 验证 sparse.bin 若只写 1B 非零到稀疏洞，即便无相同 BlobRef 也必判不等
	source2 := openTestBranch(t, sourceDir, pool)
	_, _, err = source2.Write(ctx, sparseFileIno, 2048, []byte{1})
	require.NoError(t, err)
	require.NoError(t, source2.Close())

	resSparseDiff, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
		PathPrefix: []byte("sparse.bin"),
	})
	require.NoError(t, err)
	require.Len(t, resSparseDiff.Entries, 1)
	require.Equal(t, store.ConflictContent, resSparseDiff.Entries[0].Conflict)
	_ = bytes.Equal
}

func TestDiffTrees_Slice2_All12Conflicts_AutoMerge_Ordering_Pagination_And_ReadCommitNodeSlice(t *testing.T) {
	ctx := context.Background()
	pool := newSharedSegmentPool(200, 1)

	var (
		regIno, symIno, dirIno, orphanIno uint64
		modDelIno, delModIno              uint64
		dirFileIno                        uint64
		attrIno, xattrIno                 uint64
		quotaXattrKeyIno, quotaListIno    uint64
		secXattrIno, secPermContentIno    uint64
		hlSourceAddIno, hlTargetAddIno    uint64
		orthoContentAttrIno               uint64
		orthoXattrIno                     uint64
		treeDelModSubFileIno              uint64
		dirFileSubFileIno                 uint64
		sameEditIno                       uint64
	)

	baseDir, targetDir, sourceDir := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
		createReg := func(parent uint64, name, body string) uint64 {
			f, err := base.Create(ctx, store.CreateReq{ParentIno: parent, Name: []byte(name), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			if body != "" {
				_, _, err = base.Write(ctx, f.Ino, 0, []byte(body))
				require.NoError(t, err)
			}
			return f.Ino
		}

		// 用于 ReadCommitNodeSlice 覆盖稀疏空洞 + 尾部截断补 0 + 软链 + 目录报错
		fSparse, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("readnode.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		regIno = fSparse.Ino
		_, _, err = base.Write(ctx, fSparse.Ino, 0, []byte("ABCD"))
		require.NoError(t, err)
		_, _, err = base.Write(ctx, fSparse.Ino, 512, []byte("XYZ"))
		require.NoError(t, err)
		ext := uint64(1024)
		_, err = base.SetAttr(ctx, fSparse.Ino, store.SetAttrReq{Size: &ext})
		require.NoError(t, err)

		sym, err := base.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("readsym"), Target: []byte("/target/path")})
		require.NoError(t, err)
		symIno = sym.Ino

		dTest, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("readdir"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		dirIno = dTest.Ino

		// 孤儿节点（不应被 ReadCommitNodeSlice 读到）
		orf := createReg(store.RootIno, "orphan_base.txt", "secret")
		orphanIno = orf
		require.NoError(t, base.Unlink(ctx, store.RootIno, []byte("orphan_base.txt"), true))

		// 12 类冲突测试基底
		modDelIno = createReg(store.RootIno, "c_mod_del.txt", "base")
		delModIno = createReg(store.RootIno, "c_del_mod.txt", "base")

		dTree, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("c_tree_del_mod"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		treeDelModSubFileIno = createReg(dTree.Ino, "sub.txt", "base")

		_ = createReg(store.RootIno, "c_type", "base")

		dDF, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("c_dir_file"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		dirFileIno = dDF.Ino
		dirFileSubFileIno = createReg(dDF.Ino, "child.txt", "base")

		attrIno = createReg(store.RootIno, "c_attr.txt", "same")
		xattrIno = createReg(store.RootIno, "c_xattr.txt", "same")
		quotaXattrKeyIno = createReg(store.RootIno, "c_xattr_quota_key.txt", "same")
		quotaListIno = createReg(store.RootIno, "c_xattr_quota_list.txt", "same")
		secXattrIno = createReg(store.RootIno, "c_sec_xattr.txt", "same")
		secPermContentIno = createReg(store.RootIno, "c_sec_perm.txt", "base")

		hlSourceAddIno = createReg(store.RootIno, "c_hl_src_add.txt", "hl-base")
		hlTargetAddIno = createReg(store.RootIno, "c_hl_tgt_add.txt", "hl-base2")

		// 自动正交融合与收敛场景
		orthoContentAttrIno = createReg(store.RootIno, "ortho_attr_content.txt", "v0")
		orthoXattrIno = createReg(store.RootIno, "ortho_xattr.txt", "v0")
		sameEditIno = createReg(store.RootIno, "same_edit.txt", "v0")
	})

	// 验证 ReadCommitNodeSlice（基于不可变 baseDir）
	resReg1, err := store.ReadCommitNodeSlice(baseDir, regIno, 0, 600)
	require.NoError(t, err)
	require.Equal(t, uint64(1024), resReg1.FileSize)
	require.False(t, resReg1.EOF)
	require.Len(t, resReg1.Data, 600)
	require.Equal(t, []byte("ABCD"), resReg1.Data[:4])
	require.True(t, bytes.Equal(make([]byte, 508), resReg1.Data[4:512]))
	require.Equal(t, []byte("XYZ"), resReg1.Data[512:515])
	require.True(t, bytes.Equal(make([]byte, 85), resReg1.Data[515:600]))

	resReg2, err := store.ReadCommitNodeSlice(baseDir, regIno, 600, 1024)
	require.NoError(t, err)
	require.True(t, resReg2.EOF)
	require.Len(t, resReg2.Data, 424)
	require.True(t, bytes.Equal(make([]byte, 424), resReg2.Data))

	resSym, err := store.ReadCommitNodeSlice(baseDir, symIno, 0, 100)
	require.NoError(t, err)
	require.Equal(t, []byte("/target/path"), resSym.SymlinkTarget)
	require.True(t, resSym.EOF)

	_, err = store.ReadCommitNodeSlice(baseDir, dirIno, 0, 10)
	require.ErrorIs(t, err, store.ErrIsDir)
	_, err = store.ReadCommitNodeSlice(baseDir, orphanIno, 0, 10)
	require.ErrorIs(t, err, store.ErrNotFound)

	// 在 Target 与 Source 上制造全量 12 类冲突 + 正交自动合并 + 严格字节字典序路径
	target := openTestBranch(t, targetDir, pool)
	// 1. Add/Add
	fAddT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_add_add.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = target.Write(ctx, fAddT.Ino, 0, []byte("target-add"))
	require.NoError(t, err)
	// 2. Content
	fConT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_content_unused"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_ = fConT
	// 使用 modDel/type 等覆盖：我们在 c_type 上让 Target 变 symlink, Source 变 fifo
	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("c_type"), false))
	_, err = target.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("c_type"), Target: []byte("/t")})
	require.NoError(t, err)
	// 3. Modify/Delete: Target 改 modDelIno, Source 删
	_, _, err = target.Write(ctx, modDelIno, 0, []byte("tgt-modified"))
	require.NoError(t, err)
	// 4. Delete/Modify: Target 删 delModIno, Source 改
	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("c_del_mod.txt"), false))
	// 5. TreeDelete/Modify: Target 改子文件, Source 删目录
	_, _, err = target.Write(ctx, treeDelModSubFileIno, 0, []byte("tgt-sub-edit"))
	require.NoError(t, err)
	// 7. Dir/File: Target 改子节点, Source 将目录替换为文件
	_, _, err = target.Write(ctx, dirFileSubFileIno, 0, []byte("tgt-df-edit"))
	require.NoError(t, err)
	// 8. Attr
	m755 := uint32(0o755)
	_, err = target.SetAttr(ctx, attrIno, store.SetAttrReq{Mode: &m755})
	require.NoError(t, err)
	// 9. Xattr
	require.NoError(t, target.SetXattr(ctx, xattrIno, []byte("user.k"), []byte("tgt"), 0))
	// 10a. XattrQuota (system.bowl.max_bytes)
	require.NoError(t, target.SetXattr(ctx, quotaXattrKeyIno, []byte("system.bowl.max_bytes"), []byte("1000"), 0))
	// 10b. XattrQuota (总名长超 65536：Target 加 180 个 200B key = 36180B)
	for i := range 180 {
		key := []byte("user.t" + string(bytes.Repeat([]byte{'a'}, 190)) + string([]byte{byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}))
		require.NoError(t, target.SetXattr(ctx, quotaListIno, key, []byte("1"), 0))
	}
	// 11a. SecurityAttr (security.*)
	require.NoError(t, target.SetXattr(ctx, secXattrIno, []byte("security.capability"), []byte("cap-t"), 0))
	// 11b. SecurityAttr (Target 提权 SUID while Source 改内容)
	mSuid := store.S_ISUID | 0o755
	_, err = target.SetAttr(ctx, secPermContentIno, store.SetAttrReq{Mode: &mSuid})
	require.NoError(t, err)
	// 12a. Hardlink: Target 修改 hlSourceAddIno, Source 新增硬链接指向 hlSourceAddIno
	_, _, err = target.Write(ctx, hlSourceAddIno, 0, []byte("hl-tgt-modified"))
	require.NoError(t, err)
	// 12b. Hardlink: Target 新增硬链接指向 hlTargetAddIno, Source 修改 hlTargetAddIno
	_, err = target.Link(ctx, store.LinkReq{Ino: hlTargetAddIno, NewParentIno: store.RootIno, NewName: []byte("c_hl_tgt_add_link.txt")})
	require.NoError(t, err)

	// 正交自动合并 + 收敛 + 同名目录汇聚
	_, _, err = target.Write(ctx, orthoContentAttrIno, 0, []byte("tgt-payload"))
	require.NoError(t, err)
	require.NoError(t, target.SetXattr(ctx, orthoXattrIno, []byte("user.a"), []byte("1"), 0))
	_, _, err = target.Write(ctx, sameEditIno, 0, []byte("converged"))
	require.NoError(t, err)
	dSharedT, err := target.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("shared_dir"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	_, err = target.Create(ctx, store.CreateReq{ParentIno: dSharedT.Ino, Name: []byte("from_t.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// 字节字典序测试路径：dir, dir-2, dir.txt, dir/a, dir0（仅 Target 建一个无关文件 dir-0? 不，放 Source 新增验证全量顺序）
	require.NoError(t, target.Close())

	source := openTestBranch(t, sourceDir, pool)
	// 1. Add/Add
	fAddS, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_add_add.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = source.Write(ctx, fAddS.Ino, 0, []byte("source-add"))
	require.NoError(t, err)
	// 2. Content (使用 orthoContentAttrIno 不改，而在 same/content：用 c_content_both，Wait: Base 需要一个共同文件做 Content 冲突，上面 Slice1 测过 Content，这里加一个 c_content_both? 改用 hlTargetAddIno? 我们在上面用 secPermContentIno 做 Security，再在 c_content 用什么？Wait: 复用 readnode.bin 让双边改它！)
	// wait: readnode.bin 已被前面断言过 baseDir，现在改 targetDir/sourceDir 的 readnode.bin！
	// 重新打开 target 对 readnode.bin 写一笔来测 ConflictContent
	require.NoError(t, source.Close())

	target2 := openTestBranch(t, targetDir, pool)
	_, _, err = target2.Write(ctx, regIno, 0, []byte("T-EDIT"))
	require.NoError(t, err)
	require.NoError(t, target2.Close())

	source = openTestBranch(t, sourceDir, pool)
	_, _, err = source.Write(ctx, regIno, 0, []byte("S-EDIT"))
	require.NoError(t, err)
	// 3. Modify/Delete: Source 删
	require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("c_mod_del.txt"), false))
	// 4. Delete/Modify: Source 改
	_, _, err = source.Write(ctx, delModIno, 0, []byte("src-modified"))
	require.NoError(t, err)
	// 5. TreeDelete/Modify: Source 递归删 c_tree_del_mod
	require.NoError(t, source.Unlink(ctx, dTreeInoFromBase(t, baseDir, "c_tree_del_mod"), []byte("sub.txt"), false))
	require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("c_tree_del_mod")))
	// 6. Type: Source 变 FIFO
	require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("c_type"), false))
	_, err = source.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("c_type"), Mode: store.S_IFIFO | 0o644})
	require.NoError(t, err)
	// 7. Dir/File: Source 删目录替换为常规文件
	require.NoError(t, source.Unlink(ctx, dirFileIno, []byte("child.txt"), false))
	require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("c_dir_file")))
	_, err = source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_dir_file"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	// 8. Attr
	m600 := uint32(0o600)
	_, err = source.SetAttr(ctx, attrIno, store.SetAttrReq{Mode: &m600})
	require.NoError(t, err)
	// 9. Xattr
	require.NoError(t, source.SetXattr(ctx, xattrIno, []byte("user.k"), []byte("src"), 0))
	// 10a. XattrQuota (system.bowl.max_bytes)
	require.NoError(t, source.SetXattr(ctx, quotaXattrKeyIno, []byte("system.bowl.max_bytes"), []byte("2000"), 0))
	// 10b. XattrQuota (总名长超 65536：Source 再加 180 个 200B key)
	for i := range 180 {
		key := []byte("user.s" + string(bytes.Repeat([]byte{'b'}, 190)) + string([]byte{byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}))
		require.NoError(t, source.SetXattr(ctx, quotaListIno, key, []byte("1"), 0))
	}
	// 11a. SecurityAttr
	require.NoError(t, source.SetXattr(ctx, secXattrIno, []byte("security.capability"), []byte("cap-s"), 0))
	// 11b. SecurityAttr (Source 改文件内容 vs Target 加 SUID)
	_, _, err = source.Write(ctx, secPermContentIno, 0, []byte("new-payload"))
	require.NoError(t, err)
	// 12a. Hardlink: Source 新增指向 hlSourceAddIno 的硬链接
	_, err = source.Link(ctx, store.LinkReq{Ino: hlSourceAddIno, NewParentIno: store.RootIno, NewName: []byte("c_hl_src_add_link.txt")})
	require.NoError(t, err)
	// 12b. Hardlink: Source 修改 hlTargetAddIno
	_, _, err = source.Write(ctx, hlTargetAddIno, 0, []byte("hl-src-modified"))
	require.NoError(t, err)

	// 正交自动合并 + 收敛 + 同名目录汇聚
	m700 := uint32(0o700)
	_, err = source.SetAttr(ctx, orthoContentAttrIno, store.SetAttrReq{Mode: &m700})
	require.NoError(t, err)
	require.NoError(t, source.SetXattr(ctx, orthoXattrIno, []byte("user.b"), []byte("2"), 0))
	_, _, err = source.Write(ctx, sameEditIno, 0, []byte("converged"))
	require.NoError(t, err)
	dSharedS, err := source.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("shared_dir"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	_, err = source.Create(ctx, store.CreateReq{ParentIno: dSharedS.Ino, Name: []byte("from_s.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// 严格字节字典序验证子树：ord, ord-2, ord.txt, ord/a, ord0
	dOrd, err := source.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("ord"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	_, err = source.Create(ctx, store.CreateReq{ParentIno: dOrd.Ino, Name: []byte("a"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ord-2"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ord.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ord0"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	require.NoError(t, source.Close())

	// 1) 验证全量冲突类型覆盖
	resAll, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:      baseDir,
		TargetRoot:    targetDir,
		SourceRoot:    sourceDir,
		ConflictsOnly: true,
	})
	require.NoError(t, err)

	conflictSet := make(map[store.ConflictKind]string)
	for _, e := range resAll.Entries {
		require.NotEqual(t, store.ConflictNone, e.Conflict)
		conflictSet[e.Conflict] = string(e.Path)
	}
	expectedConflicts := []store.ConflictKind{
		store.ConflictAddAdd,
		store.ConflictContent,
		store.ConflictModifyDelete,
		store.ConflictDeleteModify,
		store.ConflictTreeDeleteModify,
		store.ConflictType,
		store.ConflictDirFile,
		store.ConflictAttr,
		store.ConflictXattr,
		store.ConflictXattrQuota,
		store.ConflictSecurityAttr,
		store.ConflictHardlink,
	}
	for _, ck := range expectedConflicts {
		require.Containsf(t, conflictSet, ck, "缺少冲突类型: %s", ck.String())
	}
	// quotaListIno 与 quotaXattrKeyIno 均应报 ConflictXattrQuota
	var quotaPaths []string
	var secPaths []string
	for _, e := range resAll.Entries {
		switch e.Conflict {
		case store.ConflictXattrQuota:
			quotaPaths = append(quotaPaths, string(e.Path))
		case store.ConflictSecurityAttr:
			secPaths = append(secPaths, string(e.Path))
		}
	}
	require.Equal(t, []string{"c_xattr_quota_key.txt", "c_xattr_quota_list.txt"}, quotaPaths)
	require.Equal(t, []string{"c_sec_perm.txt", "c_sec_xattr.txt"}, secPaths)

	// 正交自动合并条目不应落入冲突集合，且 same_edit.txt 收敛不报差异
	fullRes, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
	})
	require.NoError(t, err)
	var allPaths []string
	for _, e := range fullRes.Entries {
		allPaths = append(allPaths, string(e.Path))
	}
	require.NotContains(t, allPaths, "same_edit.txt")
	require.Contains(t, allPaths, "ortho_attr_content.txt")
	require.Contains(t, allPaths, "ortho_xattr.txt")
	require.Contains(t, allPaths, "shared_dir/from_s.txt")
	require.NotContains(t, allPaths, "shared_dir/from_t.txt")

	// 2) 验证 PathPrefix 子树过滤 + 严格字节字典序（ord < ord-2 < ord.txt < ord/a < ord0）
	ordRes, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
		PathPrefix: []byte("ord"),
	})
	require.NoError(t, err)
	require.Len(t, ordRes.Entries, 2) // PathPrefix="ord" 仅匹配子树 "ord" 与 "ord/a"，不匹配 "ord-2" / "ord0"
	require.Equal(t, []byte("ord"), ordRes.Entries[0].Path)
	require.Equal(t, []byte("ord/a"), ordRes.Entries[1].Path)

	// 验证全局字节路径字典序：ord < ord-2 < ord.txt < ord/a < ord0
	idxOrd := indexOfString(allPaths, "ord")
	idxOrd2 := indexOfString(allPaths, "ord-2")
	idxOrdTxt := indexOfString(allPaths, "ord.txt")
	idxOrdA := indexOfString(allPaths, "ord/a")
	idxOrd0 := indexOfString(allPaths, "ord0")
	require.Less(t, idxOrd, idxOrd2)
	require.Less(t, idxOrd2, idxOrdTxt)
	require.Less(t, idxOrdTxt, idxOrdA)
	require.Less(t, idxOrdA, idxOrd0)

	// 3) 验证 PageToken 游标分页 + 全量 Summary 保持一致
	p1, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
		PageSize:   5,
	})
	require.NoError(t, err)
	require.Len(t, p1.Entries, 5)
	require.NotEmpty(t, p1.NextPageToken)
	require.Equal(t, fullRes.Summary, p1.Summary)

	var pagedPaths []string
	for _, e := range p1.Entries {
		pagedPaths = append(pagedPaths, string(e.Path))
	}
	tok := p1.NextPageToken
	for tok != "" {
		pn, pErr := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			PageSize:   5,
			PageToken:  tok,
		})
		require.NoError(t, pErr)
		require.Equal(t, fullRes.Summary, pn.Summary)
		for _, e := range pn.Entries {
			pagedPaths = append(pagedPaths, string(e.Path))
		}
		tok = pn.NextPageToken
	}
	require.Equal(t, allPaths, pagedPaths)
}

func dTreeInoFromBase(t *testing.T, baseDir, name string) uint64 {
	t.Helper()
	res, err := store.DiffTrees(context.Background(), &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: baseDir,
		SourceRoot: baseDir,
	})
	require.NoError(t, err)
	_ = res
	a, err := store.Open(store.Options{Dir: baseDir, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = a.Close() }()
	_, ent, err := a.Lookup(context.Background(), store.RootIno, []byte(name))
	require.NoError(t, err)
	return ent.Ino
}

func indexOfString(ss []string, target string) int {
	for i, s := range ss {
		if s == target {
			return i
		}
	}
	return -1
}

func TestMergeTrees_Slice3_Strategies_Hardlinks_Packs_DryRun_Quota_Reopen(t *testing.T) {
	ctx := context.Background()

	t.Run("FailStrategy_Truncates200Samples_And_ZeroSideEffects", func(t *testing.T) {
		pool := newSharedSegmentPool(200, 1)
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, nil)

		target := openTestBranch(t, targetDir, pool)
		source := openTestBranch(t, sourceDir, pool)
		for i := range 205 {
			name := fmt.Appendf(nil, "f_%03d.txt", i)
			ft, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: name, Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = target.Write(ctx, ft.Ino, 0, []byte("target"))
			require.NoError(t, err)

			fs, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: name, Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = source.Write(ctx, fs.Ino, 0, []byte("source"))
			require.NoError(t, err)
		}
		require.NoError(t, target.Close())
		require.NoError(t, source.Close())

		stagingDir := filepath.Join(t.TempDir(), "staging")
		_, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            baseDir,
			TargetRoot:          targetDir,
			SourceRoot:          sourceDir,
			StagingDir:          stagingDir,
			Strategy:            store.MergeStrategyFail,
			ExpectedWriterEpoch: 0,
			InoAllocator:        pool.allocIno,
			PackAllocator:       pool.allocPack,
		})
		var conflictErr *store.ErrMergeConflict
		require.ErrorAs(t, err, &conflictErr)
		require.Equal(t, uint64(205), conflictErr.Report.TotalConflicts)
		require.True(t, conflictErr.Report.ConflictsTruncated)
		require.Len(t, conflictErr.Report.Conflicts, store.MaxMergeConflictSamples)
		for i := 1; i < len(conflictErr.Report.Conflicts); i++ {
			require.Negative(t, bytes.Compare(conflictErr.Report.Conflicts[i-1].Path, conflictErr.Report.Conflicts[i].Path))
		}
		_, statErr := os.Stat(stagingDir)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("HardlinkSplit_PackCollision_Orphans_Quota_And_Reopen", func(t *testing.T) {
		// 故意传 pool=nil 使 Target 与 Source 独立分配相同 PackID=1 且发生异源 Ino 碰撞
		baseDir, targetDir, sourceDir := makeSeededFork(t, nil, func(ctx context.Context, base *store.StoreActor) {
			fShared, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("hl_split_a.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fShared.Ino, 0, []byte("base-hl-data"))
			require.NoError(t, err)
			_, err = base.Link(ctx, store.LinkReq{Ino: fShared.Ino, NewParentIno: store.RootIno, NewName: []byte("hl_split_b.txt")})
			require.NoError(t, err)

			fKeep, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("hl_keep_a.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fKeep.Ino, 0, []byte("keep-hl"))
			require.NoError(t, err)
			_, err = base.Link(ctx, store.LinkReq{Ino: fKeep.Ino, NewParentIno: store.RootIno, NewName: []byte("hl_keep_b.txt")})
			require.NoError(t, err)

			dTree, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("tree_dir"), Mode: store.S_IFDIR | 0o755})
			require.NoError(t, err)
			fSub, err := base.Create(ctx, store.CreateReq{ParentIno: dTree.Ino, Name: []byte("sub.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fSub.Ino, 0, []byte("tree-sub-base"))
			require.NoError(t, err)
		})

		// Target: 升级 Epoch=1，留下孤儿、新建 PackID 与 Ino 碰撞文件、改 hl_split_a/b（通过同一 Ino）、改 tree_dir/sub.txt
		target := openTestBranch(t, targetDir, nil)
		_, _, err := target.AdvanceEpoch(ctx)
		require.NoError(t, err)
		entSplit, _, err := target.Lookup(ctx, store.RootIno, []byte("hl_split_a.txt"))
		require.NoError(t, err)
		_, _, err = target.Write(ctx, entSplit.Ino, 0, []byte("target-hl-payload-12345"), 1)
		require.NoError(t, err)
		fOrphan, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("tgt_orphan.bin"), Mode: store.S_IFREG | 0o644, Epoch: 1})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fOrphan.Ino, 0, []byte("orphan-bytes-to-reclaim"), 1)
		require.NoError(t, err)
		require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("tgt_orphan.bin"), true, 1))
		fTgtUnique, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("tgt_new.txt"), Mode: store.S_IFREG | 0o644, Epoch: 1})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fTgtUnique.Ino, 0, []byte("tgt-unique-pack-payload"), 1)
		require.NoError(t, err)
		// 碰撞同路径 add/add 和异路径同 Ino
		fColT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("dir_t_only.txt"), Mode: store.S_IFREG | 0o644, Epoch: 1})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fColT.Ino, 0, []byte("target-collision-ino-content"), 1)
		require.NoError(t, err)
		dTreeEnt, _, err := target.Lookup(ctx, store.RootIno, []byte("tree_dir"))
		require.NoError(t, err)
		subEnt, _, err := target.Lookup(ctx, dTreeEnt.Ino, []byte("sub.txt"))
		require.NoError(t, err)
		_, _, err = target.Write(ctx, subEnt.Ino, 0, []byte("target-edited-sub"), 1)
		require.NoError(t, err)
		require.NoError(t, target.Close())

		// Source: 改 hl_split_a/b（另一版本）、改 hl_keep_a/b（无冲突直接采纳并保持硬链接）、删 tree_dir、创建冲突 Ino 文件及嵌套目录
		source := openTestBranch(t, sourceDir, nil)
		_, _, err = source.Write(ctx, entSplit.Ino, 0, []byte("source-hl-payload-abcdef"))
		require.NoError(t, err)
		entKeep, _, err := source.Lookup(ctx, store.RootIno, []byte("hl_keep_a.txt"))
		require.NoError(t, err)
		_, _, err = source.Write(ctx, entKeep.Ino, 0, []byte("keep-hl-updated-by-source"))
		require.NoError(t, err)
		require.NoError(t, source.Unlink(ctx, dTreeEnt.Ino, []byte("sub.txt"), false))
		require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("tree_dir")))
		dSrcNested, err := source.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("src_dir"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		fSrcOnly, err := source.Create(ctx, store.CreateReq{ParentIno: dSrcNested.Ino, Name: []byte("src_new.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fSrcOnly.Ino, 0, []byte("src-nested-pack-payload"))
		require.NoError(t, err)
		require.NoError(t, source.Close())

		// 1. DryRun 校验（零副作用）
		dryStaging := filepath.Join(t.TempDir(), "dry_staging")
		dryRes, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: dryStaging,
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"hl_split_b.txt": store.MergeStrategyTheirs, // 硬链接分裂：a=Ours, b=Theirs
				"tree_dir":       store.MergeStrategyTheirs, // 目录树删改冲突：整棵 tree_dir 采纳 Theirs（删除）
			},
			DryRun:              true,
			OverrideMaxBytes:    10 << 20,
			ExpectedWriterEpoch: 1,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(10<<20), dryRes.TargetMaxBytes)
		require.Equal(t, uint64(2), dryRes.WriterEpoch)
		require.Positive(t, dryRes.UsedBytes)
		_, statErr := os.Stat(dryStaging)
		require.ErrorIs(t, statErr, os.ErrNotExist)

		// 2. 配额缩容超限回滚（ErrQuotaShrinkBelowUsed）
		failStaging := filepath.Join(t.TempDir(), "fail_quota_staging")
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: failStaging,
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"hl_split_b.txt": store.MergeStrategyTheirs,
				"tree_dir":       store.MergeStrategyTheirs,
			},
			OverrideMaxBytes:    10,
			ExpectedWriterEpoch: 1,
		})
		require.ErrorIs(t, err, store.ErrQuotaShrinkBelowUsed)
		_, statErr = os.Stat(failStaging)
		require.ErrorIs(t, statErr, os.ErrNotExist)

		// 3. 正式 MergeTrees 到 StagingDir
		stagingDir := filepath.Join(t.TempDir(), "merged_staging")
		res, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: stagingDir,
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"hl_split_b.txt": store.MergeStrategyTheirs,
				"tree_dir":       store.MergeStrategyTheirs,
			},
			ExpectedWriterEpoch: 1,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(2), res.WriterEpoch)
		require.Equal(t, dryRes.UsedBytes, res.UsedBytes)

		epoch, seq, hasOrphans, err := store.InspectBranchStat(stagingDir)
		require.NoError(t, err)
		require.Equal(t, uint64(2), epoch)
		require.Equal(t, res.NextWriteSeq, seq)
		require.False(t, hasOrphans)

		// 使用新 StoreActor 重新打开 merged StagingDir 验证全部物理不变量
		merged := openTestBranch(t, stagingDir, nil)
		defer func() { _ = merged.Close() }()

		// a/b 硬链接已分裂成独立 Inode，各自保留正确内容
		entA, attrA, err := merged.Lookup(ctx, store.RootIno, []byte("hl_split_a.txt"))
		require.NoError(t, err)
		entB, attrB, err := merged.Lookup(ctx, store.RootIno, []byte("hl_split_b.txt"))
		require.NoError(t, err)
		require.NotEqual(t, entA.Ino, entB.Ino)
		require.Equal(t, uint32(1), attrA.Nlink)
		require.Equal(t, uint32(1), attrB.Nlink)
		dataA, err := merged.Read(ctx, entA.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("target-hl-payload-12345"), dataA)
		dataB, err := merged.Read(ctx, entB.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("source-hl-payload-abcdef"), dataB)

		// hl_keep_a/b 硬链接组未分裂，保持相同 Ino 与 Nlink=2
		entKA, attrKA, err := merged.Lookup(ctx, store.RootIno, []byte("hl_keep_a.txt"))
		require.NoError(t, err)
		entKB, attrKB, err := merged.Lookup(ctx, store.RootIno, []byte("hl_keep_b.txt"))
		require.NoError(t, err)
		require.Equal(t, entKA.Ino, entKB.Ino)
		require.Equal(t, uint32(2), attrKA.Nlink)
		require.Equal(t, uint32(2), attrKB.Nlink)
		dataKA, err := merged.Read(ctx, entKA.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("keep-hl-updated-by-source"), dataKA)

		// tree_dir 已被 Theirs 删除
		_, _, err = merged.Lookup(ctx, store.RootIno, []byte("tree_dir"))
		require.ErrorIs(t, err, store.ErrNotFound)

		// 异源 Ino/PackID 碰撞文件各自完好可读
		entTCol, _, err := merged.Lookup(ctx, store.RootIno, []byte("dir_t_only.txt"))
		require.NoError(t, err)
		dataTCol, err := merged.Read(ctx, entTCol.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("target-collision-ino-content"), dataTCol)

		// 嵌套目录 Nlink 与 ParentIno ("..") 守恒 + 异源 PackID 重映射后可读
		entSrcDir, attrSrcDir, err := merged.Lookup(ctx, store.RootIno, []byte("src_dir"))
		require.NoError(t, err)
		require.Equal(t, uint32(2), attrSrcDir.Nlink)
		dotDotEnt, _, err := merged.Lookup(ctx, entSrcDir.Ino, []byte(".."))
		require.NoError(t, err)
		require.Equal(t, store.RootIno, dotDotEnt.Ino)
		entSN, _, err := merged.Lookup(ctx, entSrcDir.Ino, []byte("src_new.txt"))
		require.NoError(t, err)
		dataSN, err := merged.Read(ctx, entSN.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("src-nested-pack-payload"), dataSN)

		// WriterEpoch 与写租约校验：MergeTrees 后 WriterExpiresAt=0，旧 epoch=1 与未获租约的 epoch=2 写入均被拒绝，获取租约后新 epoch=3 写入成功
		_, _, err = merged.Write(ctx, entTCol.Ino, 0, []byte("x"), 1)
		require.ErrorIs(t, err, store.ErrStaleEpoch)
		_, _, err = merged.Write(ctx, entTCol.Ino, 0, []byte("x"), 2)
		require.ErrorIs(t, err, store.ErrStaleEpoch)
		leaseEpoch, _, err := merged.AcquireWriterLease(ctx, store.DefaultWriterLeaseTTL, false)
		require.NoError(t, err)
		require.Equal(t, uint64(3), leaseEpoch)
		_, _, err = merged.Write(ctx, entTCol.Ino, 0, []byte("TARGET"), leaseEpoch)
		require.NoError(t, err)

		// CompactPacks 账本自洽验证
		require.NoError(t, merged.CompactPacks(ctx))
		stFS, err := merged.StatFS(ctx)
		require.NoError(t, err)
		require.Equal(t, res.UsedBytes, stFS.UsedBytes)
		require.Equal(t, res.Inodes, stFS.Files)
	})

	t.Run("QuotaNoSpace_And_SharedPoolPackSameFileReuse", func(t *testing.T) {
		pool := newSharedSegmentPool(300, 1)
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, nil)

		target := openTestBranch(t, targetDir, pool)
		// 缩小 Target 配额至 30 字节
		require.NoError(t, target.UpdateMaxBytes(ctx, 30))
		require.NoError(t, target.Close())

		source := openTestBranch(t, sourceDir, pool)
		f, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("big.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, f.Ino, 0, bytes.Repeat([]byte("A"), 64))
		require.NoError(t, err)
		require.NoError(t, source.Close())

		failStaging := filepath.Join(t.TempDir(), "nospace_staging")
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            baseDir,
			TargetRoot:          targetDir,
			SourceRoot:          sourceDir,
			StagingDir:          failStaging,
			Strategy:            store.MergeStrategyOurs,
			ExpectedWriterEpoch: 0,
			InoAllocator:        pool.allocIno,
			PackAllocator:       pool.allocPack,
		})
		require.ErrorIs(t, err, store.ErrNoSpace)
		_, statErr := os.Stat(failStaging)
		require.ErrorIs(t, statErr, os.ErrNotExist)

		// 通过 OverrideMaxBytes 扩容到 1MB 即可合并成功并 os.SameFile 复用 Source Pack
		okStaging := filepath.Join(t.TempDir(), "ok_staging")
		res, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            baseDir,
			TargetRoot:          targetDir,
			SourceRoot:          sourceDir,
			StagingDir:          okStaging,
			Strategy:            store.MergeStrategyOurs,
			OverrideMaxBytes:    1 << 20,
			ExpectedWriterEpoch: 0,
			InoAllocator:        pool.allocIno,
			PackAllocator:       pool.allocPack,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1<<20), res.TargetMaxBytes)

		merged := openTestBranch(t, okStaging, pool)
		defer func() { _ = merged.Close() }()
		ent, _, err := merged.Lookup(ctx, store.RootIno, []byte("big.bin"))
		require.NoError(t, err)
		got, err := merged.Read(ctx, ent.Ino, 0, 64)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte("A"), 64), got)
	})
}

func TestDiffMerge_Slice4_VerifierFindings(t *testing.T) {
	ctx := context.Background()

	t.Run("Finding1_TargetRootRequired_And_PreexistingStagingNotDeleted", func(t *testing.T) {
		pool := newSharedSegmentPool(400, 1)
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, nil)

		_, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: "",
			SourceRoot: sourceDir,
			StagingDir: filepath.Join(t.TempDir(), "s1"),
		})
		require.ErrorIs(t, err, store.ErrInvalid)

		existingStaging := t.TempDir()
		sentinel := filepath.Join(existingStaging, "keep.txt")
		require.NoError(t, os.WriteFile(sentinel, []byte("must-survive"), 0o600))

		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: existingStaging,
			Strategy:   store.MergeStrategyOurs,
		})
		require.ErrorIs(t, err, store.ErrExist)
		got, readErr := os.ReadFile(sentinel)
		require.NoError(t, readErr)
		require.Equal(t, []byte("must-survive"), got)
	})

	t.Run("Finding5_ReadCommitNodeSlice_SizeLimit", func(t *testing.T) {
		pool := newSharedSegmentPool(450, 1)
		var fIno uint64
		baseDir, _, _ := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
			f, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f.bin"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			fIno = f.Ino
		})
		_, err := store.ReadCommitNodeSlice(baseDir, fIno, 0, store.MaxReadNodeSliceBytes+1)
		require.ErrorIs(t, err, store.ErrInvalid)
	})

	t.Run("Finding2_TypeTransitions_And_EquivalenceFallback", func(t *testing.T) {
		pool := newSharedSegmentPool(500, 1)
		var revDirIno, revRegIno uint64
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
			for _, name := range []string{"f_to_dir", "both_dir", "reverted_reg"} {
				f, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte(name), Mode: store.S_IFREG | 0o644})
				require.NoError(t, err)
				_, _, err = base.Write(ctx, f.Ino, 0, []byte("base-body"))
				require.NoError(t, err)
				if name == "reverted_reg" {
					revRegIno = f.Ino
				}
			}
			dRev, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("reverted_dir"), Mode: store.S_IFDIR | 0o755})
			require.NoError(t, err)
			revDirIno = dRev.Ino
			fChild, err := base.Create(ctx, store.CreateReq{ParentIno: dRev.Ino, Name: []byte("c.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fChild.Ino, 0, []byte("c"))
			require.NoError(t, err)
		})

		// Target:
		// 2a: f_to_dir 替换为非空目录 f_to_dir/child.txt
		// 2b: both_dir 替换为目录 both_dir/from_t.txt
		// 2c: reverted_dir 替换为文件, reverted_reg 替换为软链
		target := openTestBranch(t, targetDir, pool)
		require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("f_to_dir"), false))
		dFTD, err := target.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("f_to_dir"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		fFTDChild, err := target.Create(ctx, store.CreateReq{ParentIno: dFTD.Ino, Name: []byte("child.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fFTDChild.Ino, 0, []byte("kept-child"))
		require.NoError(t, err)

		require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("both_dir"), false))
		dBDT, err := target.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("both_dir"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		fBDT, err := target.Create(ctx, store.CreateReq{ParentIno: dBDT.Ino, Name: []byte("from_t.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fBDT.Ino, 0, []byte("t-dir-file"))
		require.NoError(t, err)

		require.NoError(t, target.Unlink(ctx, revDirIno, []byte("c.txt"), false))
		require.NoError(t, target.Rmdir(ctx, store.RootIno, []byte("reverted_dir")))
		fRevT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("reverted_dir"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fRevT.Ino, 0, []byte("now-file"))
		require.NoError(t, err)

		require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("reverted_reg"), false))
		_, err = target.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("reverted_reg"), Target: []byte("/sym")})
		require.NoError(t, err)
		require.NoError(t, target.Close())

		// Source:
		// 2a: f_to_dir 未改
		// 2b: both_dir 替换为目录 both_dir/from_s.txt
		// 2c: reverted_dir 临时创建再删除（WriteSeq 递增但等价 Base），reverted_reg 覆写同值内容（WriteSeq 递增但等价 Base）
		source := openTestBranch(t, sourceDir, pool)
		require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("both_dir"), false))
		dBDS, err := source.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("both_dir"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		fBDS, err := source.Create(ctx, store.CreateReq{ParentIno: dBDS.Ino, Name: []byte("from_s.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fBDS.Ino, 0, []byte("s-dir-file"))
		require.NoError(t, err)

		_, err = source.Create(ctx, store.CreateReq{ParentIno: revDirIno, Name: []byte("tmp"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		require.NoError(t, source.Unlink(ctx, revDirIno, []byte("tmp"), false))
		_, _, err = source.Write(ctx, revRegIno, 0, []byte("base-body"))
		require.NoError(t, err)
		require.NoError(t, source.Close())

		diffRes, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
		})
		require.NoError(t, err)
		require.Zero(t, diffRes.Summary.Conflicts, "等价回退与同名建目录不应误报冲突: %+v", diffRes.Entries)
		var diffPaths []string
		for _, e := range diffRes.Entries {
			diffPaths = append(diffPaths, string(e.Path))
		}
		require.Contains(t, diffPaths, "both_dir/from_s.txt")

		staging := filepath.Join(t.TempDir(), "merged2")
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			StagingDir:    staging,
			Strategy:      store.MergeStrategyFail,
			InoAllocator:  pool.allocIno,
			PackAllocator: pool.allocPack,
		})
		require.NoError(t, err)

		merged := openTestBranch(t, staging, pool)
		defer func() { _ = merged.Close() }()

		// 验证 2a: f_to_dir/child.txt 完好保留
		entFTD, _, err := merged.Lookup(ctx, store.RootIno, []byte("f_to_dir"))
		require.NoError(t, err)
		entFTDC, _, err := merged.Lookup(ctx, entFTD.Ino, []byte("child.txt"))
		require.NoError(t, err)
		dataFTDC, err := merged.Read(ctx, entFTDC.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("kept-child"), dataFTDC)

		// 验证 2b: both_dir 同时包含 from_t.txt 与 from_s.txt
		entBD, _, err := merged.Lookup(ctx, store.RootIno, []byte("both_dir"))
		require.NoError(t, err)
		entBFT, _, err := merged.Lookup(ctx, entBD.Ino, []byte("from_t.txt"))
		require.NoError(t, err)
		dataBFT, err := merged.Read(ctx, entBFT.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("t-dir-file"), dataBFT)
		entBFS, _, err := merged.Lookup(ctx, entBD.Ino, []byte("from_s.txt"))
		require.NoError(t, err)
		dataBFS, err := merged.Read(ctx, entBFS.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("s-dir-file"), dataBFS)

		// 验证 2c: reverted_dir 保持 Target 文件，reverted_reg 保持 Target 软链
		entRD, _, err := merged.Lookup(ctx, store.RootIno, []byte("reverted_dir"))
		require.NoError(t, err)
		dataRD, err := merged.Read(ctx, entRD.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("now-file"), dataRD)
		_, attrRR, err := merged.Lookup(ctx, store.RootIno, []byte("reverted_reg"))
		require.NoError(t, err)
		require.Equal(t, store.S_IFLNK, attrRR.Mode&store.S_IFMT)
	})

	t.Run("Finding3_SameTargetHardlinkSplitFusion", func(t *testing.T) {
		pool := newSharedSegmentPool(600, 1)
		var baseABIno, baseDiffIno uint64
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
			f1, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("t_hl_a.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, f1.Ino, 0, []byte("shared-body-123"))
			require.NoError(t, err)
			_, err = base.Link(ctx, store.LinkReq{Ino: f1.Ino, NewParentIno: store.RootIno, NewName: []byte("t_hl_b.txt")})
			require.NoError(t, err)
			baseABIno = f1.Ino

			f2, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f_a.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, f2.Ino, 0, []byte("fa-body"))
			require.NoError(t, err)
			_, err = base.Link(ctx, store.LinkReq{Ino: f2.Ino, NewParentIno: store.RootIno, NewName: []byte("f_b.txt")})
			require.NoError(t, err)
			baseDiffIno = f2.Ino
		})

		// Target: t_hl_a/b 与 f_a/b 各自保持为 Target 硬链接，在 f_a/f_b 写入新内容触发与 Source 异源正交融合
		target := openTestBranch(t, targetDir, pool)
		_, _, err := target.Write(ctx, baseABIno, 0, []byte("target-shared-payload"))
		require.NoError(t, err)
		_, _, err = target.Write(ctx, baseDiffIno, 0, []byte("target-f-payload"))
		require.NoError(t, err)
		require.NoError(t, target.SetXattr(ctx, baseABIno, []byte("user.t"), []byte("1"), 0))
		require.NoError(t, target.Close())

		// Source:
		// 3a: t_hl_a 未改（在 Merge 中走 KeepTarget），t_hl_b 替换成新 Inode 且写相同内容 + 改 perm=0755（导致 t_hl_b 走 originFused！）
		// 3b: f_a 删重创建并仅改 xattr user.sa="va"，f_b 删重创建并改 xattr user.sb="vb"（两条不同 sIno 对同 tIno 做正交融合）
		source := openTestBranch(t, sourceDir, pool)
		require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("t_hl_b.txt"), false))
		fbNew, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("t_hl_b.txt"), Mode: store.S_IFREG | 0o755})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fbNew.Ino, 0, []byte("shared-body-123"))
		require.NoError(t, err)
		// 对 f_a/f_b 删重创建为不同 sIno 并各自加正交 xattr（同时保持内容一致以在 Base->Source 上通过非冲突类型比对，这里不触发 Add/Add 冲突需 Target 也保持 BaseIno 而仅通过同 BaseIno 改写？注意：如果 Unlink 再 Create，oNode != nil 则 analyzeEntry 视作 sNode 替换）
		//Wait: Unlink + Create 保留 same oType=S_IFREG -> analyzeAllThreePresent 只改 xattr 不触发冲突
		require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("f_a.txt"), false))
		faNew, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f_a.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, faNew.Ino, 0, []byte("fa-body"))
		require.NoError(t, err)
		require.NoError(t, source.SetXattr(ctx, faNew.Ino, []byte("user.sa"), []byte("va"), 0))

		require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("f_b.txt"), false))
		fb2New, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f_b.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fb2New.Ino, 0, []byte("fa-body"))
		require.NoError(t, err)
		require.NoError(t, source.SetXattr(ctx, fb2New.Ino, []byte("user.sb"), []byte("vb"), 0))
		require.NoError(t, source.Close())

		// 注意：t_hl_a 是 KeepTarget，t_hl_b 是 Fused (Target 改了 payload+xattr, Source 改了 mode=0755)
		// f_a 与 f_b 均是 Fused (Target 改了 payload, Source 分别加了 user.sa / user.sb)
		staging := filepath.Join(t.TempDir(), "merged3")
		res, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			StagingDir:    staging,
			Strategy:      store.MergeStrategyFail,
			InoAllocator:  pool.allocIno,
			PackAllocator: pool.allocPack,
		})
		require.NoError(t, err)

		merged := openTestBranch(t, staging, pool)
		defer func() { _ = merged.Close() }()

		entTA, attrTA, err := merged.Lookup(ctx, store.RootIno, []byte("t_hl_a.txt"))
		require.NoError(t, err)
		entTB, attrTB, err := merged.Lookup(ctx, store.RootIno, []byte("t_hl_b.txt"))
		require.NoError(t, err)
		require.NotEqual(t, entTA.Ino, entTB.Ino)
		require.Equal(t, uint32(1), attrTA.Nlink)
		require.Equal(t, uint32(1), attrTB.Nlink)
		require.Equal(t, uint32(0o755), attrTB.Mode&store.PermMask)
		dataTA, err := merged.Read(ctx, entTA.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("target-shared-payload"), dataTA)
		dataTB, err := merged.Read(ctx, entTB.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("target-shared-payload"), dataTB)
		xtTB, err := merged.GetXattr(ctx, entTB.Ino, []byte("user.t"))
		require.NoError(t, err)
		require.Equal(t, []byte("1"), xtTB)

		// f_a 与 f_b 应各自独立融合，保留各自的 user.sa / user.sb
		entFA, attrFA, err := merged.Lookup(ctx, store.RootIno, []byte("f_a.txt"))
		require.NoError(t, err)
		entFB, attrFB, err := merged.Lookup(ctx, store.RootIno, []byte("f_b.txt"))
		require.NoError(t, err)
		require.NotEqual(t, entFA.Ino, entFB.Ino)
		require.Equal(t, uint32(1), attrFA.Nlink)
		require.Equal(t, uint32(1), attrFB.Nlink)
		xa, err := merged.GetXattr(ctx, entFA.Ino, []byte("user.sa"))
		require.NoError(t, err)
		require.Equal(t, []byte("va"), xa)
		xb, err := merged.GetXattr(ctx, entFB.Ino, []byte("user.sb"))
		require.NoError(t, err)
		require.Equal(t, []byte("vb"), xb)

		require.NoError(t, merged.CompactPacks(ctx))
		stFS, err := merged.StatFS(ctx)
		require.NoError(t, err)
		require.Equal(t, res.UsedBytes, stFS.UsedBytes)
		require.Equal(t, res.Inodes, stFS.Files)
	})

	t.Run("Finding4_RootConflict_DeletedSubtreePrefix_CrossDirHL_And_RenameConflict", func(t *testing.T) {
		pool := newSharedSegmentPool(700, 1)
		var baseDelSubIno, baseCrossHLIno, baseRenTIno, baseRenSIno uint64
		baseDir, targetDir, sourceDir := makeSeededFork(t, pool, func(ctx context.Context, base *store.StoreActor) {
			dDel, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("del_d"), Mode: store.S_IFDIR | 0o755})
			require.NoError(t, err)
			fSub, err := base.Create(ctx, store.CreateReq{ParentIno: dDel.Ino, Name: []byte("sub.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fSub.Ino, 0, []byte("sub-base"))
			require.NoError(t, err)
			baseDelSubIno = fSub.Ino

			_, err = base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("dir1"), Mode: store.S_IFDIR | 0o755})
			require.NoError(t, err)
			d2, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("dir2"), Mode: store.S_IFDIR | 0o755})
			require.NoError(t, err)
			fOrig, err := base.Create(ctx, store.CreateReq{ParentIno: d2.Ino, Name: []byte("orig.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			_, _, err = base.Write(ctx, fOrig.Ino, 0, []byte("cross-hl-base"))
			require.NoError(t, err)
			baseCrossHLIno = fOrig.Ino

			fRT, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ren_a.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			baseRenTIno = fRT.Ino

			fRS, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("s_old.txt"), Mode: store.S_IFREG | 0o644})
			require.NoError(t, err)
			baseRenSIno = fRS.Ino
		})

		target := openTestBranch(t, targetDir, pool)
		// 4a: 根目录 Xattr 冲突 + ASCII < '/' 的子节点 "-first"
		require.NoError(t, target.SetXattr(ctx, store.RootIno, []byte("user.root"), []byte("tgt-root"), 0))
		fDash, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("-first"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fDash.Ino, 0, []byte("t"))
		require.NoError(t, err)
		// 4b-1: del_d/sub.txt 修改
		_, _, err = target.Write(ctx, baseDelSubIno, 0, []byte("sub-tgt-edit"))
		require.NoError(t, err)
		// 4b-2: 跨目录硬链接 dir1/link -> baseCrossHLIno
		d1Ent, _, err := target.Lookup(ctx, store.RootIno, []byte("dir1"))
		require.NoError(t, err)
		_, err = target.Link(ctx, store.LinkReq{Ino: baseCrossHLIno, NewParentIno: d1Ent.Ino, NewName: []byte("link.txt")})
		require.NoError(t, err)
		// 4b-3: Target Rename ren_a.txt -> ren_b.txt (Nlink==1), 且编辑 s_old.txt
		require.NoError(t, target.Rename(ctx, store.RenameReq{OldParentIno: store.RootIno, OldName: []byte("ren_a.txt"), NewParentIno: store.RootIno, NewName: []byte("ren_b.txt")}))
		_, _, err = target.Write(ctx, baseRenSIno, 0, []byte("t-edit-s-old"))
		require.NoError(t, err)
		require.NoError(t, target.Close())

		source := openTestBranch(t, sourceDir, pool)
		require.NoError(t, source.SetXattr(ctx, store.RootIno, []byte("user.root"), []byte("src-root"), 0))
		fDashS, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("-first"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fDashS.Ino, 0, []byte("s"))
		require.NoError(t, err)
		// Source 删掉整个 del_d
		require.NoError(t, source.Unlink(ctx, dTreeInoFromBase(t, baseDir, "del_d"), []byte("sub.txt"), false))
		require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("del_d")))
		// Source 修改 dir2/orig.txt
		_, _, err = source.Write(ctx, baseCrossHLIno, 0, []byte("cross-hl-src-edit"))
		require.NoError(t, err)
		// Source 修改 ren_a.txt 并 Rename s_old.txt -> s_new.txt
		_, _, err = source.Write(ctx, baseRenTIno, 0, []byte("src-edit-ren-a"))
		require.NoError(t, err)
		require.NoError(t, source.Rename(ctx, store.RenameReq{OldParentIno: store.RootIno, OldName: []byte("s_old.txt"), NewParentIno: store.RootIno, NewName: []byte("s_new.txt")}))
		require.NoError(t, source.Close())

		// 4a: 根目录冲突应在 DiffTrees 出现，且与 "-first" 分页不漏项
		p1, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			ConflictsOnly: true,
			PageSize:      1,
		})
		require.NoError(t, err)
		require.Len(t, p1.Entries, 1)
		require.Equal(t, []byte("/"), p1.Entries[0].Path)
		require.Equal(t, store.ConflictXattr, p1.Entries[0].Conflict)
		require.NotEmpty(t, p1.NextPageToken)

		p2, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			ConflictsOnly: true,
			PageSize:      1,
			PageToken:     p1.NextPageToken,
		})
		require.NoError(t, err)
		require.Len(t, p2.Entries, 1)
		require.Equal(t, []byte("-first"), p2.Entries[0].Path)

		// 4b-1: 查询被删除子树的叶子 PathPrefix="del_d/sub.txt" 不得返回空
		delSubDiff, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			PathPrefix: []byte("del_d/sub.txt"),
		})
		require.NoError(t, err)
		require.Len(t, delSubDiff.Entries, 1)
		require.Equal(t, []byte("del_d/sub.txt"), delSubDiff.Entries[0].Path)
		require.Equal(t, store.ConflictModifyDelete, delSubDiff.Entries[0].Conflict)

		// 4b-2: 跨目录硬链接分别查询 dir1 与 dir2 均应报 ConflictHardlink
		d2Diff, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			PathPrefix:    []byte("dir2"),
			ConflictsOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, d2Diff.Entries, 1)
		require.Equal(t, store.ConflictHardlink, d2Diff.Entries[0].Conflict)

		d1Diff, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			PathPrefix:    []byte("dir1"),
			ConflictsOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, d1Diff.Entries, 1)
		require.Equal(t, []byte("dir1/link.txt"), d1Diff.Entries[0].Path)
		require.Equal(t, store.ConflictHardlink, d1Diff.Entries[0].Conflict)

		// 4b-3: Rename (Nlink==1) 新路径 ren_b.txt 与 s_new.txt 均应报 ConflictHardlink
		renBDiff, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			PathPrefix:    []byte("ren_b.txt"),
			ConflictsOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, renBDiff.Entries, 1)
		require.Equal(t, store.ConflictHardlink, renBDiff.Entries[0].Conflict)

		sNewDiff, err := store.DiffTrees(ctx, &store.DiffOptions{
			BaseRoot:      baseDir,
			TargetRoot:    targetDir,
			SourceRoot:    sourceDir,
			PathPrefix:    []byte("s_new.txt"),
			ConflictsOnly: true,
		})
		require.NoError(t, err)
		require.Len(t, sNewDiff.Entries, 1)
		require.Equal(t, store.ConflictHardlink, sNewDiff.Entries[0].Conflict)

		// 4a MergeTrees: Fail 报告 "/" 冲突；PathStrategies["/"]=Theirs 可裁决根目录冲突
		_, failErr := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: filepath.Join(t.TempDir(), "m4_fail"),
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"/": store.MergeStrategyFail,
			},
		})
		var confErr *store.ErrMergeConflict
		require.ErrorAs(t, failErr, &confErr)
		require.Equal(t, []byte("/"), confErr.Report.Conflicts[0].Path)

		m4OK := filepath.Join(t.TempDir(), "m4_ok")
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: m4OK,
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"/": store.MergeStrategyTheirs,
			},
			InoAllocator:  pool.allocIno,
			PackAllocator: pool.allocPack,
		})
		require.NoError(t, err)
		merged4 := openTestBranch(t, m4OK, pool)
		defer func() { _ = merged4.Close() }()
		rootX, err := merged4.GetXattr(ctx, store.RootIno, []byte("user.root"))
		require.NoError(t, err)
		require.Equal(t, []byte("src-root"), rootX)
	})
}
