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

		f1, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("unchanged.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		sharedFileIno = f1.Ino
		_, _, err = base.Write(ctx, f1.Ino, 0, makePatternBytes(128<<10, 0x31))
		require.NoError(t, err)

		trash, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("trash.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = base.Write(ctx, trash.Ino, 0, makePatternBytes(512<<10, 0x77))
		require.NoError(t, err)

		d1, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("a"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		d2, err := base.Mkdir(ctx, store.MkdirReq{ParentIno: d1.Ino, Name: []byte("b"), Mode: store.S_IFDIR | 0o755})
		require.NoError(t, err)
		nf, err := base.Create(ctx, store.CreateReq{ParentIno: d2.Ino, Name: []byte("deep.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		nestedFileIno = nf.Ino
		_, _, err = base.Write(ctx, nf.Ino, 0, []byte("v0"))
		require.NoError(t, err)

		cf, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("cross.txt"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = base.Write(ctx, cf.Ino, 0, []byte("base-cross"))
		require.NoError(t, err)

		sf, err := base.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("sparse.bin"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		sparseFileIno = sf.Ino
		_, _, err = base.Write(ctx, sf.Ino, 0, []byte("HEAD"))
		require.NoError(t, err)
	})

	target := openTestBranch(t, targetDir, pool)

	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("trash.bin"), false))
	require.NoError(t, target.CompactPacks(ctx))

	_, crossEntry, err := target.Lookup(ctx, store.RootIno, []byte("cross.txt"))
	require.NoError(t, err)
	_, _, err = target.Write(ctx, crossEntry.Ino, 0, []byte("tgt!-cross"))
	require.NoError(t, err)

	_, _, err = target.Write(ctx, sparseFileIno, 0, append([]byte("HEAD"), make([]byte, 1020)...))
	require.NoError(t, err)
	extSize := uint64(4096)
	_, err = target.SetAttr(ctx, sparseFileIno, store.SetAttrReq{Size: &extSize})
	require.NoError(t, err)
	require.NoError(t, target.Close())

	source := openTestBranch(t, sourceDir, pool)

	_, _, err = source.Write(ctx, nestedFileIno, 0, []byte("v1"))
	require.NoError(t, err)

	_, _, err = source.Write(ctx, crossEntry.Ino, 0, []byte("src!-cross"))
	require.NoError(t, err)

	_, _, err = source.Write(ctx, sparseFileIno, 0, []byte("HEAD"))
	require.NoError(t, err)
	_, err = source.SetAttr(ctx, sparseFileIno, store.SetAttrReq{Size: &extSize})
	require.NoError(t, err)
	require.NoError(t, source.Close())

	_ = sharedFileIno

	res, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
	})
	require.NoError(t, err)

	require.Equal(t, uint64(2), res.Summary.Modified)
	require.Equal(t, uint64(1), res.Summary.Conflicts)
	require.Len(t, res.Entries, 2)

	require.Equal(t, []byte("a/b/deep.txt"), res.Entries[0].Path)
	require.Equal(t, store.DiffKindModified, res.Entries[0].Kind)
	require.Equal(t, store.ConflictNone, res.Entries[0].Conflict)

	require.Equal(t, []byte("cross.txt"), res.Entries[1].Path)
	require.Equal(t, store.DiffKindModified, res.Entries[1].Kind)
	require.Equal(t, store.ConflictContent, res.Entries[1].Conflict)

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

		orf := createReg(store.RootIno, "orphan_base.txt", "secret")
		orphanIno = orf
		require.NoError(t, base.Unlink(ctx, store.RootIno, []byte("orphan_base.txt"), true))

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

		orthoContentAttrIno = createReg(store.RootIno, "ortho_attr_content.txt", "v0")
		orthoXattrIno = createReg(store.RootIno, "ortho_xattr.txt", "v0")
		sameEditIno = createReg(store.RootIno, "same_edit.txt", "v0")
	})

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

	target := openTestBranch(t, targetDir, pool)

	fAddT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_add_add.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = target.Write(ctx, fAddT.Ino, 0, []byte("target-add"))
	require.NoError(t, err)

	fConT, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_content_unused"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_ = fConT

	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("c_type"), false))
	_, err = target.Symlink(ctx, store.SymlinkReq{ParentIno: store.RootIno, Name: []byte("c_type"), Target: []byte("/t")})
	require.NoError(t, err)

	_, _, err = target.Write(ctx, modDelIno, 0, []byte("tgt-modified"))
	require.NoError(t, err)

	require.NoError(t, target.Unlink(ctx, store.RootIno, []byte("c_del_mod.txt"), false))

	_, _, err = target.Write(ctx, treeDelModSubFileIno, 0, []byte("tgt-sub-edit"))
	require.NoError(t, err)

	_, _, err = target.Write(ctx, dirFileSubFileIno, 0, []byte("tgt-df-edit"))
	require.NoError(t, err)

	m755 := uint32(0o755)
	_, err = target.SetAttr(ctx, attrIno, store.SetAttrReq{Mode: &m755})
	require.NoError(t, err)

	require.NoError(t, target.SetXattr(ctx, xattrIno, []byte("user.k"), []byte("tgt"), 0))

	require.NoError(t, target.SetXattr(ctx, quotaXattrKeyIno, []byte("system.bowl.max_bytes"), []byte("1000"), 0))

	for i := range 180 {
		key := []byte("user.t" + string(bytes.Repeat([]byte{'a'}, 190)) + string([]byte{byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}))
		require.NoError(t, target.SetXattr(ctx, quotaListIno, key, []byte("1"), 0))
	}

	require.NoError(t, target.SetXattr(ctx, secXattrIno, []byte("security.capability"), []byte("cap-t"), 0))

	mSuid := store.S_ISUID | 0o755
	_, err = target.SetAttr(ctx, secPermContentIno, store.SetAttrReq{Mode: &mSuid})
	require.NoError(t, err)

	_, _, err = target.Write(ctx, hlSourceAddIno, 0, []byte("hl-tgt-modified"))
	require.NoError(t, err)

	_, err = target.Link(ctx, store.LinkReq{Ino: hlTargetAddIno, NewParentIno: store.RootIno, NewName: []byte("c_hl_tgt_add_link.txt")})
	require.NoError(t, err)

	_, _, err = target.Write(ctx, orthoContentAttrIno, 0, []byte("tgt-payload"))
	require.NoError(t, err)
	require.NoError(t, target.SetXattr(ctx, orthoXattrIno, []byte("user.a"), []byte("1"), 0))
	_, _, err = target.Write(ctx, sameEditIno, 0, []byte("converged"))
	require.NoError(t, err)
	dSharedT, err := target.Mkdir(ctx, store.MkdirReq{ParentIno: store.RootIno, Name: []byte("shared_dir"), Mode: store.S_IFDIR | 0o755})
	require.NoError(t, err)
	_, err = target.Create(ctx, store.CreateReq{ParentIno: dSharedT.Ino, Name: []byte("from_t.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	require.NoError(t, target.Close())

	source := openTestBranch(t, sourceDir, pool)

	fAddS, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_add_add.txt"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = source.Write(ctx, fAddS.Ino, 0, []byte("source-add"))
	require.NoError(t, err)

	require.NoError(t, source.Close())

	target2 := openTestBranch(t, targetDir, pool)
	_, _, err = target2.Write(ctx, regIno, 0, []byte("T-EDIT"))
	require.NoError(t, err)
	require.NoError(t, target2.Close())

	source = openTestBranch(t, sourceDir, pool)
	_, _, err = source.Write(ctx, regIno, 0, []byte("S-EDIT"))
	require.NoError(t, err)

	require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("c_mod_del.txt"), false))

	_, _, err = source.Write(ctx, delModIno, 0, []byte("src-modified"))
	require.NoError(t, err)

	require.NoError(t, source.Unlink(ctx, dTreeInoFromBase(t, baseDir, "c_tree_del_mod"), []byte("sub.txt"), false))
	require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("c_tree_del_mod")))

	require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("c_type"), false))
	_, err = source.Mknod(ctx, store.MknodReq{ParentIno: store.RootIno, Name: []byte("c_type"), Mode: store.S_IFIFO | 0o644})
	require.NoError(t, err)

	require.NoError(t, source.Unlink(ctx, dirFileIno, []byte("child.txt"), false))
	require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("c_dir_file")))
	_, err = source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("c_dir_file"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	m600 := uint32(0o600)
	_, err = source.SetAttr(ctx, attrIno, store.SetAttrReq{Mode: &m600})
	require.NoError(t, err)

	require.NoError(t, source.SetXattr(ctx, xattrIno, []byte("user.k"), []byte("src"), 0))

	require.NoError(t, source.SetXattr(ctx, quotaXattrKeyIno, []byte("system.bowl.max_bytes"), []byte("2000"), 0))

	for i := range 180 {
		key := []byte("user.s" + string(bytes.Repeat([]byte{'b'}, 190)) + string([]byte{byte('0' + i/100), byte('0' + (i/10)%10), byte('0' + i%10)}))
		require.NoError(t, source.SetXattr(ctx, quotaListIno, key, []byte("1"), 0))
	}

	require.NoError(t, source.SetXattr(ctx, secXattrIno, []byte("security.capability"), []byte("cap-s"), 0))

	_, _, err = source.Write(ctx, secPermContentIno, 0, []byte("new-payload"))
	require.NoError(t, err)

	_, err = source.Link(ctx, store.LinkReq{Ino: hlSourceAddIno, NewParentIno: store.RootIno, NewName: []byte("c_hl_src_add_link.txt")})
	require.NoError(t, err)

	_, _, err = source.Write(ctx, hlTargetAddIno, 0, []byte("hl-src-modified"))
	require.NoError(t, err)

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

	ordRes, err := store.DiffTrees(ctx, &store.DiffOptions{
		BaseRoot:   baseDir,
		TargetRoot: targetDir,
		SourceRoot: sourceDir,
		PathPrefix: []byte("ord"),
	})
	require.NoError(t, err)
	require.Len(t, ordRes.Entries, 2)
	require.Equal(t, []byte("ord"), ordRes.Entries[0].Path)
	require.Equal(t, []byte("ord/a"), ordRes.Entries[1].Path)

	idxOrd := indexOfString(allPaths, "ord")
	idxOrd2 := indexOfString(allPaths, "ord-2")
	idxOrdTxt := indexOfString(allPaths, "ord.txt")
	idxOrdA := indexOfString(allPaths, "ord/a")
	idxOrd0 := indexOfString(allPaths, "ord0")
	require.Less(t, idxOrd, idxOrd2)
	require.Less(t, idxOrd2, idxOrdTxt)
	require.Less(t, idxOrdTxt, idxOrdA)
	require.Less(t, idxOrdA, idxOrd0)

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

		dryStaging := filepath.Join(t.TempDir(), "dry_staging")
		dryRes, err := store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:   baseDir,
			TargetRoot: targetDir,
			SourceRoot: sourceDir,
			StagingDir: dryStaging,
			Strategy:   store.MergeStrategyOurs,
			PathStrategies: map[string]store.MergeStrategy{
				"hl_split_b.txt": store.MergeStrategyTheirs,
				"tree_dir":       store.MergeStrategyTheirs,
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

		merged := openTestBranch(t, stagingDir, nil)
		defer func() { _ = merged.Close() }()

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

		_, _, err = merged.Lookup(ctx, store.RootIno, []byte("tree_dir"))
		require.ErrorIs(t, err, store.ErrNotFound)

		entTCol, _, err := merged.Lookup(ctx, store.RootIno, []byte("dir_t_only.txt"))
		require.NoError(t, err)
		dataTCol, err := merged.Read(ctx, entTCol.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("target-collision-ino-content"), dataTCol)

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

		_, _, err = merged.Write(ctx, entTCol.Ino, 0, []byte("x"), 1)
		require.ErrorIs(t, err, store.ErrStaleEpoch)
		_, _, err = merged.Write(ctx, entTCol.Ino, 0, []byte("TARGET"), 2)
		require.NoError(t, err)

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

		entFTD, _, err := merged.Lookup(ctx, store.RootIno, []byte("f_to_dir"))
		require.NoError(t, err)
		entFTDC, _, err := merged.Lookup(ctx, entFTD.Ino, []byte("child.txt"))
		require.NoError(t, err)
		dataFTDC, err := merged.Read(ctx, entFTDC.Ino, 0, 100)
		require.NoError(t, err)
		require.Equal(t, []byte("kept-child"), dataFTDC)

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

		target := openTestBranch(t, targetDir, pool)
		_, _, err := target.Write(ctx, baseABIno, 0, []byte("target-shared-payload"))
		require.NoError(t, err)
		_, _, err = target.Write(ctx, baseDiffIno, 0, []byte("target-f-payload"))
		require.NoError(t, err)
		require.NoError(t, target.SetXattr(ctx, baseABIno, []byte("user.t"), []byte("1"), 0))
		require.NoError(t, target.Close())

		source := openTestBranch(t, sourceDir, pool)
		require.NoError(t, source.Unlink(ctx, store.RootIno, []byte("t_hl_b.txt"), false))
		fbNew, err := source.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("t_hl_b.txt"), Mode: store.S_IFREG | 0o755})
		require.NoError(t, err)
		_, _, err = source.Write(ctx, fbNew.Ino, 0, []byte("shared-body-123"))
		require.NoError(t, err)

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

		require.NoError(t, target.SetXattr(ctx, store.RootIno, []byte("user.root"), []byte("tgt-root"), 0))
		fDash, err := target.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("-first"), Mode: store.S_IFREG | 0o644})
		require.NoError(t, err)
		_, _, err = target.Write(ctx, fDash.Ino, 0, []byte("t"))
		require.NoError(t, err)

		_, _, err = target.Write(ctx, baseDelSubIno, 0, []byte("sub-tgt-edit"))
		require.NoError(t, err)

		d1Ent, _, err := target.Lookup(ctx, store.RootIno, []byte("dir1"))
		require.NoError(t, err)
		_, err = target.Link(ctx, store.LinkReq{Ino: baseCrossHLIno, NewParentIno: d1Ent.Ino, NewName: []byte("link.txt")})
		require.NoError(t, err)

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

		require.NoError(t, source.Unlink(ctx, dTreeInoFromBase(t, baseDir, "del_d"), []byte("sub.txt"), false))
		require.NoError(t, source.Rmdir(ctx, store.RootIno, []byte("del_d")))

		_, _, err = source.Write(ctx, baseCrossHLIno, 0, []byte("cross-hl-src-edit"))
		require.NoError(t, err)

		_, _, err = source.Write(ctx, baseRenTIno, 0, []byte("src-edit-ren-a"))
		require.NoError(t, err)
		require.NoError(t, source.Rename(ctx, store.RenameReq{OldParentIno: store.RootIno, OldName: []byte("s_old.txt"), NewParentIno: store.RootIno, NewName: []byte("s_new.txt")}))
		require.NoError(t, source.Close())

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
