package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep13_IncrementalPruning_SkipsUntouchedSubtreesCompletely(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step13-prune",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	untouchedDirs := make(map[uint64]struct{})
	for i := range 20 {
		dTop, mkErr := disk.Mkdir(ctx, DefaultBranchName, RootIno, fmt.Sprintf("untouched-%02d", i), 0o755, 0, 0)
		require.NoError(t, mkErr)
		untouchedDirs[dTop.Ino] = struct{}{}

		dSub, mkErr := disk.Mkdir(ctx, DefaultBranchName, dTop.Ino, "sub", 0o755, 0, 0)
		require.NoError(t, mkErr)
		untouchedDirs[dSub.Ino] = struct{}{}

		f, cErr := disk.Create(ctx, DefaultBranchName, dSub.Ino, "static.txt", 0o644, 0, 0)
		require.NoError(t, cErr)
		_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("never-changed"))
		require.NoError(t, wErr)
	}

	activeDir, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "active", 0o755, 0, 0)
	require.NoError(t, err)
	nestedDir, err := disk.Mkdir(ctx, DefaultBranchName, activeDir.Ino, "nested", 0o755, 0, 0)
	require.NoError(t, err)
	targetFile, err := disk.Create(ctx, DefaultBranchName, nestedDir.Ino, "target.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, targetFile.Ino, 0, []byte("v1"))
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork")
	require.NoError(t, err)

	_, err = disk.Write(ctx, "feat", targetFile.Ino, 0, []byte("v2-from-feat"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat", "modify target")
	require.NoError(t, err)

	var (
		mu      sync.Mutex
		visited = make(map[uint64]int)
	)
	disk.onDirVisitedHook = func(dirIno uint64) {
		mu.Lock()
		visited[dirIno]++
		mu.Unlock()
	}

	diffRes, err := disk.Diff(ctx, DiffOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Len(t, diffRes.Entries, 1)
	require.Equal(t, []byte("active/nested/target.txt"), diffRes.Entries[0].Path)
	require.Equal(t, DiffKindModified, diffRes.Entries[0].Kind)

	mu.Lock()
	for uIno := range untouchedDirs {
		require.Zero(t, visited[uIno], "untouched dir inode %d should not be expanded in Diff", uIno)
	}
	require.Positive(t, visited[RootIno])
	require.Positive(t, visited[activeDir.Ino])
	require.Positive(t, visited[nestedDir.Ino])
	visited = make(map[uint64]int)
	mu.Unlock()

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch:  "feat",
		TargetBranch:  DefaultBranchName,
		NoFastForward: true,
	})
	require.NoError(t, err)

	mu.Lock()
	for uIno := range untouchedDirs {
		require.Zero(t, visited[uIno], "untouched dir inode %d should not be expanded in Merge", uIno)
	}
	visited = make(map[uint64]int)
	mu.Unlock()

	_, err = disk.Write(ctx, DefaultBranchName, targetFile.Ino, 0, []byte("v3-work"))
	require.NoError(t, err)

	workRes, err := disk.DiffWork(ctx, DefaultBranchName, DiffWorkOptions{})
	require.NoError(t, err)
	require.Len(t, workRes.Entries, 1)
	require.Equal(t, []byte("active/nested/target.txt"), workRes.Entries[0].Path)

	mu.Lock()
	for uIno := range untouchedDirs {
		require.Zero(t, visited[uIno], "untouched dir inode %d should not be expanded in DiffWork", uIno)
	}
	mu.Unlock()
}

func TestStep13_SparseParentIndex_HardlinkUnlinkWriteRenameAndMultiHopMerge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	pKey := LayerParentKey(17, 99, 42)
	pPrefix := LayerParentPrefix(17, 99)
	require.Equal(t, len(pPrefix), SplitKey(pKey))
	require.Equal(t, pPrefix, pKey[:SplitKey(pKey)])
	require.Equal(t, len(pPrefix), SplitKey(pPrefix))
	gotL, gotI, gotP, decErr := DecodeLayerParentKey(pKey)
	require.NoError(t, decErr)
	require.Equal(t, uint64(17), gotL)
	require.Equal(t, uint64(99), gotI)
	require.Equal(t, uint64(42), gotP)

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step13-hardlink",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	dir1, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "dir1", 0o755, 0, 0)
	require.NoError(t, err)
	dir2, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "dir2", 0o755, 0, 0)
	require.NoError(t, err)
	dir3, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "dir3", 0o755, 0, 0)
	require.NoError(t, err)

	f, err := disk.Create(ctx, DefaultBranchName, dir1.Ino, "orig.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("hl-v1"))
	require.NoError(t, err)

	linked, err := disk.Link(ctx, DefaultBranchName, f.Ino, dir2.Ino, "alias.txt")
	require.NoError(t, err)
	require.Equal(t, uint32(2), linked.Nlink)
	require.NotZero(t, linked.Flags&InodeFlagEverLinked)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, dir1.Ino, "orig.txt"))
	afterUnlink, err := disk.GetAttr(ctx, DefaultBranchName, f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(1), afterUnlink.Nlink)
	require.Equal(t, dir1.Ino, afterUnlink.ParentIno)
	require.NotZero(t, afterUnlink.Flags&InodeFlagEverLinked)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork after unlink")
	require.NoError(t, err)

	_, err = disk.Write(ctx, "feat", f.Ino, 0, []byte("hl-v2-after-unlink"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat", "write only on stale parentIno")
	require.NoError(t, err)

	diffRes, err := disk.Diff(ctx, DiffOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Len(t, diffRes.Entries, 1)
	require.Equal(t, []byte("dir2/alias.txt"), diffRes.Entries[0].Path)
	require.Equal(t, DiffKindModified, diffRes.Entries[0].Kind)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "main-side.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "diverge main")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	gotV2, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("hl-v2-after-unlink"), gotV2)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat2", "fork hop2")
	require.NoError(t, err)

	require.NoError(t, disk.Rename(ctx, "feat2", dir2.Ino, "alias.txt", dir3.Ino, "moved.txt"))
	_, err = disk.Commit(ctx, "feat2", "rename ever-linked")
	require.NoError(t, err)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "main-hop2.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "main hop2")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat2",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	_, _, err = disk.Lookup(ctx, DefaultBranchName, dir2.Ino, "alias.txt")
	require.ErrorIs(t, err, ErrNotFound)
	_, movedIno, err := disk.Lookup(ctx, DefaultBranchName, dir3.Ino, "moved.txt")
	require.NoError(t, err)
	require.Equal(t, f.Ino, movedIno.Ino)

	_, err = disk.Write(ctx, "feat2", f.Ino, 0, []byte("hl-v3-multihop-updated"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat2", "write after rename")
	require.NoError(t, err)

	diffHop3, err := disk.Diff(ctx, DiffOptions{
		SourceBranch: "feat2",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Len(t, diffHop3.Entries, 1)
	require.Equal(t, []byte("dir3/moved.txt"), diffHop3.Entries[0].Path)
	require.Equal(t, DiffKindModified, diffHop3.Entries[0].Kind)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat2",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	gotV3, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("hl-v3-multihop-updated"), gotV3)

	regF, err := disk.Create(ctx, DefaultBranchName, dir1.Ino, "reg.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, regF.Ino, 0, []byte("reg-v1"))
	require.NoError(t, err)
	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat-rename", "fork reg rename")
	require.NoError(t, err)

	require.NoError(t, disk.Rename(ctx, "feat-rename", dir1.Ino, "reg.txt", dir2.Ino, "reg-moved.txt"))
	_, err = disk.Commit(ctx, "feat-rename", "rename reg")
	require.NoError(t, err)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "main-div.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "diverge")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat-rename",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	regAttr, err := disk.GetAttr(ctx, DefaultBranchName, regF.Ino)
	require.NoError(t, err)
	require.Equal(t, dir2.Ino, regAttr.ParentIno)

	_, err = disk.Write(ctx, DefaultBranchName, regF.Ino, 0, []byte("reg-v2-after-merge"))
	require.NoError(t, err)
	workRegDiff, err := disk.DiffWork(ctx, DefaultBranchName, DiffWorkOptions{})
	require.NoError(t, err)
	require.Len(t, workRegDiff.Entries, 1)
	require.Equal(t, []byte("dir2/reg-moved.txt"), workRegDiff.Entries[0].Path)
	require.Equal(t, DiffKindModified, workRegDiff.Entries[0].Kind)
}

func TestStep13_ConflictKindsStrategiesPathStrategiesPrefixAndPagination(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step13-conflicts",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	box, err := disk.Mkdir(ctx, DefaultBranchName, RootIno, "box", 0o755, 0, 0)
	require.NoError(t, err)

	contentF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "02_content.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, contentF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	modDelF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "03_moddel.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, modDelF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	delModF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "04_delmod.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, delModF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	tdmDir, err := disk.Mkdir(ctx, DefaultBranchName, box.Ino, "05_tdm_dir", 0o755, 0, 0)
	require.NoError(t, err)
	tdmFile, err := disk.Create(ctx, DefaultBranchName, tdmDir.Ino, "inner.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, tdmFile.Ino, 0, []byte("base"))
	require.NoError(t, err)

	typeF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "06_type.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, typeF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	dfF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "07_dirfile", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, dfF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	attrF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "08_attr.txt", 0o644, 0, 0)
	require.NoError(t, err)

	xattrF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "09_xattr.txt", 0o644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, xattrF.Ino, "user.k", []byte("base"), 0))

	quotaF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "10_quota.txt", 0o644, 0, 0)
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, quotaF.Ino, systemQuotaXattrName, []byte("100"), 0))

	secF, err := disk.Create(ctx, DefaultBranchName, box.Ino, "11_sec.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, secF.Ino, 0, []byte("base"))
	require.NoError(t, err)

	hlBase, err := disk.Create(ctx, DefaultBranchName, box.Ino, "12_hl_orig.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, hlBase.Ino, 0, []byte("hl-base"))
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork conflicts")
	require.NoError(t, err)

	aaT, err := disk.Create(ctx, DefaultBranchName, box.Ino, "01_addadd.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, aaT.Ino, 0, []byte("ours"))
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, contentF.Ino, 0, []byte("ours-content"))
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, modDelF.Ino, 0, []byte("ours-modified"))
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, box.Ino, "04_delmod.txt"))

	_, err = disk.Write(ctx, DefaultBranchName, tdmFile.Ino, 0, []byte("ours-inner"))
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, box.Ino, "06_type.txt"))
	_, err = disk.Symlink(ctx, DefaultBranchName, box.Ino, "06_type.txt", "/target/sym", 0, 0)
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, DefaultBranchName, box.Ino, "07_dirfile"))
	_, err = disk.Mkdir(ctx, DefaultBranchName, box.Ino, "07_dirfile", 0o755, 0, 0)
	require.NoError(t, err)

	modeOurs := uint32(0o600)
	_, err = disk.SetAttr(ctx, DefaultBranchName, attrF.Ino, SetAttrReq{Mode: &modeOurs})
	require.NoError(t, err)

	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, xattrF.Ino, "user.k", []byte("ours"), 0))
	require.NoError(t, disk.SetXattr(ctx, DefaultBranchName, quotaF.Ino, systemQuotaXattrName, []byte("200"), 0))

	_, err = disk.Write(ctx, DefaultBranchName, secF.Ino, 0, []byte("ours-payload"))
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, hlBase.Ino, 0, []byte("ours-hl-mod"))
	require.NoError(t, err)

	_, err = disk.Commit(ctx, DefaultBranchName, "main conflicts")
	require.NoError(t, err)

	aaS, err := disk.Create(ctx, "feat", box.Ino, "01_addadd.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", aaS.Ino, 0, []byte("theirs"))
	require.NoError(t, err)

	_, err = disk.Write(ctx, "feat", contentF.Ino, 0, []byte("theirs-content"))
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, "feat", box.Ino, "03_moddel.txt"))

	_, err = disk.Write(ctx, "feat", delModF.Ino, 0, []byte("theirs-modified"))
	require.NoError(t, err)

	require.NoError(t, disk.Unlink(ctx, "feat", tdmDir.Ino, "inner.txt"))
	require.NoError(t, disk.Rmdir(ctx, "feat", box.Ino, "05_tdm_dir"))

	_, err = disk.Write(ctx, "feat", typeF.Ino, 0, []byte("theirs-reg-mod"))
	require.NoError(t, err)

	_, err = disk.Write(ctx, "feat", dfF.Ino, 0, []byte("theirs-file-mod"))
	require.NoError(t, err)

	modeTheirs := uint32(0o640)
	_, err = disk.SetAttr(ctx, "feat", attrF.Ino, SetAttrReq{Mode: &modeTheirs})
	require.NoError(t, err)

	require.NoError(t, disk.SetXattr(ctx, "feat", xattrF.Ino, "user.k", []byte("theirs"), 0))
	require.NoError(t, disk.SetXattr(ctx, "feat", quotaF.Ino, systemQuotaXattrName, []byte("300"), 0))

	modeSUID := ModeSetUID | 0o644
	_, err = disk.SetAttr(ctx, "feat", secF.Ino, SetAttrReq{Mode: &modeSUID})
	require.NoError(t, err)

	_, err = disk.Link(ctx, "feat", hlBase.Ino, box.Ino, "12_hl_link.txt")
	require.NoError(t, err)

	cleanF, err := disk.Create(ctx, "feat", RootIno, "zzz_clean.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", cleanF.Ino, 0, []byte("clean-add"))
	require.NoError(t, err)

	_, err = disk.Commit(ctx, "feat", "feat conflicts")
	require.NoError(t, err)

	expectedKinds := map[string]ConflictKind{
		"box/01_addadd.txt":  ConflictAddAdd,
		"box/02_content.txt": ConflictContent,
		"box/03_moddel.txt":  ConflictModifyDelete,
		"box/04_delmod.txt":  ConflictDeleteModify,
		"box/05_tdm_dir":     ConflictTreeDeleteModify,
		"box/06_type.txt":    ConflictType,
		"box/07_dirfile":     ConflictDirFile,
		"box/08_attr.txt":    ConflictAttr,
		"box/09_xattr.txt":   ConflictXattr,
		"box/10_quota.txt":   ConflictXattrQuota,
		"box/11_sec.txt":     ConflictSecurityAttr,
		"box/12_hl_link.txt": ConflictHardlink,
	}

	var (
		allPaged []DiffEntry
		token    string
	)
	for {
		page, dErr := disk.Diff(ctx, DiffOptions{
			SourceBranch:  "feat",
			TargetBranch:  DefaultBranchName,
			PathPrefix:    []byte("box"),
			ConflictsOnly: true,
			PageSize:      3,
			PageToken:     token,
		})
		require.NoError(t, dErr)
		require.Equal(t, uint64(len(expectedKinds)), page.Summary.Conflicts)
		allPaged = append(allPaged, page.Entries...)
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, allPaged, len(expectedKinds))
	for _, entry := range allPaged {
		want, ok := expectedKinds[string(entry.Path)]
		require.True(t, ok, "unexpected conflict path %s", string(entry.Path))
		require.Equal(t, want, entry.Conflict, "path %s conflict mismatch", string(entry.Path))
	}

	mainBeforeFail, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
		Strategy:     MergeStrategyFail,
	})
	var mergeConflictErr *ErrMergeConflict
	require.ErrorAs(t, err, &mergeConflictErr)
	require.Equal(t, uint64(len(expectedKinds)), mergeConflictErr.Report.TotalConflicts)

	mainAfterFail, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, mainBeforeFail, mainAfterFail)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
		Strategy:     MergeStrategyOurs,
		PathStrategies: map[string]MergeStrategy{
			"box/01_addadd.txt":  MergeStrategyTheirs,
			"box/02_content.txt": MergeStrategyOurs,
			"box/03_moddel.txt":  MergeStrategyTheirs,
			"box/04_delmod.txt":  MergeStrategyTheirs,
			"box/05_tdm_dir":     MergeStrategyTheirs,
			"box/07_dirfile":     MergeStrategyTheirs,
			"box/09_xattr.txt":   MergeStrategyTheirs,
		},
	})
	require.NoError(t, err)

	_, aaMerged, err := disk.Lookup(ctx, DefaultBranchName, box.Ino, "01_addadd.txt")
	require.NoError(t, err)
	aaBytes, err := disk.Read(ctx, DefaultBranchName, aaMerged.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("theirs"), aaBytes)

	contentBytes, err := disk.Read(ctx, DefaultBranchName, contentF.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("ours-content"), contentBytes)

	_, _, err = disk.Lookup(ctx, DefaultBranchName, box.Ino, "03_moddel.txt")
	require.ErrorIs(t, err, ErrNotFound)

	_, dmMerged, err := disk.Lookup(ctx, DefaultBranchName, box.Ino, "04_delmod.txt")
	require.NoError(t, err)
	dmBytes, err := disk.Read(ctx, DefaultBranchName, dmMerged.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("theirs-modified"), dmBytes)

	_, _, err = disk.Lookup(ctx, DefaultBranchName, box.Ino, "05_tdm_dir")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetAttr(ctx, DefaultBranchName, tdmFile.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	_, dfMerged, err := disk.Lookup(ctx, DefaultBranchName, box.Ino, "07_dirfile")
	require.NoError(t, err)
	require.Equal(t, ModeRegular, dfMerged.Mode&ModeTypeMask)
	dfBytes, err := disk.Read(ctx, DefaultBranchName, dfMerged.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("theirs-file-mod"), dfBytes)

	xv, err := disk.GetXattr(ctx, DefaultBranchName, xattrF.Ino, "user.k")
	require.NoError(t, err)
	require.Equal(t, []byte("theirs"), xv)

	_, cleanMerged, err := disk.Lookup(ctx, DefaultBranchName, RootIno, "zzz_clean.txt")
	require.NoError(t, err)
	cleanBytes, err := disk.Read(ctx, DefaultBranchName, cleanMerged.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("clean-add"), cleanBytes)
}

func TestStep14_RealMerge_SerializesTargetWritesFencesStaleEpochAndDoesNotBlockOtherBranchOrSnapshotDiff(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	clk := newStepClock(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step14-concurrency",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	baseF, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, baseF.Ino, 0, []byte("v1"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "base")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)
	_, _, err = disk.Fork(ctx, DefaultBranchName, "dev", "fork dev")
	require.NoError(t, err)

	featF, err := disk.Create(ctx, "feat", RootIno, "feat.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", featF.Ino, 0, []byte("from-feat"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "feat", "feat commit")
	require.NoError(t, err)

	mainF, err := disk.Create(ctx, DefaultBranchName, RootIno, "main.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, mainF.Ino, 0, []byte("from-main"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "main commit")
	require.NoError(t, err)

	mount, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	staleCtx := WithWriterEpoch(ctx, mount.WriterEpoch)
	clk.Advance(31 * time.Second)

	mergeEntered := make(chan struct{})
	releaseMerge := make(chan struct{})
	var enterOnce sync.Once
	disk.beforeMergeExecHook = func(sourceBranch, targetBranch string) error {
		if sourceBranch == "feat" && targetBranch == DefaultBranchName {
			enterOnce.Do(func() { close(mergeEntered) })
			<-releaseMerge
		}
		return nil
	}

	mergeDone := make(chan error, 1)
	var mergeRes *MergeResult
	go func() {
		var mErr error
		mergeRes, mErr = disk.Merge(ctx, MergeOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
		})
		mergeDone <- mErr
	}()

	select {
	case <-mergeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for merge to enter BranchActor(main)")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, wErr := disk.Create(staleCtx, DefaultBranchName, RootIno, "stale-during-merge.txt", 0o644, 0, 0)
		writeDone <- wErr
	}()

	select {
	case wErr := <-writeDone:
		t.Fatalf("expected target write to serialize behind in-flight merge, got early result: %v", wErr)
	case <-time.After(50 * time.Millisecond):
	}

	devDone := make(chan error, 1)
	go func() {
		devF, cErr := disk.Create(ctx, "dev", RootIno, "dev-concurrent.txt", 0o644, 0, 0)
		if cErr != nil {
			devDone <- cErr
			return
		}
		if _, wErr := disk.Write(ctx, "dev", devF.Ino, 0, []byte("dev-ok")); wErr != nil {
			devDone <- wErr
			return
		}
		_, cmErr := disk.Commit(ctx, "dev", "dev commit during main merge")
		devDone <- cmErr
	}()

	select {
	case dErr := <-devDone:
		require.NoError(t, dErr)
	case <-time.After(5 * time.Second):
		t.Fatal("unrelated branch dev blocked while main merge was in flight")
	}

	snapDone := make(chan error, 1)
	go func() {
		diffRes, dErr := disk.Diff(ctx, DiffOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
		})
		if dErr != nil {
			snapDone <- dErr
			return
		}
		if len(diffRes.Entries) != 1 {
			snapDone <- errors.New("unexpected diff entries count during in-flight merge")
			return
		}
		if _, dwErr := disk.DiffWork(ctx, "dev", DiffWorkOptions{}); dwErr != nil {
			snapDone <- dwErr
			return
		}
		dryRes, dryErr := disk.Merge(ctx, MergeOptions{
			SourceBranch: "feat",
			TargetBranch: DefaultBranchName,
			DryRun:       true,
		})
		if dryErr != nil {
			snapDone <- dryErr
			return
		}
		if dryRes.Summary.Added != 1 {
			snapDone <- errors.New("unexpected dry-run summary during in-flight merge")
			return
		}
		snapDone <- nil
	}()

	select {
	case sErr := <-snapDone:
		require.NoError(t, sErr)
	case <-time.After(5 * time.Second):
		t.Fatal("lock-free snapshot Diff/DiffWork/DryRun blocked while main merge was in flight")
	}

	close(releaseMerge)

	select {
	case mErr := <-mergeDone:
		require.NoError(t, mErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for merge completion")
	}
	require.Equal(t, mount.WriterEpoch+1, mergeRes.WriterEpoch)

	select {
	case wErr := <-writeDone:
		require.ErrorIs(t, wErr, ErrStaleEpoch)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for queued target write")
	}

	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "stale-during-merge.txt")
	require.ErrorIs(t, err, ErrNotFound)

	gotFeat, err := disk.Read(ctx, DefaultBranchName, featF.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("from-feat"), gotFeat)

	_, devLookup, err := disk.Lookup(ctx, "dev", RootIno, "dev-concurrent.txt")
	require.NoError(t, err)
	gotDev, err := disk.Read(ctx, "dev", devLookup.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("dev-ok"), gotDev)
}
