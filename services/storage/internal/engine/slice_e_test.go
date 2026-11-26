package engine

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep9_TwoParentMergeCommit_PersistsAcrossReopenLinearReadAndSecondMergeLCA(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-step9-merge",
		MaxBytes:    16 << 20,
		BlockSize:   4096,
		MaxPackSize: 1 << 20,
	})
	require.NoError(t, err)

	shared, err := disk.Create(ctx, DefaultBranchName, RootIno, "shared.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, shared.Ino, 0, []byte("shared-base"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "base")
	require.NoError(t, err)

	c2, _, err := disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	mainFile, err := disk.Create(ctx, DefaultBranchName, RootIno, "main1.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, mainFile.Ino, 0, []byte("from-main-1"))
	require.NoError(t, err)
	c3, err := disk.Commit(ctx, DefaultBranchName, "main-c3")
	require.NoError(t, err)

	featPayload := bytes.Repeat([]byte{0x42}, 6000)
	featFile, err := disk.Create(ctx, "feat", RootIno, "feat1.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", featFile.Ino, 0, featPayload)
	require.NoError(t, err)
	require.NoError(t, disk.SetXattr(ctx, "feat", featFile.Ino, "user.tag", []byte("feat-v1"), 0))
	c4, err := disk.Commit(ctx, "feat", "feat-c4")
	require.NoError(t, err)

	snapBefore := disk.db.NewSnapshot()
	_, c4Layers, err := resolveCommitLayers(snapBefore, c4.ID)
	require.NoError(t, err)
	c4Ref0, ok, err := overlayGetDataRef(snapBefore, c4Layers, featFile.Ino, 0)
	require.NoError(t, err)
	require.True(t, ok)
	c4Ref1, ok, err := overlayGetDataRef(snapBefore, c4Layers, featFile.Ino, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, snapBefore.Close())

	statBefore, err := disk.StatFS(ctx)
	require.NoError(t, err)

	mergeRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
		Message:      "merge feat into main",
	})
	require.NoError(t, err)
	require.False(t, mergeRes.FastForward)
	require.False(t, mergeRes.UpToDate)
	require.Equal(t, c2.ID, mergeRes.BaseCommitID)
	require.Equal(t, c3.ID, mergeRes.TargetCommitID)
	require.Equal(t, c4.ID, mergeRes.SourceCommitID)

	statAfter, err := disk.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, statBefore.UsedBytes, statAfter.UsedBytes)

	c5, err := disk.GetCommit(mergeRes.CommitID)
	require.NoError(t, err)
	require.Equal(t, CommitKindMerge, c5.Kind)
	require.Equal(t, c3.ID, c5.ParentID)
	require.Equal(t, c4.ID, c5.MergeParentID)
	require.Positive(t, c5.LayerID)

	snapAfter := disk.db.NewSnapshot()
	_, mainLayers, err := resolveBranchLayers(snapAfter, DefaultBranchName)
	require.NoError(t, err)
	require.NotContains(t, mainLayers, c4.LayerID)
	require.Contains(t, mainLayers, c5.LayerID)
	c5Ref0, ok, err := overlayGetDataRef(snapAfter, []uint64{c5.LayerID}, featFile.Ino, 0)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, c4Ref0, c5Ref0)
	c5Ref1, ok, err := overlayGetDataRef(snapAfter, []uint64{c5.LayerID}, featFile.Ino, 1)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, c4Ref1, c5Ref1)
	require.NoError(t, snapAfter.Close())

	require.NoError(t, disk.Close())

	reopened, err := OpenDisk(DiskOptions{
		Dir:         dir,
		MaxPackSize: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})

	c5Reopened, err := reopened.GetCommit(c5.ID)
	require.NoError(t, err)
	require.Equal(t, c3.ID, c5Reopened.ParentID)
	require.Equal(t, c4.ID, c5Reopened.MergeParentID)
	require.Equal(t, CommitKindMerge, c5Reopened.Kind)
	require.Equal(t, c5.LayerID, c5Reopened.LayerID)

	gotFeat, err := reopened.Read(ctx, DefaultBranchName, featFile.Ino, 0, len(featPayload))
	require.NoError(t, err)
	require.Equal(t, featPayload, gotFeat)

	gotXattr, err := reopened.GetXattr(ctx, DefaultBranchName, featFile.Ino, "user.tag")
	require.NoError(t, err)
	require.Equal(t, []byte("feat-v1"), gotXattr)

	gotMain, err := reopened.Read(ctx, DefaultBranchName, mainFile.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("from-main-1"), gotMain)

	_, err = reopened.Write(ctx, "feat", featFile.Ino, 0, []byte("feat-v2-updated"))
	require.NoError(t, err)
	feat2, err := reopened.Create(ctx, "feat", RootIno, "feat2.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = reopened.Write(ctx, "feat", feat2.Ino, 0, []byte("second-feat-file"))
	require.NoError(t, err)
	c6, err := reopened.Commit(ctx, "feat", "feat-c6")
	require.NoError(t, err)

	main2, err := reopened.Create(ctx, DefaultBranchName, RootIno, "main2.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = reopened.Write(ctx, DefaultBranchName, main2.Ino, 0, []byte("second-main-file"))
	require.NoError(t, err)
	c7, err := reopened.Commit(ctx, DefaultBranchName, "main-c7")
	require.NoError(t, err)

	merge2Res, err := reopened.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Equal(t, c4.ID, merge2Res.BaseCommitID)
	require.Equal(t, c7.ID, merge2Res.TargetCommitID)
	require.Equal(t, c6.ID, merge2Res.SourceCommitID)

	c8, err := reopened.GetCommit(merge2Res.CommitID)
	require.NoError(t, err)
	require.Equal(t, c7.ID, c8.ParentID)
	require.Equal(t, c6.ID, c8.MergeParentID)

	gotFeat2, err := reopened.Read(ctx, DefaultBranchName, feat2.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("second-feat-file"), gotFeat2)

	gotFeat1Updated, err := reopened.Read(ctx, DefaultBranchName, featFile.Ino, 0, 15)
	require.NoError(t, err)
	require.Equal(t, []byte("feat-v2-updated"), gotFeat1Updated)
}

func TestStep9_DualParentReachability_DeleteCommitPruneAndGCProtectMergedSourceAncestors(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:         dir,
		DiskID:      "disk-step9-gc",
		MaxBytes:    1 << 20,
		BlockSize:   4096,
		MaxPackSize: 8192,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork")
	require.NoError(t, err)

	mf, err := disk.Create(ctx, DefaultBranchName, RootIno, "m.bin", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, mf.Ino, 0, bytes.Repeat([]byte{0x11}, 4096))
	require.NoError(t, err)
	c3, err := disk.Commit(ctx, DefaultBranchName, "c3")
	require.NoError(t, err)

	ff, err := disk.Create(ctx, "feat", RootIno, "f.bin", 0o644, 0, 0)
	require.NoError(t, err)
	featBytes := bytes.Repeat([]byte{0x22}, 8192)
	_, err = disk.Write(ctx, "feat", ff.Ino, 0, featBytes)
	require.NoError(t, err)
	c4, err := disk.Commit(ctx, "feat", "c4")
	require.NoError(t, err)

	mergeRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	require.NoError(t, disk.DeleteBranch(ctx, "feat"))

	require.ErrorIs(t, disk.DeleteCommit(ctx, c4.ID), ErrCommitInUse)

	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Zero(t, pruned)

	_, err = disk.RunGC(ctx)
	require.NoError(t, err)

	gotMain, err := disk.Read(ctx, DefaultBranchName, ff.Ino, 0, len(featBytes))
	require.NoError(t, err)
	require.Equal(t, featBytes, gotMain)

	gotC4, err := disk.ReadAtCommit(ctx, c4.ID, ff.Ino, 0, len(featBytes))
	require.NoError(t, err)
	require.Equal(t, featBytes, gotC4)

	_, err = disk.Reset(ctx, DefaultBranchName, c3.ID)
	require.NoError(t, err)

	prunedAfterReset, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, prunedAfterReset)

	_, err = disk.GetCommit(mergeRes.CommitID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.GetCommit(c4.ID)
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStep10_Merge_RejectsDirtySourceOrTargetWithoutMutatingEitherBranch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step10-dirty",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork")
	require.NoError(t, err)

	_, err = disk.Create(ctx, "feat", RootIno, "dirty-src.txt", 0o644, 0, 0)
	require.NoError(t, err)

	mainBefore, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	featBefore, err := disk.GetBranch("feat")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{SourceBranch: "feat", TargetBranch: DefaultBranchName})
	require.ErrorIs(t, err, ErrDirtyWorkLayer)

	_, err = disk.Merge(ctx, MergeOptions{SourceBranch: "feat", TargetBranch: DefaultBranchName, DryRun: true})
	require.ErrorIs(t, err, ErrDirtyWorkLayer)

	mainAfter, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	featAfter, err := disk.GetBranch("feat")
	require.NoError(t, err)
	require.Equal(t, mainBefore, mainAfter)
	require.Equal(t, featBefore, featAfter)

	_, err = disk.Commit(ctx, "feat", "commit src")
	require.NoError(t, err)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "dirty-tgt.txt", 0o644, 0, 0)
	require.NoError(t, err)

	mainBefore, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	featBefore, err = disk.GetBranch("feat")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{SourceBranch: "feat", TargetBranch: DefaultBranchName})
	require.ErrorIs(t, err, ErrDirtyWorkLayer)

	mainAfter, err = disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	featAfter, err = disk.GetBranch("feat")
	require.NoError(t, err)
	require.Equal(t, mainBefore, mainAfter)
	require.Equal(t, featBefore, featAfter)

	_, err = disk.Commit(ctx, DefaultBranchName, "commit tgt")
	require.NoError(t, err)

	featBeforeMerge, err := disk.GetBranch("feat")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{SourceBranch: "feat", TargetBranch: DefaultBranchName})
	require.NoError(t, err)

	featAfterMerge, err := disk.GetBranch("feat")
	require.NoError(t, err)
	require.Equal(t, featBeforeMerge, featAfterMerge)
}

func TestStep10_DiffDiffCommitsAndDiffWork_SeparateCommittedAndUncommittedViews(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step10-diff",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	a, err := disk.Create(ctx, DefaultBranchName, RootIno, "a.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, a.Ino, 0, []byte("v1"))
	require.NoError(t, err)
	baseCmt, _, err := disk.Fork(ctx, DefaultBranchName, "feat", "fork")
	require.NoError(t, err)

	b, err := disk.Create(ctx, "feat", RootIno, "b.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", b.Ino, 0, []byte("feat-committed"))
	require.NoError(t, err)
	featCmt, err := disk.Commit(ctx, "feat", "feat commit")
	require.NoError(t, err)

	c, err := disk.Create(ctx, DefaultBranchName, RootIno, "c.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, c.Ino, 0, []byte("main-committed"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "main commit")
	require.NoError(t, err)

	_, err = disk.Create(ctx, "feat", RootIno, "work-feat.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "work-main.txt", 0o644, 0, 0)
	require.NoError(t, err)

	crossDiff, err := disk.Diff(ctx, DiffOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Equal(t, baseCmt.ID, crossDiff.BaseCommitID)
	require.Equal(t, uint64(1), crossDiff.Summary.Added)
	require.Len(t, crossDiff.Entries, 1)
	require.Equal(t, []byte("b.txt"), crossDiff.Entries[0].Path)
	require.Equal(t, DiffKindAdded, crossDiff.Entries[0].Kind)

	commitDiff, err := disk.DiffCommits(ctx, DiffCommitsOptions{
		BaseCommitID:   baseCmt.ID,
		SourceCommitID: featCmt.ID,
	})
	require.NoError(t, err)
	require.Len(t, commitDiff.Entries, 1)
	require.Equal(t, []byte("b.txt"), commitDiff.Entries[0].Path)
	require.Nil(t, commitDiff.Entries[0].Target)
	require.NotNil(t, commitDiff.Entries[0].Source)

	workFeat, err := disk.DiffWork(ctx, "feat", DiffWorkOptions{})
	require.NoError(t, err)
	require.Len(t, workFeat.Entries, 1)
	require.Equal(t, []byte("work-feat.txt"), workFeat.Entries[0].Path)
	require.Equal(t, DiffKindAdded, workFeat.Entries[0].Kind)

	workMain, err := disk.DiffWork(ctx, DefaultBranchName, DiffWorkOptions{})
	require.NoError(t, err)
	require.Len(t, workMain.Entries, 1)
	require.Equal(t, []byte("work-main.txt"), workMain.Entries[0].Path)

	_, err = disk.Commit(ctx, "feat", "clean feat")
	require.NoError(t, err)
	workFeatClean, err := disk.DiffWork(ctx, "feat", DiffWorkOptions{})
	require.NoError(t, err)
	require.Empty(t, workFeatClean.Entries)
}

func TestStep11_Merge_DefaultFastForwardNoFastForwardAndUpToDateNoOp(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	clk := newStepClock(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step11-ff",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork")
	require.NoError(t, err)

	ff, err := disk.Create(ctx, "feat", RootIno, "ff.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat", ff.Ino, 0, []byte("fast-forward-data"))
	require.NoError(t, err)
	cFeat, err := disk.Commit(ctx, "feat", "feat commit")
	require.NoError(t, err)

	mountRes, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	mount := mountRes.Mount
	staleCtx := WithWriterEpoch(ctx, mount.WriterEpoch)
	clk.Advance(31 * time.Second)

	nextCmtBefore := disk.nextCmtID.Load()
	nextLayerBefore := disk.nextLayerID.Load()

	ffRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.True(t, ffRes.FastForward)
	require.False(t, ffRes.UpToDate)
	require.Equal(t, cFeat.ID, ffRes.CommitID)
	require.Equal(t, mount.WriterEpoch+1, ffRes.WriterEpoch)
	require.Equal(t, nextCmtBefore, disk.nextCmtID.Load())
	require.Equal(t, nextLayerBefore, disk.nextLayerID.Load())

	_, err = disk.Create(staleCtx, DefaultBranchName, RootIno, "stale.txt", 0o644, 0, 0)
	require.ErrorIs(t, err, ErrStaleEpoch)

	gotData, err := disk.Read(ctx, DefaultBranchName, ff.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("fast-forward-data"), gotData)

	upToDateSame, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.True(t, upToDateSame.UpToDate)
	require.False(t, upToDateSame.FastForward)
	require.Equal(t, cFeat.ID, upToDateSame.CommitID)
	require.Equal(t, ffRes.WriterEpoch, upToDateSame.WriterEpoch)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "main-ahead.txt", 0o644, 0, 0)
	require.NoError(t, err)
	cMainAhead, err := disk.Commit(ctx, DefaultBranchName, "main ahead")
	require.NoError(t, err)

	upToDateAhead, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "feat",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.True(t, upToDateAhead.UpToDate)
	require.False(t, upToDateAhead.FastForward)
	require.Equal(t, cMainAhead.ID, upToDateAhead.CommitID)
	require.Equal(t, ffRes.WriterEpoch, upToDateAhead.WriterEpoch)

	cFork2, _, err := disk.Fork(ctx, DefaultBranchName, "feat2", "fork2")
	require.NoError(t, err)
	f2, err := disk.Create(ctx, "feat2", RootIno, "noff.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "feat2", f2.Ino, 0, []byte("no-ff-data"))
	require.NoError(t, err)
	cFeat2, err := disk.Commit(ctx, "feat2", "feat2 commit")
	require.NoError(t, err)

	noFFRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch:  "feat2",
		TargetBranch:  DefaultBranchName,
		NoFastForward: true,
	})
	require.NoError(t, err)
	require.False(t, noFFRes.FastForward)
	require.False(t, noFFRes.UpToDate)
	require.Equal(t, cFork2.ID, noFFRes.BaseCommitID)
	require.Equal(t, ffRes.WriterEpoch+1, noFFRes.WriterEpoch)

	mergeCmt, err := disk.GetCommit(noFFRes.CommitID)
	require.NoError(t, err)
	require.Equal(t, CommitKindMerge, mergeCmt.Kind)
	require.Equal(t, cFork2.ID, mergeCmt.ParentID)
	require.Equal(t, cFeat2.ID, mergeCmt.MergeParentID)
	require.Positive(t, mergeCmt.LayerID)

	gotNoFF, err := disk.Read(ctx, DefaultBranchName, f2.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("no-ff-data"), gotNoFF)
}

func TestStep12_CrissCrossMergeDAG_ReturnsErrAmbiguousLCAOnDiffAndMerge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step12-crisscross",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "root.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, _, err = disk.Fork(ctx, DefaultBranchName, "b1", "fork b1")
	require.NoError(t, err)

	_, err = disk.Create(ctx, "b1", RootIno, "f1.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "b1", "c2")
	require.NoError(t, err)

	_, err = disk.Create(ctx, DefaultBranchName, RootIno, "f2.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Commit(ctx, DefaultBranchName, "c3")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, "b1", "snap-b1", "pin c2")
	require.NoError(t, err)
	_, _, err = disk.Fork(ctx, DefaultBranchName, "snap-main", "pin c3")
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "snap-main",
		TargetBranch: "b1",
	})
	require.NoError(t, err)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "snap-b1",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)

	b1Before, err := disk.GetBranch("b1")
	require.NoError(t, err)
	mainBefore, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)

	_, err = disk.Diff(ctx, DiffOptions{
		SourceBranch: "b1",
		TargetBranch: DefaultBranchName,
	})
	require.ErrorIs(t, err, ErrAmbiguousLCA)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "b1",
		TargetBranch: DefaultBranchName,
	})
	require.ErrorIs(t, err, ErrAmbiguousLCA)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "b1",
		TargetBranch: DefaultBranchName,
		DryRun:       true,
	})
	require.ErrorIs(t, err, ErrAmbiguousLCA)

	b1After, err := disk.GetBranch("b1")
	require.NoError(t, err)
	mainAfter, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, b1Before, b1After)
	require.Equal(t, mainBefore, mainAfter)
}
