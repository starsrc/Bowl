package engine

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStep2_BranchAddressing_ForkCreatesFirstClassNamedBranchAndIsolatesWrites(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       dir,
		DiskID:    "disk-fork-iso",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "shared.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("v1-main"))
	require.NoError(t, err)

	forkCmt, featBranch, err := disk.Fork(ctx, DefaultBranchName, "feat-1", "fork feat-1")
	require.NoError(t, err)
	require.Equal(t, CommitKindFork, forkCmt.Kind)
	require.Positive(t, forkCmt.LayerID)
	require.Equal(t, "feat-1", featBranch.Name)
	require.Equal(t, forkCmt.ID, featBranch.HeadCommitID)
	require.Positive(t, featBranch.WorkLayerID)

	mainAfterFork, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, forkCmt.ID, mainAfterFork.HeadCommitID)
	require.NotEqual(t, forkCmt.LayerID, mainAfterFork.WorkLayerID)
	require.NotEqual(t, mainAfterFork.WorkLayerID, featBranch.WorkLayerID)

	_, err = disk.Write(ctx, "feat-1", f.Ino, 0, []byte("v2-feat"))
	require.NoError(t, err)
	_, err = disk.Create(ctx, "feat-1", RootIno, "feat-only.txt", 0o644, 0, 0)
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("v2-main"))
	require.NoError(t, err)

	mainData, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("v2-main"), mainData)

	featData, err := disk.Read(ctx, "feat-1", f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("v2-feat"), featData)

	cmtData, err := disk.ReadAtCommit(ctx, forkCmt.ID, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("v1-main"), cmtData)

	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "feat-only.txt")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStep3_CommitAndFork_CleanForkProducesZeroLayerPureMetadataCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       dir,
		DiskID:    "disk-clean-fork",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "base.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("committed"))
	require.NoError(t, err)

	manualCmt, err := disk.Commit(ctx, DefaultBranchName, "manual checkpoint")
	require.NoError(t, err)
	require.Equal(t, CommitKindManual, manualCmt.Kind)
	require.Positive(t, manualCmt.LayerID)
	require.Equal(t, "manual checkpoint", manualCmt.Message)

	cleanForkCmt, featBranch, err := disk.Fork(ctx, DefaultBranchName, "feat-clean", "clean fork")
	require.NoError(t, err)
	require.Equal(t, CommitKindFork, cleanForkCmt.Kind)
	require.Zero(t, cleanForkCmt.LayerID)
	require.Equal(t, manualCmt.ID, cleanForkCmt.ParentID)
	require.Equal(t, cleanForkCmt.ID, featBranch.HeadCommitID)

	got, err := disk.Read(ctx, "feat-clean", f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), got)

	require.NoError(t, disk.Unlink(ctx, "feat-clean", RootIno, "base.txt", false))
	_, _, err = disk.Lookup(ctx, "feat-clean", RootIno, "base.txt")
	require.ErrorIs(t, err, ErrNotFound)

	mainGot, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("committed"), mainGot)
}

func TestStep5_MultiBranch_ParallelBranchActorsCommitAndCoalesceConcurrently(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:         dir,
		DiskID:      "disk-parallel",
		BlockSize:   4096,
		MaxPackSize: 1 << 20,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	const numBranches = 4
	const filesPerBranch = 10
	branchNames := make([]string, numBranches)
	branchNames[0] = DefaultBranchName
	for i := 1; i < numBranches; i++ {
		name := fmt.Sprintf("branch-%d", i)
		branchNames[i] = name
		_, _, forkErr := disk.Fork(ctx, DefaultBranchName, name, "fork parallel")
		require.NoError(t, forkErr)
	}

	var wg sync.WaitGroup
	errs := make(chan error, numBranches*filesPerBranch)
	for bIdx, bName := range branchNames {
		wg.Add(1)
		go func(branchIdx int, branch string) {
			defer wg.Done()
			for fIdx := range filesPerBranch {
				fname := fmt.Sprintf("f-%d-%d.bin", branchIdx, fIdx)
				ino, cErr := disk.Create(ctx, branch, RootIno, fname, 0o644, 0, 0)
				if cErr != nil {
					errs <- cErr
					return
				}
				payload := bytes.Repeat([]byte{byte(branchIdx*16 + fIdx + 1)}, 4096)
				if _, wErr := disk.Write(ctx, branch, ino.Ino, 0, payload); wErr != nil {
					errs <- wErr
					return
				}
			}
		}(bIdx, bName)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		require.NoError(t, e)
	}

	for bIdx, bName := range branchNames {
		entries, listErr := disk.ReadDir(ctx, bName, RootIno)
		require.NoError(t, listErr)
		require.Len(t, entries, filesPerBranch)
		for fIdx := range filesPerBranch {
			fname := fmt.Sprintf("f-%d-%d.bin", bIdx, fIdx)
			_, ino, lErr := disk.Lookup(ctx, bName, RootIno, fname)
			require.NoError(t, lErr)
			got, rErr := disk.Read(ctx, bName, ino.Ino, 0, 4096)
			require.NoError(t, rErr)
			require.Equal(t, bytes.Repeat([]byte{byte(bIdx*16 + fIdx + 1)}, 4096), got)
		}
	}
}

func TestStep8_Reset_RollsBackWorkingLayerFencesStaleEpochAndAllowsUndoReset(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	clk := newStepClock(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))

	disk, err := openTestDisk(testDiskOptions{
		Dir:       dir,
		DiskID:    "disk-reset",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "doc.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("state-1"))
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "c1")
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("state-2"))
	require.NoError(t, err)
	c2, err := disk.Commit(ctx, DefaultBranchName, "c2")
	require.NoError(t, err)

	mountRes, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	mount := mountRes.Mount
	staleCtx := WithWriterEpoch(ctx, mount.WriterEpoch)

	_, err = disk.Write(staleCtx, DefaultBranchName, f.Ino, 0, []byte("dirty-uncommitted"))
	require.NoError(t, err)

	clk.Advance(31 * time.Second)

	resetMeta, err := disk.Reset(ctx, DefaultBranchName, c1.ID)
	require.NoError(t, err)
	require.Equal(t, c1.ID, resetMeta.HeadCommitID)
	require.Greater(t, resetMeta.WriterEpoch, mount.WriterEpoch)

	got, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("state-1"), got)

	_, err = disk.Write(staleCtx, DefaultBranchName, f.Ino, 0, []byte("stale-write"))
	require.ErrorIs(t, err, ErrStaleEpoch)

	freshCtx := WithWriterEpoch(ctx, resetMeta.WriterEpoch)
	_, err = disk.Create(freshCtx, DefaultBranchName, RootIno, "after-reset.txt", 0o644, 0, 0)
	require.NoError(t, err)

	undoMeta, err := disk.Reset(ctx, DefaultBranchName, c2.ID)
	require.NoError(t, err)
	require.Equal(t, c2.ID, undoMeta.HeadCommitID)

	gotUndo, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("state-2"), gotUndo)

	_, _, err = disk.Lookup(ctx, DefaultBranchName, RootIno, "after-reset.txt")
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStep8_DeleteCommitAndPruneCommits_ProtectsReachableCommitsAndReclaimsUnreachable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       dir,
		DiskID:    "disk-prune",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "file.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("c1-data"))
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "c1")
	require.NoError(t, err)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, []byte("c2-data"))
	require.NoError(t, err)
	c2, err := disk.Commit(ctx, DefaultBranchName, "c2")
	require.NoError(t, err)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "temp-branch", "fork temp")
	require.NoError(t, err)
	_, err = disk.Write(ctx, "temp-branch", f.Ino, 0, []byte("c3-temp"))
	require.NoError(t, err)
	c3, err := disk.Commit(ctx, "temp-branch", "c3-temp")
	require.NoError(t, err)

	require.ErrorIs(t, disk.DeleteCommit(ctx, c1.ID), ErrCommitInUse)
	require.ErrorIs(t, disk.DeleteCommit(ctx, c2.ID), ErrCommitInUse)
	require.ErrorIs(t, disk.DeleteCommit(ctx, c3.ID), ErrCommitInUse)
	require.ErrorIs(t, disk.DeleteBranch(ctx, DefaultBranchName), ErrDefaultBranch)

	_, err = disk.Reset(ctx, DefaultBranchName, c1.ID)
	require.NoError(t, err)
	require.ErrorIs(t, disk.DeleteCommit(ctx, c2.ID), ErrCommitInUse)

	require.NoError(t, disk.DeleteBranch(ctx, "temp-branch"))
	_, err = disk.GetBranch("temp-branch")
	require.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, disk.DeleteCommit(ctx, c3.ID))
	_, err = disk.GetCommit(c3.ID)
	require.ErrorIs(t, err, ErrNotFound)

	pruned, err := disk.PruneCommits(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, pruned, 2)

	_, err = disk.GetCommit(c2.ID)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = disk.GetCommit(c1.ID)
	require.NoError(t, err)
	got, err := disk.Read(ctx, DefaultBranchName, f.Ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, []byte("c1-data"), got)
}

func TestStep8_CommitAndPruneCommits_ConcurrentNeverDeletesLiveCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	disk, err := openTestDisk(testDiskOptions{
		Dir:       dir,
		DiskID:    "disk-commit-prune-race",
		BlockSize: 4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "log.bin", 0o644, 0, 0)
	require.NoError(t, err)

	const commitCount = 60
	var (
		wg        sync.WaitGroup
		stopPrune = make(chan struct{})
		pruneErrs = make(chan error, 1)
	)

	wg.Go(func() {
		for {
			select {
			case <-stopPrune:
				return
			default:
				if _, pErr := disk.PruneCommits(ctx); pErr != nil {
					select {
					case pruneErrs <- pErr:
					default:
					}
					return
				}
			}
		}
	})

	commits := make([]CommitMeta, 0, commitCount)
	for i := range commitCount {
		payload := []byte(fmt.Sprintf("commit-payload-%03d", i))
		_, wErr := disk.Write(ctx, DefaultBranchName, f.Ino, 0, payload)
		require.NoError(t, wErr)
		cmt, cErr := disk.Commit(ctx, DefaultBranchName, fmt.Sprintf("c-%d", i))
		require.NoError(t, cErr)
		commits = append(commits, cmt)
	}

	close(stopPrune)
	wg.Wait()
	select {
	case pErr := <-pruneErrs:
		require.NoError(t, pErr)
	default:
	}

	for i, cmt := range commits {
		_, getErr := disk.GetCommit(cmt.ID)
		require.NoErrorf(t, getErr, "commit %d (id=%d) was deleted while reachable", i, cmt.ID)
		got, readErr := disk.ReadAtCommit(ctx, cmt.ID, f.Ino, 0, 64)
		require.NoErrorf(t, readErr, "commit %d (id=%d) data missing", i, cmt.ID)
		require.Equal(t, []byte(fmt.Sprintf("commit-payload-%03d", i)), got)
	}
}
