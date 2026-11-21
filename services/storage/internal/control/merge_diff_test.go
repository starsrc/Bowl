package control

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestFindLCA_LinearMultiMergeAndCrissCrossDAG(t *testing.T) {

	lineageID := uuid.Must(uuid.NewV7())
	s0 := uuid.Must(uuid.NewV7())
	s1 := uuid.Must(uuid.NewV7())
	s2 := uuid.Must(uuid.NewV7())
	s3 := uuid.Must(uuid.NewV7())
	s4 := uuid.Must(uuid.NewV7())
	s5 := uuid.Must(uuid.NewV7())

	snapshots := []db.SnapshotRecord{
		{LineageID: lineageID, SnapshotID: s0, Kind: db.SnapshotKindFork},
		{LineageID: lineageID, SnapshotID: s1, ParentSnapshotID: s0, Kind: db.SnapshotKindFork},
		{LineageID: lineageID, SnapshotID: s2, ParentSnapshotID: s0, Kind: db.SnapshotKindFork},
		{LineageID: lineageID, SnapshotID: s3, ParentSnapshotID: s1, MergeParentSnapshotID: s2, Kind: db.SnapshotKindMerge},
		{LineageID: lineageID, SnapshotID: s4, ParentSnapshotID: s2, MergeParentSnapshotID: s1, Kind: db.SnapshotKindMerge},
		{LineageID: lineageID, SnapshotID: s5, ParentSnapshotID: s3, Kind: db.SnapshotKindFork},
	}

	gotSame, err := findLCA(snapshots, s1, s1)
	require.NoError(t, err)
	require.Equal(t, s1, gotSame)

	gotMerged, err := findLCA(snapshots, s5, s2)
	require.NoError(t, err)
	require.Equal(t, s2, gotMerged)

	_, err = findLCA(snapshots, s3, s4)
	require.ErrorIs(t, err, ErrAmbiguousLCA)
}

func TestDiffDisk_SelfBaseAndTargetDisk_UsesCheckpointForOnlineRWActors(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 64 << 20})
	require.NoError(t, err)
	srcActor, err := mgr.Actor(ctx, src.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, srcActor, 0, "base.txt", []byte("base-v1\n"))
	writeRegularFile(t, ctx, srcActor, 0, "shared.txt", []byte("base-shared\n"))

	child, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "feat"})
	require.NoError(t, err)

	childActor, err := mgr.Actor(ctx, child.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, childActor, 0, "feat.txt", []byte("feature-added\n"))
	overwriteRegularFile(t, ctx, childActor, 0, "shared.txt", []byte("child-shared\n"))
	overwriteRegularFile(t, ctx, srcActor, 0, "shared.txt", []byte("main-shared\n"))

	twoWay, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: child.DiskID,
	})

	require.NoError(t, err)
	require.NotEmpty(t, twoWay.LCASnapshotID)
	require.Equal(t, uint64(1), twoWay.Summary.Added)
	require.Equal(t, uint64(1), twoWay.Summary.Modified)
	require.Equal(t, uint64(0), twoWay.Summary.Conflicts)
	require.Len(t, twoWay.Entries, 2)

	threeWay, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID:  child.DiskID,
		TargetDiskID:  src.DiskID,
		ConflictsOnly: true,
	})

	require.NoError(t, err)
	require.Equal(t, twoWay.LCASnapshotID, threeWay.LCASnapshotID)
	require.Equal(t, uint64(1), threeWay.Summary.Conflicts)
	require.Len(t, threeWay.Entries, 1)
	require.Equal(t, "shared.txt", string(threeWay.Entries[0].Path))
	require.Equal(t, store.ConflictContent, threeWay.Entries[0].Conflict)

	unrelated, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "other", MaxBytes: 16 << 20})
	require.NoError(t, err)
	_, err = mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: child.DiskID,
		TargetDiskID: unrelated.DiskID,
	})
	require.ErrorIs(t, err, ErrCrossLineage)

	tmpEntries, readErr := os.ReadDir(filepath.Join(rootDir, ".tmp"))
	if readErr == nil {
		require.Empty(t, tmpEntries, "DiffDisk 完成后不应残留临时快照目录")
	}
}

func TestReadSnapshotNode_EnforcesLineageMembershipAndReadsImmutableSlice(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
	require.NoError(t, err)
	mainActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	ino := writeRegularFile(t, ctx, mainActor, 0, "doc.txt", []byte("hello-immutable-snapshot"))
	symAttr, err := mainActor.Symlink(ctx, store.SymlinkReq{
		ParentIno: RootIno,
		Name:      []byte("link.txt"),
		Target:    []byte("/doc.txt"),
	})
	require.NoError(t, err)

	forkDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "branch"})
	require.NoError(t, err)
	otherDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "unrelated", MaxBytes: 16 << 20})
	require.NoError(t, err)

	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{SourceDiskID: forkDisk.DiskID})
	require.NoError(t, err)
	snapshotID := diffRes.LCASnapshotID

	overwriteRegularFile(t, ctx, mainActor, 0, "doc.txt", []byte("mutated-after-fork"))

	slice1, err := mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     forkDisk.DiskID,
		SnapshotID: snapshotID,
		Ino:        ino,
		Offset:     0,
		Size:       5,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), slice1.Data)
	require.False(t, slice1.EOF)
	require.Equal(t, uint64(len("hello-immutable-snapshot")), slice1.FileSize)

	slice2, err := mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     forkDisk.DiskID,
		SnapshotID: snapshotID,
		Ino:        ino,
		Offset:     5,
		Size:       100,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("-immutable-snapshot"), slice2.Data)
	require.True(t, slice2.EOF)

	symRes, err := mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     mainDisk.DiskID,
		SnapshotID: snapshotID,
		Ino:        symAttr.Ino,
		Offset:     0,
		Size:       1024,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("/doc.txt"), symRes.SymlinkTarget)

	_, err = mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
		DiskID:     otherDisk.DiskID,
		SnapshotID: snapshotID,
		Ino:        ino,
		Offset:     0,
		Size:       16,
	})
	require.ErrorIs(t, err, ErrCrossLineage)
}

func TestMergeDisk_DryRunConflictResolutionEpochFenceAndAlreadyUpToDate(t *testing.T) {

	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "cfg.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)

	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID})
	require.NoError(t, err)
	epoch0 := mnt.Epoch

	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	overwriteRegularFile(t, ctx, sActor, 0, "cfg.txt", []byte("from-source"))
	writeRegularFile(t, ctx, sActor, 0, "new.txt", []byte("new-from-source"))

	overwriteRegularFile(t, ctx, tActor, epoch0, "cfg.txt", []byte("from-target"))

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.ErrorIs(t, err, ErrDiskMounted)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		Force:        true,
	})
	var conflictErr *MergeConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.Equal(t, uint64(1), conflictErr.Report.TotalConflicts)
	require.Equal(t, []byte("from-target"), readRegularFile(t, ctx, tActor, "cfg.txt", 1024))

	tRow, err := memDB.GetDisk(ctx, uuid.MustParse(target.DiskID))
	require.NoError(t, err)
	lineageID := tRow.LineageID
	snapsBefore, err := memDB.ListLineageSnapshots(ctx, lineageID)
	require.NoError(t, err)

	newMaxBytes := uint64(64 << 20)
	dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyOurs,
		DryRun:       true,
		MaxBytes:     &newMaxBytes,
	})
	require.NoError(t, err)
	require.Empty(t, dryRes.MergeSnapshotID)
	require.Nil(t, dryRes.Disk)
	require.Equal(t, newMaxBytes, dryRes.TargetMaxBytes)

	snapsAfterDry, err := memDB.ListLineageSnapshots(ctx, lineageID)
	require.NoError(t, err)
	require.Len(t, snapsAfterDry, len(snapsBefore))

	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   source.DiskID,
		TargetDiskID:   target.DiskID,
		Strategy:       store.MergeStrategyOurs,
		PathStrategies: map[string]store.MergeStrategy{"/cfg.txt": store.MergeStrategyTheirs},
		Force:          true,
		MaxBytes:       &newMaxBytes,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeSnapshotID)
	require.NotNil(t, res.Disk)
	require.Equal(t, newMaxBytes, res.Disk.MaxBytes)

	newTActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, epoch0+1, newTActor.WriterEpoch())
	require.Equal(t, []byte("from-source"), readRegularFile(t, ctx, newTActor, "cfg.txt", 1024))
	require.Equal(t, []byte("new-from-source"), readRegularFile(t, ctx, newTActor, "new.txt", 1024))

	_, staleErr := newTActor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("stale.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch0,
	})
	require.ErrorIs(t, staleErr, store.ErrStaleEpoch)

	cleanBranch, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "clean"})
	require.NoError(t, err)
	snapsPreClean, err := memDB.ListLineageSnapshots(ctx, lineageID)
	require.NoError(t, err)

	cleanRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: cleanBranch.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.Equal(t, cleanRes.LCASnapshotID, cleanRes.MergeSnapshotID)

	snapsPostClean, err := memDB.ListLineageSnapshots(ctx, lineageID)
	require.NoError(t, err)
	require.Len(t, snapsPostClean, len(snapsPreClean), "already up-to-date merge must create 0 new snapshots")
}

func TestMergeDisk_Phase2CASConcurrentWrite_AbortsAndCleansStaging(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr, ok := New(db.NewMemStore(), WithStorageRoot(rootDir)).(*manager)
	require.True(t, ok)
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "from-source.txt", []byte("source"))

	mgr.beforePhase2CutoverHook = func() {
		writeRegularFile(t, ctx, tActor, 0, "concurrent.txt", []byte("live-write"))
	}

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})

	require.ErrorIs(t, err, ErrConcurrentMerge)

	entries, readErr := os.ReadDir(filepath.Join(rootDir, "disks"))
	require.NoError(t, readErr)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".merge.", "CAS 中止后不应残留 .merge 临时目录: %s", e.Name())
		require.NotContains(t, e.Name(), ".old.", "CAS 中止后不应残留 .old 目录: %s", e.Name())
		require.NotContains(t, e.Name(), ".srcsnap.", "CAS 中止后不应残留 .srcsnap 标记: %s", e.Name())
	}

	reloaded, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("live-write"), readRegularFile(t, ctx, reloaded, "concurrent.txt", 1024))
}

func TestOpenDiskActor_ReconcilePendingMerge_RollForwardAndRollback(t *testing.T) {
	ctx := context.Background()

	t.Run("PG已提交S_merge时崩溃前滚", func(t *testing.T) {

		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "old.txt", []byte("old-data"))

		source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
		require.NoError(t, err)
		sActor, err := mgr1.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "merged.txt", []byte("new-merged-data"))

		targetID := uuid.MustParse(target.DiskID)
		sourceID := uuid.MustParse(source.DiskID)
		tRow, err := memDB.GetDisk(ctx, targetID)
		require.NoError(t, err)
		sRow, err := memDB.GetDisk(ctx, sourceID)
		require.NoError(t, err)

		sSrc := uuid.Must(uuid.NewV7())
		sMerge := uuid.Must(uuid.NewV7())
		snapSrc := filepath.Join(rootDir, "tmp-src")
		snapTgt := filepath.Join(rootDir, "tmp-tgt")
		_, err = sActor.Checkpoint(ctx, snapSrc)
		require.NoError(t, err)
		_, err = tActor.Checkpoint(ctx, snapTgt)
		require.NoError(t, err)

		stagingDir := mgr1.stagingMergeDir(target.DiskID, sMerge)
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            mgr1.snapshotDir(tRow.BaseSnapshotID.String()),
			TargetRoot:          snapTgt,
			SourceRoot:          snapSrc,
			StagingDir:          stagingDir,
			Strategy:            store.MergeStrategyFail,
			ExpectedWriterEpoch: tActor.WriterEpoch(),
		})
		require.NoError(t, err)
		require.NoError(t, store.CloneCheckpointDir(ctx, snapSrc, mgr1.snapshotDir(sSrc.String())))
		require.NoError(t, store.CloneCheckpointDir(ctx, stagingDir, mgr1.snapshotDir(sMerge.String())))

		_, err = memDB.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    tRow.LineageID,
			SourceDiskID:                 sourceID,
			TargetDiskID:                 targetID,
			ExpectedSourceBaseSnapshotID: sRow.BaseSnapshotID,
			ExpectedTargetBaseSnapshotID: tRow.BaseSnapshotID,
			NewSourceSnapshotID:          sSrc,
			NewMergeSnapshotID:           sMerge,
		})
		require.NoError(t, err)
		require.NoError(t, mgr1.Close())
		_ = os.RemoveAll(snapSrc)
		_ = os.RemoveAll(snapTgt)

		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })

		reopened, err := mgr2.Actor(ctx, target.DiskID)

		require.NoError(t, err)
		require.Equal(t, []byte("new-merged-data"), readRegularFile(t, ctx, reopened, "merged.txt", 1024))
		_, statErr := os.Stat(stagingDir)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("PG未提交S_merge时崩溃回滚清理", func(t *testing.T) {

		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "keep.txt", []byte("keep-original"))
		require.NoError(t, mgr1.Close())

		uncommittedID := uuid.Must(uuid.NewV7())
		staleStaging := mgr1.stagingMergeDir(target.DiskID, uncommittedID)
		staleOld := mgr1.oldMergeDir(target.DiskID, uncommittedID)
		require.NoError(t, os.MkdirAll(staleStaging, 0o755))
		require.NoError(t, os.MkdirAll(staleOld, 0o755))

		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })

		reopened, err := mgr2.Actor(ctx, target.DiskID)

		require.NoError(t, err)
		require.Equal(t, []byte("keep-original"), readRegularFile(t, ctx, reopened, "keep.txt", 1024))
		_, errStaging := os.Stat(staleStaging)
		require.ErrorIs(t, errStaging, os.ErrNotExist)
		_, errOld := os.Stat(staleOld)
		require.ErrorIs(t, errOld, os.ErrNotExist)
	})
}

func TestReadSnapshotNode_ConcurrentWithDeleteDisk_NoRace(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mainActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	ino := writeRegularFile(t, ctx, mainActor, 0, "data.bin", []byte("concurrent-safe-data"))

	childDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "child"})
	require.NoError(t, err)
	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{SourceDiskID: childDisk.DiskID})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 10 {
			_, _ = mgr.ReadSnapshotNode(ctx, &ReadSnapshotNodeParams{
				DiskID:     childDisk.DiskID,
				SnapshotID: diffRes.LCASnapshotID,
				Ino:        ino,
				Offset:     0,
				Size:       64,
			})
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		_ = mgr.DeleteDisk(ctx, DeleteDiskParams{DiskID: mainDisk.DiskID})
	}()
	wg.Wait()
}

func TestMergeDisk_AllocatesLineageSegmentsOnFormalMergeAndSkipsOnDryRun(t *testing.T) {

	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	inoA := writeRegularFile(t, ctx, tActor, 0, "a.txt", []byte("base-content"))
	_, err = tActor.Link(ctx, store.LinkReq{
		Ino:          inoA,
		NewParentIno: RootIno,
		NewName:      []byte("b.txt"),
	})
	require.NoError(t, err)

	sourceDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, sourceDisk.DiskID)
	require.NoError(t, err)

	overwriteRegularFile(t, ctx, tActor, 0, "a.txt", []byte("target-content"))
	overwriteRegularFile(t, ctx, sActor, 0, "a.txt", []byte("source-content"))

	tRec, err := memDB.GetDisk(ctx, uuid.MustParse(mainDisk.DiskID))
	require.NoError(t, err)
	lineageID := tRec.LineageID

	pathStrategies := map[string]store.MergeStrategy{
		"/a.txt": store.MergeStrategyTheirs,
		"/b.txt": store.MergeStrategyOurs,
	}

	snapBeforeDry := memDB.Inspect()
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   sourceDisk.DiskID,
		TargetDiskID:   mainDisk.DiskID,
		Strategy:       store.MergeStrategyFail,
		PathStrategies: pathStrategies,
		DryRun:         true,
	})
	require.NoError(t, err)
	snapAfterDry := memDB.Inspect()
	require.Equal(t, snapBeforeDry.InodeReserveCalls, snapAfterDry.InodeReserveCalls, "DryRun 不得推进 Inode 预留调用次数")
	require.Equal(t, snapBeforeDry.Lineages[lineageID].NextIno, snapAfterDry.Lineages[lineageID].NextIno, "DryRun 不得推进 DB NextIno 水位")

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   sourceDisk.DiskID,
		TargetDiskID:   mainDisk.DiskID,
		Strategy:       store.MergeStrategyFail,
		PathStrategies: pathStrategies,
	})
	require.NoError(t, err)
	snapAfterMerge := memDB.Inspect()
	require.Greater(t, snapAfterMerge.InodeReserveCalls, snapAfterDry.InodeReserveCalls, "正式合并重分配 Inode 必须调 ReserveSegment")
	require.Greater(t, snapAfterMerge.Lineages[lineageID].NextIno, snapAfterDry.Lineages[lineageID].NextIno, "正式合并必须推进 DB 血缘 NextIno 水位")
}

func TestMergeDisk_SanitizedOrphanBaseSnapshot_WithSingleWriteFastForwardsOneSnapshot(t *testing.T) {

	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	srcDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "src-with-orphan", MaxBytes: 32 << 20})
	require.NoError(t, err)
	srcActor, err := mgr.Actor(ctx, srcDisk.DiskID)
	require.NoError(t, err)
	dataIno := writeRegularFile(t, ctx, srcActor, 0, "data.txt", []byte("v0"))
	writeRegularFile(t, ctx, srcActor, 0, "orphan.bin", []byte("orphan-payload"))
	require.NoError(t, srcActor.Unlink(ctx, RootIno, []byte("orphan.bin"), true))

	tgtDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: srcDisk.DiskID, Name: "tgt"})
	require.NoError(t, err)

	_, _, err = srcActor.Write(ctx, dataIno, 0, []byte("v1"), 0)
	require.NoError(t, err)

	srcID := uuid.MustParse(srcDisk.DiskID)
	srcRecBefore, err := memDB.GetDisk(ctx, srcID)
	require.NoError(t, err)
	snapsBefore, err := memDB.ListLineageSnapshots(ctx, srcRecBefore.LineageID)
	require.NoError(t, err)

	mergeRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: srcDisk.DiskID,
		TargetDiskID: tgtDisk.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)

	srcRecAfter, err := memDB.GetDisk(ctx, srcID)
	require.NoError(t, err)
	require.NotEqual(t, srcRecBefore.BaseSnapshotID, srcRecAfter.BaseSnapshotID, "源盘发生真实写入后快进必须创建 S_ff 并推进 BaseSnapshotID")
	require.Equal(t, mergeRes.MergeSnapshotID, srcRecAfter.BaseSnapshotID.String())
	snapsAfter, err := memDB.ListLineageSnapshots(ctx, srcRecBefore.LineageID)
	require.NoError(t, err)
	require.Len(t, snapsAfter, len(snapsBefore)+1, "单边快进应仅新增 1 个 merge 快照")
	require.Equal(t, db.SnapshotKindMerge, snapsAfter[len(snapsAfter)-1].Kind)

	_, _, err = srcActor.Write(ctx, dataIno, 0, []byte("v2"), 0)
	require.NoError(t, err)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: srcDisk.DiskID,
		TargetDiskID: tgtDisk.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
}

func TestCrashRecovery_DeleteSourceBeforeReopenTargetAndPreCommitSrcCleanup(t *testing.T) {
	ctx := context.Background()

	t.Run("PG提交S_merge并在Rename旧盘后崩溃_先删源盘清空BaseSnapshotID再开目标盘仍前滚保数据", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "old.txt", []byte("old-data"))

		source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
		require.NoError(t, err)
		sActor, err := mgr1.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "merged.txt", []byte("must-survive"))

		targetID := uuid.MustParse(target.DiskID)
		sourceID := uuid.MustParse(source.DiskID)
		tRow, err := memDB.GetDisk(ctx, targetID)
		require.NoError(t, err)
		sRow, err := memDB.GetDisk(ctx, sourceID)
		require.NoError(t, err)

		sSrc := uuid.Must(uuid.NewV7())
		sMerge := uuid.Must(uuid.NewV7())
		snapSrc := filepath.Join(rootDir, "tmp-src")
		snapTgt := filepath.Join(rootDir, "tmp-tgt")
		_, err = sActor.Checkpoint(ctx, snapSrc)
		require.NoError(t, err)
		_, err = tActor.Checkpoint(ctx, snapTgt)
		require.NoError(t, err)

		stagingDir := mgr1.stagingMergeDir(target.DiskID, sMerge)
		oldDir := mgr1.oldMergeDir(target.DiskID, sMerge)
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            mgr1.snapshotDir(tRow.BaseSnapshotID.String()),
			TargetRoot:          snapTgt,
			SourceRoot:          snapSrc,
			StagingDir:          stagingDir,
			Strategy:            store.MergeStrategyFail,
			ExpectedWriterEpoch: tActor.WriterEpoch(),
		})
		require.NoError(t, err)
		require.NoError(t, store.CloneCheckpointDir(ctx, snapSrc, mgr1.snapshotDir(sSrc.String())))
		require.NoError(t, store.CloneCheckpointDir(ctx, stagingDir, mgr1.snapshotDir(sMerge.String())))

		_, err = memDB.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    tRow.LineageID,
			SourceDiskID:                 sourceID,
			TargetDiskID:                 targetID,
			ExpectedSourceBaseSnapshotID: sRow.BaseSnapshotID,
			ExpectedTargetBaseSnapshotID: tRow.BaseSnapshotID,
			NewSourceSnapshotID:          sSrc,
			NewMergeSnapshotID:           sMerge,
		})
		require.NoError(t, err)
		require.NoError(t, mgr1.Close())
		_ = os.RemoveAll(snapSrc)
		_ = os.RemoveAll(snapTgt)

		require.NoError(t, os.Rename(mgr1.diskDir(target.DiskID), oldDir))

		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })
		require.NoError(t, mgr2.DeleteDisk(ctx, DeleteDiskParams{DiskID: source.DiskID}))

		reopened, err := mgr2.Actor(ctx, target.DiskID)
		require.NoError(t, err)

		require.Equal(t, []byte("must-survive"), readRegularFile(t, ctx, reopened, "merged.txt", 1024))
		_, errStg := os.Stat(stagingDir)
		require.ErrorIs(t, errStg, os.ErrNotExist)
		_, errOld := os.Stat(oldDir)
		require.ErrorIs(t, errOld, os.ErrNotExist)
	})

	t.Run("PG提交前物化S_src与S_merge后崩溃_回滚并同步清理未提交snapshots与标记", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "keep.txt", []byte("keep-data"))
		require.NoError(t, mgr1.Close())

		sMerge := uuid.Must(uuid.NewV7())
		sSrc := uuid.Must(uuid.NewV7())
		stagingDir := mgr1.stagingMergeDir(target.DiskID, sMerge)
		mergeSnapDir := mgr1.snapshotDir(sMerge.String())
		srcSnapDir := mgr1.snapshotDir(sSrc.String())
		markerPath := mgr1.srcSnapshotMarkerPath(target.DiskID, sMerge, sSrc)

		require.NoError(t, os.MkdirAll(stagingDir, 0o755))
		require.NoError(t, os.MkdirAll(mergeSnapDir, 0o755))
		require.NoError(t, os.MkdirAll(srcSnapDir, 0o755))
		require.NoError(t, os.WriteFile(markerPath, nil, 0o644))

		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })
		reopened, err := mgr2.Actor(ctx, target.DiskID)
		require.NoError(t, err)

		require.Equal(t, []byte("keep-data"), readRegularFile(t, ctx, reopened, "keep.txt", 1024))
		for _, leftover := range []string{stagingDir, mergeSnapDir, srcSnapDir, markerPath} {
			_, statErr := os.Stat(leftover)
			require.ErrorIs(t, statErr, os.ErrNotExist, "回滚后不得残留未提交产物: %s", leftover)
		}
	})
}
