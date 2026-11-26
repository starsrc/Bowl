package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestNodeStore_BloomFilterAfterFlushAndCrossDiskIsolation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()

	node, err := OpenNode(NodeOptions{
		DataDir:     dataDir,
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
		GCMode:      GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(ctx))
	})

	infoA, err := node.CreateDisk(ctx, "disk-a", 16<<20)
	require.NoError(t, err)
	idA := uuid.MustParse(infoA.DiskID)

	infoB, err := node.CreateDisk(ctx, "disk-b", 16<<20)
	require.NoError(t, err)
	idB := uuid.MustParse(infoB.DiskID)

	packsA, err := os.Stat(filepath.Join(dataDir, "disks", idA.String(), "packs"))
	require.NoError(t, err)
	require.True(t, packsA.IsDir())

	payloadA := bytes.Repeat([]byte("A"), 4096)
	payloadB := bytes.Repeat([]byte("B"), 4096)
	var fileAIno, fileBIno uint64

	require.NoError(t, node.WithDisk(ctx, idA, func(ds *DiskStore) error {
		f, cErr := ds.Create(ctx, DefaultBranchName, RootIno, "only-in-a.txt", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		fileAIno = f.Ino
		if _, wErr := ds.Write(ctx, DefaultBranchName, f.Ino, 0, payloadA); wErr != nil {
			return wErr
		}
		if xErr := ds.SetXattr(ctx, DefaultBranchName, f.Ino, "user.disk", []byte("a"), 0); xErr != nil {
			return xErr
		}
		return ds.Fsync(ctx, DefaultBranchName, f.Ino)
	}))

	require.NoError(t, node.WithDisk(ctx, idB, func(ds *DiskStore) error {
		f, cErr := ds.Create(ctx, DefaultBranchName, RootIno, "only-in-b.txt", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		fileBIno = f.Ino
		if _, wErr := ds.Write(ctx, DefaultBranchName, f.Ino, 0, payloadB); wErr != nil {
			return wErr
		}
		if xErr := ds.SetXattr(ctx, DefaultBranchName, f.Ino, "user.disk", []byte("b"), 0); xErr != nil {
			return xErr
		}
		return ds.Fsync(ctx, DefaultBranchName, f.Ino)
	}))

	_, _, err = node.ForkBranch(ctx, idA, DefaultBranchName, "feat-a", "fork on A")
	require.NoError(t, err)

	require.NoError(t, node.db.Flush())

	require.NoError(t, node.WithDisk(ctx, idA, func(ds *DiskStore) error {
		entries, rErr := ds.ReadDir(ctx, DefaultBranchName, RootIno)
		require.NoError(t, rErr)
		require.Equal(t, []string{"only-in-a.txt"}, dirEntryNames(entries))

		_, _, lErr := ds.Lookup(ctx, DefaultBranchName, RootIno, "only-in-b.txt")
		require.ErrorIs(t, lErr, ErrNotFound)

		got, dErr := ds.Read(ctx, DefaultBranchName, fileAIno, 0, len(payloadA))
		require.NoError(t, dErr)
		require.Equal(t, payloadA, got)

		xval, xErr := ds.GetXattr(ctx, DefaultBranchName, fileAIno, "user.disk")
		require.NoError(t, xErr)
		require.Equal(t, []byte("a"), xval)
		return nil
	}))

	require.NoError(t, node.WithDisk(ctx, idB, func(ds *DiskStore) error {
		entries, rErr := ds.ReadDir(ctx, DefaultBranchName, RootIno)
		require.NoError(t, rErr)
		require.Equal(t, []string{"only-in-b.txt"}, dirEntryNames(entries))

		_, _, lErr := ds.Lookup(ctx, DefaultBranchName, RootIno, "only-in-a.txt")
		require.ErrorIs(t, lErr, ErrNotFound)

		got, dErr := ds.Read(ctx, DefaultBranchName, fileBIno, 0, len(payloadB))
		require.NoError(t, dErr)
		require.Equal(t, payloadB, got)

		xval, xErr := ds.GetXattr(ctx, DefaultBranchName, fileBIno, "user.disk")
		require.NoError(t, xErr)
		require.Equal(t, []byte("b"), xval)
		return nil
	}))

	branchesB, err := node.ListBranches(ctx, idB)
	require.NoError(t, err)
	require.Len(t, branchesB, 1)
	require.Equal(t, DefaultBranchName, branchesB[0].Name)

	snapMetrics := node.MetricsSnapshot()
	require.NotNil(t, snapMetrics.Pebble)
	require.Positive(t, snapMetrics.Pebble.Filter.Hits+snapMetrics.Pebble.Filter.Misses)

	require.NoError(t, node.DeleteDisk(ctx, idA))

	_, err = node.GetDisk(ctx, idA)
	require.ErrorIs(t, err, ErrNotFound)
	_, statErr := os.Stat(filepath.Join(dataDir, "disks", idA.String()))
	require.True(t, os.IsNotExist(statErr))

	dkA := newDiskKeys(idA)
	startA, endA := dkA.RangeBounds()
	iter, err := node.db.NewIter(nil)
	require.NoError(t, err)
	foundA := false
	for iter.SeekGE(startA); iter.Valid() && bytes.Compare(iter.Key(), endA) < 0; iter.Next() {
		foundA = true
	}
	require.NoError(t, iter.Close())
	require.False(t, foundA)

	gotB, err := node.GetDisk(ctx, idB)
	require.NoError(t, err)
	require.Equal(t, idB.String(), gotB.DiskID)
	require.Equal(t, uint64(4096), gotB.UsedBytes)

	require.NoError(t, node.WithDisk(ctx, idB, func(ds *DiskStore) error {
		got, dErr := ds.Read(ctx, DefaultBranchName, fileBIno, 0, len(payloadB))
		require.NoError(t, dErr)
		require.Equal(t, payloadB, got)
		return nil
	}))
}

func TestNodeStore_CrossDiskWALBarrier_ConcurrentWriteAndSync_Race(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	node, err := OpenNode(NodeOptions{
		DataDir:     t.TempDir(),
		BlockSize:   4096,
		MaxPackSize: 64 << 10,
		GCMode:      GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(ctx))
	})

	infoA, err := node.CreateDisk(ctx, "disk-writer", 64<<20)
	require.NoError(t, err)
	idA := uuid.MustParse(infoA.DiskID)

	infoB, err := node.CreateDisk(ctx, "disk-syncer", 64<<20)
	require.NoError(t, err)
	idB := uuid.MustParse(infoB.DiskID)

	var (
		dsA, dsB *DiskStore
		fA, fB   Inode
	)
	require.NoError(t, node.WithDisk(ctx, idA, func(ds *DiskStore) error {
		dsA = ds
		var cErr error
		fA, cErr = ds.Create(ctx, DefaultBranchName, RootIno, "a.bin", 0o644, 0, 0)
		return cErr
	}))
	require.NoError(t, node.WithDisk(ctx, idB, func(ds *DiskStore) error {
		dsB = ds
		var cErr error
		fB, cErr = ds.Create(ctx, DefaultBranchName, RootIno, "b.bin", 0o644, 0, 0)
		return cErr
	}))

	var (
		mu             sync.Mutex
		appendedSeqA   uint64
		committedSeqA  uint64
		syncedPackSeqA uint64
		violations     int
		walSyncCount   int
		packSyncCountA int
	)

	origOnDirty := dsA.packs.onDirty
	dsA.packs.onDirty = func() {
		mu.Lock()
		appendedSeqA++
		mu.Unlock()
		origOnDirty()
	}

	dsA.packs.syncFileHook = func(_ uint64) error {
		mu.Lock()
		packSyncCountA++
		syncedPackSeqA = appendedSeqA
		mu.Unlock()
		return nil
	}

	dsA.walCommitHook = func(isSync bool) {
		if !isSync {
			mu.Lock()
			committedSeqA = appendedSeqA
			mu.Unlock()
		}
	}

	node.walCommitHook = func(isSync bool) {
		if !isSync {
			return
		}
		mu.Lock()
		walSyncCount++
		if committedSeqA > syncedPackSeqA {
			violations++
		}
		mu.Unlock()
	}

	const iterations = 40
	var wg sync.WaitGroup
	var errA, errB error

	wg.Go(func() {
		blk := bytes.Repeat([]byte("W"), 4096)
		for i := range iterations {
			if _, wErr := dsA.Write(ctx, DefaultBranchName, fA.Ino, int64(i)*4096, blk); wErr != nil {
				errA = wErr
				return
			}
		}
	})

	wg.Go(func() {
		for i := range iterations {
			if i%2 == 0 {
				if sErr := dsB.Fsync(ctx, DefaultBranchName, fB.Ino); sErr != nil {
					errB = sErr
					return
				}
			} else {
				if _, cErr := dsB.Commit(ctx, DefaultBranchName, "sync-barrier"); cErr != nil {
					errB = cErr
					return
				}
			}
		}
	})

	wg.Wait()
	require.NoError(t, errA)
	require.NoError(t, errB)

	require.NoError(t, dsB.Fsync(ctx, DefaultBranchName, fB.Ino))

	mu.Lock()
	gotViolations := violations
	gotWalSyncs := walSyncCount
	gotPackSyncsA := packSyncCountA
	mu.Unlock()

	require.Zero(t, gotViolations)
	require.GreaterOrEqual(t, gotWalSyncs, iterations/2)
	require.Positive(t, gotPackSyncsA)
	require.False(t, dsA.packs.hasDirty())
	require.False(t, node.hasDirtyPack(idA))
}

func TestNodeStore_LazyLoadColdReadsAndIdleEviction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	clk := newStepClock(time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC))

	node1, err := OpenNode(NodeOptions{
		DataDir:       dataDir,
		BlockSize:     4096,
		MaxPackSize:   64 << 10,
		Now:           clk.Now,
		MountLeaseTTL: 5 * time.Minute,
		GCMode:        GCModeDisabled,
	})
	require.NoError(t, err)

	infoA, err := node1.CreateDisk(ctx, "cold-unmounted", 16<<20)
	require.NoError(t, err)
	idA := uuid.MustParse(infoA.DiskID)
	clk.Advance(time.Second)

	infoB, err := node1.CreateDisk(ctx, "cold-mounted", 16<<20)
	require.NoError(t, err)
	idB := uuid.MustParse(infoB.DiskID)

	var fileAIno uint64
	require.NoError(t, node1.WithDisk(ctx, idA, func(ds *DiskStore) error {
		f, cErr := ds.Create(ctx, DefaultBranchName, RootIno, "a.bin", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		fileAIno = f.Ino
		_, wErr := ds.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("A"), 4096))
		return wErr
	}))
	_, err = node1.Commit(ctx, idA, DefaultBranchName, "commit-a1")
	require.NoError(t, err)
	require.NoError(t, node1.WithDisk(ctx, idA, func(ds *DiskStore) error {
		_, wErr := ds.Write(ctx, DefaultBranchName, fileAIno, 4096, bytes.Repeat([]byte("a"), 4096))
		return wErr
	}))
	_, _, err = node1.ForkBranch(ctx, idA, DefaultBranchName, "feat", "fork-a")
	require.NoError(t, err)

	require.NoError(t, node1.WithDisk(ctx, idB, func(ds *DiskStore) error {
		f, cErr := ds.Create(ctx, DefaultBranchName, RootIno, "b.bin", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		_, wErr := ds.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte("B"), 8192))
		return wErr
	}))
	mountB, err := node1.MountBranch(ctx, idB, DefaultBranchName, false, false)
	require.NoError(t, err)
	require.NoError(t, node1.Close(ctx))

	node2, err := OpenNode(NodeOptions{
		DataDir:       dataDir,
		BlockSize:     4096,
		MaxPackSize:   64 << 10,
		Now:           clk.Now,
		MountLeaseTTL: 5 * time.Minute,
		DiskIdleTTL:   10 * time.Second,
		GCMode:        GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node2.Close(ctx))
	})

	require.Zero(t, node2.MetricsSnapshot().DisksLoaded)

	page, err := node2.ListDisks(ctx, ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, page.Disks, 2)
	require.Equal(t, idB.String(), page.Disks[0].DiskID)
	require.Equal(t, uint64(8192), page.Disks[0].UsedBytes)
	require.Equal(t, idA.String(), page.Disks[1].DiskID)
	require.Equal(t, uint64(8192), page.Disks[1].UsedBytes)
	require.Zero(t, node2.MetricsSnapshot().DisksLoaded)

	gotA, err := node2.GetDisk(ctx, idA)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), gotA.UsedBytes)

	branchesA, err := node2.ListBranches(ctx, idA)
	require.NoError(t, err)
	require.Len(t, branchesA, 2)

	commitsA, err := node2.ListCommits(ctx, ListCommitsQuery{DiskID: idA})
	require.NoError(t, err)
	require.Len(t, commitsA.Commits, 3)

	mountsB, err := node2.ListMounts(ctx, idB, "")
	require.NoError(t, err)
	require.Len(t, mountsB, 1)
	require.Equal(t, mountB.Mount.ID, mountsB[0].ID)

	require.Zero(t, node2.MetricsSnapshot().DisksLoaded)

	dirtyPayload := bytes.Repeat([]byte("D"), 4096)
	require.NoError(t, node2.WithDisk(ctx, idA, func(ds *DiskStore) error {
		_, wErr := ds.Write(ctx, DefaultBranchName, fileAIno, 8192, dirtyPayload)
		return wErr
	}))
	require.True(t, node2.hasDirtyPack(idA))

	require.NoError(t, node2.WithDisk(ctx, idB, func(ds *DiskStore) error {
		_, cErr := ds.Create(ctx, DefaultBranchName, RootIno, "b2.bin", 0o644, 0, 0)
		return cErr
	}))
	require.Equal(t, 2, node2.MetricsSnapshot().DisksLoaded)

	clk.Advance(15 * time.Second)
	require.Eventually(t, func() bool {
		m := node2.MetricsSnapshot()
		return m.DisksLoaded == 1
	}, 2*time.Second, 5*time.Millisecond)

	require.False(t, node2.hasDirtyPack(idA))
	require.Nil(t, lookupLoadedEntry(node2.getSlot(idA), idA))
	require.NotNil(t, lookupLoadedEntry(node2.getSlot(idB), idB))

	require.NoError(t, node2.WithDisk(ctx, idA, func(ds *DiskStore) error {
		got, rErr := ds.Read(ctx, DefaultBranchName, fileAIno, 8192, len(dirtyPayload))
		require.NoError(t, rErr)
		require.Equal(t, dirtyPayload, got)
		return nil
	}))
	require.Equal(t, 2, node2.MetricsSnapshot().DisksLoaded)
}

func TestNodeStore_CatalogPagination_TieBreakUpdateDeleteAndInvalidCursor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC))

	node, err := OpenNode(NodeOptions{
		DataDir:   t.TempDir(),
		BlockSize: 4096,
		Now:       clk.Now,
		GCMode:    GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(ctx))
	})

	idSame1 := uuid.MustParse("01936a1b-1234-7000-8000-000000000001")
	idSame2 := uuid.MustParse("01936a1b-1234-7000-8000-000000000002")
	idSame3 := uuid.MustParse("01936a1b-1234-7000-8000-000000000003")

	_, err = node.createDiskWithID(ctx, idSame1, "same-1", 1<<20)
	require.NoError(t, err)
	_, err = node.createDiskWithID(ctx, idSame3, "same-3", 1<<20)
	require.NoError(t, err)
	_, err = node.createDiskWithID(ctx, idSame2, "same-2", 1<<20)
	require.NoError(t, err)

	clk.Advance(10 * time.Microsecond)
	dNew1, err := node.CreateDisk(ctx, "new-1", 1<<20)
	require.NoError(t, err)
	clk.Advance(10 * time.Microsecond)
	dNew2, err := node.CreateDisk(ctx, "new-2", 1<<20)
	require.NoError(t, err)
	clk.Advance(10 * time.Microsecond)
	dNew3, err := node.CreateDisk(ctx, "new-3", 1<<20)
	require.NoError(t, err)
	clk.Advance(10 * time.Microsecond)
	dNew4, err := node.CreateDisk(ctx, "new-4", 1<<20)
	require.NoError(t, err)

	wantOrder := []string{
		dNew4.DiskID,
		dNew3.DiskID,
		dNew2.DiskID,
		dNew1.DiskID,
		idSame3.String(),
		idSame2.String(),
		idSame1.String(),
	}

	var scanned []string
	token := ""
	for {
		page, lErr := node.ListDisks(ctx, ListDisksQuery{
			PageSize:  2,
			PageToken: token,
		})
		require.NoError(t, lErr)
		for _, d := range page.Disks {
			scanned = append(scanned, d.DiskID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Equal(t, wantOrder, scanned)

	newName := "renamed-same-2"
	newQuota := uint64(4 << 20)
	clk.Advance(time.Minute)
	updated, err := node.UpdateDisk(ctx, idSame2, &newName, &newQuota)
	require.NoError(t, err)
	require.Equal(t, newName, updated.Name)
	require.Equal(t, newQuota, updated.MaxBytes)

	afterUpdatePage, err := node.ListDisks(ctx, ListDisksQuery{PageSize: 10})
	require.NoError(t, err)
	gotAfterUpdate := make([]string, len(afterUpdatePage.Disks))
	for i, d := range afterUpdatePage.Disks {
		gotAfterUpdate[i] = d.DiskID
	}
	require.Equal(t, wantOrder, gotAfterUpdate)

	p1, err := node.ListDisks(ctx, ListDisksQuery{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, p1.Disks, 2)
	require.NotEmpty(t, p1.NextPageToken)

	require.NoError(t, node.DeleteDisk(ctx, uuid.MustParse(dNew3.DiskID)))

	p2, err := node.ListDisks(ctx, ListDisksQuery{PageSize: 10, PageToken: p1.NextPageToken})
	require.NoError(t, err)
	gotP2 := make([]string, len(p2.Disks))
	for i, d := range p2.Disks {
		gotP2[i] = d.DiskID
	}
	require.Equal(t, wantOrder[2:], gotP2)

	_, err = node.ListDisks(ctx, ListDisksQuery{PageToken: "not-valid-base64!!!"})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = node.ListDisks(ctx, ListDisksQuery{PageSize: MaxPageSize + 1})
	require.ErrorIs(t, err, ErrInvalid)
}

func TestNodeStore_ListCommits_FilterAndPagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	node, err := OpenNode(NodeOptions{
		DataDir:   t.TempDir(),
		BlockSize: 4096,
		GCMode:    GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(ctx))
	})

	info, err := node.CreateDisk(ctx, "commits-disk", 16<<20)
	require.NoError(t, err)
	diskID := uuid.MustParse(info.DiskID)

	c2, err := node.Commit(ctx, diskID, DefaultBranchName, "m1")
	require.NoError(t, err)
	c3, _, err := node.ForkBranch(ctx, diskID, DefaultBranchName, "feat", "f1")
	require.NoError(t, err)
	c4, err := node.Commit(ctx, diskID, DefaultBranchName, "m2")
	require.NoError(t, err)
	c5, err := node.Commit(ctx, diskID, "feat", "m3")
	require.NoError(t, err)
	mergeRes, err := node.MergeBranch(ctx, diskID, MergeOptions{
		SourceBranch:  "feat",
		TargetBranch:  DefaultBranchName,
		NoFastForward: true,
		Message:       "merge feat",
	})
	require.NoError(t, err)
	require.NotNil(t, mergeRes.Branch)
	require.NotNil(t, mergeRes.Commit)
	require.Equal(t, CommitKindMerge, mergeRes.Commit.Kind)

	var allIDs []uint64
	token := ""
	for {
		page, lErr := node.ListCommits(ctx, ListCommitsQuery{
			DiskID:    diskID,
			PageSize:  2,
			PageToken: token,
		})
		require.NoError(t, lErr)
		for _, c := range page.Commits {
			allIDs = append(allIDs, c.ID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	wantAll := []uint64{mergeRes.CommitID, c5.ID, c4.ID, c3.ID, c2.ID, 1}
	require.Equal(t, wantAll, allIDs)

	manualPage, err := node.ListCommits(ctx, ListCommitsQuery{
		DiskID:   diskID,
		Kind:     CommitKindManual,
		PageSize: 10,
	})
	require.NoError(t, err)
	gotManual := make([]uint64, len(manualPage.Commits))
	for i, c := range manualPage.Commits {
		gotManual[i] = c.ID
	}
	require.Equal(t, []uint64{c5.ID, c4.ID, c2.ID}, gotManual)

	_, err = node.ListCommits(ctx, ListCommitsQuery{DiskID: diskID, PageToken: "bad!!!"})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = node.ListCommits(ctx, ListCommitsQuery{DiskID: diskID, Kind: CommitKind(99)})
	require.ErrorIs(t, err, ErrInvalid)
	_, err = node.ListCommits(ctx, ListCommitsQuery{DiskID: uuid.New()})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestNodeStore_DeleteDisk_BusyMountRejectedAndConcurrentGC_Race(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	node, err := OpenNode(NodeOptions{
		DataDir:       t.TempDir(),
		BlockSize:     4096,
		MaxPackSize:   8192,
		MountLeaseTTL: time.Minute,
		GCMode:        GCModeDisabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, node.Close(ctx))
	})

	info, err := node.CreateDisk(ctx, "busy-disk", 16<<20)
	require.NoError(t, err)
	diskID := uuid.MustParse(info.DiskID)

	mnt, err := node.MountBranch(ctx, diskID, DefaultBranchName, false, false)
	require.NoError(t, err)

	err = node.DeleteDisk(ctx, diskID)
	require.ErrorIs(t, err, ErrDiskMounted)

	mntUUID := uuid.MustParse(mnt.Mount.ID)
	_, err = node.RenewMount(ctx, diskID, DefaultBranchName, mntUUID, false, mnt.Mount.WriterEpoch)
	require.NoError(t, err)
	require.NoError(t, node.UnmountBranch(ctx, diskID, mntUUID))
	require.NoError(t, node.DeleteDisk(ctx, diskID))

	for range 8 {
		dInfo, cErr := node.CreateDisk(ctx, "gc-race-disk", 16<<20)
		require.NoError(t, cErr)
		id := uuid.MustParse(dInfo.DiskID)

		var ds *DiskStore
		require.NoError(t, node.WithDisk(ctx, id, func(s *DiskStore) error {
			ds = s
			f, fErr := s.Create(ctx, DefaultBranchName, RootIno, "data.bin", 0o644, 0, 0)
			if fErr != nil {
				return fErr
			}
			for j := range 4 {
				if _, wErr := s.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{byte(j + 1)}, 4096)); wErr != nil {
					return wErr
				}
			}
			return s.Fsync(ctx, DefaultBranchName, f.Ino)
		}))

		var gcStarted atomic.Bool
		gcEntered := make(chan struct{})
		releaseGC := make(chan struct{})
		var releaseOnce sync.Once
		closeRelease := func() {
			releaseOnce.Do(func() { close(releaseGC) })
		}
		ds.beforeGCRelocateHook = func(_ uint64, _ BlockRefLocation) error {
			if gcStarted.CompareAndSwap(false, true) {
				close(gcEntered)
				<-releaseGC
			}
			return nil
		}

		var wg sync.WaitGroup
		var gcErr, delErr error
		wg.Go(func() {
			_, gcErr = ds.RunGC(ctx)
		})
		wg.Go(func() {
			select {
			case <-gcEntered:
			case <-time.After(20 * time.Millisecond):
			}
			closeRelease()
			delErr = node.DeleteDisk(ctx, id)
		})
		wg.Wait()

		require.NoError(t, delErr)
		if gcErr != nil {
			require.True(t, errors.Is(gcErr, ErrClosed) || errors.Is(gcErr, ErrNotFound))
		}
		_, gErr := node.GetDisk(ctx, id)
		require.ErrorIs(t, gErr, ErrNotFound)
	}

	periodicNode, err := OpenNode(NodeOptions{
		DataDir:       t.TempDir(),
		BlockSize:     4096,
		MaxPackSize:   8192,
		MountLeaseTTL: time.Minute,
		GCMode:        GCModePeriodic,
		GCInterval:    2 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, periodicNode.Close(ctx))
	})

	for range 8 {
		dInfo, cErr := periodicNode.CreateDisk(ctx, "periodic-gc-race-disk", 16<<20)
		require.NoError(t, cErr)
		id := uuid.MustParse(dInfo.DiskID)

		require.NoError(t, periodicNode.WithDisk(ctx, id, func(s *DiskStore) error {
			f, fErr := s.Create(ctx, DefaultBranchName, RootIno, "data.bin", 0o644, 0, 0)
			if fErr != nil {
				return fErr
			}
			for j := range 4 {
				if _, wErr := s.Write(ctx, DefaultBranchName, f.Ino, 0, bytes.Repeat([]byte{byte(j + 1)}, 4096)); wErr != nil {
					return wErr
				}
			}
			return nil
		}))

		time.Sleep(5 * time.Millisecond)
		require.NoError(t, periodicNode.DeleteDisk(ctx, id))
		_, gErr := periodicNode.GetDisk(ctx, id)
		require.ErrorIs(t, gErr, ErrNotFound)
	}
}

func TestNodeStore_ControlPlaneOperationsAndReady(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	node, err := OpenNode(NodeOptions{
		DataDir:       t.TempDir(),
		BlockSize:     4096,
		MountLeaseTTL: 45 * time.Second,
		GCMode:        GCModeDisabled,
	})
	require.NoError(t, err)
	require.NoError(t, node.Ready(ctx))
	require.Equal(t, 45*time.Second, node.MountLeaseTTL())

	info, err := node.CreateDisk(ctx, "ctrl-disk", 16<<20)
	require.NoError(t, err)
	diskID := uuid.MustParse(info.DiskID)

	cmt1, err := node.Commit(ctx, diskID, DefaultBranchName, "c1")
	require.NoError(t, err)
	_, _, err = node.ForkBranch(ctx, diskID, DefaultBranchName, "dev", "fork dev")
	require.NoError(t, err)
	_, err = node.Commit(ctx, diskID, "dev", "dev-c1")
	require.NoError(t, err)
	_, err = node.ResetBranch(ctx, diskID, "dev", cmt1.ID)
	require.NoError(t, err)
	pruned, err := node.PruneCommits(ctx, diskID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, pruned, 1)

	mnt, err := node.MountBranch(ctx, diskID, "dev", false, false)
	require.NoError(t, err)
	evictRes, err := node.EvictMounts(ctx, diskID, "dev", uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictRes.EvictedCount)
	require.Equal(t, []string{mnt.Mount.ID}, evictRes.EvictedMountIDs)

	require.NoError(t, node.DeleteBranch(ctx, diskID, "dev"))
	branches, err := node.ListBranches(ctx, diskID)
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.True(t, slices.ContainsFunc(branches, func(b BranchMeta) bool { return b.Name == DefaultBranchName }))

	cExtra, err := node.Commit(ctx, diskID, DefaultBranchName, "extra")
	require.NoError(t, err)
	_, err = node.ResetBranch(ctx, diskID, DefaultBranchName, cmt1.ID)
	require.NoError(t, err)
	require.NoError(t, node.DeleteCommit(ctx, diskID, cExtra.ID))

	require.NoError(t, node.Close(ctx))
	require.ErrorIs(t, node.Ready(ctx), ErrClosed)
}
