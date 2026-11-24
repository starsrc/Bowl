package projection_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/engine"
	"github.com/starsrc/bowl/services/storage/internal/projection"
)

type fakeClock struct {
	mu  sync.RWMutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start.UTC()}
}

func (c *fakeClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	return c.now
}

func writeFileOnBranch(t *testing.T, ctx context.Context, mgr *projection.ControlManager, diskID uuid.UUID, branch, name string, payload []byte) uint64 {
	t.Helper()
	ds, err := mgr.Disk(ctx, diskID)
	require.NoError(t, err)
	ino, err := ds.Create(ctx, branch, engine.RootIno, name, 0o644, 0, 0)
	require.NoError(t, err)
	n, err := ds.Write(ctx, branch, ino.Ino, 0, payload)
	require.NoError(t, err)
	require.Len(t, payload, n)
	return ino.Ino
}

func TestStep16_ControlManager_ReadWriteRoutingAndColdDiskZeroWakeup(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := projection.NewMemStore()
	clk := newFakeClock(time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC))

	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          500 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       50 * time.Millisecond,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)

	diskRec, err := mgr.CreateDisk(ctx, "alpha", 64<<20)
	require.NoError(t, err)
	require.Equal(t, "alpha", diskRec.Name)
	require.Equal(t, uint64(64<<20), diskRec.MaxBytes)
	require.Equal(t, engine.DefaultBranchName, diskRec.DefaultBranch)
	require.True(t, mgr.IsOpen(diskRec.DiskID))

	newName := "alpha-renamed"
	newMax := uint64(128 << 20)
	updatedDisk, err := mgr.UpdateDisk(ctx, diskRec.DiskID, &newName, &newMax)
	require.NoError(t, err)
	require.Equal(t, newName, updatedDisk.Name)
	require.Equal(t, newMax, updatedDisk.MaxBytes)

	ino := writeFileOnBranch(t, ctx, mgr, diskRec.DiskID, "main", "hello.txt", []byte("hello world"))
	cmt1, err := mgr.Commit(ctx, diskRec.DiskID, "main", "first commit")
	require.NoError(t, err)
	require.Equal(t, projection.CommitKindManual, cmt1.Kind)
	require.Equal(t, "first commit", cmt1.Message)

	forkCmt, featBranch, err := mgr.ForkBranch(ctx, diskRec.DiskID, "main", "feat", "fork feat")
	require.NoError(t, err)
	require.Equal(t, projection.CommitKindFork, forkCmt.Kind)
	require.Equal(t, "feat", featBranch.Name)
	require.Equal(t, forkCmt.CommitID, featBranch.HeadCommitID)

	writeFileOnBranch(t, ctx, mgr, diskRec.DiskID, "feat", "feat.txt", []byte("feature data"))
	_, err = mgr.Commit(ctx, diskRec.DiskID, "feat", "feat commit")
	require.NoError(t, err)

	mnt, err := mgr.MountBranch(ctx, diskRec.DiskID, "main", true, false)
	require.NoError(t, err)
	require.True(t, mnt.ReadOnly)

	require.NoError(t, mgr.Close(ctx))

	coldMgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          500 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       50 * time.Millisecond,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, coldMgr.Close(ctx)) }()

	require.False(t, coldMgr.IsOpen(diskRec.DiskID))
	opensBefore := coldMgr.OpenCount()

	listPage, err := coldMgr.ListDisks(ctx, projection.ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, listPage.Disks, 1)
	require.Equal(t, diskRec.DiskID, listPage.Disks[0].DiskID)
	require.Equal(t, newName, listPage.Disks[0].Name)

	gotDisk, err := coldMgr.GetDisk(ctx, diskRec.DiskID)
	require.NoError(t, err)
	require.Equal(t, newName, gotDisk.Name)
	require.Equal(t, newMax, gotDisk.MaxBytes)

	branches, err := coldMgr.ListBranches(ctx, diskRec.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, "feat", branches[0].Name)
	require.Equal(t, "main", branches[1].Name)

	commitsPage, err := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{DiskID: diskRec.DiskID})
	require.NoError(t, err)
	require.Len(t, commitsPage.Commits, 4)

	mounts, err := coldMgr.ListMounts(ctx, diskRec.DiskID, "")
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	require.Equal(t, mnt.MountID, mounts[0].MountID)

	require.Equal(t, opensBefore, coldMgr.OpenCount())
	require.False(t, coldMgr.IsOpen(diskRec.DiskID))

	var pgReadCalls atomic.Int64
	store.SetHooks(projection.MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			pgReadCalls.Add(1)
			return projection.ErrUnavailable
		},
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return projection.ErrUnavailable
		},
	})

	diffRes, err := coldMgr.Diff(ctx, diskRec.DiskID, engine.DiffOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffRes.Summary.Added)

	diffCmtRes, err := coldMgr.DiffCommits(ctx, diskRec.DiskID, engine.DiffCommitsOptions{
		BaseCommitID:   1,
		SourceCommitID: cmt1.CommitID,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffCmtRes.Summary.Added)

	writeFileOnBranch(t, ctx, coldMgr, diskRec.DiskID, "main", "work.txt", []byte("uncommitted"))
	diffWorkRes, err := coldMgr.DiffWork(ctx, diskRec.DiskID, "main", engine.DiffWorkOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffWorkRes.Summary.Added)

	nodeRes, err := coldMgr.ReadCommitNode(ctx, diskRec.DiskID, cmt1.CommitID, ino, 0, 64)
	require.NoError(t, err)
	require.Equal(t, engine.ModeRegular|0o644, nodeRes.Mode)
	require.Equal(t, uint64(len("hello world")), nodeRes.FileSize)
	require.Equal(t, []byte("hello world"), nodeRes.Data)
	require.True(t, nodeRes.EOF)
	require.Empty(t, nodeRes.SymlinkTarget)

	cmt2, err := coldMgr.Commit(ctx, diskRec.DiskID, "main", "commit during pg outage")
	require.NoError(t, err)
	require.Greater(t, cmt2.CommitID, cmt1.CommitID)

	require.Zero(t, pgReadCalls.Load())
	require.True(t, coldMgr.IsOpen(diskRec.DiskID))
}

func TestStep17_Reconciler_StartupRebuildEmptyPGDirtyRetryOrphanPruneAndUsageBatchFlush(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	clk := newFakeClock(time.Date(2026, 5, 2, 12, 0, 0, 0, time.UTC))

	seedStore := projection.NewMemStore()
	seedMgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   seedStore,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          200 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       50 * time.Millisecond,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)

	d1, err := seedMgr.CreateDisk(ctx, "disk-one", 32<<20)
	require.NoError(t, err)
	writeFileOnBranch(t, ctx, seedMgr, d1.DiskID, "main", "a.bin", make([]byte, 8192))
	_, err = seedMgr.Commit(ctx, d1.DiskID, "main", "commit a")
	require.NoError(t, err)
	_, _, err = seedMgr.ForkBranch(ctx, d1.DiskID, "main", "dev", "fork dev")
	require.NoError(t, err)
	activeMnt, err := seedMgr.MountBranch(ctx, d1.DiskID, "main", false, false)
	require.NoError(t, err)

	d2, err := seedMgr.CreateDisk(ctx, "disk-two", 64<<20)
	require.NoError(t, err)
	require.NoError(t, seedMgr.Close(ctx))

	require.NoError(t, os.WriteFile(filepath.Join(root, "disks", "not-a-uuid.tmp"), []byte("ignore"), 0o600))
	require.NoError(t, os.MkdirAll(filepath.Join(root, "disks", "temp-dir"), 0o755))

	freshStore := projection.NewMemStore()
	orphanID := uuid.New()
	require.NoError(t, freshStore.UpsertDisk(ctx, projection.DiskRecord{
		DiskID:        orphanID,
		Name:          "orphan",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     clk.Now(),
		UpdatedAt:     clk.Now(),
	}, nil, nil))

	freshStore.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, op string, _ uuid.UUID) error {
			if op == "ReplaceDiskSnapshot" {
				select {
				case <-time.After(120 * time.Millisecond):
				case <-hookCtx.Done():
					return hookCtx.Err()
				}
			}
			return nil
		},
	})

	startAt := time.Now()
	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:       root,
		Store:             freshStore,
		Now:               clk.Now,
		MountLeaseTTL:     30 * time.Second,
		BarrierTimeout:    200 * time.Millisecond,
		FlushInterval:     40 * time.Millisecond,
		ReconcileInterval: 40 * time.Millisecond,
		DiskQueueSize:     1,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()
	require.Less(t, time.Since(startAt), 80*time.Millisecond)

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	require.NoError(t, mgr.WaitStartupReconcile(waitCtx))

	require.False(t, mgr.IsOpen(d1.DiskID))
	require.False(t, mgr.IsOpen(d2.DiskID))

	rebuiltPage, err := freshStore.ListDisks(ctx, projection.ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, rebuiltPage.Disks, 2)
	_, err = freshStore.GetDisk(ctx, orphanID)
	require.ErrorIs(t, err, projection.ErrNotFound)

	rebuiltD1, err := freshStore.GetDisk(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Equal(t, "disk-one", rebuiltD1.Name)
	require.Positive(t, rebuiltD1.UsedBytes)

	rebuiltBranches, err := freshStore.ListBranches(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Len(t, rebuiltBranches, 2)

	rebuiltMounts, err := freshStore.ListMounts(ctx, d1.DiskID, "", clk.Now())
	require.NoError(t, err)
	require.Len(t, rebuiltMounts, 1)
	require.Equal(t, activeMnt.MountID, rebuiltMounts[0].MountID)

	var failWrites atomic.Bool
	var blockWrites chan struct{}
	var blockMu sync.Mutex
	var renewSyncWrites atomic.Int64
	freshStore.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, op string, _ uuid.UUID) error {
			if failWrites.Load() {
				return projection.ErrUnavailable
			}
			blockMu.Lock()
			ch := blockWrites
			blockMu.Unlock()
			if ch != nil {
				select {
				case <-ch:
				case <-hookCtx.Done():
					return hookCtx.Err()
				}
			}
			if op == "UpsertMount" {
				renewSyncWrites.Add(1)
			}
			return nil
		},
	})

	failWrites.Store(true)
	_, err = mgr.Commit(ctx, d1.DiskID, "dev", "dirty commit 1")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return mgr.Queue().IsDirty(d1.DiskID)
	}, 2*time.Second, 10*time.Millisecond)

	failWrites.Store(false)
	require.Eventually(t, func() bool {
		if mgr.Queue().IsDirty(d1.DiskID) {
			return false
		}
		p, lErr := freshStore.ListCommits(ctx, projection.ListCommitsQuery{DiskID: d1.DiskID})
		return lErr == nil && len(p.Commits) == 4
	}, 3*time.Second, 15*time.Millisecond)

	unblock := make(chan struct{})
	blockMu.Lock()
	blockWrites = unblock
	blockMu.Unlock()

	for range 5 {
		_, err = mgr.Commit(ctx, d2.DiskID, "main", "burst commit")
		require.NoError(t, err)
	}
	require.True(t, mgr.Queue().IsDirty(d2.DiskID))

	blockMu.Lock()
	blockWrites = nil
	blockMu.Unlock()
	close(unblock)

	require.Eventually(t, func() bool {
		if mgr.Queue().IsDirty(d2.DiskID) {
			return false
		}
		p, lErr := freshStore.ListCommits(ctx, projection.ListCommitsQuery{DiskID: d2.DiskID})
		return lErr == nil && len(p.Commits) == 6
	}, 3*time.Second, 15*time.Millisecond)

	beforeUsed := rebuiltD1.UsedBytes
	writeFileOnBranch(t, ctx, mgr, d1.DiskID, "main", "more.bin", make([]byte, 32768))

	clk.Advance(10 * time.Second)
	renewSyncBefore := renewSyncWrites.Load()
	renewed, err := mgr.RenewMount(ctx, d1.DiskID, "main", activeMnt.MountID, false, activeMnt.WriterEpoch)
	require.NoError(t, err)
	require.Equal(t, renewSyncBefore, renewSyncWrites.Load())

	require.Eventually(t, func() bool {
		rec, gErr := freshStore.GetDisk(ctx, d1.DiskID)
		if gErr != nil || rec.UsedBytes <= beforeUsed {
			return false
		}
		ms, mErr := freshStore.ListMounts(ctx, d1.DiskID, "main", clk.Now())
		if mErr != nil || len(ms) != 1 {
			return false
		}
		return ms[0].ExpiresAt.Equal(renewed.ExpiresAt)
	}, 3*time.Second, 15*time.Millisecond)
}

func TestStep18_ControlManager_ReadAfterWriteBarrierAndZeroWaitColdRead(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := projection.NewMemStore()
	clk := newFakeClock(time.Date(2026, 5, 3, 9, 0, 0, 0, time.UTC))

	var writeDelay atomic.Int64
	writeDelay.Store(int64(50 * time.Millisecond))

	var slowDiskMu sync.RWMutex
	var slowDiskID uuid.UUID
	var slowDelay time.Duration

	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, diskID uuid.UUID) error {
			d := time.Duration(writeDelay.Load())
			slowDiskMu.RLock()
			if slowDiskID != uuid.Nil && diskID == slowDiskID {
				d = slowDelay
			}
			slowDiskMu.RUnlock()
			if d > 0 {
				select {
				case <-time.After(d):
				case <-hookCtx.Done():
					return hookCtx.Err()
				}
			}
			return nil
		},
	})

	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		BarrierTimeout:          500 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       time.Second,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskA, err := mgr.CreateDisk(ctx, "disk-a", 32<<20)
	require.NoError(t, err)

	gotA, err := mgr.GetDisk(ctx, diskA.DiskID)
	require.NoError(t, err)
	require.Equal(t, diskA.DiskID, gotA.DiskID)

	page, err := mgr.ListDisks(ctx, projection.ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, page.Disks, 1)
	require.Equal(t, diskA.DiskID, page.Disks[0].DiskID)

	forkCmt, _, err := mgr.ForkBranch(ctx, diskA.DiskID, "main", "feature", "fork feature")
	require.NoError(t, err)
	branches, err := mgr.ListBranches(ctx, diskA.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, "feature", branches[0].Name)
	require.Equal(t, forkCmt.CommitID, branches[0].HeadCommitID)
	require.Equal(t, "main", branches[1].Name)
	require.Equal(t, forkCmt.CommitID, branches[1].HeadCommitID)

	cmt, err := mgr.Commit(ctx, diskA.DiskID, "feature", "feature work")
	require.NoError(t, err)
	commitsPage, err := mgr.ListCommits(ctx, projection.ListCommitsQuery{DiskID: diskA.DiskID})
	require.NoError(t, err)
	require.NotEmpty(t, commitsPage.Commits)
	require.Equal(t, cmt.CommitID, commitsPage.Commits[0].CommitID)

	diskB, err := mgr.CreateDisk(ctx, "disk-b", 32<<20)
	require.NoError(t, err)
	_, err = mgr.GetDisk(ctx, diskB.DiskID)
	require.NoError(t, err)

	slowDiskMu.Lock()
	slowDiskID = diskA.DiskID
	slowDelay = 180 * time.Millisecond
	slowDiskMu.Unlock()

	_, err = mgr.Commit(ctx, diskA.DiskID, "main", "slow write on A")
	require.NoError(t, err)

	t0 := time.Now()
	gotB, err := mgr.GetDisk(ctx, diskB.DiskID)
	require.NoError(t, err)
	require.Equal(t, diskB.DiskID, gotB.DiskID)
	_, err = mgr.ListBranches(ctx, diskB.DiskID)
	require.NoError(t, err)
	_, err = mgr.ListCommits(ctx, projection.ListCommitsQuery{DiskID: diskB.DiskID})
	require.NoError(t, err)
	_, err = mgr.ListMounts(ctx, diskB.DiskID, "")
	require.NoError(t, err)
	require.Less(t, time.Since(t0), 30*time.Millisecond)

	slowDiskMu.Lock()
	slowDiskID = uuid.Nil
	slowDiskMu.Unlock()

	require.NoError(t, mgr.DeleteDisk(ctx, diskA.DiskID))
	opensBefore := mgr.OpenCount()
	_, err = mgr.GetDisk(ctx, diskA.DiskID)
	require.ErrorIs(t, err, projection.ErrNotFound)
	require.Equal(t, opensBefore, mgr.OpenCount())

	pageAfterDel, err := mgr.ListDisks(ctx, projection.ListDisksQuery{})
	require.NoError(t, err)
	require.Len(t, pageAfterDel.Disks, 1)
	require.Equal(t, diskB.DiskID, pageAfterDel.Disks[0].DiskID)
}

func TestStep19_ControlManager_PGOutageAndDirtyFallbackToPebbleAndListDisksFailFast(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := projection.NewMemStore()
	clk := newFakeClock(time.Date(2026, 5, 4, 8, 0, 0, 0, time.UTC))

	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          60 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       5 * time.Second,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)

	diskRec, err := mgr.CreateDisk(ctx, "resilient", 64<<20)
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, diskRec.DiskID, "main", "base.txt", []byte("base"))
	c2, err := mgr.Commit(ctx, diskRec.DiskID, "main", "manual 1")
	require.NoError(t, err)

	c3, _, err := mgr.ForkBranch(ctx, diskRec.DiskID, "main", "dev", "fork dev")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, diskRec.DiskID, "dev", "dev.txt", []byte("dev"))
	c4, err := mgr.Commit(ctx, diskRec.DiskID, "dev", "manual 2")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, diskRec.DiskID, "main", "main2.txt", []byte("main2"))
	c5, err := mgr.Commit(ctx, diskRec.DiskID, "main", "manual 3")
	require.NoError(t, err)

	mergeRes, err := mgr.MergeBranch(ctx, diskRec.DiskID, engine.MergeOptions{
		SourceBranch: "dev",
		TargetBranch: "main",
		Message:      "merge dev",
	})
	require.NoError(t, err)
	require.False(t, mergeRes.FastForward)
	c6ID := mergeRes.CommitID

	expiredMnt, err := mgr.MountBranch(ctx, diskRec.DiskID, "dev", true, false)
	require.NoError(t, err)
	clk.Advance(40 * time.Second)
	activeMainMnt, err := mgr.MountBranch(ctx, diskRec.DiskID, "main", true, false)
	require.NoError(t, err)
	activeDevMnt, err := mgr.MountBranch(ctx, diskRec.DiskID, "dev", false, false)
	require.NoError(t, err)

	require.NoError(t, mgr.Close(ctx))

	coldMgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          60 * time.Millisecond,
		FlushInterval:           50 * time.Millisecond,
		ReconcileInterval:       5 * time.Second,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, coldMgr.Close(ctx)) }()

	store.SetHooks(projection.MemStoreHooks{
		BeforeRead: func(context.Context, string) error {
			return projection.ErrUnavailable
		},
	})

	opensBeforeList := coldMgr.OpenCount()
	_, err = coldMgr.ListDisks(ctx, projection.ListDisksQuery{})
	require.ErrorIs(t, err, projection.ErrUnavailable)
	require.Equal(t, opensBeforeList, coldMgr.OpenCount())
	require.False(t, coldMgr.IsOpen(diskRec.DiskID))

	gotDisk, err := coldMgr.GetDisk(ctx, diskRec.DiskID)
	require.NoError(t, err)
	require.Equal(t, "resilient", gotDisk.Name)
	require.False(t, coldMgr.IsOpen(diskRec.DiskID))

	_, err = coldMgr.GetDisk(ctx, uuid.New())
	require.ErrorIs(t, err, projection.ErrNotFound)

	branches, err := coldMgr.ListBranches(ctx, diskRec.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, "dev", branches[0].Name)
	require.Equal(t, "main", branches[1].Name)

	var seenAll []uint64
	token := ""
	for {
		p, lErr := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{
			DiskID:    diskRec.DiskID,
			PageSize:  2,
			PageToken: token,
		})
		require.NoError(t, lErr)
		for _, c := range p.Commits {
			seenAll = append(seenAll, c.CommitID)
		}
		if p.NextPageToken == "" {
			break
		}
		token = p.NextPageToken
	}
	require.Equal(t, []uint64{c6ID, c5.CommitID, c4.CommitID, c3.CommitID, c2.CommitID, 1}, seenAll)

	manualPage, err := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{
		DiskID:   diskRec.DiskID,
		Kind:     projection.CommitKindManual,
		PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, manualPage.Commits, 2)
	require.Equal(t, c5.CommitID, manualPage.Commits[0].CommitID)
	require.Equal(t, c4.CommitID, manualPage.Commits[1].CommitID)
	require.NotEmpty(t, manualPage.NextPageToken)

	manualPage2, err := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{
		DiskID:    diskRec.DiskID,
		Kind:      projection.CommitKindManual,
		PageSize:  2,
		PageToken: manualPage.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, manualPage2.Commits, 1)
	require.Equal(t, c2.CommitID, manualPage2.Commits[0].CommitID)
	require.Empty(t, manualPage2.NextPageToken)

	allMounts, err := coldMgr.ListMounts(ctx, diskRec.DiskID, "")
	require.NoError(t, err)
	require.Len(t, allMounts, 2)
	for _, m := range allMounts {
		require.NotEqual(t, expiredMnt.MountID, m.MountID)
	}
	devMounts, err := coldMgr.ListMounts(ctx, diskRec.DiskID, "dev")
	require.NoError(t, err)
	require.Len(t, devMounts, 1)
	require.Equal(t, activeDevMnt.MountID, devMounts[0].MountID)
	mainMounts, err := coldMgr.ListMounts(ctx, diskRec.DiskID, "main")
	require.NoError(t, err)
	require.Len(t, mainMounts, 1)
	require.Equal(t, activeMainMnt.MountID, mainMounts[0].MountID)

	releaseBlock := make(chan struct{})
	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(hookCtx context.Context, _ string, _ uuid.UUID) error {
			select {
			case <-releaseBlock:
				return nil
			case <-hookCtx.Done():
				return hookCtx.Err()
			}
		},
	})

	c7, err := coldMgr.Commit(ctx, diskRec.DiskID, "main", "barrier timeout commit")
	require.NoError(t, err)

	timeoutCommits, err := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{
		DiskID:   diskRec.DiskID,
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, timeoutCommits.Commits, 1)
	require.Equal(t, c7.CommitID, timeoutCommits.Commits[0].CommitID)

	_, err = coldMgr.CreateDisk(ctx, "blocked-catalog", 16<<20)
	require.NoError(t, err)
	_, err = coldMgr.ListDisks(ctx, projection.ListDisksQuery{})
	require.ErrorIs(t, err, projection.ErrUnavailable)

	close(releaseBlock)

	store.SetHooks(projection.MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			return projection.ErrUnavailable
		},
	})
	c8, err := coldMgr.Commit(ctx, diskRec.DiskID, "main", "dirty fallback commit")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return coldMgr.Queue().IsDirty(diskRec.DiskID)
	}, 2*time.Second, 10*time.Millisecond)

	store.SetHooks(projection.MemStoreHooks{})

	dirtyCommits, err := coldMgr.ListCommits(ctx, projection.ListCommitsQuery{
		DiskID:   diskRec.DiskID,
		PageSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, dirtyCommits.Commits, 1)
	require.Equal(t, c8.CommitID, dirtyCommits.Commits[0].CommitID)
}

func TestStep20_ControlManager_DeleteDiskAndBranchMountGatesAndIdleEviction(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store := projection.NewMemStore()
	clk := newFakeClock(time.Date(2026, 5, 5, 14, 0, 0, 0, time.UTC))

	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   store,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          200 * time.Millisecond,
		FlushInterval:           200 * time.Millisecond,
		ReconcileInterval:       5 * time.Second,
		ActorIdleTTL:            40 * time.Millisecond,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	diskA, err := mgr.CreateDisk(ctx, "gated-a", 64<<20)
	require.NoError(t, err)

	mainRW, err := mgr.MountBranch(ctx, diskA.DiskID, "main", false, false)
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, diskA.DiskID, "main", "f1.txt", []byte("one"))
	cmt1, err := mgr.Commit(ctx, diskA.DiskID, "main", "commit while mounted")
	require.NoError(t, err)

	_, _, err = mgr.ForkBranch(ctx, diskA.DiskID, "main", "feat", "fork while mounted")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, diskA.DiskID, "feat", "f2.txt", []byte("two"))
	_, err = mgr.Commit(ctx, diskA.DiskID, "feat", "feat commit")
	require.NoError(t, err)

	featRO, err := mgr.MountBranch(ctx, diskA.DiskID, "feat", true, false)
	require.NoError(t, err)

	err = mgr.DeleteDisk(ctx, diskA.DiskID)
	require.ErrorIs(t, err, engine.ErrDiskMounted)

	err = mgr.DeleteBranch(ctx, diskA.DiskID, "feat")
	require.ErrorIs(t, err, engine.ErrDiskMounted)

	_, err = mgr.ResetBranch(ctx, diskA.DiskID, "main", cmt1.CommitID)
	require.ErrorIs(t, err, engine.ErrDiskMounted)

	_, err = mgr.MergeBranch(ctx, diskA.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	require.ErrorIs(t, err, engine.ErrDiskMounted)

	dryRes, err := mgr.MergeBranch(ctx, diskA.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
		DryRun:       true,
	})
	require.NoError(t, err)
	require.True(t, dryRes.FastForward)

	require.NoError(t, mgr.UnmountBranch(ctx, diskA.DiskID, mainRW.MountID))
	require.NoError(t, mgr.UnmountBranch(ctx, diskA.DiskID, featRO.MountID))

	mergeRes, err := mgr.MergeBranch(ctx, diskA.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	require.NoError(t, err)
	require.True(t, mergeRes.FastForward)

	resetBr, err := mgr.ResetBranch(ctx, diskA.DiskID, "feat", cmt1.CommitID)
	require.NoError(t, err)
	require.Equal(t, cmt1.CommitID, resetBr.HeadCommitID)

	require.NoError(t, mgr.DeleteBranch(ctx, diskA.DiskID, "feat"))

	tempMnt, err := mgr.MountBranch(ctx, diskA.DiskID, "main", true, false)
	require.NoError(t, err)
	require.NotEqual(t, uuid.Nil, tempMnt.MountID)
	err = mgr.DeleteDisk(ctx, diskA.DiskID)
	require.ErrorIs(t, err, engine.ErrDiskMounted)

	clk.Advance(31 * time.Second)
	require.NoError(t, mgr.DeleteDisk(ctx, diskA.DiskID))

	pinnedDisk, err := mgr.CreateDisk(ctx, "pinned", 64<<20)
	require.NoError(t, err)
	pinnedMnt, err := mgr.MountBranch(ctx, pinnedDisk.DiskID, "main", false, false)
	require.NoError(t, err)

	idleDisk, err := mgr.CreateDisk(ctx, "idle-evict", 64<<20)
	require.NoError(t, err)
	writeFileOnBranch(t, ctx, mgr, idleDisk.DiskID, "main", "payload.bin", make([]byte, 16384))
	require.True(t, mgr.IsOpen(pinnedDisk.DiskID))
	require.True(t, mgr.IsOpen(idleDisk.DiskID))

	clk.Advance(1 * time.Second)

	require.Eventually(t, func() bool {
		return !mgr.IsOpen(idleDisk.DiskID)
	}, 2*time.Second, 10*time.Millisecond)
	require.True(t, mgr.IsOpen(pinnedDisk.DiskID))

	evictedRec, err := store.GetDisk(ctx, idleDisk.DiskID)
	require.NoError(t, err)
	require.Positive(t, evictedRec.UsedBytes)

	require.NoError(t, mgr.UnmountBranch(ctx, pinnedDisk.DiskID, pinnedMnt.MountID))
	clk.Advance(1 * time.Second)
	require.Eventually(t, func() bool {
		return !mgr.IsOpen(pinnedDisk.DiskID)
	}, 2*time.Second, 10*time.Millisecond)
}

func TestStep15_19_PGIntegration_ControlManagerLifecycleAndRebuild(t *testing.T) {
	pool, ctx := setupProjectionTestDB(t)
	root := t.TempDir()
	pgStore := projection.NewPGStore(pool)
	clk := newFakeClock(time.Date(2026, 5, 6, 16, 0, 0, 0, time.UTC))

	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             root,
		Store:                   pgStore,
		Now:                     clk.Now,
		MountLeaseTTL:           30 * time.Second,
		BarrierTimeout:          500 * time.Millisecond,
		FlushInterval:           40 * time.Millisecond,
		ReconcileInterval:       50 * time.Millisecond,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)

	d1, err := mgr.CreateDisk(ctx, "pg-disk-1", 64<<20)
	require.NoError(t, err)

	renamed := "pg-disk-1-v2"
	_, err = mgr.UpdateDisk(ctx, d1.DiskID, &renamed, nil)
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, d1.DiskID, "main", "a.txt", []byte("alpha"))
	c2, err := mgr.Commit(ctx, d1.DiskID, "main", "commit a")
	require.NoError(t, err)

	_, _, err = mgr.ForkBranch(ctx, d1.DiskID, "main", "feat", "fork feat")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, d1.DiskID, "feat", "b.txt", []byte("beta"))
	c4, err := mgr.Commit(ctx, d1.DiskID, "feat", "commit b")
	require.NoError(t, err)

	mnt1, err := mgr.MountBranch(ctx, d1.DiskID, "main", false, false)
	require.NoError(t, err)
	mnt2, err := mgr.MountBranch(ctx, d1.DiskID, "main", false, true)
	require.NoError(t, err)
	require.Greater(t, mnt2.WriterEpoch, mnt1.WriterEpoch)

	mounts, err := mgr.ListMounts(ctx, d1.DiskID, "main")
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	require.Equal(t, mnt2.MountID, mounts[0].MountID)

	clk.Advance(5 * time.Second)
	renewed, err := mgr.RenewMount(ctx, d1.DiskID, "main", mnt2.MountID, false, mnt2.WriterEpoch)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ms, mErr := pgStore.ListMounts(ctx, d1.DiskID, "main", clk.Now())
		return mErr == nil && len(ms) == 1 && ms[0].ExpiresAt.Equal(renewed.ExpiresAt)
	}, 2*time.Second, 15*time.Millisecond)

	evictRes, err := mgr.EvictMounts(ctx, d1.DiskID, "main", uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictRes.EvictedCount)

	mountsAfterEvict, err := mgr.ListMounts(ctx, d1.DiskID, "main")
	require.NoError(t, err)
	require.Empty(t, mountsAfterEvict)

	_, err = mgr.ResetBranch(ctx, d1.DiskID, "feat", c2.CommitID)
	require.NoError(t, err)
	require.NoError(t, mgr.DeleteCommit(ctx, d1.DiskID, c4.CommitID))

	writeFileOnBranch(t, ctx, mgr, d1.DiskID, "feat", "c.txt", []byte("gamma"))
	_, err = mgr.Commit(ctx, d1.DiskID, "feat", "commit c")
	require.NoError(t, err)
	_, err = mgr.ResetBranch(ctx, d1.DiskID, "feat", c2.CommitID)
	require.NoError(t, err)

	pruned, err := mgr.PruneCommits(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)

	require.NoError(t, mgr.DeleteBranch(ctx, d1.DiskID, "feat"))

	activeRO, err := mgr.MountBranch(ctx, d1.DiskID, "main", true, false)
	require.NoError(t, err)

	require.NoError(t, mgr.Close(ctx))

	_, err = pool.Exec(ctx, "TRUNCATE disks CASCADE")
	require.NoError(t, err)

	emptyPage, err := pgStore.ListDisks(ctx, projection.ListDisksQuery{})
	require.NoError(t, err)
	require.Empty(t, emptyPage.Disks)

	mgr2, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:       root,
		Store:             pgStore,
		Now:               clk.Now,
		MountLeaseTTL:     30 * time.Second,
		BarrierTimeout:    500 * time.Millisecond,
		FlushInterval:     40 * time.Millisecond,
		ReconcileInterval: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr2.Close(ctx)) }()

	require.NoError(t, mgr2.WaitStartupReconcile(ctx))
	require.False(t, mgr2.IsOpen(d1.DiskID))

	gotDisk, err := mgr2.GetDisk(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Equal(t, renamed, gotDisk.Name)
	require.Positive(t, gotDisk.UsedBytes)

	rebuiltBranches, err := mgr2.ListBranches(ctx, d1.DiskID)
	require.NoError(t, err)
	require.Len(t, rebuiltBranches, 1)
	require.Equal(t, "main", rebuiltBranches[0].Name)

	rebuiltCommits, err := mgr2.ListCommits(ctx, projection.ListCommitsQuery{DiskID: d1.DiskID})
	require.NoError(t, err)
	require.Len(t, rebuiltCommits.Commits, 3)

	rebuiltMounts, err := mgr2.ListMounts(ctx, d1.DiskID, "")
	require.NoError(t, err)
	require.Len(t, rebuiltMounts, 1)
	require.Equal(t, activeRO.MountID, rebuiltMounts[0].MountID)
}
