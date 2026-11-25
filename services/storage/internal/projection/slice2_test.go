package projection

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestControlManager_Step8_PassThroughAndPointerLeakMethodsDeleted(t *testing.T) {
	mgrType := reflect.TypeFor[*ControlManager]()
	for _, forbidden := range []string{
		"Disk",
		"Queue",
		"Diff",
		"DiffCommits",
		"DiffWork",
		"ReadCommitNode",
	} {
		_, exists := mgrType.MethodByName(forbidden)
		require.False(t, exists, "ControlManager must not export %s", forbidden)
	}
}

func TestMergeBranch_Step9_SingleLockResultDryRunUpToDateFastForwardAndThreeWay(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "merge-step9", 16<<20)
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "main", "base.txt", []byte("base"))
	_, err = mgr.Commit(ctx, disk.DiskID, "main", "base commit")
	require.NoError(t, err)

	forkCmt, _, err := mgr.ForkBranch(ctx, disk.DiskID, "main", "feat", "fork feat")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "feat", "feat.txt", []byte("feat"))
	featCmt, err := mgr.Commit(ctx, disk.DiskID, "feat", "feat commit")
	require.NoError(t, err)

	dryRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
		DryRun:       true,
	})
	require.NoError(t, err)
	require.True(t, dryRes.FastForward)
	require.False(t, dryRes.UpToDate)
	require.Equal(t, forkCmt.CommitID, dryRes.BaseCommitID)
	require.Nil(t, dryRes.Branch)
	require.Nil(t, dryRes.Commit)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	ffRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	require.NoError(t, err)
	require.True(t, ffRes.FastForward)
	require.False(t, ffRes.UpToDate)
	require.Equal(t, featCmt.CommitID, ffRes.CommitID)
	require.NotNil(t, ffRes.Branch)
	require.Equal(t, "main", ffRes.Branch.Name)
	require.Equal(t, featCmt.CommitID, ffRes.Branch.HeadCommitID)
	require.NotNil(t, ffRes.Commit)
	require.Equal(t, featCmt.CommitID, ffRes.Commit.CommitID)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	var pgWrites atomic.Int64
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			pgWrites.Add(1)
			return nil
		},
	})
	upToDateRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
	})
	store.SetHooks(MemStoreHooks{})
	require.NoError(t, err)
	require.True(t, upToDateRes.UpToDate)
	require.False(t, upToDateRes.FastForward)
	require.Equal(t, featCmt.CommitID, upToDateRes.CommitID)
	require.NotNil(t, upToDateRes.Branch)
	require.Equal(t, "main", upToDateRes.Branch.Name)
	require.Equal(t, featCmt.CommitID, upToDateRes.Branch.HeadCommitID)
	require.NotNil(t, upToDateRes.Commit)
	require.Equal(t, featCmt.CommitID, upToDateRes.Commit.CommitID)
	require.Zero(t, pgWrites.Load())
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "main", "main2.txt", []byte("main2"))
	main2Cmt, err := mgr.Commit(ctx, disk.DiskID, "main", "main2")
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "feat", "feat2.txt", []byte("feat2"))
	feat2Cmt, err := mgr.Commit(ctx, disk.DiskID, "feat", "feat2")
	require.NoError(t, err)

	threeWayRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "feat",
		TargetBranch: "main",
		Message:      "merge feat2",
	})
	require.NoError(t, err)
	require.False(t, threeWayRes.FastForward)
	require.False(t, threeWayRes.UpToDate)
	require.NotZero(t, threeWayRes.CommitID)
	require.NotNil(t, threeWayRes.Branch)
	require.Equal(t, "main", threeWayRes.Branch.Name)
	require.Equal(t, threeWayRes.CommitID, threeWayRes.Branch.HeadCommitID)
	require.NotNil(t, threeWayRes.Commit)
	require.Equal(t, threeWayRes.CommitID, threeWayRes.Commit.CommitID)
	require.Equal(t, CommitKindMerge, threeWayRes.Commit.Kind)
	require.Equal(t, main2Cmt.CommitID, threeWayRes.Commit.ParentID)
	require.Equal(t, feat2Cmt.CommitID, threeWayRes.Commit.MergeParentID)
	require.False(t, mgr.queue.isDirty(disk.DiskID))
}

func TestMergeBranch_Step9_PostMergeReadFailureBestEffortAndDirtyPolicy(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "merge-fail-step9", 16<<20)
	require.NoError(t, err)

	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "main", "base.txt", []byte("base"))
	_, err = mgr.Commit(ctx, disk.DiskID, "main", "base")
	require.NoError(t, err)

	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, "main", "b1", "fork b1")
	require.NoError(t, err)
	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "b1", "b1.txt", []byte("b1"))
	b1Cmt, err := mgr.Commit(ctx, disk.DiskID, "b1", "b1 commit")
	require.NoError(t, err)

	origGetBranch := mgr.getBranch
	origGetCommit := mgr.getCommit
	sentinelErr := errors.New("synthetic post-merge read failure")

	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinelErr
	}
	brFailRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "b1",
		TargetBranch: "main",
	})
	mgr.getBranch = origGetBranch
	require.NoError(t, err)
	require.True(t, brFailRes.FastForward)
	require.Equal(t, b1Cmt.CommitID, brFailRes.CommitID)
	require.Nil(t, brFailRes.Branch)
	require.NotNil(t, brFailRes.Commit)
	require.Equal(t, b1Cmt.CommitID, brFailRes.Commit.CommitID)
	require.True(t, mgr.queue.isDirty(disk.DiskID))

	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	var upToDateWrites atomic.Int64
	store.SetHooks(MemStoreHooks{
		BeforeWrite: func(context.Context, string, uuid.UUID) error {
			upToDateWrites.Add(1)
			return nil
		},
	})
	mgr.getBranch = func(*engine.DiskStore, string) (engine.BranchMeta, error) {
		return engine.BranchMeta{}, sentinelErr
	}
	mgr.getCommit = func(*engine.DiskStore, uint64) (engine.CommitMeta, error) {
		return engine.CommitMeta{}, sentinelErr
	}
	utdFailRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "b1",
		TargetBranch: "main",
	})
	mgr.getBranch = origGetBranch
	mgr.getCommit = origGetCommit
	store.SetHooks(MemStoreHooks{})
	require.NoError(t, err)
	require.True(t, utdFailRes.UpToDate)
	require.Nil(t, utdFailRes.Branch)
	require.Nil(t, utdFailRes.Commit)
	require.Zero(t, upToDateWrites.Load())
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, "main", "b2", "fork b2")
	require.NoError(t, err)
	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "main", "m2.txt", []byte("m2"))
	_, err = mgr.Commit(ctx, disk.DiskID, "main", "m2")
	require.NoError(t, err)
	writeFileOnBranch(t, ctx, mgr, disk.DiskID, "b2", "b2.txt", []byte("b2"))
	_, err = mgr.Commit(ctx, disk.DiskID, "b2", "b2")
	require.NoError(t, err)

	mgr.getCommit = func(*engine.DiskStore, uint64) (engine.CommitMeta, error) {
		return engine.CommitMeta{}, sentinelErr
	}
	cmtFailRes, err := mgr.MergeBranch(ctx, disk.DiskID, engine.MergeOptions{
		SourceBranch: "b2",
		TargetBranch: "main",
		Message:      "merge b2",
	})
	mgr.getCommit = origGetCommit
	require.NoError(t, err)
	require.False(t, cmtFailRes.FastForward)
	require.False(t, cmtFailRes.UpToDate)
	require.NotZero(t, cmtFailRes.CommitID)
	require.NotNil(t, cmtFailRes.Branch)
	require.Equal(t, cmtFailRes.CommitID, cmtFailRes.Branch.HeadCommitID)
	require.Nil(t, cmtFailRes.Commit)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
}

func TestMountBranch_Step10_SingleLockResultReturnsMountAndRootInode(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "mount-step10", 16<<20)
	require.NoError(t, err)

	roRes, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.NoError(t, err)
	require.True(t, roRes.Mount.ReadOnly)
	require.NotEqual(t, uuid.Nil, roRes.Mount.MountID)
	require.Equal(t, engine.RootIno, roRes.RootInode.Ino)
	require.Equal(t, engine.ModeDir, roRes.RootInode.Mode&engine.ModeTypeMask)

	rwRes, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	require.False(t, rwRes.Mount.ReadOnly)
	require.NotEqual(t, uuid.Nil, rwRes.Mount.MountID)
	require.Positive(t, rwRes.Mount.WriterEpoch)
	require.Equal(t, engine.RootIno, rwRes.RootInode.Ino)
	require.Equal(t, engine.ModeDir, rwRes.RootInode.Mode&engine.ModeTypeMask)
}

func TestMountBranch_Step10_GetAttrFailureRollsBackLeaseAndMarksDirtyPolicy(t *testing.T) {
	ctx := context.Background()
	store := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   store,
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "mount-rollback-step10", 16<<20)
	require.NoError(t, err)

	origGetAttr := mgr.getAttr
	origUnmount := mgr.unmount
	sentinelAttrErr := errors.New("synthetic root getattr failure")
	sentinelUnmountErr := errors.New("synthetic rollback unmount failure")

	mgr.getAttr = func(*engine.DiskStore, context.Context, string, uint64) (engine.Inode, error) {
		return engine.Inode{}, sentinelAttrErr
	}
	_, err = mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	mgr.getAttr = origGetAttr
	require.ErrorIs(t, err, sentinelAttrErr)
	require.True(t, mgr.queue.isDirty(disk.DiskID))

	mountsAfterRWFail, err := mgr.ListMounts(ctx, disk.DiskID, engine.DefaultBranchName)
	require.NoError(t, err)
	require.Empty(t, mountsAfterRWFail)

	retryRW, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	require.Equal(t, engine.RootIno, retryRW.RootInode.Ino)
	require.NoError(t, mgr.UnmountBranch(ctx, disk.DiskID, retryRW.Mount.MountID))
	require.NoError(t, mgr.reconcileDisk(ctx, disk.DiskID, false, false))
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	mgr.getAttr = func(*engine.DiskStore, context.Context, string, uint64) (engine.Inode, error) {
		return engine.Inode{}, sentinelAttrErr
	}
	_, err = mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	mgr.getAttr = origGetAttr
	require.ErrorIs(t, err, sentinelAttrErr)
	require.False(t, mgr.queue.isDirty(disk.DiskID))

	var pebbleMounts []engine.MountMeta
	require.NoError(t, mgr.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
		var lErr error
		pebbleMounts, lErr = ds.ListMounts(engine.DefaultBranchName, mgr.now())
		return lErr
	}))
	require.Empty(t, pebbleMounts)

	mgr.getAttr = func(*engine.DiskStore, context.Context, string, uint64) (engine.Inode, error) {
		return engine.Inode{}, sentinelAttrErr
	}
	mgr.unmount = func(*engine.DiskStore, context.Context, string) error {
		return sentinelUnmountErr
	}
	_, err = mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	mgr.getAttr = origGetAttr
	mgr.unmount = origUnmount
	require.ErrorIs(t, err, sentinelAttrErr)
	require.True(t, mgr.queue.isDirty(disk.DiskID))
}

func TestEvictMounts_Step11_ReturnsProjectionEvictMountsResultWithBranchRecords(t *testing.T) {
	ctx := context.Background()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		FlushInterval:           time.Hour,
		ReconcileInterval:       time.Hour,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close(ctx)) }()

	disk, err := mgr.CreateDisk(ctx, "evict-step11", 16<<20)
	require.NoError(t, err)
	_, _, err = mgr.ForkBranch(ctx, disk.DiskID, engine.DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	_, err = mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, true, false)
	require.NoError(t, err)
	mainRW, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)
	featRW, err := mgr.MountBranch(ctx, disk.DiskID, "feat", false, false)
	require.NoError(t, err)

	featEvict, err := mgr.EvictMounts(ctx, disk.DiskID, "feat", uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, uint32(1), featEvict.EvictedCount)
	require.Len(t, featEvict.UpdatedBranches, 1)
	require.Equal(t, disk.DiskID, featEvict.UpdatedBranches[0].DiskID)
	require.Equal(t, "feat", featEvict.UpdatedBranches[0].Name)
	require.Equal(t, featRW.Mount.WriterEpoch+1, featEvict.UpdatedBranches[0].WriterEpoch)

	allEvict, err := mgr.EvictMounts(ctx, disk.DiskID, "", uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, uint32(2), allEvict.EvictedCount)
	require.Len(t, allEvict.UpdatedBranches, 1)
	require.Equal(t, disk.DiskID, allEvict.UpdatedBranches[0].DiskID)
	require.Equal(t, engine.DefaultBranchName, allEvict.UpdatedBranches[0].Name)
	require.Equal(t, mainRW.Mount.WriterEpoch+1, allEvict.UpdatedBranches[0].WriterEpoch)
}

func TestNewControlManager_Step11_StrictGCModeValidationAndProvideControlManagerWiring(t *testing.T) {
	ctx := context.Background()

	invalidCases := []struct {
		name string
		opts ManagerOptions
	}{
		{
			name: "negative GCMode",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCMode(-1),
				DisableStartupReconcile: true,
			},
		},
		{
			name: "above range GCMode",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCMode(99),
				DisableStartupReconcile: true,
			},
		},
		{
			name: "negative GCInterval with periodic",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCModePeriodic,
				GCInterval:              -time.Second,
				DisableStartupReconcile: true,
			},
		},
		{
			name: "negative GCInterval with wake only",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCModeWakeOnly,
				GCInterval:              -time.Millisecond,
				DisableStartupReconcile: true,
			},
		},
		{
			name: "positive GCInterval with wake only",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCModeWakeOnly,
				GCInterval:              5 * time.Second,
				DisableStartupReconcile: true,
			},
		},
		{
			name: "positive GCInterval with disabled",
			opts: ManagerOptions{
				StorageRoot:             t.TempDir(),
				GCMode:                  GCModeDisabled,
				GCInterval:              5 * time.Second,
				DisableStartupReconcile: true,
			},
		},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			mgr, err := NewControlManager(tc.opts)
			require.ErrorIs(t, err, ErrInvalidArgument)
			require.Nil(t, mgr)
		})
	}

	defaultMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, defaultMgr.Close(ctx)) }()
	require.Equal(t, GCModePeriodic, defaultMgr.gcMode)
	require.Equal(t, 30*time.Second, defaultMgr.gcInterval)

	wakeMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		GCMode:                  GCModeWakeOnly,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, wakeMgr.Close(ctx)) }()
	require.Equal(t, GCModeWakeOnly, wakeMgr.gcMode)
	require.Zero(t, wakeMgr.gcInterval)

	disabledMgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   NewMemStore(),
		GCMode:                  GCModeDisabled,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, disabledMgr.Close(ctx)) }()
	require.Equal(t, GCModeDisabled, disabledMgr.gcMode)
	require.Zero(t, disabledMgr.gcInterval)

	wiredPeriodic, err := ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     25 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, wiredPeriodic.Close(ctx)) }()
	require.Equal(t, GCModePeriodic, wiredPeriodic.gcMode)
	require.Equal(t, 25*time.Millisecond, wiredPeriodic.gcInterval)

	wiredWake, err := ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     0,
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, wiredWake.Close(ctx)) }()
	require.Equal(t, GCModeWakeOnly, wiredWake.gcMode)
	require.Zero(t, wiredWake.gcInterval)

	_, err = ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     -time.Second,
		},
	})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestControlManager_Step11_ExportedMethodSetLockedTo22AndOptionsCleaned(t *testing.T) {
	mgrType := reflect.TypeFor[*ControlManager]()
	require.Equal(t, 22, mgrType.NumMethod())

	gotMethods := make([]string, 0, mgrType.NumMethod())
	for m := range mgrType.Methods() {
		gotMethods = append(gotMethods, m.Name)
	}
	wantMethods := []string{
		"Close",
		"Commit",
		"CreateDisk",
		"DeleteBranch",
		"DeleteCommit",
		"DeleteDisk",
		"EvictMounts",
		"ForkBranch",
		"GetDisk",
		"ListBranches",
		"ListCommits",
		"ListDisks",
		"ListMounts",
		"MergeBranch",
		"MountBranch",
		"MountLeaseTTL",
		"PruneCommits",
		"RenewMount",
		"ResetBranch",
		"UnmountBranch",
		"UpdateDisk",
		"WithDisk",
	}
	require.Equal(t, wantMethods, gotMethods)

	optsType := reflect.TypeFor[ManagerOptions]()
	for _, removed := range []string{
		"OpTimeout",
		"SharedCacheBytes",
		"DisablePeriodicGC",
		"DisableGC",
	} {
		_, exists := optsType.FieldByName(removed)
		require.False(t, exists, "ManagerOptions must not have removed field %s", removed)
	}
	_, hasGCMode := optsType.FieldByName("GCMode")
	require.True(t, hasGCMode)
}
