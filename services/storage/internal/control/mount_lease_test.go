package control

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type fakeClock struct {
	unixNano atomic.Int64
}

func newFakeClock(start time.Time) *fakeClock {
	c := &fakeClock{}
	c.unixNano.Store(start.UnixNano())
	return c
}

func (c *fakeClock) Now() time.Time {
	return time.Unix(0, c.unixNano.Load()).UTC()
}

func (c *fakeClock) Advance(d time.Duration) {
	c.unixNano.Add(d.Nanoseconds())
}

func requireValidNonNilUUID(t *testing.T, raw string) {
	t.Helper()
	parsed, err := uuid.Parse(raw)
	require.NoError(t, err, "MountID 必须为合法 UUID: got=%q", raw)
	require.NotEqual(t, uuid.Nil, parsed, "MountID 不得为全零 UUID")
}

func requireDiskMountedStatus(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrDiskMounted)
	st := ToStatus(err)
	require.NotNil(t, st)
	require.Equal(t, codes.Aborted, st.Code())
	require.Len(t, st.Details(), 1)
	info, ok := st.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "EBUSY", info.GetReason())
	require.Equal(t, ErrorDomain, info.GetDomain())
}

func TestMountLease_IssueUnmountAndRejectOnlineTargetMerge(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
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
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	rwMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	requireValidNonNilUUID(t, rwMnt.MountID)
	require.Equal(t, uint32(30), rwMnt.LeaseTTLSeconds)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	roMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: true})
	require.NoError(t, err)
	requireValidNonNilUUID(t, roMnt.MountID)
	require.Equal(t, uint32(30), roMnt.LeaseTTLSeconds)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: rwMnt.MountID}))

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: rwMnt.MountID}))

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: roMnt.MountID}))
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_SourceRWRejects_SourceROAllows_DryRunAllowsWithoutClearing(t *testing.T) {
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
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
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	srcRW, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: false})
	require.NoError(t, err)
	tgtRW, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)

	dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		DryRun:       true,
	})
	require.NoError(t, err)
	require.Empty(t, dryRes.MergeCommitID)

	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: target.DiskID, MountID: tgtRW.MountID}))
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)
	require.Contains(t, err.Error(), "source disk "+source.DiskID)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcRW.MountID}))
	srcRO, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcRO.MountID})
	})

	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_RenewAndVirtualClockExpiration(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
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
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)

	clk.Advance(20 * time.Second)
	renewRes, err := mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  mnt.MountID,
		ReadOnly: false,
		Epoch:    mnt.Epoch,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRes.LeaseTTLSeconds)

	clk.Advance(20 * time.Second)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	clk.Advance(11 * time.Second)
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
}

func TestMountLease_RestartRebuildAndForcePreemption(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))

	mgr1 := New(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr1.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr1.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "feat.txt", []byte("feat"))

	roDisk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "ro-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	tgtMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID, ReadOnly: false})
	require.NoError(t, err)
	epoch0 := tgtMnt.Epoch

	srcMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: source.DiskID, ReadOnly: false})
	require.NoError(t, err)

	roMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: roDisk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	require.NoError(t, mgr1.Close())

	mgr2 := New(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr2.Close() })

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0,
	})
	require.NoError(t, err)

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   source.DiskID,
		MountID:  srcMnt.MountID,
		ReadOnly: false,
		Epoch:    srcMnt.Epoch,
	})
	require.NoError(t, err)

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   roDisk.DiskID,
		MountID:  roMnt.MountID,
		ReadOnly: true,
		Epoch:    0,
	})
	require.NoError(t, err)

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0 + 99,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	requireDiskMountedStatus(t, err)

	mergeRes, err := mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		Force:        true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, mergeRes.MergeCommitID)

	newTActor, err := mgr2.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, epoch0+1, newTActor.WriterEpoch())

	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   target.DiskID,
		MountID:  tgtMnt.MountID,
		ReadOnly: false,
		Epoch:    epoch0,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	require.NoError(t, mgr2.UnmountDisk(ctx, UnmountDiskParams{DiskID: source.DiskID, MountID: srcMnt.MountID}))
	cleanFork, err := mgr2.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "clean"})
	require.NoError(t, err)
	_, err = mgr2.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: cleanFork.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
}

func TestMountLease_DeleteDiskClearsLeasesAndCustomTTLApplies(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now), WithMountLeaseTTL(5*time.Second))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "ephemeral", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint32(5), mnt.LeaseTTLSeconds)

	requireDiskMountedStatus(t, mgr.DeleteDisk(ctx, disk.DiskID))

	clk.Advance(6 * time.Second)
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  mnt.MountID,
		ReadOnly: false,
		Epoch:    mnt.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestMountDisk_RWAdmissionGate_RejectsDuplicateWithoutForceAndAllowsAfterReleaseOrExpiry(t *testing.T) {

	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "rw-gate-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	roMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	require.Equal(t, uint64(0), roMnt.Epoch)

	rw1, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rw1.Epoch)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor.WriterEpoch())

	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)
	require.Equal(t, uint64(1), actor.WriterEpoch())

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rw1.MountID,
		ReadOnly: false,
		Epoch:    rw1.Epoch,
	})
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, rw1.Epoch, "owner1.txt", []byte("alive"))

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: rw1.MountID}))
	rw2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(2), rw2.Epoch)

	clk.Advance(31 * time.Second)
	rw3, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(3), rw3.Epoch)
}

func TestMountDisk_ForceRW_PreemptsActiveLeaseAndAdvancesEpoch(t *testing.T) {

	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "force-preempt-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	rw1, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rw1.Epoch)

	rw2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false, Force: true})

	require.NoError(t, err)
	require.Equal(t, uint64(2), rw2.Epoch)
	require.NotEqual(t, rw1.MountID, rw2.MountID)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), actor.WriterEpoch())

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rw1.MountID,
		ReadOnly: false,
		Epoch:    rw1.Epoch,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	requireDiskMountedStatus(t, err)
}

func TestMountDisk_ConcurrentNonForceRW_AdmitsAtMostOneWriter(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "concurrent-rw-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	const workers = 16
	start := make(chan struct{})
	results := make([]*MountDiskResult, workers)
	errs := make([]error, workers)

	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			<-start
			results[i], errs[i] = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
		})
	}
	close(start)
	wg.Wait()

	succeeded := 0
	for i := range workers {
		if errs[i] == nil {
			succeeded++
			require.NotNil(t, results[i])
			require.Equal(t, uint64(1), results[i].Epoch)
			requireValidNonNilUUID(t, results[i].MountID)
			continue
		}
		requireDiskMountedStatus(t, errs[i])
	}
	require.Equal(t, 1, succeeded, "concurrent non-force RW mounts must admit exactly 1 writer")

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor.WriterEpoch())
}

func TestDeleteDisk_MountLeaseGate_RejectsActiveRWOrROAndAllowsAfterUnmountOrExpiry(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "gated-delete", MaxBytes: 16 << 20})
	require.NoError(t, err)

	rwMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	roMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	err = mgr.DeleteDisk(ctx, disk.DiskID)
	requireDiskMountedStatus(t, err)
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: rwMnt.MountID}))
	err = mgr.DeleteDisk(ctx, disk.DiskID)
	requireDiskMountedStatus(t, err)
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: roMnt.MountID}))
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
	_, err = mgr.GetDisk(ctx, disk.DiskID)
	require.ErrorIs(t, err, ErrNotFound)

	expiringDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "expiring-delete", MaxBytes: 16 << 20})
	require.NoError(t, err)
	expMnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: expiringDisk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	err = mgr.DeleteDisk(ctx, expiringDisk.DiskID)
	requireDiskMountedStatus(t, err)

	clk.Advance(31 * time.Second)
	require.NoError(t, mgr.DeleteDisk(ctx, expiringDisk.DiskID))
	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   expiringDisk.DiskID,
		MountID:  expMnt.MountID,
		ReadOnly: false,
		Epoch:    expMnt.Epoch,
	})
	require.ErrorIs(t, err, ErrNotFound)
}

func TestDeleteDisk_ConcurrentWithMountROAndRenew_SerializesWithoutLeakedLease(t *testing.T) {
	ctx := context.Background()
	clk := newFakeClock(time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC))
	mgr := newManager(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr.Close() })

	for range 20 {
		disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "race-disk", MaxBytes: 16 << 20})
		require.NoError(t, err)
		roSeed, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
		require.NoError(t, err)
		require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: roSeed.MountID}))

		start := make(chan struct{})
		var (
			wg        sync.WaitGroup
			deleteErr error
			mountRes  *MountDiskResult
			mountErr  error
			renewErr  error
		)
		wg.Go(func() {
			<-start
			deleteErr = mgr.DeleteDisk(ctx, disk.DiskID)
		})
		wg.Go(func() {
			<-start
			mountRes, mountErr = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
		})
		wg.Go(func() {
			<-start
			_, renewErr = mgr.RenewMount(ctx, RenewMountParams{
				DiskID:   disk.DiskID,
				MountID:  roSeed.MountID,
				ReadOnly: true,
				Epoch:    0,
			})
		})
		close(start)
		wg.Wait()

		if deleteErr == nil {
			require.ErrorIs(t, mountErr, ErrNotFound)
			require.ErrorIs(t, renewErr, ErrNotFound)
			mgr.mountsMu.Lock()
			leaked := len(mgr.activeMounts[disk.DiskID])
			mgr.mountsMu.Unlock()
			require.Zero(t, leaked, "deleted disk must not retain leaked mount leases")
		} else {
			requireDiskMountedStatus(t, deleteErr)
			if mountErr == nil {
				require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mountRes.MountID}))
			}
			if renewErr == nil {
				require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: roSeed.MountID}))
			}
			require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
		}
	}
}

func TestListMounts_ReturnsSortedActiveLeasesPrunesExpiredAndAvoidsColdOpen(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	clk := newFakeClock(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC))

	mgr1 := newManager(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "list-mounts-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)
	require.NoError(t, mgr1.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr2.Close() })

	_, err = mgr2.ListMounts(ctx, "not-a-uuid")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr2.ListMounts(ctx, uuid.Nil.String())
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr2.ListMounts(ctx, uuid.Must(uuid.NewV7()).String())
	require.ErrorIs(t, err, ErrNotFound)

	coldLeases, err := mgr2.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Empty(t, coldLeases)
	mgr2.poolMu.RLock()
	loadedActor := mgr2.actors[disk.DiskID]
	mgr2.poolMu.RUnlock()
	require.Nil(t, loadedActor, "ListMounts must not cold-open Pebble actor")

	rwMnt, err := mgr2.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	ro1, err := mgr2.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	ro2, err := mgr2.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	active, err := mgr2.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Len(t, active, 3)
	require.Less(t, active[0].MountID, active[1].MountID)
	require.Less(t, active[1].MountID, active[2].MountID)

	byID := make(map[string]MountLeaseInfo, len(active))
	for _, item := range active {
		byID[item.MountID] = item
	}
	require.Equal(t, MountLeaseInfo{
		MountID:   rwMnt.MountID,
		ReadOnly:  false,
		Epoch:     1,
		ExpiresAt: clk.Now().Add(DefaultMountLeaseTTL),
	}, byID[rwMnt.MountID])
	require.Equal(t, MountLeaseInfo{
		MountID:   ro1.MountID,
		ReadOnly:  true,
		Epoch:     0,
		ExpiresAt: clk.Now().Add(DefaultMountLeaseTTL),
	}, byID[ro1.MountID])
	require.Equal(t, MountLeaseInfo{
		MountID:   ro2.MountID,
		ReadOnly:  true,
		Epoch:     0,
		ExpiresAt: clk.Now().Add(DefaultMountLeaseTTL),
	}, byID[ro2.MountID])

	clk.Advance(20 * time.Second)
	_, err = mgr2.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  ro2.MountID,
		ReadOnly: true,
		Epoch:    0,
	})
	require.NoError(t, err)
	renewedExpiry := clk.Now().Add(DefaultMountLeaseTTL)

	clk.Advance(11 * time.Second)
	remaining, err := mgr2.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []MountLeaseInfo{{
		MountID:   ro2.MountID,
		ReadOnly:  true,
		Epoch:     0,
		ExpiresAt: renewedExpiry,
	}}, remaining)
}

func TestEvictMounts_SingleRO_SingleRW_AllAndRestartFencing(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	clk := newFakeClock(time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC))

	mgr := newManager(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "evict-disk", MaxBytes: 16 << 20})
	require.NoError(t, err)

	_, err = mgr.EvictMounts(ctx, EvictMountsParams{DiskID: "bad"})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.EvictMounts(ctx, EvictMountsParams{DiskID: uuid.Nil.String()})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.EvictMounts(ctx, EvictMountsParams{DiskID: disk.DiskID, MountID: "bad"})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.EvictMounts(ctx, EvictMountsParams{DiskID: disk.DiskID, MountID: uuid.Nil.String()})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.EvictMounts(ctx, EvictMountsParams{DiskID: uuid.Must(uuid.NewV7()).String()})
	require.ErrorIs(t, err, ErrNotFound)

	rw1, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rw1.Epoch)
	ro1, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	ro2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	missingRes, err := mgr.EvictMounts(ctx, EvictMountsParams{
		DiskID:  disk.DiskID,
		MountID: uuid.Must(uuid.NewV7()).String(),
	})
	require.NoError(t, err)
	require.Equal(t, &EvictMountsResult{EvictedCount: 0, WriterEpoch: 0}, missingRes)

	roEvictRes, err := mgr.EvictMounts(ctx, EvictMountsParams{
		DiskID:  disk.DiskID,
		MountID: ro1.MountID,
	})
	require.NoError(t, err)
	require.Equal(t, &EvictMountsResult{EvictedCount: 1, WriterEpoch: 0}, roEvictRes)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor.WriterEpoch(), "evicting RO lease must not advance WriterEpoch")
	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rw1.MountID,
		ReadOnly: false,
		Epoch:    rw1.Epoch,
	})
	require.NoError(t, err)

	rwEvictRes, err := mgr.EvictMounts(ctx, EvictMountsParams{
		DiskID:  disk.DiskID,
		MountID: rw1.MountID,
	})
	require.NoError(t, err)
	require.Equal(t, &EvictMountsResult{EvictedCount: 1, WriterEpoch: 2}, rwEvictRes)
	require.Equal(t, uint64(2), actor.WriterEpoch())

	_, err = mgr.RenewMount(ctx, RenewMountParams{
		DiskID:   disk.DiskID,
		MountID:  rw1.MountID,
		ReadOnly: false,
		Epoch:    rw1.Epoch,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("fenced.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     rw1.Epoch,
	})
	require.ErrorIs(t, err, ErrStaleEpoch)

	listed, err := mgr.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, ro2.MountID, listed[0].MountID)

	rw2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(3), rw2.Epoch)

	evictAllRes, err := mgr.EvictMounts(ctx, EvictMountsParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Equal(t, &EvictMountsResult{EvictedCount: 2, WriterEpoch: 4}, evictAllRes)
	require.Equal(t, uint64(4), actor.WriterEpoch())

	emptyList, err := mgr.ListMounts(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Empty(t, emptyList)

	require.NoError(t, mgr.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir), WithClock(clk.Now))
	t.Cleanup(func() { _ = mgr2.Close() })

	postRestartEvict, err := mgr2.EvictMounts(ctx, EvictMountsParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Equal(t, &EvictMountsResult{EvictedCount: 0, WriterEpoch: 5}, postRestartEvict)

	actor2, err := mgr2.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(5), actor2.WriterEpoch())

	require.NoError(t, mgr2.DeleteDisk(ctx, disk.DiskID))
}
