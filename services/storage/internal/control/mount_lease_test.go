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
