package engine

import (
	"context"
	"math"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stepClock struct {
	nowNano atomic.Int64
}

func newStepClock(t time.Time) *stepClock {
	c := &stepClock{}
	c.nowNano.Store(t.UnixNano())
	return c
}

func (c *stepClock) Now() time.Time {
	return time.Unix(0, c.nowNano.Load()).UTC()
}

func (c *stepClock) Advance(d time.Duration) {
	c.nowNano.Add(d.Nanoseconds())
}

func TestStep15_DiskMeta_NameUpdatedAtUpdateDiskMetaShrinkGuardAndControlSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC))

	_, err := OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "bad-name-nul",
		Name:      "bad\x00name",
		BlockSize: 4096,
	})
	require.ErrorIs(t, err, ErrInvalid)

	_, err = OpenDisk(DiskOptions{
		Dir:       t.TempDir(),
		DiskID:    "bad-name-long",
		Name:      strings.Repeat("a", MaxFileNameBytes+1),
		BlockSize: 4096,
	})
	require.ErrorIs(t, err, ErrInvalid)

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step15",
		Name:      "alpha-disk",
		MaxBytes:  64 << 10,
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)

	meta := disk.DiskMeta()
	require.Equal(t, "disk-step15", meta.DiskID)
	require.Equal(t, "alpha-disk", meta.Name)
	require.Equal(t, uint64(64<<10), meta.MaxBytes)
	require.Equal(t, clk.Now().UnixNano(), meta.CreatedAt)
	require.Equal(t, meta.CreatedAt, meta.UpdatedAt)

	encoded := EncodeDiskMeta(meta)
	decoded, err := DecodeDiskMeta(encoded)
	require.NoError(t, err)
	require.Equal(t, meta, decoded)
	_, err = DecodeDiskMeta(encoded[:len(encoded)-1])
	require.ErrorIs(t, err, ErrInvalidValue)

	f, err := disk.Create(ctx, DefaultBranchName, RootIno, "payload.bin", 0o644, 0, 0)
	require.NoError(t, err)
	payload := make([]byte, 8192)
	for i := range payload {
		payload[i] = byte(i%251 + 1)
	}
	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, 0, payload)
	require.NoError(t, err)
	c1, err := disk.Commit(ctx, DefaultBranchName, "first data commit")
	require.NoError(t, err)

	used := disk.packs.PhysicalBytes()
	require.GreaterOrEqual(t, used, uint64(8192))

	clk.Advance(5 * time.Second)
	_, err = disk.UpdateDiskMeta(ctx, nil, nil)
	require.ErrorIs(t, err, ErrInvalid)

	badLong := strings.Repeat("x", MaxFileNameBytes+1)
	_, err = disk.UpdateDiskMeta(ctx, &badLong, nil)
	require.ErrorIs(t, err, ErrInvalid)

	badNul := "a\x00b"
	_, err = disk.UpdateDiskMeta(ctx, &badNul, nil)
	require.ErrorIs(t, err, ErrInvalid)

	zeroQuota := uint64(0)
	_, err = disk.UpdateDiskMeta(ctx, nil, &zeroQuota)
	require.ErrorIs(t, err, ErrInvalid)

	overQuota := uint64(math.MaxInt64) + 1
	_, err = disk.UpdateDiskMeta(ctx, nil, &overQuota)
	require.ErrorIs(t, err, ErrInvalid)

	tooSmall := used - 1
	_, err = disk.UpdateDiskMeta(ctx, nil, &tooSmall)
	require.ErrorIs(t, err, ErrQuotaShrinkBelowUsed)
	require.Equal(t, "alpha-disk", disk.DiskMeta().Name)
	require.Equal(t, uint64(64<<10), disk.DiskMeta().MaxBytes)

	newName := "beta-disk"
	updated, err := disk.UpdateDiskMeta(ctx, &newName, nil)
	require.NoError(t, err)
	require.Equal(t, "beta-disk", updated.Name)
	require.Equal(t, uint64(64<<10), updated.MaxBytes)
	require.Equal(t, meta.CreatedAt, updated.CreatedAt)
	require.Equal(t, clk.Now().UnixNano(), updated.UpdatedAt)

	clk.Advance(5 * time.Second)
	exactQuota := used
	updated, err = disk.UpdateDiskMeta(ctx, nil, &exactQuota)
	require.NoError(t, err)
	require.Equal(t, exactQuota, updated.MaxBytes)
	require.Equal(t, exactQuota, disk.packs.MaxBytes())
	require.Equal(t, clk.Now().UnixNano(), updated.UpdatedAt)

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, int64(len(payload)), make([]byte, 4096))
	require.ErrorIs(t, err, ErrNoSpace)

	clk.Advance(5 * time.Second)
	grownQuota := uint64(128 << 10)
	finalName := "gamma-disk"
	updated, err = disk.UpdateDiskMeta(ctx, &finalName, &grownQuota)
	require.NoError(t, err)
	require.Equal(t, finalName, updated.Name)
	require.Equal(t, grownQuota, updated.MaxBytes)
	require.Equal(t, grownQuota, disk.packs.MaxBytes())

	_, err = disk.Write(ctx, DefaultBranchName, f.Ino, int64(len(payload)), make([]byte, 4096))
	require.NoError(t, err)

	cFork, featBranch, err := disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	expMount, err := disk.MountBranch(ctx, "feat", true, false, 10*time.Second)
	require.NoError(t, err)
	clk.Advance(15 * time.Second)

	activeMount, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)

	commits, err := disk.ListCommits()
	require.NoError(t, err)
	require.Len(t, commits, 3)
	require.Equal(t, cFork.ID, commits[0].ID)
	require.Equal(t, c1.ID, commits[1].ID)
	require.Equal(t, uint64(1), commits[2].ID)

	snap, err := disk.SnapshotControlMeta(clk.Now())
	require.NoError(t, err)
	require.Equal(t, updated, snap.Disk)
	require.Equal(t, disk.packs.PhysicalBytes(), snap.UsedBytes)
	require.Len(t, snap.Branches, 2)
	require.Equal(t, featBranch.Name, snap.Branches[0].Name)
	require.Equal(t, DefaultBranchName, snap.Branches[1].Name)
	require.Len(t, snap.Commits, 3)
	require.Equal(t, cFork.ID, snap.Commits[0].ID)
	require.Equal(t, uint64(1), snap.Commits[2].ID)
	require.Len(t, snap.Mounts, 1)
	require.Equal(t, activeMount.ID, snap.Mounts[0].ID)
	require.NotEqual(t, expMount.ID, snap.Mounts[0].ID)

	require.NoError(t, disk.Close())

	_, err = disk.UpdateDiskMeta(ctx, &newName, nil)
	require.ErrorIs(t, err, ErrClosed)
	_, err = disk.ListCommits()
	require.ErrorIs(t, err, ErrClosed)
	_, err = disk.SnapshotControlMeta(clk.Now())
	require.ErrorIs(t, err, ErrClosed)

	reopened, err := OpenDisk(DiskOptions{
		Dir:       dir,
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, reopened.Close())
	})
	require.Equal(t, finalName, reopened.DiskMeta().Name)
	require.Equal(t, grownQuota, reopened.DiskMeta().MaxBytes)
	require.Equal(t, meta.CreatedAt, reopened.DiskMeta().CreatedAt)
	require.Equal(t, updated.UpdatedAt, reopened.DiskMeta().UpdatedAt)
}

func TestStep20_MountLease_BranchRWExclusivityForcePreemptRenewAndEvict(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC))

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step20-lease",
		BlockSize: 4096,
		Now:       clk.Now,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, disk.Close())
	})

	_, _, err = disk.Fork(ctx, DefaultBranchName, "feat", "fork feat")
	require.NoError(t, err)

	sampleMount := MountMeta{
		ID:          "m-codec-test",
		BranchName:  "main",
		ReadOnly:    true,
		WriterEpoch: 0,
		MountedAt:   100,
		ExpiresAt:   200,
	}
	encMount := EncodeMountMeta(sampleMount)
	decMount, err := DecodeMountMeta(encMount)
	require.NoError(t, err)
	require.Equal(t, sampleMount, decMount)
	_, err = DecodeMountMeta(encMount[:len(encMount)-1])
	require.ErrorIs(t, err, ErrInvalidValue)

	_, err = disk.MountBranch(ctx, "", false, false, 30*time.Second)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = disk.MountBranch(ctx, "missing", false, false, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.MountBranch(ctx, DefaultBranchName, false, false, 0)
	require.ErrorIs(t, err, ErrInvalid)

	mainBefore, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)

	ro1, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)
	require.True(t, ro1.ReadOnly)
	require.Equal(t, uint64(0), ro1.WriterEpoch)
	require.Equal(t, clk.Now().UnixNano(), ro1.MountedAt)
	require.Equal(t, clk.Now().Add(30*time.Second).UnixNano(), ro1.ExpiresAt)

	ro2, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)
	require.True(t, ro2.ReadOnly)
	require.Equal(t, uint64(0), ro2.WriterEpoch)

	mainAfterRO, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, mainBefore.WriterEpoch, mainAfterRO.WriterEpoch)

	rw1, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	require.False(t, rw1.ReadOnly)
	require.Equal(t, mainBefore.WriterEpoch+1, rw1.WriterEpoch)

	_, err = disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.ErrorIs(t, err, ErrDiskMounted)
	mainAfterReject, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, rw1.WriterEpoch, mainAfterReject.WriterEpoch)

	featRW, err := disk.MountBranch(ctx, "feat", false, false, 20*time.Second)
	require.NoError(t, err)
	require.Equal(t, "feat", featRW.BranchName)
	require.False(t, featRW.ReadOnly)

	allMounts, err := disk.ListMounts("", clk.Now())
	require.NoError(t, err)
	require.Len(t, allMounts, 4)
	for i := 1; i < len(allMounts); i++ {
		require.Less(t, allMounts[i-1].ID, allMounts[i].ID)
	}

	mainMounts, err := disk.ListMounts(DefaultBranchName, clk.Now())
	require.NoError(t, err)
	require.Len(t, mainMounts, 3)

	activeAll, err := disk.HasActiveMounts("", clk.Now())
	require.NoError(t, err)
	require.True(t, activeAll)
	activeFeat, err := disk.HasActiveMounts("feat", clk.Now())
	require.NoError(t, err)
	require.True(t, activeFeat)

	_, err = disk.RenewMount(ctx, DefaultBranchName, ro1.ID, true, 1, 30*time.Second)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = disk.RenewMount(ctx, DefaultBranchName, rw1.ID, false, 0, 30*time.Second)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = disk.RenewMount(ctx, DefaultBranchName, rw1.ID, false, rw1.WriterEpoch, 0)
	require.ErrorIs(t, err, ErrInvalid)
	_, err = disk.RenewMount(ctx, DefaultBranchName, "", false, rw1.WriterEpoch, 30*time.Second)
	require.ErrorIs(t, err, ErrInvalid)

	clk.Advance(15 * time.Second)
	renewedRW, err := disk.RenewMount(ctx, DefaultBranchName, rw1.ID, false, rw1.WriterEpoch, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, clk.Now().Add(30*time.Second).UnixNano(), renewedRW.ExpiresAt)

	renewedRO, err := disk.RenewMount(ctx, DefaultBranchName, ro1.ID, true, 0, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, clk.Now().Add(30*time.Second).UnixNano(), renewedRO.ExpiresAt)

	_, err = disk.RenewMount(ctx, DefaultBranchName, rw1.ID, false, rw1.WriterEpoch+99, 30*time.Second)
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = disk.RenewMount(ctx, DefaultBranchName, rw1.ID, true, 0, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = disk.RenewMount(ctx, "feat", ro1.ID, true, 0, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)

	rw2, err := disk.MountBranch(ctx, DefaultBranchName, false, true, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, rw1.WriterEpoch+1, rw2.WriterEpoch)
	_, err = disk.GetMount(rw1.ID)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = disk.RenewMount(ctx, DefaultBranchName, rw1.ID, false, rw1.WriterEpoch, 30*time.Second)
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = disk.Create(WithWriterEpoch(ctx, rw1.WriterEpoch), DefaultBranchName, RootIno, "stale.txt", 0o644, 0, 0)
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = disk.Create(WithWriterEpoch(ctx, rw2.WriterEpoch), DefaultBranchName, RootIno, "fresh.txt", 0o644, 0, 0)
	require.NoError(t, err)

	clk.Advance(10 * time.Second)
	featMountsAfterExp, err := disk.ListMounts("feat", clk.Now())
	require.NoError(t, err)
	require.Empty(t, featMountsAfterExp)
	hasFeat, err := disk.HasActiveMounts("feat", clk.Now())
	require.NoError(t, err)
	require.False(t, hasFeat)

	_, err = disk.RenewMount(ctx, "feat", featRW.ID, false, featRW.WriterEpoch, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)

	featRW2, err := disk.MountBranch(ctx, "feat", false, false, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, featRW.WriterEpoch+1, featRW2.WriterEpoch)
	_, err = disk.GetMount(featRW.ID)
	require.ErrorIs(t, err, ErrNotFound)

	clk.Advance(6 * time.Second)
	pruned, err := disk.PruneExpiredMounts(clk.Now())
	require.NoError(t, err)
	require.Equal(t, []string{ro2.ID}, pruned)

	require.NoError(t, disk.Unmount(ctx, ro1.ID))
	require.NoError(t, disk.Unmount(ctx, ro1.ID))
	_, err = disk.RenewMount(ctx, DefaultBranchName, ro1.ID, true, 0, 30*time.Second)
	require.ErrorIs(t, err, ErrNotFound)

	ro3, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)
	mainEpochBeforeROEvict := rw2.WriterEpoch

	_, err = disk.EvictMounts(ctx, "feat", ro3.ID)
	require.ErrorIs(t, err, ErrNotFound)

	evictRO, err := disk.EvictMounts(ctx, DefaultBranchName, ro3.ID)
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictRO.EvictedCount)
	require.Equal(t, []string{ro3.ID}, evictRO.EvictedMountIDs)
	require.Empty(t, evictRO.UpdatedBranches)
	mainNow, err := disk.GetBranch(DefaultBranchName)
	require.NoError(t, err)
	require.Equal(t, mainEpochBeforeROEvict, mainNow.WriterEpoch)

	evictMissing, err := disk.EvictMounts(ctx, DefaultBranchName, "nonexistent-mount-id")
	require.NoError(t, err)
	require.Equal(t, uint32(0), evictMissing.EvictedCount)
	require.Empty(t, evictMissing.EvictedMountIDs)
	require.Empty(t, evictMissing.UpdatedBranches)

	evictRW, err := disk.EvictMounts(ctx, "", rw2.ID)
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictRW.EvictedCount)
	require.Equal(t, []string{rw2.ID}, evictRW.EvictedMountIDs)
	require.Len(t, evictRW.UpdatedBranches, 1)
	require.Equal(t, DefaultBranchName, evictRW.UpdatedBranches[0].Name)
	require.Equal(t, rw2.WriterEpoch+1, evictRW.UpdatedBranches[0].WriterEpoch)
	_, err = disk.Create(WithWriterEpoch(ctx, rw2.WriterEpoch), DefaultBranchName, RootIno, "after-evict.txt", 0o644, 0, 0)
	require.ErrorIs(t, err, ErrStaleEpoch)

	mainRW3, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	mainRO4, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)

	evictBranch, err := disk.EvictMounts(ctx, DefaultBranchName, "")
	require.NoError(t, err)
	require.Equal(t, uint32(2), evictBranch.EvictedCount)
	require.Len(t, evictBranch.EvictedMountIDs, 2)
	require.Contains(t, evictBranch.EvictedMountIDs, mainRW3.ID)
	require.Contains(t, evictBranch.EvictedMountIDs, mainRO4.ID)
	require.Len(t, evictBranch.UpdatedBranches, 1)
	require.Equal(t, mainRW3.WriterEpoch+1, evictBranch.UpdatedBranches[0].WriterEpoch)

	featActiveStill, err := disk.HasActiveMounts("feat", clk.Now())
	require.NoError(t, err)
	require.True(t, featActiveStill)

	mainRW4, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	expRO, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 5*time.Second)
	require.NoError(t, err)
	clk.Advance(6 * time.Second)

	evictAll, err := disk.EvictMounts(ctx, "", "")
	require.NoError(t, err)
	require.Equal(t, uint32(2), evictAll.EvictedCount)
	require.Contains(t, evictAll.EvictedMountIDs, mainRW4.ID)
	require.Contains(t, evictAll.EvictedMountIDs, featRW2.ID)
	require.Contains(t, evictAll.EvictedMountIDs, expRO.ID)
	require.Len(t, evictAll.UpdatedBranches, 2)
	require.Equal(t, "feat", evictAll.UpdatedBranches[0].Name)
	require.Equal(t, featRW2.WriterEpoch+1, evictAll.UpdatedBranches[0].WriterEpoch)
	require.Equal(t, DefaultBranchName, evictAll.UpdatedBranches[1].Name)
	require.Equal(t, mainRW4.WriterEpoch+1, evictAll.UpdatedBranches[1].WriterEpoch)

	hasAny, err := disk.HasActiveMounts("", clk.Now())
	require.NoError(t, err)
	require.False(t, hasAny)
}

func TestStep20_MountGates_DeleteBranchResetMergeForkCommitAndExpiredZombieFencing(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	clk := newStepClock(time.Date(2026, 11, 24, 14, 0, 0, 0, time.UTC))

	disk, err := OpenDisk(DiskOptions{
		Dir:       dir,
		DiskID:    "disk-step20-gates",
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
	cBase, err := disk.Commit(ctx, DefaultBranchName, "base")
	require.NoError(t, err)

	mainRW, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	mainRO, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)

	rwCtx := WithWriterEpoch(ctx, mainRW.WriterEpoch)
	_, err = disk.Write(rwCtx, DefaultBranchName, baseF.Ino, 0, []byte("v2"))
	require.NoError(t, err)
	cMounted, err := disk.Commit(ctx, DefaultBranchName, "commit while mounted")
	require.NoError(t, err)
	require.Greater(t, cMounted.ID, cBase.ID)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "temp", "fork while mounted")
	require.NoError(t, err)

	require.NoError(t, disk.Unmount(ctx, mainRW.ID))
	require.NoError(t, disk.Unmount(ctx, mainRO.ID))

	tempRO, err := disk.MountBranch(ctx, "temp", true, false, 20*time.Second)
	require.NoError(t, err)
	require.ErrorIs(t, disk.DeleteBranch(ctx, "temp"), ErrDiskMounted)
	require.NoError(t, disk.Unmount(ctx, tempRO.ID))

	_, err = disk.MountBranch(ctx, "temp", false, false, 20*time.Second)
	require.NoError(t, err)
	require.ErrorIs(t, disk.DeleteBranch(ctx, "temp"), ErrDiskMounted)

	clk.Advance(21 * time.Second)
	require.NoError(t, disk.DeleteBranch(ctx, "temp"))

	resetRO, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 20*time.Second)
	require.NoError(t, err)
	_, err = disk.Reset(ctx, DefaultBranchName, cBase.ID)
	require.ErrorIs(t, err, ErrDiskMounted)
	require.NoError(t, disk.Unmount(ctx, resetRO.ID))

	zombieReset, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 20*time.Second)
	require.NoError(t, err)
	_, err = disk.Reset(ctx, DefaultBranchName, cBase.ID)
	require.ErrorIs(t, err, ErrDiskMounted)

	clk.Advance(21 * time.Second)
	resetBM, err := disk.Reset(ctx, DefaultBranchName, cBase.ID)
	require.NoError(t, err)
	require.Equal(t, cBase.ID, resetBM.HeadCommitID)
	require.Equal(t, zombieReset.WriterEpoch+1, resetBM.WriterEpoch)

	_, err = disk.Write(WithWriterEpoch(ctx, zombieReset.WriterEpoch), DefaultBranchName, baseF.Ino, 0, []byte("zombie"))
	require.ErrorIs(t, err, ErrStaleEpoch)
	_, err = disk.RenewMount(ctx, DefaultBranchName, zombieReset.ID, false, zombieReset.WriterEpoch, 20*time.Second)
	require.ErrorIs(t, err, ErrStaleEpoch)

	_, _, err = disk.Fork(ctx, DefaultBranchName, "src", "fork src")
	require.NoError(t, err)
	srcF, err := disk.Create(ctx, "src", RootIno, "src.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "src", srcF.Ino, 0, []byte("from-src"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "src", "src commit")
	require.NoError(t, err)

	tgtRW, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 30*time.Second)
	require.NoError(t, err)
	srcRW, err := disk.MountBranch(ctx, "src", false, false, 30*time.Second)
	require.NoError(t, err)

	dryRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "src",
		TargetBranch: DefaultBranchName,
		DryRun:       true,
	})
	require.NoError(t, err)
	require.True(t, dryRes.FastForward)

	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "src",
		TargetBranch: DefaultBranchName,
	})
	require.ErrorIs(t, err, ErrDiskMounted)

	require.NoError(t, disk.Unmount(ctx, tgtRW.ID))
	tgtRO, err := disk.MountBranch(ctx, DefaultBranchName, true, false, 30*time.Second)
	require.NoError(t, err)
	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "src",
		TargetBranch: DefaultBranchName,
	})
	require.ErrorIs(t, err, ErrDiskMounted)

	require.NoError(t, disk.Unmount(ctx, tgtRO.ID))
	_, err = disk.Merge(ctx, MergeOptions{
		SourceBranch: "src",
		TargetBranch: DefaultBranchName,
	})
	require.ErrorIs(t, err, ErrDiskMounted)

	require.NoError(t, disk.Unmount(ctx, srcRW.ID))
	srcRO, err := disk.MountBranch(ctx, "src", true, false, 30*time.Second)
	require.NoError(t, err)

	mergeRes, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "src",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.True(t, mergeRes.FastForward)
	require.Equal(t, tgtRW.WriterEpoch+1, mergeRes.WriterEpoch)
	require.NoError(t, disk.Unmount(ctx, srcRO.ID))

	_, _, err = disk.Fork(ctx, DefaultBranchName, "src2", "fork src2")
	require.NoError(t, err)
	src2F, err := disk.Create(ctx, "src2", RootIno, "src2.txt", 0o644, 0, 0)
	require.NoError(t, err)
	_, err = disk.Write(ctx, "src2", src2F.Ino, 0, []byte("from-src2"))
	require.NoError(t, err)
	_, err = disk.Commit(ctx, "src2", "src2 commit")
	require.NoError(t, err)

	zombieMerge, err := disk.MountBranch(ctx, DefaultBranchName, false, false, 20*time.Second)
	require.NoError(t, err)
	clk.Advance(21 * time.Second)

	mergeRes2, err := disk.Merge(ctx, MergeOptions{
		SourceBranch: "src2",
		TargetBranch: DefaultBranchName,
	})
	require.NoError(t, err)
	require.Equal(t, zombieMerge.WriterEpoch+1, mergeRes2.WriterEpoch)

	_, err = disk.Create(WithWriterEpoch(ctx, zombieMerge.WriterEpoch), DefaultBranchName, RootIno, "zombie-merge.txt", 0o644, 0, 0)
	require.ErrorIs(t, err, ErrStaleEpoch)
}
