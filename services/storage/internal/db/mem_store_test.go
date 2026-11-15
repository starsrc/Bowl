package db_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

func TestMemStore_GivenInjectedWriteError_WhenForkDiskCalled_ThenRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	injected := errors.New("injected write error")
	mem.SetFailNextWrite(injected)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  mustNewUUIDv7(t),
		CommitID:     mustNewUUIDv7(t),
		Name:         "fail-child",
	})
	require.ErrorIs(t, err, injected)

	snapAfterFail := mem.Inspect()
	require.Len(t, snapAfterFail.Disks, 1, "注入写失败后盘数量 want=1, got=%d", len(snapAfterFail.Disks))
	require.Empty(t, snapAfterFail.Commits, "注入写失败后不得残留切面记录")

	// 故障注入仅触发一次，后续写操作自动恢复。
	childID := mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		CommitID:     mustNewUUIDv7(t),
		Name:         "recovered-child",
	})
	require.NoError(t, err)

	snapRecovered := mem.Inspect()
	require.Len(t, snapRecovered.Disks, 2, "恢复后盘数量 want=2, got=%d", len(snapRecovered.Disks))
	require.Len(t, snapRecovered.Commits, 1, "恢复后切面数量 want=1, got=%d", len(snapRecovered.Commits))
}

func TestMemStore_GivenInjectedWriteError_WhenMergeDiskCalled_ThenRollsBackAtomically(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	root, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	childID, cmFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		CommitID:     cmFork,
		Name:         "child",
	})
	require.NoError(t, err)

	injected := errors.New("injected merge write error")
	mem.SetFailNextWrite(injected)
	_, err = mem.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:                  root.LineageID,
		SourceDiskID:               childID,
		TargetDiskID:               rootID,
		ExpectedSourceBaseCommitID: cmFork,
		ExpectedTargetBaseCommitID: cmFork,
		NewSourceCommitID:          mustNewUUIDv7(t),
		NewMergeCommitID:           mustNewUUIDv7(t),
	})
	require.ErrorIs(t, err, injected)

	snapAfterFail := mem.Inspect()
	require.Len(t, snapAfterFail.Commits, 1, "注入写失败后不得残留新 Commit")
	require.Equal(t, cmFork, snapAfterFail.Disks[rootID].BaseCommitID)
	require.Equal(t, cmFork, snapAfterFail.Disks[childID].BaseCommitID)
}

func TestMemStore_GivenInjectedWriteError_WhenReserveSegmentCalled_ThenFailsWithoutAdvancing(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	injectedInoErr := errors.New("injected inode reserve error")
	mem.SetFailNextWrite(injectedInoErr)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindInode, rootID, 2, 10)
	require.ErrorIs(t, err, injectedInoErr)

	snapAfterInoFail := mem.Inspect()
	require.Equal(t, int64(2), snapAfterInoFail.Lineages[rootID].NextIno, "注入失败后 NextIno 不得推进")
	require.Equal(t, int64(1), snapAfterInoFail.Lineages[rootID].NextPackID, "注入失败后 NextPackID 不得推进")
	require.Equal(t, 0, snapAfterInoFail.InodeReserveCalls, "注入失败后 InodeReserveCalls 应保持 0")

	segIno, err := mem.ReserveSegment(ctx, db.SegmentKindInode, rootID, 2, 10)
	require.NoError(t, err)
	require.Equal(t, db.SegmentRange{First: 2, Last: 11}, segIno)

	injectedPackErr := errors.New("injected pack reserve error")
	mem.SetFailNextWrite(injectedPackErr)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindPack, rootID, 1, 8)
	require.ErrorIs(t, err, injectedPackErr)

	snapAfterPackFail := mem.Inspect()
	require.Equal(t, int64(12), snapAfterPackFail.Lineages[rootID].NextIno)
	require.Equal(t, int64(1), snapAfterPackFail.Lineages[rootID].NextPackID, "注入失败后 NextPackID 不得推进")
	require.Equal(t, 0, snapAfterPackFail.PackReserveCalls, "注入失败后 PackReserveCalls 应保持 0")

	segPack, err := mem.ReserveSegment(ctx, db.SegmentKindPack, rootID, 1, 8)
	require.NoError(t, err)
	require.Equal(t, db.SegmentRange{First: 1, Last: 8}, segPack)
}

func TestMemStore_GivenCanceledContext_WhenMethodsCalled_ThenFailsFast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mem := db.NewMemStore()
	id := mustNewUUIDv7(t)

	_, err := mem.CreateDisk(ctx, id, "canceled")
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: id, ChildDiskID: mustNewUUIDv7(t), CommitID: mustNewUUIDv7(t), Name: "c"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.GetDisk(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListDisks(ctx, db.ListDisksQuery{})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.UpdateDiskName(ctx, id, "x")
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.DeleteDisk(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ListLineageCommits(ctx, id)
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.MergeDisk(ctx, &db.MergeDiskInput{
		LineageID:         id,
		SourceDiskID:      id,
		TargetDiskID:      mustNewUUIDv7(t),
		NewSourceCommitID: mustNewUUIDv7(t),
		NewMergeCommitID:  mustNewUUIDv7(t),
	})
	require.ErrorIs(t, err, context.Canceled)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindInode, id, 2, 10)
	require.ErrorIs(t, err, context.Canceled)
}

func TestMemStore_GivenReserveSegmentCalls_WhenInspected_ThenTracksCounters(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	_, err = mem.ReserveSegment(ctx, db.SegmentKindInode, rootID, 2, 10)
	require.NoError(t, err)
	_, err = mem.ReserveSegment(ctx, db.SegmentKindPack, rootID, 1, 10)
	require.NoError(t, err)

	snap := mem.Inspect()
	require.Equal(t, 1, snap.InodeReserveCalls, "InodeReserveCalls want=1, got=%d", snap.InodeReserveCalls)
	require.Equal(t, 1, snap.PackReserveCalls, "PackReserveCalls want=1, got=%d", snap.PackReserveCalls)
}

func TestMemStore_GivenTriStateDelete_WhenInspected_ThenClearsSurvivorBaseCommit(t *testing.T) {
	ctx := context.Background()
	mem := db.NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	childID, cmID := mustNewUUIDv7(t), mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, db.ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  childID,
		CommitID:     cmID,
		Name:         "child",
	})
	require.NoError(t, err)
	require.Equal(t, cmID, mem.Inspect().Disks[childID].BaseCommitID)

	_, err = mem.DeleteDisk(ctx, rootID)
	require.NoError(t, err)
	snapAfterDelete := mem.Inspect()
	require.Equal(t, uuid.Nil, snapAfterDelete.Disks[childID].BaseCommitID, "|D|==1 后幸存盘 BaseCommitID 应清空为 Nil UUID")
	require.Empty(t, snapAfterDelete.Commits)
}

func TestDBModule_GivenInvalidOrValidPoolConfig_WhenOpenPoolOrFxValidated_ThenMatchesContract(t *testing.T) {
	_, err := db.OpenPool(config.DBPoolConfig{DSN: "://bad-dsn"})
	require.Error(t, err, "非法 DSN 调用 OpenPool 必须返回错误")

	poolCfg := config.DBPoolConfig{
		DSN:            resolveMetaTestDSN(),
		TracingEnabled: true,
		MetricsEnabled: false,
	}
	require.NoError(t, fx.ValidateApp(fx.Supply(poolCfg), db.Module), "db.Module 依赖图校验应通过")
}
