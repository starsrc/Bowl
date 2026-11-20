//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func e2eRegisterDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			require.NoError(t, err, "清理测试磁盘失败: %s", diskID)
		}
	})
}

func e2eWriteNewFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) uint64 {
	t.Helper()
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte(name),
		Mode:      0o100644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	ino := createResp.Msg.GetInode().GetIno()
	if len(data) > 0 {
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Ino:    ino,
			Offset: 0,
			Data:   data,
			Epoch:  epoch,
		}))
		require.NoError(t, err)
	}
	return ino
}

func e2eOverwriteFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte(name),
	}))
	require.NoError(t, err)
	ino := lookupResp.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Ino:    ino,
		Offset: 0,
		Data:   data,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	size := uint64(len(data))
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID,
		Ino:    ino,
		Size:   &size,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
}

func TestE2E_DiffReadCommitNodeAndMergeDisk_WithPostgreSQLAndWriterFence(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-merge-main-%d", suffix),
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, mainID)

	mainMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()
	mainMountID := mainMnt.Msg.GetMountId()

	sharedIno := e2eWriteNewFile(t, ctx, client, mainID, mainEpoch, "shared.txt", []byte("base-shared\n"))
	e2eWriteNewFile(t, ctx, client, mainID, mainEpoch, "base.txt", []byte("base-only\n"))

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         fmt.Sprintf("e2e-merge-feat-%d", suffix),
	}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, featID)

	featMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()
	featMountID := featMnt.Msg.GetMountId()

	e2eOverwriteFile(t, ctx, client, mainID, mainEpoch, "shared.txt", []byte("main-edited\n"))
	e2eOverwriteFile(t, ctx, client, featID, featEpoch, "shared.txt", []byte("feat-edited\n"))
	e2eWriteNewFile(t, ctx, client, featID, featEpoch, "feat.txt", []byte("feat-added\n"))

	diffTwoWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
	}))
	require.NoError(t, err)
	lcaCommitID := diffTwoWay.Msg.GetLcaCommitId()
	require.NotEmpty(t, lcaCommitID)
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetAdded())
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetModified())

	diffThreeWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId:  featID,
		TargetDiskId:  &mainID,
		ConflictsOnly: true,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffThreeWay.Msg.GetSummary().GetConflicts())
	require.Len(t, diffThreeWay.Msg.GetEntries(), 1)
	require.Equal(t, []byte("shared.txt"), diffThreeWay.Msg.GetEntries()[0].GetPath())
	require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, diffThreeWay.Msg.GetEntries()[0].GetConflict())

	commitSlice, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   featID,
		CommitId: lcaCommitID,
		Ino:      sharedIno,
		Offset:   0,
		Size:     1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("base-shared\n"), commitSlice.Msg.GetData())
	require.True(t, commitSlice.Msg.GetEof())

	dryResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
		DryRun:       true,
	}))
	require.NoError(t, err)
	require.Empty(t, dryResp.Msg.GetMergeCommitId())

	_, busyBothErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busyBothErr, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMountID,
	}))
	require.NoError(t, err)

	_, busyTargetErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busyTargetErr, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  mainID,
		MountId: mainMountID,
	}))
	require.NoError(t, err)

	_, conflictErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
	}))
	testutil.RequireConnectError(t, conflictErr, connect.CodeAborted, "ECONFLICT")

	connErr, ok := errors.AsType[*connect.Error](conflictErr)
	require.True(t, ok)
	var report *storagev1.MergeConflictReport
	for _, d := range connErr.Details() {
		v, valErr := d.Value()
		if valErr == nil {
			if r, isRep := v.(*storagev1.MergeConflictReport); isRep {
				report = r
				break
			}
		}
	}
	require.NotNil(t, report)
	require.Equal(t, lcaCommitID, report.GetLcaCommitId())
	require.Equal(t, uint64(1), report.GetTotalConflicts())

	mergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"/shared.txt": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
	}))
	require.NoError(t, err)
	require.NotEmpty(t, mergeResp.Msg.GetMergeCommitId())

	_, staleErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("stale.txt"),
		Mode:      0o100644,
		Epoch:     mainEpoch,
	}))
	testutil.RequireConnectError(t, staleErr, connect.CodeFailedPrecondition, "EBUSY")

	reMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	reMntEpoch := reMnt.Msg.GetEpoch()
	require.Greater(t, reMntEpoch, mainEpoch)

	lookupFeat, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("feat.txt"),
	}))
	require.NoError(t, err)
	readFeat, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-added\n"), readFeat.Msg.GetData())

	lookupShared, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("shared.txt"),
	}))
	require.NoError(t, err)
	readShared, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupShared.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-edited\n"), readShared.Msg.GetData())

	fork2Resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         fmt.Sprintf("e2e-merge-feat2-%d", suffix),
	}))
	require.NoError(t, err)
	feat2ID := fork2Resp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, feat2ID)

	feat2Mnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: feat2ID}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, feat2ID, feat2Mnt.Msg.GetEpoch(), "feat2.txt", []byte("feat2-forced\n"))

	forceMergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: feat2ID,
		TargetDiskId: mainID,
		Force:        true,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, forceMergeResp.Msg.GetMergeCommitId())

	_, fencedErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("fenced-after-force.txt"),
		Mode:      0o100644,
		Epoch:     reMntEpoch,
	}))
	testutil.RequireConnectError(t, fencedErr, connect.CodeFailedPrecondition, "EBUSY")

	reMntAfterForce, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	require.Greater(t, reMntAfterForce.Msg.GetEpoch(), reMntEpoch)

	lookupFeat2, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("feat2.txt"),
	}))
	require.NoError(t, err)
	readFeat2, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat2.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat2-forced\n"), readFeat2.Msg.GetData())
}
