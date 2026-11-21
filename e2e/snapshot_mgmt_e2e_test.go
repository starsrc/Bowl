//go:build e2e

package e2e_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_ExplicitSnapshotLifecycleAndActiveLCAHull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-snapmgmt-main-%d", suffix),
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, mainID)

	mainMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()
	mainMountID := mainMnt.Msg.GetMountId()

	baseIno := e2eWriteNewFile(t, ctx, client, mainID, mainEpoch, "app.cfg", []byte("ver=1\n"))

	namedSnapResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: mainID,
		Name:   "pre-fork-named",
	}))
	require.NoError(t, err)
	namedSnap := namedSnapResp.Msg.GetSnapshot()
	require.NotNil(t, namedSnap)
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL, namedSnap.GetKind())
	require.Equal(t, "pre-fork-named", namedSnap.GetName())
	require.False(t, namedSnap.GetProtected())

	e2eOverwriteFile(t, ctx, client, mainID, mainEpoch, "app.cfg", []byte("ver=2\n"))
	unnamedSnapResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: mainID,
	}))
	require.NoError(t, err)
	unnamedSnap := unnamedSnapResp.Msg.GetSnapshot()
	require.NotNil(t, unnamedSnap)
	require.Empty(t, unnamedSnap.GetName())
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL, unnamedSnap.GetKind())
	require.False(t, unnamedSnap.GetProtected())

	readNamed, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     mainID,
		SnapshotId: namedSnap.GetSnapshotId(),
		Ino:        baseIno,
		Offset:     0,
		Size:       1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("ver=1\n"), readNamed.Msg.GetData())
	require.True(t, readNamed.Msg.GetEof())

	readUnnamed, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     mainID,
		SnapshotId: unnamedSnap.GetSnapshotId(),
		Ino:        baseIno,
		Offset:     0,
		Size:       1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("ver=2\n"), readUnnamed.Msg.GetData())

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  mainID,
		MountId: mainMountID,
	}))
	require.NoError(t, err)

	featResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         fmt.Sprintf("e2e-snapmgmt-feat-%d", suffix),
	}))
	require.NoError(t, err)
	featID := featResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, featID)

	featMnt1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, featID, featMnt1.Msg.GetEpoch(), "step1.txt", []byte("s1\n"))
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMnt1.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	merge1, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
	}))
	require.NoError(t, err)

	featMnt2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, featID, featMnt2.Msg.GetEpoch(), "step2.txt", []byte("s2\n"))
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMnt2.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	merge2, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
	}))
	require.NoError(t, err)

	var allSnaps []*storagev1.Snapshot
	token := ""
	for {
		listResp, lErr := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
			DiskId:    mainID,
			PageSize:  2,
			PageToken: token,
		}))
		require.NoError(t, lErr)
		allSnaps = append(allSnaps, listResp.Msg.GetSnapshots()...)
		if listResp.Msg.GetNextPageToken() == "" {
			break
		}
		token = listResp.Msg.GetNextPageToken()
	}
	require.Len(t, allSnaps, 5)

	protectedByID := make(map[string]bool, len(allSnaps))
	for _, s := range allSnaps {
		protectedByID[s.GetSnapshotId()] = s.GetProtected()
	}
	require.True(t, protectedByID[merge2.Msg.GetMergeSnapshotId()])
	require.False(t, protectedByID[merge1.Msg.GetMergeSnapshotId()])
	require.False(t, protectedByID[merge1.Msg.GetLcaSnapshotId()])
	require.False(t, protectedByID[namedSnap.GetSnapshotId()])
	require.False(t, protectedByID[unnamedSnap.GetSnapshotId()])

	manualFilterResp, err := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
		DiskId: mainID,
		Kind:   storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
	}))
	require.NoError(t, err)
	require.Len(t, manualFilterResp.Msg.GetSnapshots(), 2)

	otherResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-snapmgmt-other-%d", suffix),
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	otherID := otherResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, otherID)
	otherSnapResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: otherID,
		Name:   "other-lineage",
	}))
	require.NoError(t, err)

	_, crossErr := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: otherSnapResp.Msg.GetSnapshot().GetSnapshotId(),
	}))
	testutil.RequireConnectError(t, crossErr, connect.CodeFailedPrecondition, "EXDEV")

	_, busyErr := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: merge2.Msg.GetMergeSnapshotId(),
	}))
	testutil.RequireConnectError(t, busyErr, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: unnamedSnap.GetSnapshotId(),
	}))
	require.NoError(t, err)

	_, readDeletedErr := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     mainID,
		SnapshotId: unnamedSnap.GetSnapshotId(),
		Ino:        baseIno,
		Offset:     0,
		Size:       1024,
	}))
	testutil.RequireConnectError(t, readDeletedErr, connect.CodeNotFound, "ENOENT")

	dryPrune, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: mainID,
		DryRun: true,
	}))
	require.NoError(t, err)
	require.True(t, dryPrune.Msg.GetDryRun())
	require.Equal(t, uint32(2), dryPrune.Msg.GetReclaimedCount())

	realPrune, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: mainID,
		DryRun: false,
	}))
	require.NoError(t, err)
	require.False(t, realPrune.Msg.GetDryRun())
	require.Equal(t, uint32(2), realPrune.Msg.GetReclaimedCount())

	featMnt3, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, featID, featMnt3.Msg.GetEpoch(), "step3.txt", []byte("s3\n"))
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMnt3.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	diffAfterPrune, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: &mainID,
	}))
	require.NoError(t, err)
	require.Equal(t, merge2.Msg.GetMergeSnapshotId(), diffAfterPrune.Msg.GetLcaSnapshotId())
	require.Equal(t, uint64(1), diffAfterPrune.Msg.GetSummary().GetAdded())

	merge3, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, merge3.Msg.GetMergeSnapshotId())

	featManualResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: featID,
		Name:   "feat-owned-manual",
	}))
	require.NoError(t, err)
	featManualID := featManualResp.Msg.GetSnapshot().GetSnapshotId()

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId:    featID,
		Recursive: false,
	}))
	require.NoError(t, err)

	afterDeleteList, err := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
		DiskId: mainID,
		Kind:   storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
	}))
	require.NoError(t, err)
	require.Len(t, afterDeleteList.Msg.GetSnapshots(), 1)
	require.Equal(t, namedSnap.GetSnapshotId(), afterDeleteList.Msg.GetSnapshots()[0].GetSnapshotId())
	require.NotEqual(t, featManualID, afterDeleteList.Msg.GetSnapshots()[0].GetSnapshotId())

	readSurviving, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     mainID,
		SnapshotId: namedSnap.GetSnapshotId(),
		Ino:        baseIno,
		Offset:     0,
		Size:       1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("ver=1\n"), readSurviving.Msg.GetData())
}
