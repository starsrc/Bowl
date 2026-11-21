package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
)

func TestToConnectError_SnapshotErrors_MapsCodeAndReason(t *testing.T) {
	requireConnectErrorCodeAndReason(t, toConnectError(control.ErrSnapshotInUse), connect.CodeFailedPrecondition, "EBUSY")
	requireConnectErrorCodeAndReason(t, toConnectError(control.ErrCrossLineage), connect.CodeFailedPrecondition, "EXDEV")
}

func TestServer_SnapshotLifecycleWireContract(t *testing.T) {
	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "main",
		MaxBytes: 32 << 20,
	}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()

	mainMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()

	fileIno := createTestFileViaRPC(t, ctx, client, mainID, mainEpoch, "hello.txt", []byte("hello-snap"))

	snapResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: mainID,
		Name:   "manual-1",
	}))
	require.NoError(t, err)
	manualSnap := snapResp.Msg.GetSnapshot()
	require.NotNil(t, manualSnap)
	require.NotEmpty(t, manualSnap.GetSnapshotId())
	require.Equal(t, mainID, manualSnap.GetSourceDiskId())
	require.Empty(t, manualSnap.GetParentSnapshotId())
	require.Equal(t, storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL, manualSnap.GetKind())
	require.Equal(t, "manual-1", manualSnap.GetName())
	require.False(t, manualSnap.GetProtected())
	require.NotNil(t, manualSnap.GetCreatedAt())

	readSnap, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     mainID,
		SnapshotId: manualSnap.GetSnapshotId(),
		Ino:        fileIno,
		Offset:     0,
		Size:       1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello-snap"), readSnap.Msg.GetData())
	require.True(t, readSnap.Msg.GetEof())

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         "feat",
	}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()

	featMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()
	createTestFileViaRPC(t, ctx, client, featID, featEpoch, "f1.txt", []byte("f1"))

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	merge1, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Force:        true,
	}))
	require.NoError(t, err)

	featMnt2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	createTestFileViaRPC(t, ctx, client, featID, featMnt2.Msg.GetEpoch(), "f2.txt", []byte("f2"))
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

	var listed []*storagev1.Snapshot
	token := ""
	for {
		listResp, lErr := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
			DiskId:    mainID,
			PageSize:  2,
			PageToken: token,
		}))
		require.NoError(t, lErr)
		listed = append(listed, listResp.Msg.GetSnapshots()...)
		if listResp.Msg.GetNextPageToken() == "" {
			break
		}
		token = listResp.Msg.GetNextPageToken()
	}
	require.Len(t, listed, 4)

	protectedMap := make(map[string]bool, len(listed))
	for _, s := range listed {
		protectedMap[s.GetSnapshotId()] = s.GetProtected()
	}
	require.True(t, protectedMap[merge2.Msg.GetMergeSnapshotId()])
	require.False(t, protectedMap[merge1.Msg.GetMergeSnapshotId()])
	require.False(t, protectedMap[merge1.Msg.GetLcaSnapshotId()])
	require.False(t, protectedMap[manualSnap.GetSnapshotId()])

	manualList, err := client.ListSnapshots(ctx, connect.NewRequest(&storagev1.ListSnapshotsRequest{
		DiskId: mainID,
		Kind:   storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
	}))
	require.NoError(t, err)
	require.Len(t, manualList.Msg.GetSnapshots(), 1)
	require.Equal(t, manualSnap.GetSnapshotId(), manualList.Msg.GetSnapshots()[0].GetSnapshotId())

	otherResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "other",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	otherSnapResp, err := client.CreateSnapshot(ctx, connect.NewRequest(&storagev1.CreateSnapshotRequest{
		DiskId: otherResp.Msg.GetDisk().GetDiskId(),
		Name:   "other-snap",
	}))
	require.NoError(t, err)

	_, crossErr := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: otherSnapResp.Msg.GetSnapshot().GetSnapshotId(),
	}))
	requireConnectErrorCodeAndReason(t, crossErr, connect.CodeFailedPrecondition, "EXDEV")

	_, busyErr := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: merge2.Msg.GetMergeSnapshotId(),
	}))
	requireConnectErrorCodeAndReason(t, busyErr, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: manualSnap.GetSnapshotId(),
	}))
	require.NoError(t, err)

	_, notFoundErr := client.DeleteSnapshot(ctx, connect.NewRequest(&storagev1.DeleteSnapshotRequest{
		DiskId:     mainID,
		SnapshotId: manualSnap.GetSnapshotId(),
	}))
	requireConnectErrorCodeAndReason(t, notFoundErr, connect.CodeNotFound, "ENOENT")

	dryPrune, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: mainID,
		DryRun: true,
	}))
	require.NoError(t, err)
	require.True(t, dryPrune.Msg.GetDryRun())
	require.Equal(t, uint32(2), dryPrune.Msg.GetReclaimedCount())
	require.Len(t, dryPrune.Msg.GetSnapshots(), 2)

	realPrune, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: mainID,
		DryRun: false,
	}))
	require.NoError(t, err)
	require.False(t, realPrune.Msg.GetDryRun())
	require.Equal(t, uint32(2), realPrune.Msg.GetReclaimedCount())
	require.Len(t, realPrune.Msg.GetSnapshots(), 2)

	emptyPrune, err := client.PruneSnapshots(ctx, connect.NewRequest(&storagev1.PruneSnapshotsRequest{
		DiskId: mainID,
		DryRun: false,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(0), emptyPrune.Msg.GetReclaimedCount())
	require.Empty(t, emptyPrune.Msg.GetSnapshots())
}
