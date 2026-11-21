package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func createTestFileViaRPC(
	t *testing.T,
	ctx context.Context,
	client interface {
		Create(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
		Write(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	},
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) uint64 {
	t.Helper()
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		ParentIno: control.RootIno,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
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

func overwriteTestFileViaRPC(
	t *testing.T,
	ctx context.Context,
	client interface {
		Lookup(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
		Write(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
		SetAttr(context.Context, *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error)
	},
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: control.RootIno,
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

func TestServer_DiffReadSnapshotNodeAndMergeDisk_EndToEndWireContract(t *testing.T) {

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

	cfgIno := createTestFileViaRPC(t, ctx, client, mainID, mainEpoch, "cfg.ini", []byte("key=base\n"))
	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("cur.ini"),
		Target:    []byte("/cfg.ini"),
		Epoch:     mainEpoch,
	}))
	require.NoError(t, err)

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         "feature",
	}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()

	featMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()

	overwriteTestFileViaRPC(t, ctx, client, mainID, mainEpoch, "cfg.ini", []byte("key=main\n"))
	overwriteTestFileViaRPC(t, ctx, client, featID, featEpoch, "cfg.ini", []byte("key=feature\n"))
	createTestFileViaRPC(t, ctx, client, featID, featEpoch, "feature.txt", []byte("feat-content\n"))

	diffTwoWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
	}))
	require.NoError(t, err)
	lcaSnapshotID := diffTwoWay.Msg.GetLcaSnapshotId()
	require.NotEmpty(t, lcaSnapshotID)
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetAdded())
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetModified())

	diffPage1, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: &mainID,
		PageSize:     1,
	}))
	require.NoError(t, err)
	require.Len(t, diffPage1.Msg.GetEntries(), 1)
	require.NotEmpty(t, diffPage1.Msg.GetNextPageToken())
	require.Equal(t, uint64(1), diffPage1.Msg.GetSummary().GetConflicts())

	nodeSlice, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     featID,
		SnapshotId: lcaSnapshotID,
		Ino:        cfgIno,
		Offset:     4,
		Size:       16,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("base\n"), nodeSlice.Msg.GetData())
	require.True(t, nodeSlice.Msg.GetEof())

	symSlice, err := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{
		DiskId:     featID,
		SnapshotId: lcaSnapshotID,
		Ino:        symResp.Msg.GetInode().GetIno(),
		Offset:     0,
		Size:       128,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("/cfg.ini"), symSlice.Msg.GetSymlinkTarget())

	_, busyErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
	}))
	requireConnectErrorCodeAndReason(t, busyErr, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  featID,
		MountId: featMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, mergeErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Force:        true,
	}))
	requireConnectErrorCodeAndReason(t, mergeErr, connect.CodeAborted, "ECONFLICT")

	connectErr, ok := errors.AsType[*connect.Error](mergeErr)
	require.True(t, ok)
	var conflictReport *storagev1.MergeConflictReport
	for _, detail := range connectErr.Details() {
		msg, valErr := detail.Value()
		if valErr != nil {
			continue
		}
		if report, isReport := msg.(*storagev1.MergeConflictReport); isReport {
			conflictReport = report
			break
		}
	}
	require.NotNil(t, conflictReport, "ECONFLICT 响应必须附带 *storagev1.MergeConflictReport Detail")
	require.Equal(t, uint64(1), conflictReport.GetTotalConflicts())
	require.Len(t, conflictReport.GetConflicts(), 1)
	require.Equal(t, []byte("cfg.ini"), conflictReport.GetConflicts()[0].GetPath())
	require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, conflictReport.GetConflicts()[0].GetKind())

	overrideQuota := uint64(64 << 20)
	dryResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		DryRun:       true,
		MaxBytes:     &overrideQuota,
	}))
	require.NoError(t, err)
	require.Empty(t, dryResp.Msg.GetMergeSnapshotId())
	require.Equal(t, overrideQuota, dryResp.Msg.GetTargetMaxBytes())

	mergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"/cfg.ini": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
		Force:    true,
		MaxBytes: &overrideQuota,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, mergeResp.Msg.GetMergeSnapshotId())
	require.Equal(t, overrideQuota, mergeResp.Msg.GetDisk().GetMaxBytes())

	_, staleErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("stale.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mainEpoch,
	}))
	requireConnectErrorCodeAndReason(t, staleErr, connect.CodeFailedPrecondition, "EBUSY")

	lookupFeat, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("feature.txt"),
	}))
	require.NoError(t, err)
	readFeat, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-content\n"), readFeat.Msg.GetData())

	_, renewStaleErr := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   mainID,
		MountId:  mainMnt.Msg.GetMountId(),
		ReadOnly: false,
		Epoch:    mainEpoch,
	}))
	requireConnectErrorCodeAndReason(t, renewStaleErr, connect.CodeFailedPrecondition, "EBUSY")

	mainMnt2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	renewResp, err := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   mainID,
		MountId:  mainMnt2.Msg.GetMountId(),
		ReadOnly: false,
		Epoch:    mainMnt2.Msg.GetEpoch(),
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewResp.Msg.GetLeaseTtlSeconds())
}

func TestToConnectError_ConcurrentMerge_MapsToAbortedEBUSY(t *testing.T) {
	requireConnectErrorCodeAndReason(t, toConnectError(control.ErrConcurrentMerge), connect.CodeAborted, "EBUSY")
}

func TestServer_DeleteDiskRecursiveWireContract(t *testing.T) {
	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	rootResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "root",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	rootID := rootResp.Msg.GetDisk().GetDiskId()

	childResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: rootID,
		Name:         "child",
	}))
	require.NoError(t, err)
	childID := childResp.Msg.GetDisk().GetDiskId()

	grandResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: childID,
		Name:         "grand",
	}))
	require.NoError(t, err)
	grandID := grandResp.Msg.GetDisk().GetDiskId()

	grandMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   grandID,
		ReadOnly: true,
	}))
	require.NoError(t, err)

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId:    childID,
		Recursive: true,
	}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeAborted, "EBUSY")

	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId:  grandID,
		MountId: grandMnt.Msg.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId:    childID,
		Recursive: true,
	}))
	require.NoError(t, err)

	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: childID}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")
	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: grandID}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")
	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: rootID}))
	require.NoError(t, err)
}
