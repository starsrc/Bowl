//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func registerDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = client.EvictMounts(cleanupCtx, connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: diskID}))
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})
}

func branchWriterEpoch(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID, branch string,
) uint64 {
	t.Helper()
	resp, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
	require.NoError(t, err)
	for _, br := range resp.Msg.GetBranches() {
		if br.GetName() == branch {
			return br.GetWriterEpoch()
		}
	}
	require.Failf(t, "branch not found", "disk=%s branch=%s", diskID, branch)
	return 0
}

func TestStorageLive_ProbesAndMetricsReachable(t *testing.T) {
	adminAddr := testutil.GetStorageAdminAddr()

	healthBody := getHTTPStatusAndBody(t, adminAddr+"/healthz", http.StatusOK)
	require.Contains(t, healthBody, `"status":"ok"`)

	readyBody := getHTTPStatusAndBody(t, adminAddr+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := testutil.NewStorageClient().ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 1}))
	require.NoError(t, err)

	metricsBody := getHTTPStatusAndBody(t, adminAddr+"/metrics", http.StatusOK)
	require.Contains(t, metricsBody, "bowl_storage_disks_loaded")
	require.Contains(t, metricsBody, "rpc_server_call_duration")
}

func TestStorageLive_Step23BranchFieldStrictnessGuards(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-step23-guards-%d", time.Now().UnixNano()),
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)

	mainMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     mainEpoch,
		ParentIno: 1,
		Name:      []byte("guard.txt"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    "",
		ParentIno: 1,
		Name:      []byte("guard.txt"),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: "",
		Ino:    fileIno,
		Offset: 0,
		Size:   64,
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: "",
		Epoch:  mainEpoch,
		Ino:    fileIno,
		Offset: 0,
		Data:   []byte("bad"),
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.CreateCommit(ctx, connect.NewRequest(&storagev1.CreateCommitRequest{
		DiskId:  diskID,
		Branch:  "",
		Message: "empty branch",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: "",
		NewBranch:    "feat",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.ResetBranch(ctx, connect.NewRequest(&storagev1.ResetBranchRequest{
		DiskId:         diskID,
		Branch:         "",
		TargetCommitId: 1,
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{
		DiskId: diskID,
		Branch: "",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.DiffWork(ctx, connect.NewRequest(&storagev1.DiffWorkRequest{
		DiskId: diskID,
		Branch: "",
	}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	_, err = client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "fork for optional branch mount checks",
	}))
	require.NoError(t, err)

	featBaseEpoch := branchWriterEpoch(t, ctx, client, diskID, "feat")
	featMnt, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	require.Equal(t, featBaseEpoch+1, featMnt.Msg.GetEpoch())

	allMounts, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
		Branch: "",
	}))
	require.NoError(t, err)
	require.Len(t, allMounts.Msg.GetMounts(), 2)

	mainOnlyMounts, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.Len(t, mainOnlyMounts.Msg.GetMounts(), 1)
	require.Equal(t, testutil.DefaultBranch, mainOnlyMounts.Msg.GetMounts()[0].GetBranch())
	require.Equal(t, mainMnt.Msg.GetMountId(), mainOnlyMounts.Msg.GetMounts()[0].GetMountId())

	featOnlyMounts, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	require.Len(t, featOnlyMounts.Msg.GetMounts(), 1)
	require.Equal(t, "feat", featOnlyMounts.Msg.GetMounts()[0].GetBranch())
	require.Equal(t, featMnt.Msg.GetMountId(), featOnlyMounts.Msg.GetMounts()[0].GetMountId())

	evictFeat, err := client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictFeat.Msg.GetEvictedCount())
	require.Len(t, evictFeat.Msg.GetUpdatedBranches(), 1)
	require.Equal(t, "feat", evictFeat.Msg.GetUpdatedBranches()[0].GetName())
	require.Equal(t, featMnt.Msg.GetEpoch()+1, evictFeat.Msg.GetUpdatedBranches()[0].GetWriterEpoch())

	remainingMounts, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Len(t, remainingMounts.Msg.GetMounts(), 1)
	require.Equal(t, testutil.DefaultBranch, remainingMounts.Msg.GetMounts()[0].GetBranch())

	evictAll, err := client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{
		DiskId: diskID,
		Branch: "",
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictAll.Msg.GetEvictedCount())
	require.Len(t, evictAll.Msg.GetUpdatedBranches(), 1)
	require.Equal(t, testutil.DefaultBranch, evictAll.Msg.GetUpdatedBranches()[0].GetName())
	require.Equal(t, mainEpoch+1, evictAll.Msg.GetUpdatedBranches()[0].GetWriterEpoch())
}

func TestStorageLive_DiskLifecycleQuotaPaginationAndEpochFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-live-lifecycle-%d", suffix),
		MaxBytes: 4 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)
	require.Equal(t, testutil.DefaultBranch, created.Msg.GetDisk().GetDefaultBranch())
	require.Equal(t, uint64(4<<20), created.Msg.GetDisk().GetMaxBytes())
	require.Zero(t, created.Msg.GetDisk().GetUsedBytes())

	extraDiskIDs := make([]string, 0, 2)
	for i := range 2 {
		extra, createErr := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-live-page-%d-%d", suffix, i),
			MaxBytes: 4 << 20,
		}))
		require.NoError(t, createErr)
		extraID := extra.Msg.GetDisk().GetDiskId()
		registerDiskCleanup(t, client, extraID)
		extraDiskIDs = append(extraDiskIDs, extraID)
	}

	got, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, testutil.DefaultBranch, got.Msg.GetDisk().GetDefaultBranch())
	require.Equal(t, uint64(4<<20), got.Msg.GetDisk().GetMaxBytes())
	require.Zero(t, got.Msg.GetDisk().GetUsedBytes())

	foundIDs := make(map[string]bool)
	pageToken := ""
	pagesRead := 0
	for {
		page, listErr := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
			PageSize:  2,
			PageToken: pageToken,
		}))
		require.NoError(t, listErr)
		pagesRead++
		for _, d := range page.Msg.GetDisks() {
			foundIDs[d.GetDiskId()] = true
		}
		if page.Msg.GetNextPageToken() == "" || (foundIDs[diskID] && foundIDs[extraDiskIDs[0]] && foundIDs[extraDiskIDs[1]]) {
			break
		}
		pageToken = page.Msg.GetNextPageToken()
		require.Less(t, pagesRead, 500, "ListDisks pagination did not terminate")
	}
	require.GreaterOrEqual(t, pagesRead, 2)
	require.True(t, foundIDs[diskID], "disk list must contain %s", diskID)
	require.True(t, foundIDs[extraDiskIDs[0]])
	require.True(t, foundIDs[extraDiskIDs[1]])

	mountRO, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId:   diskID,
		Branch:   testutil.DefaultBranch,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, mountRO.Msg.GetEpoch())
	require.NotEmpty(t, mountRO.Msg.GetMountId())
	require.Positive(t, mountRO.Msg.GetLeaseTtlSeconds())
	require.Equal(t, uint64(1), mountRO.Msg.GetRootInode().GetIno())

	renewRO, err := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   testutil.DefaultBranch,
		MountId:  mountRO.Msg.GetMountId(),
		ReadOnly: true,
		Epoch:    0,
	}))
	require.NoError(t, err)
	require.Positive(t, renewRO.Msg.GetLeaseTtlSeconds())

	baseEpoch := branchWriterEpoch(t, ctx, client, diskID, testutil.DefaultBranch)
	mount1, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, baseEpoch+1, epoch1)

	renewRW, err := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   testutil.DefaultBranch,
		MountId:  mount1.Msg.GetMountId(),
		ReadOnly: false,
		Epoch:    epoch1,
	}))
	require.NoError(t, err)
	require.Positive(t, renewRW.Msg.GetLeaseTtlSeconds())

	_, conflictMountErr := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	testutil.RequireConnectError(t, conflictMountErr, connect.CodeAborted, "EBUSY")

	expanded, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskID,
		Name:     new("e2e-live-expanded"),
		MaxBytes: new(uint64(16 << 20)),
	}))
	require.NoError(t, err)
	require.Equal(t, "e2e-live-expanded", expanded.Msg.GetDisk().GetName())
	require.Equal(t, uint64(16<<20), expanded.Msg.GetDisk().GetMaxBytes())

	statfs, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(16<<20), statfs.Msg.GetTotalBytes())

	h2cClient := testutil.NewH2CStorageClient(testutil.GetStorageServiceAddr())
	bigFile, err := h2cClient.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("h2c_8m.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	bigIno := bigFile.Msg.GetInode().GetIno()

	const chunkBytes = 4 * 1024 * 1024
	chunk0 := bytes.Repeat([]byte{0x11, 0xa1, 0x3c, 0xc3}, chunkBytes/4)
	chunk1 := bytes.Repeat([]byte{0x22, 0xb2, 0x5a, 0xa5}, chunkBytes/4)

	var (
		wg           sync.WaitGroup
		wResp0       *connect.Response[storagev1.WriteResponse]
		wResp1       *connect.Response[storagev1.WriteResponse]
		wErr0, wErr1 error
	)
	wg.Go(func() {
		wResp0, wErr0 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Branch: testutil.DefaultBranch,
			Epoch:  epoch1,
			Ino:    bigIno,
			Offset: 0,
			Data:   chunk0,
		}))
	})
	wg.Go(func() {
		wResp1, wErr1 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Branch: testutil.DefaultBranch,
			Epoch:  epoch1,
			Ino:    bigIno,
			Offset: chunkBytes,
			Data:   chunk1,
		}))
	})
	wg.Wait()
	require.NoError(t, wErr0)
	require.NoError(t, wErr1)
	require.Equal(t, uint32(chunkBytes), wResp0.Msg.GetSize())
	require.Equal(t, uint32(chunkBytes), wResp1.Msg.GetSize())

	r0, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    bigIno,
		Offset: 0,
		Size:   chunkBytes,
	}))
	require.NoError(t, err)
	r1, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    bigIno,
		Offset: chunkBytes,
		Size:   chunkBytes,
	}))
	require.NoError(t, err)
	require.Equal(t, chunk0, r0.Msg.GetData())
	require.Equal(t, chunk1, r1.Msg.GetData())

	statAfterWrite, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.GreaterOrEqual(t, statAfterWrite.Msg.GetUsedBytes(), uint64(2*chunkBytes))

	_, shrinkErr := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskID,
		MaxBytes: new(uint64(1 << 20)),
	}))
	testutil.RequireConnectError(t, shrinkErr, connect.CodeFailedPrecondition, "EINVAL")

	mount2, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Force:  true,
	}))
	require.NoError(t, err)
	epoch2 := mount2.Msg.GetEpoch()
	require.Equal(t, epoch1+1, epoch2)

	_, staleRenewErr := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   testutil.DefaultBranch,
		MountId:  mount1.Msg.GetMountId(),
		ReadOnly: false,
		Epoch:    epoch1,
	}))
	testutil.RequireConnectError(t, staleRenewErr, connect.CodeFailedPrecondition, "EBUSY")

	_, fencedErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    bigIno,
		Offset: 0,
		Data:   []byte("stale"),
	}))
	testutil.RequireConnectError(t, fencedErr, connect.CodeFailedPrecondition, "EBUSY")

	_, busyDelErr := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, busyDelErr, connect.CodeAborted, "EBUSY")

	mountsResp, err := client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	require.Len(t, mountsResp.Msg.GetMounts(), 2)
	require.True(t, slices.IsSortedFunc(mountsResp.Msg.GetMounts(), func(a, b *storagev1.MountLease) int {
		switch {
		case a.GetMountId() < b.GetMountId():
			return -1
		case a.GetMountId() > b.GetMountId():
			return 1
		default:
			return 0
		}
	}))

	evictResp, err := client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint32(2), evictResp.Msg.GetEvictedCount())
	require.Len(t, evictResp.Msg.GetUpdatedBranches(), 1)
	require.Equal(t, testutil.DefaultBranch, evictResp.Msg.GetUpdatedBranches()[0].GetName())
	require.Equal(t, epoch2+1, evictResp.Msg.GetUpdatedBranches()[0].GetWriterEpoch())

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	_, getDeletedErr := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, getDeletedErr, connect.CodeNotFound, "ENOENT")
}

func TestStorageLive_OnlineForkBranchIsolationAndOrphanPreservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-live-branch-fork-%d", time.Now().UnixNano()),
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)

	mainMount, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	mainEpoch := mainMount.Msg.GetEpoch()

	sharedCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     mainEpoch,
		ParentIno: 1,
		Name:      []byte("shared.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	sharedIno := sharedCreate.Msg.GetInode().GetIno()
	sharedPayload := bytes.Repeat([]byte("FORK-BASE-DATA-"), 4096)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  mainEpoch,
		Ino:    sharedIno,
		Offset: 0,
		Data:   sharedPayload,
	}))
	require.NoError(t, err)

	orphanCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     mainEpoch,
		ParentIno: 1,
		Name:      []byte("orphan.bin"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	orphanIno := orphanCreate.Msg.GetInode().GetIno()
	orphanPayload := bytes.Repeat([]byte{0xEE}, 64*1024)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  mainEpoch,
		Ino:    orphanIno,
		Offset: 0,
		Data:   orphanPayload,
	}))
	require.NoError(t, err)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       diskID,
		Branch:       testutil.DefaultBranch,
		Epoch:        mainEpoch,
		ParentIno:    1,
		Name:         []byte("orphan.bin"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err)

	forkResp, err := client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: testutil.DefaultBranch,
		NewBranch:    "feat",
		Message:      "online fork with open orphan",
	}))
	require.NoError(t, err)
	require.Equal(t, storagev1.CommitKind_COMMIT_KIND_FORK, forkResp.Msg.GetCommit().GetKind())
	require.Equal(t, "feat", forkResp.Msg.GetBranch().GetName())
	require.Equal(t, forkResp.Msg.GetCommit().GetCommitId(), forkResp.Msg.GetBranch().GetHeadCommitId())

	mainMutated := []byte("MAIN-NEW-PAYLOAD-AFTER-FORK")
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  mainEpoch,
		Ino:    sharedIno,
		Offset: 0,
		Data:   mainMutated,
	}))
	require.NoError(t, err)

	orphanRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    orphanIno,
		Offset: 0,
		Size:   uint32(len(orphanPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, orphanPayload, orphanRead.Msg.GetData())

	_, featOrphanErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Branch: "feat",
		Ino:    orphanIno,
	}))
	testutil.RequireConnectError(t, featOrphanErr, connect.CodeNotFound, "ENOENT")

	featMount, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	featEpoch := featMount.Msg.GetEpoch()

	featLookup, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    "feat",
		ParentIno: 1,
		Name:      []byte("shared.bin"),
	}))
	require.NoError(t, err)
	featRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: "feat",
		Ino:    featLookup.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   uint32(len(sharedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload, featRead.Msg.GetData())

	mainNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     mainEpoch,
		ParentIno: 1,
		Name:      []byte("main-only.txt"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	featNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Branch:    "feat",
		Epoch:     featEpoch,
		ParentIno: 1,
		Name:      []byte("feat-only.txt"),
		Mode:      0o100644,
	}))
	require.NoError(t, err)
	require.NotEqual(t, mainNewFile.Msg.GetInode().GetIno(), featNewFile.Msg.GetInode().GetIno())

	_, featMissingMainErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    "feat",
		ParentIno: 1,
		Name:      []byte("main-only.txt"),
	}))
	testutil.RequireConnectError(t, featMissingMainErr, connect.CodeNotFound, "ENOENT")

	_, mainMissingFeatErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		ParentIno: 1,
		Name:      []byte("feat-only.txt"),
	}))
	testutil.RequireConnectError(t, mainMissingFeatErr, connect.CodeNotFound, "ENOENT")

	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMount.Msg.GetMountId(),
	}))
	require.NoError(t, err)
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: mainMount.Msg.GetMountId(),
	}))
	require.NoError(t, err)
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: featMount.Msg.GetMountId(),
	}))
	require.NoError(t, err)
}

func TestStorageLive_CreateTmpfileLinkPromotionAndOrphanSweepOnRemount(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-live-tmpfile-%d", time.Now().UnixNano()),
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)

	mount1, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
	}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Positive(t, epoch1)

	linkableResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      0o100644,
		Exclusive: false,
	}))
	require.NoError(t, err)
	linkableIno := linkableResp.Msg.GetInode().GetIno()
	require.NotZero(t, linkableIno)
	require.Zero(t, linkableResp.Msg.GetInode().GetAttr().GetNlink())
	require.Equal(t, uint32(0o100644), linkableResp.Msg.GetInode().GetAttr().GetMode())

	linkablePayload := bytes.Repeat([]byte{0xA5}, 8192)
	writeResp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    linkableIno,
		Offset: 0,
		Data:   linkablePayload,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(len(linkablePayload)), writeResp.Msg.GetSize())

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    linkableIno,
		Offset: 0,
		Size:   uint32(len(linkablePayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, linkablePayload, readResp.Msg.GetData())

	linkResp, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Branch:       testutil.DefaultBranch,
		Epoch:        epoch1,
		Ino:          linkableIno,
		NewParentIno: 1,
		NewName:      []byte("promoted.bin"),
	}))
	require.NoError(t, err)
	require.Equal(t, linkableIno, linkResp.Msg.GetInode().GetIno())
	require.Equal(t, uint32(1), linkResp.Msg.GetInode().GetAttr().GetNlink())

	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		ParentIno: 1,
		Name:      []byte("promoted.bin"),
	}))
	require.NoError(t, err)
	require.Equal(t, linkableIno, lookupResp.Msg.GetInode().GetIno())
	require.Equal(t, uint32(1), lookupResp.Msg.GetInode().GetAttr().GetNlink())
	require.Equal(t, uint64(len(linkablePayload)), lookupResp.Msg.GetInode().GetAttr().GetSize())

	exclResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      0o100600,
		Exclusive: true,
	}))
	require.NoError(t, err)
	exclIno := exclResp.Msg.GetInode().GetIno()
	require.NotZero(t, exclIno)
	require.Zero(t, exclResp.Msg.GetInode().GetAttr().GetNlink())

	exclPayload := bytes.Repeat([]byte{0x5B}, 4096)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    exclIno,
		Offset: 0,
		Data:   exclPayload,
	}))
	require.NoError(t, err)

	_, linkExclErr := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Branch:       testutil.DefaultBranch,
		Epoch:        epoch1,
		Ino:          exclIno,
		NewParentIno: 1,
		NewName:      []byte("exclusive.bin"),
	}))
	testutil.RequireConnectError(t, linkExclErr, connect.CodeNotFound, "ENOENT")

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    exclIno,
	}))
	require.NoError(t, err)

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    exclIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	lingeringResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
		Branch:    testutil.DefaultBranch,
		Epoch:     epoch1,
		ParentIno: 1,
		Mode:      0o100600,
		Exclusive: false,
	}))
	require.NoError(t, err)
	lingeringIno := lingeringResp.Msg.GetInode().GetIno()
	require.NotZero(t, lingeringIno)

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Epoch:  epoch1,
		Ino:    lingeringIno,
		Offset: 0,
		Data:   bytes.Repeat([]byte{0x7C}, 2048),
	}))
	require.NoError(t, err)

	mount2, err := client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Force:  true,
	}))
	require.NoError(t, err)
	require.Equal(t, epoch1+1, mount2.Msg.GetEpoch())

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    lingeringIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	promotedRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Branch: testutil.DefaultBranch,
		Ino:    linkableIno,
		Offset: 0,
		Size:   uint32(len(linkablePayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, linkablePayload, promotedRead.Msg.GetData())
}
