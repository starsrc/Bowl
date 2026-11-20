//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
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
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})
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

func TestStorageLive_DiskLifecycleResizeH2CAndEpochFence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-live-lifecycle",
		MaxBytes: 4 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)

	got, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.Msg.GetDisk().GetRootIno())
	require.Equal(t, uint64(4<<20), got.Msg.GetDisk().GetMaxBytes())

	page, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 50}))
	require.NoError(t, err)
	require.True(t, slices.ContainsFunc(page.Msg.GetDisks(), func(d *storagev1.Disk) bool {
		return d.GetDiskId() == diskID
	}), "磁盘列表必须包含新创建的磁盘 %s", diskID)

	mountRO, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, mountRO.Msg.GetEpoch())

	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	expanded, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskID,
		Name:     new("e2e-live-expanded"),
		MaxBytes: new(uint64(16 << 20)),
	}))
	require.NoError(t, err)
	require.Equal(t, "e2e-live-expanded", expanded.Msg.GetDisk().GetName())
	require.Equal(t, uint64(16<<20), expanded.Msg.GetDisk().GetMaxBytes())

	statfs, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{DiskId: diskID, Ino: 1}))
	require.NoError(t, err)
	require.Equal(t, uint64(16<<20), statfs.Msg.GetTotalBytes())

	h2cClient := testutil.NewH2CStorageClient(testutil.GetStorageServiceAddr())
	bigFile, err := h2cClient.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("h2c_8m.bin"),
		Mode:      0o644,
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
			DiskId: diskID, Epoch: epoch1, Ino: bigIno, Offset: 0, Data: chunk0,
		}))
	})
	wg.Go(func() {
		wResp1, wErr1 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID, Epoch: epoch1, Ino: bigIno, Offset: chunkBytes, Data: chunk1,
		}))
	})
	wg.Wait()
	require.NoError(t, wErr0)
	require.NoError(t, wErr1)
	require.Equal(t, uint32(chunkBytes), wResp0.Msg.GetSize())
	require.Equal(t, uint32(chunkBytes), wResp1.Msg.GetSize())

	r0, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: 0, Size: chunkBytes}))
	require.NoError(t, err)
	r1, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: chunkBytes, Size: chunkBytes}))
	require.NoError(t, err)
	require.Equal(t, chunk0, r0.Msg.GetData())
	require.Equal(t, chunk1, r1.Msg.GetData())

	mount2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, Force: true}))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mount2.Msg.GetEpoch())

	_, fencedErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID, Epoch: epoch1, Ino: bigIno, Offset: 0, Data: []byte("stale"),
	}))
	testutil.RequireConnectError(t, fencedErr, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	_, getDeletedErr := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, getDeletedErr, connect.CodeNotFound, "ENOENT")
}

func TestStorageLive_OnlineForkIsolationAndLineageDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	createdParent, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-live-fork-parent",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	parentID := createdParent.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, parentID)

	parentMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: parentID}))
	require.NoError(t, err)
	parentEpoch := parentMount.Msg.GetEpoch()

	sharedCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: parentID, Epoch: parentEpoch, ParentIno: 1, Name: []byte("shared.bin"), Mode: 0o644,
	}))
	require.NoError(t, err)
	sharedIno := sharedCreate.Msg.GetInode().GetIno()
	sharedPayload := bytes.Repeat([]byte("FORK-BASE-DATA-"), 20480)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID, Epoch: parentEpoch, Ino: sharedIno, Offset: 0, Data: sharedPayload,
	}))
	require.NoError(t, err)

	orphanCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: parentID, Epoch: parentEpoch, ParentIno: 1, Name: []byte("orphan.bin"), Mode: 0o644,
	}))
	require.NoError(t, err)
	orphanIno := orphanCreate.Msg.GetInode().GetIno()
	orphanPayload := bytes.Repeat([]byte{0xEE}, 256*1024)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID, Epoch: parentEpoch, Ino: orphanIno, Offset: 0, Data: orphanPayload,
	}))
	require.NoError(t, err)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId: parentID, Epoch: parentEpoch, ParentIno: 1, Name: []byte("orphan.bin"), OpenInClient: new(true),
	}))
	require.NoError(t, err)

	child1Resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: parentID,
		Name:         "e2e-live-fork-child1",
		MaxBytes:     new(uint64(512 * 1024)),
	}))
	require.NoError(t, err)
	child1 := child1Resp.Msg.GetDisk()
	child1ID := child1.GetDiskId()
	registerDiskCleanup(t, client, child1ID)
	require.Equal(t, parentID, child1.GetParentDiskId())
	require.Equal(t, uint64(len(sharedPayload)), child1.GetUsedBytes())

	parentMutated := []byte("PARENT-NEW-PAYLOAD-AFTER-FORK")
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID, Epoch: parentEpoch, Ino: sharedIno, Offset: 0, Data: parentMutated,
	}))
	require.NoError(t, err)
	orphanRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: parentID, Ino: orphanIno, Offset: 0, Size: uint32(len(orphanPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, orphanPayload, orphanRead.Msg.GetData())

	child1Mount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: child1ID}))
	require.NoError(t, err)
	child1Epoch := child1Mount.Msg.GetEpoch()
	child1Lookup, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId: child1ID, ParentIno: 1, Name: []byte("shared.bin"),
	}))
	require.NoError(t, err)
	child1Read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: child1ID, Ino: child1Lookup.Msg.GetInode().GetIno(), Offset: 0, Size: uint32(len(sharedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload, child1Read.Msg.GetData())

	parentNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: parentID, Epoch: parentEpoch, ParentIno: 1, Name: []byte("p-new.txt"), Mode: 0o644,
	}))
	require.NoError(t, err)
	childNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: child1ID, Epoch: child1Epoch, ParentIno: 1, Name: []byte("c-new.txt"), Mode: 0o644,
	}))
	require.NoError(t, err)
	require.NotEqual(t, parentNewFile.Msg.GetInode().GetIno(), childNewFile.Msg.GetInode().GetIno())

	grandResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: child1ID,
		Name:         "e2e-live-fork-grandchild",
	}))
	require.NoError(t, err)
	grandID := grandResp.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, grandID)
	require.Equal(t, child1ID, grandResp.Msg.GetDisk().GetParentDiskId())

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: parentID}))
	require.NoError(t, err)
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: grandID}))
	require.NoError(t, err)

	child1AfterGC, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: child1ID, Ino: child1Lookup.Msg.GetInode().GetIno(), Offset: 0, Size: uint32(len(sharedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload, child1AfterGC.Msg.GetData())

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: child1ID}))
	require.NoError(t, err)
}

func TestStorageLive_CreateTmpfileLinkPromotionAndOrphanReclaim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-live-tmpfile",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	registerDiskCleanup(t, client, diskID)

	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	linkableResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
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
		Epoch:  epoch1,
		Ino:    linkableIno,
		Offset: 0,
		Data:   linkablePayload,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(len(linkablePayload)), writeResp.Msg.GetSize())

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    linkableIno,
		Offset: 0,
		Size:   uint32(len(linkablePayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, linkablePayload, readResp.Msg.GetData())

	linkResp, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
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
		ParentIno: 1,
		Name:      []byte("promoted.bin"),
	}))
	require.NoError(t, err)
	require.Equal(t, linkableIno, lookupResp.Msg.GetInode().GetIno())
	require.Equal(t, uint32(1), lookupResp.Msg.GetInode().GetAttr().GetNlink())
	require.Equal(t, uint64(len(linkablePayload)), lookupResp.Msg.GetInode().GetAttr().GetSize())

	statPromoted, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(linkablePayload)), statPromoted.Msg.GetUsedBytes())
	require.Equal(t, uint64(2), statPromoted.Msg.GetFiles())

	exclResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
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
		Epoch:  epoch1,
		Ino:    exclIno,
		Offset: 0,
		Data:   exclPayload,
	}))
	require.NoError(t, err)

	statWithExcl, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(linkablePayload)+len(exclPayload)), statWithExcl.Msg.GetUsedBytes())
	require.Equal(t, uint64(3), statWithExcl.Msg.GetFiles())

	_, linkExclErr := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		Ino:          exclIno,
		NewParentIno: 1,
		NewName:      []byte("exclusive.bin"),
	}))
	testutil.RequireConnectError(t, linkExclErr, connect.CodeNotFound, "ENOENT")

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    exclIno,
	}))
	require.NoError(t, err)

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    exclIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	statReclaimed, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(linkablePayload)), statReclaimed.Msg.GetUsedBytes())
	require.Equal(t, uint64(2), statReclaimed.Msg.GetFiles())

	lingeringResp, err := client.CreateTmpfile(ctx, connect.NewRequest(&storagev1.CreateTmpfileRequest{
		DiskId:    diskID,
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
		Epoch:  epoch1,
		Ino:    lingeringIno,
		Offset: 0,
		Data:   bytes.Repeat([]byte{0x7C}, 2048),
	}))
	require.NoError(t, err)

	mount2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, Force: true}))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mount2.Msg.GetEpoch())

	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    lingeringIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	statFinal, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskID,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(linkablePayload)), statFinal.Msg.GetUsedBytes())
	require.Equal(t, uint64(2), statFinal.Msg.GetFiles())

	promotedRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    linkableIno,
		Offset: 0,
		Size:   uint32(len(linkablePayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, linkablePayload, promotedRead.Msg.GetData())
}
