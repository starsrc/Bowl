package service_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/disk/internal/db"
	"github.com/starsrc/bowl/services/disk/internal/service"
)

func setupTestServer(t *testing.T) (bowlv1connect.DiskServiceClient, func()) {
	t.Helper()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	store := db.NewStore(pool)
	diskService := service.NewDiskService(store)

	// Build Connect-RPC Server and mount onto HTTP multiplexer
	connectServer := connect.NewServer()
	bowlv1connect.RegisterDiskServiceHandler(connectServer, diskService)

	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)

	testServer := httptest.NewServer(mux)

	transport := connecthttp.NewTransport(testServer.Client(), testServer.URL)
	connectClient := connect.NewClient(transport)
	client := bowlv1connect.NewDiskServiceClient(connectClient)

	cleanup := func() {
		testServer.Close()
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disks CASCADE")
		store.Close()
	}

	return client, cleanup
}

func TestDiskService_CreateDisk_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	req := &bowlv1.CreateDiskRequest{
		Name:          "prod-data-volume",
		CapacityBytes: 10737418240, // 10 GiB
		InodesQuota:   500000,
	}

	resp, err := client.CreateDisk(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Disk)

	disk := resp.Disk
	parsedUUID, err := uuid.Parse(disk.DiskId)
	require.NoError(t, err, "disk_id must be a valid UUID")
	assert.NotEqual(t, uuid.Nil, parsedUUID)

	assert.Equal(t, "prod-data-volume", disk.Name)
	assert.Equal(t, uint64(10737418240), disk.CapacityBytes)
	assert.Equal(t, uint64(0), disk.UsedBytes)
	assert.Equal(t, uint64(500000), disk.InodesQuota)
	assert.Equal(t, uint64(0), disk.InodesUsed)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, disk.Status)
	require.NotNil(t, disk.CreatedAt)
	require.NotNil(t, disk.UpdatedAt)
	assert.WithinDuration(t, time.Now(), disk.CreatedAt.AsTime(), 5*time.Second)
	assert.WithinDuration(t, time.Now(), disk.UpdatedAt.AsTime(), 5*time.Second)
}

func TestDiskService_CreateDisk_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty name returns InvalidArgument", func(t *testing.T) {
		req := &bowlv1.CreateDiskRequest{
			Name:          "   ",
			CapacityBytes: 10737418240,
		}
		resp, err := client.CreateDisk(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("zero capacity returns InvalidArgument", func(t *testing.T) {
		req := &bowlv1.CreateDiskRequest{
			Name:          "zero-capacity-disk",
			CapacityBytes: 0,
		}
		resp, err := client.CreateDisk(ctx, req)
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestDiskService_GetDisk_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create a disk first
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "get-test-volume",
		CapacityBytes: 5368709120, // 5 GiB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	createdDisk := createResp.Disk

	// 2. Query the created disk via GetDisk
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{
		DiskId: createdDisk.DiskId,
	})
	require.NoError(t, err)
	require.NotNil(t, getResp)
	require.NotNil(t, getResp.Disk)

	disk := getResp.Disk
	assert.Equal(t, createdDisk.DiskId, disk.DiskId)
	assert.Equal(t, "get-test-volume", disk.Name)
	assert.Equal(t, uint64(5368709120), disk.CapacityBytes)
	assert.Equal(t, uint64(0), disk.UsedBytes)
	assert.Equal(t, uint64(100000), disk.InodesQuota)
	assert.Equal(t, uint64(0), disk.InodesUsed)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, disk.Status)
	assert.Equal(t, createdDisk.CreatedAt.AsTime().Unix(), disk.CreatedAt.AsTime().Unix())
	assert.Equal(t, createdDisk.UpdatedAt.AsTime().Unix(), disk.UpdatedAt.AsTime().Unix())
}

func TestDiskService_GetDisk_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	randomUUID := uuid.New().String()

	resp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{
		DiskId: randomUUID,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestDiskService_GetDisk_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{
			DiskId: "",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("malformed disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{
			DiskId: "invalid-uuid-format",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestDiskService_AttachDisk_OWMR_Lifecycle(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create a disk (status READY)
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "owmr-disk-lifecycle",
		CapacityBytes: 10737418240,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, createResp.Disk.Status)
	assert.Empty(t, createResp.Disk.WriterClientId)
	assert.Empty(t, createResp.Disk.ReaderClientIds)

	// 2. Attach reader client-1 (read_only = true) -> status transitions to ATTACHED
	attachResp1, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-01",
		ReadOnly: true,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, attachResp1.Disk.Status)
	assert.Empty(t, attachResp1.Disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01"}, attachResp1.Disk.ReaderClientIds)

	// 3. Attach reader client-2 (read_only = true) -> multi-reader allowed
	attachResp2, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-02",
		ReadOnly: true,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, attachResp2.Disk.Status)
	assert.Empty(t, attachResp2.Disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01", "reader-node-02"}, attachResp2.Disk.ReaderClientIds)

	// 4. Attach exclusive writer client-3 (read_only = false) -> succeeds for first writer
	attachResp3, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-03",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, attachResp3.Disk.Status)
	assert.Equal(t, "writer-node-03", attachResp3.Disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01", "reader-node-02"}, attachResp3.Disk.ReaderClientIds)

	// 5. Attach second writer client-4 (read_only = false) -> fails with CodeFailedPrecondition (OWMR mutual exclusion)
	attachResp4, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-04",
		ReadOnly: false,
	})
	require.Error(t, err)
	assert.Nil(t, attachResp4)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// 6. Duplicate attachment from same client (reader-node-01) -> fails with CodeAlreadyExists
	dupResp, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-01",
		ReadOnly: true,
	})
	require.Error(t, err)
	assert.Nil(t, dupResp)
	assert.Equal(t, connect.CodeAlreadyExists, connect.CodeOf(err))

	// 7. Verify GetDisk reflects all current attachments
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, getResp.Disk.Status)
	assert.Equal(t, "writer-node-03", getResp.Disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01", "reader-node-02"}, getResp.Disk.ReaderClientIds)

	// 8. Detach writer-node-03 -> releases exclusive write lease
	detachResp3, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-03",
		Force:    false,
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, detachResp3.Disk.Status) // readers still active
	assert.Empty(t, detachResp3.Disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01", "reader-node-02"}, detachResp3.Disk.ReaderClientIds)

	// 9. Now writer-node-04 can attach as writer!
	attachResp4Retry, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-04",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "writer-node-04", attachResp4Retry.Disk.WriterClientId)

	// 10. Detach remaining clients one by one
	_, err = client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-04",
	})
	require.NoError(t, err)

	_, err = client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-01",
	})
	require.NoError(t, err)

	// Detach last reader -> status transitions back to READY
	detachLast, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-02",
	})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, detachLast.Disk.Status)
	assert.Empty(t, detachLast.Disk.WriterClientId)
	assert.Empty(t, detachLast.Disk.ReaderClientIds)

	// Verify GetDisk after all clients detached
	finalGet, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, finalGet.Disk.Status)
	assert.Empty(t, finalGet.Disk.WriterClientId)
	assert.Empty(t, finalGet.Disk.ReaderClientIds)
}

func TestDiskService_DetachDisk_ForceAndNotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "detach-force-disk",
		CapacityBytes: 1024,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	t.Run("detach unattached client without force returns NotFound", func(t *testing.T) {
		resp, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   diskID,
			ClientId: "unattached-client",
			Force:    false,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("detach unattached client with force succeeds idempotently", func(t *testing.T) {
		resp, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   diskID,
			ClientId: "unattached-client",
			Force:    true,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, resp.Disk.Status)
	})

	t.Run("attach and detach with force succeeds", func(t *testing.T) {
		_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
			DiskId:   diskID,
			ClientId: "client-force-test",
			ReadOnly: false,
		})
		require.NoError(t, err)

		resp, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   diskID,
			ClientId: "client-force-test",
			Force:    true,
		})
		require.NoError(t, err)
		assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, resp.Disk.Status)
		assert.Empty(t, resp.Disk.WriterClientId)
	})
}

func TestDiskService_AttachAndDetach_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	randomUUID := uuid.New().String()

	t.Run("AttachDisk empty disk_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
			DiskId:   "",
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("AttachDisk malformed disk_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
			DiskId:   "not-a-uuid",
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("AttachDisk empty client_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
			DiskId:   randomUUID,
			ClientId: "   ",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("AttachDisk non-existent disk returns NotFound", func(t *testing.T) {
		_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
			DiskId:   randomUUID,
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("DetachDisk empty disk_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   "",
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("DetachDisk malformed disk_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   "not-a-uuid",
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("DetachDisk empty client_id returns InvalidArgument", func(t *testing.T) {
		_, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   randomUUID,
			ClientId: "   ",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("DetachDisk non-existent disk returns NotFound", func(t *testing.T) {
		_, err := client.DetachDisk(ctx, &bowlv1.DetachDiskRequest{
			DiskId:   randomUUID,
			ClientId: "c-1",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})
}

func TestDiskService_ListDisks_PaginationBoundaryConditions(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create 7 disks
	createdIDs := make([]string, 7)
	for i := 0; i < 7; i++ {
		resp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
			Name:          fmt.Sprintf("pagination-disk-%02d", i+1),
			CapacityBytes: 1024 * 1024 * 1024 * uint64(i+1),
			InodesQuota:   10000,
		})
		require.NoError(t, err)
		createdIDs[i] = resp.Disk.DiskId
		time.Sleep(5 * time.Millisecond) // Ensure unique created_at timestamps
	}

	// 1. First page: page_size = 3, page_token = ""
	page1Resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		PageSize:  3,
		PageToken: "",
	})
	require.NoError(t, err)
	require.NotNil(t, page1Resp)
	assert.Equal(t, int64(7), page1Resp.TotalCount)
	require.Len(t, page1Resp.Disks, 3)
	assert.NotEmpty(t, page1Resp.NextPageToken)
	assert.Equal(t, "3", page1Resp.NextPageToken)

	// Items are ordered by created_at DESC -> createdIDs[6], createdIDs[5], createdIDs[4]
	assert.Equal(t, createdIDs[6], page1Resp.Disks[0].DiskId)
	assert.Equal(t, createdIDs[5], page1Resp.Disks[1].DiskId)
	assert.Equal(t, createdIDs[4], page1Resp.Disks[2].DiskId)

	// 2. Second page: page_size = 3, page_token = page1Resp.NextPageToken
	page2Resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		PageSize:  3,
		PageToken: page1Resp.NextPageToken,
	})
	require.NoError(t, err)
	require.NotNil(t, page2Resp)
	assert.Equal(t, int64(7), page2Resp.TotalCount)
	require.Len(t, page2Resp.Disks, 3)
	assert.NotEmpty(t, page2Resp.NextPageToken)
	assert.Equal(t, "6", page2Resp.NextPageToken)
	assert.Equal(t, createdIDs[3], page2Resp.Disks[0].DiskId)
	assert.Equal(t, createdIDs[2], page2Resp.Disks[1].DiskId)
	assert.Equal(t, createdIDs[1], page2Resp.Disks[2].DiskId)

	// 3. Third page: page_size = 3, page_token = page2Resp.NextPageToken
	page3Resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		PageSize:  3,
		PageToken: page2Resp.NextPageToken,
	})
	require.NoError(t, err)
	require.NotNil(t, page3Resp)
	assert.Equal(t, int64(7), page3Resp.TotalCount)
	require.Len(t, page3Resp.Disks, 1)
	assert.Empty(t, page3Resp.NextPageToken, "last page must have empty next_page_token")
	assert.Equal(t, createdIDs[0], page3Resp.Disks[0].DiskId)

	// 4. Empty page: offset beyond total_count
	emptyResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		PageSize:  3,
		PageToken: "10",
	})
	require.NoError(t, err)
	require.NotNil(t, emptyResp)
	assert.Equal(t, int64(7), emptyResp.TotalCount)
	assert.Empty(t, emptyResp.Disks)
	assert.Empty(t, emptyResp.NextPageToken)

	// 5. Query with base64 encoded offset token
	b64Token := base64.StdEncoding.EncodeToString([]byte("3"))
	b64Resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		PageSize:  3,
		PageToken: b64Token,
	})
	require.NoError(t, err)
	require.NotNil(t, b64Resp)
	assert.Equal(t, int64(7), b64Resp.TotalCount)
	require.Len(t, b64Resp.Disks, 3)
	assert.Equal(t, createdIDs[3], b64Resp.Disks[0].DiskId)
	assert.Equal(t, createdIDs[2], b64Resp.Disks[1].DiskId)
	assert.Equal(t, createdIDs[1], b64Resp.Disks[2].DiskId)
}

func TestDiskService_ListDisks_StatusFiltering(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create 5 disks
	disks := make([]*bowlv1.Disk, 5)
	for i := 0; i < 5; i++ {
		resp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
			Name:          fmt.Sprintf("filter-disk-%02d", i+1),
			CapacityBytes: 1024 * 1024,
		})
		require.NoError(t, err)
		disks[i] = resp.Disk
	}

	// Attach 2 disks to make their status ATTACHED
	_, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   disks[1].DiskId,
		ClientId: "reader-node-a",
		ReadOnly: true,
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   disks[3].DiskId,
		ClientId: "writer-node-b",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// Disks status:
	// disks[0]: READY
	// disks[1]: ATTACHED
	// disks[2]: READY
	// disks[3]: ATTACHED
	// disks[4]: READY

	// 1. Query with DISK_STATUS_UNSPECIFIED (returns all 5)
	allResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		FilterStatus: bowlv1.DiskStatus_DISK_STATUS_UNSPECIFIED,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(5), allResp.TotalCount)
	assert.Len(t, allResp.Disks, 5)

	// 2. Query with DISK_STATUS_READY (returns 3)
	readyResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		FilterStatus: bowlv1.DiskStatus_DISK_STATUS_READY,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(3), readyResp.TotalCount)
	require.Len(t, readyResp.Disks, 3)
	for _, d := range readyResp.Disks {
		assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_READY, d.Status)
	}

	// 3. Query with DISK_STATUS_ATTACHED (returns 2)
	attachedResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		FilterStatus: bowlv1.DiskStatus_DISK_STATUS_ATTACHED,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), attachedResp.TotalCount)
	require.Len(t, attachedResp.Disks, 2)
	for _, d := range attachedResp.Disks {
		assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, d.Status)
	}

	// 4. Query with DISK_STATUS_ERROR (returns 0)
	errResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
		FilterStatus: bowlv1.DiskStatus_DISK_STATUS_ERROR,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), errResp.TotalCount)
	assert.Empty(t, errResp.Disks)
	assert.Empty(t, errResp.NextPageToken)
}

func TestDiskService_ListDisks_AttachmentFieldsPopulated(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "attachment-list-disk",
		CapacityBytes: 2048,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// Attach 2 readers and 1 writer
	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-01",
		ReadOnly: true,
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-node-02",
		ReadOnly: true,
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-node-01",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// List disks and verify attachment fields
	listResp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{})
	require.NoError(t, err)
	require.Len(t, listResp.Disks, 1)

	disk := listResp.Disks[0]
	assert.Equal(t, diskID, disk.DiskId)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, disk.Status)
	assert.Equal(t, "writer-node-01", disk.WriterClientId)
	assert.Equal(t, []string{"reader-node-01", "reader-node-02"}, disk.ReaderClientIds)
}

func TestDiskService_ListDisks_ValidationAndDefaults(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create 25 disks to test default page size of 20
	for i := 0; i < 25; i++ {
		_, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
			Name:          fmt.Sprintf("bulk-disk-%02d", i+1),
			CapacityBytes: 1024,
		})
		require.NoError(t, err)
	}

	t.Run("page_size <= 0 defaults to 20", func(t *testing.T) {
		resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageSize: 0,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(25), resp.TotalCount)
		assert.Len(t, resp.Disks, 20)
		assert.Equal(t, "20", resp.NextPageToken)
	})

	t.Run("page_size negative defaults to 20", func(t *testing.T) {
		resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageSize: -5,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(25), resp.TotalCount)
		assert.Len(t, resp.Disks, 20)
		assert.Equal(t, "20", resp.NextPageToken)
	})

	t.Run("page_size > 100 caps at 100", func(t *testing.T) {
		resp, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageSize: 200,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(25), resp.TotalCount)
		assert.Len(t, resp.Disks, 25) // All 25 returned since 25 <= 100
		assert.Empty(t, resp.NextPageToken)
	})

	t.Run("malformed page_token returns InvalidArgument", func(t *testing.T) {
		_, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageToken: "invalid-token",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("negative page_token returns InvalidArgument", func(t *testing.T) {
		_, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageToken: "-5",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("overflowing page_token returns InvalidArgument", func(t *testing.T) {
		_, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			PageToken: "999999999999999999999999999999",
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("invalid filter_status returns InvalidArgument", func(t *testing.T) {
		_, err := client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			FilterStatus: bowlv1.DiskStatus(-1),
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

		_, err = client.ListDisks(ctx, &bowlv1.ListDisksRequest{
			FilterStatus: bowlv1.DiskStatus(999),
		})
		require.Error(t, err)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}


