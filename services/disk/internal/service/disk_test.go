package service_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"sync"
	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/disk/internal/config"
	"github.com/starsrc/bowl/services/disk/internal/db"
	"github.com/starsrc/bowl/services/disk/internal/service"
	"github.com/starsrc/bowl/services/disk/internal/storage"
	"github.com/starsrc/bowl/services/disk/internal/worker"
)

func setupTestServerWithDataDir(t *testing.T, dataDir string) (bowlv1connect.DiskServiceClient, *pgxpool.Pool, func()) {
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
	opts := []service.Option{}
	if dataDir != "" {
		opts = append(opts, service.WithDataDir(dataDir))
	}

	// 为测试配置清理驱动与工作器
	purger, err := storage.NewMinIOObjectStoragePurger(config.S3Config{}, config.StorageConfig{DataDir: dataDir})
	require.NoError(t, err)

	cleanupWorker := worker.NewDiskCleanupWorker(store, purger, worker.WithPollInterval(50*time.Millisecond))
	cleanupWorker.Start(ctx)
	opts = append(opts, service.WithCleanupWorker(cleanupWorker))

	diskService := service.NewDiskService(store, opts...)

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
		cleanupWorker.Stop()
		testServer.Close()
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disk_cleanup_tasks, disks CASCADE")
		store.Close()
	}

	return client, pool, cleanup
}

func setupTestServer(t *testing.T) (bowlv1connect.DiskServiceClient, func()) {
	client, _, cleanup := setupTestServerWithDataDir(t, "")
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

func TestDiskService_UpdateDisk_Success_WithoutFieldMask(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create a disk
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "initial-volume",
		CapacityBytes: 10737418240, // 10 GiB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// 2. Attach a reader and a writer to verify attachments are populated in UpdateDisk response
	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-01",
		ReadOnly: true,
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-01",
		ReadOnly: false,
	})
	require.NoError(t, err)

	// Small pause so updated_at differs
	time.Sleep(10 * time.Millisecond)

	// 3. Update without field mask
	updateResp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
		DiskId:        diskID,
		Name:          "renamed-volume",
		CapacityBytes: 21474836480, // 20 GiB
		InodesQuota:   200000,
	})
	require.NoError(t, err)
	require.NotNil(t, updateResp)
	require.NotNil(t, updateResp.Disk)

	updatedDisk := updateResp.Disk
	assert.Equal(t, diskID, updatedDisk.DiskId)
	assert.Equal(t, "renamed-volume", updatedDisk.Name)
	assert.Equal(t, uint64(21474836480), updatedDisk.CapacityBytes)
	assert.Equal(t, uint64(200000), updatedDisk.InodesQuota)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, updatedDisk.Status)
	assert.Equal(t, "writer-01", updatedDisk.WriterClientId)
	assert.Equal(t, []string{"reader-01"}, updatedDisk.ReaderClientIds)
	assert.True(t, updatedDisk.UpdatedAt.AsTime().After(createResp.Disk.UpdatedAt.AsTime()))

	// 4. Verify GetDisk returns the updated state
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	assert.Equal(t, "renamed-volume", getResp.Disk.Name)
	assert.Equal(t, uint64(21474836480), getResp.Disk.CapacityBytes)
	assert.Equal(t, uint64(200000), getResp.Disk.InodesQuota)
}

func TestDiskService_UpdateDisk_Success_WithFieldMask(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create initial disk
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "original-name",
		CapacityBytes: 10737418240, // 10 GiB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	t.Run("update only name via field mask", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			Name:          "only-name-changed",
			CapacityBytes: 0, // Ignored because not in mask
			InodesQuota:   0, // Ignored because not in mask
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "only-name-changed", resp.Disk.Name)
		assert.Equal(t, uint64(10737418240), resp.Disk.CapacityBytes, "capacity should be preserved")
		assert.Equal(t, uint64(100000), resp.Disk.InodesQuota, "inodes quota should be preserved")
	})

	t.Run("update only capacity_bytes via field mask", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			Name:          "",          // Ignored because not in mask
			CapacityBytes: 32212254720, // 30 GiB
			InodesQuota:   0,           // Ignored because not in mask
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"capacity_bytes"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "only-name-changed", resp.Disk.Name, "name should be preserved")
		assert.Equal(t, uint64(32212254720), resp.Disk.CapacityBytes)
		assert.Equal(t, uint64(100000), resp.Disk.InodesQuota, "inodes quota should be preserved")
	})

	t.Run("update only inodes_quota via field mask", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			Name:          "", // Ignored because not in mask
			CapacityBytes: 0,  // Ignored because not in mask
			InodesQuota:   500000,
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"inodes_quota"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "only-name-changed", resp.Disk.Name, "name should be preserved")
		assert.Equal(t, uint64(32212254720), resp.Disk.CapacityBytes, "capacity should be preserved")
		assert.Equal(t, uint64(500000), resp.Disk.InodesQuota)
	})

	t.Run("update multiple fields via field mask", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			Name:          "multi-updated-name",
			CapacityBytes: 42949672960, // 40 GiB
			InodesQuota:   0,           // Ignored because not in mask
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"name", "capacity_bytes"}},
		})
		require.NoError(t, err)
		assert.Equal(t, "multi-updated-name", resp.Disk.Name)
		assert.Equal(t, uint64(42949672960), resp.Disk.CapacityBytes)
		assert.Equal(t, uint64(500000), resp.Disk.InodesQuota, "inodes quota should be preserved")
	})
}

func TestDiskService_UpdateDisk_DownsizingProtection(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "downsize-test-disk",
		CapacityBytes: 10737418240, // 10 GiB
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// Simulate existing used_bytes = 5 GiB directly in DB
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	const usedBytes = int64(5368709120) // 5 GiB
	_, err = pool.Exec(ctx, "UPDATE disks SET used_bytes = $1 WHERE disk_id = $2", usedBytes, diskID)
	require.NoError(t, err)

	t.Run("downsizing below used_bytes returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			Name:          "downsize-test-disk",
			CapacityBytes: 4294967296, // 4 GiB (< 5 GiB used)
			InodesQuota:   100000,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("downsizing below used_bytes with field_mask returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			CapacityBytes: 2147483648, // 2 GiB (< 5 GiB used)
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"capacity_bytes"}},
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("downsizing to exactly used_bytes succeeds", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			CapacityBytes: uint64(usedBytes), // 5 GiB == 5 GiB used
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"capacity_bytes"}},
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(usedBytes), resp.Disk.CapacityBytes)
	})

	t.Run("upsizing above used_bytes succeeds", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        diskID,
			CapacityBytes: 21474836480, // 20 GiB
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"capacity_bytes"}},
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(21474836480), resp.Disk.CapacityBytes)
	})
}

func TestDiskService_UpdateDisk_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "validation-test-disk",
		CapacityBytes: 10737418240,
		InodesQuota:   100000,
	})
	require.NoError(t, err)
	validDiskID := createResp.Disk.DiskId

	t.Run("empty disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        "",
			Name:          "renamed",
			CapacityBytes: 10737418240,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("malformed disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        "not-a-uuid",
			Name:          "renamed",
			CapacityBytes: 10737418240,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("empty name without field mask returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        validDiskID,
			Name:          "   ",
			CapacityBytes: 10737418240,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("empty name with field mask returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:    validDiskID,
			Name:      "",
			FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("zero capacity without field mask returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        validDiskID,
			Name:          "renamed",
			CapacityBytes: 0,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("zero capacity with field mask returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:        validDiskID,
			CapacityBytes: 0,
			FieldMask:     &fieldmaskpb.FieldMask{Paths: []string{"capacity_bytes"}},
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("unsupported field mask path returns InvalidArgument", func(t *testing.T) {
		resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
			DiskId:    validDiskID,
			Name:      "renamed",
			FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"unsupported_path"}},
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestDiskService_UpdateDisk_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	randomUUID := uuid.New().String()

	resp, err := client.UpdateDisk(ctx, &bowlv1.UpdateDiskRequest{
		DiskId:        randomUUID,
		Name:          "non-existent-disk",
		CapacityBytes: 10737418240,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestDiskService_GetDiskStatfs_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create a disk with 10 MiB capacity and 1000 inodes quota
	const capacityBytes = uint64(10485760) // 10 MiB (2560 blocks of 4096)
	const inodesQuota = uint64(1000)

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "statfs-test-disk",
		CapacityBytes: capacityBytes,
		InodesQuota:   inodesQuota,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// 2. Simulate used_bytes = 2 MiB (512 blocks) and inodes_used = 150 directly in DB
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	const usedBytes = int64(2097152) // 2 MiB
	const inodesUsed = int64(150)
	_, err = pool.Exec(ctx, "UPDATE disks SET used_bytes = $1, inodes_used = $2 WHERE disk_id = $3", usedBytes, inodesUsed, diskID)
	require.NoError(t, err)

	// 3. Query GetDiskStatfs
	statfsResp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{
		DiskId: diskID,
	})
	require.NoError(t, err)
	require.NotNil(t, statfsResp)
	assert.Equal(t, diskID, statfsResp.DiskId)
	require.NotNil(t, statfsResp.Statfs)

	statfs := statfsResp.Statfs
	assert.Equal(t, uint32(4096), statfs.BlockSize)
	assert.Equal(t, uint32(4096), statfs.FragmentSize)
	assert.Equal(t, uint64(2560), statfs.Blocks)
	assert.Equal(t, uint64(2048), statfs.BlocksFree) // (10485760 - 2097152) / 4096 = 2048
	assert.Equal(t, uint64(2048), statfs.BlocksAvail)
	assert.Equal(t, inodesQuota, statfs.Files)
	assert.Equal(t, uint64(850), statfs.FilesFree) // 1000 - 150 = 850
	assert.Equal(t, uint32(255), statfs.MaxNameLen)
}

func TestDiskService_GetDiskStatfs_CapacityEqualsUsed(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	const capacityBytes = uint64(40960) // 10 blocks
	const inodesQuota = uint64(50)

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "full-capacity-disk",
		CapacityBytes: capacityBytes,
		InodesQuota:   inodesQuota,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	t.Run("capacity equals used_bytes yields blocks_free = 0 and files_free = 0", func(t *testing.T) {
		_, err = pool.Exec(ctx, "UPDATE disks SET used_bytes = $1, inodes_used = $2 WHERE disk_id = $3", int64(capacityBytes), int64(inodesQuota), diskID)
		require.NoError(t, err)

		resp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{DiskId: diskID})
		require.NoError(t, err)
		require.NotNil(t, resp.Statfs)
		assert.Equal(t, uint64(10), resp.Statfs.Blocks)
		assert.Equal(t, uint64(0), resp.Statfs.BlocksFree)
		assert.Equal(t, uint64(0), resp.Statfs.BlocksAvail)
		assert.Equal(t, uint64(50), resp.Statfs.Files)
		assert.Equal(t, uint64(0), resp.Statfs.FilesFree)
	})

	t.Run("used_bytes exceeds capacity yields blocks_free = 0 without underflow", func(t *testing.T) {
		_, err = pool.Exec(ctx, "UPDATE disks SET used_bytes = $1, inodes_used = $2 WHERE disk_id = $3", int64(capacityBytes+8192), int64(inodesQuota+10), diskID)
		require.NoError(t, err)

		resp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{DiskId: diskID})
		require.NoError(t, err)
		require.NotNil(t, resp.Statfs)
		assert.Equal(t, uint64(0), resp.Statfs.BlocksFree)
		assert.Equal(t, uint64(0), resp.Statfs.BlocksAvail)
		assert.Equal(t, uint64(0), resp.Statfs.FilesFree)
	})
}

func TestDiskService_GetDiskStatfs_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	randomUUID := uuid.New().String()

	resp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{
		DiskId: randomUUID,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestDiskService_GetDiskStatfs_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{
			DiskId: "   ",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("malformed disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.GetDiskStatfs(ctx, &bowlv1.GetDiskStatfsRequest{
			DiskId: "invalid-uuid-format",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

func TestDiskService_GetDisk_IncludesStatfs(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Create a disk
	const capacityBytes = uint64(819200) // 200 blocks of 4096
	const inodesQuota = uint64(200)

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "includes-statfs-volume",
		CapacityBytes: capacityBytes,
		InodesQuota:   inodesQuota,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// CreateDisk response should include Statfs
	require.NotNil(t, createResp.Disk.Statfs)
	assert.Equal(t, uint32(4096), createResp.Disk.Statfs.BlockSize)
	assert.Equal(t, uint64(200), createResp.Disk.Statfs.Blocks)
	assert.Equal(t, uint64(200), createResp.Disk.Statfs.BlocksFree)
	assert.Equal(t, uint64(200), createResp.Disk.Statfs.Files)
	assert.Equal(t, uint64(200), createResp.Disk.Statfs.FilesFree)

	// 2. Query GetDisk
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	require.NotNil(t, getResp.Disk)
	require.NotNil(t, getResp.Disk.Statfs)

	statfs := getResp.Disk.Statfs
	assert.Equal(t, uint32(4096), statfs.BlockSize)
	assert.Equal(t, uint32(4096), statfs.FragmentSize)
	assert.Equal(t, uint64(200), statfs.Blocks)
	assert.Equal(t, uint64(200), statfs.BlocksFree)
	assert.Equal(t, uint64(200), statfs.BlocksAvail)
	assert.Equal(t, inodesQuota, statfs.Files)
	assert.Equal(t, inodesQuota, statfs.FilesFree)
	assert.Equal(t, uint32(255), statfs.MaxNameLen)
}

func TestDiskService_DeleteDisk_NormalDeletion_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "delete-normal-volume",
		CapacityBytes: 1073741824,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// Normal deletion without attachments (force = false)
	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskID,
		Force:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	// Disk should no longer exist
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.Error(t, err)
	assert.Nil(t, getResp)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestDiskService_DeleteDisk_ActiveAttachments_Rejection(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "delete-active-attachment-volume",
		CapacityBytes: 1073741824,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// Attach client to disk
	attachResp, err := client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "worker-node-1",
		ReadOnly: false,
	})
	require.NoError(t, err)
	require.NotNil(t, attachResp)

	// Normal deletion with active attachments (force = false) must fail
	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskID,
		Force:  false,
	})
	require.Error(t, err)
	assert.Nil(t, deleteResp)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.Contains(t, err.Error(), "cannot delete disk with active attachments without force=true")

	// Verify disk still exists and is attached
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.NoError(t, err)
	assert.Equal(t, bowlv1.DiskStatus_DISK_STATUS_ATTACHED, getResp.Disk.Status)
}

func TestDiskService_DeleteDisk_ForceCascadeDeletion_Success(t *testing.T) {
	dataDir := t.TempDir()
	client, pool, cleanup := setupTestServerWithDataDir(t, dataDir)
	defer cleanup()

	ctx := context.Background()
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "force-cascade-volume",
		CapacityBytes: 1073741824,
	})
	require.NoError(t, err)
	diskID := createResp.Disk.DiskId

	// Attach writer and reader
	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "writer-client-1",
		ReadOnly: false,
	})
	require.NoError(t, err)

	_, err = client.AttachDisk(ctx, &bowlv1.AttachDiskRequest{
		DiskId:   diskID,
		ClientId: "reader-client-1",
		ReadOnly: true,
	})
	require.NoError(t, err)

	// Create PostgreSQL schema per ADR 0006 & ADR 0010
	parsedUUID := uuid.MustParse(diskID)
	schemaName := fmt.Sprintf("disk_%s", strings.ReplaceAll(parsedUUID.String(), "-", "_"))
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, schemaName))
	require.NoError(t, err)

	// Create dummy table inside schema to ensure CASCADE drops dependent objects
	_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE "%s"."chunks" (id text)`, schemaName))
	require.NoError(t, err)

	// Create local chunk storage directory
	diskChunkDir := filepath.Join(dataDir, diskID)
	err = os.MkdirAll(diskChunkDir, 0755)
	require.NoError(t, err)
	err = os.WriteFile(filepath.Join(diskChunkDir, "chunk_001.bin"), []byte("sample chunk data"), 0644)
	require.NoError(t, err)

	// Force deletion (force = true)
	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskID,
		Force:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	// 1. Verify disk record deleted
	getResp, err := client.GetDisk(ctx, &bowlv1.GetDiskRequest{DiskId: diskID})
	require.Error(t, err)
	assert.Nil(t, getResp)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. Verify disk attachments deleted
	var attCount int64
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM disk_attachments WHERE disk_id = $1", parsedUUID).Scan(&attCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), attCount)

	// 3. Verify PostgreSQL schema dropped
	var schemaExists bool
	err = pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)", schemaName).Scan(&schemaExists)
	require.NoError(t, err)
	assert.False(t, schemaExists, "PostgreSQL schema should have been dropped with CASCADE")

	// 4. Verify cleanup task created and transitioned to COMPLETED
	require.Eventually(t, func() bool {
		var status int16
		err := pool.QueryRow(ctx, "SELECT status FROM disk_cleanup_tasks WHERE disk_id = $1", parsedUUID).Scan(&status)
		return err == nil && status == db.CleanupTaskStatusCompleted
	}, 5*time.Second, 20*time.Millisecond, "清理任务应成功流转至 COMPLETED")

	// 5. Verify local chunk directory deleted by background worker
	require.Eventually(t, func() bool {
		_, err := os.Stat(diskChunkDir)
		return os.IsNotExist(err)
	}, 5*time.Second, 20*time.Millisecond, "本地切片目录应被后台清理工作器安全移除")
}

// mockCleanupWorker 用于测试验证 DeleteDisk 的 Notify 调用与通知参数。
type mockCleanupWorker struct {
	mu          sync.Mutex
	notifiedIDs []uuid.UUID
}

func (m *mockCleanupWorker) Notify(diskID uuid.UUID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.notifiedIDs = append(m.notifiedIDs, diskID)
}

func (m *mockCleanupWorker) NotifiedIDs() []uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]uuid.UUID, len(m.notifiedIDs))
	copy(copied, m.notifiedIDs)
	return copied
}

func TestDiskService_DeleteDisk_CreatesPendingTaskAndNotifiesWorker(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	store := db.NewStore(pool)
	defer store.Close()

	mockWorker := &mockCleanupWorker{}
	diskService := service.NewDiskService(store, service.WithCleanupWorker(mockWorker))

	connectServer := connect.NewServer()
	bowlv1connect.RegisterDiskServiceHandler(connectServer, diskService)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()
	defer func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disk_cleanup_tasks, disks CASCADE")
	}()

	client := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(testServer.Client(), testServer.URL)))

	// 1. 创建虚拟磁盘
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "pending-task-test-disk",
		CapacityBytes: 1048576,
	})
	require.NoError(t, err)
	diskIDStr := createResp.Disk.DiskId
	parsedUUID := uuid.MustParse(diskIDStr)

	// 2. 执行删除
	start := time.Now()
	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskIDStr,
		Force:  true,
	})
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.NotNil(t, deleteResp)
	assert.Less(t, elapsed, 500*time.Millisecond, "前台 DeleteDisk 响应耗时应远小于 500ms")

	// 3. 校验 mockWorker 收到即时通知
	notified := mockWorker.NotifiedIDs()
	require.Len(t, notified, 1, "应收到恰好一次清理通知")
	assert.Equal(t, parsedUUID, notified[0], "通知的 diskID 必须与删除的磁盘 UUID 一致")

	// 4. 校验数据库中的清理任务状态为 PENDING
	var (
		taskStatus int16
		attempts   int32
	)
	err = pool.QueryRow(ctx, "SELECT status, attempts FROM disk_cleanup_tasks WHERE disk_id = $1", parsedUUID).Scan(&taskStatus, &attempts)
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusPending, taskStatus, "清理任务初始状态必须为 PENDING (1)")
	assert.Equal(t, int32(0), attempts, "初始尝试次数应为 0")
}

func TestDiskService_DeleteDisk_NormalDeletion_CreatesPendingTask(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	store := db.NewStore(pool)
	defer store.Close()

	mockWorker := &mockCleanupWorker{}
	diskService := service.NewDiskService(store, service.WithCleanupWorker(mockWorker))

	connectServer := connect.NewServer()
	bowlv1connect.RegisterDiskServiceHandler(connectServer, diskService)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()
	defer func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disk_cleanup_tasks, disks CASCADE")
	}()

	client := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(testServer.Client(), testServer.URL)))

	// 1. 创建虚拟磁盘（无活跃挂载）
	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "normal-deletion-pending-disk",
		CapacityBytes: 2097152,
	})
	require.NoError(t, err)
	diskIDStr := createResp.Disk.DiskId
	parsedUUID := uuid.MustParse(diskIDStr)

	// 2. 执行常规删除 (force = false)
	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskIDStr,
		Force:  false,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	// 3. 校验通知与持久化状态
	notified := mockWorker.NotifiedIDs()
	require.Len(t, notified, 1)
	assert.Equal(t, parsedUUID, notified[0])

	var taskStatus int16
	err = pool.QueryRow(ctx, "SELECT status FROM disk_cleanup_tasks WHERE disk_id = $1", parsedUUID).Scan(&taskStatus)
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusPending, taskStatus)
}

func TestDiskService_DeleteDisk_WithNilWorker_Succeeds(t *testing.T) {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	store := db.NewStore(pool)
	defer store.Close()

	// 未注入任何 worker (worker == nil)
	diskService := service.NewDiskService(store)

	connectServer := connect.NewServer()
	bowlv1connect.RegisterDiskServiceHandler(connectServer, diskService)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	testServer := httptest.NewServer(mux)
	defer testServer.Close()
	defer func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disk_cleanup_tasks, disks CASCADE")
	}()

	client := bowlv1connect.NewDiskServiceClient(connect.NewClient(connecthttp.NewTransport(testServer.Client(), testServer.URL)))

	createResp, err := client.CreateDisk(ctx, &bowlv1.CreateDiskRequest{
		Name:          "nil-worker-disk",
		CapacityBytes: 1048576,
	})
	require.NoError(t, err)
	diskIDStr := createResp.Disk.DiskId
	parsedUUID := uuid.MustParse(diskIDStr)

	deleteResp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
		DiskId: diskIDStr,
		Force:  true,
	})
	require.NoError(t, err)
	require.NotNil(t, deleteResp)

	// 校验数据库中仍然正确创建了 PENDING 任务，供后续轮询兜底执行
	var taskStatus int16
	err = pool.QueryRow(ctx, "SELECT status FROM disk_cleanup_tasks WHERE disk_id = $1", parsedUUID).Scan(&taskStatus)
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusPending, taskStatus)
}

func TestDiskService_DeleteDisk_NotFound(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	nonExistentID := uuid.New().String()

	t.Run("force=false returns NotFound", func(t *testing.T) {
		resp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
			DiskId: nonExistentID,
			Force:  false,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})

	t.Run("force=true returns NotFound", func(t *testing.T) {
		resp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
			DiskId: nonExistentID,
			Force:  true,
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})
}

func TestDiskService_DeleteDisk_ValidationErrors(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
			DiskId: "   ",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})

	t.Run("malformed disk_id returns InvalidArgument", func(t *testing.T) {
		resp, err := client.DeleteDisk(ctx, &bowlv1.DeleteDiskRequest{
			DiskId: "invalid-uuid-string",
		})
		require.Error(t, err)
		assert.Nil(t, resp)
		assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	})
}

