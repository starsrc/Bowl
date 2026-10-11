package service_test

import (
	"context"
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
