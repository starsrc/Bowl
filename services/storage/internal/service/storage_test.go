package service

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
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
	"github.com/starsrc/bowl/services/storage/internal/session"
)

// fakeDiskService mocks DiskService over Connect-RPC for testing.
type fakeDiskService struct {
	bowlv1connect.UnimplementedDiskServiceHandler
	mu          sync.Mutex
	attachCalls []*bowlv1.AttachDiskRequest
	detachCalls []*bowlv1.DetachDiskRequest
	getCalls    []*bowlv1.GetDiskRequest
	statfsCalls []*bowlv1.GetDiskStatfsRequest
	attachHook  func(ctx context.Context, req *bowlv1.AttachDiskRequest) (*bowlv1.AttachDiskResponse, error)
	detachHook  func(ctx context.Context, req *bowlv1.DetachDiskRequest) (*bowlv1.DetachDiskResponse, error)
	getHook     func(ctx context.Context, req *bowlv1.GetDiskRequest) (*bowlv1.GetDiskResponse, error)
	statfsHook  func(ctx context.Context, req *bowlv1.GetDiskStatfsRequest) (*bowlv1.GetDiskStatfsResponse, error)
}

func (f *fakeDiskService) AttachDisk(ctx context.Context, req *bowlv1.AttachDiskRequest) (*bowlv1.AttachDiskResponse, error) {
	f.mu.Lock()
	f.attachCalls = append(f.attachCalls, req)
	hook := f.attachHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, req)
	}

	return &bowlv1.AttachDiskResponse{
		Disk: &bowlv1.Disk{
			DiskId:        req.GetDiskId(),
			Name:          "test-disk",
			CapacityBytes: 10 * 1024 * 1024 * 1024, // 10 GB
			UsedBytes:     1024 * 1024,             // 1 MB
			InodesQuota:   1000000,
			InodesUsed:    10,
			Status:        bowlv1.DiskStatus_DISK_STATUS_ATTACHED,
		},
	}, nil
}

func (f *fakeDiskService) DetachDisk(ctx context.Context, req *bowlv1.DetachDiskRequest) (*bowlv1.DetachDiskResponse, error) {
	f.mu.Lock()
	f.detachCalls = append(f.detachCalls, req)
	hook := f.detachHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, req)
	}

	return &bowlv1.DetachDiskResponse{
		Disk: &bowlv1.Disk{
			DiskId:        req.GetDiskId(),
			Name:          "test-disk",
			CapacityBytes: 10 * 1024 * 1024 * 1024,
			Status:        bowlv1.DiskStatus_DISK_STATUS_READY,
		},
	}, nil
}

func (f *fakeDiskService) GetDisk(ctx context.Context, req *bowlv1.GetDiskRequest) (*bowlv1.GetDiskResponse, error) {
	f.mu.Lock()
	f.getCalls = append(f.getCalls, req)
	hook := f.getHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, req)
	}

	return &bowlv1.GetDiskResponse{
		Disk: &bowlv1.Disk{
			DiskId:        req.GetDiskId(),
			Name:          "test-disk",
			CapacityBytes: 10 * 1024 * 1024 * 1024, // 10 GB
			UsedBytes:     1024 * 1024,             // 1 MB
			InodesQuota:   1000000,
			InodesUsed:    10,
			Status:        bowlv1.DiskStatus_DISK_STATUS_ATTACHED,
		},
	}, nil
}

func (f *fakeDiskService) GetDiskStatfs(ctx context.Context, req *bowlv1.GetDiskStatfsRequest) (*bowlv1.GetDiskStatfsResponse, error) {
	f.mu.Lock()
	f.statfsCalls = append(f.statfsCalls, req)
	hook := f.statfsHook
	f.mu.Unlock()

	if hook != nil {
		return hook(ctx, req)
	}

	return nil, connect.NewError(connect.CodeUnimplemented, "GetDiskStatfs unimplemented")
}

type testHarness struct {
	storageClient bowlv1connect.StorageServiceClient
	fakeDiskSvc   *fakeDiskService
	sessionMgr    *session.MemoryManager
	dbPool        *pgxpool.Pool
	dataDir       string
	cleanup       func()
}

func setupTestHarness(t *testing.T) *testHarness {
	t.Helper()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	tempDir, err := os.MkdirTemp("", "bowl-storage-test-*")
	require.NoError(t, err)

	// 1. Setup in-process DiskService test server
	fakeDisk := &fakeDiskService{}
	diskConnectServer := connect.NewServer()
	bowlv1connect.RegisterDiskServiceHandler(diskConnectServer, fakeDisk)
	diskMux := http.NewServeMux()
	connecthttp.Mount(diskMux, diskConnectServer)
	diskTestServer := httptest.NewServer(diskMux)

	diskTransport := connecthttp.NewTransport(diskTestServer.Client(), diskTestServer.URL)
	diskConnectClient := connect.NewClient(diskTransport)
	diskClient := bowlv1connect.NewDiskServiceClient(diskConnectClient)

	// 2. Setup in-process StorageService test server
	sessionMgr := session.NewMemoryManager()
	storageSvc := NewStorageService(diskClient, pool, sessionMgr, tempDir)

	storageConnectServer := connect.NewServer()
	bowlv1connect.RegisterStorageServiceHandler(storageConnectServer, storageSvc)
	storageMux := http.NewServeMux()
	connecthttp.Mount(storageMux, storageConnectServer)
	storageTestServer := httptest.NewServer(storageMux)

	storageTransport := connecthttp.NewTransport(storageTestServer.Client(), storageTestServer.URL)
	storageConnectClient := connect.NewClient(storageTransport)
	storageClient := bowlv1connect.NewStorageServiceClient(storageConnectClient)

	cleanup := func() {
		storageTestServer.Close()
		diskTestServer.Close()
		_ = os.RemoveAll(tempDir)
		pool.Close()
	}

	return &testHarness{
		storageClient: storageClient,
		fakeDiskSvc:   fakeDisk,
		sessionMgr:    sessionMgr,
		dbPool:        pool,
		dataDir:       tempDir,
		cleanup:       cleanup,
	}
}

func TestStorageService_Mount_Success_ReadWrite(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-node-1"

	// Cleanup schema on test completion
	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	resp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// 1. Verify response fields
	assert.NotEmpty(t, resp.GetSessionId())
	assert.True(t, resp.GetIsWriteLease())
	assert.Equal(t, int64(10), resp.GetLeaseTimeout().GetSeconds())
	assert.True(t, resp.GetLeaseExpireTime().AsTime().After(time.Now()))

	// Root Inode check
	root := resp.GetRootInode()
	require.NotNil(t, root)
	assert.Equal(t, uint64(1), root.GetIno())
	assert.Equal(t, bowlv1.InodeType_INODE_TYPE_DIRECTORY, root.GetType())
	assert.Equal(t, uint32(0o755), root.GetMode())

	// Statfs check
	statfs := resp.GetStatfs()
	require.NotNil(t, statfs)
	assert.Equal(t, uint32(4096), statfs.GetBlockSize())
	assert.Equal(t, uint32(4096), statfs.GetFragmentSize())
	assert.Equal(t, uint64(10*1024*1024*1024/4096), statfs.GetBlocks())
	assert.Equal(t, uint32(255), statfs.GetMaxNameLen())

	// 2. Verify ADR 0005: DiskService.AttachDisk call
	h.fakeDiskSvc.mu.Lock()
	require.Len(t, h.fakeDiskSvc.attachCalls, 1)
	assert.Equal(t, diskID.String(), h.fakeDiskSvc.attachCalls[0].GetDiskId())
	assert.Equal(t, clientID, h.fakeDiskSvc.attachCalls[0].GetClientId())
	assert.False(t, h.fakeDiskSvc.attachCalls[0].GetReadOnly())
	h.fakeDiskSvc.mu.Unlock()

	// 3. Verify ADR 0006: PostgreSQL Schema created
	var schemaCount int
	err = h.dbPool.QueryRow(ctx, "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1", schemaName).Scan(&schemaCount)
	require.NoError(t, err)
	assert.Equal(t, 1, schemaCount, "schema %s must exist in PostgreSQL", schemaName)

	// 4. Verify ADR 0009: Local chunk directory created
	chunkDir := filepath.Join(h.dataDir, diskID.String())
	dirStat, err := os.Stat(chunkDir)
	require.NoError(t, err)
	assert.True(t, dirStat.IsDir())

	// 5. Verify in-memory session
	sess, found := h.sessionMgr.Get(resp.GetSessionId())
	require.True(t, found)
	assert.Equal(t, diskID.String(), sess.DiskID)
	assert.Equal(t, clientID, sess.ClientID)
	assert.False(t, sess.ReadOnly)
	assert.True(t, sess.IsWriteLease)
}

func TestStorageService_Mount_Success_ReadOnly(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-reader-1"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	resp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Read-only mount must NOT acquire write lease
	assert.False(t, resp.GetIsWriteLease())

	// In-memory session check
	sess, found := h.sessionMgr.Get(resp.GetSessionId())
	require.True(t, found)
	assert.True(t, sess.ReadOnly)
	assert.False(t, sess.IsWriteLease)
}

func TestStorageService_Mount_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()

	// 1. Empty disk_id
	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   "",
		ClientId: "client-1",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Malformed disk_id UUID
	_, err = h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   "not-a-valid-uuid",
		ClientId: "client-1",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Empty client_id
	_, err = h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   uuid.New().String(),
		ClientId: "",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Mount_AttachDiskFailure(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()

	// Configure fake DiskService to return FailedPrecondition (e.g. concurrent writer)
	h.fakeDiskSvc.mu.Lock()
	h.fakeDiskSvc.attachHook = func(ctx context.Context, req *bowlv1.AttachDiskRequest) (*bowlv1.AttachDiskResponse, error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, "disk already has an exclusive writer attached")
	}
	h.fakeDiskSvc.mu.Unlock()

	_, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: "writer-2",
		ReadOnly: false,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	// Ensure no session was created
	assert.Empty(t, h.sessionMgr.ListByDiskID(diskID.String()))
}

func TestStorageService_Unmount_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-node-unmount"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Verify session exists
	_, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)

	// Call Unmount
	unmountResp, err := h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID,
		Force:     false,
	})
	require.NoError(t, err)
	require.NotNil(t, unmountResp)

	// Verify ADR 0005: DetachDisk called on DiskService
	h.fakeDiskSvc.mu.Lock()
	require.Len(t, h.fakeDiskSvc.detachCalls, 1)
	assert.Equal(t, diskID.String(), h.fakeDiskSvc.detachCalls[0].GetDiskId())
	assert.Equal(t, clientID, h.fakeDiskSvc.detachCalls[0].GetClientId())
	assert.False(t, h.fakeDiskSvc.detachCalls[0].GetForce())
	h.fakeDiskSvc.mu.Unlock()

	// Verify session removed from memory
	_, found = h.sessionMgr.Get(sessionID)
	assert.False(t, found)
}

func TestStorageService_Unmount_NotFoundAndForce(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	fakeSessionID := uuid.New().String()

	// 1. Unmount non-existent session without force returns NotFound
	_, err := h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: fakeSessionID,
		Force:     false,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 2. Unmount non-existent session with force succeeds idempotently
	unmountResp, err := h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: fakeSessionID,
		Force:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, unmountResp)

	// 3. Validation error: empty session_id
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: "",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Unmount_DetachDiskFailureWithoutForce(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-node-err"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Configure DetachDisk to fail
	h.fakeDiskSvc.mu.Lock()
	h.fakeDiskSvc.detachHook = func(ctx context.Context, req *bowlv1.DetachDiskRequest) (*bowlv1.DetachDiskResponse, error) {
		return nil, connect.NewError(connect.CodeInternal, "network timeout calling storage daemon")
	}
	h.fakeDiskSvc.mu.Unlock()

	// Unmount without force should fail and retain session
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID,
		Force:     false,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))

	_, found := h.sessionMgr.Get(sessionID)
	assert.True(t, found, "session should be retained if non-force detach fails")

	// Unmount WITH force should succeed and delete session
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID,
		Force:     true,
	})
	require.NoError(t, err)

	_, found = h.sessionMgr.Get(sessionID)
	assert.False(t, found, "session must be removed on force unmount")
}

func TestStorageService_KeepAlive_ActiveWriter_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "writer-client-keepalive"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()
	initialExpiry := mountResp.GetLeaseExpireTime().AsTime()

	time.Sleep(15 * time.Millisecond)

	keepAliveResp, err := h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: sessionID,
	})
	require.NoError(t, err)
	require.NotNil(t, keepAliveResp)

	assert.True(t, keepAliveResp.GetIsWriteLease())
	assert.False(t, keepAliveResp.GetLeaseRevocation())
	assert.True(t, keepAliveResp.GetLeaseExpireTime().AsTime().After(initialExpiry))

	// Verify session manager reflects renewed expiry
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	assert.True(t, sess.IsWriteLease)
	assert.False(t, sess.Revoked)
	assert.True(t, sess.LeaseExpiresAt.After(initialExpiry))
}

func TestStorageService_KeepAlive_ReadOnly(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "reader-client-keepalive"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	keepAliveResp, err := h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: sessionID,
	})
	require.NoError(t, err)
	require.NotNil(t, keepAliveResp)

	assert.False(t, keepAliveResp.GetIsWriteLease())
	assert.False(t, keepAliveResp.GetLeaseRevocation())
}

func TestStorageService_KeepAlive_Timeout_TriggersRevocation(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "expired-client-keepalive"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Simulate expired lease by updating the session expiration to the past
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	sess.LeaseExpiresAt = time.Now().Add(-2 * time.Second)
	h.sessionMgr.Create(sess)

	// KeepAlive should detect expiration and trigger revocation
	keepAliveResp, err := h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: sessionID,
	})
	require.NoError(t, err)
	require.NotNil(t, keepAliveResp)

	assert.False(t, keepAliveResp.GetIsWriteLease())
	assert.True(t, keepAliveResp.GetLeaseRevocation())

	// Session in manager must be marked revoked
	storedSess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	assert.False(t, storedSess.IsWriteLease)
	assert.True(t, storedSess.Revoked)
}

func TestStorageService_KeepAlive_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()

	// 1. Empty session_id -> InvalidArgument
	_, err := h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: "",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Whitespace session_id -> InvalidArgument
	_, err = h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: "   ",
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 3. Non-existent session_id -> NotFound
	_, err = h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: uuid.New().String(),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Statfs_Success(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "statfs-client-1"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()
	require.NotEmpty(t, sessionID)

	statfsResp, err := h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	require.NotNil(t, statfsResp)

	statfs := statfsResp.GetStatfs()
	require.NotNil(t, statfs)

	// Block and fragment size: 4096
	assert.Equal(t, uint32(4096), statfs.GetBlockSize())
	assert.Equal(t, uint32(4096), statfs.GetFragmentSize())

	// Capacity: 10GB = 10 * 1024 * 1024 * 1024 bytes -> 2621440 blocks
	expectedBlocks := uint64(10 * 1024 * 1024 * 1024 / 4096)
	assert.Equal(t, expectedBlocks, statfs.GetBlocks())

	// Used: 1MB = 1024 * 1024 bytes -> free = (10GB - 1MB) / 4096
	expectedBlocksFree := uint64((10*1024*1024*1024 - 1024*1024) / 4096)
	assert.Equal(t, expectedBlocksFree, statfs.GetBlocksFree())
	assert.Equal(t, expectedBlocksFree, statfs.GetBlocksAvail())

	// Inodes: quota = 1000000, used = 10 -> free = 999990
	assert.Equal(t, uint64(1000000), statfs.GetFiles())
	assert.Equal(t, uint64(999990), statfs.GetFilesFree())

	// Max filename length: 255
	assert.Equal(t, uint32(255), statfs.GetMaxNameLen())
}

func TestStorageService_Statfs_ValidationErrors(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()

	// 1. Empty session_id -> InvalidArgument
	_, err := h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: "",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	// 2. Whitespace session_id -> InvalidArgument
	_, err = h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: "   ",
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestStorageService_Statfs_NotFound(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	unknownSessionID := uuid.New().String()

	_, err := h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: unknownSessionID,
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestStorageService_Statfs_DirectFromDiskStatfs(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "statfs-client-direct"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Configure fake DiskService to return custom Statfs via GetDiskStatfs
	customStatfs := &bowlv1.Statfs{
		BlockSize:    4096,
		FragmentSize: 4096,
		Blocks:       5000000,
		BlocksFree:   4500000,
		BlocksAvail:  4500000,
		Files:        2000000,
		FilesFree:    1900000,
		MaxNameLen:   255,
	}
	h.fakeDiskSvc.mu.Lock()
	h.fakeDiskSvc.statfsHook = func(ctx context.Context, req *bowlv1.GetDiskStatfsRequest) (*bowlv1.GetDiskStatfsResponse, error) {
		return &bowlv1.GetDiskStatfsResponse{
			DiskId: req.GetDiskId(),
			Statfs: customStatfs,
		}, nil
	}
	h.fakeDiskSvc.mu.Unlock()

	statfsResp, err := h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	require.NotNil(t, statfsResp)
	assert.Equal(t, customStatfs.GetBlocks(), statfsResp.GetStatfs().GetBlocks())
	assert.Equal(t, customStatfs.GetBlocksFree(), statfsResp.GetStatfs().GetBlocksFree())
	assert.Equal(t, customStatfs.GetFiles(), statfsResp.GetStatfs().GetFiles())
	assert.Equal(t, customStatfs.GetFilesFree(), statfsResp.GetStatfs().GetFilesFree())
}

func TestStorageService_Statfs_DiskServiceError(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "statfs-client-err"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, "DROP SCHEMA IF EXISTS \""+schemaName+"\" CASCADE")
	}()

	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// Configure fake DiskService GetDisk to fail
	h.fakeDiskSvc.mu.Lock()
	h.fakeDiskSvc.getHook = func(ctx context.Context, req *bowlv1.GetDiskRequest) (*bowlv1.GetDiskResponse, error) {
		return nil, connect.NewError(connect.CodeInternal, "disk store query error")
	}
	h.fakeDiskSvc.mu.Unlock()

	_, err = h.storageClient.Statfs(ctx, &bowlv1.StatfsRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(err))
}
