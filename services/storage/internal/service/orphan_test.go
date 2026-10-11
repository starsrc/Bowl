package service

import (
	"context"
	"os"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
)

func TestStorageService_OrphanCleanup_Unmount(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID1 := "client-unmount-orphan-1"
	clientID2 := "client-unmount-orphan-2"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. Mount session 1 (Read-Write)
	mountResp1, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID1,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID1 := mountResp1.GetSessionId()

	// 2. Mount session 2 (Read-Only) on the same disk to test session isolation
	mountResp2, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID2,
		ReadOnly: true,
	})
	require.NoError(t, err)
	sessionID2 := mountResp2.GetSessionId()

	// 3. Create a file in session 1
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID1,
		ParentIno: 1,
		Name:      "unmount_data.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()
	fh1 := createResp.GetFileHandle().GetFh()

	// Write data via fh1
	testData := []byte("unmount dirty buffer payload to be synced")
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh1,
		Offset:    0,
		Data:      testData,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(testData)), writeResp.GetWritten())

	// 4. Open second file handle for the same file in session 1
	openResp, err := h.storageClient.Open(ctx, &bowlv1.OpenRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		OpenMode:  bowlv1.OpenMode_OPEN_MODE_READ_ONLY,
	})
	require.NoError(t, err)
	fh2 := openResp.GetFileHandle().GetFh()

	// 5. Open directory handle in session 1
	openDirResp1, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID1,
		Ino:       1,
	})
	require.NoError(t, err)
	dh1 := openDirResp1.GetFileHandle().GetFh()

	// 6. Open directory handle in session 2
	openDirResp2, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID2,
		Ino:       1,
	})
	require.NoError(t, err)
	dh2 := openDirResp2.GetFileHandle().GetFh()

	// Verify initial handle registries count
	assert.Equal(t, 2, h.storageSvc.OpenFileHandlesCount(sessionID1), "session 1 should have 2 open file handles")
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID1), "session 1 should have 1 open dir handle")
	assert.Equal(t, 0, h.storageSvc.OpenFileHandlesCount(sessionID2), "session 2 should have 0 open file handles")
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID2), "session 2 should have 1 open dir handle")

	// 7. Unmount session 1
	unmountResp, err := h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID1,
	})
	require.NoError(t, err)
	require.NotNil(t, unmountResp)

	// 8. Verify handle registries return 0 remaining handles for session 1
	assert.Equal(t, 0, h.storageSvc.OpenFileHandlesCount(sessionID1), "all file handles for session 1 must be reclaimed")
	assert.Equal(t, 0, h.storageSvc.OpenDirHandlesCount(sessionID1), "all dir handles for session 1 must be reclaimed")

	// Verify session 2's handles remain unaffected (isolation)
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID2), "session 2 dir handle must remain unaffected")

	// 9. Verify subsequent operations using evicted handles return CodeNotFound
	// Read with fh1
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh1,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Write with fh1
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh1,
		Offset:    0,
		Data:      []byte("post-unmount write"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Release with fh1
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Read with fh2
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh2,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Release with fh2
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID1,
		Ino:       fileIno,
		Fh:        fh2,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// ReadDir with dh1
	_, err = h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID1,
		Ino:       1,
		Fh:        dh1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// ReleaseDir with dh1
	_, err = h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID1,
		Ino:       1,
		Fh:        dh1,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 10. Verify data was synced before closing: file content on disk is intact
	filePath := h.storageSvc.filePath(diskID.String(), fileIno)
	dataOnDisk, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, testData, dataOnDisk, "written data must be flushed/synced to disk on unmount")

	// 11. ReadDir with session 2's dh2 must still work
	readDirResp2, err := h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID2,
		Ino:       1,
		Fh:        dh2,
	})
	require.NoError(t, err)
	require.NotNil(t, readDirResp2)
}

func TestStorageService_OrphanCleanup_LeaseTimeout(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-lease-timeout-orphan"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. Mount read-write session
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. Create file and write data
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "lease_timeout_test.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	testData := []byte("payload to be flushed on lease timeout eviction")
	writeResp, err := h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Data:      testData,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(testData)), writeResp.GetWritten())

	// 3. Open directory handle
	openDirResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openDirResp.GetFileHandle().GetFh()

	// Verify handles registered
	assert.Equal(t, 1, h.storageSvc.OpenFileHandlesCount(sessionID))
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID))

	// 4. Simulate lease expiration via CheckExpiredLeases (15 seconds in future)
	futureTime := time.Now().Add(15 * time.Second)
	revokedSessions := h.storageSvc.CheckExpiredLeases(futureTime)
	require.Len(t, revokedSessions, 1)
	assert.Equal(t, sessionID, revokedSessions[0].ID)
	assert.True(t, revokedSessions[0].Revoked)

	// 5. Verify handle registries return 0 remaining handles for session
	assert.Equal(t, 0, h.storageSvc.OpenFileHandlesCount(sessionID), "file handles must be evicted on lease timeout")
	assert.Equal(t, 0, h.storageSvc.OpenDirHandlesCount(sessionID), "dir handles must be evicted on lease timeout")

	// 6. Verify subsequent operations using evicted handles return CodeNotFound
	// Read with fh
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Write with fh
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Data:      []byte("post-timeout write"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Release with fh
	_, err = h.storageClient.Release(ctx, &bowlv1.ReleaseRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// ReadDir with dh
	_, err = h.storageClient.ReadDir(ctx, &bowlv1.ReadDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// ReleaseDir with dh
	_, err = h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 7. Verify file data was flushed and is intact on disk
	filePath := h.storageSvc.filePath(diskID.String(), fileIno)
	dataOnDisk, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, testData, dataOnDisk, "dirty buffer must be synced to disk on lease timeout eviction")
}

func TestStorageService_OrphanCleanup_KeepAliveRevocation(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-keepalive-orphan"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. Mount read-write session
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. Create file and open dir
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "keepalive_test.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	fileIno := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	testData := []byte("keepalive timeout flush verification")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Data:      testData,
	})
	require.NoError(t, err)

	openDirResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openDirResp.GetFileHandle().GetFh()

	assert.Equal(t, 1, h.storageSvc.OpenFileHandlesCount(sessionID))
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID))

	// 3. Artificially expire lease in memory manager
	sess, found := h.sessionMgr.Get(sessionID)
	require.True(t, found)
	sess.LeaseExpiresAt = time.Now().Add(-2 * time.Second)
	h.sessionMgr.Create(sess)

	// 4. Send KeepAlive -> triggers lease revocation
	keepAliveResp, err := h.storageClient.KeepAlive(ctx, &bowlv1.KeepAliveRequest{
		SessionId: sessionID,
	})
	require.NoError(t, err)
	require.NotNil(t, keepAliveResp)
	assert.True(t, keepAliveResp.GetLeaseRevocation(), "lease must be reported as revoked")

	// 5. Verify handle registries return 0 remaining handles
	assert.Equal(t, 0, h.storageSvc.OpenFileHandlesCount(sessionID))
	assert.Equal(t, 0, h.storageSvc.OpenDirHandlesCount(sessionID))

	// 6. Verify subsequent operations using evicted handles return CodeNotFound
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Ino:       fileIno,
		Fh:        fh,
		Offset:    0,
		Data:      []byte("post-revocation write"),
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// 7. Verify file was synced
	filePath := h.storageSvc.filePath(diskID.String(), fileIno)
	dataOnDisk, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, testData, dataOnDisk)
}

func TestStorageService_OrphanCleanup_UnmountForce_CleansHandles(t *testing.T) {
	h := setupTestHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "client-force-unmount"

	schemaName := FormatDiskSchema(diskID)
	defer func() {
		_, _ = h.dbPool.Exec(ctx, `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	// 1. Mount session
	mountResp, err := h.storageClient.Mount(ctx, &bowlv1.MountRequest{
		DiskId:   diskID.String(),
		ClientId: clientID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	sessionID := mountResp.GetSessionId()

	// 2. Create file handle and dir handle
	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "force_unmount.txt",
		Mode:      0o644,
		Flags:     2,
	})
	require.NoError(t, err)
	fh := createResp.GetFileHandle().GetFh()

	openDirResp, err := h.storageClient.OpenDir(ctx, &bowlv1.OpenDirRequest{
		SessionId: sessionID,
		Ino:       1,
	})
	require.NoError(t, err)
	dh := openDirResp.GetFileHandle().GetFh()

	assert.Equal(t, 1, h.storageSvc.OpenFileHandlesCount(sessionID))
	assert.Equal(t, 1, h.storageSvc.OpenDirHandlesCount(sessionID))

	// 3. Unmount with Force: true
	_, err = h.storageClient.Unmount(ctx, &bowlv1.UnmountRequest{
		SessionId: sessionID,
		Force:     true,
	})
	require.NoError(t, err)

	// 4. Verify handles are cleaned up
	assert.Equal(t, 0, h.storageSvc.OpenFileHandlesCount(sessionID))
	assert.Equal(t, 0, h.storageSvc.OpenDirHandlesCount(sessionID))

	// 5. Operations return CodeNotFound
	_, err = h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Ino:       createResp.GetInode().GetIno(),
		Fh:        fh,
		Offset:    0,
		Size:      10,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	_, err = h.storageClient.ReleaseDir(ctx, &bowlv1.ReleaseDirRequest{
		SessionId: sessionID,
		Ino:       1,
		Fh:        dh,
	})
	require.Error(t, err)
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}
