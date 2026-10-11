package service

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1 "github.com/starsrc/bowl/gen/go/bowl/v1"
	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

func TestChunkDriver_LocateSlice_PostgreSQL(t *testing.T) {
	h, _, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	clientID := "locate-slice-client"
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

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "test_locate.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write 1KB payload to allocate slice
	payload := []byte("test locate slice in postgresql")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// Inode write allocated sliceID = 1
	sliceID := uint64(1)
	foundIno, foundChunk, found, err := driver.LocateSlice(ctx, diskID.String(), sliceID)
	require.NoError(t, err)
	assert.True(t, found, "slice must be found in PostgreSQL chunks table")
	assert.Equal(t, ino, foundIno)
	assert.Equal(t, uint32(0), foundChunk)

	// Non-existent slice ID
	_, _, foundMissing, err := driver.LocateSlice(ctx, diskID.String(), 999999)
	require.NoError(t, err)
	assert.False(t, foundMissing, "non-existent slice must return found=false")
}

func TestChunkDriver_CrashRecovery_ReplaysUnflushedSlicesToS3(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID := uuid.New()
	clientID := "crash-recovery-client"
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

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "unflushed_file.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// 1. Write file when S3 uploader is NOT yet attached (simulating dirty unflushed slices before crash)
	payload1 := bytes.Repeat([]byte("A"), 1024*1024) // 1MB
	payload2 := bytes.Repeat([]byte("B"), 1024*1024) // 1MB
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload1,
	})
	require.NoError(t, err)

	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    1024 * 1024,
		Data:      payload2,
	})
	require.NoError(t, err)

	// Verify slice files exist on local staging NVMe
	slice1Path := stagingMgr.SlicePath(diskID.String(), 1)
	slice2Path := stagingMgr.SlicePath(diskID.String(), 2)
	assert.FileExists(t, slice1Path)
	assert.FileExists(t, slice2Path)

	// 2. Simulate server restart:
	// Configure S3 uploader with mock S3 client
	mockClient := newMockS3Client()
	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(5*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	// S3 is initially empty
	assert.Empty(t, mockClient.getObjects())

	// 3. Trigger RecoverStaging
	report, err := driver.RecoverStaging(ctx)
	require.NoError(t, err)
	require.NotNil(t, report)

	assert.Equal(t, 2, report.ScannedSlices)
	assert.Equal(t, 2, report.RequeuedSlices, "both unflushed slices must be re-queued to S3")
	assert.Equal(t, 0, report.SkippedSlices)
	assert.Equal(t, uint64(2), report.MaxSliceID)

	// 4. Wait for uploader workers to finish
	err = uploader.WaitForAll(ctx)
	require.NoError(t, err)

	// Verify S3 now contains both slices (0 data loss)
	objs := mockClient.getObjects()
	key1 := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	key2 := chunk.FormatObjectKey(diskID.String(), 0, 2, 0)
	assert.Contains(t, objs, key1)
	assert.Contains(t, objs, key2)
	assert.Equal(t, payload1, objs[key1])
	assert.Equal(t, payload2, objs[key2])

	// Verify subsequent write generates slice ID > 2, avoiding overwriting existing slices
	nextID := stagingMgr.NextSliceID()
	assert.Greater(t, nextID, uint64(2))
}

func TestChunkDriver_CrashRecovery_TransparentReadFromStagingBeforeAndAfterUpload(t *testing.T) {
	h, stagingMgr, driver := setupChunkHarness(t)
	defer h.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID := uuid.New()
	clientID := "transparent-read-client"
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

	createResp, err := h.storageClient.Create(ctx, &bowlv1.CreateRequest{
		SessionId: sessionID,
		ParentIno: 1,
		Name:      "concurrent_read.bin",
		Mode:      0o644,
		Flags:     0,
	})
	require.NoError(t, err)
	ino := createResp.GetInode().GetIno()
	fh := createResp.GetFileHandle().GetFh()

	// Write payload without S3 uploader running (unflushed staging slice)
	payload := []byte("transparent read payload across recovery phases")
	_, err = h.storageClient.Write(ctx, &bowlv1.WriteRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Data:      payload,
	})
	require.NoError(t, err)

	// Phase 1: Client reads BEFORE recovery replay
	readResp1, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(len(payload)),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp1.GetData(), "must read transparently from NVMe staging before recovery")

	// Phase 2: Start recovery with simulated delayed S3 upload
	mockClient := newMockS3Client()
	mockClient.putDelay = 50 * time.Millisecond
	uploader := chunk.NewS3UploaderWithClient(
		mockClient,
		"bowl-data",
		chunk.WithStaging(stagingMgr),
		chunk.WithWorkers(1),
		chunk.WithInitialBackoff(5*time.Millisecond),
	)
	driver.SetUploader(uploader)
	uploader.Start(ctx)
	defer uploader.Stop()

	report, err := driver.RecoverStaging(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, report.RequeuedSlices)

	// Phase 3: Client reads DURING recovery replay (while upload is in flight)
	readResp2, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(len(payload)),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp2.GetData(), "must read transparently from NVMe staging during recovery")

	// Wait for upload to complete
	err = uploader.WaitForAll(ctx)
	require.NoError(t, err)

	// Phase 4: Client reads AFTER recovery upload finishes
	readResp3, err := h.storageClient.Read(ctx, &bowlv1.ReadRequest{
		SessionId: sessionID,
		Fh:        fh,
		Ino:       ino,
		Offset:    0,
		Size:      uint32(len(payload)),
	})
	require.NoError(t, err)
	assert.Equal(t, payload, readResp3.GetData(), "must read transparently after recovery completion")

	// Verify object has landed in S3
	key := chunk.FormatObjectKey(diskID.String(), 0, 1, 0)
	objs := mockClient.getObjects()
	assert.Contains(t, objs, key)
	assert.Equal(t, payload, objs[key])
}
