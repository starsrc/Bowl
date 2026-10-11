package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
	"github.com/starsrc/bowl/services/storage/internal/config"
)

func isMinIOAvailable(endpoint string) bool {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(endpoint + "/minio/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func TestMinIO_Integration_BackgroundUpload4MBBlock(t *testing.T) {
	minioEndpoint := "http://127.0.0.1:9000"
	if !isMinIOAvailable(minioEndpoint) {
		t.Skip("MinIO is not running at 127.0.0.1:9000, skipping live integration test")
	}

	cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       minioEndpoint,
		Bucket:         "bowl-data",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		UseSSL:         false,
		ForcePathStyle: true,
	}

	tempDir := t.TempDir()
	staging := chunk.NewStagingManager(tempDir)

	uploader, err := chunk.NewS3Uploader(cfg, staging,
		chunk.WithWorkers(4),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "d4b4a11b-0000-0000-0000-000000000001"
	sliceID := staging.NextSliceID()
	chunkIndx := uint32(0)
	ino := uint64(1001)

	// Generate 4MB + 512KB payload (~4.5MB, partition into 2 blocks: 4MB block 0, 512KB block 1)
	payloadSize := chunk.BlockSize + 512*1024
	payload := make([]byte, payloadSize)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	// 1. Stage slice to NVMe staging cache
	err = staging.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	// 2. Enqueue background upload task
	task := &chunk.UploadTask{
		DiskID:    diskID,
		Ino:       ino,
		ChunkIndx: chunkIndx,
		SliceID:   sliceID,
	}
	err = uploader.Enqueue(ctx, task)
	require.NoError(t, err)

	// 3. Wait for barrier completion
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()

	err = uploader.WaitForSlice(waitCtx, diskID, sliceID)
	require.NoError(t, err)

	assert.Equal(t, chunk.SliceStatusSynced, uploader.GetSliceStatus(diskID, sliceID))

	// 4. Verify blocks in real MinIO
	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)

	key0 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 0)
	key1 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 1)

	defer func() {
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, key0, minio.RemoveObjectOptions{})
		_ = minioClient.RemoveObject(context.Background(), cfg.Bucket, key1, minio.RemoveObjectOptions{})
	}()

	// Verify Block 0 (4MB)
	stat0, err := minioClient.StatObject(context.Background(), cfg.Bucket, key0, minio.StatObjectOptions{})
	require.NoError(t, err, "object %s must exist in MinIO", key0)
	assert.Equal(t, int64(chunk.BlockSize), stat0.Size)

	obj0, err := minioClient.GetObject(context.Background(), cfg.Bucket, key0, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj0.Close()
	data0, err := io.ReadAll(obj0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload[:chunk.BlockSize], data0), "Block 0 content must match original payload")

	// Verify Block 1 (512KB)
	stat1, err := minioClient.StatObject(context.Background(), cfg.Bucket, key1, minio.StatObjectOptions{})
	require.NoError(t, err, "object %s must exist in MinIO", key1)
	assert.Equal(t, int64(512*1024), stat1.Size)

	obj1, err := minioClient.GetObject(context.Background(), cfg.Bucket, key1, minio.GetObjectOptions{})
	require.NoError(t, err)
	defer obj1.Close()
	data1, err := io.ReadAll(obj1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload[chunk.BlockSize:], data1), "Block 1 content must match original payload")

	fmt.Printf("Successfully validated 4MB and 512KB blocks uploaded to MinIO: %s, %s\n", key0, key1)
}

func TestMinIO_Integration_GarbageCollectionMultiBlock(t *testing.T) {
	minioEndpoint := "http://127.0.0.1:9000"
	if !isMinIOAvailable(minioEndpoint) {
		t.Skip("MinIO is not running at 127.0.0.1:9000, skipping live integration test")
	}

	cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       minioEndpoint,
		Bucket:         "bowl-data",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		UseSSL:         false,
		ForcePathStyle: true,
	}

	tempDir := t.TempDir()
	staging := chunk.NewStagingManager(tempDir)

	uploader, err := chunk.NewS3Uploader(cfg, staging,
		chunk.WithWorkers(2),
		chunk.WithInitialBackoff(10*time.Millisecond),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "d4b4a11b-0000-0000-0000-000000000099"
	sliceID := staging.NextSliceID()
	chunkIndx := uint32(1)

	payloadSize := chunk.BlockSize + 512*1024
	payload := make([]byte, payloadSize)
	_, err = rand.Read(payload)
	require.NoError(t, err)

	err = staging.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	task := &chunk.UploadTask{
		DiskID:    diskID,
		ChunkIndx: chunkIndx,
		SliceID:   sliceID,
	}
	err = uploader.Enqueue(ctx, task)
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID, sliceID)
	require.NoError(t, err)

	key0 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 0)
	key1 := chunk.FormatObjectKey(diskID, chunkIndx, sliceID, 1)

	minioClient, err := chunk.NewMinIOClient(cfg)
	require.NoError(t, err)

	// Verify both objects exist before GC
	_, err = minioClient.StatObject(ctx, cfg.Bucket, key0, minio.StatObjectOptions{})
	require.NoError(t, err)
	_, err = minioClient.StatObject(ctx, cfg.Bucket, key1, minio.StatObjectOptions{})
	require.NoError(t, err)

	// Setup GC with expired slice (refs = 0)
	db := newMockGCDB()
	db.rows[sliceID] = &mockSliceRef{
		id:        sliceID,
		chunkIndx: chunkIndx,
		size:      uint32(payloadSize),
		refs:      0,
	}

	gc := chunk.NewGarbageCollector(db, staging,
		chunk.WithGCClient(minioClient, cfg.Bucket),
	)

	n, err := gc.RunGC(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Both blocks must be removed from MinIO
	_, err = minioClient.StatObject(ctx, cfg.Bucket, key0, minio.StatObjectOptions{})
	assert.Error(t, err)
	_, err = minioClient.StatObject(ctx, cfg.Bucket, key1, minio.StatObjectOptions{})
	assert.Error(t, err)

	// Staging file must be removed
	_, err = os.Stat(staging.SlicePath(diskID, sliceID))
	assert.True(t, os.IsNotExist(err))

	// Row deleted
	_, exists := db.rows[sliceID]
	assert.False(t, exists)
}

