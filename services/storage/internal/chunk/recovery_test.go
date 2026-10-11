package chunk

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockLocator struct {
	mapping map[string]map[uint64]struct {
		ino       uint64
		chunkIndx uint32
	}
}

func newMockLocator() *mockLocator {
	return &mockLocator{
		mapping: make(map[string]map[uint64]struct {
			ino       uint64
			chunkIndx uint32
		}),
	}
}

func (m *mockLocator) add(diskID string, sliceID uint64, ino uint64, chunkIndx uint32) {
	if _, ok := m.mapping[diskID]; !ok {
		m.mapping[diskID] = make(map[uint64]struct {
			ino       uint64
			chunkIndx uint32
		})
	}
	m.mapping[diskID][sliceID] = struct {
		ino       uint64
		chunkIndx uint32
	}{ino: ino, chunkIndx: chunkIndx}
}

func (m *mockLocator) LocateSlice(ctx context.Context, diskID string, sliceID uint64) (uint64, uint32, bool, error) {
	if diskMap, ok := m.mapping[diskID]; ok {
		if meta, ok := diskMap[sliceID]; ok {
			return meta.ino, meta.chunkIndx, true, nil
		}
	}
	return 0, 0, false, nil
}

func TestRecoverStagingPipeline_EmptyStaging(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "empty_staging")
	staging := NewStagingManager(stagingDir)
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket", WithStaging(staging))
	locator := newMockLocator()

	report, err := RecoverStagingPipeline(ctx, staging, uploader, locator)
	require.NoError(t, err)
	require.NotNil(t, report)
	assert.Equal(t, 0, report.ScannedDisks)
	assert.Equal(t, 0, report.ScannedSlices)
	assert.Equal(t, 0, report.RequeuedSlices)
	assert.Equal(t, 0, report.SkippedSlices)
	assert.Equal(t, 0, report.OrphanedSlices)
}

func TestRecoverStagingPipeline_UnflushedSlices_Requeued(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stagingDir := filepath.Join(t.TempDir(), "recovery_staging")
	staging := NewStagingManager(stagingDir)
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(2),
	)
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-replay-1"
	payload1 := []byte("payload for slice 10")
	payload2 := []byte("payload for slice 25")

	// Pre-populate staging with unflushed slices
	err := staging.WriteSlice(diskID, 10, payload1)
	require.NoError(t, err)
	err = staging.WriteSlice(diskID, 25, payload2)
	require.NoError(t, err)

	locator := newMockLocator()
	locator.add(diskID, 10, 101, 0)
	locator.add(diskID, 25, 101, 1)

	report, err := RecoverStagingPipeline(ctx, staging, uploader, locator)
	require.NoError(t, err)
	require.NotNil(t, report)

	assert.Equal(t, 1, report.ScannedDisks)
	assert.Equal(t, 2, report.ScannedSlices)
	assert.Equal(t, 2, report.RequeuedSlices)
	assert.Equal(t, 0, report.SkippedSlices)
	assert.Equal(t, 0, report.OrphanedSlices)
	assert.Equal(t, uint64(25), report.MaxSliceID)

	// Wait for background workers to complete upload
	err = uploader.WaitForAll(ctx)
	require.NoError(t, err)

	// Verify both objects are in S3
	key1 := FormatObjectKey(diskID, 0, 10, 0)
	key2 := FormatObjectKey(diskID, 1, 25, 0)

	mockClient.mu.Lock()
	assert.Equal(t, payload1, mockClient.objects[key1])
	assert.Equal(t, payload2, mockClient.objects[key2])
	mockClient.mu.Unlock()

	// Verify next slice ID has been advanced past max existing slice ID
	nextID := staging.NextSliceID()
	assert.Greater(t, nextID, uint64(25))
}

func TestRecoverStagingPipeline_AlreadyPersisted_Skipped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stagingDir := filepath.Join(t.TempDir(), "recovery_staging_skipped")
	staging := NewStagingManager(stagingDir)
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(2),
	)
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-replay-skipped"
	payload1 := []byte("already in s3")
	payload2 := []byte("needs replay to s3")

	err := staging.WriteSlice(diskID, 10, payload1)
	require.NoError(t, err)
	err = staging.WriteSlice(diskID, 11, payload2)
	require.NoError(t, err)

	// Pre-populate slice 10 in S3
	key1 := FormatObjectKey(diskID, 0, 10, 0)
	mockClient.mu.Lock()
	mockClient.objects[key1] = payload1
	mockClient.mu.Unlock()

	locator := newMockLocator()
	locator.add(diskID, 10, 201, 0)
	locator.add(diskID, 11, 201, 0)

	report, err := RecoverStagingPipeline(ctx, staging, uploader, locator)
	require.NoError(t, err)
	require.NotNil(t, report)

	assert.Equal(t, 2, report.ScannedSlices)
	assert.Equal(t, 1, report.SkippedSlices, "slice 10 should be skipped as it is already in S3")
	assert.Equal(t, 1, report.RequeuedSlices, "slice 11 should be requeued")

	err = uploader.WaitForAll(ctx)
	require.NoError(t, err)

	key2 := FormatObjectKey(diskID, 0, 11, 0)
	mockClient.mu.Lock()
	assert.Equal(t, payload2, mockClient.objects[key2])
	mockClient.mu.Unlock()
}

func TestRecoverStagingPipeline_OrphanedSlice_Skipped(t *testing.T) {
	ctx := context.Background()
	stagingDir := filepath.Join(t.TempDir(), "recovery_staging_orphan")
	staging := NewStagingManager(stagingDir)
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket", WithStaging(staging))
	locator := newMockLocator()

	diskID := "disk-orphan"
	err := staging.WriteSlice(diskID, 99, []byte("uncommitted slice data"))
	require.NoError(t, err)

	// Note: locator does NOT have slice 99
	report, err := RecoverStagingPipeline(ctx, staging, uploader, locator)
	require.NoError(t, err)
	require.NotNil(t, report)

	assert.Equal(t, 1, report.ScannedSlices)
	assert.Equal(t, 1, report.OrphanedSlices)
	assert.Equal(t, 0, report.RequeuedSlices)
	assert.Equal(t, 0, report.SkippedSlices)
}

func TestRecoverStagingPipeline_MultiBlock_PartialCrash(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stagingDir := filepath.Join(t.TempDir(), "recovery_staging_multiblock")
	staging := NewStagingManager(stagingDir)
	mockClient := newMockStorageClient()
	batcher := NewBlockBatcher(2 * 1024 * 1024) // 2MB block size for fast test
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithBatcher(batcher),
		WithWorkers(2),
	)
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-multiblock"
	// 3MB payload -> 2 blocks (2MB + 1MB)
	payload := bytes.Repeat([]byte("M"), 3*1024*1024)
	sliceID := uint64(50)

	err := staging.WriteSlice(diskID, sliceID, payload)
	require.NoError(t, err)

	// Simulate partial crash: block 0 is in S3, but block 1 is missing
	key0 := FormatObjectKey(diskID, 0, sliceID, 0)
	mockClient.mu.Lock()
	mockClient.objects[key0] = payload[:2*1024*1024]
	mockClient.mu.Unlock()

	locator := newMockLocator()
	locator.add(diskID, sliceID, 301, 0)

	report, err := RecoverStagingPipeline(ctx, staging, uploader, locator)
	require.NoError(t, err)
	require.NotNil(t, report)

	assert.Equal(t, 1, report.ScannedSlices)
	assert.Equal(t, 1, report.RequeuedSlices, "partial slice must be detected and requeued")
	assert.Equal(t, 0, report.SkippedSlices)

	err = uploader.WaitForAll(ctx)
	require.NoError(t, err)

	key1 := FormatObjectKey(diskID, 0, sliceID, 1)
	mockClient.mu.Lock()
	assert.Contains(t, mockClient.objects, key0)
	assert.Contains(t, mockClient.objects, key1)
	assert.Equal(t, payload[2*1024*1024:], mockClient.objects[key1])
	mockClient.mu.Unlock()
}
