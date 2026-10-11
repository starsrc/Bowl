package chunk_test

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/chunk"
)

type mockSliceRef struct {
	id        uint64
	chunkIndx uint32
	size      uint32
	refs      int32
}

type mockGCDB struct {
	mu   sync.Mutex
	rows map[uint64]*mockSliceRef
}

func newMockGCDB() *mockGCDB {
	return &mockGCDB{
		rows: make(map[uint64]*mockSliceRef),
	}
}

type mockGCRows struct {
	pgx.Rows
	items []*mockSliceRef
	idx   int
}

func (r *mockGCRows) Close() {}
func (r *mockGCRows) Err() error { return nil }
func (r *mockGCRows) Next() bool {
	if r.idx < len(r.items) {
		r.idx++
		return true
	}
	return false
}
func (r *mockGCRows) Scan(dest ...any) error {
	item := r.items[r.idx-1]
	*dest[0].(*int64) = int64(item.id)
	*dest[1].(*int32) = int32(item.chunkIndx)
	*dest[2].(*int32) = int32(item.size)
	return nil
}

func (db *mockGCDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	var expired []*mockSliceRef
	for _, r := range db.rows {
		if r.refs <= 0 {
			expired = append(expired, r)
		}
	}
	return &mockGCRows{items: expired}, nil
}

func (db *mockGCDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if len(args) > 0 {
		id := uint64(args[0].(int64))
		delete(db.rows, id)
	}
	return pgconn.NewCommandTag("DELETE 1"), nil
}

func (db *mockGCDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

type mockGCObjectStorageClient struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockGCObjectStorageClient() *mockGCObjectStorageClient {
	return &mockGCObjectStorageClient{
		objects: make(map[string][]byte),
	}
}

func (m *mockGCObjectStorageClient) PutObject(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	data, _ := io.ReadAll(reader)
	m.mu.Lock()
	m.objects[objectKey] = data
	m.mu.Unlock()
	return minio.UploadInfo{Bucket: bucket, Key: objectKey, Size: int64(len(data))}, nil
}

func (m *mockGCObjectStorageClient) StatObject(ctx context.Context, bucket, objectKey string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[objectKey]
	if !ok {
		return minio.ObjectInfo{}, minio.ErrorResponse{Code: "NoSuchKey"}
	}
	return minio.ObjectInfo{Key: objectKey, Size: int64(len(data))}, nil
}

func (m *mockGCObjectStorageClient) GetObject(ctx context.Context, bucket, objectKey string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return nil, nil
}

func (m *mockGCObjectStorageClient) RemoveObject(ctx context.Context, bucket, objectKey string, opts minio.RemoveObjectOptions) error {
	m.mu.Lock()
	delete(m.objects, objectKey)
	m.mu.Unlock()
	return nil
}

func (m *mockGCObjectStorageClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return true, nil
}

func (m *mockGCObjectStorageClient) MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error {
	return nil
}

func TestGarbageCollector_RunGC_CleansStagingAndS3Blocks_AndDeletesRow(t *testing.T) {
	db := newMockGCDB()
	staging := chunk.NewStagingManager(t.TempDir())
	mockS3 := newMockGCObjectStorageClient()
	bucket := "test-bucket"
	diskID := "d4b4a11b-0000-0000-0000-000000000001"

	gc := chunk.NewGarbageCollector(db, staging,
		chunk.WithGCClient(mockS3, bucket),
	)

	// Slice 1: refs = 0, size = 4MB + 1KB (2 blocks: block 0 and block 1)
	slice1ID := uint64(101)
	slice1Size := uint32(chunk.BlockSize + 1024)
	db.rows[slice1ID] = &mockSliceRef{id: slice1ID, chunkIndx: 0, size: slice1Size, refs: 0}

	// Slice 2: refs = 1 (active, should NOT be collected!)
	slice2ID := uint64(102)
	db.rows[slice2ID] = &mockSliceRef{id: slice2ID, chunkIndx: 0, size: 4096, refs: 1}

	// Create local staging file for Slice 1
	err := staging.WriteSlice(diskID, slice1ID, []byte("staged-data-to-delete"))
	require.NoError(t, err)

	// Create S3 blocks for Slice 1 in mockS3
	key0 := chunk.FormatObjectKey(diskID, 0, slice1ID, 0)
	key1 := chunk.FormatObjectKey(diskID, 0, slice1ID, 1)
	mockS3.objects[key0] = []byte("block0")
	mockS3.objects[key1] = []byte("block1")

	// Run GC
	n, err := gc.RunGC(context.Background(), diskID)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "1 expired slice must be collected")

	// 1. Assert local staging file is deleted
	_, err = os.Stat(staging.SlicePath(diskID, slice1ID))
	assert.True(t, os.IsNotExist(err), "local staging file must be removed by GC")

	// 2. Assert S3 objects are removed
	_, ok0 := mockS3.objects[key0]
	assert.False(t, ok0, "S3 block 0 must be removed by GC")
	_, ok1 := mockS3.objects[key1]
	assert.False(t, ok1, "S3 block 1 must be removed by GC")

	// 3. Assert slice 1 row is deleted from database
	_, exists1 := db.rows[slice1ID]
	assert.False(t, exists1, "slice 1 row must be deleted from db")

	// 4. Assert slice 2 is untouched
	_, exists2 := db.rows[slice2ID]
	assert.True(t, exists2, "slice 2 must remain in db")
}

func TestGarbageCollector_PeriodicStartStop(t *testing.T) {
	db := newMockGCDB()
	staging := chunk.NewStagingManager(t.TempDir())
	diskID := "d4b4a11b-0000-0000-0000-000000000002"

	gc := chunk.NewGarbageCollector(db, staging,
		chunk.WithGCInterval(20*time.Millisecond),
	)
	gc.RegisterDisk(diskID)

	// Add an expired slice
	db.rows[201] = &mockSliceRef{id: 201, chunkIndx: 1, size: 4096, refs: 0}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gc.Start(ctx)

	// Wait for periodic GC to clean up slice 201
	assert.Eventually(t, func() bool {
		db.mu.Lock()
		defer db.mu.Unlock()
		_, exists := db.rows[201]
		return !exists
	}, 1*time.Second, 10*time.Millisecond)

	gc.Stop()
}
