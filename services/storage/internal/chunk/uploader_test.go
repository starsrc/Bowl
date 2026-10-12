package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/object"
)

// mockStorageClient implements ObjectStorageClient for white-box transient error testing.
type mockStorageClient struct {
	mu           sync.Mutex
	putCalls     atomic.Int32
	putFunc      func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error)
	statCalls    atomic.Int32
	objects      map[string][]byte
	bucketExists bool
}

func newMockStorageClient() *mockStorageClient {
	return &mockStorageClient{
		objects:      make(map[string][]byte),
		bucketExists: true,
	}
}

func (m *mockStorageClient) PutObject(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	m.putCalls.Add(1)
	if m.putFunc != nil {
		return m.putFunc(ctx, bucket, objectKey, reader, objectSize, opts)
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		return minio.UploadInfo{}, err
	}
	m.mu.Lock()
	m.objects[objectKey] = data
	m.mu.Unlock()

	return minio.UploadInfo{
		Bucket: bucket,
		Key:    objectKey,
		Size:   int64(len(data)),
	}, nil
}

func (m *mockStorageClient) StatObject(ctx context.Context, bucket, objectKey string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	m.statCalls.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()

	data, ok := m.objects[objectKey]
	if !ok {
		return minio.ObjectInfo{}, minio.ErrorResponse{
			StatusCode: 404,
			Code:       "NoSuchKey",
			Message:    "The specified key does not exist.",
		}
	}

	return minio.ObjectInfo{
		Key:  objectKey,
		Size: int64(len(data)),
	}, nil

}

func (m *mockStorageClient) GetObject(ctx context.Context, bucket, objectKey string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return nil, errors.New("not implemented in mock")
}

func (m *mockStorageClient) RemoveObject(ctx context.Context, bucket, objectKey string, opts minio.RemoveObjectOptions) error {
	m.mu.Lock()
	delete(m.objects, objectKey)
	m.mu.Unlock()
	return nil
}

func (m *mockStorageClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	return m.bucketExists, nil
}

func (m *mockStorageClient) MakeBucket(ctx context.Context, bucket string, opts minio.MakeBucketOptions) error {
	m.bucketExists = true
	return nil
}

type netTimeoutError struct{}

func (e *netTimeoutError) Error() string   { return "i/o timeout" }
func (e *netTimeoutError) Timeout() bool   { return true }
func (e *netTimeoutError) Temporary() bool { return true }

var _ net.Error = (*netTimeoutError)(nil)

func TestS3Uploader_UploadBlock_Success(t *testing.T) {
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithMaxRetries(3),
		WithInitialBackoff(5*time.Millisecond),
	)

	block := Block{
		DiskID:    "disk-1",
		ChunkIndx: 0,
		SliceID:   10,
		BlockIndx: 0,
		Data:      []byte("hello 4MB block payload"),
		Key:       FormatObjectKey("disk-1", 0, 10, 0),
	}

	err := uploader.UploadBlock(context.Background(), block)
	require.NoError(t, err)

	assert.Equal(t, int32(1), mockClient.putCalls.Load())
	mockClient.mu.Lock()
	assert.Equal(t, []byte("hello 4MB block payload"), mockClient.objects[block.Key])
	mockClient.mu.Unlock()
}

func TestS3Uploader_UploadBlock_TransientRetry(t *testing.T) {
	mockClient := newMockStorageClient()
	var callCount atomic.Int32

	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		current := callCount.Add(1)
		if current < 3 {
			// Simulate transient 503 error
			return minio.UploadInfo{}, minio.ErrorResponse{
				StatusCode: 503,
				Code:       "ServiceUnavailable",
				Message:    "Slow down or retry",
			}
		}
		data, _ := io.ReadAll(reader)
		mockClient.mu.Lock()
		mockClient.objects[objectKey] = data
		mockClient.mu.Unlock()
		return minio.UploadInfo{Bucket: bucket, Key: objectKey, Size: objectSize}, nil
	}

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithMaxRetries(4),
		WithInitialBackoff(5*time.Millisecond),
	)

	block := Block{
		DiskID:    "disk-retry",
		ChunkIndx: 0,
		SliceID:   1,
		BlockIndx: 0,
		Data:      []byte("transient retry data"),
		Key:       FormatObjectKey("disk-retry", 0, 1, 0),
	}

	err := uploader.UploadBlock(context.Background(), block)
	require.NoError(t, err)
	assert.Equal(t, int32(3), callCount.Load(), "should succeed on 3rd attempt after 2 transient failures")
}

func TestS3Uploader_UploadBlock_PermanentErrorNoRetry(t *testing.T) {
	mockClient := newMockStorageClient()
	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		return minio.UploadInfo{}, minio.ErrorResponse{
			StatusCode: 403,
			Code:       "AccessDenied",
			Message:    "Access Denied",
		}
	}

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithMaxRetries(5),
		WithInitialBackoff(5*time.Millisecond),
	)

	block := Block{
		DiskID:    "disk-denied",
		ChunkIndx: 0,
		SliceID:   1,
		BlockIndx: 0,
		Data:      []byte("data"),
		Key:       FormatObjectKey("disk-denied", 0, 1, 0),
	}

	err := uploader.UploadBlock(context.Background(), block)
	require.Error(t, err)
	assert.Equal(t, int32(1), mockClient.putCalls.Load(), "permanent error should fail fast without retrying")
}

func TestS3Uploader_UploadBlock_MaxRetriesExceeded(t *testing.T) {
	mockClient := newMockStorageClient()
	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		return minio.UploadInfo{}, &netTimeoutError{}
	}

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithMaxRetries(2), // 1 initial + 2 retries = 3 attempts total
		WithInitialBackoff(5*time.Millisecond),
	)

	block := Block{
		DiskID:    "disk-timeout",
		ChunkIndx: 0,
		SliceID:   1,
		BlockIndx: 0,
		Data:      []byte("timeout"),
		Key:       FormatObjectKey("disk-timeout", 0, 1, 0),
	}

	err := uploader.UploadBlock(context.Background(), block)
	require.Error(t, err)
	assert.Equal(t, int32(3), mockClient.putCalls.Load(), "should fail after initial + 2 retries = 3 calls")
}

func TestS3Uploader_UploadBlock_ContextCanceled(t *testing.T) {
	mockClient := newMockStorageClient()
	uploader := NewS3UploaderWithClient(mockClient, "test-bucket")

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	block := Block{
		DiskID: "disk-cancel",
		Key:    FormatObjectKey("disk-cancel", 0, 1, 0),
		Data:   []byte("canceled"),
	}

	err := uploader.UploadBlock(ctx, block)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, int32(0), mockClient.putCalls.Load())
}

func TestS3Uploader_BackgroundWorker_UploadAndStatus(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(2),
		WithInitialBackoff(5*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-worker-test"
	sliceID := staging.NextSliceID()
	data := []byte("staged data to be picked up by background worker")

	err := staging.WriteSlice(diskID, sliceID, data)
	require.NoError(t, err)

	task := &UploadTask{
		DiskID:    diskID,
		Ino:       101,
		ChunkIndx: 0,
		SliceID:   sliceID,
	}

	err = uploader.Enqueue(ctx, task)
	require.NoError(t, err)

	// Wait for slice to complete
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID, sliceID)
	require.NoError(t, err)

	// Status must be Synced
	status := uploader.GetSliceStatus(diskID, sliceID)
	assert.Equal(t, SliceStatusSynced, status)

	// Verify object exists in storage
	expectedKey := FormatObjectKey(diskID, 0, sliceID, 0)
	mockClient.mu.Lock()
	assert.Equal(t, data, mockClient.objects[expectedKey])
	mockClient.mu.Unlock()
}

func TestS3Uploader_BackgroundWorker_MultipleBlocksPerSlice(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(3),
		WithInitialBackoff(5*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-multi-block"
	sliceID := staging.NextSliceID()
	// 9MB payload = 4MB + 4MB + 1MB
	totalSize := 9 * 1024 * 1024
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 127)
	}

	err := staging.WriteSlice(diskID, sliceID, data)
	require.NoError(t, err)

	task := &UploadTask{
		DiskID:    diskID,
		Ino:       202,
		ChunkIndx: 1, // chunkIndx = 1 -> prefix 000
		SliceID:   sliceID,
	}

	err = uploader.Enqueue(ctx, task)
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID, sliceID)
	require.NoError(t, err)

	assert.Equal(t, SliceStatusSynced, uploader.GetSliceStatus(diskID, sliceID))

	// Verify all 3 blocks exist
	mockClient.mu.Lock()
	defer mockClient.mu.Unlock()

	key0 := FormatObjectKey(diskID, 1, sliceID, 0)
	key1 := FormatObjectKey(diskID, 1, sliceID, 1)
	key2 := FormatObjectKey(diskID, 1, sliceID, 2)

	assert.Equal(t, data[:BlockSize], mockClient.objects[key0])
	assert.Equal(t, data[BlockSize:2*BlockSize], mockClient.objects[key1])
	assert.Equal(t, data[2*BlockSize:], mockClient.objects[key2])
}

func TestS3Uploader_WaitForIno(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(4),
		WithInitialBackoff(5*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-ino-test"
	ino := uint64(555)

	// Enqueue 3 slices for the same Inode
	for i := 0; i < 3; i++ {
		sID := staging.NextSliceID()
		d := []byte(fmt.Sprintf("slice data %d for ino %d", i, ino))
		err := staging.WriteSlice(diskID, sID, d)
		require.NoError(t, err)

		err = uploader.Enqueue(ctx, &UploadTask{
			DiskID:    diskID,
			Ino:       ino,
			ChunkIndx: 0,
			SliceID:   sID,
		})
		require.NoError(t, err)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	err := uploader.WaitForIno(waitCtx, diskID, ino)
	require.NoError(t, err)
}

func TestS3Uploader_BackgroundWorker_FailureStatus(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()
	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		return minio.UploadInfo{}, errors.New("unrecoverable disk failure")
	}

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(1),
		WithMaxRetries(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-fail"
	sliceID := staging.NextSliceID()
	_ = staging.WriteSlice(diskID, sliceID, []byte("fail data"))

	err := uploader.Enqueue(ctx, &UploadTask{
		DiskID:    diskID,
		Ino:       99,
		ChunkIndx: 0,
		SliceID:   sliceID,
	})
	require.NoError(t, err)

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer waitCancel()
	err = uploader.WaitForSlice(waitCtx, diskID, sliceID)
	require.Error(t, err)

	assert.Equal(t, SliceStatusFailed, uploader.GetSliceStatus(diskID, sliceID))
}

func TestS3Uploader_Barrier_Channel_SuccessAndConcurrent(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(4),
		WithInitialBackoff(5*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-barrier-test"
	ino := uint64(777)

	// 1. 入队 3 个在途切片推流任务
	for i := 0; i < 3; i++ {
		sID := staging.NextSliceID()
		d := []byte(fmt.Sprintf("barrier slice %d for ino %d", i, ino))
		err := staging.WriteSlice(diskID, sID, d)
		require.NoError(t, err)

		err = uploader.Enqueue(ctx, &UploadTask{
			DiskID:    diskID,
			Ino:       ino,
			ChunkIndx: 0,
			SliceID:   sID,
		})
		require.NoError(t, err)
	}

	// 2. 同时创建两个针对该 Inode 的持久化同步屏障信道 (模拟并发 RPC 调用)
	barrierCh1 := uploader.Barrier(diskID, ino)
	barrierCh2 := uploader.Barrier(diskID, ino)

	// 3. 并发等待两个屏障信道达成
	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)

	go func() {
		defer wg.Done()
		err1 = <-barrierCh1
	}()
	go func() {
		defer wg.Done()
		err2 = <-barrierCh2
	}()

	wg.Wait()
	require.NoError(t, err1, "屏障 1 必须成功")
	require.NoError(t, err2, "屏障 2 必须成功")

	// 4. 再次调用 Barrier 时应立即返回 nil
	immediateCh := uploader.Barrier(diskID, ino)
	select {
	case err := <-immediateCh:
		require.NoError(t, err)
	case <-time.After(50 * time.Millisecond):
		t.Fatal("无在途切片时 Barrier 必须非阻塞立即返回")
	}
}

func TestS3Uploader_Barrier_Channel_ErrorPropagation_Concurrent(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()
	mockClient.putFunc = func(ctx context.Context, bucket, objectKey string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
		return minio.UploadInfo{}, errors.New("s3 fatal error: simulated disk crash")
	}

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithWorkers(2),
		WithMaxRetries(0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	uploader.Start(ctx)
	defer uploader.Stop()

	diskID := "disk-barrier-err"
	ino := uint64(888)

	sID := staging.NextSliceID()
	_ = staging.WriteSlice(diskID, sID, []byte("fatal payload"))

	err := uploader.Enqueue(ctx, &UploadTask{
		DiskID:    diskID,
		Ino:       ino,
		ChunkIndx: 0,
		SliceID:   sID,
	})
	require.NoError(t, err)

	barrierCh1 := uploader.Barrier(diskID, ino)
	barrierCh2 := uploader.Barrier(diskID, ino)

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)

	go func() {
		defer wg.Done()
		err1 = <-barrierCh1
	}()
	go func() {
		defer wg.Done()
		err2 = <-barrierCh2
	}()

	wg.Wait()
	require.Error(t, err1, "并发屏障 1 必须收到不可恢复错误")
	require.Error(t, err2, "并发屏障 2 必须收到不可恢复错误")
	assert.Contains(t, err1.Error(), "fatal error")
	assert.Contains(t, err2.Error(), "fatal error")
}

func TestS3Uploader_InlinedJuiceFS_ObjectStorage(t *testing.T) {
	mockClient := newMockStorageClient()
	objStore := object.NewMemStorage()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithObjectStorage(objStore),
		WithMaxRetries(1),
	)

	assert.NotNil(t, uploader.ObjectStorage(), "ObjectStorage 必须可读取")
	assert.Equal(t, objStore, uploader.ObjectStorage())

	block := Block{
		DiskID:    "disk-jfs",
		ChunkIndx: 0,
		SliceID:   123,
		BlockIndx: 0,
		Data:      []byte("juicefs inlined data payload"),
		Key:       FormatObjectKey("disk-jfs", 0, 123, 0),
	}

	err := uploader.UploadBlock(context.Background(), block)
	require.NoError(t, err)

	// 校验数据已通过 JuiceFS ObjectStorage 上传
	head, err := objStore.Head(context.Background(), block.Key)
	require.NoError(t, err)
	assert.Equal(t, int64(len(block.Data)), head.Size())
}

func TestS3Uploader_Enqueue_ContextCanceled_NoDeadlock(t *testing.T) {
	tempDir := t.TempDir()
	staging := NewStagingManager(tempDir)
	mockClient := newMockStorageClient()

	uploader := NewS3UploaderWithClient(mockClient, "test-bucket",
		WithStaging(staging),
		WithQueueSize(1), // 极小队列
		WithWorkers(0),   // 无 worker 消费，队列将瞬间填满
	)

	ctx := context.Background()
	task1 := &UploadTask{DiskID: "disk-dl", Ino: 999, SliceID: 1}
	err := uploader.Enqueue(ctx, task1)
	require.NoError(t, err)

	// 队列已满，传入已取消的上下文入队 task2
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	task2 := &UploadTask{DiskID: "disk-dl", Ino: 999, SliceID: 2}
	err = uploader.Enqueue(cancelCtx, task2)
	require.Error(t, err)

	// 验证对于 task2，其在途状态必须被清理，不能导致 Barrier 永久死锁
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer waitCancel()

	// 对该 Inode 发起 WaitForIno，等待应当因 task2 被标记失败或从在途移除而正常返回，不应挂起死锁
	barrierCh := uploader.Barrier("disk-dl", 999)
	select {
	case <-barrierCh:
		// 正常返回
	case <-waitCtx.Done():
		// task1 仍然在队列中未被消费，但 task2 不能造成死锁，如果我们在 waitCtx 超时前检查 task2 的状态：
		assert.Equal(t, SliceStatusFailed, uploader.GetSliceStatus("disk-dl", 2))
	}
}


