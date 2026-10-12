package storage_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/config"
	"github.com/starsrc/bowl/services/disk/internal/storage"
)

// mockMinIOClient 用于单元测试的 MinIO 客户端 Mock 实现。
type mockMinIOClient struct {
	mu             sync.Mutex
	listObjectsFn  func(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	removeObjectsFn func(ctx context.Context, bucket string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
	listedPrefixes []string
	removedObjects []string
}

func (m *mockMinIOClient) ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
	m.mu.Lock()
	m.listedPrefixes = append(m.listedPrefixes, opts.Prefix)
	fn := m.listObjectsFn
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, bucket, opts)
	}

	ch := make(chan minio.ObjectInfo)
	close(ch)
	return ch
}

func (m *mockMinIOClient) RemoveObjects(ctx context.Context, bucket string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
	m.mu.Lock()
	fn := m.removeObjectsFn
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, bucket, objectsCh, opts)
	}

	errCh := make(chan minio.RemoveObjectError, 1)
	go func() {
		defer close(errCh)
		for obj := range objectsCh {
			m.mu.Lock()
			m.removedObjects = append(m.removedObjects, obj.Key)
			m.mu.Unlock()
		}
	}()
	return errCh
}

// TestMinIOObjectStoragePurger_DisabledS3_FallbackLocalDirs 验证未启用 S3 时回退到仅清理本地目录
func TestMinIOObjectStoragePurger_DisabledS3_FallbackLocalDirs(t *testing.T) {
	tempDataDir := t.TempDir()
	tempStagingDir := t.TempDir()
	diskID := uuid.New()

	diskDataDir := filepath.Join(tempDataDir, diskID.String())
	diskStagingDir := filepath.Join(tempStagingDir, diskID.String())

	require.NoError(t, os.MkdirAll(diskDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(diskDataDir, "block-001"), []byte("data"), 0o644))

	require.NoError(t, os.MkdirAll(diskStagingDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(diskStagingDir, "slice-100"), []byte("staging"), 0o644))

	s3Cfg := config.S3Config{
		Enabled: false,
	}
	storageCfg := config.StorageConfig{
		DataDir:    tempDataDir,
		StagingDir: tempStagingDir,
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg)
	require.NoError(t, err)

	ctx := context.Background()
	err = purger.PurgeDisk(ctx, diskID)
	require.NoError(t, err)

	// 验证本地磁盘子目录均已被删除
	_, err = os.Stat(diskDataDir)
	assert.True(t, os.IsNotExist(err), "DATA_DIR 下的磁盘目录必须被删除")

	_, err = os.Stat(diskStagingDir)
	assert.True(t, os.IsNotExist(err), "STAGING_DIR 下的磁盘目录必须被删除")

	// 验证父目录仍完好无损
	_, err = os.Stat(tempDataDir)
	assert.NoError(t, err)
	_, err = os.Stat(tempStagingDir)
	assert.NoError(t, err)
}

// TestMinIOObjectStoragePurger_NilDiskID 验证传入空的 uuid.Nil 时直接拦截报错
func TestMinIOObjectStoragePurger_NilDiskID(t *testing.T) {
	s3Cfg := config.S3Config{Enabled: false}
	storageCfg := config.StorageConfig{
		DataDir:    t.TempDir(),
		StagingDir: t.TempDir(),
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg)
	require.NoError(t, err)

	err = purger.PurgeDisk(context.Background(), uuid.Nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "diskID")
}

// TestMinIOObjectStoragePurger_MockStreamingBatchDeletion 验证通过流式通道批量删除 S3 对象
func TestMinIOObjectStoragePurger_MockStreamingBatchDeletion(t *testing.T) {
	diskID := uuid.New()
	expectedPrefix := "chunks/" + diskID.String() + "/"

	dummyKeys := []string{
		expectedPrefix + "000/0/1_0",
		expectedPrefix + "000/0/1_1",
		expectedPrefix + "000/1/2_0",
	}

	mockClient := &mockMinIOClient{
		listObjectsFn: func(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, len(dummyKeys))
			for _, k := range dummyKeys {
				ch <- minio.ObjectInfo{Key: k}
			}
			close(ch)
			return ch
		},
	}

	tempDataDir := t.TempDir()
	tempStagingDir := t.TempDir()
	s3Cfg := config.S3Config{
		Enabled:   true,
		Bucket:    "bowl-data",
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "test",
		SecretKey: "test",
	}
	storageCfg := config.StorageConfig{
		DataDir:    tempDataDir,
		StagingDir: tempStagingDir,
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg, storage.WithMinIOClient(mockClient))
	require.NoError(t, err)

	ctx := context.Background()
	err = purger.PurgeDisk(ctx, diskID)
	require.NoError(t, err)

	mockClient.mu.Lock()
	defer mockClient.mu.Unlock()
	require.Len(t, mockClient.listedPrefixes, 1)
	assert.Equal(t, expectedPrefix, mockClient.listedPrefixes[0])
	assert.ElementsMatch(t, dummyKeys, mockClient.removedObjects)
}

// TestMinIOObjectStoragePurger_EmptyPrefixIdempotency_Mock 验证 S3 中无任何对象时的幂等清理
func TestMinIOObjectStoragePurger_EmptyPrefixIdempotency_Mock(t *testing.T) {
	diskID := uuid.New()
	mockClient := &mockMinIOClient{
		listObjectsFn: func(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo)
			close(ch)
			return ch
		},
	}

	s3Cfg := config.S3Config{
		Enabled:   true,
		Bucket:    "bowl-data",
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "test",
		SecretKey: "test",
	}
	storageCfg := config.StorageConfig{
		DataDir:    t.TempDir(),
		StagingDir: t.TempDir(),
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg, storage.WithMinIOClient(mockClient))
	require.NoError(t, err)

	err = purger.PurgeDisk(context.Background(), diskID)
	require.NoError(t, err)

	mockClient.mu.Lock()
	defer mockClient.mu.Unlock()
	assert.Empty(t, mockClient.removedObjects)
}

// TestMinIOObjectStoragePurger_ListError 验证 ListObjects 返回错误时的异常传播
func TestMinIOObjectStoragePurger_ListError(t *testing.T) {
	diskID := uuid.New()
	mockClient := &mockMinIOClient{
		listObjectsFn: func(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, 1)
			ch <- minio.ObjectInfo{
				Err: errors.New("network timeout listing objects"),
			}
			close(ch)
			return ch
		},
	}

	s3Cfg := config.S3Config{
		Enabled:   true,
		Bucket:    "bowl-data",
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "test",
		SecretKey: "test",
	}
	storageCfg := config.StorageConfig{
		DataDir:    t.TempDir(),
		StagingDir: t.TempDir(),
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg, storage.WithMinIOClient(mockClient))
	require.NoError(t, err)

	err = purger.PurgeDisk(context.Background(), diskID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "network timeout listing objects")
}

// TestMinIOObjectStoragePurger_RemoveError 验证 RemoveObjects 返回错误时的异常聚合
func TestMinIOObjectStoragePurger_RemoveError(t *testing.T) {
	diskID := uuid.New()
	key := "chunks/" + diskID.String() + "/000/0/1_0"

	mockClient := &mockMinIOClient{
		listObjectsFn: func(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo {
			ch := make(chan minio.ObjectInfo, 1)
			ch <- minio.ObjectInfo{Key: key}
			close(ch)
			return ch
		},
		removeObjectsFn: func(ctx context.Context, bucket string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError {
			errCh := make(chan minio.RemoveObjectError, 1)
			go func() {
				defer close(errCh)
				for range objectsCh {
					errCh <- minio.RemoveObjectError{
						ObjectName: key,
						Err:        errors.New("permission denied"),
					}
				}
			}()
			return errCh
		},
	}

	s3Cfg := config.S3Config{
		Enabled:   true,
		Bucket:    "bowl-data",
		Endpoint:  "http://127.0.0.1:9000",
		AccessKey: "test",
		SecretKey: "test",
	}
	storageCfg := config.StorageConfig{
		DataDir:    t.TempDir(),
		StagingDir: t.TempDir(),
	}

	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg, storage.WithMinIOClient(mockClient))
	require.NoError(t, err)

	err = purger.PurgeDisk(context.Background(), diskID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "permission denied")
}
