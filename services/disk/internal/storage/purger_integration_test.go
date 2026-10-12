package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/config"
	"github.com/starsrc/bowl/services/disk/internal/storage"
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

// TestMinIOObjectStoragePurger_Integration_LivePrefixErasure 验证在真实 MinIO 环境下的批量前缀擦除与本地目录级联清理
func TestMinIOObjectStoragePurger_Integration_LivePrefixErasure(t *testing.T) {
	minioEndpoint := "http://127.0.0.1:9000"
	if !isMinIOAvailable(minioEndpoint) {
		t.Skip("MinIO 未在 127.0.0.1:9000 运行，跳过真实集成测试")
	}

	bucketName := "bowl-data"
	s3Cfg := config.S3Config{
		Enabled:        true,
		Endpoint:       minioEndpoint,
		Bucket:         bucketName,
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin",
		Region:         "us-east-1",
		UseSSL:         false,
		ForcePathStyle: true,
	}

	ctx := context.Background()
	rawMinioClient, err := minio.New("127.0.0.1:9000", &minio.Options{
		Creds:  credentials.NewStaticV4(s3Cfg.AccessKey, s3Cfg.SecretKey, ""),
		Secure: false,
	})
	require.NoError(t, err)

	exists, err := rawMinioClient.BucketExists(ctx, bucketName)
	require.NoError(t, err)
	if !exists {
		err = rawMinioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		require.NoError(t, err)
	}

	diskID := uuid.New()
	otherDiskID := uuid.New()

	targetKeys := []string{
		fmt.Sprintf("chunks/%s/000/0/1_0", diskID),
		fmt.Sprintf("chunks/%s/000/0/1_1", diskID),
		fmt.Sprintf("chunks/%s/000/1/2_0", diskID),
		fmt.Sprintf("chunks/%s/001/1000/3_0", diskID),
	}
	otherKeys := []string{
		fmt.Sprintf("chunks/%s/000/0/99_0", otherDiskID),
	}

	// 1. 上传测试对象至 MinIO
	for _, key := range targetKeys {
		payload := []byte("dummy payload for " + key)
		_, err = rawMinioClient.PutObject(ctx, bucketName, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		require.NoError(t, err)
	}
	for _, key := range otherKeys {
		payload := []byte("dummy other payload for " + key)
		_, err = rawMinioClient.PutObject(ctx, bucketName, key, bytes.NewReader(payload), int64(len(payload)), minio.PutObjectOptions{
			ContentType: "application/octet-stream",
		})
		require.NoError(t, err)
	}

	// 2. 创建本地 DATA_DIR 与 STAGING_DIR 目录及文件
	tempDataDir := t.TempDir()
	tempStagingDir := t.TempDir()

	diskDataDir := filepath.Join(tempDataDir, diskID.String())
	diskStagingDir := filepath.Join(tempStagingDir, diskID.String())

	require.NoError(t, os.MkdirAll(diskDataDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(diskDataDir, "slice_0"), []byte("local slice"), 0o644))

	require.NoError(t, os.MkdirAll(diskStagingDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(diskStagingDir, "staged_0"), []byte("staged slice"), 0o644))

	storageCfg := config.StorageConfig{
		DataDir:    tempDataDir,
		StagingDir: tempStagingDir,
	}

	// 3. 执行物理清理
	purger, err := storage.NewMinIOObjectStoragePurger(s3Cfg, storageCfg)
	require.NoError(t, err)

	err = purger.PurgeDisk(ctx, diskID)
	require.NoError(t, err)

	// 4. 验证 S3 对象已被批量擦除
	for _, key := range targetKeys {
		_, err = rawMinioClient.StatObject(ctx, bucketName, key, minio.StatObjectOptions{})
		require.Error(t, err, "已清理的目标对象不应存在: %s", key)
	}

	// 验证其它磁盘的对象未被误删
	for _, key := range otherKeys {
		info, err := rawMinioClient.StatObject(ctx, bucketName, key, minio.StatObjectOptions{})
		require.NoError(t, err, "非目标磁盘对象必须保留")
		assert.Equal(t, key, info.Key)
	}

	// 5. 验证本地目录已被删除
	_, err = os.Stat(diskDataDir)
	assert.True(t, os.IsNotExist(err), "DATA_DIR 磁盘子目录应被完全清除")

	_, err = os.Stat(diskStagingDir)
	assert.True(t, os.IsNotExist(err), "STAGING_DIR 磁盘子目录应被完全清除")

	// 6. 验证空前缀幂等性：再次对同一个 diskID 调用 PurgeDisk 不报错
	err = purger.PurgeDisk(ctx, diskID)
	assert.NoError(t, err, "二次清理空前缀必须保持幂等")

	// 清理测试用的 otherDiskID 对象
	for _, key := range otherKeys {
		_ = rawMinioClient.RemoveObject(ctx, bucketName, key, minio.RemoveObjectOptions{})
	}
}
