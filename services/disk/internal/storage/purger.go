package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/starsrc/bowl/services/disk/internal/config"
)

// ObjectStoragePurger 定义虚拟磁盘底层物理数据清理接口。
type ObjectStoragePurger interface {
	// PurgeDisk 执行虚拟磁盘在底层物理存储（包含 S3/MinIO 对象存储与本地切片目录）中的完整级联清理。
	PurgeDisk(ctx context.Context, diskID uuid.UUID) error
	// PurgeDiskObjects 执行虚拟磁盘在底层对象存储（S3/MinIO）中的物理切片对象批量清理。
	PurgeDiskObjects(ctx context.Context, diskID uuid.UUID) error
}

// MinIOClient 抽象 MinIO/S3 批量对象列表与删除操作接口，便于单元测试 Mock。
type MinIOClient interface {
	ListObjects(ctx context.Context, bucket string, opts minio.ListObjectsOptions) <-chan minio.ObjectInfo
	RemoveObjects(ctx context.Context, bucket string, objectsCh <-chan minio.ObjectInfo, opts minio.RemoveObjectsOptions) <-chan minio.RemoveObjectError
}

// PurgerOption 定义 MinIOObjectStoragePurger 的功能选项。
type PurgerOption func(*MinIOObjectStoragePurger)

// WithMinIOClient 显式注入 MinIOClient（用于测试或共享客户端）。
func WithMinIOClient(client MinIOClient) PurgerOption {
	return func(p *MinIOObjectStoragePurger) {
		p.client = client
	}
}

// MinIOObjectStoragePurger 基于 MinIO/S3 客户端与本地文件系统实现级联物理清理驱动。
type MinIOObjectStoragePurger struct {
	client     MinIOClient
	s3Cfg      config.S3Config
	storageCfg config.StorageConfig
}

var _ ObjectStoragePurger = (*MinIOObjectStoragePurger)(nil)

// NewMinIOObjectStoragePurger 创建一个新的 MinIOObjectStoragePurger 实例。
func NewMinIOObjectStoragePurger(s3Cfg config.S3Config, storageCfg config.StorageConfig, opts ...PurgerOption) (*MinIOObjectStoragePurger, error) {
	p := &MinIOObjectStoragePurger{
		s3Cfg:      s3Cfg,
		storageCfg: storageCfg,
	}

	for _, opt := range opts {
		opt(p)
	}

	if s3Cfg.Enabled && p.client == nil {
		client, err := newMinIOClient(s3Cfg)
		if err != nil {
			return nil, fmt.Errorf("初始化 MinIO 客户端失败: %w", err)
		}
		p.client = client
	}

	return p, nil
}

// PurgeDisk 执行虚拟磁盘在对象存储与本地目录中的物理数据级联清理。
func (p *MinIOObjectStoragePurger) PurgeDisk(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return errors.New("diskID 不能为空")
	}

	// 1. 若启用了 S3 对象存储，批量清理 S3 物理对象
	if err := p.PurgeDiskObjects(ctx, diskID); err != nil {
		return err
	}

	// 2. 无论是否启用 S3，均清理本地 DATA_DIR/<disk_id> 与 STAGING_DIR/<disk_id> 目录
	if err := p.purgeLocalDirs(diskID); err != nil {
		return fmt.Errorf("清理本地目录失败: %w", err)
	}

	return nil
}

// PurgeDiskObjects 清理指定虚拟磁盘在 S3/MinIO 对象存储中的全部物理切片对象。
func (p *MinIOObjectStoragePurger) PurgeDiskObjects(ctx context.Context, diskID uuid.UUID) error {
	if diskID == uuid.Nil {
		return errors.New("diskID 不能为空")
	}

	if p.s3Cfg.Enabled && p.client != nil {
		if err := p.purgeS3Objects(ctx, diskID); err != nil {
			return fmt.Errorf("批量清理 S3 物理对象失败: %w", err)
		}
	}

	return nil
}

// purgeS3Objects 流式列出并批量删除 S3 中前缀为 chunks/<disk_id>/ 的全部物理对象。
func (p *MinIOObjectStoragePurger) purgeS3Objects(ctx context.Context, diskID uuid.UUID) error {
	prefix := fmt.Sprintf("chunks/%s/", diskID.String())
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	listCh := p.client.ListObjects(ctx, p.s3Cfg.Bucket, opts)
	toDeleteCh := make(chan minio.ObjectInfo)

	var (
		listErr   error
		listErrMu sync.Mutex
	)

	go func() {
		defer close(toDeleteCh)
		for obj := range listCh {
			if obj.Err != nil {
				listErrMu.Lock()
				if listErr == nil {
					listErr = obj.Err
				}
				listErrMu.Unlock()
				return
			}
			select {
			case <-ctx.Done():
				return
			case toDeleteCh <- obj:
			}
		}
	}()

	removeOpts := minio.RemoveObjectsOptions{}
	errCh := p.client.RemoveObjects(ctx, p.s3Cfg.Bucket, toDeleteCh, removeOpts)

	var removeErrs []error
	for rErr := range errCh {
		if rErr.Err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("删除对象 %s 失败: %w", rErr.ObjectName, rErr.Err))
		}
	}

	listErrMu.Lock()
	defer listErrMu.Unlock()
	if listErr != nil {
		return fmt.Errorf("列出对象失败 (prefix: %s): %w", prefix, listErr)
	}

	if len(removeErrs) > 0 {
		return errors.Join(removeErrs...)
	}

	return nil
}

// purgeLocalDirs 清理本地 DATA_DIR/<disk_id> 与 STAGING_DIR/<disk_id> 物理切片与暂存目录。
func (p *MinIOObjectStoragePurger) purgeLocalDirs(diskID uuid.UUID) error {
	diskIDStr := diskID.String()

	if p.storageCfg.DataDir != "" {
		dataDir := filepath.Join(p.storageCfg.DataDir, diskIDStr)
		if err := os.RemoveAll(dataDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("清理 DATA_DIR 目录 %s 失败: %w", dataDir, err)
		}
	}

	if p.storageCfg.StagingDir != "" {
		stagingDir := filepath.Join(p.storageCfg.StagingDir, diskIDStr)
		if err := os.RemoveAll(stagingDir); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("清理 STAGING_DIR 目录 %s 失败: %w", stagingDir, err)
		}
	}

	return nil
}

// newMinIOClient 根据 S3Config 初始化原生的 MinIO 客户端。
func newMinIOClient(cfg config.S3Config) (*minio.Client, error) {
	endpoint := cfg.Endpoint
	u, err := url.Parse(endpoint)
	var host string
	var secure bool
	if err == nil && u.Host != "" {
		host = u.Host
		secure = u.Scheme == "https" || cfg.UseSSL
	} else {
		host = strings.TrimPrefix(endpoint, "http://")
		host = strings.TrimPrefix(host, "https://")
		secure = cfg.UseSSL
	}

	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: secure,
		Region: cfg.Region,
	}
	if cfg.ForcePathStyle {
		opts.BucketLookup = minio.BucketLookupPath
	}

	return minio.New(host, opts)
}
