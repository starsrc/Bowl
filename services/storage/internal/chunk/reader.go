package chunk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

// ReaderOption 配置 S3Reader 实例的可选函数
type ReaderOption func(*S3Reader)

// WithReaderMaxRetries 设置针对瞬时错误的最大重试次数
func WithReaderMaxRetries(n int) ReaderOption {
	return func(r *S3Reader) {
		r.maxRetries = n
	}
}

// WithReaderInitialBackoff 设置首次重试前的初始退避时间
func WithReaderInitialBackoff(d time.Duration) ReaderOption {
	return func(r *S3Reader) {
		r.initialBackoff = d
	}
}

// WithReaderMaxBackoff 设置重试之间的最大退避等待上限
func WithReaderMaxBackoff(d time.Duration) ReaderOption {
	return func(r *S3Reader) {
		r.maxBackoff = d
	}
}

// WithReaderBackoffFactor 设置指数退避乘数因子
func WithReaderBackoffFactor(f float64) ReaderOption {
	return func(r *S3Reader) {
		r.backoffFactor = f
	}
}

// WithReaderBatcher 配置分块切分批处理器 BlockBatcher
func WithReaderBatcher(b *BlockBatcher) ReaderOption {
	return func(r *S3Reader) {
		r.batcher = b
	}
}

// S3Reader 提供切片数据的透明穿透回源读取：
// 优先命中本地 NVMe Staging 缓存，本地缺失时透明回源至 S3/MinIO。
type S3Reader struct {
	client         ObjectStorageClient
	bucket         string
	staging        *StagingManager
	batcher        *BlockBatcher
	maxRetries     int
	initialBackoff time.Duration
	maxBackoff     time.Duration
	backoffFactor  float64
}

// NewS3Reader 构造 S3Reader 实例
func NewS3Reader(client ObjectStorageClient, bucket string, staging *StagingManager, opts ...ReaderOption) *S3Reader {
	r := &S3Reader{
		client:         client,
		bucket:         bucket,
		staging:        staging,
		batcher:        NewBlockBatcher(),
		maxRetries:     3,
		initialBackoff: 50 * time.Millisecond,
		maxBackoff:     2 * time.Second,
		backoffFactor:  2.0,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewS3ReaderFromConfig 基于 S3Config 配置初始化连接并构造 S3Reader 实例
func NewS3ReaderFromConfig(cfg config.S3Config, staging *StagingManager, opts ...ReaderOption) (*S3Reader, error) {
	client, err := NewMinIOClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化 minio 客户端失败: %w", err)
	}
	return NewS3Reader(client, cfg.Bucket, staging, opts...), nil
}

// Client 返回底层绑定的 ObjectStorageClient 实例
func (r *S3Reader) Client() ObjectStorageClient {
	return r.client
}

// Bucket 返回目标 S3 存储桶名称
func (r *S3Reader) Bucket() string {
	return r.bucket
}

// Staging 返回配置的 StagingManager 实例
func (r *S3Reader) Staging() *StagingManager {
	return r.staging
}

// isS3NotFoundError 检查指定错误是否表示对象在 S3/MinIO 中不存在
func isS3NotFoundError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	resp := minio.ToErrorResponse(err)
	if resp.StatusCode == http.StatusNotFound || resp.Code == "NoSuchKey" || resp.Code == "ResourceNotFound" {
		return true
	}
	return false
}

// ReadBlock 从 S3 读取单个对象物理块，并在遭遇瞬时错误时自动执行指数退避重试
func (r *S3Reader) ReadBlock(ctx context.Context, key string, opts minio.GetObjectOptions) ([]byte, error) {
	if r.client == nil {
		return nil, fmt.Errorf("s3 客户端为空: %w", os.ErrNotExist)
	}

	maxAttempts := 1 + r.maxRetries
	var lastErr error

	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		obj, err := r.client.GetObject(ctx, r.bucket, key, opts)
		if err != nil {
			lastErr = err
			if isS3NotFoundError(err) || !isTransientError(err) {
				return nil, err
			}
		} else {
			data, readErr := io.ReadAll(obj)
			_ = obj.Close()
			if readErr == nil {
				return data, nil
			}

			lastErr = readErr
			if isS3NotFoundError(readErr) {
				return nil, readErr
			}
			if !isTransientError(readErr) {
				return nil, readErr
			}
		}

		if attempt == maxAttempts-1 {
			break
		}

		backoff := float64(r.initialBackoff) * math.Pow(r.backoffFactor, float64(attempt))
		delay := time.Duration(backoff)
		if delay > r.maxBackoff {
			delay = r.maxBackoff
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return nil, fmt.Errorf("s3 get %s 超过最大重试次数 (%d): %w", key, r.maxRetries, lastErr)
}

// ReadSlice 尝试读取切片完整数据：优先命中本地 NVMe Staging 缓存，本地缺失时透明回源拉取 S3
func (r *S3Reader) ReadSlice(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64) ([]byte, error) {
	// 1. 热路径：优先尝试本地 NVMe Staging 缓存
	if r.staging != nil {
		data, err := r.staging.ReadSlice(diskID, sliceID)
		if err == nil {
			return data, nil
		}
	}

	// 2. 冷路径：透明回源 S3 / MinIO
	if r.client == nil {
		return nil, fmt.Errorf("切片 %d 在本地暂存缺失且未配置 S3 客户端: %w", sliceID, os.ErrNotExist)
	}

	var totalData []byte
	blockSize := r.batcher.BlockSize()

	for blockIndx := uint32(0); ; blockIndx++ {
		key := FormatObjectKey(diskID, chunkIndx, sliceID, blockIndx)
		data, err := r.ReadBlock(ctx, key, minio.GetObjectOptions{})
		if err != nil {
			if isS3NotFoundError(err) {
				if blockIndx == 0 {
					return nil, fmt.Errorf("切片 %d (对象键: %s) 不存在: %w", sliceID, key, os.ErrNotExist)
				}
				// 达到多块切片的尾部
				break
			}
			return nil, fmt.Errorf("读取切片物理块 %s 失败: %w", key, err)
		}

		totalData = append(totalData, data...)

		// 若读取的数据块小于 4MB，说明已达到该切片的最后一个物理块
		if len(data) < blockSize {
			break
		}
	}

	return totalData, nil
}

// ReadSliceRange 读取切片的有效载荷子区间 [offset, offset+length)。
// 优先查验本地 NVMe Staging 文件，未命中时通过 S3 HTTP Range 发起按需区间请求。
func (r *S3Reader) ReadSliceRange(ctx context.Context, diskID string, chunkIndx uint32, sliceID uint64, offset uint32, length uint32) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	// 1. 热路径：从本地 NVMe Staging 缓存文件读取
	if r.staging != nil {
		path := r.staging.SlicePath(diskID, sliceID)
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			buf := make([]byte, length)
			n, readErr := f.ReadAt(buf, int64(offset))
			if readErr == nil || (errors.Is(readErr, io.EOF) && n > 0) {
				return buf[:n], nil
			}
		}
	}

	// 2. 冷路径：通过 S3 HTTP Range 局部区间回源拉取
	if r.client == nil {
		return nil, fmt.Errorf("切片 %d 在本地暂存缺失且未配置 S3 客户端: %w", sliceID, os.ErrNotExist)
	}

	blockSize := uint32(r.batcher.BlockSize())
	startBlock := offset / blockSize
	endBlock := (offset + length - 1) / blockSize

	result := make([]byte, 0, length)

	for b := startBlock; b <= endBlock; b++ {
		key := FormatObjectKey(diskID, chunkIndx, sliceID, b)

		blockStart := int64(0)
		if b == startBlock {
			blockStart = int64(offset % blockSize)
		}

		blockEnd := int64(blockSize - 1)
		if b == endBlock {
			blockEnd = int64((offset + length - 1) % blockSize)
		}

		opts := minio.GetObjectOptions{}
		if err := opts.SetRange(blockStart, blockEnd); err != nil {
			return nil, fmt.Errorf("无效的 HTTP Range 区间 [%d, %d]: %w", blockStart, blockEnd, err)
		}

		data, err := r.ReadBlock(ctx, key, opts)
		if err != nil {
			return nil, fmt.Errorf("拉取切片物理块区间 %s [%d-%d] 失败: %w", key, blockStart, blockEnd, err)
		}

		result = append(result, data...)
	}

	return result, nil
}

// ForChunk 返回绑定到特定 chunk 索引的包装器（默认使用 context.Background）
func (r *S3Reader) ForChunk(chunkIndx uint32) *chunkReaderWrapper {
	return r.ForChunkWithContext(context.Background(), chunkIndx)
}

// ForChunkWithContext 返回绑定到特定 chunk 索引与上下文的包装读取器
func (r *S3Reader) ForChunkWithContext(ctx context.Context, chunkIndx uint32) *chunkReaderWrapper {
	return &chunkReaderWrapper{
		reader:    r,
		ctx:       ctx,
		chunkIndx: chunkIndx,
	}
}

// chunkReaderWrapper 适配特定 chunk 的切片范围读取器
type chunkReaderWrapper struct {
	reader    *S3Reader
	ctx       context.Context
	chunkIndx uint32
}

func (w *chunkReaderWrapper) ReadSlice(diskID string, sliceID uint64) ([]byte, error) {
	return w.reader.ReadSlice(w.ctx, diskID, w.chunkIndx, sliceID)
}

func (w *chunkReaderWrapper) ReadSliceRange(diskID string, sliceID uint64, off uint32, length uint32) ([]byte, error) {
	return w.reader.ReadSliceRange(w.ctx, diskID, w.chunkIndx, sliceID, off, length)
}

// chunkSliceReaderAdapter 保持向后兼容性别名
type chunkSliceReaderAdapter = chunkReaderWrapper
