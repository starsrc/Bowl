package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// s3Store 基于 minio-go 纯 Go SDK 实现的 S3 对象存储驱动
type s3Store struct {
	DefaultObjectStorage
	client   *minio.Client
	endpoint string
	bucket   string
}

// String 返回 S3 存储驱动的 URI 描述
func (s *s3Store) String() string {
	return fmt.Sprintf("s3://%s/%s/", s.endpoint, s.bucket)
}

// Limits 返回 S3 协议分块上传限制
func (s *s3Store) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              5 << 20,
		MaxPartSize:              5 << 30,
		MaxPartCount:             10000,
	}
}

// Create 若存储桶不存在则尝试自动创建
func (s *s3Store) Create(ctx context.Context) error {
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	}
	return nil
}

// Head 查询对象的元数据信息
func (s *s3Store) Head(ctx context.Context, key string) (Object, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &obj{
		key:    key,
		size:   info.Size,
		mtime:  info.LastModified,
		isDir:  strings.HasSuffix(key, "/"),
		sc:     info.StorageClass,
		status: "NORMAL",
	}, nil
}

// Get 读取指定对象数据，支持按偏移 off 与长度 limit 进行 HTTP Range 局部区间读取
func (s *s3Store) Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error) {
	opts := minio.GetObjectOptions{}
	if off > 0 || limit > 0 {
		end := int64(0)
		if limit > 0 {
			end = off + limit - 1
		}
		if err := opts.SetRange(off, end); err != nil {
			return nil, err
		}
	}
	return s.client.GetObject(ctx, s.bucket, key, opts)
}

// Put 写入数据至指定对象（支持 4MB 物理分块与通用流式载荷高效上传）
func (s *s3Store) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	var size int64 = -1
	switch r := in.(type) {
	case *bytes.Reader:
		size = int64(r.Len())
	case *bytes.Buffer:
		size = int64(r.Len())
	case *strings.Reader:
		size = int64(r.Len())
	case io.Seeker:
		if cur, err := r.Seek(0, io.SeekCurrent); err == nil {
			if total, err := r.Seek(0, io.SeekEnd); err == nil {
				size = total - cur
				_, _ = r.Seek(cur, io.SeekStart)
			}
		}
	}

	_, err := s.client.PutObject(ctx, s.bucket, key, in, size, minio.PutObjectOptions{
		ContentType:          "application/octet-stream",
		DisableContentSha256: true,
	})
	return err
}

// Copy 在存储端复制对象
func (s *s3Store) Copy(ctx context.Context, dst, src string) error {
	srcOpts := minio.CopySrcOptions{
		Bucket: s.bucket,
		Object: src,
	}
	dstOpts := minio.CopyDestOptions{
		Bucket: s.bucket,
		Object: dst,
	}
	_, err := s.client.CopyObject(ctx, dstOpts, srcOpts)
	return err
}

// Delete 删除单个指定对象
func (s *s3Store) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

// DeleteBatch 批量删除指定对象列表（基于 S3 Multi-Object Delete 协议优化）
func (s *s3Store) DeleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	objectsCh := make(chan minio.ObjectInfo, len(keys))
	for _, k := range keys {
		objectsCh <- minio.ObjectInfo{Key: k}
	}
	close(objectsCh)

	var delErrs []error
	for err := range s.client.RemoveObjects(ctx, s.bucket, objectsCh, minio.RemoveObjectsOptions{}) {
		if err.Err != nil {
			delErrs = append(delErrs, fmt.Errorf("删除对象 %s 失败: %w", err.ObjectName, err.Err))
		}
	}
	if len(delErrs) > 0 {
		return errors.Join(delErrs...)
	}
	return nil
}

// DeletePrefix 流式列举并批量删除匹配前缀的所有对象
func (s *s3Store) DeletePrefix(ctx context.Context, prefix string) error {
	opts := minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	}

	listCh := s.client.ListObjects(ctx, s.bucket, opts)
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

	var removeErrs []error
	for rErr := range s.client.RemoveObjects(ctx, s.bucket, toDeleteCh, minio.RemoveObjectsOptions{}) {
		if rErr.Err != nil {
			removeErrs = append(removeErrs, fmt.Errorf("批量删除对象 %s 失败: %w", rErr.ObjectName, rErr.Err))
		}
	}

	listErrMu.Lock()
	defer listErrMu.Unlock()
	if listErr != nil {
		return fmt.Errorf("列出对象失败 (前缀: %s): %w", prefix, listErr)
	}

	if len(removeErrs) > 0 {
		return errors.Join(removeErrs...)
	}
	return nil
}

// List 列出匹配前缀的对象
func (s *s3Store) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	opts := minio.ListObjectsOptions{
		Prefix:     prefix,
		StartAfter: startAfter,
		Recursive:  true,
		MaxKeys:    int(limit),
	}
	ch := s.client.ListObjects(ctx, s.bucket, opts)
	var objs []Object
	for item := range ch {
		if item.Err != nil {
			return nil, false, "", item.Err
		}
		objs = append(objs, &obj{
			key:    item.Key,
			size:   item.Size,
			mtime:  item.LastModified,
			isDir:  strings.HasSuffix(item.Key, "/"),
			sc:     item.StorageClass,
			status: "NORMAL",
		})
		if limit > 0 && int64(len(objs)) >= limit {
			break
		}
	}
	return objs, false, "", nil
}

// NewS3 构造 S3 对象存储驱动实例
func NewS3(endpoint, bucket, accessKey, secretKey, token string, useSSL bool) (ObjectStorage, error) {
	host := endpoint
	if strings.Contains(endpoint, "://") {
		uri, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("无效的 S3 endpoint: %w", err)
		}
		host = uri.Host
		if strings.ToLower(uri.Scheme) == "https" {
			useSSL = true
		}
		if bucket == "" {
			pathBucket := strings.TrimPrefix(uri.Path, "/")
			if idx := strings.Index(pathBucket, "/"); idx >= 0 {
				pathBucket = pathBucket[:idx]
			}
			bucket = pathBucket
		}
	}

	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = os.Getenv("MINIO_ROOT_USER")
		}
		if accessKey == "" {
			accessKey = os.Getenv("S3_ACCESS_KEY")
		}
	}
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = os.Getenv("MINIO_ROOT_PASSWORD")
		}
		if secretKey == "" {
			secretKey = os.Getenv("S3_SECRET_KEY")
		}
	}
	if token == "" {
		token = os.Getenv("AWS_SESSION_TOKEN")
	}

	if bucket == "" {
		bucket = os.Getenv("S3_BUCKET")
		if bucket == "" {
			bucket = os.Getenv("AWS_BUCKET")
		}
		if bucket == "" {
			bucket = "bowl-data"
		}
	}

	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, token),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化 S3 客户端失败: %w", err)
	}

	return &s3Store{
		client:   client,
		endpoint: host,
		bucket:   bucket,
	}, nil
}

// NewS3WithClient 基于已有 minio.Client 实例构造 S3 存储驱动
func NewS3WithClient(client *minio.Client, bucket string) ObjectStorage {
	return &s3Store{
		client:   client,
		endpoint: client.EndpointURL().Host,
		bucket:   bucket,
	}
}

// newS3 供 CreateStorage 工厂使用的 S3 构造器
func newS3(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	useSSL := false
	if strings.HasPrefix(strings.ToLower(endpoint), "https://") || strings.ToLower(os.Getenv("S3_USE_SSL")) == "true" {
		useSSL = true
	}
	return NewS3(endpoint, "", accessKey, secretKey, token, useSSL)
}

func init() {
	Register("s3", newS3)
	Register("minio", newS3)
}
