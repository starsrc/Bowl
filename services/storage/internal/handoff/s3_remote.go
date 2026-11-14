package handoff

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config 描述一个 S3 兼容的对象存储远端。
type S3Config struct {
	// Endpoint 形如 s3.amazonaws.com 或 127.0.0.1:9000（不带协议）。
	Endpoint string
	Bucket   string
	// Prefix 是桶内的键前缀，多个环境可以共用一个桶。
	Prefix    string
	Region    string
	AccessKey string
	SecretKey string
	// Insecure 为 true 时用明文 HTTP（本地 MinIO）。
	Insecure bool
}

// ParseS3URL 解析形如 s3://bucket/prefix?endpoint=host:port&region=...&insecure=true 的远端地址。
// 凭证不放进 URL，由调用方另行提供。
func ParseS3URL(raw string) (S3Config, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "s3" || u.Host == "" {
		return S3Config{}, fmt.Errorf("handoff: invalid s3 url %q (want s3://bucket/prefix?endpoint=host:port)", raw)
	}
	q := u.Query()
	cfg := S3Config{
		Endpoint: q.Get("endpoint"),
		Bucket:   u.Host,
		Prefix:   strings.Trim(u.Path, "/"),
		Region:   q.Get("region"),
		Insecure: q.Get("insecure") == "true" || q.Get("insecure") == "1",
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "s3.amazonaws.com"
	}
	return cfg, nil
}

// S3Remote 用 S3 兼容的对象存储实现 Remote。
// HEAD 的原子改写依赖对象存储的条件写（If-Match / If-None-Match），AWS S3 与 MinIO 均支持。
type S3Remote struct {
	client *minio.Client
	bucket string
	prefix string
}

// NewS3Remote 连接对象存储并确认桶存在。
func NewS3Remote(ctx context.Context, cfg *S3Config) (*S3Remote, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: !cfg.Insecure,
		Region: cfg.Region,
	})
	if err != nil {
		return nil, fmt.Errorf("handoff: init s3 client: %w", err)
	}
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("handoff: check bucket %q: %w", cfg.Bucket, err)
	}
	if !exists {
		return nil, fmt.Errorf("handoff: bucket %q does not exist", cfg.Bucket)
	}
	prefix := cfg.Prefix
	if prefix != "" {
		prefix += "/"
	}
	return &S3Remote{client: client, bucket: cfg.Bucket, prefix: prefix}, nil
}

func (s *S3Remote) key(key string) string { return s.prefix + key }

// s3Status 取出错误链里对象存储返回的 HTTP 状态码（错误可能被 errors.Join 等包装过）。
func s3Status(err error) int {
	if resp, ok := errors.AsType[minio.ErrorResponse](err); ok {
		return resp.StatusCode
	}
	return 0
}

func mapS3Err(err error) error {
	switch {
	case err == nil:
		return nil
	case s3Status(err) == http.StatusNotFound:
		return ErrNotFound
	case s3Status(err) == http.StatusPreconditionFailed:
		return ErrConflict
	default:
		return err
	}
}

// Put 上传一个对象。能确定长度时（文件、内存缓冲）走单次上传，否则由客户端分片流式上传。
func (s *S3Remote) Put(ctx context.Context, key string, r io.Reader) error {
	return s.put(ctx, key, r, &minio.PutObjectOptions{})
}

func (s *S3Remote) put(ctx context.Context, key string, r io.Reader, opts *minio.PutObjectOptions) error {
	size := int64(-1)
	switch v := r.(type) {
	case *os.File:
		if info, err := v.Stat(); err == nil {
			size = info.Size()
		}
	case *bytes.Reader:
		size = int64(v.Len())
	}
	_, err := s.client.PutObject(ctx, s.bucket, s.key(key), r, size, *opts)
	return mapS3Err(err)
}

// Get 打开对象供读取。
func (s *S3Remote) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return nil, mapS3Err(err)
	}
	// GetObject 是惰性的：对象是否存在要到第一次访问才知道，这里先问一次以便返回 ErrNotFound。
	if _, statErr := obj.Stat(); statErr != nil {
		_ = obj.Close()
		return nil, mapS3Err(statErr)
	}
	return obj, nil
}

// ReadAt 用范围请求读取对象的一段。
func (s *S3Remote) ReadAt(ctx context.Context, key string, off int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	var opts minio.GetObjectOptions
	if err := opts.SetRange(off, off+int64(len(dst))-1); err != nil {
		return err
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(key), opts)
	if err != nil {
		return mapS3Err(err)
	}
	_, readErr := io.ReadFull(obj, dst)
	return mapS3Err(errors.Join(readErr, obj.Close()))
}

// Size 返回对象大小。
func (s *S3Remote) Size(ctx context.Context, key string) (int64, error) {
	info, err := s.client.StatObject(ctx, s.bucket, s.key(key), minio.StatObjectOptions{})
	if err != nil {
		return 0, mapS3Err(err)
	}
	return info.Size, nil
}

// List 列出前缀下的全部对象。
func (s *S3Remote) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var out []ObjectInfo
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.key(prefix), Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		out = append(out, ObjectInfo{Key: strings.TrimPrefix(obj.Key, s.prefix), Size: obj.Size, ModTime: obj.LastModified})
	}
	return out, nil
}

// Delete 删除对象。
func (s *S3Remote) Delete(ctx context.Context, key string) error {
	err := s.client.RemoveObject(ctx, s.bucket, s.key(key), minio.RemoveObjectOptions{})
	if errors.Is(mapS3Err(err), ErrNotFound) {
		return nil
	}
	return err
}

// CompareAndSwap 用条件写实现：old 为 nil 时要求对象不存在（If-None-Match: *），
// 否则先读出当前内容与 ETag，内容相符再以 If-Match: <etag> 写入。
// 两个节点同时改写时，对象存储保证只有一个条件写成功。
func (s *S3Remote) CompareAndSwap(ctx context.Context, key string, old, next []byte) error {
	var opts minio.PutObjectOptions
	if old == nil {
		opts.SetMatchETagExcept("*")
		return s.put(ctx, key, bytes.NewReader(next), &opts)
	}
	obj, err := s.client.GetObject(ctx, s.bucket, s.key(key), minio.GetObjectOptions{})
	if err != nil {
		return mapS3Err(err)
	}
	info, statErr := obj.Stat()
	if statErr != nil {
		_ = obj.Close()
		if errors.Is(mapS3Err(statErr), ErrNotFound) {
			return ErrConflict
		}
		return statErr
	}
	current, readErr := io.ReadAll(obj)
	if err := errors.Join(readErr, obj.Close()); err != nil {
		return err
	}
	if !bytes.Equal(current, old) {
		return ErrConflict
	}
	opts.SetMatchETag(info.ETag)
	return s.put(ctx, key, bytes.NewReader(next), &opts)
}
