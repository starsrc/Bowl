package object

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type s3Store struct {
	DefaultObjectStorage
	client   *minio.Client
	endpoint string
	bucket   string
}

func (s *s3Store) String() string {
	return fmt.Sprintf("s3://%s/%s/", s.endpoint, s.bucket)
}

func (s *s3Store) Limits() Limits {
	return Limits{
		IsSupportMultipartUpload: true,
		IsSupportUploadPartCopy:  true,
		MinPartSize:              5 << 20,
		MaxPartSize:              5 << 30,
		MaxPartCount:             10000,
	}
}

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

func (s *s3Store) Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error {
	_, err := s.client.PutObject(ctx, s.bucket, key, in, -1, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	return err
}

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

func (s *s3Store) Delete(ctx context.Context, key string, getters ...AttrGetter) error {
	return s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{})
}

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

func newS3(endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	if !strings.Contains(endpoint, "://") {
		endpoint = "http://" + endpoint
	}
	uri, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("无效的 S3 endpoint: %w", err)
	}

	useSSL := strings.ToLower(uri.Scheme) == "https"
	host := uri.Host

	if accessKey == "" {
		accessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		if accessKey == "" {
			accessKey = os.Getenv("MINIO_ROOT_USER")
		}
	}
	if secretKey == "" {
		secretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		if secretKey == "" {
			secretKey = os.Getenv("MINIO_ROOT_PASSWORD")
		}
	}

	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, token),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化 S3 客户端失败: %w", err)
	}

	bucket := strings.TrimPrefix(uri.Path, "/")
	if idx := strings.Index(bucket, "/"); idx >= 0 {
		bucket = bucket[:idx]
	}

	return &s3Store{
		client:   client,
		endpoint: host,
		bucket:   bucket,
	}, nil
}

func init() {
	Register("s3", newS3)
	Register("minio", newS3)
}
