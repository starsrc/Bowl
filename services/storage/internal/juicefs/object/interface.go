package object

import (
	"context"
	"io"
	"time"
)

// Object 存储对象元数据抽象
type Object interface {
	Key() string
	Size() int64
	Mtime() time.Time
	IsDir() bool
	IsSymlink() bool
	StorageClass() string
	Status() string
}

type obj struct {
	key    string
	size   int64
	mtime  time.Time
	isDir  bool
	sc     string
	status string
}

func (o *obj) Key() string          { return o.key }
func (o *obj) Size() int64          { return o.size }
func (o *obj) Mtime() time.Time     { return o.mtime }
func (o *obj) IsDir() bool          { return o.isDir }
func (o *obj) IsSymlink() bool      { return false }
func (o *obj) StorageClass() string { return o.sc }
func (o *obj) Status() string       { return o.status }

// MultipartUpload 分块上传会话结构
type MultipartUpload struct {
	MinPartSize int64
	MaxCount    int64
	UploadID    string
}

// Part 单个分块上传信息
type Part struct {
	Num  int
	Size int
	ETag string
}

// PendingPart 待处理的分块信息
type PendingPart struct {
	Key      string
	UploadID string
	Created  time.Time
}

// Limits 对象存储驱动的规格约束
type Limits struct {
	IsSupportMultipartUpload bool
	IsSupportUploadPartCopy  bool
	MinPartSize              int64
	MaxPartSize              int64
	MaxPartCount             int64
}


// ObjectStorage 对象存储统一抽象接口（全部方法均应幂等）
type ObjectStorage interface {
	// String 描述当前存储驱动及目标路径
	String() string
	// Limits 返回底层存储的限制规则
	Limits() Limits
	// Create 若存储桶不存在则尝试创建
	Create(ctx context.Context) error
	// Get 读取指定对象数据（支持按偏移 off 与长度 limit 进行范围读取）
	Get(ctx context.Context, key string, off, limit int64, getters ...AttrGetter) (io.ReadCloser, error)
	// Put 写入数据至指定对象
	Put(ctx context.Context, key string, in io.Reader, getters ...AttrGetter) error
	// Copy 在存储端复制对象
	Copy(ctx context.Context, dst, src string) error
	// Delete 删除指定对象
	Delete(ctx context.Context, key string, getters ...AttrGetter) error
	// DeleteBatch 批量删除指定对象列表
	DeleteBatch(ctx context.Context, keys []string) error
	// DeletePrefix 批量删除指定前缀的所有对象
	DeletePrefix(ctx context.Context, prefix string) error
	// Head 查询对象元数据
	Head(ctx context.Context, key string) (Object, error)
	// List 列出匹配前缀的对象
	List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error)
	// ListAll 返回所有对象的读取通道
	ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan Object, error)

	// CreateMultipartUpload 初始化分块上传
	CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error)
	// UploadPart 上传单一分块
	UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*Part, error)
	// UploadPartCopy 从现有对象复制并作为分块
	UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*Part, error)
	// AbortUpload 中止分块上传
	AbortUpload(ctx context.Context, key string, uploadID string)
	// CompleteUpload 完成分块上传
	CompleteUpload(ctx context.Context, key string, uploadID string, parts []*Part) error
	// ListUploads 列出未完成的分块上传
	ListUploads(ctx context.Context, marker string) ([]*PendingPart, string, error)
	// Restore 解冻归档对象
	Restore(ctx context.Context, key string, days int32) error
}

// Shutdownable 可安全关闭的存储接口
type Shutdownable interface {
	Shutdown()
}

// Shutdown 安全关闭 ObjectStorage 实例
func Shutdown(o ObjectStorage) {
	if s, ok := o.(Shutdownable); ok {
		s.Shutdown()
	}
}
