package object

import (
	"context"
	"fmt"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/utils"
)

var notSupported = utils.ErrNotSUP
var logger = utils.GetLogger("juicefs")

// DefaultObjectStorage 提供通用默认空实现的基类
type DefaultObjectStorage struct{}

func (s DefaultObjectStorage) Create(ctx context.Context) error {
	return nil
}

func (s DefaultObjectStorage) Limits() Limits {
	return Limits{IsSupportMultipartUpload: false, IsSupportUploadPartCopy: false}
}

func (s DefaultObjectStorage) Head(ctx context.Context, key string) (Object, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) Copy(ctx context.Context, dst, src string) error {
	return notSupported
}

func (s DefaultObjectStorage) CreateMultipartUpload(ctx context.Context, key string) (*MultipartUpload, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*Part, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*Part, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) AbortUpload(ctx context.Context, key string, uploadID string) {}

func (s DefaultObjectStorage) CompleteUpload(ctx context.Context, key string, uploadID string, parts []*Part) error {
	return notSupported
}

func (s DefaultObjectStorage) ListUploads(ctx context.Context, marker string) ([]*PendingPart, string, error) {
	return nil, "", nil
}

func (s DefaultObjectStorage) List(ctx context.Context, prefix, start, token, delimiter string, limit int64, followLink bool) ([]Object, bool, string, error) {
	return nil, false, "", notSupported
}

func (s DefaultObjectStorage) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan Object, error) {
	return nil, notSupported
}

func (s DefaultObjectStorage) Restore(ctx context.Context, key string, days int32) error {
	return notSupported
}

// Creator 构造 ObjectStorage 的工厂函数类型
type Creator func(bucket, accessKey, secretKey, token string) (ObjectStorage, error)

var storages = make(map[string]Creator)

// Register 注册命名的存储驱动工厂
func Register(name string, register Creator) {
	storages[name] = register
}

// IsSupported 校验指定名称的驱动是否已注册
func IsSupported(name string) bool {
	_, ok := storages[name]
	return ok
}

// CreateStorage 依据配置名称实例化对象存储驱动
func CreateStorage(name, endpoint, accessKey, secretKey, token string) (ObjectStorage, error) {
	f, ok := storages[name]
	if ok {
		logger.Debugf("正在创建存储驱动 %s，目标路径: %s", name, endpoint)
		return f(endpoint, accessKey, secretKey, token)
	}
	return nil, fmt.Errorf("未受支持或未注册的存储驱动: %s", name)
}

// TierKey 存储分层键类型
type TierKey struct{}
