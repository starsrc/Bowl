package chunk

import (
	"github.com/starsrc/bowl/services/storage/internal/juicefs/adapter"
	"github.com/starsrc/bowl/services/storage/internal/juicefs/object"
)

// Slice 为 JuiceFS 元数据适配层切片结构的类型别名，保持与历史调用点兼容
type Slice = adapter.Slice

const (
	// SliceBytes 24 字节大端序二进制切片序列化规约固定长度
	SliceBytes = adapter.SliceBytes

	// ChunkSize 单个 Chunk 的最大逻辑长度（64 MiB）
	ChunkSize = adapter.ChunkSize

	// BlockSize 单个物理块的标准尺寸（4 MiB）
	BlockSize = adapter.BlockSize
)

var (
	// ErrTruncatedBuffer 表示切片二进制缓冲区长度受损或非 24 字节整数倍
	ErrTruncatedBuffer = adapter.ErrTruncatedBuffer

	// ErrOutOfBounds 表示切片区间超出逻辑边界约束
	ErrOutOfBounds = adapter.ErrOutOfBounds

	// ErrInvalidObjectKey 当 S3 对象键不符合命名规约时返回
	ErrInvalidObjectKey = object.ErrInvalidObjectKey
)

// EncodeSlice 序列化单切片为 24 字节大端序二进制切片
func EncodeSlice(s Slice) []byte {
	return adapter.EncodeSlice(s)
}

// AppendSlice 将切片按 24 字节大端序追加写入目标字节数组
func AppendSlice(dst []byte, s Slice) []byte {
	return adapter.AppendSlice(dst, s)
}

// DecodeSlice 从 24 字节二进制数据反序列化为单切片并执行合法性校验
func DecodeSlice(data []byte) (Slice, error) {
	return adapter.DecodeSlice(data)
}

// DecodeSlices 从连续二进制字节流反序列化切片列表
func DecodeSlices(data []byte) ([]Slice, error) {
	return adapter.DecodeSlices(data)
}

// EncodeSlices 序列化切片列表为连续紧凑二进制字节流
func EncodeSlices(slices []Slice) []byte {
	return adapter.EncodeSlices(slices)
}

// FormatObjectKey 根据多租户命名规范格式化 S3 对象键，委托给内联 JuiceFS 对象存储模块
func FormatObjectKey(diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32) string {
	return object.FormatObjectKey(diskID, chunkIndx, sliceID, blockIndx)
}

// ParseObjectKey 解析 S3 对象键，委托给内联 JuiceFS 对象存储模块
func ParseObjectKey(key string) (diskID string, chunkIndx uint32, sliceID uint64, blockIndx uint32, err error) {
	return object.ParseObjectKey(key)
}
