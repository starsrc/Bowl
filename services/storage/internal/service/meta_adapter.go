package service

import (
	"github.com/starsrc/bowl/services/storage/internal/juicefs/adapter"
)

// MetaAdapter 为 JuiceFS 元数据适配层的类型别名
type MetaAdapter = adapter.MetaAdapter

// Slice 为 JuiceFS 元数据适配层切片结构的类型别名
type Slice = adapter.Slice

const (
	// SliceBytes 24 字节大端序二进制切片序列化规约固定长度
	SliceBytes = adapter.SliceBytes

	// ChunkSize 单个 Chunk 的最大逻辑长度（64 MiB）
	ChunkSize = adapter.ChunkSize

	// BlockSize 单个物理块的标准尺寸（4 MiB）
	BlockSize = adapter.BlockSize
)

// EncodeSlice 序列化单切片为 24 字节大端序二进制切片
func EncodeSlice(s Slice) []byte {
	return adapter.EncodeSlice(s)
}

// DecodeSlice 从 24 字节二进制数据反序列化为单切片
func DecodeSlice(data []byte) (Slice, error) {
	return adapter.DecodeSlice(data)
}

// EncodeSlices 序列化切片切片为连续紧凑二进制字节流
func EncodeSlices(slices []Slice) []byte {
	return adapter.EncodeSlices(slices)
}

// DecodeSlices 从连续二进制字节流反序列化切片列表
func DecodeSlices(data []byte) ([]Slice, error) {
	return adapter.DecodeSlices(data)
}

// NewMetaAdapter 构造绑定指定数据库执行器的 JuiceFS 元数据切片账本适配器
func NewMetaAdapter(db DBExecutor) MetaAdapter {
	return adapter.NewMetaAdapter(db)
}

