package adapter

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/starsrc/bowl/services/storage/internal/juicefs/meta"
)

const (
	// SliceBytes 24 字节大端序二进制切片序列化规约固定长度
	SliceBytes = 24

	// ChunkSize 单个 Chunk 的最大逻辑长度（64 MiB）
	ChunkSize = 64 * 1024 * 1024

	// BlockSize 单个物理块的标准尺寸（4 MiB）
	BlockSize = 4 * 1024 * 1024
)

var (
	// ErrTruncatedBuffer 表示切片缓冲区长度受损或非 24 字节整数倍
	ErrTruncatedBuffer = errors.New("adapter: 切片二进制缓冲区长度受损或非 24 字节整数倍")

	// ErrOutOfBounds 表示切片区间超出逻辑边界约束
	ErrOutOfBounds = errors.New("adapter: 切片偏移量或长度超出合法范围")

	// ErrInvalidSliceID 表示切片编号非法（例如为 0）
	ErrInvalidSliceID = errors.New("adapter: 切片编号非法")

	// ErrSrcInodeNotFound 表示写时复制克隆时源 Inode 不存在
	ErrSrcInodeNotFound = errors.New("adapter: 源 Inode 不存在")

	// ErrDstInodeNotFound 表示写时复制克隆时目标 Inode 不存在
	ErrDstInodeNotFound = errors.New("adapter: 目标 Inode 不存在")
)

// Slice 对应 64MB Chunk 内部写入的不可变切片元数据记录。
// 严格对齐 24 字节大端序二进制格式：
//
//	Pos:  uint32 (bytes 0..3)   - Chunk 内部逻辑起始偏移量 [0, ChunkSize)
//	ID:   uint64 (bytes 4..11)  - 全局唯一切片编号 (> 0)
//	Size: uint32 (bytes 12..15) - 切片物理对象总字节尺寸
//	Off:  uint32 (bytes 16..19) - 切片在物理对象中的起始偏移 [0, Size)
//	Len:  uint32 (bytes 20..23) - 当前切片有效数据字节长度 [0, Size - Off]
type Slice struct {
	Pos  uint32 // Chunk 内字节起始偏移量
	ID   uint64 // 全局唯一切片编号
	Size uint32 // 切片物理对象总大小
	Off  uint32 // 切片有效数据起始偏移
	Len  uint32 // 切片有效数据字节长度
}

// Validate 校验切片结构体字段完整性与边界不变量
func (s Slice) Validate() error {
	if s.ID == 0 {
		// ID 为 0 代表稀疏空洞切片 (Sparse Hole)，物理尺寸与对象偏移必须为 0
		if s.Size != 0 || s.Off != 0 {
			return ErrInvalidSliceID
		}
		if uint64(s.Pos)+uint64(s.Len) > ChunkSize {
			return fmt.Errorf("%w: pos (%d) + len (%d) > chunk size (%d)", ErrOutOfBounds, s.Pos, s.Len, ChunkSize)
		}
		return nil
	}
	// 防范整数溢出并确保切片内载荷引用未超出物理尺寸
	if uint64(s.Off)+uint64(s.Len) > uint64(s.Size) {
		return fmt.Errorf("%w: off (%d) + len (%d) > size (%d)", ErrOutOfBounds, s.Off, s.Len, s.Size)
	}
	// 防范整数溢出并确保未超出 64MB Chunk 边界
	if uint64(s.Pos)+uint64(s.Len) > ChunkSize {
		return fmt.Errorf("%w: pos (%d) + len (%d) > chunk size (%d)", ErrOutOfBounds, s.Pos, s.Len, ChunkSize)
	}
	return nil
}

// AppendSlice 将切片按 24 字节大端序追加写入目标字节数组
func AppendSlice(dst []byte, s Slice) []byte {
	dst = binary.BigEndian.AppendUint32(dst, s.Pos)
	dst = binary.BigEndian.AppendUint64(dst, s.ID)
	dst = binary.BigEndian.AppendUint32(dst, s.Size)
	dst = binary.BigEndian.AppendUint32(dst, s.Off)
	dst = binary.BigEndian.AppendUint32(dst, s.Len)
	return dst
}

// EncodeSlice 序列化单切片为 24 字节大端序二进制字节切片
func EncodeSlice(s Slice) []byte {
	return AppendSlice(make([]byte, 0, SliceBytes), s)
}

// DecodeSlice 从 24 字节二进制数据反序列化为单切片并执行合法性校验
func DecodeSlice(data []byte) (Slice, error) {
	if len(data) < SliceBytes {
		return Slice{}, fmt.Errorf("%w: 期望 %d 字节，实际得到 %d 字节", ErrTruncatedBuffer, SliceBytes, len(data))
	}
	s := Slice{
		Pos:  binary.BigEndian.Uint32(data[0:4]),
		ID:   binary.BigEndian.Uint64(data[4:12]),
		Size: binary.BigEndian.Uint32(data[12:16]),
		Off:  binary.BigEndian.Uint32(data[16:20]),
		Len:  binary.BigEndian.Uint32(data[20:24]),
	}
	if err := s.Validate(); err != nil {
		return Slice{}, err
	}
	return s, nil
}

// EncodeSlices 序列化切片切片为连续紧凑二进制字节流
func EncodeSlices(slices []Slice) []byte {
	buf := make([]byte, 0, len(slices)*SliceBytes)
	for _, s := range slices {
		buf = AppendSlice(buf, s)
	}
	return buf
}

// DecodeSlices 从连续二进制字节流反序列化切片列表
func DecodeSlices(data []byte) ([]Slice, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data)%SliceBytes != 0 {
		return nil, fmt.Errorf("%w: 总长度 %d 非 %d 的整数倍", ErrTruncatedBuffer, len(data), SliceBytes)
	}
	count := len(data) / SliceBytes
	res := make([]Slice, 0, count)
	for i := 0; i < len(data); i += SliceBytes {
		s, err := DecodeSlice(data[i : i+SliceBytes])
		if err != nil {
			return nil, fmt.Errorf("解析索引 %d 处切片失败: %w", i/SliceBytes, err)
		}
		res = append(res, s)
	}
	return res, nil
}

// BuildSlice 复用 JuiceFS meta 图层覆盖算法，将多切片重组为互不重叠的顺序逻辑切片
func BuildSlice(slices []Slice) []meta.Slice {
	if len(slices) == 0 {
		return nil
	}
	raw := EncodeSlices(slices)
	return BuildSliceFromRaw(raw)
}

// BuildSliceFromRaw 直接从二进制字节流重组为顺序无重叠切片图层
func BuildSliceFromRaw(raw []byte) []meta.Slice {
	if len(raw) == 0 {
		return nil
	}
	internalSlices := meta.ReadSliceBuf(raw)
	return meta.BuildSlice(internalSlices)
}
