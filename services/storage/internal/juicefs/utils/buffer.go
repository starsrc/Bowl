package utils

import (
	"encoding/binary"
	"unsafe"
)

// Buffer 用于多字节整数与二进制切片的编码与解码缓冲区
type Buffer struct {
	endian binary.ByteOrder
	off    int
	buf    []byte
}

// NewBuffer 创建指定容量的缓冲区
func NewBuffer(sz uint32) *Buffer {
	return FromBuffer(make([]byte, sz))
}

// ReadBuffer 根据现有字节数组创建只读/读取缓冲区
func ReadBuffer(buf []byte) *Buffer {
	return FromBuffer(buf)
}

// FromBuffer 根据现有字节数组构造大端序缓冲区
func FromBuffer(buf []byte) *Buffer {
	return &Buffer{binary.BigEndian, 0, buf}
}

// Len 返回缓冲区底层总长度
func (b *Buffer) Len() int {
	return len(b.buf)
}

// HasMore 判断是否还有未读字节
func (b *Buffer) HasMore() bool {
	return b.off < len(b.buf)
}

// Left 返回剩余未读字节数
func (b *Buffer) Left() int {
	return len(b.buf) - b.off
}

// Seek 调整读取偏移位置
func (b *Buffer) Seek(p int) {
	b.off = p
}

// Offset 返回当前读取偏移量
func (b *Buffer) Offset() int {
	return b.off
}

// Buffer 返回从当前偏移开始的剩余切片
func (b *Buffer) Buffer() []byte {
	return b.buf[b.off:]
}

// Put8 写入单字节 uint8
func (b *Buffer) Put8(v uint8) {
	b.buf[b.off] = v
	b.off++
}

// Get8 读取单字节 uint8
func (b *Buffer) Get8() uint8 {
	v := b.buf[b.off]
	b.off++
	return v
}

// Put16 写入双字节 uint16
func (b *Buffer) Put16(v uint16) {
	b.endian.PutUint16(b.buf[b.off:b.off+2], v)
	b.off += 2
}

// Get16 读取双字节 uint16
func (b *Buffer) Get16() uint16 {
	v := b.endian.Uint16(b.buf[b.off : b.off+2])
	b.off += 2
	return v
}

// Put32 写入四字节 uint32
func (b *Buffer) Put32(v uint32) {
	b.endian.PutUint32(b.buf[b.off:b.off+4], v)
	b.off += 4
}

// Get32 读取四字节 uint32
func (b *Buffer) Get32() uint32 {
	v := b.endian.Uint32(b.buf[b.off : b.off+4])
	b.off += 4
	return v
}

// Put64 写入八字节 uint64
func (b *Buffer) Put64(v uint64) {
	b.endian.PutUint64(b.buf[b.off:b.off+8], v)
	b.off += 8
}

// Get64 读取八字节 uint64
func (b *Buffer) Get64() uint64 {
	v := b.endian.Uint64(b.buf[b.off : b.off+8])
	b.off += 8
	return v
}

// Put 写入原始字节切片
func (b *Buffer) Put(v []byte) {
	l := len(v)
	copy(b.buf[b.off:b.off+l], v)
	b.off += l
}

// Get 读取指定长度字节切片
func (b *Buffer) Get(l int) []byte {
	b.off += l
	return b.buf[b.off-l : b.off]
}

// SetBytes 重置缓冲区底层字节流为大端序
func (b *Buffer) SetBytes(buf []byte) {
	b.endian = binary.BigEndian
	b.off = 0
	b.buf = buf
}

// Bytes 返回底层完整字节切片
func (b *Buffer) Bytes() []byte {
	return b.buf
}

// NativeEndian 宿主机本地字节序
var NativeEndian binary.ByteOrder

// NewNativeBuffer 构造使用宿主机本地字节序的缓冲区
func NewNativeBuffer(buf []byte) *Buffer {
	return &Buffer{NativeEndian, 0, buf}
}

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		NativeEndian = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		NativeEndian = binary.BigEndian
	default:
		panic("无法判定当前宿主机的原生字节序")
	}
}
