package adapter

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlice_EncodeAndDecode 测试单个切片在 24 字节大端序二进制规约下的序列化与反序列化
func TestSlice_EncodeAndDecode(t *testing.T) {
	orig := Slice{
		Pos:  1024,
		ID:   123456789,
		Size: 4096,
		Off:  512,
		Len:  2048,
	}

	buf := EncodeSlice(orig)
	require.Len(t, buf, SliceBytes, "单切片编码长度必须严格为 24 字节")

	// 验证大端序逐字段字节排布
	assert.Equal(t, uint32(1024), binary.BigEndian.Uint32(buf[0:4]))
	assert.Equal(t, uint64(123456789), binary.BigEndian.Uint64(buf[4:12]))
	assert.Equal(t, uint32(4096), binary.BigEndian.Uint32(buf[12:16]))
	assert.Equal(t, uint32(512), binary.BigEndian.Uint32(buf[16:20]))
	assert.Equal(t, uint32(2048), binary.BigEndian.Uint32(buf[20:24]))

	decoded, err := DecodeSlice(buf)
	require.NoError(t, err)
	assert.Equal(t, orig, decoded)
}

// TestSlice_Validation 测试切片越界与边界合法性校验
func TestSlice_Validation(t *testing.T) {
	// 场景 1: 合法切片边界
	valid := Slice{
		Pos:  0,
		ID:   1,
		Size: 4096,
		Off:  0,
		Len:  4096,
	}
	assert.NoError(t, valid.Validate())

	// 场景 2: 切片载荷越界 (Off + Len > Size)
	invalidPayload := Slice{
		Pos:  0,
		ID:   1,
		Size: 4096,
		Off:  2048,
		Len:  3000,
	}
	assert.ErrorIs(t, invalidPayload.Validate(), ErrOutOfBounds)

	// 场景 3: 超出 64MB Chunk 边界 (Pos + Len > ChunkSize)
	invalidChunkBound := Slice{
		Pos:  ChunkSize - 100,
		ID:   1,
		Size: 4096,
		Off:  0,
		Len:  200,
	}
	assert.ErrorIs(t, invalidChunkBound.Validate(), ErrOutOfBounds)

	// 场景 4: 零切片 ID 校验
	zeroID := Slice{
		Pos:  0,
		ID:   0,
		Size: 4096,
		Off:  0,
		Len:  4096,
	}
	assert.ErrorIs(t, zeroID.Validate(), ErrInvalidSliceID)
}

// TestSlice_EncodeDecodeSlices 测试多个切片的批量编解码与截断异常处理
func TestSlice_EncodeDecodeSlices(t *testing.T) {
	slices := []Slice{
		{Pos: 0, ID: 1, Size: 4096, Off: 0, Len: 4096},
		{Pos: 4096, ID: 2, Size: 8192, Off: 0, Len: 4096},
		{Pos: 8192, ID: 3, Size: 4096, Off: 0, Len: 4096},
	}

	raw := EncodeSlices(slices)
	require.Len(t, raw, 3*SliceBytes)

	decoded, err := DecodeSlices(raw)
	require.NoError(t, err)
	assert.Equal(t, slices, decoded)

	// 测试非 24 字节倍数的截断缓冲区
	truncated := raw[:len(raw)-5]
	_, err = DecodeSlices(truncated)
	assert.ErrorIs(t, err, ErrTruncatedBuffer)
}

// TestSlice_BuildSliceOverlay 测试基于 JuiceFS meta 算法的图层覆盖计算
func TestSlice_BuildSliceOverlay(t *testing.T) {
	// 场景：初始写入 0~4096，后续在 2048~3072 进行覆写
	slices := []Slice{
		{Pos: 0, ID: 1, Size: 4096, Off: 0, Len: 4096},
		{Pos: 2048, ID: 2, Size: 1024, Off: 0, Len: 1024},
	}

	reconstituted := BuildSlice(slices)
	require.Len(t, reconstituted, 3)

	assert.Equal(t, uint64(1), reconstituted[0].Id)
	assert.Equal(t, uint32(2048), reconstituted[0].Len)
	assert.Equal(t, uint32(0), reconstituted[0].Off)

	assert.Equal(t, uint64(2), reconstituted[1].Id)
	assert.Equal(t, uint32(1024), reconstituted[1].Len)
	assert.Equal(t, uint32(0), reconstituted[1].Off)

	assert.Equal(t, uint64(1), reconstituted[2].Id)
	assert.Equal(t, uint32(1024), reconstituted[2].Len)
	assert.Equal(t, uint32(3072), reconstituted[2].Off)
}
