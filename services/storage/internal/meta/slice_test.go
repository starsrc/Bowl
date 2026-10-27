package meta

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestDataLayerConstants(t *testing.T) {
	if ChunkSize != 64*1024*1024 {
		t.Fatalf("ChunkSize expected 67108864 (64 MiB), got %d", ChunkSize)
	}
	if BlockSize != 4*1024*1024 {
		t.Fatalf("BlockSize expected 4194304 (4 MiB), got %d", BlockSize)
	}
	if SliceBytes != 24 {
		t.Fatalf("SliceBytes expected 24, got %d", SliceBytes)
	}
	if CompactSliceThreshold != 64 {
		t.Fatalf("CompactSliceThreshold expected 64, got %d", CompactSliceThreshold)
	}
}

func TestSlice_EncodeDecode(t *testing.T) {
	tests := []struct {
		name     string
		slice    Slice
		expected []byte
	}{
		{
			name: "标准物理切片",
			slice: Slice{
				Pos:  1024,
				ID:   0x0102030405060708,
				Size: 4096,
				Off:  512,
				Len:  2048,
			},
			expected: []byte{
				// Pos: 1024 = 0x00000400
				0x00, 0x00, 0x04, 0x00,
				// ID: 0x0102030405060708
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				// Size: 4096 = 0x00001000
				0x00, 0x00, 0x10, 0x00,
				// Off: 512 = 0x00000200
				0x00, 0x00, 0x02, 0x00,
				// Len: 2048 = 0x00000800
				0x00, 0x00, 0x08, 0x00,
			},
		},
		{
			name: "零偏移稀疏空洞切片 (ID = 0)",
			slice: Slice{
				Pos:  0,
				ID:   0,
				Size: 0,
				Off:  0,
				Len:  4096,
			},
			expected: []byte{
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x10, 0x00,
			},
		},
		{
			name: "Chunk 边界满量程切片",
			slice: Slice{
				Pos:  0,
				ID:   999,
				Size: ChunkSize,
				Off:  0,
				Len:  ChunkSize,
			},
			expected: func() []byte {
				buf := make([]byte, SliceBytes)
				binary.BigEndian.PutUint32(buf[0:4], 0)
				binary.BigEndian.PutUint64(buf[4:12], 999)
				binary.BigEndian.PutUint32(buf[12:16], ChunkSize)
				binary.BigEndian.PutUint32(buf[16:20], 0)
				binary.BigEndian.PutUint32(buf[20:24], ChunkSize)
				return buf
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeSlice(tt.slice)
			if len(encoded) != SliceBytes {
				t.Fatalf("EncodeSlice length expected %d, got %d", SliceBytes, len(encoded))
			}
			for i := range tt.expected {
				if encoded[i] != tt.expected[i] {
					t.Fatalf("byte at index %d mismatch: expected 0x%02X, got 0x%02X", i, tt.expected[i], encoded[i])
				}
			}

			// AppendEncodedSlice 测试
			appended := AppendEncodedSlice([]byte{0xAA, 0xBB}, tt.slice)
			if len(appended) != 2+SliceBytes {
				t.Fatalf("AppendEncodedSlice length expected %d, got %d", 2+SliceBytes, len(appended))
			}
			if appended[0] != 0xAA || appended[1] != 0xBB {
				t.Fatalf("AppendEncodedSlice prefix corrupted")
			}

			// DecodeSlice 测试
			decoded, err := DecodeSlice(encoded)
			if err != nil {
				t.Fatalf("DecodeSlice failed: %v", err)
			}
			if decoded != tt.slice {
				t.Fatalf("DecodeSlice result mismatch: expected %+v, got %+v", tt.slice, decoded)
			}
		})
	}
}

func TestSlice_EncodeDecodeSlices(t *testing.T) {
	slices := []Slice{
		{Pos: 0, ID: 1, Size: 1024, Off: 0, Len: 512},
		{Pos: 512, ID: 2, Size: 2048, Off: 100, Len: 1024},
		{Pos: 1536, ID: 0, Size: 0, Off: 0, Len: 4096},
	}

	encoded := EncodeSlices(slices)
	if len(encoded) != len(slices)*SliceBytes {
		t.Fatalf("EncodeSlices len expected %d, got %d", len(slices)*SliceBytes, len(encoded))
	}

	decoded, err := DecodeSlices(encoded)
	if err != nil {
		t.Fatalf("DecodeSlices failed: %v", err)
	}
	if len(decoded) != len(slices) {
		t.Fatalf("DecodeSlices count expected %d, got %d", len(slices), len(decoded))
	}
	for i := range slices {
		if decoded[i] != slices[i] {
			t.Fatalf("slice[%d] mismatch: expected %+v, got %+v", i, slices[i], decoded[i])
		}
	}

	// 空切片编解码
	emptyDecoded, err := DecodeSlices(nil)
	if err != nil || len(emptyDecoded) != 0 {
		t.Fatalf("DecodeSlices(nil) expected empty, got %v, err=%v", emptyDecoded, err)
	}
	emptyDecoded2, err := DecodeSlices([]byte{})
	if err != nil || len(emptyDecoded2) != 0 {
		t.Fatalf("DecodeSlices([]byte{}) expected empty, got %v, err=%v", emptyDecoded2, err)
	}
}

func TestSlice_DecodeValidationErrors(t *testing.T) {
	t.Run("非24字节整数倍", func(t *testing.T) {
		invalidBufs := [][]byte{
			{1, 2, 3},
			make([]byte, 23),
			make([]byte, 25),
			make([]byte, 47),
		}
		for _, buf := range invalidBufs {
			_, err := DecodeSlice(buf)
			if err == nil || !errors.Is(err, ErrInvalidSliceBytes) {
				t.Fatalf("DecodeSlice expected ErrInvalidSliceBytes for len %d, got %v", len(buf), err)
			}
			_, err = DecodeSlices(buf)
			if err == nil || !errors.Is(err, ErrInvalidSliceBytes) {
				t.Fatalf("DecodeSlices expected ErrInvalidSliceBytes for len %d, got %v", len(buf), err)
			}
		}
	})

	t.Run("Len为0", func(t *testing.T) {
		s := Slice{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 0}
		raw := make([]byte, SliceBytes)
		binary.BigEndian.PutUint32(raw[0:4], s.Pos)
		binary.BigEndian.PutUint64(raw[4:12], s.ID)
		binary.BigEndian.PutUint32(raw[12:16], s.Size)
		binary.BigEndian.PutUint32(raw[16:20], s.Off)
		binary.BigEndian.PutUint32(raw[20:24], s.Len)

		_, err := DecodeSlice(raw)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData for Len=0, got %v", err)
		}
	})

	t.Run("Pos+Len超过ChunkSize (64 MiB)", func(t *testing.T) {
		s := Slice{Pos: ChunkSize - 10, ID: 1, Size: 100, Off: 0, Len: 11}
		raw := make([]byte, SliceBytes)
		binary.BigEndian.PutUint32(raw[0:4], s.Pos)
		binary.BigEndian.PutUint64(raw[4:12], s.ID)
		binary.BigEndian.PutUint32(raw[12:16], s.Size)
		binary.BigEndian.PutUint32(raw[16:20], s.Off)
		binary.BigEndian.PutUint32(raw[20:24], s.Len)

		_, err := DecodeSlice(raw)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData for Pos+Len > ChunkSize, got %v", err)
		}
	})

	t.Run("有效切片(ID>0)但Size为0", func(t *testing.T) {
		s := Slice{Pos: 0, ID: 1, Size: 0, Off: 0, Len: 10}
		raw := make([]byte, SliceBytes)
		binary.BigEndian.PutUint32(raw[0:4], s.Pos)
		binary.BigEndian.PutUint64(raw[4:12], s.ID)
		binary.BigEndian.PutUint32(raw[12:16], s.Size)
		binary.BigEndian.PutUint32(raw[16:20], s.Off)
		binary.BigEndian.PutUint32(raw[20:24], s.Len)

		_, err := DecodeSlice(raw)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData for ID>0 with Size=0, got %v", err)
		}
	})

	t.Run("有效切片(ID>0)但Off+Len超过Size", func(t *testing.T) {
		s := Slice{Pos: 0, ID: 1, Size: 100, Off: 60, Len: 41}
		raw := make([]byte, SliceBytes)
		binary.BigEndian.PutUint32(raw[0:4], s.Pos)
		binary.BigEndian.PutUint64(raw[4:12], s.ID)
		binary.BigEndian.PutUint32(raw[12:16], s.Size)
		binary.BigEndian.PutUint32(raw[16:20], s.Off)
		binary.BigEndian.PutUint32(raw[20:24], s.Len)

		_, err := DecodeSlice(raw)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData for Off+Len > Size, got %v", err)
		}
	})

	t.Run("DecodeSlices解析到中间损坏切片报错", func(t *testing.T) {
		valid := EncodeSlice(Slice{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100})
		corrupt := make([]byte, SliceBytes) // Len = 0
		raw := append(valid, corrupt...)
		_, err := DecodeSlices(raw)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData for DecodeSlices with corrupt slice, got %v", err)
		}
	})
}

func TestBuildSlice_Scenarios(t *testing.T) {
	tests := []struct {
		name     string
		input    []Slice
		expected []Slice
	}{
		{
			name:     "空切片序列",
			input:    nil,
			expected: nil,
		},
		{
			name: "单个切片首部从0开始",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
			},
			expected: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
			},
		},
		{
			name: "单个切片首部有间隙(自动补齐[0, Pos)空洞)",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100},
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100},
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100},
			},
		},
		{
			name: "顺序不重叠紧密追加",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
				{Pos: 100, ID: 2, Size: 200, Off: 50, Len: 150},
			},
			expected: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
				{Pos: 100, ID: 2, Size: 200, Off: 50, Len: 150},
			},
		},
		{
			name: "顺序追加但中间有未写入逻辑间隙",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
				{Pos: 300, ID: 2, Size: 100, Off: 0, Len: 100},
			},
			expected: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100},
				{Pos: 100, ID: 0, Size: 0, Off: 0, Len: 200}, // 自动合成空洞 [100, 300)
				{Pos: 300, ID: 2, Size: 100, Off: 0, Len: 100},
			},
		},
		{
			name: "逆序追加且首尾均有间隙",
			input: []Slice{
				{Pos: 300, ID: 2, Size: 100, Off: 0, Len: 100},
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100},
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100}, // 首部空洞 [0, 100)
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100},
				{Pos: 200, ID: 0, Size: 0, Off: 0, Len: 100}, // 中间空洞 [200, 300)
				{Pos: 300, ID: 2, Size: 100, Off: 0, Len: 100},
			},
		},
		{
			name: "完全覆盖 (新切片覆盖吞没旧切片)",
			input: []Slice{
				{Pos: 50, ID: 1, Size: 100, Off: 0, Len: 100}, // [50, 150)
				{Pos: 0, ID: 2, Size: 300, Off: 50, Len: 200}, // [0, 200) 完全吞没 [50, 150)
			},
			expected: []Slice{
				{Pos: 0, ID: 2, Size: 300, Off: 50, Len: 200},
			},
		},
		{
			name: "左端覆盖 (新切片覆盖旧切片前半部分)",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100}, // [100, 200)
				{Pos: 50, ID: 2, Size: 80, Off: 0, Len: 80},    // [50, 130)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 50},      // [0, 50) 空洞
				{Pos: 50, ID: 2, Size: 80, Off: 0, Len: 80},    // [50, 130) 新切片
				{Pos: 130, ID: 1, Size: 100, Off: 30, Len: 70}, // [130, 200) 旧切片右半段
			},
		},
		{
			name: "右端覆盖 (新切片覆盖旧切片后半部分)",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100}, // [100, 200)
				{Pos: 170, ID: 2, Size: 80, Off: 0, Len: 80},   // [170, 250)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100},    // [0, 100) 空洞
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 70}, // [100, 170) 旧切片前半段
				{Pos: 170, ID: 2, Size: 80, Off: 0, Len: 80},  // [170, 250) 新切片
			},
		},
		{
			name: "中间劈裂覆盖 (新切片将旧切片劈裂为左右两段)",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 10, Len: 100 - 10}, // [100, 190), Off: 10
				{Pos: 130, ID: 2, Size: 40, Off: 0, Len: 40},         // [130, 170)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100},     // [0, 100) 空洞
				{Pos: 100, ID: 1, Size: 100, Off: 10, Len: 30}, // [100, 130) 左段
				{Pos: 130, ID: 2, Size: 40, Off: 0, Len: 40},   // [130, 170) 新切片
				{Pos: 170, ID: 1, Size: 100, Off: 80, Len: 20}, // [170, 190) 右段: Off=10+70=80, Len=90-70=20
			},
		},
		{
			name: "多层交错覆盖",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 400, Off: 0, Len: 400},  // [0, 400)
				{Pos: 50, ID: 2, Size: 200, Off: 0, Len: 200}, // [50, 250)
				{Pos: 100, ID: 3, Size: 80, Off: 0, Len: 80},  // [100, 180)
				{Pos: 70, ID: 4, Size: 70, Off: 0, Len: 70},   // [70, 140)
			},
			expected: []Slice{
				{Pos: 0, ID: 1, Size: 400, Off: 0, Len: 50},      // [0, 50)
				{Pos: 50, ID: 2, Size: 200, Off: 0, Len: 20},     // [50, 70)
				{Pos: 70, ID: 4, Size: 70, Off: 0, Len: 70},      // [70, 140)
				{Pos: 140, ID: 3, Size: 80, Off: 40, Len: 40},    // [140, 180)
				{Pos: 180, ID: 2, Size: 200, Off: 130, Len: 70},  // [180, 250)
				{Pos: 250, ID: 1, Size: 400, Off: 250, Len: 150}, // [250, 400)
			},
		},
		{
			name: "空洞(ID=0)打洞覆盖与二次覆写",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 100}, // [0, 100)
				{Pos: 20, ID: 0, Size: 0, Off: 0, Len: 50},   // 打洞 [20, 70)
				{Pos: 40, ID: 2, Size: 20, Off: 0, Len: 20},  // 在洞内覆写 [40, 60)
			},
			expected: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 20},   // [0, 20)
				{Pos: 20, ID: 0, Size: 0, Off: 0, Len: 20},    // [20, 40) 空洞
				{Pos: 40, ID: 2, Size: 20, Off: 0, Len: 20},   // [40, 60) 新切片
				{Pos: 60, ID: 0, Size: 0, Off: 0, Len: 10},    // [60, 70) 空洞
				{Pos: 70, ID: 1, Size: 100, Off: 70, Len: 30}, // [70, 100)
			},
		},
		{
			name: "跨越多个旧切片的覆盖",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100}, // [100, 200)
				{Pos: 300, ID: 2, Size: 100, Off: 0, Len: 100}, // [300, 400)
				{Pos: 150, ID: 3, Size: 200, Off: 0, Len: 200}, // [150, 350)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100},     // [0, 100) 空洞
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 50},  // [100, 150)
				{Pos: 150, ID: 3, Size: 200, Off: 0, Len: 200}, // [150, 350)
				{Pos: 350, ID: 2, Size: 100, Off: 50, Len: 50}, // [350, 400)
			},
		},
		{
			name: "包含零长度切片被忽略",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 0}, // 零长度切片跳过
				{Pos: 100, ID: 2, Size: 100, Off: 0, Len: 100},
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 100},
				{Pos: 100, ID: 2, Size: 100, Off: 0, Len: 100},
			},
		},
		{
			name: "全部切片均为零长度返回nil",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 100, Off: 0, Len: 0},
				{Pos: 100, ID: 2, Size: 100, Off: 0, Len: 0},
			},
			expected: nil,
		},
		{
			name: "连续打洞空洞切片合并",
			input: []Slice{
				{Pos: 100, ID: 1, Size: 100, Off: 0, Len: 100}, // [100, 200)
				{Pos: 50, ID: 0, Size: 0, Off: 0, Len: 50},     // 打洞 [50, 100)
				{Pos: 100, ID: 0, Size: 0, Off: 0, Len: 50},    // 打洞 [100, 150)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 150},     // [0, 50) + [50, 100) + [100, 150) 融合成 [0, 150)
				{Pos: 150, ID: 1, Size: 100, Off: 50, Len: 50}, // [150, 200)
			},
		},
		{
			name: "打洞完全覆盖整块数据",
			input: []Slice{
				{Pos: 0, ID: 1, Size: 200, Off: 0, Len: 200}, // [0, 200)
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 200},   // 打洞完全覆盖 [0, 200)
			},
			expected: []Slice{
				{Pos: 0, ID: 0, Size: 0, Off: 0, Len: 200},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildSlice(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("BuildSlice slice count mismatch: expected %d, got %d.\nResult:   %+v\nExpected: %+v",
					len(tt.expected), len(result), result, tt.expected)
			}
			for i := range tt.expected {
				if result[i] != tt.expected[i] {
					t.Fatalf("result[%d] mismatch:\ngot:      %+v\nexpected: %+v", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestCountSliceRefs(t *testing.T) {
	t.Run("空输入与零切片", func(t *testing.T) {
		refs, err := CountSliceRefs()
		if err != nil || len(refs) != 0 {
			t.Fatalf("expected empty refs, got %v, err=%v", refs, err)
		}

		refs, err = CountSliceRefs(nil, []byte{})
		if err != nil || len(refs) != 0 {
			t.Fatalf("expected empty refs, got %v, err=%v", refs, err)
		}
	})

	t.Run("多段合法切片且过滤空洞切片(ID=0)", func(t *testing.T) {
		raw1 := EncodeSlices([]Slice{
			{Pos: 0, ID: 10, Size: 100, Off: 0, Len: 50},
			{Pos: 50, ID: 0, Size: 0, Off: 0, Len: 50}, // 空洞切片不计入
			{Pos: 100, ID: 20, Size: 200, Off: 0, Len: 100},
		})
		raw2 := EncodeSlices([]Slice{
			{Pos: 200, ID: 10, Size: 100, Off: 50, Len: 50}, // ID=10 再次出现
			{Pos: 300, ID: 30, Size: 500, Off: 0, Len: 200},
		})

		refs, err := CountSliceRefs(raw1, raw2)
		if err != nil {
			t.Fatalf("CountSliceRefs failed: %v", err)
		}

		expected := map[uint64]int32{
			10: 2,
			20: 1,
			30: 1,
		}
		if len(refs) != len(expected) {
			t.Fatalf("refs count mismatch: expected %d, got %d (%v)", len(expected), len(refs), refs)
		}
		for id, expCount := range expected {
			if refs[id] != expCount {
				t.Fatalf("ref count for ID %d mismatch: expected %d, got %d", id, expCount, refs[id])
			}
		}
	})

	t.Run("非法字节流报错拦截", func(t *testing.T) {
		// 长度非法
		_, err := CountSliceRefs([]byte{1, 2, 3})
		if err == nil || !errors.Is(err, ErrInvalidSliceBytes) {
			t.Fatalf("expected ErrInvalidSliceBytes, got %v", err)
		}

		// 包含非法数据
		badSlice := make([]byte, SliceBytes) // 全部为 0，Len=0
		_, err = CountSliceRefs(badSlice)
		if err == nil || !errors.Is(err, ErrInvalidSliceData) {
			t.Fatalf("expected ErrInvalidSliceData, got %v", err)
		}
	})
}
