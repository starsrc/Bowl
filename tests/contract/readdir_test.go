package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestReadDirRequestValidation 验证 ReadDirRequest 的字段约束与边界逻辑。
func TestReadDirRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name   string
		ino    uint64
		fh     uint64
		offset uint64
		limit  uint32
		valid  bool
	}{
		// 正向边界用例
		{
			name:   "合法: 初始读取 (offset=0, limit=10)",
			ino:    1,
			fh:     10,
			offset: 0,
			limit:  10,
			valid:  true,
		},
		{
			name:   "合法: 最大合法分页上限 (limit=4096)",
			ino:    42,
			fh:     100,
			offset: 2,
			limit:  4096,
			valid:  true,
		},
		{
			name:   "合法: 最大 int64 边界参数",
			ino:    math.MaxInt64,
			fh:     math.MaxInt64,
			offset: math.MaxInt64,
			limit:  4096,
			valid:  true,
		},
		{
			name:   "合法: 最小单项配额 (limit=1)",
			ino:    1,
			fh:     1,
			offset: 0,
			limit:  1,
			valid:  true,
		},

		// 边界与防御负向用例
		{
			name:   "非法: ino 为 0",
			ino:    0,
			fh:     10,
			offset: 0,
			limit:  10,
			valid:  false,
		},
		{
			name:   "非法: ino 超过 math.MaxInt64",
			ino:    uint64(math.MaxInt64) + 1,
			fh:     10,
			offset: 0,
			limit:  10,
			valid:  false,
		},
		{
			name:   "非法: fh 为 0",
			ino:    1,
			fh:     0,
			offset: 0,
			limit:  10,
			valid:  false,
		},
		{
			name:   "非法: fh 超过 math.MaxInt64",
			ino:    1,
			fh:     uint64(math.MaxInt64) + 1,
			offset: 0,
			limit:  10,
			valid:  false,
		},
		{
			name:   "非法: offset 超过 math.MaxInt64",
			ino:    1,
			fh:     10,
			offset: uint64(math.MaxInt64) + 1,
			limit:  10,
			valid:  false,
		},
		{
			name:   "非法: limit 为 0",
			ino:    1,
			fh:     10,
			offset: 0,
			limit:  0,
			valid:  false,
		},
		{
			name:   "非法: limit 超过 4096 上限",
			ino:    1,
			fh:     10,
			offset: 0,
			limit:  4097,
			valid:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.ReadDirRequest{
				Ino:    tc.ino,
				Fh:     tc.fh,
				Offset: tc.offset,
				Limit:  tc.limit,
			}

			valErr := validator.Validate(req)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestDirEntryValidation 验证 DirEntry 的字段约束契约。
func TestDirEntryValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name       string
		ino        uint64
		entryName  []byte
		mode       uint32
		nextOffset uint64
		valid      bool
	}{
		{
			name:       "合法: 目录自引用项 .",
			ino:        1,
			entryName:  []byte("."),
			mode:       0o040755,
			nextOffset: 1,
			valid:      true,
		},
		{
			name:       "合法: 父目录引用项 ..",
			ino:        1,
			entryName:  []byte(".."),
			mode:       0o040755,
			nextOffset: 2,
			valid:      true,
		},
		{
			name:       "合法: 常规文件项",
			ino:        42,
			entryName:  []byte("file.txt"),
			mode:       0o100644,
			nextOffset: 3,
			valid:      true,
		},
		{
			name:       "合法: 非 UTF-8 字节序列目录项名称",
			ino:        42,
			entryName:  []byte{0xff, 0xfe, 0x80, 0x81},
			mode:       0o100644,
			nextOffset: 3,
			valid:      true,
		},
		{
			name:       "合法: 255 字节最长文件名",
			ino:        42,
			entryName:  []byte(strings.Repeat("a", 255)),
			mode:       0o100644,
			nextOffset: 3,
			valid:      true,
		},
		{
			name:       "非法: ino 为 0",
			ino:        0,
			entryName:  []byte("file.txt"),
			mode:       0o100644,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: name 为空",
			ino:        42,
			entryName:  []byte(""),
			mode:       0o100644,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: name 包含斜杠",
			ino:        42,
			entryName:  []byte("sub/dir"),
			mode:       0o040755,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: name 包含空字节",
			ino:        42,
			entryName:  []byte("file\x00.txt"),
			mode:       0o100644,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: name 超过 255 字节",
			ino:        42,
			entryName:  []byte(strings.Repeat("a", 256)),
			mode:       0o100644,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: mode 为 0",
			ino:        42,
			entryName:  []byte("file.txt"),
			mode:       0,
			nextOffset: 3,
			valid:      false,
		},
		{
			name:       "非法: next_offset 为 0",
			ino:        42,
			entryName:  []byte("file.txt"),
			mode:       0o100644,
			nextOffset: 0,
			valid:      false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &storagev1.DirEntry{
				Ino:        tc.ino,
				Name:       tc.entryName,
				Mode:       tc.mode,
				NextOffset: tc.nextOffset,
			}

			valErr := validator.Validate(entry)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestReadDirResponseValidation 验证 ReadDirResponse 响应结构校验契约。
func TestReadDirResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	t.Run("合法: 空目录项列表", func(t *testing.T) {
		resp := &storagev1.ReadDirResponse{
			Entries: []*storagev1.DirEntry{},
		}
		if err := validator.Validate(resp); err != nil {
			t.Fatalf("预期空响应校验通过，实际失败: %v", err)
		}
	})

	t.Run("合法: 包含有效目录项", func(t *testing.T) {
		resp := &storagev1.ReadDirResponse{
			Entries: []*storagev1.DirEntry{
				{
					Ino:        1,
					Name:       []byte("."),
					Mode:       0o040755,
					NextOffset: 1,
				},
				{
					Ino:        1,
					Name:       []byte(".."),
					Mode:       0o040755,
					NextOffset: 2,
				},
			},
		}
		if err := validator.Validate(resp); err != nil {
			t.Fatalf("预期校验通过，实际失败: %v", err)
		}
	})

	t.Run("非法: 列表内包含无效目录项", func(t *testing.T) {
		resp := &storagev1.ReadDirResponse{
			Entries: []*storagev1.DirEntry{
				{
					Ino:        0, // 非法
					Name:       []byte("."),
					Mode:       0o040755,
					NextOffset: 1,
				},
			},
		}
		if err := validator.Validate(resp); err == nil {
			t.Fatalf("预期包含无效条目的响应校验失败，实际通过")
		}
	})
}
