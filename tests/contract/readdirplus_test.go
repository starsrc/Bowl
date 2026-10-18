package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestReadDirPlusRequestValidation 验证 ReadDirPlusRequest 的字段约束与边界逻辑。
func TestReadDirPlusRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validSessionID := uint64(1001)
	maxSessionID := uint64(math.MaxInt64)
	zeroSessionID := uint64(0)
	overflowSessionID := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name      string
		ino       uint64
		fh        uint64
		offset    uint64
		limit     uint32
		sessionID *uint64
		valid     bool
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
			name:      "合法: 最大合法分页上限 (limit=4096, 携带有效 session_id)",
			ino:       42,
			fh:        100,
			offset:    2,
			limit:     4096,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:      "合法: 最大 int64 边界参数与 session_id",
			ino:       math.MaxInt64,
			fh:        math.MaxInt64,
			offset:    math.MaxInt64,
			limit:     4096,
			sessionID: &maxSessionID,
			valid:     true,
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
		{
			name:      "非法: session_id 为 0",
			ino:       1,
			fh:        10,
			offset:    0,
			limit:     10,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        10,
			offset:    0,
			limit:     10,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.ReadDirPlusRequest{
				Ino:       tc.ino,
				Fh:        tc.fh,
				Offset:    tc.offset,
				Limit:     tc.limit,
				SessionId: tc.sessionID,
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

// TestDirEntryPlusValidation 验证 DirEntryPlus 的字段约束契约。
func TestDirEntryPlusValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        1,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o040755,
			Size:    4096,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Blocks:  8,
			Atime:   timestamppb.Now(),
			Mtime:   timestamppb.Now(),
			Ctime:   timestamppb.Now(),
		},
	}

	for _, tc := range []struct {
		name         string
		ino          uint64
		entryName    []byte
		nextOffset   uint64
		inode        *storagev1.Inode
		entryTimeout float64
		attrTimeout  float64
		valid        bool
	}{
		{
			name:         "合法: 目录自引用项 . (entry_timeout=0)",
			ino:          1,
			entryName:    []byte("."),
			nextOffset:   1,
			inode:        validInode,
			entryTimeout: 0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "合法: 父目录引用项 .. (entry_timeout=0)",
			ino:          1,
			entryName:    []byte(".."),
			nextOffset:   2,
			inode:        validInode,
			entryTimeout: 0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "合法: 常规文件子项 (正常缓存时效)",
			ino:          42,
			entryName:    []byte("file.txt"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "合法: 非 UTF-8 字节序列目录项名称",
			ino:          42,
			entryName:    []byte{0xff, 0xfe, 0x80, 0x81},
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "合法: 255 字节最长文件名",
			ino:          42,
			entryName:    []byte(strings.Repeat("a", 255)),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "非法: ino 为 0",
			ino:          0,
			entryName:    []byte("file.txt"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: name 为空",
			ino:          42,
			entryName:    []byte(""),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: name 包含斜杠",
			ino:          42,
			entryName:    []byte("sub/dir"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: name 包含空字节",
			ino:          42,
			entryName:    []byte("file\x00.txt"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: name 超过 255 字节",
			ino:          42,
			entryName:    []byte(strings.Repeat("a", 256)),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: next_offset 为 0",
			ino:          42,
			entryName:    []byte("file.txt"),
			nextOffset:   0,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: inode 为 nil (required)",
			ino:          42,
			entryName:    []byte("file.txt"),
			nextOffset:   3,
			inode:        nil,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: entry_timeout 为负数",
			ino:          42,
			entryName:    []byte("file.txt"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: -0.1,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: attr_timeout 为负数",
			ino:          42,
			entryName:    []byte("file.txt"),
			nextOffset:   3,
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  -0.1,
			valid:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry := &storagev1.DirEntryPlus{
				Ino:                 tc.ino,
				Name:                tc.entryName,
				NextOffset:          tc.nextOffset,
				Inode:               tc.inode,
				EntryTimeoutSeconds: tc.entryTimeout,
				AttrTimeoutSeconds:  tc.attrTimeout,
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

// TestReadDirPlusResponseValidation 验证 ReadDirPlusResponse 响应结构校验契约。
func TestReadDirPlusResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        1,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o040755,
			Size:    4096,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Blocks:  8,
			Atime:   timestamppb.Now(),
			Mtime:   timestamppb.Now(),
			Ctime:   timestamppb.Now(),
		},
	}

	t.Run("合法: 空目录项列表", func(t *testing.T) {
		resp := &storagev1.ReadDirPlusResponse{
			Entries: []*storagev1.DirEntryPlus{},
		}
		if err := validator.Validate(resp); err != nil {
			t.Fatalf("预期空响应校验通过，实际失败: %v", err)
		}
	})

	t.Run("合法: 包含有效目录项", func(t *testing.T) {
		resp := &storagev1.ReadDirPlusResponse{
			Entries: []*storagev1.DirEntryPlus{
				{
					Ino:                 1,
					Name:                []byte("."),
					NextOffset:          1,
					Inode:               validInode,
					EntryTimeoutSeconds: 0,
					AttrTimeoutSeconds:  1.0,
				},
				{
					Ino:                 1,
					Name:                []byte(".."),
					NextOffset:          2,
					Inode:               validInode,
					EntryTimeoutSeconds: 0,
					AttrTimeoutSeconds:  1.0,
				},
			},
		}
		if err := validator.Validate(resp); err != nil {
			t.Fatalf("预期校验通过，实际失败: %v", err)
		}
	})

	t.Run("非法: 列表内包含无效目录项", func(t *testing.T) {
		resp := &storagev1.ReadDirPlusResponse{
			Entries: []*storagev1.DirEntryPlus{
				{
					Ino:                 0, // 非法
					Name:                []byte("."),
					NextOffset:          1,
					Inode:               validInode,
					EntryTimeoutSeconds: 0,
					AttrTimeoutSeconds:  1.0,
				},
			},
		}
		if err := validator.Validate(resp); err == nil {
			t.Fatalf("预期包含无效条目的响应校验失败，实际通过")
		}
	})
}
