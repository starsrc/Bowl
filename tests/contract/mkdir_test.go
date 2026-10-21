package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestMkdirRequestValidation 验证 MkdirRequest 的字段约束与边界逻辑。
func TestMkdirRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name      string
		parentIno uint64
		entryName []byte
		mode      uint32
		uid       uint32
		gid       uint32
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: 子目录标准模式",
			parentIno: 1,
			entryName: []byte("subdir"),
			mode:      0o040755,
			uid:       1000,
			gid:       1000,
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列目录名称",
			parentIno: 1,
			entryName: []byte{0xff, 0xfe, 0x80, 0x81},
			mode:      0o040755,
			uid:       1000,
			gid:       1000,
			valid:     true,
		},
		{
			name:      "合法: 目录名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("d", 255)),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: 多字节 UTF-8 目录名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("字", 85)),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: 目录名称包含换行符",
			parentIno: 1,
			entryName: []byte("dir\nname"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: 父节点 parent_ino 处于正整数上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			entryName: []byte("dir"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效下限 1",
			parentIno: 1,
			entryName: []byte("dir"),
			mode:      1,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效上限 0o177777 (65535)",
			parentIno: 1,
			entryName: []byte("dir"),
			mode:      65535,
			uid:       0,
			gid:       0,
			valid:     true,
		},

		// 负向边界用例: parent_ino 校验
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			entryName: []byte("dir"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			entryName: []byte("dir"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},

		// 负向边界用例: name 校验
		{
			name:      "非法: name 为空字符串",
			parentIno: 1,
			entryName: []byte(""),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: name 长度超过 255 字节 (256 字节)",
			parentIno: 1,
			entryName: []byte(strings.Repeat("a", 256)),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: name 包含路径分隔符 /",
			parentIno: 1,
			entryName: []byte("sub/dir"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: name 仅为单个路径分隔符 /",
			parentIno: 1,
			entryName: []byte("/"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: name 包含空字节 \\x00",
			parentIno: 1,
			entryName: []byte("dir\x00name"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},
		{
			name:      "非法: name 仅包含空字节 \\x00",
			parentIno: 1,
			entryName: []byte("\x00"),
			mode:      0o040755,
			uid:       0,
			gid:       0,
			valid:     false,
		},

		{
			name:      "合法: mode 处于有效下限 0 (零权限目录 0000)",
			parentIno: 1,
			entryName: []byte("dir"),
			mode:      0,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: mode 携带高位 (如 65536 或 S_IFDIR 0o040755)",
			parentIno: 1,
			entryName: []byte("dir"),
			mode:      65536,
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "非法: uid 超过 math.MaxInt32",
			parentIno: 1,
			entryName: []byte("dir"),
			mode:      0o755,
			uid:       2147483648,
			gid:       0,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.MkdirRequest{
				ParentIno: tc.parentIno,
				Name:      tc.entryName,
				Mode:      tc.mode,
				Uid:       tc.uid,
				Gid:       tc.gid,
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

// TestMkdirResponseValidation 验证 MkdirResponse 的字段约束逻辑。
func TestMkdirResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o040755,
			Size:    0,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Blocks:  0,
			Atime:   timestamppb.Now(),
			Mtime:   timestamppb.Now(),
			Ctime:   timestamppb.Now(),
		},
	}

	for _, tc := range []struct {
		name                string
		inode               *storagev1.Inode
		entryTimeoutSeconds float64
		attrTimeoutSeconds  float64
		valid               bool
	}{
		{
			name:                "合法: 完整 Inode 且超时时长大于等于 0",
			inode:               validInode,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               true,
		},
		{
			name:                "合法: 超时时长为 0",
			inode:               validInode,
			entryTimeoutSeconds: 0.0,
			attrTimeoutSeconds:  0.0,
			valid:               true,
		},
		{
			name:                "非法: 缺失 Inode 必填字段",
			inode:               nil,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: 目录项超时时长为负数",
			inode:               validInode,
			entryTimeoutSeconds: -0.1,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: 属性超时时长为负数",
			inode:               validInode,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  -0.1,
			valid:               false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.MkdirResponse{
				Inode:               tc.inode,
				EntryTimeoutSeconds: tc.entryTimeoutSeconds,
				AttrTimeoutSeconds:  tc.attrTimeoutSeconds,
			}

			valErr := validator.Validate(resp)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}
