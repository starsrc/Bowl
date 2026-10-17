package contract_test

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestSymlinkRequestValidation 验证 SymlinkRequest 的字段约束与边界逻辑。
func TestSymlinkRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name      string
		parentIno uint64
		entryName []byte
		target    []byte
		uid       uint32
		gid       uint32
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: 相对路径符号链接",
			parentIno: 1,
			entryName: []byte("link_rel"),
			target:    []byte("../foo/bar.txt"),
			uid:       1000,
			gid:       1000,
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列符号链接名称",
			parentIno: 1,
			entryName: []byte{0xff, 0xfe, 0x80, 0x81},
			target:    []byte("target"),
			uid:       1000,
			gid:       1000,
			valid:     true,
		},
		{
			name:      "合法: 绝对路径符号链接",
			parentIno: 1,
			entryName: []byte("link_abs"),
			target:    []byte("/usr/local/bin/app"),
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: target 恰好 4095 字节",
			parentIno: 1,
			entryName: []byte("link_long"),
			target:    bytes.Repeat([]byte("a"), 4095),
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: target 为非 UTF-8 原始字节序列",
			parentIno: 1,
			entryName: []byte("link_raw_bytes"),
			target:    []byte{0xff, 0xfe, 0xfd, 0x80, 0x81, 'a', 'b'},
			uid:       1000,
			gid:       1000,
			valid:     true,
		},
		{
			name:      "合法: name 恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("l", 255)),
			target:    []byte("target"),
			uid:       0,
			gid:       0,
			valid:     true,
		},
		{
			name:      "合法: parent_ino 处于上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			entryName: []byte("link"),
			target:    []byte("target"),
			uid:       0,
			gid:       0,
			valid:     true,
		},

		// 负向边界用例: parent_ino
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			entryName: []byte("link"),
			target:    []byte("target"),
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			entryName: []byte("link"),
			target:    []byte("target"),
			valid:     false,
		},

		// 负向边界用例: name
		{
			name:      "非法: name 为空",
			parentIno: 1,
			entryName: []byte(""),
			target:    []byte("target"),
			valid:     false,
		},
		{
			name:      "非法: name 超过 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("a", 256)),
			target:    []byte("target"),
			valid:     false,
		},
		{
			name:      "非法: name 包含 /",
			parentIno: 1,
			entryName: []byte("dir/link"),
			target:    []byte("target"),
			valid:     false,
		},
		{
			name:      "非法: name 包含 \\x00",
			parentIno: 1,
			entryName: []byte("link\x00name"),
			target:    []byte("target"),
			valid:     false,
		},

		// 负向边界用例: target
		{
			name:      "非法: target 为 nil",
			parentIno: 1,
			entryName: []byte("link"),
			target:    nil,
			valid:     false,
		},
		{
			name:      "非法: target 为空",
			parentIno: 1,
			entryName: []byte("link"),
			target:    []byte{},
			valid:     false,
		},
		{
			name:      "非法: target 恰好 4096 字节 (超过 4095 上限)",
			parentIno: 1,
			entryName: []byte("link"),
			target:    bytes.Repeat([]byte("a"), 4096),
			valid:     false,
		},
		{
			name:      "非法: target 超过 4095 字节",
			parentIno: 1,
			entryName: []byte("link"),
			target:    bytes.Repeat([]byte("a"), 4097),
			valid:     false,
		},
		{
			name:      "非法: target 包含 \\x00",
			parentIno: 1,
			entryName: []byte("link"),
			target:    []byte("/path/\x00/file"),
			valid:     false,
		},
		{
			name:      "非法: target 非 UTF-8 字节中包含 \\x00",
			parentIno: 1,
			entryName: []byte("link"),
			target:    []byte{0xff, 0x00, 0xfe},
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.SymlinkRequest{
				ParentIno: tc.parentIno,
				Name:      tc.entryName,
				Target:    tc.target,
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

// TestSymlinkResponseValidation 验证 SymlinkResponse 的字段约束逻辑。
func TestSymlinkResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o120777,
			Size:    6,
			Nlink:   1,
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
			name:                "合法: 正常响应",
			inode:               validInode,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               true,
		},
		{
			name:                "非法: 缺少 inode",
			inode:               nil,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: entryTimeoutSeconds 为负数",
			inode:               validInode,
			entryTimeoutSeconds: -0.5,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: attrTimeoutSeconds 为负数",
			inode:               validInode,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  -0.5,
			valid:               false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.SymlinkResponse{
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

// TestReadlinkRequestValidation 验证 ReadlinkRequest 的字段约束与边界逻辑。
func TestReadlinkRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name  string
		ino   uint64
		valid bool
	}{
		{
			name:  "合法: ino 正整数",
			ino:   100,
			valid: true,
		},
		{
			name:  "合法: ino 处于上限 math.MaxInt64",
			ino:   math.MaxInt64,
			valid: true,
		},
		{
			name:  "非法: ino 为 0",
			ino:   0,
			valid: false,
		},
		{
			name:  "非法: ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.ReadlinkRequest{
				Ino: tc.ino,
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

// TestReadlinkResponseValidation 验证 ReadlinkResponse 的字段约束逻辑。
func TestReadlinkResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name   string
		target []byte
		valid  bool
	}{
		{
			name:   "合法: 非空 target",
			target: []byte("/etc/passwd"),
			valid:  true,
		},
		{
			name:   "合法: target 恰好 4095 字节",
			target: bytes.Repeat([]byte("a"), 4095),
			valid:  true,
		},
		{
			name:   "合法: target 为非 UTF-8 原始字节序列",
			target: []byte{0xfe, 0xed, 0xfa, 0xce, '/', 0x80},
			valid:  true,
		},
		{
			name:   "非法: target 为 nil",
			target: nil,
			valid:  false,
		},
		{
			name:   "非法: 空 target",
			target: []byte{},
			valid:  false,
		},
		{
			name:   "非法: target 恰好 4096 字节 (超过 4095 上限)",
			target: bytes.Repeat([]byte("a"), 4096),
			valid:  false,
		},
		{
			name:   "非法: target 超过 4095 字节",
			target: bytes.Repeat([]byte("a"), 4097),
			valid:  false,
		},
		{
			name:   "非法: target 包含 \\x00",
			target: []byte("path/\x00/foo"),
			valid:  false,
		},
		{
			name:   "非法: target 非 UTF-8 字节中包含 \\x00",
			target: []byte{0xff, 0x00, 0xfe},
			valid:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.ReadlinkResponse{
				Target: tc.target,
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
