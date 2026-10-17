package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestMknodRequestValidation 验证 MknodRequest 的字段约束与边界逻辑。
func TestMknodRequestValidation(t *testing.T) {
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
		rdev      uint32
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: 普通文件",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列节点名称",
			parentIno: 1,
			entryName: []byte{0xff, 0xfe, 0x80, 0x81},
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 节点名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("a", 255)),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 多字节 UTF-8 节点名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("界", 85)),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 节点名称包含换行符",
			parentIno: 1,
			entryName: []byte("a\nb"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 父节点 parent_ino 处于正整数上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效下限 1",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      1,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效上限 0o177777 (65535)",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      65535,
			uid:       0,
			gid:       0,
			rdev:      0,
			valid:     true,
		},
		{
			name:      "合法: 设备文件携带 rdev",
			parentIno: 1,
			entryName: []byte("null"),
			mode:      0o020666,
			uid:       0,
			gid:       0,
			rdev:      12345,
			valid:     true,
		},

		// 负向边界用例 - parent_ino
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: parent_ino 为 uint64 最大值 math.MaxUint64",
			parentIno: math.MaxUint64,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			valid:     false,
		},

		// 负向边界用例 - name
		{
			name:      "非法: 节点名称为空",
			parentIno: 1,
			entryName: []byte(""),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称达到 256 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("a", 256)),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 多字节 UTF-8 节点名称超过 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("界", 86)),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称包含斜杠 /",
			parentIno: 1,
			entryName: []byte("a/b"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称以斜杠 / 开头",
			parentIno: 1,
			entryName: []byte("/file"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称中间包含空字符 NUL",
			parentIno: 1,
			entryName: []byte("a\x00b"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称仅包含空字符 NUL",
			parentIno: 1,
			entryName: []byte("\x00"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称以空字符 NUL 开头",
			parentIno: 1,
			entryName: []byte("\x00file"),
			mode:      0o100644,
			valid:     false,
		},
		{
			name:      "非法: 节点名称以空字符 NUL 结尾",
			parentIno: 1,
			entryName: []byte("file\x00"),
			mode:      0o100644,
			valid:     false,
		},

		// 负向边界用例 - mode
		{
			name:      "非法: mode 为 0",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0,
			valid:     false,
		},
		{
			name:      "非法: mode 超过 65535",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      65536,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.MknodRequest{
				ParentIno: tc.parentIno,
				Name:      tc.entryName,
				Mode:      tc.mode,
				Uid:       tc.uid,
				Gid:       tc.gid,
				Rdev:      tc.rdev,
			}
			err := validator.Validate(req)
			if tc.valid && err != nil {
				t.Fatalf("用例 %q 期望验证通过，但返回错误: %v", tc.name, err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("用例 %q 期望验证失败，但返回成功", tc.name)
			}
		})
	}
}

// TestMknodResponseValidation 验证 MknodResponse 的约束规则。
func TestMknodResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 0,
		Attr: &storagev1.InodeAttribute{
			Mode:  0o100644,
			Nlink: 1,
			Atime: timestamppb.Now(),
			Mtime: timestamppb.Now(),
			Ctime: timestamppb.Now(),
		},
	}

	for _, tc := range []struct {
		name         string
		inode        *storagev1.Inode
		entryTimeout float64
		attrTimeout  float64
		valid        bool
	}{
		{
			name:         "合法: 响应包含正常超时时长",
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        true,
		},
		{
			name:         "合法: 响应包含零超时时长",
			inode:        validInode,
			entryTimeout: 0.0,
			attrTimeout:  0.0,
			valid:        true,
		},
		{
			name:         "非法: inode 为空指针",
			inode:        nil,
			entryTimeout: 1.0,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: entry_timeout 为负数",
			inode:        validInode,
			entryTimeout: -0.1,
			attrTimeout:  1.0,
			valid:        false,
		},
		{
			name:         "非法: attr_timeout 为负数",
			inode:        validInode,
			entryTimeout: 1.0,
			attrTimeout:  -0.1,
			valid:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.MknodResponse{
				Inode:               tc.inode,
				EntryTimeoutSeconds: tc.entryTimeout,
				AttrTimeoutSeconds:  tc.attrTimeout,
			}
			err := validator.Validate(resp)
			if tc.valid && err != nil {
				t.Fatalf("用例 %q 期望验证通过，但返回错误: %v", tc.name, err)
			}
			if !tc.valid && err == nil {
				t.Fatalf("用例 %q 期望验证失败，但返回成功", tc.name)
			}
		})
	}
}
