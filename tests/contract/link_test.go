package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestLinkRequestValidation 验证 LinkRequest 的字段约束与边界逻辑。
func TestLinkRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name         string
		ino          uint64
		newParentIno uint64
		newName      string
		valid        bool
	}{
		// 正向边界用例
		{
			name:         "合法: 正常创建硬链接请求",
			ino:          100,
			newParentIno: 1,
			newName:      "hardlink.txt",
			valid:        true,
		},
		{
			name:         "合法: new_name 恰好 255 字节",
			ino:          100,
			newParentIno: 1,
			newName:      strings.Repeat("h", 255),
			valid:        true,
		},
		{
			name:         "合法: ino 与 new_parent_ino 处于上限 math.MaxInt64",
			ino:          math.MaxInt64,
			newParentIno: math.MaxInt64,
			newName:      "link",
			valid:        true,
		},

		// 负向边界用例: ino
		{
			name:         "非法: ino 为 0",
			ino:          0,
			newParentIno: 1,
			newName:      "link",
			valid:        false,
		},
		{
			name:         "非法: ino 超过 math.MaxInt64",
			ino:          uint64(math.MaxInt64) + 1,
			newParentIno: 1,
			newName:      "link",
			valid:        false,
		},

		// 负向边界用例: new_parent_ino
		{
			name:         "非法: new_parent_ino 为 0",
			ino:          100,
			newParentIno: 0,
			newName:      "link",
			valid:        false,
		},
		{
			name:         "非法: new_parent_ino 超过 math.MaxInt64",
			ino:          100,
			newParentIno: uint64(math.MaxInt64) + 1,
			newName:      "link",
			valid:        false,
		},

		// 负向边界用例: new_name
		{
			name:         "非法: new_name 为空",
			ino:          100,
			newParentIno: 1,
			newName:      "",
			valid:        false,
		},
		{
			name:         "非法: new_name 超过 255 字节",
			ino:          100,
			newParentIno: 1,
			newName:      strings.Repeat("a", 256),
			valid:        false,
		},
		{
			name:         "非法: new_name 包含 /",
			ino:          100,
			newParentIno: 1,
			newName:      "sub/link",
			valid:        false,
		},
		{
			name:         "非法: new_name 包含 \\x00",
			ino:          100,
			newParentIno: 1,
			newName:      "link\x00name",
			valid:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.LinkRequest{
				Ino:          tc.ino,
				NewParentIno: tc.newParentIno,
				NewName:      tc.newName,
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

// TestLinkResponseValidation 验证 LinkResponse 的字段约束逻辑。
func TestLinkResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o100644,
			Size:    1024,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Blocks:  2,
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
			entryTimeoutSeconds: -0.1,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: attrTimeoutSeconds 为负数",
			inode:               validInode,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  -0.1,
			valid:               false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.LinkResponse{
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
