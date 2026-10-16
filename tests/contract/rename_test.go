package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestRenameRequestValidation 验证 RenameRequest 的字段约束与边界逻辑。
func TestRenameRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name         string
		oldParentIno uint64
		oldName      string
		newParentIno uint64
		newName      string
		valid        bool
	}{
		// 正向边界用例
		{
			name:         "合法: 正常重命名请求",
			oldParentIno: 1,
			oldName:      "old_file.txt",
			newParentIno: 2,
			newName:      "new_file.txt",
			valid:        true,
		},
		{
			name:         "合法: 文件名恰好 255 字节",
			oldParentIno: 1,
			oldName:      strings.Repeat("a", 255),
			newParentIno: 2,
			newName:      strings.Repeat("b", 255),
			valid:        true,
		},
		{
			name:         "合法: Ino 处于上限 math.MaxInt64",
			oldParentIno: math.MaxInt64,
			oldName:      "a",
			newParentIno: math.MaxInt64,
			newName:      "b",
			valid:        true,
		},

		// 负向边界用例: old_parent_ino
		{
			name:         "非法: old_parent_ino 为 0",
			oldParentIno: 0,
			oldName:      "old",
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},
		{
			name:         "非法: old_parent_ino 超过 math.MaxInt64",
			oldParentIno: uint64(math.MaxInt64) + 1,
			oldName:      "old",
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},

		// 负向边界用例: new_parent_ino
		{
			name:         "非法: new_parent_ino 为 0",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: 0,
			newName:      "new",
			valid:        false,
		},
		{
			name:         "非法: new_parent_ino 超过 math.MaxInt64",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: uint64(math.MaxInt64) + 1,
			newName:      "new",
			valid:        false,
		},

		// 负向边界用例: old_name
		{
			name:         "非法: old_name 为空",
			oldParentIno: 1,
			oldName:      "",
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},
		{
			name:         "非法: old_name 超过 255 字节",
			oldParentIno: 1,
			oldName:      strings.Repeat("a", 256),
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},
		{
			name:         "非法: old_name 包含 /",
			oldParentIno: 1,
			oldName:      "sub/file",
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},
		{
			name:         "非法: old_name 包含 \\x00",
			oldParentIno: 1,
			oldName:      "file\x00name",
			newParentIno: 1,
			newName:      "new",
			valid:        false,
		},

		// 负向边界用例: new_name
		{
			name:         "非法: new_name 为空",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: 1,
			newName:      "",
			valid:        false,
		},
		{
			name:         "非法: new_name 超过 255 字节",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: 1,
			newName:      strings.Repeat("b", 256),
			valid:        false,
		},
		{
			name:         "非法: new_name 包含 /",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: 1,
			newName:      "sub/file",
			valid:        false,
		},
		{
			name:         "非法: new_name 包含 \\x00",
			oldParentIno: 1,
			oldName:      "old",
			newParentIno: 1,
			newName:      "file\x00name",
			valid:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.RenameRequest{
				OldParentIno: tc.oldParentIno,
				OldName:      tc.oldName,
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

// TestRenameResponseValidation 验证 RenameResponse 的字段约束逻辑。
func TestRenameResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.RenameResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("空响应校验失败: %v", err)
	}
}
