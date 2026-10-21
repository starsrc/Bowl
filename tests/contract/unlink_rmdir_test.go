package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestUnlinkRequestValidation 验证 UnlinkRequest 的字段约束与边界逻辑。
func TestUnlinkRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name      string
		parentIno uint64
		reqName   []byte
		valid     bool
	}{
		{
			name:      "合法: 正常删除非目录项请求",
			parentIno: 1,
			reqName:   []byte("file.txt"),
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列删除非目录项请求",
			parentIno: 1,
			reqName:   []byte{0xff, 0xfe, 0x80, 0x81},
			valid:     true,
		},
		{
			name:      "合法: name 恰好 255 字节",
			parentIno: 1,
			reqName:   []byte(strings.Repeat("f", 255)),
			valid:     true,
		},
		{
			name:      "合法: parent_ino 处于上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			reqName:   []byte("file.txt"),
			valid:     true,
		},
		// 负向用例: parent_ino
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			reqName:   []byte("file.txt"),
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			reqName:   []byte("file.txt"),
			valid:     false,
		},
		// 边界用例: name 为空允许进入领域层先校验父目录后返回 ENOENT
		{
			name:      "合法: name 为空允许进入领域层",
			parentIno: 1,
			reqName:   []byte(""),
			valid:     true,
		},
		{
			name:      "非法: name 超过 255 字节",
			parentIno: 1,
			reqName:   []byte(strings.Repeat("a", 256)),
			valid:     false,
		},
		{
			name:      "非法: name 包含 /",
			parentIno: 1,
			reqName:   []byte("sub/file"),
			valid:     false,
		},
		{
			name:      "非法: name 包含 \\x00",
			parentIno: 1,
			reqName:   []byte("file\x00name"),
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.UnlinkRequest{
				ParentIno: tc.parentIno,
				Name:      tc.reqName,
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

// TestUnlinkResponseValidation 验证 UnlinkResponse 的字段约束逻辑。
func TestUnlinkResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.UnlinkResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("空响应校验失败: %v", err)
	}
}

// TestRmdirRequestValidation 验证 RmdirRequest 的字段约束与边界逻辑。
func TestRmdirRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name      string
		parentIno uint64
		reqName   []byte
		valid     bool
	}{
		{
			name:      "合法: 正常删除空目录请求",
			parentIno: 1,
			reqName:   []byte("sub_dir"),
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列删除空目录请求",
			parentIno: 1,
			reqName:   []byte{0xff, 0xfe, 0x80, 0x81},
			valid:     true,
		},
		{
			name:      "合法: name 恰好 255 字节",
			parentIno: 1,
			reqName:   []byte(strings.Repeat("d", 255)),
			valid:     true,
		},
		{
			name:      "合法: parent_ino 处于上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			reqName:   []byte("sub_dir"),
			valid:     true,
		},
		// 负向用例: parent_ino
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			reqName:   []byte("sub_dir"),
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			reqName:   []byte("sub_dir"),
			valid:     false,
		},
		// 边界用例: name 为空允许进入领域层先校验父目录后返回 ENOENT
		{
			name:      "合法: name 为空允许进入领域层",
			parentIno: 1,
			reqName:   []byte(""),
			valid:     true,
		},
		{
			name:      "非法: name 超过 255 字节",
			parentIno: 1,
			reqName:   []byte(strings.Repeat("a", 256)),
			valid:     false,
		},
		{
			name:      "非法: name 包含 /",
			parentIno: 1,
			reqName:   []byte("sub/dir"),
			valid:     false,
		},
		{
			name:      "非法: name 包含 \\x00",
			parentIno: 1,
			reqName:   []byte("dir\x00name"),
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.RmdirRequest{
				ParentIno: tc.parentIno,
				Name:      tc.reqName,
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

// TestRmdirResponseValidation 验证 RmdirResponse 的字段约束逻辑。
func TestRmdirResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.RmdirResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("空响应校验失败: %v", err)
	}
}
