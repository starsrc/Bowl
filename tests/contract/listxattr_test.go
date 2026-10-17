package contract_test

import (
	"bytes"
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListXattrRequestValidation 验证 ListXattrRequest 的字段约束与边界逻辑。
func TestListXattrRequestValidation(t *testing.T) {
	v, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	for _, tc := range []struct {
		name  string
		ino   uint64
		valid bool
	}{
		// 正向边界用例
		{
			name:  "合法: 最小边界参数 (ino=1)",
			ino:   1,
			valid: true,
		},
		{
			name:  "合法: ino 边界为 math.MaxInt64",
			ino:   math.MaxInt64,
			valid: true,
		},

		// 边界与防御负向用例：ino
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
		{
			name:  "非法: ino 为 uint64 最大值",
			ino:   math.MaxUint64,
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.ListXattrRequest{
				Ino: tc.ino,
			}
			err := v.Validate(req)
			if tc.valid {
				assert.NoError(t, err, "用例应校验通过: %s", tc.name)
			} else {
				assert.Error(t, err, "用例应校验失败: %s", tc.name)
			}
		})
	}
}

// TestListXattrResponseValidation 验证 ListXattrResponse 的字段约束与边界逻辑。
func TestListXattrResponseValidation(t *testing.T) {
	v, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	for _, tc := range []struct {
		name  string
		names [][]byte
		valid bool
	}{
		// 正向用例
		{
			name:  "合法: names 为空切片 (0 个属性)",
			names: [][]byte{},
			valid: true,
		},
		{
			name:  "合法: names 为 nil",
			names: nil,
			valid: true,
		},
		{
			name:  "合法: 单个最小属性名 (1 字节)",
			names: [][]byte{[]byte("a")},
			valid: true,
		},
		{
			name:  "合法: 属性名达到最大 255 字节",
			names: [][]byte{bytes.Repeat([]byte("k"), 255)},
			valid: true,
		},
		{
			name:  "合法: 属性名包含斜杠 / (xattr 允许包含斜杠)",
			names: [][]byte{[]byte("user.mime/type")},
			valid: true,
		},
		{
			name:  "合法: 属性名包含非 UTF-8 原始字节序列",
			names: [][]byte{[]byte{0xff, 0xfe, 0x80, 'a'}},
			valid: true,
		},
		{
			name: "合法: 多个不同命名空间的扩展属性名",
			names: [][]byte{
				[]byte("security.selinux"),
				[]byte("system.posix_acl_access"),
				[]byte("trusted.overlay.opaque"),
				[]byte("user.comment"),
			},
			valid: true,
		},

		// 负向用例：repeated items 约束
		{
			name:  "非法: names 包含空字节切片 (0 字节属性名)",
			names: [][]byte{[]byte{}},
			valid: false,
		},
		{
			name:  "非法: names 包含 nil 字节切片",
			names: [][]byte{nil},
			valid: false,
		},
		{
			name:  "非法: names 中属性名超过 255 字节 (256 字节)",
			names: [][]byte{bytes.Repeat([]byte("k"), 256)},
			valid: false,
		},
		{
			name:  "非法: names 中属性名开头包含空字节 NUL",
			names: [][]byte{[]byte("\x00user.test")},
			valid: false,
		},
		{
			name:  "非法: names 中属性名中间包含空字节 NUL",
			names: [][]byte{[]byte("user\x00test")},
			valid: false,
		},
		{
			name:  "非法: names 中属性名结尾包含空字节 NUL",
			names: [][]byte{[]byte("user.test\x00")},
			valid: false,
		},
		{
			name: "非法: names 中某一项合法但另一项包含空字节 NUL",
			names: [][]byte{
				[]byte("user.valid"),
				[]byte("user.in\x00valid"),
			},
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.ListXattrResponse{
				Names: tc.names,
			}
			err := v.Validate(resp)
			if tc.valid {
				assert.NoError(t, err, "用例应校验通过: %s", tc.name)
			} else {
				assert.Error(t, err, "用例应校验失败: %s", tc.name)
			}
		})
	}
}

// TestListXattr_PrecompiledValidator 验证通过预编译校验器执行 ListXattr 校验。
func TestListXattr_PrecompiledValidator(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化预编译 StorageValidator 失败")

	validReq := &storagev1.ListXattrRequest{
		Ino: 1,
	}
	assert.NoError(t, v.Validate(validReq), "合法请求通过预编译校验器应无错误")

	invalidReq := &storagev1.ListXattrRequest{
		Ino: 0,
	}
	assert.Error(t, v.Validate(invalidReq), "非法请求通过预编译校验器应报错")

	validResp := &storagev1.ListXattrResponse{
		Names: [][]byte{[]byte("user.test")},
	}
	assert.NoError(t, v.Validate(validResp), "合法响应通过预编译校验器应无错误")

	invalidResp := &storagev1.ListXattrResponse{
		Names: [][]byte{[]byte("user.\x00test")},
	}
	assert.Error(t, v.Validate(invalidResp), "非法响应通过预编译校验器应报错")
}
