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

// TestGetXattrRequestValidation 验证 GetXattrRequest 的字段约束与边界逻辑。
func TestGetXattrRequestValidation(t *testing.T) {
	v, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	for _, tc := range []struct {
		name  string
		ino   uint64
		xName []byte
		valid bool
	}{
		// 正向边界用例
		{
			name:  "合法: 最小边界参数 (name=1字节, ino=1)",
			ino:   1,
			xName: []byte("a"),
			valid: true,
		},
		{
			name:  "合法: name 长度达到最大 255 字节",
			ino:   42,
			xName: bytes.Repeat([]byte("n"), 255),
			valid: true,
		},
		{
			name:  "合法: name 包含斜杠 / (xattr 允许包含斜杠)",
			ino:   42,
			xName: []byte("user.mime/type"),
			valid: true,
		},
		{
			name:  "合法: name 包含非 UTF-8 原始字节序列",
			ino:   42,
			xName: []byte{0xff, 0xfe, 0x80, 'a'},
			valid: true,
		},
		{
			name:  "合法: ino 边界为 math.MaxInt64",
			ino:   math.MaxInt64,
			xName: []byte("user.test"),
			valid: true,
		},

		// 边界与防御负向用例：ino
		{
			name:  "非法: ino 为 0",
			ino:   0,
			xName: []byte("user.test"),
			valid: false,
		},
		{
			name:  "非法: ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			xName: []byte("user.test"),
			valid: false,
		},
		{
			name:  "非法: ino 为 uint64 最大值",
			ino:   math.MaxUint64,
			xName: []byte("user.test"),
			valid: false,
		},

		// 边界与防御负向用例：name
		{
			name:  "非法: name 为空切片 (0 字节)",
			ino:   1,
			xName: []byte{},
			valid: false,
		},
		{
			name:  "非法: name 为 nil (0 字节)",
			ino:   1,
			xName: nil,
			valid: false,
		},
		{
			name:  "非法: name 超过 255 字节 (256 字节)",
			ino:   1,
			xName: bytes.Repeat([]byte("n"), 256),
			valid: false,
		},
		{
			name:  "非法: name 开头包含空字节 NUL",
			ino:   1,
			xName: []byte("\x00user.test"),
			valid: false,
		},
		{
			name:  "非法: name 中间包含空字节 NUL",
			ino:   1,
			xName: []byte("user\x00test"),
			valid: false,
		},
		{
			name:  "非法: name 结尾包含空字节 NUL",
			ino:   1,
			xName: []byte("user.test\x00"),
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.GetXattrRequest{
				Ino:  tc.ino,
				Name: tc.xName,
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

// TestGetXattrResponseValidation 验证 GetXattrResponse 的字段约束与边界逻辑。
func TestGetXattrResponseValidation(t *testing.T) {
	v, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	for _, tc := range []struct {
		name  string
		value []byte
		valid bool
	}{
		{
			name:  "合法: value 为空切片 (0 字节)",
			value: []byte{},
			valid: true,
		},
		{
			name:  "合法: value 为 nil",
			value: nil,
			valid: true,
		},
		{
			name:  "合法: value 长度达到最大 65536 字节",
			value: bytes.Repeat([]byte("v"), 65536),
			valid: true,
		},
		{
			name:  "合法: value 包含非 UTF-8 原始字节序列与 NUL 字节",
			value: []byte{0x00, 0xff, 0xfe, 0x80},
			valid: true,
		},
		{
			name:  "非法: value 超过 65536 字节 (65537 字节)",
			value: bytes.Repeat([]byte("v"), 65537),
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.GetXattrResponse{
				Value: tc.value,
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

// TestGetXattr_PrecompiledValidator 验证通过预编译校验器执行 GetXattr 校验。
func TestGetXattr_PrecompiledValidator(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化预编译 StorageValidator 失败")

	validReq := &storagev1.GetXattrRequest{
		Ino:  1,
		Name: []byte("user.test"),
	}
	assert.NoError(t, v.Validate(validReq), "合法请求通过预编译校验器应无错误")

	invalidReq := &storagev1.GetXattrRequest{
		Ino:  0,
		Name: []byte("user.test"),
	}
	assert.Error(t, v.Validate(invalidReq), "非法请求通过预编译校验器应报错")

	validResp := &storagev1.GetXattrResponse{
		Value: []byte("value"),
	}
	assert.NoError(t, v.Validate(validResp), "合法响应通过预编译校验器应无错误")

	invalidResp := &storagev1.GetXattrResponse{
		Value: bytes.Repeat([]byte("x"), 65537),
	}
	assert.Error(t, v.Validate(invalidResp), "非法响应通过预编译校验器应报错")
}
