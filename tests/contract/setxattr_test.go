package contract_test

import (
	"bytes"
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSetXattrRequestValidation 验证 SetXattrRequest 的字段约束与边界逻辑。
func TestSetXattrRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	for _, tc := range []struct {
		name  string
		ino   uint64
		xName []byte
		value []byte
		flags uint32
		valid bool
	}{
		// 正向边界用例
		{
			name:  "合法: 最小边界参数 (name=1字节, value=0字节, flags=0)",
			ino:   1,
			xName: []byte("a"),
			value: []byte{},
			flags: 0,
			valid: true,
		},
		{
			name:  "合法: value 为 nil 空切片",
			ino:   1,
			xName: []byte("user.test"),
			value: nil,
			flags: 0,
			valid: true,
		},
		{
			name:  "合法: name 长度达到最大 255 字节",
			ino:   42,
			xName: bytes.Repeat([]byte("n"), 255),
			value: []byte("hello"),
			flags: 1,
			valid: true,
		},
		{
			name:  "合法: name 包含斜杠 / (xattr 允许包含斜杠)",
			ino:   42,
			xName: []byte("user.mime/type"),
			value: []byte("text/plain"),
			flags: 0,
			valid: true,
		},
		{
			name:  "合法: name 包含非 UTF-8 原始字节序列",
			ino:   42,
			xName: []byte{0xff, 0xfe, 0x80, 'a'},
			value: []byte{0x00, 0xff},
			flags: 2,
			valid: true,
		},
		{
			name:  "合法: value 长度达到最大 65536 字节",
			ino:   100,
			xName: []byte("user.big"),
			value: bytes.Repeat([]byte("v"), 65536),
			flags: 0,
			valid: true,
		},
		{
			name:  "合法: ino 边界为 math.MaxInt64 且 flags=2 (XATTR_REPLACE)",
			ino:   math.MaxInt64,
			xName: []byte("user.both_flags"),
			value: []byte("val"),
			flags: 2,
			valid: true,
		},
		{
			name:  "合法: flags 为 3 (XATTR_CREATE | XATTR_REPLACE，允许放行至领域层)",
			ino:   1,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: 3,
			valid: true,
		},

		// 边界与防御负向用例：ino
		{
			name:  "非法: ino 为 0",
			ino:   0,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: ino 为 uint64 最大值",
			ino:   math.MaxUint64,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},

		// 边界与防御负向用例：name
		{
			name:  "非法: name 为空切片 (0 字节)",
			ino:   1,
			xName: []byte{},
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: name 为 nil (0 字节)",
			ino:   1,
			xName: nil,
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: name 超过 255 字节 (256 字节)",
			ino:   1,
			xName: bytes.Repeat([]byte("n"), 256),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: name 开头包含空字节 NUL",
			ino:   1,
			xName: []byte("\x00user.test"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: name 中间包含空字节 NUL",
			ino:   1,
			xName: []byte("user\x00test"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},
		{
			name:  "非法: name 结尾包含空字节 NUL",
			ino:   1,
			xName: []byte("user.test\x00"),
			value: []byte("val"),
			flags: 0,
			valid: false,
		},

		// 边界与防御负向用例：value
		{
			name:  "非法: value 超过 65536 字节 (65537 字节)",
			ino:   1,
			xName: []byte("user.test"),
			value: bytes.Repeat([]byte("v"), 65537),
			flags: 0,
			valid: false,
		},

		// 边界与防御负向用例：flags
		{
			name:  "非法: flags 超过 3 (flags=4)",
			ino:   1,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: 4,
			valid: false,
		},
		{
			name:  "非法: flags 为 uint32 最大值",
			ino:   1,
			xName: []byte("user.test"),
			value: []byte("val"),
			flags: math.MaxUint32,
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.SetXattrRequest{
				Ino:   tc.ino,
				Name:  tc.xName,
				Value: tc.value,
				Flags: tc.flags,
			}
			err := validator.Validate(req)
			if tc.valid {
				assert.NoError(t, err, "用例应校验通过: %s", tc.name)
			} else {
				assert.Error(t, err, "用例应校验失败: %s", tc.name)
			}
		})
	}
}

// TestSetXattrResponseValidation 验证 SetXattrResponse 结构校验契约。
func TestSetXattrResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	resp := &storagev1.SetXattrResponse{}
	err = validator.Validate(resp)
	assert.NoError(t, err, "SetXattrResponse 应合法")
}
