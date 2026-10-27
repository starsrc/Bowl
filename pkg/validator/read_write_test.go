package validator_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func assertReason(t *testing.T, err error, expectedReason string) {
	t.Helper()
	require.Error(t, err)

	reason, ok := validator.ErrnoReason(err)
	require.True(t, ok, "ErrnoReason 必须成功提取 POSIX errno")
	assert.Equal(t, expectedReason, reason)

	st := validator.ToStatus(err)
	require.NotNil(t, st)
	assert.Equal(t, codes.InvalidArgument, st.Code())

	details := st.Details()
	require.Len(t, details, 1, "必须携带且仅携带 1 个 ErrorInfo detail")
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "detail 类型应为 *errdetails.ErrorInfo")
	assert.Equal(t, validator.ErrorDomain, info.GetDomain())
	assert.Equal(t, expectedReason, info.GetReason())
}

func TestReadRequest_Validation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("有效 ReadRequest", func(t *testing.T) {
		validCases := []struct {
			name string
			req  *storagev1.ReadRequest
		}{
			{
				name: "最小边界零长度读取",
				req: &storagev1.ReadRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Size:   0,
				},
			},
			{
				name: "常规 64 KiB 读取",
				req: &storagev1.ReadRequest{
					Ino:    100,
					Fh:     2,
					Offset: 4096,
					Size:   65536,
				},
			},
			{
				name: "上限 4 MiB (4194304) 读取且携带会话租约",
				req: &storagev1.ReadRequest{
					Ino:       math.MaxInt64,
					Fh:        math.MaxInt64,
					Offset:    math.MaxInt64,
					Size:      4194304,
					SessionId: proto.Uint64(12345),
				},
			},
			{
				name: "会话租约最大边界 (MaxInt64)",
				req: &storagev1.ReadRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Size:      1024,
					SessionId: proto.Uint64(math.MaxInt64),
				},
			},
		}

		for _, tc := range validCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.NoError(t, v.Validate(tc.req))
			})
		}
	})

	t.Run("非法 ReadRequest 拦截并映射为 EINVAL", func(t *testing.T) {
		invalidCases := []struct {
			name string
			req  *storagev1.ReadRequest
		}{
			{
				name: "ino 为 0",
				req: &storagev1.ReadRequest{
					Ino:    0,
					Fh:     1,
					Offset: 0,
					Size:   1024,
				},
			},
			{
				name: "ino 超出 MaxInt64",
				req: &storagev1.ReadRequest{
					Ino:    math.MaxInt64 + 1,
					Fh:     1,
					Offset: 0,
					Size:   1024,
				},
			},
			{
				name: "fh 为 0",
				req: &storagev1.ReadRequest{
					Ino:    1,
					Fh:     0,
					Offset: 0,
					Size:   1024,
				},
			},
			{
				name: "fh 超出 MaxInt64",
				req: &storagev1.ReadRequest{
					Ino:    1,
					Fh:     math.MaxInt64 + 1,
					Offset: 0,
					Size:   1024,
				},
			},
			{
				name: "offset 超出 MaxInt64",
				req: &storagev1.ReadRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64 + 1,
					Size:   1024,
				},
			},
			{
				name: "size 超出 4 MiB (4194305)",
				req: &storagev1.ReadRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Size:   4194305,
				},
			},
			{
				name: "session_id 为 0",
				req: &storagev1.ReadRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Size:      1024,
					SessionId: proto.Uint64(0),
				},
			},
			{
				name: "session_id 超出 MaxInt64",
				req: &storagev1.ReadRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Size:      1024,
					SessionId: proto.Uint64(math.MaxInt64 + 1),
				},
			},
		}

		for _, tc := range invalidCases {
			t.Run(tc.name, func(t *testing.T) {
				err := v.Validate(tc.req)
				assertReason(t, err, validator.ReasonInvalidArgument)
			})
		}
	})
}

func TestWriteRequest_Validation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("有效 WriteRequest", func(t *testing.T) {
		validCases := []struct {
			name string
			req  *storagev1.WriteRequest
		}{
			{
				name: "最小边界零长度写入",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Data:   []byte{},
				},
			},
			{
				name: "nil 数据切片零长度写入",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Data:   nil,
				},
			},
			{
				name: "常规 64 KiB 写入",
				req: &storagev1.WriteRequest{
					Ino:    100,
					Fh:     2,
					Offset: 4096,
					Data:   bytes.Repeat([]byte{0x5a}, 65536),
				},
			},
			{
				name: "上限 4 MiB (4194304) 写入且 offset = 0",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Data:   make([]byte, 4194304),
				},
			},
			{
				name: "边界 offset + len(data) 恰好等于 MaxInt64",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64 - 1024,
					Data:   make([]byte, 1024),
				},
			},
			{
				name: "边界 offset 等于 MaxInt64 且 len(data) 为 0",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64,
					Data:   nil,
				},
			},
			{
				name: "合法 session_id",
				req: &storagev1.WriteRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Data:      []byte("hello"),
					SessionId: proto.Uint64(math.MaxInt64),
				},
			},
		}

		for _, tc := range validCases {
			t.Run(tc.name, func(t *testing.T) {
				assert.NoError(t, v.Validate(tc.req))
			})
		}
	})

	t.Run("非法 WriteRequest 拦截并映射为 EINVAL", func(t *testing.T) {
		invalidCases := []struct {
			name string
			req  *storagev1.WriteRequest
		}{
			{
				name: "ino 为 0",
				req: &storagev1.WriteRequest{
					Ino:    0,
					Fh:     1,
					Offset: 0,
					Data:   []byte("test"),
				},
			},
			{
				name: "ino 超出 MaxInt64",
				req: &storagev1.WriteRequest{
					Ino:    math.MaxInt64 + 1,
					Fh:     1,
					Offset: 0,
					Data:   []byte("test"),
				},
			},
			{
				name: "fh 为 0",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     0,
					Offset: 0,
					Data:   []byte("test"),
				},
			},
			{
				name: "fh 超出 MaxInt64",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     math.MaxInt64 + 1,
					Offset: 0,
					Data:   []byte("test"),
				},
			},
			{
				name: "offset 超出 MaxInt64",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64 + 1,
					Data:   []byte("test"),
				},
			},
			{
				name: "data 长度超出 4 MiB (4194305 字节)",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: 0,
					Data:   make([]byte, 4194305),
				},
			},
			{
				name: "session_id 为 0",
				req: &storagev1.WriteRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Data:      []byte("test"),
					SessionId: proto.Uint64(0),
				},
			},
			{
				name: "session_id 超出 MaxInt64",
				req: &storagev1.WriteRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Data:      []byte("test"),
					SessionId: proto.Uint64(math.MaxInt64 + 1),
				},
			},
			{
				name: "CEL 校验: offset + len(data) 溢出 MaxInt64 (MaxInt64 + 1 字节)",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64,
					Data:   []byte("x"),
				},
			},
			{
				name: "CEL 校验: offset + len(data) 溢出 MaxInt64 (MaxInt64 - 5 且载荷 6 字节)",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64 - 5,
					Data:   make([]byte, 6),
				},
			},
		}

		for _, tc := range invalidCases {
			t.Run(tc.name, func(t *testing.T) {
				err := v.Validate(tc.req)
				assertReason(t, err, validator.ReasonInvalidArgument)
			})
		}
	})
}
