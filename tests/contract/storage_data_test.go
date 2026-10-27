package contract_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

// TestReadRequestContract 验证 ReadRequest 的契约边界与 protovalidate 校验规则。
func TestReadRequestContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("有效请求", func(t *testing.T) {
		cases := []struct {
			name string
			req  *storagev1.ReadRequest
		}{
			{
				name: "典型 4 MiB 满载读取",
				req: &storagev1.ReadRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Size:      4194304,
					SessionId: proto.Uint64(1),
				},
			},
			{
				name: "0 字节 EOF 探测读取",
				req: &storagev1.ReadRequest{
					Ino:       100,
					Fh:        200,
					Offset:    1024,
					Size:      0,
					SessionId: proto.Uint64(10),
				},
			},
			{
				name: "最大合法 offset 边界",
				req: &storagev1.ReadRequest{
					Ino:    math.MaxInt64,
					Fh:     math.MaxInt64,
					Offset: math.MaxInt64,
					Size:   1024,
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				assert.NoError(t, v.Validate(tc.req))
			})
		}
	})

	t.Run("非法参数拦截", func(t *testing.T) {
		cases := []struct {
			name string
			req  *storagev1.ReadRequest
		}{
			{name: "ino 为 0", req: &storagev1.ReadRequest{Ino: 0, Fh: 1, Offset: 0, Size: 1024}},
			{name: "ino > MaxInt64", req: &storagev1.ReadRequest{Ino: math.MaxInt64 + 1, Fh: 1, Offset: 0, Size: 1024}},
			{name: "fh 为 0", req: &storagev1.ReadRequest{Ino: 1, Fh: 0, Offset: 0, Size: 1024}},
			{name: "fh > MaxInt64", req: &storagev1.ReadRequest{Ino: 1, Fh: math.MaxInt64 + 1, Offset: 0, Size: 1024}},
			{name: "offset > MaxInt64", req: &storagev1.ReadRequest{Ino: 1, Fh: 1, Offset: math.MaxInt64 + 1, Size: 1024}},
			{name: "size > 4 MiB", req: &storagev1.ReadRequest{Ino: 1, Fh: 1, Offset: 0, Size: 4194305}},
			{name: "session_id 为 0", req: &storagev1.ReadRequest{Ino: 1, Fh: 1, Offset: 0, Size: 1024, SessionId: proto.Uint64(0)}},
			{name: "session_id > MaxInt64", req: &storagev1.ReadRequest{Ino: 1, Fh: 1, Offset: 0, Size: 1024, SessionId: proto.Uint64(math.MaxInt64 + 1)}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				assertErrorInfoReason(t, v.Validate(tc.req), validator.ReasonInvalidArgument)
			})
		}
	})
}

// TestReadResponseContract 验证 ReadResponse 的契约规范与 Protobuf 编解码正确性。
func TestReadResponseContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("有效空载荷响应", func(t *testing.T) {
		resp := &storagev1.ReadResponse{
			Data: []byte{},
		}
		assert.NoError(t, v.Validate(resp))

		wire, err := proto.Marshal(resp)
		require.NoError(t, err)

		var unmarshaled storagev1.ReadResponse
		require.NoError(t, proto.Unmarshal(wire, &unmarshaled))
		assert.Empty(t, unmarshaled.Data)
	})

	t.Run("有效 4 MiB 满载数据响应序列化往返", func(t *testing.T) {
		data := bytes.Repeat([]byte{0xAB}, 4194304)
		resp := &storagev1.ReadResponse{
			Data: data,
		}
		assert.NoError(t, v.Validate(resp))

		wire, err := proto.Marshal(resp)
		require.NoError(t, err)

		var unmarshaled storagev1.ReadResponse
		require.NoError(t, proto.Unmarshal(wire, &unmarshaled))
		assert.Equal(t, data, unmarshaled.Data)
	})
}

// TestWriteRequestContract 验证 WriteRequest 的契约边界与 protovalidate/CEL 校验规则。
func TestWriteRequestContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("有效请求", func(t *testing.T) {
		req := &storagev1.WriteRequest{
			Ino:       1,
			Fh:        1,
			Offset:    0,
			Data:      make([]byte, 4194304),
			SessionId: proto.Uint64(math.MaxInt64),
		}
		assert.NoError(t, v.Validate(req))
	})

	t.Run("非法参数拦截", func(t *testing.T) {
		cases := []struct {
			name string
			req  *storagev1.WriteRequest
		}{
			{name: "ino 为 0", req: &storagev1.WriteRequest{Ino: 0, Fh: 1, Offset: 0, Data: []byte("a")}},
			{name: "ino > MaxInt64", req: &storagev1.WriteRequest{Ino: math.MaxInt64 + 1, Fh: 1, Offset: 0, Data: []byte("a")}},
			{name: "fh 为 0", req: &storagev1.WriteRequest{Ino: 1, Fh: 0, Offset: 0, Data: []byte("a")}},
			{name: "fh > MaxInt64", req: &storagev1.WriteRequest{Ino: 1, Fh: math.MaxInt64 + 1, Offset: 0, Data: []byte("a")}},
			{name: "offset > MaxInt64", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: math.MaxInt64 + 1, Data: []byte("a")}},
			{name: "data > 4 MiB", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: 0, Data: make([]byte, 4194305)}},
			{name: "session_id 为 0", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: 0, Data: []byte("a"), SessionId: proto.Uint64(0)}},
			{name: "session_id > MaxInt64", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: 0, Data: []byte("a"), SessionId: proto.Uint64(math.MaxInt64 + 1)}},
			{name: "CEL 校验: offset + len(data) 溢出 MaxInt64", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: math.MaxInt64, Data: []byte("a")}},
			{name: "CEL 校验: offset + len(data) 溢出 MaxInt64 (大载荷)", req: &storagev1.WriteRequest{Ino: 1, Fh: 1, Offset: math.MaxInt64 - 100, Data: bytes.Repeat([]byte("x"), 101)}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				assertErrorInfoReason(t, v.Validate(tc.req), validator.ReasonInvalidArgument)
			})
		}
	})
}
