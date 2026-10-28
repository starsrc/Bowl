package contract_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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
		cases := []struct {
			name string
			req  *storagev1.WriteRequest
		}{
			{
				name: "典型 4 MiB 写入",
				req: &storagev1.WriteRequest{
					Ino:       1,
					Fh:        1,
					Offset:    0,
					Data:      make([]byte, 4194304),
					SessionId: proto.Uint64(math.MaxInt64),
				},
			},
			{
				name: "空数据写入 (0 字节)",
				req: &storagev1.WriteRequest{
					Ino:    10,
					Fh:     20,
					Offset: 100,
					Data:   []byte{},
				},
			},
			{
				name: "边界 offset 与小数据载荷",
				req: &storagev1.WriteRequest{
					Ino:    1,
					Fh:     1,
					Offset: math.MaxInt64 - 10,
					Data:   []byte("1234567890"),
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

// TestWriteResponseContract 验证 WriteResponse 的契约规范与 Protobuf 编解码正确性。
func TestWriteResponseContract(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	now := timestamppb.Now()
	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o100644,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Size:    4096,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
			Nlink:   1,
			Blksize: 4096,
			Blocks:  1,
			Flags:   0,
		},
	}

	t.Run("有效响应与序列化往返", func(t *testing.T) {
		cases := []struct {
			name string
			resp *storagev1.WriteResponse
		}{
			{
				name: "典型写入响应",
				resp: &storagev1.WriteResponse{
					Size:  4096,
					Inode: validInode,
				},
			},
			{
				name: "零字节写入响应",
				resp: &storagev1.WriteResponse{
					Size:  0,
					Inode: validInode,
				},
			},
			{
				name: "4 MiB 满载写入响应",
				resp: &storagev1.WriteResponse{
					Size:  4194304,
					Inode: validInode,
				},
			},
			{
				name: "4 MiB 满载写入且 Inode.Attr.Size 为 MaxInt64",
				resp: &storagev1.WriteResponse{
					Size: 4194304,
					Inode: &storagev1.Inode{
						Ino:        validInode.Ino,
						Generation: validInode.Generation,
						Attr: &storagev1.InodeAttribute{
							Mode:    validInode.Attr.Mode,
							Uid:     validInode.Attr.Uid,
							Gid:     validInode.Attr.Gid,
							Rdev:    validInode.Attr.Rdev,
							Size:    math.MaxInt64,
							Atime:   validInode.Attr.Atime,
							Mtime:   validInode.Attr.Mtime,
							Ctime:   validInode.Attr.Ctime,
							Nlink:   validInode.Attr.Nlink,
							Blksize: validInode.Attr.Blksize,
							Blocks:  validInode.Attr.Blocks,
							Flags:   validInode.Attr.Flags,
						},
					},
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				assert.NoError(t, v.Validate(tc.resp))

				wire, err := proto.Marshal(tc.resp)
				require.NoError(t, err)

				var unmarshaled storagev1.WriteResponse
				require.NoError(t, proto.Unmarshal(wire, &unmarshaled))
				assert.Equal(t, tc.resp.Size, unmarshaled.Size)
				assert.Equal(t, tc.resp.Inode.Ino, unmarshaled.Inode.Ino)
				assert.Equal(t, tc.resp.Inode.Attr.Size, unmarshaled.Inode.Attr.Size)
			})
		}
	})

	t.Run("非法参数与必填字段拦截", func(t *testing.T) {
		cases := []struct {
			name string
			resp *storagev1.WriteResponse
		}{
			{
				name: "缺失必填 inode 字段",
				resp: &storagev1.WriteResponse{
					Size:  1024,
					Inode: nil,
				},
			},
			{
				name: "size 超出 4 MiB 边界",
				resp: &storagev1.WriteResponse{
					Size:  4194305,
					Inode: validInode,
				},
			},
			{
				name: "嵌套 Inode.Ino 为 0",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        0,
						Generation: 1,
						Attr:       validInode.Attr,
					},
				},
			},
			{
				name: "嵌套 Inode.Attr.Mode 为 0",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        100,
						Generation: 1,
						Attr: &storagev1.InodeAttribute{
							Mode:    0,
							Size:    1024,
							Atime:   now,
							Mtime:   now,
							Ctime:   now,
							Nlink:   1,
							Blksize: 4096,
						},
					},
				},
			},
			{
				name: "嵌套 Inode 缺失 Attr 字段",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        100,
						Generation: 1,
						Attr:       nil,
					},
				},
			},
			{
				name: "嵌套 Inode.Attr 缺失必填 Mtime 时间戳",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        100,
						Generation: 1,
						Attr: &storagev1.InodeAttribute{
							Mode:    0o100644,
							Size:    1024,
							Atime:   now,
							Mtime:   nil,
							Ctime:   now,
							Nlink:   1,
							Blksize: 4096,
						},
					},
				},
			},
			{
				name: "嵌套 Inode.Attr.Flags 超出 MaxInt32",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        100,
						Generation: 1,
						Attr: &storagev1.InodeAttribute{
							Mode:    0o100644,
							Size:    1024,
							Atime:   now,
							Mtime:   now,
							Ctime:   now,
							Nlink:   1,
							Blksize: 4096,
							Flags:   math.MaxInt32 + 1,
						},
					},
				},
			},
			{
				name: "嵌套 Inode.Attr.Uid 超出 MaxInt32",
				resp: &storagev1.WriteResponse{
					Size: 1024,
					Inode: &storagev1.Inode{
						Ino:        100,
						Generation: 1,
						Attr: &storagev1.InodeAttribute{
							Mode:    0o100644,
							Uid:     math.MaxInt32 + 1,
							Size:    1024,
							Atime:   now,
							Mtime:   now,
							Ctime:   now,
							Nlink:   1,
							Blksize: 4096,
						},
					},
				},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				assertErrorInfoReason(t, v.Validate(tc.resp), validator.ReasonInvalidArgument)
			})
		}
	})
}
