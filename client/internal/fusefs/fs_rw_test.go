package fusefs_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type rwMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	readFn         func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	writeFn        func(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *rwMockStorageServer) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	if m.readFn != nil {
		return m.readFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *rwMockStorageServer) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	if m.writeFn != nil {
		return m.writeFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *rwMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupRWTestRPCServer(t *testing.T, srv *rwMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Read_ChunkingAndBoundaries(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 12345
	)

	t.Run("9 MiB 大块读取精确 3 次分片 (4 MiB + 4 MiB + 1 MiB)", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024 // 9 MiB
		pattern := make([]byte, totalSize)
		for i := range pattern {
			pattern[i] = byte(i % 251)
		}

		type callRecord struct {
			ino    uint64
			fh     uint64
			offset uint64
			size   uint32
			sid    uint64
		}
		var calls []callRecord

		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				msg := req.Msg
				var sid uint64
				if msg.SessionId != nil {
					sid = *msg.SessionId
				}
				calls = append(calls, callRecord{
					ino:    msg.GetIno(),
					fh:     msg.GetFh(),
					offset: msg.GetOffset(),
					size:   msg.GetSize(),
					sid:    sid,
				})

				start := int(msg.GetOffset())
				end := start + int(msg.GetSize())
				if end > len(pattern) {
					end = len(pattern)
				}
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: pattern[start:end],
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		curSID := mgr.CurrentSessionID()
		mgr.BindHandle(testFH, curSID)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:  diskID,
			RootIno: rootIno,
		})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     totalSize,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, res)
		require.Equal(t, totalSize, res.Size())

		buf, code := res.Bytes(nil)
		require.Equal(t, fuse.OK, code)
		require.True(t, bytes.Equal(pattern, buf))

		// 验证精确 3 次 RPC 调用
		require.Len(t, calls, 3)
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 0, size: 4 * 1024 * 1024, sid: curSID}, calls[0])
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 4 * 1024 * 1024, size: 4 * 1024 * 1024, sid: curSID}, calls[1])
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 8 * 1024 * 1024, size: 1 * 1024 * 1024, sid: curSID}, calls[2])
	})

	t.Run("<= 4 MiB 单次直通读取并复用传入 buf", func(t *testing.T) {
		const readSize = 64 * 1024
		data := bytes.Repeat([]byte("BOWL_DATA_"), readSize/10)

		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Equal(t, uint64(testFH), req.Msg.GetFh())
				require.Equal(t, uint64(1024), req.Msg.GetOffset())
				require.Equal(t, uint32(readSize), req.Msg.GetSize())
				return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		buf := make([]byte, readSize)
		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   1024,
			Size:     readSize,
		}

		res, status := fs.Read(nil, input, buf)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 1, calls)
		require.Equal(t, len(data), res.Size())

		resBytes, code := res.Bytes(nil)
		require.Equal(t, fuse.OK, code)
		require.Equal(t, data, resBytes)
	})

	t.Run("want == 0 直接返回空数据不触发 RPC", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				t.Fatal("want == 0 不应触发 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     0,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, res)
		require.Equal(t, 0, res.Size())
	})

	t.Run("len(buf) > 0 且小于 input.Size 时以 len(buf) 为准截断", func(t *testing.T) {
		var reqSize uint32
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				reqSize = req.Msg.GetSize()
				return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, reqSize)}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		buf := make([]byte, 512)
		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     4096, // input.Size 较大，但 buf 仅 512
		}

		res, status := fs.Read(nil, input, buf)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(512), reqSize)
		require.Equal(t, 512, res.Size())
	})

	t.Run("中途短读与遇到 EOF 立即停止分片循环", func(t *testing.T) {
		// 请求 9 MiB，第 1 片返回 4 MiB，第 2 片返回 2 MiB (< 4 MiB 短读/EOF)
		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				if calls == 1 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 4*1024*1024)}), nil
				}
				if calls == 2 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 2*1024*1024)}), nil
				}
				t.Fatal("第 2 片短读后不应再触发第 3 次 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     9 * 1024 * 1024,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 2, calls)
		require.Equal(t, 6*1024*1024, res.Size())
	})

	t.Run("中途分片报错但 totalRead > 0 返回已读数据与 fuse.OK", func(t *testing.T) {
		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				if calls == 1 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 4*1024*1024)}), nil
				}
				return nil, makeStorageError(connect.CodeInternal, "EIO", "i/o error")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     9 * 1024 * 1024,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 2, calls)
		require.Equal(t, 4*1024*1024, res.Size())
	})

	t.Run("首片读取失败返回对应 errno", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, makeStorageError(connect.CodeInternal, "EIO", "disk read failure")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.Status(syscall.EIO), status)
	})

	t.Run("无效句柄与未绑定句柄返回 EBADF", func(t *testing.T) {
		client := setupRWTestRPCServer(t, &rwMockStorageServer{})
		mgr := setupTestSessionManager(t, client)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       999999, // 未绑定
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("只写句柄读取服务端返回 EBADF", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, makeStorageError(connect.CodeFailedPrecondition, "EBADF", "bad file descriptor")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.EBADF, status)
	})
}

func TestRawFS_Write_ChunkingAndBoundaries(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 12345
	)

	t.Run("只读模式直接返回 EROFS", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				t.Fatal("只读模式不应调用 Write RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:   diskID,
			RootIno:  rootIno,
			ReadOnly: true, // 只读挂载
		})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("hello"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("无效句柄返回 EBADF", func(t *testing.T) {
		client := setupRWTestRPCServer(t, &rwMockStorageServer{})
		mgr := setupTestSessionManager(t, client)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       999999, // 未绑定
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("hello"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("0 字节写入透传服务端返回 0 与 fuse.OK", func(t *testing.T) {
		var called bool
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				called = true
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Equal(t, uint64(testFH), req.Msg.GetFh())
				require.Equal(t, 0, len(req.Msg.GetData()))
				return connect.NewResponse(&storagev1.WriteResponse{Size: 0}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   100,
		}

		written, status := fs.Write(nil, input, []byte{})
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(0), written)
		require.True(t, called)
	})

	t.Run("<= 4 MiB 单次写入直接返回", func(t *testing.T) {
		data := []byte("Hello Bowl Direct IO")
		var called bool
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				called = true
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Equal(t, uint64(testFH), req.Msg.GetFh())
				require.Equal(t, uint64(50), req.Msg.GetOffset())
				require.Equal(t, data, req.Msg.GetData())
				return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(data))}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   50,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(data)), written)
		require.True(t, called)
	})

	t.Run("9 MiB 大块写入精确 3 次分片 (4 MiB + 4 MiB + 1 MiB)", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024 // 9 MiB
		pattern := make([]byte, totalSize)
		for i := range pattern {
			pattern[i] = byte(i % 253)
		}

		type callRecord struct {
			ino     uint64
			fh      uint64
			offset  uint64
			dataLen int
			sid     uint64
		}
		var calls []callRecord

		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				msg := req.Msg
				var sid uint64
				if msg.SessionId != nil {
					sid = *msg.SessionId
				}
				calls = append(calls, callRecord{
					ino:     msg.GetIno(),
					fh:      msg.GetFh(),
					offset:  msg.GetOffset(),
					dataLen: len(msg.GetData()),
					sid:     sid,
				})
				return connect.NewResponse(&storagev1.WriteResponse{
					Size: uint32(len(msg.GetData())),
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		curSID := mgr.CurrentSessionID()
		mgr.BindHandle(testFH, curSID)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   100,
		}

		written, status := fs.Write(nil, input, pattern)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(totalSize), written)

		// 验证精确 3 次分片与偏移计算
		require.Len(t, calls, 3)
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 100, dataLen: 4 * 1024 * 1024, sid: curSID}, calls[0])
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 100 + 4*1024*1024, dataLen: 4 * 1024 * 1024, sid: curSID}, calls[1])
		require.Equal(t, callRecord{ino: fileIno, fh: testFH, offset: 100 + 8*1024*1024, dataLen: 1 * 1024 * 1024, sid: curSID}, calls[2])
	})

	t.Run("第 3 片写入返回 ENOSPC 时返回前 8 MiB 短写与 fuse.OK", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024
		data := make([]byte, totalSize)

		var calls int
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				calls++
				if calls == 1 || calls == 2 {
					return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
				}
				// 第 3 片报错 ENOSPC
				return nil, makeStorageError(connect.CodeResourceExhausted, "ENOSPC", "disk quota exceeded")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status, "POSIX 短写在已有数据写入成功时必须返回 fuse.OK")
		require.Equal(t, uint32(8*1024*1024), written, "应返回前 2 片累计成功写入字节数 (8 MiB)")
		require.Equal(t, 3, calls)
	})

	t.Run("首片写入失败返回对应 errno 与 0 字节", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				return nil, makeStorageError(connect.CodeResourceExhausted, "ENOSPC", "disk quota exceeded")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, make([]byte, 1024))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.Status(syscall.ENOSPC), status)
	})

	t.Run("某片返回 written < len(chunk) 时立即停止分片循环", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024
		data := make([]byte, totalSize)

		var calls int
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				calls++
				if calls == 1 {
					// 仅写入 2 MiB (< 4 MiB)
					return connect.NewResponse(&storagev1.WriteResponse{Size: 2 * 1024 * 1024}), nil
				}
				t.Fatal("第 1 片短写后不应再触发后续 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(2*1024*1024), written)
		require.Equal(t, 1, calls)
	})

	t.Run("只读句柄写入服务端返回 EBADF", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				return nil, makeStorageError(connect.CodeFailedPrecondition, "EBADF", "bad file descriptor")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("data"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.EBADF, status)
	})
}
