package fusefs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockDirStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
	openDirFn      func(context.Context, *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error)
	releaseDirFn   func(context.Context, *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error)
	readDirFn      func(context.Context, *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error)
	readDirPlusFn  func(context.Context, *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error)
	fsyncDirFn     func(context.Context, *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error)
}

func (m *mockDirStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func (m *mockDirStorageServer) OpenDir(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
	if m.openDirFn != nil {
		return m.openDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockDirStorageServer) ReleaseDir(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
	if m.releaseDirFn != nil {
		return m.releaseDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockDirStorageServer) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	if m.readDirFn != nil {
		return m.readDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockDirStorageServer) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	if m.readDirPlusFn != nil {
		return m.readDirPlusFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockDirStorageServer) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	if m.fsyncDirFn != nil {
		return m.fsyncDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupDirTestRPCServer(t *testing.T, srv *mockDirStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func createTestSessionManager(t *testing.T, client storagev1connect.StorageServiceClient) *session.Manager {
	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval: 50 * time.Millisecond,
		MaxInterval: 100 * time.Millisecond,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

func TestRawFS_OpenDir_And_ReleaseDir(t *testing.T) {
	var (
		capturedOpenReq    *storagev1.OpenDirRequest
		capturedReleaseReq *storagev1.ReleaseDirRequest
	)

	mockServer := &mockDirStorageServer{
		openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
			capturedOpenReq = req.Msg
			return connect.NewResponse(&storagev1.OpenDirResponse{
				Fh:        8888,
				OpenFlags: 0,
			}), nil
		},
		releaseDirFn: func(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
			capturedReleaseReq = req.Msg
			return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
		},
	}

	client := setupDirTestRPCServer(t, mockServer)
	mgr := createTestSessionManager(t, client)
	activeSID := mgr.CurrentSessionID()
	require.Greater(t, activeSID, uint64(0))

	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	})

	// 1. 测试根目录打开 (input.NodeId = 1 应映射为 1001)
	openIn := &fuse.OpenIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
		},
		Flags: syscall.O_RDONLY | 0x10000, // 带有宿主机 O_LARGEFILE 等额外标志位
	}
	var openOut fuse.OpenOut

	st := rawFS.OpenDir(nil, openIn, &openOut)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(8888), openOut.Fh)

	// 校验 OpenDir 请求入参
	require.NotNil(t, capturedOpenReq)
	require.Equal(t, uint64(1001), capturedOpenReq.GetIno(), "根目录 1 必须映射为服务端 RootIno 1001")
	require.Equal(t, uint32(0), capturedOpenReq.GetFlags(), "Flags 必须清洗为 0")
	require.NotNil(t, capturedOpenReq.SessionId)
	require.Equal(t, activeSID, *capturedOpenReq.SessionId)

	// 校验句柄已绑定到 session
	sid, resolveSt := rawFS.ResolveHandle(8888)
	require.Equal(t, fuse.OK, resolveSt)
	require.Equal(t, activeSID, sid)

	// 2. 测试 ReleaseDir
	releaseIn := &fuse.ReleaseIn{
		InHeader: fuse.InHeader{
			NodeId: 1,
		},
		Fh: 8888,
	}
	rawFS.ReleaseDir(releaseIn)

	// 校验 ReleaseDir 请求入参
	require.NotNil(t, capturedReleaseReq)
	require.Equal(t, uint64(1001), capturedReleaseReq.GetIno())
	require.Equal(t, uint64(8888), capturedReleaseReq.GetFh())
	require.NotNil(t, capturedReleaseReq.SessionId)
	require.Equal(t, activeSID, *capturedReleaseReq.SessionId)

	// 校验句柄已解绑，再次 ResolveHandle 应返回 EBADF
	_, resolveSt = rawFS.ResolveHandle(8888)
	require.Equal(t, fuse.EBADF, resolveSt)
}

func TestRawFS_ReleaseDir(t *testing.T) {
	mockServer := &mockDirStorageServer{}
	client := setupDirTestRPCServer(t, mockServer)
	mgr := createTestSessionManager(t, client)

	t.Run("transient network error enqueues async retry until server succeeds or returns ENOENT", func(t *testing.T) {
		retryFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:               "00000000-0000-0000-0000-000000000001",
			RootIno:              1001,
			ReleaseRetryInterval: 15 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		// 1. 初次网络失败，后续重试成功
		fh1 := uint64(8889)
		mgr.BindHandle(fh1, mgr.CurrentSessionID())

		var attempts1 atomic.Int32
		mockServer.releaseDirFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
			if req.Msg.GetFh() != fh1 {
				return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
			}
			n := attempts1.Add(1)
			if n < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporary network failure"))
			}
			return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
		}

		retryFS.ReleaseDir(&fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Fh:       fh1,
		})

		// 本地句柄立即解绑
		_, st := retryFS.ResolveHandle(fh1)
		require.Equal(t, fuse.EBADF, st, "初次 ReleaseDir 失败后本地句柄仍须立即解绑")

		require.Eventually(t, func() bool {
			return attempts1.Load() == 3
		}, 150*time.Millisecond, 5*time.Millisecond, "后台重试队列应自动重发 ReleaseDir 直至服务端成功")

		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(3), attempts1.Load())

		// 2. 初次失败入队，重试期间服务端返回 ENOENT（视为已完成销毁并出队）
		fh2 := uint64(8890)
		mgr.BindHandle(fh2, mgr.CurrentSessionID())

		var attempts2 atomic.Int32
		mockServer.releaseDirFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
			if req.Msg.GetFh() != fh2 {
				return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
			}
			n := attempts2.Add(1)
			if n == 1 {
				return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("timeout"))
			}
			cerr := connect.NewError(connect.CodeNotFound, errors.New("dir handle not found"))
			detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
				Reason: "ENOENT",
				Domain: "bowl.storage",
			})
			require.NoError(t, err)
			cerr.AddDetail(detail)
			return nil, cerr
		}

		retryFS.ReleaseDir(&fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Fh:       fh2,
		})

		require.Eventually(t, func() bool {
			return attempts2.Load() == 2
		}, 150*time.Millisecond, 5*time.Millisecond, "重试收到 ENOENT 后应停止重试")

		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(2), attempts2.Load(), "ENOENT 视为已完成销毁，出队后不再重发")
	})

	t.Run("session rotation drops stale session pending releasedir items", func(t *testing.T) {
		var rotateFlag atomic.Bool
		localSrv := &mockDirStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				recreated := rotateFlag.Swap(false)
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
					SessionRecreated: recreated,
				}), nil
			},
		}
		localClient := setupDirTestRPCServer(t, localSrv)
		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval: 15 * time.Millisecond,
			MaxInterval: 30 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, localMgr.Start(context.Background()))
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		retryFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			DiskID:               "00000000-0000-0000-0000-000000000001",
			RootIno:              1001,
			ReleaseRetryInterval: 80 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		fh := uint64(8891)
		localMgr.BindHandle(fh, oldSID)

		var releaseDirCalls atomic.Int32
		localSrv.releaseDirFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
			releaseDirCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("network down"))
		}

		retryFS.ReleaseDir(&fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Fh:       fh,
		})
		require.Equal(t, int32(1), releaseDirCalls.Load())

		rotateFlag.Store(true)
		require.Eventually(t, func() bool {
			return localMgr.CurrentSessionID() != oldSID
		}, 500*time.Millisecond, 10*time.Millisecond, "会话应完成轮换")

		time.Sleep(120 * time.Millisecond)
		require.Equal(t, int32(1), releaseDirCalls.Load(), "会话轮换后队列应自动丢弃旧会话待释放目录句柄，不再发送 RPC")
	})
}

func TestRawFS_ReadDir(t *testing.T) {
	var capturedReadDirReq *storagev1.ReadDirRequest

	mockServer := &mockDirStorageServer{
		openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
			return connect.NewResponse(&storagev1.OpenDirResponse{
				Fh:        1234,
				OpenFlags: 0,
			}), nil
		},
		readDirFn: func(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
			capturedReadDirReq = req.Msg
			if req.Msg.Offset == 0 {
				return connect.NewResponse(&storagev1.ReadDirResponse{
					Entries: []*storagev1.DirEntry{
						{Ino: 1001, Name: []byte("."), Mode: syscall.S_IFDIR | 0o755, NextOffset: 1},
						{Ino: 1001, Name: []byte(".."), Mode: syscall.S_IFDIR | 0o755, NextOffset: 2},
						{Ino: 2001, Name: []byte("file1"), Mode: syscall.S_IFREG | 0o644, NextOffset: 3},
						{Ino: 2002, Name: []byte("file2"), Mode: syscall.S_IFREG | 0o644, NextOffset: 4},
					},
				}), nil
			}
			return connect.NewResponse(&storagev1.ReadDirResponse{
				Entries: []*storagev1.DirEntry{},
			}), nil
		},
	}

	client := setupDirTestRPCServer(t, mockServer)
	mgr := createTestSessionManager(t, client)
	activeSID := mgr.CurrentSessionID()

	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	})

	// 1. 先 OpenDir 得到有效句柄 1234
	openIn := &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
	}
	var openOut fuse.OpenOut
	st := rawFS.OpenDir(nil, openIn, &openOut)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(1234), openOut.Fh)

	// 2. 正常 ReadDir
	readIn := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       1234,
		Offset:   0,
	}
	dirList := fuse.NewDirEntryList(make([]byte, 4096), 0)
	st = rawFS.ReadDir(nil, readIn, dirList)
	require.Equal(t, fuse.OK, st)

	require.NotNil(t, capturedReadDirReq)
	require.Equal(t, uint64(1001), capturedReadDirReq.GetIno(), "根目录 1 映射为 1001")
	require.Equal(t, uint64(1234), capturedReadDirReq.GetFh())
	require.Equal(t, uint64(0), capturedReadDirReq.GetOffset())
	require.Equal(t, uint32(512), capturedReadDirReq.GetLimit())
	require.NotNil(t, capturedReadDirReq.SessionId)
	require.Equal(t, activeSID, *capturedReadDirReq.SessionId)

	// 解析 dirList 条目并校验 Inode 重写及 Offset
	entries, err := fuse.ParseDirEntries(dirList.Bytes())
	require.NoError(t, err)
	require.Len(t, entries, 4)

	// . 与 .. 的 Ino 必须被重写为内核根节点 1
	require.Equal(t, ".", entries[0].Name)
	require.Equal(t, uint64(1), entries[0].Ino)
	require.Equal(t, uint64(1), entries[0].Off)

	require.Equal(t, "..", entries[1].Name)
	require.Equal(t, uint64(1), entries[1].Ino)
	require.Equal(t, uint64(2), entries[1].Off)

	require.Equal(t, "file1", entries[2].Name)
	require.Equal(t, uint64(2001), entries[2].Ino)
	require.Equal(t, uint64(3), entries[2].Off)

	require.Equal(t, "file2", entries[3].Name)
	require.Equal(t, uint64(2002), entries[3].Ino)
	require.Equal(t, uint64(4), entries[3].Off)

	// 3. 缓冲区满时中断断言
	// 40 字节仅能容纳 1 个条目
	smallDirList := fuse.NewDirEntryList(make([]byte, 40), 0)
	st = rawFS.ReadDir(nil, readIn, smallDirList)
	require.Equal(t, fuse.OK, st)
	smallEntries, err := fuse.ParseDirEntries(smallDirList.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, len(smallEntries), "缓冲区满应提前截断，包含 1 个条目")
	require.Equal(t, ".", smallEntries[0].Name)

	// 4. 会话淘汰 / 未知句柄断言返回 EBADF
	badReadIn := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       999999, // 未绑定的句柄
		Offset:   0,
	}
	st = rawFS.ReadDir(nil, badReadIn, dirList)
	require.Equal(t, fuse.EBADF, st)
}

func TestRawFS_ReadDirPlus(t *testing.T) {
	var capturedReadDirPlusReq *storagev1.ReadDirPlusRequest

	mockServer := &mockDirStorageServer{
		openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
			return connect.NewResponse(&storagev1.OpenDirResponse{
				Fh:        5555,
				OpenFlags: 0,
			}), nil
		},
		readDirPlusFn: func(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
			capturedReadDirPlusReq = req.Msg
			return connect.NewResponse(&storagev1.ReadDirPlusResponse{
				Entries: []*storagev1.DirEntryPlus{
					{
						Ino:        1001,
						Name:       []byte("."),
						NextOffset: 1,
						Inode: &storagev1.Inode{
							Ino: 1001,
							Attr: &storagev1.InodeAttribute{
								Mode: syscall.S_IFDIR | 0o755,
							},
						},
						EntryTimeoutSeconds: 1.0,
						AttrTimeoutSeconds:  1.0,
					},
					{
						Ino:        1001,
						Name:       []byte(".."),
						NextOffset: 2,
						Inode: &storagev1.Inode{
							Ino: 1001,
							Attr: &storagev1.InodeAttribute{
								Mode: syscall.S_IFDIR | 0o755,
							},
						},
						EntryTimeoutSeconds: 1.0,
						AttrTimeoutSeconds:  1.0,
					},
					{
						Ino:        3001,
						Name:       []byte("subfile"),
						NextOffset: 3,
						Inode: &storagev1.Inode{
							Ino:        3001,
							Generation: 7,
							Attr: &storagev1.InodeAttribute{
								Mode:  syscall.S_IFREG | 0o644,
								Size:  1024,
								Nlink: 1,
								Uid:   1000,
								Gid:   1000,
							},
						},
						EntryTimeoutSeconds: 2.5,
						AttrTimeoutSeconds:  3.5,
					},
				},
			}), nil
		},
	}

	client := setupDirTestRPCServer(t, mockServer)
	mgr := createTestSessionManager(t, client)
	activeSID := mgr.CurrentSessionID()

	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	})

	// 1. OpenDir 获取句柄 5555
	openIn := &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
	}
	var openOut fuse.OpenOut
	st := rawFS.OpenDir(nil, openIn, &openOut)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint64(5555), openOut.Fh)

	// 2. 正常 ReadDirPlus
	readIn := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       5555,
		Offset:   0,
	}
	dirList := fuse.NewDirEntryList(make([]byte, 4096), 0)
	st = rawFS.ReadDirPlus(nil, readIn, dirList)
	require.Equal(t, fuse.OK, st)

	require.NotNil(t, capturedReadDirPlusReq)
	require.Equal(t, uint64(1001), capturedReadDirPlusReq.GetIno())
	require.Equal(t, uint64(5555), capturedReadDirPlusReq.GetFh())
	require.Equal(t, uint64(0), capturedReadDirPlusReq.GetOffset())
	require.Equal(t, uint32(512), capturedReadDirPlusReq.GetLimit())
	require.NotNil(t, capturedReadDirPlusReq.SessionId)
	require.Equal(t, activeSID, *capturedReadDirPlusReq.SessionId)

	// 验证条目解析与 "." / ".." 的 EntryOut 保持全零 (NodeId == 0)
	buf := dirList.Bytes()
	entries, entryOuts, err := fuse.ParseDirLookupEntries(buf)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	require.Len(t, entryOuts, 3)

	// 条目 1: "." -> EntryOut 必须全零 (NodeId == 0)，Ino 重写为 1
	require.Equal(t, ".", entries[0].Name)
	require.Equal(t, uint64(1), entries[0].Ino, ". 的 Inode 必须重写为内核根节点 1")
	require.Equal(t, uint64(1), entries[0].Off)
	require.Equal(t, uint64(0), entryOuts[0].NodeId, ". 的 NodeId 必须严格为 0")
	require.Equal(t, uint64(0), entryOuts[0].Generation)

	// 条目 2: ".." -> EntryOut 也必须全零 (NodeId == 0)，Ino 重写为 1
	require.Equal(t, "..", entries[1].Name)
	require.Equal(t, uint64(1), entries[1].Ino, ".. 的 Inode 必须重写为内核根节点 1")
	require.Equal(t, uint64(2), entries[1].Off)
	require.Equal(t, uint64(0), entryOuts[1].NodeId, ".. 的 NodeId 必须严格为 0")
	require.Equal(t, uint64(0), entryOuts[1].Generation)

	// 条目 3: "subfile" -> 真实子项，EntryOut 完整填充
	require.Equal(t, "subfile", entries[2].Name)
	require.Equal(t, uint64(3001), entries[2].Ino)
	require.Equal(t, uint64(3), entries[2].Off)
	require.Equal(t, uint64(3001), entryOuts[2].NodeId)
	require.Equal(t, uint64(7), entryOuts[2].Generation)
	require.Equal(t, uint64(1024), entryOuts[2].Attr.Size)
	require.Equal(t, 2500*time.Millisecond, entryOuts[2].EntryTimeout())
	require.Equal(t, 3500*time.Millisecond, entryOuts[2].AttrTimeout())

	// 3. 缓冲区满提前终止
	// 单个 ReadDirPlus 条目至少包含 EntryOut(136B) + Dirent(24B) + Name = ~168B
	smallList := fuse.NewDirEntryList(make([]byte, 180), 0)
	st = rawFS.ReadDirPlus(nil, readIn, smallList)
	require.Equal(t, fuse.OK, st)
	smallEntries, _, err := fuse.ParseDirLookupEntries(smallList.Bytes())
	require.NoError(t, err)
	require.Equal(t, 1, len(smallEntries), "小缓冲区只应装入 1 个条目")
	require.Equal(t, ".", smallEntries[0].Name)

	// 4. 会话淘汰 / 未知句柄断言返回 EBADF
	badReadIn := &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       777777,
		Offset:   0,
	}
	st = rawFS.ReadDirPlus(nil, badReadIn, dirList)
	require.Equal(t, fuse.EBADF, st)
}

func TestRawFS_FsyncDir(t *testing.T) {
	var capturedFsyncDirReq *storagev1.FsyncDirRequest

	mockServer := &mockDirStorageServer{
		openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
			return connect.NewResponse(&storagev1.OpenDirResponse{
				Fh:        6666,
				OpenFlags: 0,
			}), nil
		},
		fsyncDirFn: func(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
			capturedFsyncDirReq = req.Msg
			return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
		},
	}

	client := setupDirTestRPCServer(t, mockServer)
	mgr := createTestSessionManager(t, client)
	activeSID := mgr.CurrentSessionID()

	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	})

	// 1. 先 OpenDir
	openIn := &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
	}
	var openOut fuse.OpenOut
	st := rawFS.OpenDir(nil, openIn, &openOut)
	require.Equal(t, fuse.OK, st)

	// 2. FsyncDir (datasync = true, FsyncFlags = 1)
	fsyncIn := &fuse.FsyncIn{
		InHeader:   fuse.InHeader{NodeId: 1},
		Fh:         6666,
		FsyncFlags: 1, // datasync
	}
	st = rawFS.FsyncDir(nil, fsyncIn)
	require.Equal(t, fuse.OK, st)

	require.NotNil(t, capturedFsyncDirReq)
	require.Equal(t, uint64(1001), capturedFsyncDirReq.GetIno(), "NodeId 1 映射为 1001")
	require.Equal(t, uint64(6666), capturedFsyncDirReq.GetFh())
	require.True(t, capturedFsyncDirReq.GetDatasync())
	require.NotNil(t, capturedFsyncDirReq.SessionId)
	require.Equal(t, activeSID, *capturedFsyncDirReq.SessionId)

	// 3. FsyncDir (datasync = false, FsyncFlags = 0)
	fsyncIn.FsyncFlags = 0
	st = rawFS.FsyncDir(nil, fsyncIn)
	require.Equal(t, fuse.OK, st)
	require.False(t, capturedFsyncDirReq.GetDatasync())

	// 4. 未知句柄断言返回 EBADF
	badFsyncIn := &fuse.FsyncIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       999999,
	}
	st = rawFS.FsyncDir(nil, badFsyncIn)
	require.Equal(t, fuse.EBADF, st)
}
