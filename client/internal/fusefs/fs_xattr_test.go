package fusefs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockXattrServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	setXattrFn    func(context.Context, *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error)
	getXattrFn    func(context.Context, *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error)
	listXattrFn   func(context.Context, *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error)
	removeXattrFn func(context.Context, *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error)
}

func (m *mockXattrServer) SetXattr(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	if m.setXattrFn != nil {
		return m.setXattrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockXattrServer) GetXattr(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	if m.getXattrFn != nil {
		return m.getXattrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockXattrServer) ListXattr(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	if m.listXattrFn != nil {
		return m.listXattrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockXattrServer) RemoveXattr(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	if m.removeXattrFn != nil {
		return m.removeXattrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupXattrRPCServer(t *testing.T, srv *mockXattrServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func makeErrorInfo(t *testing.T, reason string) *connect.ErrorDetail {
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "bowl.storage",
	})
	require.NoError(t, err)
	return detail
}

func TestRawFS_SetXAttr(t *testing.T) {
	mockSrv := &mockXattrServer{}
	client := setupXattrRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:   "disk-xattr-1",
		RootIno:  1001,
		ReadOnly: false,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("read-only mode returns EROFS directly without RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{ReadOnly: true, RootIno: 1001})
		input := &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    0,
		}
		status := roFS.SetXAttr(nil, input, "user.test", []byte("val"))
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("invalid nodeId returns ENOENT", func(t *testing.T) {
		input := &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: 1001}, // RootIno 且非 1 必须被 ToServerIno 拦截
		}
		status := rawFS.SetXAttr(nil, input, "user.test", []byte("val"))
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("successful setxattr forwards arguments and flags", func(t *testing.T) {
		var capturedReq *storagev1.SetXattrRequest
		mockSrv.setXattrFn = func(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
			capturedReq = req.Msg
			return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
		}

		input := &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: 1}, // 根节点 1 应转换为 RootIno 1001
			Flags:    unix.XATTR_CREATE,
		}
		data := []byte("binary\x00data\xff")
		status := rawFS.SetXAttr(nil, input, "user.foo", data)

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(1001), capturedReq.GetIno())
		require.Equal(t, "user.foo", string(capturedReq.GetName()))
		require.Equal(t, data, capturedReq.GetValue())
		require.Equal(t, uint32(unix.XATTR_CREATE), capturedReq.GetFlags())
	})

	t.Run("XATTR_CREATE conflict maps to EEXIST", func(t *testing.T) {
		mockSrv.setXattrFn = func(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
			err := connect.NewError(connect.CodeAlreadyExists, errors.New("attribute already exists"))
			err.AddDetail(makeErrorInfo(t, "EEXIST"))
			return nil, err
		}

		input := &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Flags:    unix.XATTR_CREATE,
		}
		status := rawFS.SetXAttr(nil, input, "user.foo", []byte("val"))
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})

	t.Run("exceeding 64 KiB maps to E2BIG", func(t *testing.T) {
		mockSrv.setXattrFn = func(ctx context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
			err := connect.NewError(connect.CodeInvalidArgument, errors.New("xattr value too large"))
			err.AddDetail(makeErrorInfo(t, "E2BIG"))
			return nil, err
		}

		input := &fuse.SetXAttrIn{
			InHeader: fuse.InHeader{NodeId: 2002},
		}
		status := rawFS.SetXAttr(nil, input, "user.large", make([]byte, 65537))
		require.Equal(t, fuse.Status(syscall.E2BIG), status)
	})
}

func TestRawFS_GetXAttr(t *testing.T) {
	mockSrv := &mockXattrServer{}
	client := setupXattrRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "disk-xattr-get",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("invalid nodeId returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		sz, status := rawFS.GetXAttr(nil, header, "user.attr", nil)
		require.Equal(t, fuse.ENOENT, status)
		require.Equal(t, uint32(0), sz)
	})

	t.Run("attribute not found returns ENODATA", func(t *testing.T) {
		mockSrv.getXattrFn = func(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
			err := connect.NewError(connect.CodeNotFound, errors.New("attribute not found"))
			err.AddDetail(makeErrorInfo(t, "ENODATA"))
			return nil, err
		}

		header := &fuse.InHeader{NodeId: 1}
		sz, status := rawFS.GetXAttr(nil, header, "user.nonexistent", nil)
		require.Equal(t, fuse.Status(syscall.ENODATA), status)
		require.Equal(t, uint32(0), sz)
	})

	t.Run("zero-length probe returns exact size and fuse.OK", func(t *testing.T) {
		val := []byte("hello-xattr-probe-binary\x00\x01\x02")
		mockSrv.getXattrFn = func(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno(), "NodeId 1 应映射为 1001")
			require.Equal(t, "user.probe", string(req.Msg.GetName()))
			return connect.NewResponse(&storagev1.GetXattrResponse{
				Value: val,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		sz, status := rawFS.GetXAttr(nil, header, "user.probe", nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(val)), sz)

		// 显式空切片 len == 0
		sz, status = rawFS.GetXAttr(nil, header, "user.probe", []byte{})
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(val)), sz)
	})

	t.Run("small buffer returns required size and ERANGE", func(t *testing.T) {
		val := []byte("ten-bytes!")
		mockSrv.getXattrFn = func(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
			return connect.NewResponse(&storagev1.GetXattrResponse{
				Value: val,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		dest := make([]byte, 5) // 5 < 10
		sz, status := rawFS.GetXAttr(nil, header, "user.foo", dest)
		require.Equal(t, fuse.Status(syscall.ERANGE), status)
		require.Equal(t, uint32(len(val)), sz)
	})

	t.Run("adequate buffer copies value and returns exact size", func(t *testing.T) {
		val := []byte("exact-copy-binary\x00\xff")
		mockSrv.getXattrFn = func(ctx context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
			return connect.NewResponse(&storagev1.GetXattrResponse{
				Value: val,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		dest := make([]byte, len(val)+4)
		sz, status := rawFS.GetXAttr(nil, header, "user.foo", dest)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(val)), sz)
		require.Equal(t, val, dest[:sz])
	})
}

func TestRawFS_ListXAttr(t *testing.T) {
	mockSrv := &mockXattrServer{}
	client := setupXattrRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "disk-xattr-list",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("invalid nodeId returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		sz, status := rawFS.ListXAttr(nil, header, nil)
		require.Equal(t, fuse.ENOENT, status)
		require.Equal(t, uint32(0), sz)
	})

	t.Run("empty list returns zero size and fuse.OK", func(t *testing.T) {
		mockSrv.listXattrFn = func(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
			return connect.NewResponse(&storagev1.ListXattrResponse{
				Names: [][]byte{},
			}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		sz, status := rawFS.ListXAttr(nil, header, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(0), sz)
	})

	t.Run("zero-length probe returns total bytes with null terminators and fuse.OK", func(t *testing.T) {
		names := [][]byte{[]byte("user.a"), []byte("user.b")}
		// "user.a\x00" = 7 bytes, "user.b\x00" = 7 bytes, total = 14
		mockSrv.listXattrFn = func(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno())
			return connect.NewResponse(&storagev1.ListXattrResponse{
				Names: names,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 1}
		sz, status := rawFS.ListXAttr(nil, header, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(14), sz)

		sz, status = rawFS.ListXAttr(nil, header, []byte{})
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(14), sz)
	})

	t.Run("small buffer returns total bytes and ERANGE", func(t *testing.T) {
		names := [][]byte{[]byte("user.a"), []byte("user.b")}
		mockSrv.listXattrFn = func(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
			return connect.NewResponse(&storagev1.ListXattrResponse{
				Names: names,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		dest := make([]byte, 10) // 10 < 14
		sz, status := rawFS.ListXAttr(nil, header, dest)
		require.Equal(t, fuse.Status(syscall.ERANGE), status)
		require.Equal(t, uint32(14), sz)
	})

	t.Run("adequate buffer packs names separated by null bytes", func(t *testing.T) {
		names := [][]byte{[]byte("user.first"), []byte("user.second")}
		// "user.first\x00" (11) + "user.second\x00" (12) = 23
		mockSrv.listXattrFn = func(ctx context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
			return connect.NewResponse(&storagev1.ListXattrResponse{
				Names: names,
			}), nil
		}

		header := &fuse.InHeader{NodeId: 2002}
		dest := make([]byte, 30)
		sz, status := rawFS.ListXAttr(nil, header, dest)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(23), sz)
		require.Equal(t, "user.first\x00user.second\x00", string(dest[:sz]))
	})
}

func TestRawFS_RemoveXAttr(t *testing.T) {
	mockSrv := &mockXattrServer{}
	client := setupXattrRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:   "disk-xattr-remove",
		RootIno:  1001,
		ReadOnly: false,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("read-only mode returns EROFS directly without RPC", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{ReadOnly: true, RootIno: 1001})
		header := &fuse.InHeader{NodeId: 1}
		status := roFS.RemoveXAttr(nil, header, "user.test")
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("invalid nodeId returns ENOENT", func(t *testing.T) {
		header := &fuse.InHeader{NodeId: 1001}
		status := rawFS.RemoveXAttr(nil, header, "user.test")
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("attribute not found returns ENODATA", func(t *testing.T) {
		mockSrv.removeXattrFn = func(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
			err := connect.NewError(connect.CodeNotFound, errors.New("attribute not found"))
			err.AddDetail(makeErrorInfo(t, "ENODATA"))
			return nil, err
		}

		header := &fuse.InHeader{NodeId: 2002}
		status := rawFS.RemoveXAttr(nil, header, "user.nonexistent")
		require.Equal(t, fuse.Status(syscall.ENODATA), status)
	})

	t.Run("successful removexattr forwards arguments", func(t *testing.T) {
		var capturedReq *storagev1.RemoveXattrRequest
		mockSrv.removeXattrFn = func(ctx context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
			capturedReq = req.Msg
			return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
		}

		header := &fuse.InHeader{NodeId: 1} // 根节点 1 应转换为 RootIno 1001
		status := rawFS.RemoveXAttr(nil, header, "user.trash")

		require.Equal(t, fuse.OK, status)
		require.NotNil(t, capturedReq)
		require.Equal(t, uint64(1001), capturedReq.GetIno())
		require.Equal(t, "user.trash", string(capturedReq.GetName()))
	})
}
