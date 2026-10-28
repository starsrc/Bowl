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
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type mockRenameStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	renameFn func(context.Context, *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error)
}

func (m *mockRenameStorageServer) Rename(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	if m.renameFn != nil {
		return m.renameFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupTestRenameRPCServer(t *testing.T, srv *mockRenameStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Rename(t *testing.T) {
	mockSrv := &mockRenameStorageServer{}
	client := setupTestRenameRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("read-only filesystem returns EROFS without RPC call", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, nil, fusefs.Config{
			DiskID:   "00000000-0000-0000-0000-000000000001",
			RootIno:  1001,
			ReadOnly: true,
		})

		rpcCalled := false
		mockSrv.renameFn = func(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
			rpcCalled = true
			return connect.NewResponse(&storagev1.RenameResponse{}), nil
		}

		input := &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
			Flags:    0,
		}
		status := roFS.Rename(nil, input, "old.txt", "new.txt")
		require.Equal(t, fuse.Status(syscall.EROFS), status)
		require.False(t, rpcCalled)
	})

	t.Run("invalid old parent ino returns ENOENT", func(t *testing.T) {
		input := &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: 1001}, // RootIno passed directly without mapping
			Newdir:   fuse.FUSE_ROOT_ID,
		}
		status := rawFS.Rename(nil, input, "old.txt", "new.txt")
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("invalid new parent ino returns ENOENT", func(t *testing.T) {
		input := &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   1001, // RootIno passed directly without mapping
		}
		status := rawFS.Rename(nil, input, "old.txt", "new.txt")
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("root inode bidirectional rewriting and parameters passthrough", func(t *testing.T) {
		var receivedReq *storagev1.RenameRequest
		mockSrv.renameFn = func(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
			receivedReq = req.Msg
			return connect.NewResponse(&storagev1.RenameResponse{}), nil
		}

		input := &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   2002,
			Flags:    0,
		}
		status := rawFS.Rename(nil, input, "file_a.txt", "file_b.txt")
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, receivedReq)
		require.Equal(t, uint64(1001), receivedReq.GetOldParentIno())
		require.Equal(t, []byte("file_a.txt"), receivedReq.GetOldName())
		require.Equal(t, uint64(2002), receivedReq.GetNewParentIno())
		require.Equal(t, []byte("file_b.txt"), receivedReq.GetNewName())
		require.Equal(t, uint32(0), receivedReq.GetFlags())
	})

	t.Run("flags explicit mapping", func(t *testing.T) {
		const (
			linuxRenameNoReplace = 0x1
			linuxRenameExchange  = 0x2
			linuxRenameWhiteout  = 0x4
		)
		testCases := []struct {
			name          string
			inputFlags    uint32
			expectedFlags uint32
		}{
			{
				name:          "standard rename (flags=0)",
				inputFlags:    0,
				expectedFlags: 0,
			},
			{
				name:          "RENAME_NOREPLACE (0x1)",
				inputFlags:    linuxRenameNoReplace,
				expectedFlags: 0x1,
			},
			{
				name:          "RENAME_EXCHANGE (0x2)",
				inputFlags:    linuxRenameExchange,
				expectedFlags: 0x2,
			},
			{
				name:          "RENAME_WHITEOUT (0x4)",
				inputFlags:    linuxRenameWhiteout,
				expectedFlags: 0x4,
			},
			{
				name:          "unknown bit preserved (e.g. 0x10)",
				inputFlags:    0x10,
				expectedFlags: 0x10,
			},
			{
				name:          "NOREPLACE combined with unknown bit",
				inputFlags:    linuxRenameNoReplace | 0x8,
				expectedFlags: 0x1 | 0x8,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				var receivedReq *storagev1.RenameRequest
				mockSrv.renameFn = func(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
					receivedReq = req.Msg
					return connect.NewResponse(&storagev1.RenameResponse{}), nil
				}

				input := &fuse.RenameIn{
					InHeader: fuse.InHeader{NodeId: 3001},
					Newdir:   fuse.FUSE_ROOT_ID,
					Flags:    tc.inputFlags,
				}
				status := rawFS.Rename(nil, input, "src", "dst")
				require.Equal(t, fuse.OK, status)
				require.NotNil(t, receivedReq)
				require.Equal(t, uint64(3001), receivedReq.GetOldParentIno())
				require.Equal(t, uint64(1001), receivedReq.GetNewParentIno())
				require.Equal(t, tc.expectedFlags, receivedReq.GetFlags())
			})
		}
	})

	t.Run("server error mapping", func(t *testing.T) {
		errCases := []struct {
			name           string
			code           connect.Code
			reason         string
			expectedStatus fuse.Status
		}{
			{
				name:           "EINVAL - invalid argument / cycle detected",
				code:           connect.CodeInvalidArgument,
				reason:         "EINVAL",
				expectedStatus: fuse.EINVAL,
			},
			{
				name:           "ENOENT - source not found",
				code:           connect.CodeNotFound,
				reason:         "ENOENT",
				expectedStatus: fuse.ENOENT,
			},
			{
				name:           "EEXIST - RENAME_NOREPLACE destination exists",
				code:           connect.CodeAlreadyExists,
				reason:         "EEXIST",
				expectedStatus: fuse.Status(syscall.EEXIST),
			},
			{
				name:           "ENOTEMPTY - destination dir not empty",
				code:           connect.CodeFailedPrecondition,
				reason:         "ENOTEMPTY",
				expectedStatus: fuse.Status(syscall.ENOTEMPTY),
			},
			{
				name:           "EOPNOTSUPP - RENAME_WHITEOUT unsupported",
				code:           connect.CodeUnimplemented,
				reason:         "EOPNOTSUPP",
				expectedStatus: fuse.Status(syscall.EOPNOTSUPP),
			},
		}

		for _, tc := range errCases {
			t.Run(tc.name, func(t *testing.T) {
				mockSrv.renameFn = func(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
					connErr := connect.NewError(tc.code, errors.New("rpc error"))
					detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
						Reason: tc.reason,
						Domain: "bowl.storage",
					})
					if err == nil {
						connErr.AddDetail(detail)
					}
					return nil, connErr
				}

				input := &fuse.RenameIn{
					InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
					Newdir:   fuse.FUSE_ROOT_ID,
				}
				status := rawFS.Rename(nil, input, "a", "b")
				require.Equal(t, tc.expectedStatus, status)
			})
		}
	})

	t.Run("cancellation returns EINTR", func(t *testing.T) {
		cancelChan := make(chan struct{})
		close(cancelChan)

		mockSrv.renameFn = func(ctx context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
			return nil, connect.NewError(connect.CodeCanceled, context.Canceled)
		}

		input := &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Newdir:   fuse.FUSE_ROOT_ID,
		}
		status := rawFS.Rename(cancelChan, input, "a", "b")
		require.Equal(t, fuse.EINTR, status)
	})
}
