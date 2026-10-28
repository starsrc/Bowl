package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func setupDataTestServer(t *testing.T) (*metamock.MockManager, storagev1connect.StorageServiceClient, func()) {
	t.Helper()
	mockMgr := metamock.NewMockManager(t)
	srv, err := server.New(server.Params{
		Config:      defaultTestConfig(),
		MetaManager: mockMgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	client := storagev1connect.NewStorageServiceClient(http.DefaultClient, ts.URL)

	cleanup := func() {
		ts.Close()
		mockMgr.AssertExpectations(t)
	}
	return mockMgr, client, cleanup
}

func TestServer_Write(t *testing.T) {
	ctx := context.Background()

	t.Run("写入成功返回写入大小与最新 Inode 元数据", func(t *testing.T) {
		mockMgr, client, cleanup := setupDataTestServer(t)
		defer cleanup()

		testIno := uint64(100)
		testFh := uint64(200)
		testOffset := uint64(0)
		testData := []byte("hello world")
		sessionID := uint64(12345)

		now := time.Now().Truncate(time.Microsecond)
		mockInode := &meta.Inode{
			Ino:        testIno,
			Generation: 1,
			Attr: meta.Attribute{
				Mode:      meta.S_IFREG | 0o644,
				Size:      uint64(len(testData)),
				Nlink:     1,
				UID:       1000,
				GID:       1000,
				Rdev:      0,
				Blksize:   meta.DefaultBlockSize,
				Blocks:    1,
				Atime:     now,
				Mtime:     now,
				Ctime:     now,
				Flags:     0,
				ParentIno: meta.RootIno,
			},
		}

		mockMgr.On("Write", mock.Anything, &meta.WriteRequest{
			Ino:       testIno,
			Fh:        testFh,
			Offset:    testOffset,
			Data:      testData,
			SessionID: &sessionID,
		}).Return(&meta.WriteResponse{
			Size:  uint32(len(testData)),
			Inode: mockInode,
		}, nil).Once()

		resp, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:       testIno,
			Fh:        testFh,
			Offset:    testOffset,
			Data:      testData,
			SessionId: proto.Uint64(sessionID),
		}))

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(testData)), resp.Msg.Size)
		require.NotNil(t, resp.Msg.Inode)
		assert.Equal(t, testIno, resp.Msg.Inode.Ino)
		require.NotNil(t, resp.Msg.Inode.Attr)
		assert.Equal(t, uint64(len(testData)), resp.Msg.Inode.Attr.Size)
	})

	t.Run("写入失败返回对应 Connect 状态码与 POSIX ErrorInfo 契约", func(t *testing.T) {
		tests := []struct {
			name           string
			metaErr        error
			expectedCode   connect.Code
			expectedReason string
		}{
			{
				name:           "目标节点不存在返回 NotFound (ENOENT)",
				metaErr:        meta.ErrNotFound,
				expectedCode:   connect.CodeNotFound,
				expectedReason: "ENOENT",
			},
			{
				name:           "非法句柄返回 FailedPrecondition (EBADF)",
				metaErr:        meta.ErrBadFileHandle,
				expectedCode:   connect.CodeFailedPrecondition,
				expectedReason: "EBADF",
			},
			{
				name:           "目标是目录返回 FailedPrecondition (EISDIR)",
				metaErr:        meta.ErrIsDir,
				expectedCode:   connect.CodeFailedPrecondition,
				expectedReason: "EISDIR",
			},
			{
				name:           "参数非法返回 InvalidArgument (EINVAL)",
				metaErr:        meta.ErrInvalidArgument,
				expectedCode:   connect.CodeInvalidArgument,
				expectedReason: "EINVAL",
			},
			{
				name:           "底层存储 IO 错误返回 Internal (EIO)",
				metaErr:        meta.ErrIO,
				expectedCode:   connect.CodeInternal,
				expectedReason: "EIO",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				mockMgr, client, cleanup := setupDataTestServer(t)
				defer cleanup()

				testIno := uint64(101)
				testFh := uint64(201)

				mockMgr.On("Write", mock.Anything, mock.Anything).Return(nil, tc.metaErr).Once()

				_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
					Ino:    testIno,
					Fh:     testFh,
					Offset: 0,
					Data:   []byte("test"),
				}))

				require.Error(t, err)
				var connErr *connect.Error
				require.ErrorAs(t, err, &connErr)
				assert.Equal(t, tc.expectedCode, connErr.Code())

				var hasErrorInfo bool
				for _, detail := range connErr.Details() {
					val, err := detail.Value()
					require.NoError(t, err)
					if errInfo, ok := val.(*errdetails.ErrorInfo); ok {
						hasErrorInfo = true
						assert.Equal(t, "bowl.storage", errInfo.Domain)
						assert.Equal(t, tc.expectedReason, errInfo.Reason)
					}
				}
				assert.True(t, hasErrorInfo, "应携带 google.rpc.ErrorInfo 详情")
			})
		}
	})
}

func TestServer_Read(t *testing.T) {
	ctx := context.Background()

	t.Run("读取成功返回读取到的字节流", func(t *testing.T) {
		mockMgr, client, cleanup := setupDataTestServer(t)
		defer cleanup()

		testIno := uint64(102)
		testFh := uint64(202)
		testOffset := uint64(0)
		testSize := uint32(11)
		testData := []byte("hello world")

		mockMgr.On("Read", mock.Anything, &meta.ReadRequest{
			Ino:    testIno,
			Fh:     testFh,
			Offset: testOffset,
			Size:   testSize,
		}).Return(&meta.ReadResponse{
			Data: testData,
		}, nil).Once()

		resp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    testIno,
			Fh:     testFh,
			Offset: testOffset,
			Size:   testSize,
		}))

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, testData, resp.Msg.Data)
	})

	t.Run("读取成功透传显式 SessionID", func(t *testing.T) {
		mockMgr, client, cleanup := setupDataTestServer(t)
		defer cleanup()

		testIno := uint64(104)
		testFh := uint64(204)
		testOffset := uint64(128)
		testSize := uint32(64)
		testData := []byte("session read payload")
		expectedSessionID := uint64(9988)

		mockMgr.EXPECT().Read(mock.Anything, mock.MatchedBy(func(req *meta.ReadRequest) bool {
			return req != nil &&
				req.Ino == testIno &&
				req.Fh == testFh &&
				req.Offset == testOffset &&
				req.Size == testSize &&
				req.SessionID != nil &&
				*req.SessionID == expectedSessionID
		})).Return(&meta.ReadResponse{
			Data: testData,
		}, nil).Once()

		resp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       testIno,
			Fh:        testFh,
			Offset:    testOffset,
			Size:      testSize,
			SessionId: proto.Uint64(expectedSessionID),
		}))

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, testData, resp.Msg.Data)
	})

	t.Run("读取失败返回对应 Connect 状态码与 POSIX ErrorInfo 契约", func(t *testing.T) {
		mockMgr, client, cleanup := setupDataTestServer(t)
		defer cleanup()

		mockMgr.On("Read", mock.Anything, mock.Anything).Return(nil, meta.ErrBadFileHandle).Once()

		_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    103,
			Fh:     203,
			Offset: 0,
			Size:   10,
		}))

		require.Error(t, err)
		var connErr *connect.Error
		require.ErrorAs(t, err, &connErr)
		assert.Equal(t, connect.CodeFailedPrecondition, connErr.Code())

		var hasErrorInfo bool
		for _, detail := range connErr.Details() {
			val, err := detail.Value()
			require.NoError(t, err)
			if errInfo, ok := val.(*errdetails.ErrorInfo); ok {
				hasErrorInfo = true
				assert.Equal(t, "bowl.storage", errInfo.Domain)
				assert.Equal(t, "EBADF", errInfo.Reason)
			}
		}
		assert.True(t, hasErrorInfo, "应携带 reason=EBADF 的 google.rpc.ErrorInfo 详情")
	})
}
