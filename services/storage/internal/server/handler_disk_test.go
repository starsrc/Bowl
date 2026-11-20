package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type memoryRoundTripper struct {
	handler http.Handler
}

func (rt memoryRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	rt.handler.ServeHTTP(rec, req)
	res := rec.Result()
	res.Request = req
	return res, nil
}

func newTestServerAndClient(t *testing.T, opts ...control.Option) (*Server, storagev1connect.StorageServiceClient) {
	t.Helper()
	ctrlOpts := append([]control.Option{control.WithStorageRoot(t.TempDir())}, opts...)
	mgr := control.New(db.NewMemStore(), ctrlOpts...)
	t.Cleanup(func() { _ = mgr.Close() })

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		Control: mgr,
	})
	require.NoError(t, err, "初始化 Server 失败")

	httpClient := &http.Client{Transport: memoryRoundTripper{handler: srv.Handler()}}
	client := storagev1connect.NewStorageServiceClient(httpClient, "http://storage.local")
	return srv, client
}

func requireConnectErrorCodeAndReason(t *testing.T, err error, wantCode connect.Code, wantReason string) {
	t.Helper()
	require.Error(t, err, "预期返回 Connect 错误，但得到 nil")
	gotCode := connect.CodeOf(err)
	require.Equal(t, wantCode, gotCode, "Connect 状态码不匹配: want=%v got=%v err=%v", wantCode, gotCode, err)
	info := ExtractErrorInfo(err)
	require.NotNil(t, info, "错误应携带 google.rpc.ErrorInfo 详情: err=%v", err)
	require.Equal(t, control.ErrorDomain, info.GetDomain(), "ErrorInfo 领域不匹配: want=%q got=%q", control.ErrorDomain, info.GetDomain())
	require.Equal(t, wantReason, info.GetReason(), "ErrorInfo reason 不匹配: want=%q got=%q", wantReason, info.GetReason())
}

func findDiskByID(disks []*storagev1.Disk, diskID string) *storagev1.Disk {
	for _, d := range disks {
		if d.GetDiskId() == diskID {
			return d
		}
	}
	return nil
}

func TestServerDiskRPC_LifecycleCRUD_SucceedsEndToEnd(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t, control.WithEntryTimeout(2.5), control.WithAttrTimeout(3.5))

	createResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "rpc-disk",
		MaxBytes: 16 << 20,
	}))

	require.NoError(t, err, "CreateDisk RPC 失败")
	created := createResp.Msg.GetDisk()
	require.NotNil(t, created, "CreateDisk 必须返回 Disk 实体")
	diskID := created.GetDiskId()
	require.NotEmpty(t, diskID, "DiskId 不能为空")
	require.Equal(t, "rpc-disk", created.GetName())
	require.Equal(t, uint64(1), created.GetRootIno())
	require.Equal(t, uint64(16<<20), created.GetMaxBytes())
	require.Nil(t, created.ParentDiskId, "根盘不应设置 ParentDiskId")

	getResp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err, "GetDisk RPC 失败")
	require.Equal(t, diskID, getResp.Msg.GetDisk().GetDiskId())

	listResp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 10}))
	require.NoError(t, err, "ListDisks RPC 失败")
	require.NotNil(t, findDiskByID(listResp.Msg.GetDisks(), diskID), "ListDisks 结果必须包含新建磁盘")

	newName := "rpc-disk-renamed"
	newQuota := uint64(32 << 20)
	updateResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskID,
		Name:     &newName,
		MaxBytes: &newQuota,
	}))
	require.NoError(t, err, "UpdateDisk RPC 失败")
	require.Equal(t, "rpc-disk-renamed", updateResp.Msg.GetDisk().GetName())
	require.Equal(t, uint64(32<<20), updateResp.Msg.GetDisk().GetMaxBytes())

	rwMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err, "可写 MountDisk RPC 失败")
	require.Equal(t, uint64(1), rwMount.Msg.GetEpoch())
	require.Equal(t, uint64(1), rwMount.Msg.GetRootInode().GetIno())
	require.InDelta(t, 2.5, rwMount.Msg.GetEntryTimeoutSeconds(), 1e-9)
	require.InDelta(t, 3.5, rwMount.Msg.GetAttrTimeoutSeconds(), 1e-9)

	roMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID, ReadOnly: true}))
	require.NoError(t, err, "只读 MountDisk RPC 失败")
	require.Equal(t, uint64(0), roMount.Msg.GetEpoch())

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err, "DeleteDisk RPC 失败")

	_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	requireConnectErrorCodeAndReason(t, err, connect.CodeNotFound, "ENOENT")
}

func TestServerDiskRPC_ForkDisk_CreatesIsolatedChildWithParentDiskID(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	srcResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "fork-src",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err, "CreateDisk 失败")
	srcID := srcResp.Msg.GetDisk().GetDiskId()

	mntResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: srcID}))
	require.NoError(t, err, "MountDisk 失败")

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    srcID,
		Epoch:     mntResp.Msg.GetEpoch(),
		ParentIno: 1,
		Name:      []byte("seed.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Create 文件失败")
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: srcID,
		Epoch:  mntResp.Msg.GetEpoch(),
		Ino:    fileResp.Msg.GetInode().GetIno(),
		Offset: 0,
		Data:   []byte("seed-data"),
	}))
	require.NoError(t, err, "Write 写入失败")

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: srcID,
		Name:         "fork-child",
	}))

	require.NoError(t, err, "ForkDisk RPC 失败")
	forked := forkResp.Msg.GetDisk()
	require.NotNil(t, forked)
	require.Equal(t, srcID, forked.GetParentDiskId(), "派生盘 ParentDiskId 不匹配")
	require.Equal(t, uint64(9), forked.GetUsedBytes(), "派生盘 UsedBytes 不匹配: want=9 got=%d", forked.GetUsedBytes())
}

func TestServerDiskRPC_ValidationErrors_MapsToExpectedConnectCodeAndReason(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	validUUID := "018f0000-0000-7000-8000-000000000001"
	nilUUID := "00000000-0000-0000-0000-000000000000"
	tooLongName := strings.Repeat("d", 256)
	newValidName := "valid"
	zeroQuota := uint64(0)

	cases := []struct {
		name       string
		invoke     func() error
		wantReason string
	}{
		{
			name: "CreateDisk 配额为零",
			invoke: func() error {
				_, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "x", MaxBytes: 0}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "CreateDisk 配额溢出 MaxInt64",
			invoke: func() error {
				_, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "x", MaxBytes: uint64(math.MaxInt64) + 1}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "CreateDisk 名称超长",
			invoke: func() error {
				_, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: tooLongName, MaxBytes: 4096}))
				return err
			},
			wantReason: "ENAMETOOLONG",
		},
		{
			name: "CreateDisk 名称含 NUL",
			invoke: func() error {
				_, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "bad\x00name", MaxBytes: 4096}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "GetDisk 非法 UUID",
			invoke: func() error {
				_, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: "bad-uuid"}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "GetDisk 全零 Nil UUID",
			invoke: func() error {
				_, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: nilUUID}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ListDisks PageSize 越界",
			invoke: func() error {
				_, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 1001}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "UpdateDisk 未传任何更新字段",
			invoke: func() error {
				_, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{DiskId: validUUID}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "UpdateDisk 名称超长",
			invoke: func() error {
				_, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{DiskId: validUUID, Name: &tooLongName}))
				return err
			},
			wantReason: "ENAMETOOLONG",
		},
		{
			name: "UpdateDisk 配额为零",
			invoke: func() error {
				_, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{DiskId: validUUID, MaxBytes: &zeroQuota}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "DeleteDisk 非法 UUID",
			invoke: func() error {
				_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: "invalid"}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "ForkDisk 全零源盘 UUID",
			invoke: func() error {
				_, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: nilUUID, Name: newValidName}))
				return err
			},
			wantReason: "EINVAL",
		},
		{
			name: "MountDisk 全零 UUID",
			invoke: func() error {
				_, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: nilUUID}))
				return err
			},
			wantReason: "EINVAL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.invoke()
			requireConnectErrorCodeAndReason(t, err, connect.CodeInvalidArgument, tc.wantReason)
		})
	}
}

func TestServerDiskRPC_DomainErrors_MapsToExpectedConnectCodeAndReason(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	srcResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "domain-err-disk",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err, "CreateDisk 失败")
	diskID := srcResp.Msg.GetDisk().GetDiskId()

	mntResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: diskID}))
	require.NoError(t, err, "MountDisk 失败")
	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     mntResp.Msg.GetEpoch(),
		ParentIno: 1,
		Name:      []byte("used.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err, "Create 文件失败")
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  mntResp.Msg.GetEpoch(),
		Ino:    fileResp.Msg.GetInode().GetIno(),
		Offset: 0,
		Data:   make([]byte, 8192),
	}))
	require.NoError(t, err, "Write 写入失败")

	missingID := "018f0000-0000-7000-8000-000000009999"
	shrinkQuota := uint64(4096)
	newName := "x"

	cases := []struct {
		name       string
		invoke     func() error
		wantCode   connect.Code
		wantReason string
	}{
		{
			name: "GetDisk 不存在",
			invoke: func() error {
				_, gErr := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: missingID}))
				return gErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
		{
			name: "DeleteDisk 不存在",
			invoke: func() error {
				_, dErr := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: missingID}))
				return dErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
		{
			name: "MountDisk 不存在",
			invoke: func() error {
				_, mErr := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: missingID}))
				return mErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
		{
			name: "ForkDisk 源盘不存在",
			invoke: func() error {
				_, fErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: missingID, Name: "child"}))
				return fErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
		{
			name: "UpdateDisk 不存在",
			invoke: func() error {
				_, uErr := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{DiskId: missingID, Name: &newName}))
				return uErr
			},
			wantCode:   connect.CodeNotFound,
			wantReason: "ENOENT",
		},
		{
			name: "UpdateDisk 缩容低于已用空间",
			invoke: func() error {
				_, uErr := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{DiskId: diskID, MaxBytes: &shrinkQuota}))
				return uErr
			},
			wantCode:   connect.CodeFailedPrecondition,
			wantReason: "EINVAL",
		},
		{
			name: "ForkDisk 缩容低于已用空间",
			invoke: func() error {
				_, fErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{SourceDiskId: diskID, Name: "small", MaxBytes: &shrinkQuota}))
				return fErr
			},
			wantCode:   connect.CodeFailedPrecondition,
			wantReason: "EINVAL",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireConnectErrorCodeAndReason(t, tc.invoke(), tc.wantCode, tc.wantReason)
		})
	}
}

func TestServerErrors_ToConnectError_MapsSentinelsContextAndGRPCStatus(t *testing.T) {

	stWithInfo, err := status.New(codes.PermissionDenied, "perm denied").WithDetails(&errdetails.ErrorInfo{
		Reason: "EPERM",
		Domain: control.ErrorDomain,
	})
	require.NoError(t, err, "构造测试 status 失败")

	require.NoError(t, ToConnectError(nil), "nil 错误转换必须返回 nil")
	require.NoError(t, StatusToConnectError(nil), "nil status 转换必须返回 nil")

	cancelErr := ToConnectError(fmt.Errorf("client aborted: %w", context.Canceled))
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(cancelErr))
	require.ErrorIs(t, cancelErr, context.Canceled)

	deadlineErr := ToConnectError(fmt.Errorf("rpc timed out: %w", context.DeadlineExceeded))
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(deadlineErr))
	require.ErrorIs(t, deadlineErr, context.DeadlineExceeded)

	requireConnectErrorCodeAndReason(t, ToConnectError(store.ErrStaleEpoch), connect.CodeFailedPrecondition, "EBUSY")
	requireConnectErrorCodeAndReason(t, StatusToConnectError(stWithInfo), connect.CodePermissionDenied, "EPERM")

	unknownErr := ToConnectError(errors.New("opaque failure"))
	require.Equal(t, connect.CodeUnknown, connect.CodeOf(unknownErr))
	require.Nil(t, ExtractErrorInfo(unknownErr))
}

func TestServerDiskRPC_MountDisk_NonForceRejectsActiveRWAndForcePreempts(t *testing.T) {

	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "mount-force-rpc-disk",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()

	roMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, roMount.Msg.GetEpoch())

	rw1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), rw1.Msg.GetEpoch())

	_, busyErr := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
	}))

	requireConnectErrorCodeAndReason(t, busyErr, connect.CodeAborted, "EBUSY")
	_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     rw1.Msg.GetEpoch(),
		ParentIno: 1,
		Name:      []byte("still-owner.txt"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err)

	rw2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
		Force:  true,
	}))

	require.NoError(t, err)
	require.Equal(t, uint64(2), rw2.Msg.GetEpoch())

	_, fencedErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     rw1.Msg.GetEpoch(),
		ParentIno: 1,
		Name:      []byte("fenced.txt"),
		Mode:      store.S_IFREG | 0o644,
	}))
	requireConnectErrorCodeAndReason(t, fencedErr, connect.CodeFailedPrecondition, "EBUSY")
}
