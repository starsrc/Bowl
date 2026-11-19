package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/handoff"
)

// handoffReasonOf 取出交接专用 ErrorInfo 的 Reason。
func handoffReasonOf(t *testing.T, err error) string {
	t.Helper()
	var connectErr *connect.Error
	require.ErrorAs(t, err, &connectErr)
	for _, detail := range connectErr.Details() {
		msg, valueErr := detail.Value()
		require.NoError(t, valueErr)
		if info, ok := msg.(*errdetails.ErrorInfo); ok && info.GetDomain() == HandoffErrorDomain {
			return info.GetReason()
		}
	}
	return ""
}

func TestServer_交接RPC_推送查询与失败原因(t *testing.T) {
	remote, err := handoff.NewDirRemote(t.TempDir())
	require.NoError(t, err)
	_, client := newTestServerAndClient(t, control.WithHandoff(remote, "local"))
	ctx := context.Background()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "handoff", MaxBytes: 1 << 20}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()

	before, err := client.GetDiskHandoffStatus(ctx, connect.NewRequest(&storagev1.GetDiskHandoffStatusRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_UNBOUND, before.Msg.GetRole())
	require.Nil(t, before.Msg.Head, "没推送过的盘不应带远端记录")

	_, err = client.GetDiskHead(ctx, connect.NewRequest(&storagev1.GetDiskHeadRequest{DiskId: diskID}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	require.Equal(t, "NOT_FOUND", handoffReasonOf(t, err))

	pushed, err := client.PushDisk(ctx, connect.NewRequest(&storagev1.PushDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, "local", pushed.Msg.GetHead().GetOwner())
	require.Equal(t, uint64(1), pushed.Msg.GetHead().GetGeneration())
	require.NotNil(t, pushed.Msg.GetHead().GetUpdatedAt())

	head, err := client.GetDiskHead(ctx, connect.NewRequest(&storagev1.GetDiskHeadRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, pushed.Msg.GetHead().GetSnapshot(), head.Msg.GetHead().GetSnapshot())

	after, err := client.GetDiskHandoffStatus(ctx, connect.NewRequest(&storagev1.GetDiskHandoffStatusRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, storagev1.DiskHandoffRole_DISK_HANDOFF_ROLE_WRITER, after.Msg.GetRole())
	require.Equal(t, "local", after.Msg.GetNodeId())

	pulled, err := client.PullDisk(ctx, connect.NewRequest(&storagev1.PullDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.False(t, pulled.Msg.GetInstalled(), "写权限本来就在本节点时不应替换本地副本")

	released, err := client.PushDisk(ctx, connect.NewRequest(&storagev1.PushDiskRequest{DiskId: diskID, Release: true}))
	require.NoError(t, err)
	require.Empty(t, released.Msg.GetHead().GetOwner())

	_, err = client.PushDisk(ctx, connect.NewRequest(&storagev1.PushDiskRequest{DiskId: "not-a-uuid"}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestServer_交接RPC_未配置远端时返回Unimplemented(t *testing.T) {
	_, client := newTestServerAndClient(t)
	_, err := client.GetDiskHead(context.Background(), connect.NewRequest(&storagev1.GetDiskHeadRequest{DiskId: "01950000-0000-7000-8000-000000000001"}))
	require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
	require.Equal(t, "DISABLED", handoffReasonOf(t, err))
}
