package server

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func createTestFileViaRPC(
	t *testing.T,
	ctx context.Context,
	client interface {
		Create(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
		Write(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	},
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) uint64 {
	t.Helper()
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		ParentIno: control.RootIno,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	ino := createResp.Msg.GetInode().GetIno()
	if len(data) > 0 {
		_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID,
			Ino:    ino,
			Offset: 0,
			Data:   data,
			Epoch:  epoch,
		}))
		require.NoError(t, err)
	}
	return ino
}

func overwriteTestFileViaRPC(
	t *testing.T,
	ctx context.Context,
	client interface {
		Lookup(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
		Write(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
		SetAttr(context.Context, *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error)
	},
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: control.RootIno,
		Name:      []byte(name),
	}))
	require.NoError(t, err)
	ino := lookupResp.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Ino:    ino,
		Offset: 0,
		Data:   data,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
	size := uint64(len(data))
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID,
		Ino:    ino,
		Size:   &size,
		Epoch:  epoch,
	}))
	require.NoError(t, err)
}

func TestServer_DiffReadCommitNodeAndMergeDisk_EndToEndWireContract(t *testing.T) {
	// Given: 通过 ConnectRPC 内存客户端创建主盘、写初始文件与符号链接、Fork 分支并制造冲突与独立变更
	ctx := context.Background()
	_, client := newTestServerAndClient(t)

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "main",
		MaxBytes: 32 << 20,
	}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()

	mainMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()

	cfgIno := createTestFileViaRPC(t, ctx, client, mainID, mainEpoch, "cfg.ini", []byte("key=base\n"))
	symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("cur.ini"),
		Target:    []byte("/cfg.ini"),
		Epoch:     mainEpoch,
	}))
	require.NoError(t, err)

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         "feature",
	}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()

	featMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()

	// 制造双边冲突 cfg.ini 及源盘新增 feature.txt
	overwriteTestFileViaRPC(t, ctx, client, mainID, mainEpoch, "cfg.ini", []byte("key=main\n"))
	overwriteTestFileViaRPC(t, ctx, client, featID, featEpoch, "cfg.ini", []byte("key=feature\n"))
	createTestFileViaRPC(t, ctx, client, featID, featEpoch, "feature.txt", []byte("feat-content\n"))

	// When 1: DiffDisk 省略 target_disk_id（2-way diff）与指定 target_disk_id（3-way diff + 分页）
	diffTwoWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
	}))
	require.NoError(t, err)
	lcaCommitID := diffTwoWay.Msg.GetLcaCommitId()
	require.NotEmpty(t, lcaCommitID)
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetAdded())
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetModified())

	diffPage1, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: &mainID,
		PageSize:     1,
	}))
	require.NoError(t, err)
	require.Len(t, diffPage1.Msg.GetEntries(), 1)
	require.NotEmpty(t, diffPage1.Msg.GetNextPageToken())
	require.Equal(t, uint64(1), diffPage1.Msg.GetSummary().GetConflicts())

	// When 2: ReadCommitNode 分片读取不可变 LCA 切面文件与符号链接
	nodeSlice, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   featID,
		CommitId: lcaCommitID,
		Ino:      cfgIno,
		Offset:   4,
		Size:     16,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("base\n"), nodeSlice.Msg.GetData())
	require.True(t, nodeSlice.Msg.GetEof())

	symSlice, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   featID,
		CommitId: lcaCommitID,
		Ino:      symResp.Msg.GetInode().GetIno(),
		Offset:   0,
		Size:     128,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("/cfg.ini"), symSlice.Msg.GetSymlinkTarget())

	// 未卸载且未传 Force 时，MergeDisk 应被门禁拦截为 CodeAborted + EBUSY
	_, busyErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
	}))
	requireConnectErrorCodeAndReason(t, busyErr, connect.CodeAborted, "EBUSY")

	// 卸载源盘租约（保留目标盘 mainID 在线租约），后续用 Force=true 验证强行合并与旧 epoch 栅栏
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId: featID,
		Epoch:  featEpoch,
	}))
	require.NoError(t, err)

	// When 3: MergeDisk 默认 Fail 策略命中冲突，应返回 CodeAborted + ErrorInfo(ECONFLICT) + *MergeConflictReport Detail
	_, mergeErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Force:        true,
	}))
	requireConnectErrorCodeAndReason(t, mergeErr, connect.CodeAborted, "ECONFLICT")

	connectErr, ok := errors.AsType[*connect.Error](mergeErr)
	require.True(t, ok)
	var conflictReport *storagev1.MergeConflictReport
	for _, detail := range connectErr.Details() {
		msg, valErr := detail.Value()
		if valErr != nil {
			continue
		}
		if report, isReport := msg.(*storagev1.MergeConflictReport); isReport {
			conflictReport = report
			break
		}
	}
	require.NotNil(t, conflictReport, "ECONFLICT 响应必须附带 *storagev1.MergeConflictReport Detail")
	require.Equal(t, uint64(1), conflictReport.GetTotalConflicts())
	require.Len(t, conflictReport.GetConflicts(), 1)
	require.Equal(t, []byte("cfg.ini"), conflictReport.GetConflicts()[0].GetPath())
	require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, conflictReport.GetConflicts()[0].GetKind())

	// When 4: DryRun 预演合并返回估算容量且不推进 epoch 或生成 commit
	overrideQuota := uint64(64 << 20)
	dryResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		DryRun:       true,
		MaxBytes:     &overrideQuota,
	}))
	require.NoError(t, err)
	require.Empty(t, dryResp.Msg.GetMergeCommitId())
	require.Equal(t, overrideQuota, dryResp.Msg.GetTargetMaxBytes())

	// When 5: 使用 PathStrategies + Force 正式提交合并并扩容配额
	mergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"/cfg.ini": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
		Force:    true,
		MaxBytes: &overrideQuota,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, mergeResp.Msg.GetMergeCommitId())
	require.Equal(t, overrideQuota, mergeResp.Msg.GetDisk().GetMaxBytes())

	// 验证旧 epoch 写入被原子切换后的新 WriterEpoch 栅栏拒绝，新数据可读
	_, staleErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("stale.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mainEpoch,
	}))
	requireConnectErrorCodeAndReason(t, staleErr, connect.CodeFailedPrecondition, "EBUSY")

	lookupFeat, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: control.RootIno,
		Name:      []byte("feature.txt"),
	}))
	require.NoError(t, err)
	readFeat, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-content\n"), readFeat.Msg.GetData())

	// 被 Force 抢占后，旧写者以原 mainEpoch 调用 RenewMount 应返回 CodeFailedPrecondition + EBUSY
	_, renewStaleErr := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId: mainID,
		Epoch:  mainEpoch,
	}))
	requireConnectErrorCodeAndReason(t, renewStaleErr, connect.CodeFailedPrecondition, "EBUSY")

	// 重新挂载 mainID 后 RenewMount 应成功返回 TTL = 30
	mainMnt2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	renewResp, err := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId: mainID,
		Epoch:  mainMnt2.Msg.GetEpoch(),
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewResp.Msg.GetLeaseTtlSeconds())
}

func TestToConnectError_ConcurrentMerge_MapsToAbortedEBUSY(t *testing.T) {
	requireConnectErrorCodeAndReason(t, toConnectError(control.ErrConcurrentMerge), connect.CodeAborted, "EBUSY")
}
