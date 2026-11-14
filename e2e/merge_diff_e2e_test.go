//go:build e2e

package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func e2eRegisterDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			require.NoError(t, err, "清理测试磁盘失败: %s", diskID)
		}
	})
}

func e2eWriteNewFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) uint64 {
	t.Helper()
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte(name),
		Mode:      0o100644,
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

func e2eOverwriteFile(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	epoch uint64,
	name string,
	data []byte,
) {
	t.Helper()
	lookupResp, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
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

func TestE2E_DiffReadCommitNodeAndMergeDisk_WithPostgreSQLAndWriterFence(t *testing.T) {
	// Given: 连接真实 storage-service + PostgreSQL，创建主盘、写入基线并 Fork 分支盘
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	suffix := time.Now().UnixNano()

	mainResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     fmt.Sprintf("e2e-merge-main-%d", suffix),
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	mainID := mainResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, mainID)

	mainMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	mainEpoch := mainMnt.Msg.GetEpoch()
	require.Equal(t, uint32(30), mainMnt.Msg.GetLeaseTtlSeconds())

	// 只读挂载支持无限并发且不颁发写租约 TTL，也不得阻塞 RW 写者。
	for range 3 {
		roMnt, roErr := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID, ReadOnly: true}))
		require.NoError(t, roErr)
		require.Zero(t, roMnt.Msg.GetEpoch())
		require.Zero(t, roMnt.Msg.GetLeaseTtlSeconds())
	}

	sharedIno := e2eWriteNewFile(t, ctx, client, mainID, mainEpoch, "shared.txt", []byte("base-shared\n"))
	e2eWriteNewFile(t, ctx, client, mainID, mainEpoch, "base.txt", []byte("base-only\n"))

	forkResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         fmt.Sprintf("e2e-merge-feat-%d", suffix),
	}))
	require.NoError(t, err)
	featID := forkResp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, featID)

	featMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID}))
	require.NoError(t, err)
	featEpoch := featMnt.Msg.GetEpoch()

	// 在双边同时修改 shared.txt 并各自新增独占文件
	e2eOverwriteFile(t, ctx, client, mainID, mainEpoch, "shared.txt", []byte("main-edited\n"))
	e2eOverwriteFile(t, ctx, client, featID, featEpoch, "shared.txt", []byte("feat-edited\n"))
	e2eWriteNewFile(t, ctx, client, featID, featEpoch, "feat.txt", []byte("feat-added\n"))

	// When 1: DiffDisk 两路与三路对比 + ReadCommitNode 读取 LCA 基线内容
	diffTwoWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId: featID,
	}))
	require.NoError(t, err)
	lcaCommitID := diffTwoWay.Msg.GetLcaCommitId()
	require.NotEmpty(t, lcaCommitID)
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetAdded())
	require.Equal(t, uint64(1), diffTwoWay.Msg.GetSummary().GetModified())

	diffThreeWay, err := client.DiffDisk(ctx, connect.NewRequest(&storagev1.DiffDiskRequest{
		SourceDiskId:  featID,
		TargetDiskId:  &mainID,
		ConflictsOnly: true,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), diffThreeWay.Msg.GetSummary().GetConflicts())
	require.Len(t, diffThreeWay.Msg.GetEntries(), 1)
	require.Equal(t, []byte("shared.txt"), diffThreeWay.Msg.GetEntries()[0].GetPath())
	require.Equal(t, storagev1.ConflictKind_CONFLICT_KIND_CONTENT, diffThreeWay.Msg.GetEntries()[0].GetConflict())

	commitSlice, err := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{
		DiskId:   featID,
		CommitId: lcaCommitID,
		Ino:      sharedIno,
		Offset:   0,
		Size:     1024,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("base-shared\n"), commitSlice.Msg.GetData())
	require.True(t, commitSlice.Msg.GetEof())

	// When 2: 验证双盘在线挂载租约门禁契约
	// 2.1 DryRun=true 允许在挂载态下直接预演成功且不修改数据或触发 epoch 围栏
	dryResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
		DryRun:       true,
	}))
	require.NoError(t, err)
	require.Empty(t, dryResp.Msg.GetMergeCommitId())

	// 2.2 未卸载且 Force=false 时，MergeDisk 被服务端挂载门禁拦截为 CodeAborted + Reason=EBUSY
	_, busyBothErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busyBothErr, connect.CodeAborted, "EBUSY")

	// 2.3 仅 UnmountDisk 释放源盘 featID 并挂源盘 RO、保留目标盘 mainID RW 时，MergeDisk(Force:false) 仍被 EBUSY 拦截
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId: featID,
		Epoch:  featEpoch,
	}))
	require.NoError(t, err)
	_, err = client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: featID, ReadOnly: true}))
	require.NoError(t, err)

	_, busyTargetErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
	}))
	testutil.RequireConnectError(t, busyTargetErr, connect.CodeAborted, "EBUSY")

	// When 3 (方式一): UnmountDisk 释放目标盘 mainID 后（源盘 RO 无阻塞），校验冲突检测与无 Force 正式合并成功
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{
		DiskId: mainID,
		Epoch:  mainEpoch,
	}))
	require.NoError(t, err)

	// Strategy=FAIL 命中冲突，校验 CodeAborted + ECONFLICT + MergeConflictReport
	_, conflictErr := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
	}))
	testutil.RequireConnectError(t, conflictErr, connect.CodeAborted, "ECONFLICT")

	connErr, ok := errors.AsType[*connect.Error](conflictErr)
	require.True(t, ok)
	var report *storagev1.MergeConflictReport
	for _, d := range connErr.Details() {
		v, valErr := d.Value()
		if valErr == nil {
			if r, isRep := v.(*storagev1.MergeConflictReport); isRep {
				report = r
				break
			}
		}
	}
	require.NotNil(t, report)
	require.Equal(t, lcaCommitID, report.GetLcaCommitId())
	require.Equal(t, uint64(1), report.GetTotalConflicts())

	// PathStrategies 定向裁决后在已卸载态正式提交合并 (Force=false)
	mergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: featID,
		TargetDiskId: mainID,
		Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
		PathStrategies: map[string]storagev1.MergeStrategy{
			"/shared.txt": storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
		},
	}))
	require.NoError(t, err)
	require.NotEmpty(t, mergeResp.Msg.GetMergeCommitId())

	// Then: 旧 mainEpoch 写入被栅栏拒绝（FailedPrecondition + EBUSY），重新挂载后验证合并数据
	_, staleErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("stale.txt"),
		Mode:      0o100644,
		Epoch:     mainEpoch,
	}))
	testutil.RequireConnectError(t, staleErr, connect.CodeFailedPrecondition, "EBUSY")

	reMnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	reMntEpoch := reMnt.Msg.GetEpoch()
	require.Greater(t, reMntEpoch, mainEpoch)

	lookupFeat, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("feat.txt"),
	}))
	require.NoError(t, err)
	readFeat, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-added\n"), readFeat.Msg.GetData())

	lookupShared, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("shared.txt"),
	}))
	require.NoError(t, err)
	readShared, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupShared.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat-edited\n"), readShared.Msg.GetData())

	// When 4 (方式二): 验证 Force=true 抢占围栏契约
	// 保持 mainID 在 reMntEpoch 在线挂载不卸载，并从 mainID Fork 新分支盘 feat2ID 写入后亦不卸载
	fork2Resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: mainID,
		Name:         fmt.Sprintf("e2e-merge-feat2-%d", suffix),
	}))
	require.NoError(t, err)
	feat2ID := fork2Resp.Msg.GetDisk().GetDiskId()
	e2eRegisterDiskCleanup(t, client, feat2ID)

	feat2Mnt, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: feat2ID}))
	require.NoError(t, err)
	e2eWriteNewFile(t, ctx, client, feat2ID, feat2Mnt.Msg.GetEpoch(), "feat2.txt", []byte("feat2-forced\n"))

	forceMergeResp, err := client.MergeDisk(ctx, connect.NewRequest(&storagev1.MergeDiskRequest{
		SourceDiskId: feat2ID,
		TargetDiskId: mainID,
		Force:        true,
	}))
	require.NoError(t, err)
	require.NotEmpty(t, forceMergeResp.Msg.GetMergeCommitId())

	// Then: 原客户端持旧 reMntEpoch 写入与 RenewMount 均被 CodeFailedPrecondition (EBUSY) 栅栏拦截
	_, fencedErr := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("fenced-after-force.txt"),
		Mode:      0o100644,
		Epoch:     reMntEpoch,
	}))
	testutil.RequireConnectError(t, fencedErr, connect.CodeFailedPrecondition, "EBUSY")

	_, staleRenewErr := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId: mainID,
		Epoch:  reMntEpoch,
	}))
	testutil.RequireConnectError(t, staleRenewErr, connect.CodeFailedPrecondition, "EBUSY")

	reMntAfterForce, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: mainID}))
	require.NoError(t, err)
	require.Greater(t, reMntAfterForce.Msg.GetEpoch(), reMntEpoch)

	renewOKResp, err := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId: mainID,
		Epoch:  reMntAfterForce.Msg.GetEpoch(),
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewOKResp.Msg.GetLeaseTtlSeconds())

	lookupFeat2, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    mainID,
		ParentIno: 1,
		Name:      []byte("feat2.txt"),
	}))
	require.NoError(t, err)
	readFeat2, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: mainID,
		Ino:    lookupFeat2.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   100,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("feat2-forced\n"), readFeat2.Msg.GetData())
}
