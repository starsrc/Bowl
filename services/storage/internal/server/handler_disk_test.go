package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestHandlerDisk_LifecycleAndQuotaGuards(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, nil)

	d1 := env.createDisk(t, "disk-one", 4*1024*1024)
	require.NotEmpty(t, d1.GetDiskId())
	require.Equal(t, "disk-one", d1.GetName())
	require.Equal(t, defaultBranch(), d1.GetDefaultBranch())
	require.Equal(t, uint64(4*1024*1024), d1.GetMaxBytes())
	require.Zero(t, d1.GetUsedBytes())

	d2 := env.createDisk(t, "disk-two", 8*1024*1024)
	d3 := env.createDisk(t, "disk-three", 16*1024*1024)

	gotResp, err := env.client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: d2.GetDiskId(),
	}))
	require.NoError(t, err)
	require.Equal(t, d2.GetDiskId(), gotResp.Msg.GetDisk().GetDiskId())
	require.Equal(t, "disk-two", gotResp.Msg.GetDisk().GetName())

	page1, err := env.client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize: 2,
	}))
	require.NoError(t, err)
	require.Len(t, page1.Msg.GetDisks(), 2)
	require.NotEmpty(t, page1.Msg.GetNextPageToken())

	page2, err := env.client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  2,
		PageToken: page1.Msg.GetNextPageToken(),
	}))
	require.NoError(t, err)
	require.Len(t, page2.Msg.GetDisks(), 1)
	require.Empty(t, page2.Msg.GetNextPageToken())

	listed := []string{
		page1.Msg.GetDisks()[0].GetDiskId(),
		page1.Msg.GetDisks()[1].GetDiskId(),
		page2.Msg.GetDisks()[0].GetDiskId(),
	}
	require.ElementsMatch(t, []string{d1.GetDiskId(), d2.GetDiskId(), d3.GetDiskId()}, listed)

	newName := "disk-one-renamed"
	newQuota := uint64(2 * 1024 * 1024)
	updResp, err := env.client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   d1.GetDiskId(),
		Name:     &newName,
		MaxBytes: &newQuota,
	}))
	require.NoError(t, err)
	require.Equal(t, newName, updResp.Msg.GetDisk().GetName())
	require.Equal(t, newQuota, updResp.Msg.GetDisk().GetMaxBytes())

	mount := env.mountBranchRW(t, d1.GetDiskId(), defaultBranch(), false)
	createdFile, err := env.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    d1.GetDiskId(),
		Branch:    defaultBranch(),
		ParentIno: engine.RootIno,
		Name:      []byte("blob.bin"),
		Mode:      0o644,
		Epoch:     mount.GetEpoch(),
	}))
	require.NoError(t, err)

	payload := make([]byte, 8192)
	_, err = env.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: d1.GetDiskId(),
		Branch: defaultBranch(),
		Ino:    createdFile.Msg.GetInode().GetIno(),
		Offset: 0,
		Data:   payload,
		Epoch:  mount.GetEpoch(),
	}))
	require.NoError(t, err)

	tooSmall := uint64(1024)
	_, err = env.client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   d1.GetDiskId(),
		MaxBytes: &tooSmall,
	}))
	requireConnectErr(t, err, connect.CodeFailedPrecondition, "EINVAL")

	_, err = env.client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: d1.GetDiskId(),
	}))
	requireConnectErr(t, err, connect.CodeAborted, "EBUSY")

	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  d1.GetDiskId(),
		MountId: mount.GetMountId(),
	}))
	require.NoError(t, err)

	_, err = env.client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: d1.GetDiskId(),
	}))
	require.NoError(t, err)

	_, err = env.client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: d1.GetDiskId(),
	}))
	requireConnectErr(t, err, connect.CodeNotFound, "ENOENT")
}

func TestHandlerMount_LeasesEpochFencingAndEvictMounts(t *testing.T) {
	ctx := context.Background()
	env := newTestEnv(t, &config.MetaConfig{
		AtimeModeVal:              config.DefaultAtimeMode,
		EntryTimeoutSecondsVal:    2.5,
		AttrTimeoutSecondsVal:     3.5,
		DirEntryTimeoutSecondsVal: 4.5,
	})

	disk := env.createDisk(t, "mount-disk", 4*1024*1024)
	diskID := disk.GetDiskId()

	_, err := env.client.ForkBranch(ctx, connect.NewRequest(&storagev1.ForkBranchRequest{
		DiskId:       diskID,
		SourceBranch: defaultBranch(),
		NewBranch:    "feat",
		Message:      "fork feat",
	}))
	require.NoError(t, err)

	ro1, err := env.client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId:   diskID,
		Branch:   defaultBranch(),
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, ro1.Msg.GetEpoch())
	require.Equal(t, engine.RootIno, ro1.Msg.GetRootInode().GetIno())
	require.InDelta(t, 4.5, ro1.Msg.GetEntryTimeoutSeconds(), 1e-9)
	require.InDelta(t, 3.5, ro1.Msg.GetAttrTimeoutSeconds(), 1e-9)
	require.Equal(t, uint32(30), ro1.Msg.GetLeaseTtlSeconds())

	rw1 := env.mountBranchRW(t, diskID, defaultBranch(), false)
	require.Greater(t, rw1.GetEpoch(), uint64(1))
	require.Equal(t, engine.RootIno, rw1.GetRootInode().GetIno())

	_, err = env.client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: defaultBranch(),
	}))
	requireConnectErr(t, err, connect.CodeAborted, "EBUSY")

	renewRO, err := env.client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   defaultBranch(),
		MountId:  ro1.Msg.GetMountId(),
		ReadOnly: true,
		Epoch:    0,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRO.Msg.GetLeaseTtlSeconds())

	renewRW, err := env.client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   defaultBranch(),
		MountId:  rw1.GetMountId(),
		ReadOnly: false,
		Epoch:    rw1.GetEpoch(),
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(30), renewRW.Msg.GetLeaseTtlSeconds())

	rw2 := env.mountBranchRW(t, diskID, defaultBranch(), true)
	require.Equal(t, rw1.GetEpoch()+1, rw2.GetEpoch())

	_, err = env.client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{
		DiskId:   diskID,
		Branch:   defaultBranch(),
		MountId:  rw1.GetMountId(),
		ReadOnly: false,
		Epoch:    rw1.GetEpoch(),
	}))
	requireConnectErr(t, err, connect.CodeFailedPrecondition, "EBUSY")

	featRW := env.mountBranchRW(t, diskID, "feat", false)

	allMounts, err := env.client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Len(t, allMounts.Msg.GetMounts(), 3)

	featMounts, err := env.client.ListMounts(ctx, connect.NewRequest(&storagev1.ListMountsRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	require.Len(t, featMounts.Msg.GetMounts(), 1)
	require.Equal(t, featRW.GetMountId(), featMounts.Msg.GetMounts()[0].GetMountId())
	require.Equal(t, "feat", featMounts.Msg.GetMounts()[0].GetBranch())

	evictFeat, err := env.client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{
		DiskId: diskID,
		Branch: "feat",
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(1), evictFeat.Msg.GetEvictedCount())
	require.Len(t, evictFeat.Msg.GetUpdatedBranches(), 1)
	require.Equal(t, "feat", evictFeat.Msg.GetUpdatedBranches()[0].GetName())
	require.Equal(t, featRW.GetEpoch()+1, evictFeat.Msg.GetUpdatedBranches()[0].GetWriterEpoch())

	evictRest, err := env.client.EvictMounts(ctx, connect.NewRequest(&storagev1.EvictMountsRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	require.Equal(t, uint32(2), evictRest.Msg.GetEvictedCount())
	require.Len(t, evictRest.Msg.GetUpdatedBranches(), 1)
	require.Equal(t, defaultBranch(), evictRest.Msg.GetUpdatedBranches()[0].GetName())
	require.Equal(t, rw2.GetEpoch()+1, evictRest.Msg.GetUpdatedBranches()[0].GetWriterEpoch())

	_, err = env.client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{
		DiskId:  diskID,
		MountId: rw2.GetMountId(),
	}))
	require.NoError(t, err)
}
