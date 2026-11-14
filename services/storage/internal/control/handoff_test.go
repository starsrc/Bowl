package control

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/handoff"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

// newHandoffNode 起一个独立的存储节点：自己的控制面库、自己的数据目录，只和别的节点共用 remote。
func newHandoffNode(t *testing.T, remote handoff.Remote, nodeID string) *manager {
	t.Helper()
	mgr := newManager(db.NewMemStore(), WithStorageRoot(t.TempDir()), WithHandoff(remote, nodeID))
	t.Cleanup(func() { _ = mgr.Close() })
	return mgr
}

func newSharedRemote(t *testing.T) handoff.Remote {
	t.Helper()
	remote, err := handoff.NewDirRemote(t.TempDir())
	require.NoError(t, err)
	return remote
}

func TestHandoff_RoundTrip_本地交给云端再交回来(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	// Given: 本地节点上一块写过数据的盘
	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	localActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	fromLocal := bytes.Repeat([]byte("WRITTEN-ON-LOCAL-"), 4096) // 跨多个 256 KiB 块之内的一段真实数据
	writeRegularFile(t, ctx, localActor, mount.Epoch, "local.txt", fromLocal)

	// 写者还挂着时不允许释放
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.ErrorIs(t, err, ErrDiskMounted)
	require.NoError(t, local.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mount.MountID}))

	// When: 本地释放，云端认领
	pushed, err := local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)
	require.Empty(t, pushed.Head.Owner, "释放后 HEAD 不应有写者")
	require.Positive(t, pushed.Stats.PacksUploaded)

	pulled, err := cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Equal(t, "cloud", pulled.Head.Owner)

	// Then: 云端看到同一份数据、同一个盘名，并能继续写
	cloudDisk, err := cloud.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "workspace", cloudDisk.Name)
	cloudMount, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Greater(t, cloudMount.Epoch, mount.Epoch, "写世代必须跨节点单调递增")
	cloudActor, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, fromLocal, readRegularFile(t, ctx, cloudActor, "local.txt", uint32(len(fromLocal)+64)))
	fromCloud := []byte("written-on-cloud")
	writeRegularFile(t, ctx, cloudActor, cloudMount.Epoch, "cloud.txt", fromCloud)

	// 本地已不是写者：可写挂载被 HEAD 挡住，推送也被拒
	_, err = local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)
	// 云端没释放之前，本地也认领不回来
	_, err = local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)

	// When: 云端交回本地
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: cloudMount.MountID}))
	back, err := cloud.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)
	require.Equal(t, back.Stats.Packs-1, back.Stats.Packs-back.Stats.PacksUploaded,
		"交回时只应上传云端新写出来的那个包，本地原来的包按名字复用")
	pulledBack, err := local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Equal(t, 1, pulledBack.Pulled.PacksDeferred, "切回去过的节点时只有离开期间新写出来的包需要从远端取")
	require.Equal(t, pulledBack.Pulled.Packs-1, pulledBack.Pulled.PacksReused, "本地还留着的包按内容复用")
	require.Equal(t, pulled.Pulled.Packs, pulled.Pulled.PacksDeferred, "第一次去一个新节点：一个包都不用先下载")
	require.Zero(t, pulled.Pulled.PacksDownloaded)

	// Then: 本地同时看到两边写的内容
	localMount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Greater(t, localMount.Epoch, cloudMount.Epoch)
	localActor, err = local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, fromLocal, readRegularFile(t, ctx, localActor, "local.txt", uint32(len(fromLocal)+64)))
	require.Equal(t, fromCloud, readRegularFile(t, ctx, localActor, "cloud.txt", 64))
}

func TestHandoff_ForceTakeover_强行接管后旧写者不能覆盖也不会被静默覆盖(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	localActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, localActor, mount.Epoch, "synced.txt", []byte("synced"))

	// Given: 本地只做了一次同步（没释放），之后又写了一个没同步的文件，然后"失联"
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	writeRegularFile(t, ctx, localActor, mount.Epoch, "unsynced.txt", []byte("only on local"))

	// 不带 force 认领不了别人持有的盘
	_, err = cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)

	// When: 云端强行接管
	_, err = cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID, Force: true})
	require.NoError(t, err)
	cloudActor, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)

	// Then: 云端拿到的是最后一次同步的状态，没同步的那部分不在
	require.Equal(t, []byte("synced"), readRegularFile(t, ctx, cloudActor, "synced.txt", 64))
	_, _, lookupErr := cloudActor.Lookup(ctx, RootIno, []byte("unsynced.txt"))
	require.ErrorIs(t, lookupErr, ErrNotFound)

	// 旧写者回来后：推不上去（不会覆盖云端），也不会被一次普通的 pull 静默抹掉本地未同步的写入
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)
	require.NoError(t, local.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mount.MountID}))
	cloudMount, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: cloudMount.MountID}))
	_, err = cloud.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)
	_, err = local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrLocalDiverged)

	// 未同步的写入可以先 fork 出来保住，再强制拉取
	rescued, err := local.ForkDisk(ctx, ForkDiskParams{SourceDiskID: disk.DiskID, Name: "rescued"})
	require.NoError(t, err)
	_, err = local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID, Force: true})
	require.NoError(t, err)
	rescuedActor, err := local.Actor(ctx, rescued.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("only on local"), readRegularFile(t, ctx, rescuedActor, "unsynced.txt", 64))
}

func TestHandoff_ConcurrentClaim_两个节点同时认领只有一个成功(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)

	results := make(chan error, 2)
	for _, node := range []string{"cloud-a", "cloud-b"} {
		mgr := newHandoffNode(t, remote, node)
		go func() {
			_, pullErr := mgr.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
			results <- pullErr
		}()
	}
	first, second := <-results, <-results
	if first != nil {
		first, second = second, first
	}
	require.NoError(t, first, "必须有一个节点认领成功")
	require.Error(t, second, "另一个节点必须失败")

	head, err := local.HeadDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Contains(t, []string{"cloud-a", "cloud-b"}, head.Owner)
}

func TestHandoff_BackgroundSync_只同步有新写入的已绑定磁盘(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mount.Epoch, "first.txt", []byte("first"))

	// 从没推送过的盘不参与后台同步
	require.Zero(t, local.syncDirtyDisks(ctx), "未绑定远端的盘不应被自动上传")
	_, err = local.HeadDisk(ctx, disk.DiskID)
	require.ErrorIs(t, err, ErrNotFound)

	// 显式推送一次即绑定
	bound, err := local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)

	// 续租 = 仍有写者在；之后写入的内容由后台同步带上去
	_, err = local.RenewMount(ctx, RenewMountParams{DiskID: disk.DiskID, MountID: mount.MountID, Epoch: mount.Epoch})
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mount.Epoch, "second.txt", []byte("second"))
	require.Equal(t, 1, local.syncDirtyDisks(ctx))
	head, err := local.HeadDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, bound.Head.Generation+1, head.Generation)
	require.Equal(t, "local", head.Owner, "后台同步不改变写者")

	// 没有新的写入活动时什么都不传
	require.Zero(t, local.syncDirtyDisks(ctx))
	_, err = local.RenewMount(ctx, RenewMountParams{DiskID: disk.DiskID, MountID: mount.MountID, Epoch: mount.Epoch})
	require.NoError(t, err)
	require.Zero(t, local.syncDirtyDisks(ctx), "有写者但没有新写入时不应产生新快照")
	unchanged, err := local.HeadDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, head.Generation, unchanged.Generation)

	// 云端强行接管能拿到后台同步上去的内容
	_, err = cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID, Force: true})
	require.NoError(t, err)
	cloudActor, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), readRegularFile(t, ctx, cloudActor, "second.txt", 64))

	// 旧写者在下一次续租时被切断：续租失败，之后用旧世代的写入也失败
	_, err = local.RenewMount(ctx, RenewMountParams{DiskID: disk.DiskID, MountID: mount.MountID, Epoch: mount.Epoch})
	require.ErrorIs(t, err, ErrNotOwner)
	_, createErr := actor.Create(ctx, store.CreateReq{ParentIno: RootIno, Name: []byte("late.txt"), Mode: store.S_IFREG | 0o644, Epoch: mount.Epoch})
	require.ErrorIs(t, createErr, ErrStaleEpoch, "被接管后旧写者不能再落盘")
}

func TestHandoff_BackgroundSyncLoop_按周期自动上传并随Close退出(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newManager(db.NewMemStore(), WithStorageRoot(t.TempDir()),
		WithHandoff(remote, "local"), WithHandoffSync(20*time.Millisecond))

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	bound, err := local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	actor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, actor, mount.Epoch, "auto.txt", []byte("auto"))

	require.Eventually(t, func() bool {
		head, headErr := local.HeadDisk(ctx, disk.DiskID)
		return headErr == nil && head.Generation > bound.Head.Generation
	}, 5*time.Second, 20*time.Millisecond, "后台同步应在几个周期内把新写入传上去")

	require.NoError(t, local.Close(), "Close 必须能停掉后台同步并正常退出")
}

func TestHandoff_MountFollowsRemote_挂载时自动跟上远端(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	localActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, localActor, mount.Epoch, "v1.txt", []byte("v1"))
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)

	// 写者在本地：云端从没见过这块盘，只读挂载直接得到最新同步的内容
	ro, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	cloudActor, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), readRegularFile(t, ctx, cloudActor, "v1.txt", 16))
	// 但云端不能写
	_, err = cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrNotOwner)

	// 本地继续写并同步；云端重新只读挂载后看到新内容
	writeRegularFile(t, ctx, localActor, mount.Epoch, "v2.txt", []byte("v2"))
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: ro.MountID}))
	ro, err = cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	cloudActor, err = cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), readRegularFile(t, ctx, cloudActor, "v2.txt", 16))
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: ro.MountID}))
	head, err := cloud.HeadDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "local", head.Owner, "只读跟随不改变写者")

	// 本地释放后，云端直接可写挂载即可，不需要先手动 pull
	require.NoError(t, local.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mount.MountID}))
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)
	rw, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	cloudActor, err = cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, cloudActor, rw.Epoch, "v3.txt", []byte("v3"))
	head, err = cloud.HeadDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "cloud", head.Owner)
}

func TestHandoff_LazyPull_拉取不下载数据包且fork与merge仍然可用(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	localActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	base := bytes.Repeat([]byte("BASE-DATA-"), 8192)
	writeRegularFile(t, ctx, localActor, mount.Epoch, "base.txt", base)
	require.NoError(t, local.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mount.MountID}))
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)

	pulled, err := cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	require.Zero(t, pulled.Pulled.PacksDownloaded, "拉取时一个数据包都不应下载")
	require.Positive(t, pulled.Pulled.PacksDeferred)

	// 在云端 fork、各自修改、再合并回来：涉及的数据包按需补齐，结果正确
	child, err := cloud.ForkDisk(ctx, ForkDiskParams{SourceDiskID: disk.DiskID, Name: "attempt"})
	require.NoError(t, err)
	childMount, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: child.DiskID})
	require.NoError(t, err)
	childActor, err := cloud.Actor(ctx, child.DiskID)
	require.NoError(t, err)
	require.Equal(t, base, readRegularFile(t, ctx, childActor, "base.txt", uint32(len(base)+64)), "fork 出来的盘读得到还没下载的数据")
	writeRegularFile(t, ctx, childActor, childMount.Epoch, "from-child.txt", []byte("child"))
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: child.DiskID, MountID: childMount.MountID}))

	_, err = cloud.MergeDisk(ctx, &MergeDiskParams{SourceDiskID: child.DiskID, TargetDiskID: disk.DiskID})
	require.NoError(t, err)
	merged, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("child"), readRegularFile(t, ctx, merged, "from-child.txt", 16))
	require.Equal(t, base, readRegularFile(t, ctx, merged, "base.txt", uint32(len(base)+64)))
}

func TestHandoff_KeepLocal_掉线期间的本地改动在云端接管后能合并回来(t *testing.T) {
	ctx := context.Background()
	remote := newSharedRemote(t)
	local := newHandoffNode(t, remote, "local")
	cloud := newHandoffNode(t, remote, "cloud")

	disk, err := local.CreateDisk(ctx, CreateDiskParams{Name: "workspace", MaxBytes: 64 << 20})
	require.NoError(t, err)
	mount, err := local.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	localActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, localActor, mount.Epoch, "shared.txt", []byte("base"))
	writeRegularFile(t, ctx, localActor, mount.Epoch, "both-edit.txt", []byte("base"))
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)

	// Given: 本地同步之后又写了东西，然后掉线；云端强行接管并在同一个基线上各干各的
	writeRegularFile(t, ctx, localActor, mount.Epoch, "only-local.txt", []byte("written offline on local"))
	overwriteRegularFile(t, ctx, localActor, mount.Epoch, "shared.txt", []byte("edited on local"))
	overwriteRegularFile(t, ctx, localActor, mount.Epoch, "both-edit.txt", []byte("local version"))

	_, err = cloud.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID, Force: true})
	require.NoError(t, err)
	cloudMount, err := cloud.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
	cloudActor, err := cloud.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	// 两边新建的文件很可能拿到相同的 inode 号与数据包编号：合并必须能处理
	writeRegularFile(t, ctx, cloudActor, cloudMount.Epoch, "only-cloud.txt", []byte("written on cloud"))
	overwriteRegularFile(t, ctx, cloudActor, cloudMount.Epoch, "both-edit.txt", []byte("cloud version"))
	require.NoError(t, cloud.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: cloudMount.MountID}))
	_, err = cloud.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID, Release: true})
	require.NoError(t, err)

	// When: 本地恢复，带 KeepLocal 拉取
	require.NoError(t, local.UnmountDisk(ctx, UnmountDiskParams{DiskID: disk.DiskID, MountID: mount.MountID}))
	_, err = local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrLocalDiverged, "不带 KeepLocal 时仍然拒绝覆盖")
	pulled, err := local.PullDisk(ctx, PullDiskParams{DiskID: disk.DiskID, KeepLocal: true})
	require.NoError(t, err)
	require.NotEmpty(t, pulled.KeptLocalDiskID)
	require.Equal(t, "local", pulled.Head.Owner)

	// Then: 原盘是云端的内容，本地改动完整地在另存的那块盘里
	mainActor, err := local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("written on cloud"), readRegularFile(t, ctx, mainActor, "only-cloud.txt", 64))
	_, _, lookupErr := mainActor.Lookup(ctx, RootIno, []byte("only-local.txt"))
	require.ErrorIs(t, lookupErr, ErrNotFound)
	keptActor, err := local.Actor(ctx, pulled.KeptLocalDiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("written offline on local"), readRegularFile(t, ctx, keptActor, "only-local.txt", 64))

	// 两边都改过的文件：默认策略下合并报冲突，不会悄悄选边
	_, err = local.MergeDisk(ctx, &MergeDiskParams{SourceDiskID: pulled.KeptLocalDiskID, TargetDiskID: disk.DiskID})
	require.Error(t, err, "both-edit.txt 两边都改了，应当报冲突")

	// When: 指定冲突时取本地那份（source）再合并
	_, err = local.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: pulled.KeptLocalDiskID, TargetDiskID: disk.DiskID, Strategy: store.MergeStrategyTheirs,
	})
	require.NoError(t, err)

	// Then: 原盘同时拥有两边各自新增的文件、只有一边改过的文件，以及按策略解决的冲突文件
	mainActor, err = local.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("written on cloud"), readRegularFile(t, ctx, mainActor, "only-cloud.txt", 64))
	require.Equal(t, []byte("written offline on local"), readRegularFile(t, ctx, mainActor, "only-local.txt", 64))
	require.Equal(t, []byte("edited on local"), readRegularFile(t, ctx, mainActor, "shared.txt", 64))
	require.Equal(t, []byte("local version"), readRegularFile(t, ctx, mainActor, "both-edit.txt", 64))

	// 合并结果能正常推回远端
	_, err = local.PushDisk(ctx, PushDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err)
}
