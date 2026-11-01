package meta_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestDiskQuotaInMemoryAndColdLoad_PostgresIntegration 验证 ADR-0024 决策 4：
// 1. Create、Mkdir、Write、SetAttr、Fallocate、Unlink、Rmdir 在事务内完全不写 disks 表，而在内存中实时预占与更新 UsedBytes / UsedInodes；
// 2. GetDisk、ListDisks、StatFS 实时读取内存中的精确配额；
// 3. 超出磁盘 MaxBytes 时在内存中立即精确拦截并返回 ErrNoSpace (ENOSPC)，失败回滚释放预占字节；
// 4. 冷启动/新建 Manager 实例后，首次访问（StatFS / GetDisk / ListDisks / 写操作）自动从 inodes 表覆盖索引 idx_inodes_root_ino_quota 动态聚合恢复 UsedBytes 与 UsedInodes（含未物理回收的 Orphan Inode）；
// 5. UpdateDisk 在排他锁下基于实时已用空间拦截非法缩容请求（返回 ErrInvalidShrink / ErrInvalidArgument），合法扩容/缩容后同步刷新内存 max_bytes。
func TestDiskQuotaInMemoryAndColdLoad_PostgresIntegration(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store := db.NewStore(pool)
	bsDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(bsDir)
	mgr := meta.New(
		store,
		meta.WithBlockStore(bs),
	)

	diskName := fmt.Sprintf("disk-inmem-quota-%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 16384, // 16 KiB
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_ = mgr.DeleteDisk(cleanCtx, disk.DiskID)
	})

	// 1. 4 并发协程各自在独立子目录下并发创建文件并写入数据（验证零 disks 行锁争用与纯内存配额累加）
	const workers = 4
	dirInos := make([]uint64, workers)
	for i := 0; i < workers; i++ {
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("dir_%d", i)),
			Mode:      meta.S_IFDIR | 0o755,
		})
		require.NoError(t, err)
		dirInos[i] = subDir.Ino
	}

	var wg sync.WaitGroup
	fileInos := make([]uint64, workers)
	fileFhs := make([]uint64, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cRes, cErr := mgr.Create(ctx, meta.CreateParams{
				ParentIno: dirInos[idx],
				Name:      []byte("file.bin"),
				Mode:      meta.S_IFREG | 0o644,
				Flags:     meta.O_RDWR,
			})
			require.NoError(t, cErr)
			fileInos[idx] = cRes.Inode.Ino
			fileFhs[idx] = cRes.Fh

			_, wErr := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    cRes.Inode.Ino,
				Fh:     cRes.Fh,
				Offset: 0,
				Data:   make([]byte, 2048), // 每个文件写入 2048 B，共 8192 B
			})
			require.NoError(t, wErr)
		}(i)
	}
	wg.Wait()

	// 2. 通过 GetDisk 与 StatFS 验证实时内存可见性（1 个根目录 + 4 个子目录 + 4 个常规文件 = 9 个 Inode，已用 8192 字节）
	gotDisk, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), gotDisk.UsedBytes)
	assert.Equal(t, uint64(9), gotDisk.UsedInodes)

	statRes, err := mgr.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), statRes.UsedBytes)
	assert.Equal(t, uint64(8192), statRes.AvailBytes)
	assert.Equal(t, uint64(9), statRes.Files)

	// 3. 验证内存超配额拦截：剩余可用 8192 B，尝试 Fallocate 到 12288 B（增量 +10240 B > 8192 B）必须返回 ErrNoSpace
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    fileInos[0],
		Fh:     fileFhs[0],
		Offset: 0,
		Length: 12288,
		Mode:   0,
	})
	require.ErrorIs(t, err, meta.ErrNoSpace)

	// 超配额失败后配额保持 8192 B 不变
	gotDiskAfterFail, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), gotDiskAfterFail.UsedBytes)

	// 4. 构造冷启动的新 Manager 实例，验证首次调用 StatFS / GetDisk / 写操作时从 inodes 表覆盖索引动态聚合恢复精确配额
	coldMgr := meta.New(
		store,
		meta.WithBlockStore(bs),
	)
	coldStat, err := coldMgr.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), coldStat.UsedBytes)
	assert.Equal(t, uint64(9), coldStat.Files)

	// 5. 验证 UpdateDisk 缩容水位拦截与合法调整
	tooSmallMax := uint64(8191)
	_, err = coldMgr.UpdateDisk(ctx, meta.UpdateDiskParams{
		DiskID:   disk.DiskID,
		MaxBytes: &tooSmallMax,
	})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)

	exactMax := uint64(10240)
	updatedDisk, err := coldMgr.UpdateDisk(ctx, meta.UpdateDiskParams{
		DiskID:   disk.DiskID,
		MaxBytes: &exactMax,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(10240), updatedDisk.MaxBytes)
	assert.Equal(t, uint64(8192), updatedDisk.UsedBytes)
	assert.Equal(t, uint64(9), updatedDisk.UsedInodes)

	// 6. 解链仍有打开句柄的 fileInos[0]（成为 Orphan Inode），验证 Orphan 期间配额仍计入，Release 后才物理回收退还
	require.NoError(t, coldMgr.Unlink(ctx, dirInos[0], []byte("file.bin")))
	orphanDisk, err := coldMgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), orphanDisk.UsedBytes, "未关闭句柄的 Orphan Inode 仍计入已用空间")
	assert.Equal(t, uint64(9), orphanDisk.UsedInodes, "未关闭句柄的 Orphan Inode 仍计入已用节点数")

	// 重建第三个冷实例，验证数据库覆盖索引聚合同样包含 Orphan Inode
	coldMgr2 := meta.New(store, meta.WithBlockStore(bs))
	coldDiskWithOrphan, err := coldMgr2.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), coldDiskWithOrphan.UsedBytes)
	assert.Equal(t, uint64(9), coldDiskWithOrphan.UsedInodes)

	// Release 最后一个句柄触发 Orphan 物理回收，内存配额实时扣减
	require.NoError(t, coldMgr2.Release(ctx, meta.ReleaseParams{Ino: fileInos[0], Fh: fileFhs[0]}))
	gotDiskAfterRelease, err := coldMgr2.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(6144), gotDiskAfterRelease.UsedBytes)
	assert.Equal(t, uint64(8), gotDiskAfterRelease.UsedInodes)
}
