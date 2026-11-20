package control

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func fileHardLinkCount(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err, "获取文件 Stat 失败: path=%s", path)
	v := reflect.ValueOf(info.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	field := v.FieldByName("Nlink")
	require.True(t, field.IsValid(), "Stat_t 必须包含 Nlink 字段")
	return field.Uint()
}

func writeRegularFile(t *testing.T, ctx context.Context, actor *store.StoreActor, epoch uint64, name string, data []byte) uint64 {
	t.Helper()
	attr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err, "创建测试文件失败: name=%s", name)
	if len(data) > 0 {
		_, _, err = actor.Write(ctx, attr.Ino, 0, data, epoch)
		require.NoError(t, err, "写入测试文件失败: name=%s", name)
	}
	return attr.Ino
}

func overwriteRegularFile(t *testing.T, ctx context.Context, actor *store.StoreActor, epoch uint64, name string, data []byte) {
	t.Helper()
	_, attr, err := actor.Lookup(ctx, RootIno, []byte(name))
	require.NoError(t, err, "查找待覆写文件失败: name=%s", name)
	_, _, err = actor.Write(ctx, attr.Ino, 0, data, epoch)
	require.NoError(t, err, "覆写文件数据失败: name=%s", name)
	_, err = actor.SetAttr(ctx, attr.Ino, store.SetAttrReq{
		Size:  new(uint64(len(data))),
		Epoch: epoch,
	}, epoch)
	require.NoError(t, err, "截断更新文件大小失败: name=%s", name)
}

func readRegularFile(t *testing.T, ctx context.Context, actor *store.StoreActor, name string, size uint32) []byte {
	t.Helper()
	_, attr, err := actor.Lookup(ctx, RootIno, []byte(name))
	require.NoError(t, err, "查找待读取文件失败: name=%s", name)
	data, err := actor.Read(ctx, attr.Ino, 0, size)
	require.NoError(t, err, "读取测试文件失败: name=%s", name)
	return data
}

func singlePackFilePath(t *testing.T, packsDir string) string {
	t.Helper()
	entries, err := os.ReadDir(packsDir)
	require.NoError(t, err, "读取 packs 目录失败: dir=%s", packsDir)
	require.Len(t, entries, 1, "packs 目录预期仅有 1 个封包文件: dir=%s got=%d", packsDir, len(entries))
	return filepath.Join(packsDir, entries[0].Name())
}

func createBatchFilesInBranch(actor *store.StoreActor, ctx context.Context, epoch uint64, prefix string, count int, blk []byte) ([]uint64, error) {
	inos := make([]uint64, count)
	for i := range count {
		attr, err := actor.Create(ctx, store.CreateReq{
			ParentIno: RootIno,
			Name:      []byte(fmt.Sprintf("%s-%d.bin", prefix, i)),
			Mode:      store.S_IFREG | 0o644,
			Epoch:     epoch,
		})
		if err != nil {
			return nil, err
		}
		if _, _, err = actor.Write(ctx, attr.Ino, 0, blk, epoch); err != nil {
			return nil, err
		}
		inos[i] = attr.Ino
	}
	return inos, nil
}

func assertDisjointInodes(t *testing.T, parentInos, childInos []uint64) map[uint64]struct{} {
	t.Helper()
	seen := make(map[uint64]struct{}, len(parentInos)+len(childInos))
	for _, ino := range parentInos {
		_, dup := seen[ino]
		require.False(t, dup, "父盘分配到重复 Inode=%d", ino)
		seen[ino] = struct{}{}
	}
	for _, ino := range childInos {
		_, dup := seen[ino]
		require.False(t, dup, "子盘与父盘或自身发生 Inode 碰撞: ino=%d", ino)
		seen[ino] = struct{}{}
	}
	return seen
}

func assertDisjointPackFiles(t *testing.T, parentPacksDir, childPacksDir string) {
	t.Helper()
	pPacks, err := os.ReadDir(parentPacksDir)
	require.NoError(t, err, "读取父盘 packs 目录失败")
	cPacks, err := os.ReadDir(childPacksDir)
	require.NoError(t, err, "读取子盘 packs 目录失败")
	seen := make(map[string]struct{}, len(pPacks))
	for _, e := range pPacks {
		seen[e.Name()] = struct{}{}
	}
	for _, e := range cPacks {
		_, exists := seen[e.Name()]
		require.False(t, exists, "同源父子分支新生成的封包名碰撞: %s", e.Name())
	}
}

func gatherForkStageCount(t *testing.T, reg *prometheus.Registry, stage string) uint64 {
	t.Helper()
	return gatherMetricSampleCount(t, reg, "bowl_storage_control_fork_stage_duration_seconds", "stage", stage)
}

func gatherForkCounter(t *testing.T, reg *prometheus.Registry, result string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	for _, fam := range fams {
		if fam.GetName() != "bowl_storage_control_forks_total" {
			continue
		}
		for _, m := range fam.GetMetric() {
			if hasMetricLabel(m, "result", result) && m.GetCounter() != nil {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

type threadSafeLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *threadSafeLogBuf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *threadSafeLogBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestForkDisk_OnlineFork_IsolatesParentAndChildWrites(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	parentDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "root-disk", MaxBytes: 64 << 20})
	require.NoError(t, err, "创建源盘失败")
	parentMount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: parentDisk.DiskID})
	require.NoError(t, err, "挂载源盘失败")
	parentActor, err := mgr.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err, "获取源盘 StoreActor 失败")

	basePayload := bytes.Repeat([]byte("BASE-ROOT-CONTENT-"), 512)
	writeRegularFile(t, ctx, parentActor, parentMount.Epoch, "shared.txt", basePayload)

	childDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: parentDisk.DiskID,
		Name:         "child-1",
	})
	require.NoError(t, err, "在线 ForkDisk 失败")

	require.Equal(t, parentDisk.DiskID, childDisk.ParentDiskID, "子盘 ParentDiskID 应指向源盘")
	require.Equal(t, parentDisk.MaxBytes, childDisk.MaxBytes, "省略 MaxBytes 时应继承源盘配额")
	require.Equal(t, uint64(len(basePayload)), childDisk.UsedBytes, "子盘初始 UsedBytes 应等于基准数据大小")
	require.Equal(t, uint64(2), childDisk.UsedInodes, "子盘初始 UsedInodes 应为 2")
	require.Equal(t, uint64(1), parentActor.WriterEpoch(), "Fork 不得扰动源盘活跃 WriterEpoch")

	parentUpdated := []byte("PARENT-MUTATED-AFTER-FORK")
	overwriteRegularFile(t, ctx, parentActor, parentMount.Epoch, "shared.txt", parentUpdated)
	writeRegularFile(t, ctx, parentActor, parentMount.Epoch, "only-in-parent.txt", []byte("parent-only"))

	childActor, err := mgr.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err, "获取子盘 StoreActor 失败")
	require.Zero(t, childActor.WriterEpoch(), "子盘初始 WriterEpoch 必须归零")

	childMount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: childDisk.DiskID})
	require.NoError(t, err, "挂载子盘失败")
	require.Equal(t, uint64(1), childMount.Epoch, "子盘首次可写挂载 epoch 应为 1")

	require.Equal(t, basePayload, readRegularFile(t, ctx, childActor, "shared.txt", uint32(len(basePayload)+64)), "子盘必须读取到 Fork 瞬间快照")
	_, _, lookupErr := childActor.Lookup(ctx, RootIno, []byte("only-in-parent.txt"))
	require.ErrorIs(t, lookupErr, ErrNotFound, "父盘 Fork 后新建文件不得泄露到子盘")

	childMutated := bytes.Repeat([]byte("CHILD1-BRANCH-DATA-"), 256)
	overwriteRegularFile(t, ctx, childActor, childMount.Epoch, "shared.txt", childMutated)
	require.Equal(t, parentUpdated, readRegularFile(t, ctx, parentActor, "shared.txt", 128), "子盘写入不得影响父盘内容")
	require.Equal(t, childMutated, readRegularFile(t, ctx, childActor, "shared.txt", uint32(len(childMutated)+64)), "子盘覆写内容应独立生效")
}

func TestForkDisk_CascadingDAG_MaintainsLineageAndParentLinks(t *testing.T) {

	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "root", MaxBytes: 16 << 20})
	require.NoError(t, err, "创建根盘失败")
	child1, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child-1"})
	require.NoError(t, err, "派生 child-1 失败")
	grandChild, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: child1.DiskID, Name: "grand-child"})
	require.NoError(t, err, "级联派生 grand-child 失败")
	child2, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "child-2"})
	require.NoError(t, err, "二次派生 child-2 失败")

	listed, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 10})
	require.NoError(t, err, "列出磁盘失败")
	gotGrand, err := mgr.GetDisk(ctx, grandChild.DiskID)
	require.NoError(t, err, "查询 grand-child 失败")

	require.Len(t, listed.Disks, 4, "应列出 4 块磁盘: got=%d", len(listed.Disks))
	require.Equal(t, child1.DiskID, gotGrand.ParentDiskID, "GrandChild 父盘应为 Child1")
	require.Equal(t, rootDisk.DiskID, child2.ParentDiskID, "Child2 父盘应为 Root")

	idRoot, err := parseDiskID(rootDisk.DiskID)
	require.NoError(t, err)
	idChild1, err := parseDiskID(child1.DiskID)
	require.NoError(t, err)
	idChild2, err := parseDiskID(child2.DiskID)
	require.NoError(t, err)
	idGrand, err := parseDiskID(grandChild.DiskID)
	require.NoError(t, err)

	snap := memDB.Inspect()
	require.Len(t, snap.Lineages, 1, "同源级联派生应共享 1 条血缘")
	require.Len(t, snap.Commits, 3, "3 次 Fork 应产生 3 个基准切面")
	c2 := snap.Commits[snap.Disks[idRoot].BaseCommitID]
	c3 := snap.Commits[snap.Disks[idChild1].BaseCommitID]
	c1 := snap.Commits[c2.ParentCommitID]

	require.Equal(t, idRoot, snap.Disks[idGrand].LineageID, "GrandChild 应归属根盘血缘")
	require.Equal(t, uuid.Nil, c1.ParentCommitID, "首次切面父切面应为空")
	require.Equal(t, c1.CommitID, c2.ParentCommitID, "Root 二次切面应指向首次切面 C1")
	require.Equal(t, c1.CommitID, c3.ParentCommitID, "Child1 级联切面应指向首次切面 C1")
	require.Equal(t, c2.CommitID, snap.Disks[idChild2].BaseCommitID, "Child2 基准切面应为 C2")
	require.Equal(t, c3.CommitID, snap.Disks[idGrand].BaseCommitID, "GrandChild 基准切面应为 C3")
}

func TestForkDisk_QuotaShrinkWithoutOrphans_RejectsBeforeCreatingCommitDir(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	srcDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "quota-src", MaxBytes: 16 << 20})
	require.NoError(t, err, "创建源盘失败")
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: srcDisk.DiskID})
	require.NoError(t, err, "挂载源盘失败")
	srcActor, err := mgr.Actor(ctx, srcDisk.DiskID)
	require.NoError(t, err, "获取源盘 StoreActor 失败")
	writeRegularFile(t, ctx, srcActor, mnt.Epoch, "keep.bin", bytes.Repeat([]byte{0xAA}, 4096))

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "too-small-fast",
		MaxBytes:     new(uint64(2048)),
	})

	require.ErrorIs(t, err, ErrInvalidShrink, "配额缩容应返回 ErrInvalidShrink: want=%v got=%v", ErrInvalidShrink, err)
	_, statErr := os.Stat(filepath.Join(rootDir, "commits"))
	require.ErrorIs(t, statErr, os.ErrNotExist, "无孤儿前置快检拦截不得触发任何切面目录 I/O")
}

func TestForkDisk_QuotaShrinkWithOrphans_SanitizesAndEnforcesEffectiveUsage(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	srcDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "orphan-src", MaxBytes: 16 << 20})
	require.NoError(t, err, "创建源盘失败")
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: srcDisk.DiskID})
	require.NoError(t, err, "挂载源盘失败")
	srcActor, err := mgr.Actor(ctx, srcDisk.DiskID)
	require.NoError(t, err, "获取源盘 StoreActor 失败")

	writeRegularFile(t, ctx, srcActor, mnt.Epoch, "keep.bin", bytes.Repeat([]byte{0xAA}, 4096))
	orphanPayload := bytes.Repeat([]byte{0xBB}, store.BlobSize)
	orphanIno := writeRegularFile(t, ctx, srcActor, mnt.Epoch, "temp-orphan.bin", orphanPayload)
	require.NoError(t, srcActor.Unlink(ctx, RootIno, []byte("temp-orphan.bin"), true, mnt.Epoch), "解链构造孤儿失败")

	_, shrinkErr := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "too-small-after-sanitize",
		MaxBytes:     new(uint64(2048)),
	})

	require.ErrorIs(t, shrinkErr, ErrInvalidShrink, "净化后配额仍不足应返回 ErrInvalidShrink")
	commitEntries, readErr := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, readErr, "读取 commits 目录失败")
	require.Empty(t, commitEntries, "配额拦截回滚后 commits/ 目录必须零残留")

	forked, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "sanitized-child",
		MaxBytes:     new(uint64(8192)),
	})

	require.NoError(t, err, "净化孤儿后配额满足时应派生成功")
	require.Equal(t, uint64(8192), forked.MaxBytes)
	require.Equal(t, uint64(4096), forked.UsedBytes)
	require.Equal(t, uint64(2), forked.UsedInodes)

	orphanData, err := srcActor.Read(ctx, orphanIno, 0, store.BlobSize)
	require.NoError(t, err, "源盘孤儿节点应仍可读取")
	require.Equal(t, orphanPayload, orphanData, "源盘孤儿数据不得因 Fork 净化而受损")
}

func TestForkDisk_InvalidParams_RejectsWithSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "valid-src", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建源盘失败")

	cases := []struct {
		name    string
		params  ForkDiskParams
		wantErr error
	}{
		{name: "格式非法源盘 UUID", params: ForkDiskParams{SourceDiskID: "bad-uuid", Name: "x"}, wantErr: ErrInvalidArgument},
		{name: "全零 Nil 源盘 UUID", params: ForkDiskParams{SourceDiskID: "00000000-0000-0000-0000-000000000000", Name: "x"}, wantErr: ErrInvalidArgument},
		{name: "名称超过 255 字节", params: ForkDiskParams{SourceDiskID: src.DiskID, Name: strings.Repeat("a", 256)}, wantErr: ErrNameTooLong},
		{name: "名称包含 NUL 字节", params: ForkDiskParams{SourceDiskID: src.DiskID, Name: "bad\x00name"}, wantErr: ErrInvalidArgument},
		{name: "配额为零", params: ForkDiskParams{SourceDiskID: src.DiskID, Name: "zero", MaxBytes: new(uint64(0))}, wantErr: ErrInvalidArgument},
		{name: "配额超过 MaxInt64", params: ForkDiskParams{SourceDiskID: src.DiskID, Name: "overflow", MaxBytes: new(uint64(math.MaxInt64) + 1)}, wantErr: ErrInvalidArgument},
		{name: "源盘不存在", params: ForkDiskParams{SourceDiskID: "018f0000-0000-7000-8000-000000009999", Name: "missing"}, wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, fErr := mgr.ForkDisk(ctx, tc.params)
			require.ErrorIs(t, fErr, tc.wantErr, "ForkDisk(%s) 错误不匹配: want=%v got=%v", tc.name, tc.wantErr, fErr)
		})
	}
}

func TestForkDisk_DBTxFailure_RollsBackChildDirCommitDirAndRegistry(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "rollback-src", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建源盘失败")

	injectedErr := errors.New("simulated database failure")
	memDB.SetFailNextWrite(injectedErr)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "tx-fail-child"})

	require.ErrorIs(t, err, injectedErr, "事务失败应向上返回注入错误: want=%v got=%v", injectedErr, err)

	diskDirEntries, readErr := os.ReadDir(filepath.Join(rootDir, "disks"))
	require.NoError(t, readErr, "读取 disks 目录失败")
	require.Len(t, diskDirEntries, 1, "事务回滚后 disks/ 下只能保留源盘目录: got=%d", len(diskDirEntries))
	require.Equal(t, src.DiskID, diskDirEntries[0].Name())

	commitDirEntries, readErr := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, readErr, "读取 commits 目录失败")
	require.Empty(t, commitDirEntries, "事务回滚后 commits/ 下不得残留切面目录: got=%d", len(commitDirEntries))

	listed, listErr := mgr.ListDisks(ctx, ListDisksParams{})
	require.NoError(t, listErr, "ListDisks 失败")
	require.Len(t, listed.Disks, 1, "回滚后应仅存源盘 1 块: got=%d", len(listed.Disks))
}

func TestDeleteDisk_LineageCommitGC_ReclaimsCommitsAtSingleSurvivor(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "lineage-root", MaxBytes: 64 << 20})
	require.NoError(t, err, "创建根盘失败")
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: rootDisk.DiskID})
	require.NoError(t, err, "挂载根盘失败")
	rootActor, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err, "获取根盘 StoreActor 失败")

	packPayload := bytes.Repeat([]byte{0x5C}, store.BlobSize)
	writeRegularFile(t, ctx, rootActor, mnt.Epoch, "shared-pack.bin", packPayload)

	child1, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "sibling-1"})
	require.NoError(t, err, "派生 sibling-1 失败")
	sharedPackPath := singlePackFilePath(t, filepath.Join(rootDir, "disks", child1.DiskID, "packs"))
	require.Equal(t, uint64(3), fileHardLinkCount(t, sharedPackPath), "首次 Fork 后硬链接计数应为 3")

	child2, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: rootDisk.DiskID, Name: "sibling-2"})
	require.NoError(t, err, "派生 sibling-2 失败")
	require.Equal(t, uint64(5), fileHardLinkCount(t, sharedPackPath), "二次 Fork 后硬链接计数应为 5")

	require.NoError(t, mgr.UnmountDisk(ctx, UnmountDiskParams{DiskID: rootDisk.DiskID, MountID: mnt.MountID}), "卸载根盘失败")
	require.NoError(t, mgr.DeleteDisk(ctx, rootDisk.DiskID), "删除根盘失败")

	snapAfterRoot := memDB.Inspect()
	require.Len(t, snapAfterRoot.Commits, 2, "|D|==2 时必须保留全部切面 DAG")
	commitDirsAfterRoot, err := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err, "读取 commits 目录失败")
	require.Len(t, commitDirsAfterRoot, 2)
	require.Equal(t, uint64(4), fileHardLinkCount(t, sharedPackPath))

	require.NoError(t, mgr.DeleteDisk(ctx, child2.DiskID), "删除 sibling-2 失败")

	idChild1, err := parseDiskID(child1.DiskID)
	require.NoError(t, err)
	snapAfterSibling := memDB.Inspect()
	require.Empty(t, snapAfterSibling.Commits, "|D|==1 时必须自动清空同血缘全部切面")
	require.Len(t, snapAfterSibling.Lineages, 1, "唯一幸存盘仍在时血缘行必须保留")
	require.Equal(t, uuid.Nil, snapAfterSibling.Disks[idChild1].BaseCommitID, "唯一幸存盘 base_commit_id 必须置空")

	commitDirsAfterSibling, err := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err, "读取 commits 目录失败")
	require.Empty(t, commitDirsAfterSibling, "|D|==1 时 commits/ 目录下切面必须全部清理")
	require.Equal(t, uint64(1), fileHardLinkCount(t, sharedPackPath), "唯一幸存盘 .pack st_nlink 必须回落至 1")

	child1Actor, err := mgr.Actor(ctx, child1.DiskID)
	require.NoError(t, err, "获取幸存盘 StoreActor 失败")
	require.Equal(t, packPayload, readRegularFile(t, ctx, child1Actor, "shared-pack.bin", store.BlobSize), "幸存盘数据必须完好可读")

	require.NoError(t, mgr.DeleteDisk(ctx, child1.DiskID), "删除最后幸存盘失败")

	snapAfterLast := memDB.Inspect()
	require.Empty(t, snapAfterLast.Disks)
	require.Empty(t, snapAfterLast.Commits)
	require.Empty(t, snapAfterLast.Lineages, "|D|==0 时必须清除血缘记录")
}

func TestForkDisk_ConcurrentSegmentAllocation_CollisionFreeAcrossRestart(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := newManager(memDB, WithStorageRoot(rootDir), withMaxPackSize(store.BlobSize))

	parentDisk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "seg-parent", MaxBytes: 512 << 20})
	require.NoError(t, err, "创建父盘失败")
	childDisk, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: parentDisk.DiskID, Name: "seg-child"})
	require.NoError(t, err, "派生子盘失败")

	pMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: parentDisk.DiskID})
	require.NoError(t, err, "挂载父盘失败")
	cMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: childDisk.DiskID})
	require.NoError(t, err, "挂载子盘失败")
	pActor, err := mgr1.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err, "获取父盘 StoreActor 失败")
	cActor, err := mgr1.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err, "获取子盘 StoreActor 失败")

	const filesPerBranch = 70
	blk := bytes.Repeat([]byte{0x33}, store.BlobSize)

	var (
		pInos, cInos []uint64
		pErr, cErr   error
		wg           sync.WaitGroup
	)
	wg.Go(func() { pInos, pErr = createBatchFilesInBranch(pActor, ctx, pMnt.Epoch, "p", filesPerBranch, blk) })
	wg.Go(func() { cInos, cErr = createBatchFilesInBranch(cActor, ctx, cMnt.Epoch, "c", filesPerBranch, blk) })
	wg.Wait()

	require.NoError(t, pErr, "父盘并发批量写入失败")
	require.NoError(t, cErr, "子盘并发批量写入失败")
	allInos := assertDisjointInodes(t, pInos, cInos)
	require.Len(t, allInos, filesPerBranch*2, "总分配唯一 Inode 数不匹配: want=%d got=%d", filesPerBranch*2, len(allInos))

	require.NoError(t, mgr1.Close(), "关闭首任管理器失败")
	mgr2 := newManager(memDB, WithStorageRoot(rootDir), withMaxPackSize(store.BlobSize))
	t.Cleanup(func() { _ = mgr2.Close() })

	pActor2, err := mgr2.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err, "重启后获取父盘 StoreActor 失败")
	cActor2, err := mgr2.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err, "重启后获取子盘 StoreActor 失败")

	pInoRestart := writeRegularFile(t, ctx, pActor2, pMnt.Epoch, "after-restart-p.bin", blk)
	cInoRestart := writeRegularFile(t, ctx, cActor2, cMnt.Epoch, "after-restart-c.bin", blk)

	require.NotContains(t, allInos, pInoRestart, "重启后父盘新 Inode 不得与历史冲突")
	require.NotContains(t, allInos, cInoRestart, "重启后子盘新 Inode 不得与历史冲突")
	require.NotEqual(t, pInoRestart, cInoRestart, "重启后父子新 Inode 不得相互冲突")

	assertDisjointPackFiles(t,
		filepath.Join(rootDir, "disks", parentDisk.DiskID, "packs"),
		filepath.Join(rootDir, "disks", childDisk.DiskID, "packs"),
	)

	snap := memDB.Inspect()
	require.GreaterOrEqual(t, snap.InodeReserveCalls, 4, "初始与重启懒加载应调用外部 Inode 号段分配器")
	require.GreaterOrEqual(t, snap.PackReserveCalls, 6, "跨越 PackSegment=64 与重启后应多次调用 Pack 号段分配器")
}

func TestForkDisk_Observability_ExportsDedupMetricsSpansAndAuditLogs(t *testing.T) {

	tp, exporter := setupTestTracer(t)
	var logBuf threadSafeLogBuf
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	rootDir := t.TempDir()

	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir), WithLogger(logger))
	t.Cleanup(func() { _ = mgr.Close() })
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()), "注册 Collector 失败")

	ctx, rootSpan := tp.Tracer("test-fork-ctrl").Start(context.Background(), "rpc.ForkLifecycle")
	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "dedup-parent", MaxBytes: 64 << 20})
	require.NoError(t, err, "创建源盘失败")
	srcActor, err := mgr.Actor(ctx, src.DiskID)
	require.NoError(t, err, "获取源盘 StoreActor 失败")

	const payloadSize = 1 << 20
	writeRegularFile(t, ctx, srcActor, 0, "payload-1m.bin", bytes.Repeat([]byte{0x5A}, payloadSize))
	writeRegularFile(t, ctx, srcActor, 0, "orphan.bin", bytes.Repeat([]byte{0x7E}, 8192))
	require.NoError(t, srcActor.Unlink(ctx, RootIno, []byte("orphan.bin"), true), "解链构造孤儿失败")

	child1, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "child-a"})
	require.NoError(t, err, "Fork child-a 失败")
	child2, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "child-b"})
	require.NoError(t, err, "Fork child-b 失败")

	newName := "dedup-parent-renamed"
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: src.DiskID, Name: &newName})
	require.NoError(t, err, "UpdateDisk 重命名失败")

	physBytes := gatherGaugeValue(t, reg, "bowl_storage_pack_physical_bytes")
	refBytes := gatherGaugeValue(t, reg, "bowl_storage_pack_referenced_bytes")
	require.InDelta(t, float64(payloadSize), physBytes, 65536, "Fork 后物理占用应去重维持约 1 MiB: got=%v", physBytes)
	require.GreaterOrEqual(t, refBytes, float64(3*payloadSize), "逻辑引用占用应累加 3 份盘: got=%v", refBytes)
	require.InDelta(t, 2.0, gatherGaugeValue(t, reg, "bowl_storage_commits_active"), 1e-9, "应存在 2 个活跃切面")
	require.GreaterOrEqual(t, gatherMetricSampleCount(t, reg, "bowl_storage_control_open_actor_duration_seconds", "result", "ok"), uint64(5))
	require.Equal(t, uint64(2), gatherForkStageCount(t, reg, "checkpoint"))
	require.Equal(t, uint64(2), gatherForkStageCount(t, reg, "sanitize"))
	require.Equal(t, uint64(2), gatherForkStageCount(t, reg, "clone_dir"))
	require.Equal(t, uint64(2), gatherForkStageCount(t, reg, "open_child"))
	require.Equal(t, uint64(2), gatherForkStageCount(t, reg, "commit_tx"))
	require.InDelta(t, 2.0, gatherForkCounter(t, reg, "ok"), 1e-9)

	require.NoError(t, mgr.DeleteDisk(ctx, src.DiskID))
	require.NoError(t, mgr.DeleteDisk(ctx, child1.DiskID))
	require.NoError(t, mgr.DeleteDisk(ctx, child2.DiskID))
	rootSpan.End()

	require.InDelta(t, 0.0, gatherGaugeValue(t, reg, "bowl_storage_commits_active"), 1e-9, "全删后 commits_active 应归零")
	require.InDelta(t, 2.0, gatherCounterValue(t, reg, "bowl_storage_control_commits_reclaimed_total"), 1e-9, "应累计回收 2 个切面")

	spans := exporter.GetSpans()
	require.GreaterOrEqual(t, countSpansByName(spans, "control.ReserveSegment"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "control.OpenDiskActor"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "control.ForkCommitTx"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "control.ReclaimCommits"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "store.Checkpoint"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "store.SanitizeCheckpoint"), 1)
	require.GreaterOrEqual(t, countSpansByName(spans, "store.CloneCheckpointDir"), 1)
	require.Contains(t, logBuf.String(), `"msg":"disk updated"`, "应记录 UpdateDisk 结构化审计日志")
}
