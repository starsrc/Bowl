package control

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func gatherCounterValue(t *testing.T, reg *prometheus.Registry, metricName string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	for _, fam := range fams {
		if fam.GetName() == metricName && len(fam.GetMetric()) > 0 {
			return fam.GetMetric()[0].GetCounter().GetValue()
		}
	}
	return 0
}

func gatherGaugeValue(t *testing.T, reg *prometheus.Registry, metricName string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	for _, fam := range fams {
		if fam.GetName() == metricName && len(fam.GetMetric()) > 0 {
			return fam.GetMetric()[0].GetGauge().GetValue()
		}
	}
	return 0
}

func collectDiskIDSet(disks []*Disk) map[string]struct{} {
	out := make(map[string]struct{}, len(disks))
	for _, d := range disks {
		out[d.DiskID] = struct{}{}
	}
	return out
}

func gatherMetricLabelValue(t *testing.T, reg *prometheus.Registry, metricName, labelKey, labelVal string) float64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	for _, fam := range fams {
		if fam.GetName() != metricName {
			continue
		}
		for _, m := range fam.GetMetric() {
			if hasMetricLabel(m, labelKey, labelVal) && m.GetGauge() != nil {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

var _ = (*dto.MetricFamily)(nil)

func TestCreateDisk_ValidParams_PersistsQuotaAndReadsBackConsistentState(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	created, err := mgr.CreateDisk(ctx, CreateDiskParams{
		Name:     "alpha-disk",
		MaxBytes: 16 << 20,
	})

	require.NoError(t, err, "创建磁盘失败")
	require.NotEmpty(t, created.DiskID, "创建磁盘必须分配非空 DiskID")
	require.Empty(t, created.ParentDiskID, "根磁盘 ParentDiskID 必须为空")
	require.Equal(t, "alpha-disk", created.Name, "磁盘名称不匹配: want=alpha-disk got=%q", created.Name)
	require.Equal(t, RootIno, created.RootIno, "根 Inode 不匹配: want=%d got=%d", RootIno, created.RootIno)
	require.Equal(t, uint64(16<<20), created.MaxBytes, "MaxBytes 不匹配: want=%d got=%d", uint64(16<<20), created.MaxBytes)
	require.Equal(t, uint64(0), created.UsedBytes, "初始 UsedBytes 应为 0: got=%d", created.UsedBytes)
	require.Equal(t, uint64(1), created.UsedInodes, "初始 UsedInodes 应为 1: got=%d", created.UsedInodes)

	got, err := mgr.GetDisk(ctx, created.DiskID)
	require.NoError(t, err, "GetDisk 读回失败")
	require.Equal(t, created.DiskID, got.DiskID, "GetDisk 返回 DiskID 不匹配")
	require.Equal(t, "alpha-disk", got.Name, "GetDisk 返回名称不匹配")
	require.Equal(t, uint64(16<<20), got.MaxBytes, "GetDisk 返回 MaxBytes 不匹配")
}

func TestCreateDisk_InvalidParams_RejectsWithSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		params  CreateDiskParams
		wantErr error
	}{
		{name: "配额为零", params: CreateDiskParams{Name: "zero", MaxBytes: 0}, wantErr: ErrInvalidArgument},
		{name: "配额超过 MaxInt64", params: CreateDiskParams{Name: "overflow", MaxBytes: uint64(math.MaxInt64) + 1}, wantErr: ErrInvalidArgument},
		{name: "名称超过 255 字节", params: CreateDiskParams{Name: strings.Repeat("n", 256), MaxBytes: 4096}, wantErr: ErrNameTooLong},
		{name: "名称包含 NUL 字节", params: CreateDiskParams{Name: "bad\x00disk", MaxBytes: 4096}, wantErr: ErrInvalidArgument},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.CreateDisk(ctx, tc.params)
			require.ErrorIs(t, err, tc.wantErr, "CreateDisk(%s) 错误不匹配: want=%v got=%v", tc.name, tc.wantErr, err)
		})
	}
}

func TestCreateDisk_DBWriteFailure_RollsBackActorAndDirectory(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	injectedErr := errors.New("simulated db create failure")
	memDB.SetFailNextWrite(injectedErr)

	_, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "rollback-disk", MaxBytes: 8 << 20})

	require.ErrorIs(t, err, injectedErr, "CreateDisk 应返回注入的数据库错误: want=%v got=%v", injectedErr, err)

	entries, readErr := os.ReadDir(filepath.Join(rootDir, "disks"))
	require.NoError(t, readErr, "读取 disks 目录失败")
	require.Empty(t, entries, "数据库创建失败回滚后 disks/ 不得残留子目录: got=%d", len(entries))

	listed, listErr := mgr.ListDisks(ctx, ListDisksParams{})
	require.NoError(t, listErr, "ListDisks 查询失败")
	require.Empty(t, listed.Disks, "回滚后 ListDisks 应为空: got=%d", len(listed.Disks))
}

func TestGetDisk_InvalidOrMissingID_ReturnsExpectedSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		diskID  string
		wantErr error
	}{
		{name: "格式非法 UUID", diskID: "bad-uuid", wantErr: ErrInvalidArgument},
		{name: "全零 Nil UUID", diskID: "00000000-0000-0000-0000-000000000000", wantErr: ErrInvalidArgument},
		{name: "不存在的磁盘", diskID: "018f0000-0000-7000-8000-000000009999", wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.GetDisk(ctx, tc.diskID)
			require.ErrorIs(t, err, tc.wantErr, "GetDisk(%q) 错误不匹配: want=%v got=%v", tc.diskID, tc.wantErr, err)
		})
	}
}

func TestGetDisk_AfterRestart_LazyLoadsPersistedUsageAndQuota(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := New(memDB, WithStorageRoot(rootDir))

	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "persist-disk", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建磁盘失败")
	mnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "挂载磁盘失败")
	actor, err := mgr1.Actor(ctx, disk.DiskID)
	require.NoError(t, err, "获取 StoreActor 失败")
	fAttr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("file.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mnt.Epoch,
	})
	require.NoError(t, err, "创建测试文件失败")
	_, _, err = actor.Write(ctx, fAttr.Ino, 0, make([]byte, 4096), mnt.Epoch)
	require.NoError(t, err, "写入测试数据失败")
	require.NoError(t, mgr1.Close(), "关闭首任管理器失败")

	mgr2 := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr2.Close() })
	got, err := mgr2.GetDisk(ctx, disk.DiskID)

	require.NoError(t, err, "重启后 GetDisk 失败")
	require.Equal(t, uint64(8<<20), got.MaxBytes, "MaxBytes 不匹配: want=%d got=%d", uint64(8<<20), got.MaxBytes)
	require.Equal(t, uint64(4096), got.UsedBytes, "UsedBytes 不匹配: want=4096 got=%d", got.UsedBytes)
	require.Equal(t, uint64(2), got.UsedInodes, "UsedInodes 不匹配: want=2 got=%d", got.UsedInodes)
}

func TestListDisks_CursorPagination_ReturnsAllPagesWithoutOverlap(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	d1, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "disk-1", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建 disk-1 失败")
	d2, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "disk-2", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建 disk-2 失败")
	d3, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "disk-3", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建 disk-3 失败")

	page1, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 2})
	require.NoError(t, err, "拉取第 1 页失败")
	require.Len(t, page1.Disks, 2, "第 1 页条数不匹配: want=2 got=%d", len(page1.Disks))
	require.NotEmpty(t, page1.NextPageToken, "第 1 页应返回非空 NextPageToken")

	page2, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 2, PageToken: page1.NextPageToken})
	require.NoError(t, err, "拉取第 2 页失败")

	require.Len(t, page2.Disks, 1, "第 2 页条数不匹配: want=1 got=%d", len(page2.Disks))
	require.Empty(t, page2.NextPageToken, "最后一页 NextPageToken 必须为空: got=%q", page2.NextPageToken)

	combined := collectDiskIDSet(append(page1.Disks, page2.Disks...))
	require.Len(t, combined, 3, "分页汇总唯一盘数不匹配: want=3 got=%d", len(combined))
	require.Contains(t, combined, d1.DiskID)
	require.Contains(t, combined, d2.DiskID)
	require.Contains(t, combined, d3.DiskID)
}

func TestListDisks_ZeroPageSize_UsesDefaultPageSize(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	created, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "default-page", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建磁盘失败")

	res, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 0})

	require.NoError(t, err, "PageSize=0 查询失败")
	require.Len(t, res.Disks, 1, "返回条数不匹配: want=1 got=%d", len(res.Disks))
	require.Equal(t, created.DiskID, res.Disks[0].DiskID)
}

func TestListDisks_PageSizeExceedsMax_ReturnsInvalidArgument(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	_, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 1001})

	require.ErrorIs(t, err, ErrInvalidArgument, "超限 PageSize 错误不匹配: want=%v got=%v", ErrInvalidArgument, err)
}

func TestUpdateDisk_NameAndQuota_PersistsAcrossRestart(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := New(memDB, WithStorageRoot(rootDir))

	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "before", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建磁盘失败")

	newName := "after"
	newQuota := uint64(16 << 20)
	updated, err := mgr1.UpdateDisk(ctx, UpdateDiskParams{
		DiskID:   disk.DiskID,
		Name:     &newName,
		MaxBytes: &newQuota,
	})

	require.NoError(t, err, "UpdateDisk 失败")
	require.Equal(t, "after", updated.Name, "更新后名称不匹配: want=after got=%q", updated.Name)
	require.Equal(t, uint64(16<<20), updated.MaxBytes, "更新后配额不匹配: want=%d got=%d", uint64(16<<20), updated.MaxBytes)
	require.NoError(t, mgr1.Close(), "关闭首任管理器失败")

	mgr2 := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr2.Close() })
	reloaded, err := mgr2.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err, "重启后 GetDisk 失败")
	require.Equal(t, "after", reloaded.Name, "重启后名称未持久化: want=after got=%q", reloaded.Name)
	require.Equal(t, uint64(16<<20), reloaded.MaxBytes, "重启后配额未持久化: want=%d got=%d", uint64(16<<20), reloaded.MaxBytes)
}

func TestUpdateDisk_ShrinkBelowUsedBytes_ReturnsInvalidShrink(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "shrink-guard", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建磁盘失败")
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "挂载磁盘失败")
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err, "获取 StoreActor 失败")
	fAttr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("used.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mnt.Epoch,
	})
	require.NoError(t, err, "创建文件失败")
	_, _, err = actor.Write(ctx, fAttr.Ino, 0, make([]byte, 8192), mnt.Epoch)
	require.NoError(t, err, "写入文件失败")

	shrinkBytes := uint64(4096)
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &shrinkBytes})

	require.ErrorIs(t, err, ErrInvalidShrink, "缩容低于已用应返回 ErrInvalidShrink: want=%v got=%v", ErrInvalidShrink, err)

	after, getErr := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, getErr, "查询原磁盘状态失败")
	require.Equal(t, uint64(8<<20), after.MaxBytes, "缩容失败后原配额不应变更: want=%d got=%d", uint64(8<<20), after.MaxBytes)
}

func TestUpdateDisk_InvalidParams_RejectsWithSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "valid", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建测试磁盘失败")

	tooLongName := strings.Repeat("u", 256)
	nulName := "bad\x00update"
	zeroBytes := uint64(0)
	overflowBytes := uint64(math.MaxInt64) + 1
	validName := "ok"

	cases := []struct {
		name    string
		params  UpdateDiskParams
		wantErr error
	}{
		{name: "格式非法 UUID", params: UpdateDiskParams{DiskID: "bad-uuid", Name: &validName}, wantErr: ErrInvalidArgument},
		{name: "全零 Nil UUID", params: UpdateDiskParams{DiskID: "00000000-0000-0000-0000-000000000000", Name: &validName}, wantErr: ErrInvalidArgument},
		{name: "未指定任何更新字段", params: UpdateDiskParams{DiskID: disk.DiskID}, wantErr: ErrInvalidArgument},
		{name: "名称超过 255 字节", params: UpdateDiskParams{DiskID: disk.DiskID, Name: &tooLongName}, wantErr: ErrNameTooLong},
		{name: "名称包含 NUL 字节", params: UpdateDiskParams{DiskID: disk.DiskID, Name: &nulName}, wantErr: ErrInvalidArgument},
		{name: "配额为零", params: UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &zeroBytes}, wantErr: ErrInvalidArgument},
		{name: "配额超过 MaxInt64", params: UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &overflowBytes}, wantErr: ErrInvalidArgument},
		{name: "目标磁盘不存在", params: UpdateDiskParams{DiskID: "018f0000-0000-7000-8000-000000009999", Name: &validName}, wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, uErr := mgr.UpdateDisk(ctx, tc.params)
			require.ErrorIs(t, uErr, tc.wantErr, "UpdateDisk(%s) 错误不匹配: want=%v got=%v", tc.name, tc.wantErr, uErr)
		})
	}
}

func TestDeleteDisk_ExistingDisk_RemovesDBRecordAndPhysicalDirectory(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "to-delete", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建磁盘失败")

	err = mgr.DeleteDisk(ctx, disk.DiskID)

	require.NoError(t, err, "DeleteDisk 失败")
	_, getErr := mgr.GetDisk(ctx, disk.DiskID)
	require.ErrorIs(t, getErr, ErrNotFound, "删除后 GetDisk 应返回 ErrNotFound: got=%v", getErr)

	_, statErr := os.Stat(filepath.Join(rootDir, "disks", disk.DiskID))
	require.ErrorIs(t, statErr, os.ErrNotExist, "删除后物理目录不应存在: got=%v", statErr)
}

func TestDeleteDisk_InvalidOrMissingID_ReturnsExpectedSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		diskID  string
		wantErr error
	}{
		{name: "格式非法 UUID", diskID: "bad-uuid", wantErr: ErrInvalidArgument},
		{name: "全零 Nil UUID", diskID: "00000000-0000-0000-0000-000000000000", wantErr: ErrInvalidArgument},
		{name: "不存在的磁盘", diskID: "018f0000-0000-7000-8000-000000009999", wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mgr.DeleteDisk(ctx, tc.diskID)
			require.ErrorIs(t, err, tc.wantErr, "DeleteDisk(%q) 错误不匹配: want=%v got=%v", tc.diskID, tc.wantErr, err)
		})
	}
}

func TestDeleteDisk_DirCleanupFailure_ReturnsErrorAndIncrementsCleanupMetric(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := newManager(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()), "注册 Collector 失败")

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "cleanup-fail", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建磁盘失败")

	removeErr := errors.New("simulated remove dir failure")
	mgr.removeAll = func(string) error { return removeErr }

	err = mgr.DeleteDisk(ctx, disk.DiskID)

	require.ErrorIs(t, err, removeErr, "目录清理失败应向上暴露错误: want=%v got=%v", removeErr, err)
	cleanupErrs := gatherCounterValue(t, reg, "bowl_storage_control_cleanup_errors_total")
	require.InDelta(t, 1.0, cleanupErrs, 1e-9, "cleanup_errors_total 应递增为 1: got=%v", cleanupErrs)
}

func TestManagerOptions_AccessorsAndCollector_ReflectConfiguredValues(t *testing.T) {

	rootDir := t.TempDir()
	mgr := New(
		db.NewMemStore(),
		WithStorageRoot(rootDir),
		WithEntryTimeout(2.0),
		WithAttrTimeout(3.0),
		WithDirEntryTimeout(4.0),
		WithEntryTimeout(-1.0),
		WithOpenKeepCache(true),
		WithOpenDirCache(true),
		WithAtimeMode(AtimeModeRelAtime),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()), "注册 Collector 失败")

	require.Equal(t, rootDir, mgr.StorageRoot(), "StorageRoot 不匹配")
	require.InDelta(t, 2.0, mgr.EntryTimeoutSeconds(), 1e-9, "负值不应覆盖 EntryTimeoutSeconds")
	require.InDelta(t, 3.0, mgr.AttrTimeoutSeconds(), 1e-9, "AttrTimeoutSeconds 不匹配")
	require.InDelta(t, 4.0, mgr.DirEntryTimeoutSeconds(), 1e-9, "DirEntryTimeoutSeconds 不匹配")
	require.InDelta(t, 4.0, mgr.EntryTimeoutForMode(store.S_IFDIR|0o755), 1e-9, "目录模式应返回 DirEntryTimeoutSeconds")
	require.InDelta(t, 2.0, mgr.EntryTimeoutForMode(store.S_IFREG|0o644), 1e-9, "常规文件模式应返回 EntryTimeoutSeconds")
	require.True(t, mgr.OpenKeepCache(), "OpenKeepCache 应为 true")
	require.True(t, mgr.OpenDirCache(), "OpenDirCache 应为 true")
	require.Equal(t, AtimeModeRelAtime, mgr.AtimeMode(), "AtimeMode 不匹配")
	require.InDelta(t, 0.0, gatherMetricLabelValue(t, reg, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9, "初始加载 rw 实例数应为 0")
	require.InDelta(t, 0.0, gatherGaugeValue(t, reg, "bowl_storage_commits_active"), 1e-9, "初始 commits_active 应为 0")
}
