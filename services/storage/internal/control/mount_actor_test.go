package control

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func gatherMetricSampleCount(t *testing.T, reg *prometheus.Registry, metricName, labelKey, labelVal string) uint64 {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err, "采集 Prometheus 指标失败")
	for _, fam := range fams {
		if fam.GetName() != metricName {
			continue
		}
		for _, m := range fam.GetMetric() {
			if hasMetricLabel(m, labelKey, labelVal) {
				return m.GetHistogram().GetSampleCount()
			}
		}
	}
	return 0
}

func hasMetricLabel(m *dto.Metric, key, val string) bool {
	for _, lp := range m.GetLabel() {
		if lp.GetName() == key && lp.GetValue() == val {
			return true
		}
	}
	return false
}

func countSpansByName(spans tracetest.SpanStubs, name string) int {
	total := 0
	for i := range spans {
		if spans[i].Name == name {
			total++
		}
	}
	return total
}

func setupTestTracer(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	prevTP := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})
	return tp, exporter
}

func TestMountDisk_ReadWrite_AdvancesEpochAndFencesStaleWriter(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "fence-disk", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建测试磁盘失败")

	mnt1, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "首次可写挂载失败")
	require.Equal(t, uint64(1), mnt1.Epoch, "首次可写挂载 epoch 不匹配: want=1 got=%d", mnt1.Epoch)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err, "获取 StoreActor 失败")
	fAttr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("data.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mnt1.Epoch,
	})
	require.NoError(t, err, "初始 epoch 创建文件失败")

	mnt2, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, Force: true})
	require.NoError(t, err, "第二次可写挂载失败")

	require.Equal(t, uint64(2), mnt2.Epoch, "第二次可写挂载 epoch 不匹配: want=2 got=%d", mnt2.Epoch)

	_, _, staleErr := actor.Write(ctx, fAttr.Ino, 0, []byte("stale"), mnt1.Epoch)
	require.ErrorIs(t, staleErr, ErrStaleEpoch, "陈旧 epoch=1 写入应被栅栏拦截: want=%v got=%v", ErrStaleEpoch, staleErr)

	written, _, freshErr := actor.Write(ctx, fAttr.Ino, 0, []byte("fresh"), mnt2.Epoch)
	require.NoError(t, freshErr, "新 epoch=2 写入应成功")
	require.Equal(t, uint32(5), written, "写入字节数不匹配: want=5 got=%d", written)
}

func TestMountDisk_ReadOnly_ReturnsZeroEpochWithoutFencingActiveWriter(t *testing.T) {

	ctx := context.Background()
	mgr := New(
		db.NewMemStore(),
		WithStorageRoot(t.TempDir()),
		WithEntryTimeout(2.5),
		WithAttrTimeout(3.5),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "ro-disk", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建测试磁盘失败")

	rwMount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "可写挂载失败")
	require.Equal(t, uint64(1), rwMount.Epoch, "可写挂载 epoch 不匹配: want=1 got=%d", rwMount.Epoch)

	roMount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})

	require.NoError(t, err, "只读挂载失败")
	require.Equal(t, uint64(0), roMount.Epoch, "只读挂载 epoch 应固定为 0: want=0 got=%d", roMount.Epoch)
	require.Equal(t, RootIno, roMount.RootAttr.Ino, "根 Inode 不匹配: want=%d got=%d", RootIno, roMount.RootAttr.Ino)
	require.InDelta(t, 2.5, roMount.EntryTimeoutSeconds, 1e-9, "EntryTimeoutSeconds 不匹配: want=2.5 got=%v", roMount.EntryTimeoutSeconds)
	require.InDelta(t, 3.5, roMount.AttrTimeoutSeconds, 1e-9, "AttrTimeoutSeconds 不匹配: want=3.5 got=%v", roMount.AttrTimeoutSeconds)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err, "获取 StoreActor 失败")
	_, err = actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("still-writable.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     rwMount.Epoch,
	})
	require.NoError(t, err, "只读挂载不得栅栏封锁已有可写 epoch=1")
}

func TestMountDisk_AfterRestart_PersistsEpochAndReclaimsOrphans(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := New(memDB, WithStorageRoot(rootDir))

	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "orphan-disk", MaxBytes: 16 << 20})
	require.NoError(t, err, "创建磁盘失败")
	mnt1, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "首次挂载失败")

	actor1, err := mgr1.Actor(ctx, disk.DiskID)
	require.NoError(t, err, "获取 StoreActor 失败")
	orphanAttr, err := actor1.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("orphan.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     mnt1.Epoch,
	})
	require.NoError(t, err, "创建孤儿测试文件失败")
	_, _, err = actor1.Write(ctx, orphanAttr.Ino, 0, []byte("orphan-payload"), mnt1.Epoch)
	require.NoError(t, err, "写入孤儿测试数据失败")
	require.NoError(t, actor1.Unlink(ctx, RootIno, []byte("orphan.bin"), true, mnt1.Epoch), "解链保留孤儿失败")
	require.NoError(t, mgr1.Close(), "关闭旧管理器失败")

	mgr2 := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr2.Close() })
	_, nonForceErr := mgr2.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, nonForceErr, ErrDiskMounted, "重启后未过期租约应继续拦截非强制挂载")
	mnt2, err := mgr2.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID, Force: true})

	require.NoError(t, err, "重启后可写挂载失败")
	require.Equal(t, uint64(2), mnt2.Epoch, "重启后可写挂载 epoch 应延续递增: want=2 got=%d", mnt2.Epoch)

	gotDisk, err := mgr2.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err, "查询清理后磁盘状态失败")
	require.Equal(t, uint64(0), gotDisk.UsedBytes, "MountDisk 应清扫孤儿节点释放空间: want=0 got=%d", gotDisk.UsedBytes)
	require.Equal(t, uint64(1), gotDisk.UsedInodes, "MountDisk 应仅保留根 Inode: want=1 got=%d", gotDisk.UsedInodes)
}

func TestMountDisk_InvalidOrMissingID_ReturnsExpectedSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		diskID  string
		wantErr error
	}{
		{name: "格式非法 UUID", diskID: "not-a-uuid", wantErr: ErrInvalidArgument},
		{name: "全零 Nil UUID", diskID: "00000000-0000-0000-0000-000000000000", wantErr: ErrInvalidArgument},
		{name: "不存在的磁盘", diskID: "018f0000-0000-7000-8000-000000009999", wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: tc.diskID})
			require.ErrorIs(t, err, tc.wantErr, "MountDisk(%q) 错误不匹配: want=%v got=%v", tc.diskID, tc.wantErr, err)
		})
	}
}

func assertActorZeroAllocHit(t *testing.T, ctx context.Context, mgr Manager, diskID string, want *store.StoreActor) {
	t.Helper()
	allocs := testing.AllocsPerRun(100, func() {
		got, err := mgr.Actor(ctx, diskID)
		if err != nil || got != want {
			t.Fatalf("热路径 Actor 返回异常: want=%v got=%v err=%v", want, got, err)
		}
	})
	require.Zero(t, allocs, "已加载磁盘热路径 Actor(ctx, diskID) 必须保持 0 allocs/op: got=%v", allocs)
}

func TestActor_ColdLoadAndWarmPoolHit_ObservabilityAndZeroAllocHotPath(t *testing.T) {

	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := db.NewMemStore()
	mgr1 := New(memDB, WithStorageRoot(rootDir))
	disk, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "hotpath-disk", MaxBytes: 8 << 20})
	require.NoError(t, err, "创建测试磁盘失败")
	_, err = mgr1.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.NoError(t, err, "挂载测试磁盘失败")
	require.NoError(t, mgr1.Close(), "关闭首任管理器失败")

	tp, exporter := setupTestTracer(t)
	mgr2 := New(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr2.Close() })
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr2.Collector()), "注册 Collector 失败")

	sampledCtx, rootSpan := tp.Tracer("test-control").Start(ctx, "rpc.ColdLoad")
	loaded, err := mgr2.Actor(sampledCtx, disk.DiskID)
	require.NoError(t, err, "冷加载 StoreActor 失败")
	require.Equal(t, uint64(1), loaded.WriterEpoch(), "重启懒加载后应保留持久化的 WriterEpoch=1: got=%d", loaded.WriterEpoch())
	rootSpan.End()

	coldOpenSpans := countSpansByName(exporter.GetSpans(), "control.OpenDiskActor")
	require.Equal(t, 1, coldOpenSpans, "冷加载应记录 1 个 OpenDiskActor Span: want=1 got=%d", coldOpenSpans)
	openOKCount := gatherMetricSampleCount(t, reg, "bowl_storage_control_open_actor_duration_seconds", "result", "ok")
	require.Equal(t, uint64(1), openOKCount, "冷加载成功应记录 1 次 open_actor result=ok: want=1 got=%d", openOKCount)

	exporter.Reset()
	assertActorZeroAllocHit(t, ctx, mgr2, disk.DiskID, loaded)
	require.Empty(t, exporter.GetSpans(), "热路径命中缓存池不应产生 OpenDiskActor 子 Span")
}

func TestActor_InvalidOrMissingID_ReturnsExpectedSentinel(t *testing.T) {

	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	cases := []struct {
		name    string
		diskID  string
		wantErr error
	}{
		{name: "格式非法 UUID", diskID: "not-a-uuid", wantErr: ErrInvalidArgument},
		{name: "全零 Nil UUID", diskID: "00000000-0000-0000-0000-000000000000", wantErr: ErrInvalidArgument},
		{name: "不存在的磁盘", diskID: "018f0000-0000-7000-8000-000000009999", wantErr: ErrNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := mgr.Actor(ctx, tc.diskID)
			require.ErrorIs(t, err, tc.wantErr, "Actor(%q) 错误不匹配: want=%v got=%v", tc.diskID, tc.wantErr, err)
		})
	}
}

func TestClose_IdempotentAndFencesSubsequentOperations(t *testing.T) {

	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))

	disk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "closed-fence", MaxBytes: 4 << 20})
	require.NoError(t, err, "创建测试磁盘失败")

	require.NoError(t, mgr.Close(), "首次 Close 应成功")
	require.NoError(t, mgr.Close(), "重复 Close 应幂等成功")

	_, err = mgr.CreateDisk(ctx, CreateDiskParams{Name: "after-close", MaxBytes: 4096})
	require.ErrorIs(t, err, ErrClosed, "关闭后 CreateDisk 应返回 ErrClosed: got=%v", err)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: disk.DiskID, Name: "after-close-fork"})
	require.ErrorIs(t, err, ErrClosed, "关闭后 ForkDisk 应返回 ErrClosed: got=%v", err)

	_, err = mgr.MountDisk(ctx, MountDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, ErrClosed, "关闭后 MountDisk 应返回 ErrClosed: got=%v", err)

	_, err = mgr.Actor(ctx, disk.DiskID)
	require.ErrorIs(t, err, ErrClosed, "关闭后 Actor 应返回 ErrClosed: got=%v", err)

	_, err = mgr.GetDisk(ctx, "018f0000-0000-7000-8000-000000009999")
	require.ErrorIs(t, err, ErrClosed, "关闭后 GetDisk 应返回 ErrClosed: got=%v", err)

	_, err = mgr.ListDisks(ctx, ListDisksParams{})
	require.ErrorIs(t, err, ErrClosed, "关闭后 ListDisks 应返回 ErrClosed: got=%v", err)

	newName := "should-fail"
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: disk.DiskID, Name: &newName})
	require.ErrorIs(t, err, ErrClosed, "关闭后 UpdateDisk 应返回 ErrClosed: got=%v", err)

	err = mgr.DeleteDisk(ctx, disk.DiskID)
	require.ErrorIs(t, err, ErrClosed, "关闭后 DeleteDisk 应返回 ErrClosed: got=%v", err)

	snap := memDB.Inspect()
	require.Len(t, snap.Disks, 1, "关闭后 DeleteDisk 不得破坏已持久化数据库记录: want=1 got=%d", len(snap.Disks))
}
