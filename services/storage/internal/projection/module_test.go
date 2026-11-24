package projection

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
)

func TestProjectionModule_GivenInvalidOrValidPoolConfig_WhenOpenPoolOrFxValidated_ThenMatchesContract(t *testing.T) {
	_, err := OpenPool(config.DBPoolConfig{DSN: "://bad-dsn"})
	require.Error(t, err)

	poolCfg := config.DBPoolConfig{
		DSN:             "postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable",
		MaxConns:        4,
		MinConns:        1,
		MaxConnLifetime: time.Minute,
		MaxConnIdleTime: time.Minute,
	}
	metaCfg := config.MetaConfig{
		AtimeModeVal:              config.DefaultAtimeMode,
		EntryTimeoutSecondsVal:    config.DefaultEntryTimeoutSeconds,
		AttrTimeoutSecondsVal:     config.DefaultAttrTimeoutSeconds,
		DirEntryTimeoutSecondsVal: config.DefaultDirEntryTimeoutSeconds,
		ActorIdleTTLVal:           config.DefaultActorIdleTTL,
		GCIntervalVal:             config.DefaultGCInterval,
		StorageDataDirVal:         t.TempDir(),
	}
	require.NoError(t, fx.ValidateApp(
		fx.Supply(poolCfg, metaCfg),
		Module,
	))
}

func TestProvideControlManager_WiresGCIntervalFromMetaConfig(t *testing.T) {
	ctx := context.Background()

	periodicMgr, err := ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     15 * time.Millisecond,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, periodicMgr.Close(context.Background())) })
	require.Equal(t, 15*time.Millisecond, periodicMgr.gcInterval)
	require.False(t, periodicMgr.disableGC)

	zeroPeriodicMgr, err := ProvideControlManager(ControlManagerParams{
		Store: NewMemStore(),
		MetaCfg: &config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
			GCIntervalVal:     0,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, zeroPeriodicMgr.Close(context.Background())) })
	require.Zero(t, zeroPeriodicMgr.gcInterval)
	require.False(t, zeroPeriodicMgr.disableGC)

	disk, err := periodicMgr.CreateDisk(ctx, "gc-wired", 4*1024*1024)
	require.NoError(t, err)
	require.NoError(t, periodicMgr.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
		f, cErr := ds.Create(ctx, engine.DefaultBranchName, engine.RootIno, "dead.bin", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		if _, wErr := ds.Write(ctx, engine.DefaultBranchName, f.Ino, 0, make([]byte, 32*1024)); wErr != nil {
			return wErr
		}
		return ds.Unlink(ctx, engine.DefaultBranchName, engine.RootIno, "dead.bin", false)
	}))
	require.Eventually(t, func() bool {
		var phys uint64
		if wErr := periodicMgr.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
			phys = ds.PackPhysicalBytes()
			return nil
		}); wErr != nil {
			return false
		}
		return phys == 0
	}, 3*time.Second, 15*time.Millisecond)
}

func TestStorageMetricsCollector_ExportsEngineMetricsWithoutDiskIDLabels(t *testing.T) {
	ctx := context.Background()
	mem := NewMemStore()
	mgr, err := NewControlManager(ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   mem,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close(context.Background()))
	})

	disk, err := mgr.CreateDisk(ctx, "metrics-disk", 4*1024*1024)
	require.NoError(t, err)

	mount, err := mgr.MountBranch(ctx, disk.DiskID, engine.DefaultBranchName, false, false)
	require.NoError(t, err)

	epochCtx := engine.WithWriterEpoch(ctx, mount.WriterEpoch)
	require.NoError(t, mgr.WithDisk(ctx, disk.DiskID, func(ds *engine.DiskStore) error {
		f, cErr := ds.Create(epochCtx, engine.DefaultBranchName, engine.RootIno, "payload.bin", 0o644, 0, 0)
		if cErr != nil {
			return cErr
		}
		_, wErr := ds.Write(epochCtx, engine.DefaultBranchName, f.Ino, 0, []byte("hello metrics"))
		return wErr
	}))

	collector := NewStorageMetricsCollector(mgr)
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(collector))

	fams, err := reg.Gather()
	require.NoError(t, err)

	var buf bytes.Buffer
	enc := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, fam := range fams {
		require.NoError(t, enc.Encode(fam))
	}
	text := buf.String()

	require.Contains(t, text, "bowl_storage_disks_loaded 1")
	require.Contains(t, text, "bowl_storage_logical_bytes ")
	require.Contains(t, text, "bowl_storage_pack_physical_bytes ")
	require.Contains(t, text, "bowl_storage_inodes ")
	require.Contains(t, text, "bowl_storage_branch_actors_loaded 1")
	require.Contains(t, text, "bowl_storage_pebble_block_cache_bytes ")
	require.NotContains(t, text, disk.DiskID.String())
	require.NotContains(t, text, "disk_id=")
}
