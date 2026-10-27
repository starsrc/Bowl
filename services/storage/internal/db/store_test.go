package db_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

// InodeAttrQuerier 提取 Inode 悲观锁读取与属性更新接口契约。
type InodeAttrQuerier interface {
	GetInodeForUpdate(ctx context.Context, ino int64) (db.Inode, error)
	UpdateInodeAttr(ctx context.Context, arg db.UpdateInodeAttrParams) (db.Inode, error)
	IncrementInodeNlinkAndTouch(ctx context.Context, ino int64) error
}

// SymlinkQuerier 提取符号链接创建与读取接口契约。
type SymlinkQuerier interface {
	CreateSymlinkInode(ctx context.Context, arg db.CreateSymlinkInodeParams) (db.Inode, error)
	GetSymlinkTarget(ctx context.Context, ino int64) (db.GetSymlinkTargetRow, error)
}

// LinkQuerier 提取硬链接原子递增与 ctime 刷新接口契约。
type LinkQuerier interface {
	IncrementInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
}

// UnlinkRmdirQuerier 提取 Unlink 与 Rmdir 目录项解链与计数更新接口契约。
type UnlinkRmdirQuerier interface {
	GetEntry(ctx context.Context, arg db.GetEntryParams) (db.Entry, error)
	DeleteEntry(ctx context.Context, arg db.DeleteEntryParams) error
	HasChildEntries(ctx context.Context, parentIno int64) (bool, error)
	DecrementInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
	ClearInodeNlinkAndTouchCtime(ctx context.Context, ino int64) (db.Inode, error)
	DecrementInodeNlinkAndTouch(ctx context.Context, ino int64) error
}

// RenameQuerier 提取 Rename 原子重命名、更新目录项与递归环检测接口契约。
type RenameQuerier interface {
	UpdateEntry(ctx context.Context, arg db.UpdateEntryParams) error
	UpdateEntryIno(ctx context.Context, arg db.UpdateEntryInoParams) error
	TouchInodeCtime(ctx context.Context, ino int64) error
	IsSubdirectory(ctx context.Context, arg db.IsSubdirectoryParams) (bool, error)
}

// FileHandleQuerier 提取文件句柄创建与持久化接口契约。
type FileHandleQuerier interface {
	CreateFileHandle(ctx context.Context, arg db.CreateFileHandleParams) (db.FileHandle, error)
	HasFileHandlesByIno(ctx context.Context, ino int64) (bool, error)
	GetFileHandle(ctx context.Context, arg db.GetFileHandleParams) (db.FileHandle, error)
	HasLinkableTmpFileHandleByIno(ctx context.Context, ino int64) (bool, error)
	ClearTmpFileFlagByIno(ctx context.Context, ino int64) error
	DeleteFileHandle(ctx context.Context, arg db.DeleteFileHandleParams) (int64, error)
}

// InodeLifecycleQuerier 提取 Inode 生命周期物理回收接口契约。
type InodeLifecycleQuerier interface {
	DeleteInode(ctx context.Context, ino int64) error
}

// ReadDirQuerier 提取目录遍历与父节点查询接口契约。
type ReadDirQuerier interface {
	GetParentInode(ctx context.Context, ino int64) (db.Inode, error)
	ListDirEntries(ctx context.Context, arg db.ListDirEntriesParams) ([]db.ListDirEntriesRow, error)
}

// XattrQuerier 提取扩展属性存取与配额查询接口契约。
type XattrQuerier interface {
	GetInodeXattr(ctx context.Context, arg db.GetInodeXattrParams) (db.InodeXattr, error)
	UpsertInodeXattr(ctx context.Context, arg db.UpsertInodeXattrParams) error
	GetInodeXattrNamesTotalBytes(ctx context.Context, ino int64) (int64, error)
	ListInodeXattrNames(ctx context.Context, ino int64) ([][]byte, error)
	DeleteInodeXattr(ctx context.Context, arg db.DeleteInodeXattrParams) (int64, error)
}

// FileLockQuerier 提取文件锁、阻塞等待者与客户端会话租约存取接口契约。
type FileLockQuerier interface {
	DeleteExpiredClientSessions(ctx context.Context) (int64, error)
	UpsertClientSession(ctx context.Context, arg db.UpsertClientSessionParams) (db.ClientSession, error)
	DeleteUnreferencedOrphanInodes(ctx context.Context) ([]db.DeleteUnreferencedOrphanInodesRow, error)
	GetFirstConflictingFileLock(ctx context.Context, arg db.GetFirstConflictingFileLockParams) (db.FileLock, error)
	ListActiveFileLocksByInoAndFlags(ctx context.Context, arg db.ListActiveFileLocksByInoAndFlagsParams) ([]db.FileLock, error)
	CreateFileLock(ctx context.Context, arg db.CreateFileLockParams) (db.FileLock, error)
	DeleteFileLocksByIDs(ctx context.Context, ids []int64) error
	DeletePosixFileLocksBySessionAndOwner(ctx context.Context, arg db.DeletePosixFileLocksBySessionAndOwnerParams) (int64, error)
	DeletePosixFileLocksByOwner(ctx context.Context, arg db.DeletePosixFileLocksByOwnerParams) (int64, error)
	DeleteFlockFileLocksBySessionAndFh(ctx context.Context, arg db.DeleteFlockFileLocksBySessionAndFhParams) (int64, error)
	DeleteFlockFileLocksByFh(ctx context.Context, arg db.DeleteFlockFileLocksByFhParams) (int64, error)
	GetFileLockWaiter(ctx context.Context, arg db.GetFileLockWaiterParams) (db.FileLockWaiter, error)
	ListPendingFileLockWaitersByInoAndFlags(ctx context.Context, arg db.ListPendingFileLockWaitersByInoAndFlagsParams) ([]db.FileLockWaiter, error)
	UpsertFileLockWaiter(ctx context.Context, arg db.UpsertFileLockWaiterParams) (db.FileLockWaiter, error)
	DeleteFileLockWaiter(ctx context.Context, arg db.DeleteFileLockWaiterParams) (int64, error)
}

// DiskQuerier 提取磁盘元数据增删改查、配额更新与按根节点级联清理接口契约。
type DiskQuerier interface {
	CreateDisk(ctx context.Context, arg db.CreateDiskParams) (db.Disk, error)
	GetDisk(ctx context.Context, diskID pgtype.UUID) (db.Disk, error)
	GetDiskForUpdate(ctx context.Context, diskID pgtype.UUID) (db.Disk, error)
	GetDiskByRootIno(ctx context.Context, rootIno int64) (db.Disk, error)
	GetDiskByRootInoForUpdate(ctx context.Context, rootIno int64) (db.Disk, error)
	UpdateDisk(ctx context.Context, arg db.UpdateDiskParams) (db.Disk, error)
	UpdateDiskUsedBytes(ctx context.Context, arg db.UpdateDiskUsedBytesParams) (db.Disk, error)
	UpdateDiskQuotaByRootIno(ctx context.Context, arg db.UpdateDiskQuotaByRootInoParams) (db.Disk, error)
	AddDiskQuotaByRootIno(ctx context.Context, arg db.AddDiskQuotaByRootInoParams) error
	DeleteDisk(ctx context.Context, diskID pgtype.UUID) (int64, error)
	DeleteInodesByRootIno(ctx context.Context, rootIno int64) ([]int64, error)
	CreateRootInode(ctx context.Context, arg db.CreateRootInodeParams) (db.Inode, error)
}

// 静态验证 db.Querier、db.Store 与 *db.Queries 完备实现各接口契约。
var (
	_ InodeAttrQuerier = (db.Querier)(nil)
	_ InodeAttrQuerier = (db.Store)(nil)
	_ InodeAttrQuerier = (*db.Queries)(nil)

	_ SymlinkQuerier = (db.Querier)(nil)
	_ SymlinkQuerier = (db.Store)(nil)
	_ SymlinkQuerier = (*db.Queries)(nil)

	_ LinkQuerier = (db.Querier)(nil)
	_ LinkQuerier = (db.Store)(nil)
	_ LinkQuerier = (*db.Queries)(nil)

	_ UnlinkRmdirQuerier = (db.Querier)(nil)
	_ UnlinkRmdirQuerier = (db.Store)(nil)
	_ UnlinkRmdirQuerier = (*db.Queries)(nil)

	_ RenameQuerier = (db.Querier)(nil)
	_ RenameQuerier = (db.Store)(nil)
	_ RenameQuerier = (*db.Queries)(nil)

	_ FileHandleQuerier = (db.Querier)(nil)
	_ FileHandleQuerier = (db.Store)(nil)
	_ FileHandleQuerier = (*db.Queries)(nil)

	_ InodeLifecycleQuerier = (db.Querier)(nil)
	_ InodeLifecycleQuerier = (db.Store)(nil)
	_ InodeLifecycleQuerier = (*db.Queries)(nil)

	_ ReadDirQuerier = (db.Querier)(nil)
	_ ReadDirQuerier = (db.Store)(nil)
	_ ReadDirQuerier = (*db.Queries)(nil)

	_ XattrQuerier = (db.Querier)(nil)
	_ XattrQuerier = (db.Store)(nil)
	_ XattrQuerier = (*db.Queries)(nil)

	_ FileLockQuerier = (db.Querier)(nil)
	_ FileLockQuerier = (db.Store)(nil)
	_ FileLockQuerier = (*db.Queries)(nil)

	_ DiskQuerier = (db.Querier)(nil)
	_ DiskQuerier = (db.Store)(nil)
	_ DiskQuerier = (*db.Queries)(nil)

	_ tx.Transactor[db.Querier] = (db.Store)(nil)
)

// recordingLifecycle 用于离线捕获 Fx 生命周期钩子。
type recordingLifecycle struct {
	hooks []fx.Hook
}

func (r *recordingLifecycle) Append(hook fx.Hook) {
	r.hooks = append(r.hooks, hook)
}

func TestOpenPool_InvalidDSN(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://%invalid-url",
	}

	pool, err := db.OpenPool(cfg)
	require.Error(t, err)
	assert.Nil(t, pool)
	assert.Contains(t, err.Error(), "failed to parse dbpool config")
}

func TestOpenPool_TracingEnabled(t *testing.T) {
	t.Run("TracingEnabled 为 true 时挂载 OTel Tracer", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/testdb",
			TracingEnabled: true,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.NotNil(t, pool.Config().ConnConfig.Tracer)
	})

	t.Run("TracingEnabled 为 false 时不挂载 OTel Tracer", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/testdb",
			TracingEnabled: false,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Nil(t, pool.Config().ConnConfig.Tracer)
	})
}

func TestOpenPool_MinConnsClamped(t *testing.T) {
	t.Run("MinConns 大于 MaxConns 时触发尺寸保护钳制为 MaxConns", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:      "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns: 10,
			MinConns: 20,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Equal(t, int32(10), pool.Config().MaxConns)
		assert.Equal(t, int32(10), pool.Config().MinConns)
	})

	t.Run("MinConns 小于等于 MaxConns 时保持原样", func(t *testing.T) {
		cfg := config.DBPoolConfig{
			DSN:      "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns: 20,
			MinConns: 5,
		}

		pool, err := db.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		assert.Equal(t, int32(20), pool.Config().MaxConns)
		assert.Equal(t, int32(5), pool.Config().MinConns)
	})
}

func TestOpenPool_PoolConfigOptions(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN:               "postgres://user:pass@127.0.0.1:5432/testdb?connect_timeout=5",
		MaxConns:          15,
		MinConns:          3,
		MaxConnLifetime:   45 * time.Minute,
		MaxConnIdleTime:   15 * time.Minute,
		HealthCheckPeriod: 30 * time.Second,
		TracingEnabled:    false,
	}

	pool, err := db.OpenPool(cfg)
	require.NoError(t, err)
	require.NotNil(t, pool)
	defer pool.Close()

	poolCfg := pool.Config()
	assert.Equal(t, int32(15), poolCfg.MaxConns)
	assert.Equal(t, int32(3), poolCfg.MinConns)
	assert.Equal(t, 45*time.Minute, poolCfg.MaxConnLifetime)
	assert.Equal(t, 15*time.Minute, poolCfg.MaxConnIdleTime)
	assert.Equal(t, 30*time.Second, poolCfg.HealthCheckPeriod)
	assert.Equal(t, 5*time.Second, poolCfg.ConnConfig.ConnectTimeout)
}

func TestNewStore(t *testing.T) {
	store := db.NewStore(nil)
	assert.NotNil(t, store)

	var _ db.Store = store
}

func TestNewStore_PoolAndClose(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}
	pool, err := db.OpenPool(cfg)
	require.NoError(t, err)
	require.NotNil(t, pool)

	store := db.NewStore(pool)
	require.NotNil(t, store)

	sqlStore, ok := store.(*db.SQLStore)
	require.True(t, ok)
	assert.Same(t, pool, sqlStore.Pool())

	// 调用 Close 关闭连接池
	sqlStore.Close()

	// 验证 pool 已关闭
	_, acquireErr := pool.Acquire(context.Background())
	require.Error(t, acquireErr)
	assert.Contains(t, acquireErr.Error(), "closed pool")

	// 幂等重复调用 Close 不 panic
	assert.NotPanics(t, func() {
		sqlStore.Close()
	})

	// pool 为 nil 时的 SQLStore.Close() 不 panic
	nilStore := db.NewStore(nil)
	assert.NotPanics(t, func() {
		nilStore.(*db.SQLStore).Close()
	})
	assert.Nil(t, nilStore.(*db.SQLStore).Pool())
}

func TestModule_FxProvidesStoreAndPool(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}

	t.Run("同时提供 *pgxpool.Pool 与 db.Store (携带 telemetry.Ready)", func(t *testing.T) {
		var (
			store db.Store
			pool  *pgxpool.Pool
		)
		app := fxtest.New(
			t,
			fx.Supply(cfg),
			fx.Supply(telemetry.Ready{}),
			db.Module,
			fx.Populate(&store, &pool),
		)
		require.NoError(t, app.Err())
		assert.NotNil(t, store)
		assert.NotNil(t, pool)
	})

	t.Run("未显式提供 telemetry.Ready 时通过 optional 正常装配", func(t *testing.T) {
		var (
			store db.Store
			pool  *pgxpool.Pool
		)
		app := fxtest.New(
			t,
			fx.Supply(cfg),
			db.Module,
			fx.Populate(&store, &pool),
		)
		require.NoError(t, app.Err())
		assert.NotNil(t, store)
		assert.NotNil(t, pool)
	})

	t.Run("非法 DSN 时 Fx 初始化报错", func(t *testing.T) {
		invalidCfg := config.DBPoolConfig{
			DSN: "postgres://%invalid-url",
		}
		var store db.Store
		app := fx.New(
			fx.NopLogger,
			fx.Supply(invalidCfg),
			db.Module,
			fx.Populate(&store),
		)
		require.Error(t, app.Err())
		assert.Contains(t, app.Err().Error(), "failed to initialize dbpool")
	})
}

func TestModule_Lifecycle_PingFailure(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:59999/testdb?connect_timeout=1",
	}

	var (
		store db.Store
		pool  *pgxpool.Pool
	)
	app := fxtest.New(
		t,
		fx.Supply(cfg),
		db.Module,
		fx.Populate(&store, &pool),
	)
	require.NoError(t, app.Err())
	require.NotNil(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := app.Start(ctx)
	require.Error(t, err, "不可达数据库端口在 OnStart 探活必须报错")
	assert.Contains(t, err.Error(), "failed to ping database")

	// 探活失败后连接池必须已被自动关闭
	_, acquireErr := pool.Acquire(context.Background())
	require.Error(t, acquireErr)
	assert.Contains(t, acquireErr.Error(), "closed pool")
}

func TestModule_Lifecycle_OnStop(t *testing.T) {
	cfg := config.DBPoolConfig{
		DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
	}

	t.Run("OnStop 钩子成功执行并关闭底层连接池", func(t *testing.T) {
		lc := &recordingLifecycle{}
		pool, err := db.ProvidePool(db.PoolParams{
			Lifecycle: lc,
			Config:    cfg,
		})
		require.NoError(t, err)
		require.NotNil(t, pool)
		require.Len(t, lc.hooks, 1, "应注册单个 Fx 生命周期钩子")

		hook := lc.hooks[0]
		require.NotNil(t, hook.OnStop, "OnStop 回调函数不得为空")

		// 执行 OnStop 关闭钩子
		ctx := context.Background()
		err = hook.OnStop(ctx)
		assert.NoError(t, err, "OnStop 钩子应成功执行")

		// 验证连接池已关闭
		_, acquireErr := pool.Acquire(ctx)
		require.Error(t, acquireErr, "已关闭的连接池调用 Acquire 应返回错误")
		assert.Contains(t, acquireErr.Error(), "closed pool")

		// 验证 OnStop 重复调用具备幂等性
		repeatErr := hook.OnStop(ctx)
		assert.NoError(t, repeatErr, "重复调用 OnStop 不应报错")
	})

	t.Run("fxtest.Lifecycle 正常接收并触发 OnStop 钩子", func(t *testing.T) {
		lc := fxtest.NewLifecycle(t)
		pool, err := db.ProvidePool(db.PoolParams{
			Lifecycle: lc,
			Config:    cfg,
		})
		require.NoError(t, err)
		require.NotNil(t, pool)

		// 触发 Stop 阶段验证 Fx 集成正常
		lc.RequireStop()
	})
}

func TestModule_Lifecycle_PingSuccess(t *testing.T) {
	dsn := os.Getenv("STORAGE_TEST_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	probePool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过成功探活测试: 连接 postgres 失败: %v", err)
	}
	defer probePool.Close()

	if err := probePool.Ping(ctx); err != nil {
		t.Skipf("跳过成功探活测试 (ping 失败): %v", err)
	}

	cfg := config.DBPoolConfig{
		DSN: dsn,
	}

	var (
		store db.Store
		pool  *pgxpool.Pool
	)
	app := fxtest.New(
		t,
		fx.Supply(cfg),
		db.Module,
		fx.Populate(&store, &pool),
	)
	require.NoError(t, app.Err())

	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer startCancel()

	err = app.Start(startCtx)
	require.NoError(t, err)

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = app.Stop(stopCtx)
	require.NoError(t, err)
}

func TestQuerierInterface(t *testing.T) {
	var _ db.Querier = (*db.Queries)(nil)
	var _ db.Querier = (db.Store)(nil)
}


func TestQuerierInterface_MknodOperators(t *testing.T) {
	type mknodQuerier interface {
		CreateInode(ctx context.Context, arg db.CreateInodeParams) (db.Inode, error)
		CreateEntry(ctx context.Context, arg db.CreateEntryParams) error
		TouchInodeTimestamps(ctx context.Context, ino int64) error
	}

	var _ mknodQuerier = (db.Querier)(nil)
	var _ mknodQuerier = (db.Store)(nil)
}


// ==========================================
// 数据库持久化集成测试 (Database Integration Tests)
// ==========================================

var testInoSeq = time.Now().UnixMicro()

func nextTestIno() int64 {
	return atomic.AddInt64(&testInoSeq, 1)
}

func newTestUUID() pgtype.UUID {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return pgtype.UUID{Bytes: b, Valid: true}
}

func setupTestDB(t *testing.T) (db.Store, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("STORAGE_DB_DSN")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var pool *pgxpool.Pool
	var err error

	if dsn != "" {
		pool, err = pgxpool.New(ctx, dsn)
		if err == nil {
			err = pool.Ping(ctx)
		}
		if err != nil {
			if pool != nil {
				pool.Close()
			}
			t.Skipf("跳过 PostgreSQL 测试 (配置的 DSN 连接/ping 失败): %v", err)
		}
	} else {
		// 未指定环境变量时，优先尝试 5432，失败自动尝试 9432
		dsn5432 := "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
		pool, err = pgxpool.New(ctx, dsn5432)
		if err == nil {
			err = pool.Ping(ctx)
		}
		if err != nil {
			if pool != nil {
				pool.Close()
			}
			dsn9432 := "postgres://postgres:postgres@localhost:9432/bowl?sslmode=disable"
			pool, err = pgxpool.New(ctx, dsn9432)
			if err == nil {
				err = pool.Ping(ctx)
			}
			if err != nil {
				if pool != nil {
					pool.Close()
				}
				t.Skipf("跳过 PostgreSQL 测试 (5432 与 9432 均连接失败): %v", err)
			}
		}
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return db.NewStore(pool), pool
}

func TestStore_Integration_CreateAndGetDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "integration-storage-disk-1",
		RootIno:  rootIno,
		MaxBytes: 100 * 1024 * 1024,
	}

	// 1. 创建磁盘实体
	created, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, diskID, created.DiskID)
	assert.Equal(t, "integration-storage-disk-1", created.Name)
	assert.Equal(t, rootIno, created.RootIno)
	assert.Equal(t, int64(100*1024*1024), created.MaxBytes)
	assert.Equal(t, int64(0), created.UsedBytes)
	assert.Equal(t, int64(1), created.UsedInodes)
	assert.True(t, created.CreatedAt.Valid)
	assert.True(t, created.UpdatedAt.Valid)

	// 2. 主键查询磁盘实体
	got, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, got.DiskID)
	assert.Equal(t, created.Name, got.Name)
	assert.Equal(t, created.RootIno, got.RootIno)
	assert.Equal(t, created.MaxBytes, got.MaxBytes)
	assert.Equal(t, created.UsedBytes, got.UsedBytes)
	assert.Equal(t, created.UsedInodes, got.UsedInodes)

	// 3. 按 root_ino 查询磁盘实体
	gotByRoot, err := store.GetDiskByRootIno(ctx, rootIno)
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, gotByRoot.DiskID)
	assert.Equal(t, created.Name, gotByRoot.Name)
	assert.Equal(t, created.RootIno, gotByRoot.RootIno)

	// 4. 查询不存在的磁盘实体返回 pgx.ErrNoRows
	nonExistentID := newTestUUID()
	_, err = store.GetDisk(ctx, nonExistentID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetDiskByRootIno(ctx, nextTestIno())
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_GetDiskForUpdate(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "for-update-storage-disk",
		RootIno:  rootIno,
		MaxBytes: 50 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 事务中悲观排他锁读取
	err = store.ExecTx(ctx, func(q db.Querier) error {
		locked, getErr := q.GetDiskForUpdate(ctx, diskID)
		require.NoError(t, getErr)
		assert.Equal(t, diskID, locked.DiskID)
		assert.Equal(t, "for-update-storage-disk", locked.Name)

		lockedByRoot, getByRootErr := q.GetDiskByRootInoForUpdate(ctx, rootIno)
		require.NoError(t, getByRootErr)
		assert.Equal(t, diskID, lockedByRoot.DiskID)
		assert.Equal(t, rootIno, lockedByRoot.RootIno)
		return nil
	})
	require.NoError(t, err)

	// 查询不存在的磁盘返回 pgx.ErrNoRows
	_, err = store.GetDiskForUpdate(ctx, newTestUUID())
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetDiskByRootInoForUpdate(ctx, nextTestIno())
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_UpdateDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "original-disk-name",
		RootIno:  rootIno,
		MaxBytes: 50 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 更新名称与容量上限
	updateParams := db.UpdateDiskParams{
		DiskID:   diskID,
		Name:     "renamed-storage-disk",
		MaxBytes: 200 * 1024 * 1024,
	}
	updated, err := store.UpdateDisk(ctx, updateParams)
	require.NoError(t, err)
	assert.Equal(t, "renamed-storage-disk", updated.Name)
	assert.Equal(t, int64(200*1024*1024), updated.MaxBytes)

	// 更新已用字节数
	usedParams := db.UpdateDiskUsedBytesParams{
		DiskID:    diskID,
		UsedBytes: 10 * 1024 * 1024,
	}
	updatedUsed, err := store.UpdateDiskUsedBytes(ctx, usedParams)
	require.NoError(t, err)
	assert.Equal(t, int64(10*1024*1024), updatedUsed.UsedBytes)
}

func TestStore_Integration_DiskQuotaUpdates(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "quota-disk",
		RootIno:  rootIno,
		MaxBytes: 100 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 1. UpdateDiskQuotaByRootIno 正向增加
	q1, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  4096,
		DeltaInodes: 1,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(4096), q1.UsedBytes)
	assert.Equal(t, int64(2), q1.UsedInodes)

	// 2. UpdateDiskQuotaByRootIno 负向扣减
	q2, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  -2048,
		DeltaInodes: -1,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2048), q2.UsedBytes)
	assert.Equal(t, int64(1), q2.UsedInodes)

	// 3. UpdateDiskQuotaByRootIno 超额扣减（GREATEST 钳位归零）
	q3, err := store.UpdateDiskQuotaByRootIno(ctx, db.UpdateDiskQuotaByRootInoParams{
		DeltaBytes:  -100000,
		DeltaInodes: -10,
		RootIno:     rootIno,
	})
	require.NoError(t, err)
	assert.Equal(t, int64(0), q3.UsedBytes)
	assert.Equal(t, int64(0), q3.UsedInodes)

	// 4. AddDiskQuotaByRootIno 增量累加
	err = store.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
		DeltaBytes:  8192,
		DeltaInodes: 5,
		RootIno:     rootIno,
	})
	require.NoError(t, err)

	got, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(8192), got.UsedBytes)
	assert.Equal(t, int64(5), got.UsedInodes)

	// 5. 对不存在的 root_ino 调用 AddDiskQuotaByRootIno 影响 0 行且不返回错误
	err = store.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
		DeltaBytes:  1024,
		DeltaInodes: 1,
		RootIno:     nextTestIno(),
	})
	require.NoError(t, err)
}

func TestStore_Integration_DeleteDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	params := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "delete-target-disk",
		RootIno:  rootIno,
		MaxBytes: 10 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 正常删除返回 1
	rows, err := store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)

	// 再次查询返回 pgx.ErrNoRows
	_, err = store.GetDisk(ctx, diskID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 再次删除影响 0 行
	rows, err = store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestStore_Integration_DiskConstraints(t *testing.T) {
	store, pool := setupTestDB(t)
	ctx := context.Background()

	t.Run("chk_disks_root_ino 约束: root_ino <= 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "invalid-root-ino",
			RootIno:  0,
			MaxBytes: 100 * 1024 * 1024,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_root_ino", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: max_bytes <= 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "invalid-max-bytes",
			RootIno:  nextTestIno(),
			MaxBytes: 0,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: used_bytes > max_bytes 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "capacity-test-disk",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 1001,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: used_inodes < 0 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "inode-capacity-test",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = pool.Exec(ctx, "UPDATE disks SET used_inodes = -1 WHERE disk_id = $1", diskID)
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("root_ino UNIQUE 约束: 重复 root_ino 被拒绝", func(t *testing.T) {
		diskID1 := newTestUUID()
		diskID2 := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID1)
			_, _ = store.DeleteDisk(context.Background(), diskID2)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID1,
			Name:     "disk-1",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		_, err = store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID2,
			Name:     "disk-2",
			RootIno:  rootIno,
			MaxBytes: 2000,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr))
		assert.Equal(t, "23505", pgErr.Code)
	})
}

func TestStore_Integration_InodesRootInoAndCascade(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	// 1. CreateRootInode 自动设置 ino == root_ino == parent_ino
	rootInode, err := store.CreateRootInode(ctx, db.CreateRootInodeParams{
		Mode:  0o040755,
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
		Rdev:  0,
		Size:  4096,
	})
	require.NoError(t, err)
	assert.Greater(t, rootInode.Ino, int64(1))
	assert.Equal(t, rootInode.Ino, rootInode.RootIno)
	assert.Equal(t, rootInode.Ino, rootInode.ParentIno)

	t.Cleanup(func() {
		_, _ = store.DeleteInodesByRootIno(context.Background(), rootInode.Ino)
	})

	// 2. CreateInode 继承 root_ino
	childInode, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:      0o100644,
		Nlink:     1,
		Uid:       1000,
		Gid:       1000,
		Rdev:      0,
		Size:      1024,
		ParentIno: rootInode.Ino,
		RootIno:   rootInode.Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, childInode.RootIno)
	assert.Equal(t, rootInode.Ino, childInode.ParentIno)

	// 3. CreateSymlinkInode 继承 root_ino
	symlinkInode, err := store.CreateSymlinkInode(ctx, db.CreateSymlinkInodeParams{
		Mode:          0o120777,
		Size:          10,
		Nlink:         1,
		Uid:           1000,
		Gid:           1000,
		Rdev:          0,
		SymlinkTarget: []byte("/target/path"),
		ParentIno:     rootInode.Ino,
		RootIno:       rootInode.Ino,
	})
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, symlinkInode.RootIno)
	assert.Equal(t, rootInode.Ino, symlinkInode.ParentIno)

	// 4. 创建 entries 目录项
	err = store.CreateEntry(ctx, db.CreateEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
		Ino:       childInode.Ino,
	})
	require.NoError(t, err)

	err = store.CreateEntry(ctx, db.CreateEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-symlink"),
		Ino:       symlinkInode.Ino,
	})
	require.NoError(t, err)

	// 5. 验证 GetInode 与 GetInodeByEntry 能够查到带有正确 root_ino 的数据
	gotChild, err := store.GetInode(ctx, childInode.Ino)
	require.NoError(t, err)
	assert.Equal(t, rootInode.Ino, gotChild.RootIno)

	gotByEntry, err := store.GetInodeByEntry(ctx, db.GetInodeByEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
	})
	require.NoError(t, err)
	assert.Equal(t, childInode.Ino, gotByEntry.Ino)
	assert.Equal(t, rootInode.Ino, gotByEntry.RootIno)

	// 6. DeleteInodesByRootIno 级联清理所有 inodes 及 entries 表中的关联记录
	deletedInos, err := store.DeleteInodesByRootIno(ctx, rootInode.Ino)
	require.NoError(t, err)
	assert.ElementsMatch(t, []int64{rootInode.Ino, childInode.Ino, symlinkInode.Ino}, deletedInos)

	// 7. 验证 inodes 已全部被物理清理
	_, err = store.GetInode(ctx, rootInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetInode(ctx, childInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetInode(ctx, symlinkInode.Ino)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 8. 验证 entries 记录因 fk_entries_ino ON DELETE CASCADE 也已被级联清理
	_, err = store.GetEntry(ctx, db.GetEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-file.txt"),
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = store.GetEntry(ctx, db.GetEntryParams{
		ParentIno: rootInode.Ino,
		Name:      []byte("child-symlink"),
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_TransactionRollbackAndCommit(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	t.Run("事务发生错误时原子回滚", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()
		simulatedErr := errors.New("simulated business error inside tx")

		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "rollback-disk",
				RootIno:  rootIno,
				MaxBytes: 10 * 1024 * 1024,
			})
			require.NoError(t, createErr)

			return simulatedErr
		})
		require.ErrorIs(t, err, simulatedErr)

		// 验证 disk 未持久化
		_, err = store.GetDisk(ctx, diskID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})

	t.Run("事务成功提交后持久化生效", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "commit-disk",
				RootIno:  rootIno,
				MaxBytes: 20 * 1024 * 1024,
			})
			require.NoError(t, createErr)

			return nil
		})
		require.NoError(t, err)

		// 验证 disk 已持久化
		disk, err := store.GetDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, diskID, disk.DiskID)
		assert.Equal(t, "commit-disk", disk.Name)
	})
}

// TestStore_Integration_InodeXattrsAndTotalBytes 验证基于真实 PostgreSQL 的扩展属性各算子及 NUL 分隔符计费准确性。
func TestStore_Integration_InodeXattrsAndTotalBytes(t *testing.T) {
	store, pool := setupTestDB(t)
	ctx := context.Background()

	ino := nextTestIno()
	_, err := pool.Exec(ctx, `
		INSERT INTO inodes (ino, mode, nlink, uid, gid, rdev, size, root_ino, atime, mtime, ctime)
		VALUES ($1, $2, 1, 1000, 1000, 0, 0, $1, clock_timestamp(), clock_timestamp(), clock_timestamp());
	`, ino, int32(0o100644))
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM inode_xattrs WHERE ino = $1; DELETE FROM inodes WHERE ino = $1;`, ino)
	})

	// 1. 无扩展属性时 GetInodeXattrNamesTotalBytes 返回 0
	totalBytes, err := store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(0), totalBytes, "无扩展属性时 GetInodeXattrNamesTotalBytes 必须返回 0")

	names, err := store.ListInodeXattrNames(ctx, ino)
	require.NoError(t, err)
	assert.Empty(t, names)

	// 2. 插入属性 "user.a" (len 6 字节，计费 6+1=7 字节)
	err = store.UpsertInodeXattr(ctx, db.UpsertInodeXattrParams{
		Ino:   ino,
		Name:  []byte("user.a"),
		Value: []byte("val-a"),
	})
	require.NoError(t, err)

	totalBytes, err = store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(7), totalBytes, "插入 user.a 后计费应为 6+1=7")

	// 3. 插入属性 "user.bc" (len 7 字节，计费 7+1=8 字节)
	// 合计总字节数必须精确返回 (6+1)+(7+1) = 15 字节（验证 SQL 中的 + 1 NUL 分隔符计费）
	err = store.UpsertInodeXattr(ctx, db.UpsertInodeXattrParams{
		Ino:   ino,
		Name:  []byte("user.bc"),
		Value: []byte("val-bc"),
	})
	require.NoError(t, err)

	totalBytes, err = store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(15), totalBytes, "插入 user.a 与 user.bc 后计费应精确为 (6+1)+(7+1)=15")

	names, err = store.ListInodeXattrNames(ctx, ino)
	require.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{[]byte("user.a"), []byte("user.bc")}, names)

	xattrA, err := store.GetInodeXattr(ctx, db.GetInodeXattrParams{Ino: ino, Name: []byte("user.a")})
	require.NoError(t, err)
	assert.Equal(t, []byte("val-a"), xattrA.Value)

	// 4. 覆盖更新同名属性 user.a 的值
	err = store.UpsertInodeXattr(ctx, db.UpsertInodeXattrParams{
		Ino:   ino,
		Name:  []byte("user.a"),
		Value: []byte("val-a-updated"),
	})
	require.NoError(t, err)

	totalBytes, err = store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(15), totalBytes, "更新同名属性 user.a 值后计费仍应为 15")

	xattrAUpdated, err := store.GetInodeXattr(ctx, db.GetInodeXattrParams{Ino: ino, Name: []byte("user.a")})
	require.NoError(t, err)
	assert.Equal(t, []byte("val-a-updated"), xattrAUpdated.Value)

	// 5. 删除属性 user.a 后 GetInodeXattrNamesTotalBytes 精确扣减
	deletedRows, err := store.DeleteInodeXattr(ctx, db.DeleteInodeXattrParams{
		Ino:  ino,
		Name: []byte("user.a"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedRows)

	totalBytes, err = store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(8), totalBytes, "删除 user.a 后计费应精确为 7+1=8")

	// 删除属性 user.bc 后计费归零
	deletedRows, err = store.DeleteInodeXattr(ctx, db.DeleteInodeXattrParams{
		Ino:  ino,
		Name: []byte("user.bc"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedRows)

	totalBytes, err = store.GetInodeXattrNamesTotalBytes(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, int64(0), totalBytes, "全部属性删除后计费应精确归 0")
}

// TestMigration_AddDisksAndInodeRootIno_BackfillAndDown 验证 20261023000000 迁移的存量目录树递归回填、子盘边界截断、孤儿节点归属及回滚数据保护。
func TestMigration_AddDisksAndInodeRootIno_BackfillAndDown(t *testing.T) {
	pool, _ := createUniqueTempDB(t, "bowl_mig_backfill")
	ctx := context.Background()

	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
		"20261017010000_convert_symlink_target_to_bytea.sql",
		"20261017020000_convert_entries_name_to_bytea.sql",
		"20261017030000_create_inode_xattrs_table.sql",
		"20261018000000_create_file_locks_tables.sql",
		"20261018010000_add_session_id_to_file_handles.sql",
		"20261019000000_align_juicefs_meta_foundation.sql",
	}
	for _, f := range earlyMigrations {
		up := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, pool, up)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 准备存量环境基础表结构（保证 inodes.root_ino 与 disks 表存在供测试插入初始数据）
	initialDisksSQL := `
		ALTER TABLE inodes ADD COLUMN IF NOT EXISTS root_ino BIGINT NOT NULL DEFAULT 0;
		CREATE TABLE IF NOT EXISTS disks (
			disk_id UUID PRIMARY KEY,
			name VARCHAR(255) NOT NULL DEFAULT '',
			root_ino BIGINT NOT NULL UNIQUE,
			max_bytes BIGINT NOT NULL,
			used_bytes BIGINT NOT NULL DEFAULT 0,
			used_inodes BIGINT NOT NULL DEFAULT 1,
			created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
			CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes AND used_inodes >= 0)
		);
	`
	_, err := pool.Exec(ctx, initialDisksSQL)
	require.NoError(t, err)

	migrationData, err := os.ReadFile("../../db/migrations/20261023000000_add_disks_and_inode_root_ino.sql")
	require.NoError(t, err)

	parts := strings.Split(string(migrationData), "-- +goose Down")
	require.Len(t, parts, 2)

	cleanSQL := func(s string) string {
		s = strings.ReplaceAll(s, "-- +goose Up", "")
		s = strings.ReplaceAll(s, "-- +goose StatementBegin", "")
		s = strings.ReplaceAll(s, "-- +goose StatementEnd", "")
		return strings.TrimSpace(s)
	}

	upSQL := cleanSQL(parts[0])
	downSQL := cleanSQL(parts[1])

	t.Run("单根环境下存量孤儿文件可安全回填至唯一根", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		// 清理事务内的表数据，确保单根环境无任何干扰
		_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
		require.NoError(t, err)

		disk1ID := newTestUUID()
		diskRoot1 := int64(1)
		dirIno := int64(20)
		fileIno := int64(21)
		orphanIsolatedIno := int64(22)

		// 1. 创建单根盘 1 与根节点 (root_ino 初始为 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $1, $2, 2, 1000, 1000, 0);
		`, diskRoot1, int32(0o040755))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'root-disk-1', $2, 104857600, 0, 1);
		`, disk1ID, diskRoot1)
		require.NoError(t, err)

		// 2. 创建普通子目录与文件 (root_ino 初始为 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, dirIno, diskRoot1, int32(0o040755), fileIno, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdir', $2),
			       ($2, 'child.txt', $3);
		`, diskRoot1, dirIno, fileIno)
		require.NoError(t, err)

		// 3. 创建存量孤立孤儿文件 (无 entries，parent_ino = 0，root_ino = 0)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, 0, $2, 0, 1000, 1000, 0);
		`, orphanIsolatedIno, int32(0o100644))
		require.NoError(t, err)

		// 4. 执行 Up 迁移
		_, err = tx.Exec(ctx, upSQL)
		require.NoError(t, err)

		// 5. 验证单根环境下孤儿节点及常规节点均成功回填为 root_ino = 1
		var r1, rDir, rFile, rIsolated int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", diskRoot1).Scan(&r1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, r1)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", dirIno).Scan(&rDir)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rDir)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", fileIno).Scan(&rFile)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rFile)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIsolatedIno).Scan(&rIsolated)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rIsolated, "单根环境下孤立孤儿文件必须安全回填为唯一根 1")

		// 6. 验证 disks.used_inodes 统计
		var usedInodes1 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk1ID).Scan(&usedInodes1)
		require.NoError(t, err)
		assert.Equal(t, int64(4), usedInodes1, "根盘 1 的 used_inodes 应包含根、子目录、子文件与孤立孤儿共 4 个 inode")
	})

	t.Run("多根环境下子盘边界截断与明确父目录孤儿正常回填", func(t *testing.T) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
		require.NoError(t, err)

		disk1ID := newTestUUID()
		disk10ID := newTestUUID()

		diskRoot1 := int64(1)
		subdiskRoot10 := int64(10)
		subdiskChild11 := int64(11)
		dirIno := int64(20)
		fileIno := int64(21)
		orphanIno1 := int64(22)
		orphanIno2 := int64(23)
		orphanIno10 := int64(12)

		// 1. 创建根盘 1 与根节点
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $1, $2, 2, 1000, 1000, 0);
		`, diskRoot1, int32(0o040755))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'root-disk-1', $2, 104857600, 0, 1);
		`, disk1ID, diskRoot1)
		require.NoError(t, err)

		// 2. 创建挂载在根 1 下的子盘根 10 及其磁盘记录，以及挂在 10 下的文件 11
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, subdiskRoot10, diskRoot1, int32(0o040755), subdiskChild11, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'sub-disk-10', $2, 104857600, 0, 1);
		`, disk10ID, subdiskRoot10)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdisk', $2),
			       ($2, 'child11.txt', $3);
		`, diskRoot1, subdiskRoot10, subdiskChild11)
		require.NoError(t, err)

		// 3. 创建挂在根 1 下的普通目录与孙文件
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 2, 1000, 1000, 0),
			       ($4, $1, $5, 1, 1000, 1000, 0);
		`, dirIno, diskRoot1, int32(0o040755), fileIno, int32(0o100644))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, `
			INSERT INTO entries (parent_ino, name, ino)
			VALUES ($1, 'subdir', $2),
			       ($2, 'child.txt', $3);
		`, diskRoot1, dirIno, fileIno)
		require.NoError(t, err)

		// 4. 创建具有明确 parent_ino 的孤儿目录（无 entries）：
		// orphanIno1: parent_ino = dirIno (归属于根 1)
		// orphanIno2: parent_ino = orphanIno1 (二级孤儿目录，沿链归属于根 1)
		// orphanIno10: parent_ino = subdiskRoot10 (归属于子盘 10)
		_, err = tx.Exec(ctx, `
			INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
			VALUES ($1, $2, $3, 0, 1000, 1000, 0),
			       ($4, $1, $3, 0, 1000, 1000, 0),
			       ($5, $6, $3, 0, 1000, 1000, 0);
		`, orphanIno1, dirIno, int32(0o040755), orphanIno2, orphanIno10, subdiskRoot10)
		require.NoError(t, err)

		// 5. 执行 Up 迁移
		_, err = tx.Exec(ctx, upSQL)
		require.NoError(t, err)

		// 6. 验证子盘根 10 及其子节点 11 不会被根 1 覆盖，其 root_ino 均为 10
		var r10, r11, rOrphan10 int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", subdiskRoot10).Scan(&r10)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, r10, "挂载在根 1 下的子盘根 10 的 root_ino 必须为 10，不能被根 1 覆盖")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", subdiskChild11).Scan(&r11)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, r11, "子盘下的子节点 11 的 root_ino 必须为 10")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno10).Scan(&rOrphan10)
		require.NoError(t, err)
		assert.Equal(t, subdiskRoot10, rOrphan10, "子盘下的孤儿目录应继承父节点 10 的 root_ino")

		// 7. 验证根 1 树节点归属
		var r1, rDir, rFile, rOrphan1, rOrphan2 int64
		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", diskRoot1).Scan(&r1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, r1)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", dirIno).Scan(&rDir)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rDir)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", fileIno).Scan(&rFile)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rFile)

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno1).Scan(&rOrphan1)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rOrphan1, "根 1 下的一级孤儿目录应继承父目录的 root_ino")

		err = tx.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = $1", orphanIno2).Scan(&rOrphan2)
		require.NoError(t, err)
		assert.Equal(t, diskRoot1, rOrphan2, "二级孤儿目录应沿 parent_ino 链递归继承父孤儿目录的 root_ino")

		// 8. 验证 disks.used_inodes 统计
		var usedInodes10 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk10ID).Scan(&usedInodes10)
		require.NoError(t, err)
		assert.Equal(t, int64(3), usedInodes10, "子盘 10 应包含节点 10、11 及孤儿目录 12，共 3 个 inode")

		var usedInodes1 int64
		err = tx.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE disk_id = $1", disk1ID).Scan(&usedInodes1)
		require.NoError(t, err)
		assert.Equal(t, int64(5), usedInodes1, "根盘 1 的 used_inodes 应包含节点 1、20、21 以及孤儿目录 22、23，共 5 个 inode")
	})

	t.Run("多根环境下存在归属不明孤儿或跨盘硬链接时阻止迁移并报错", func(t *testing.T) {
		t.Run("多根环境下存在 parent_ino = 0 且无 entries 的孤立孤儿时阻止迁移", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer func() {
				_ = tx.Rollback(context.Background())
			}()

			_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
			require.NoError(t, err)

			// 插入根盘 1 与子盘 10
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (1, 1, $1, 2, 1000, 1000, 0),
				       (10, 1, $1, 2, 1000, 1000, 0);
			`, int32(0o040755))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
				VALUES ($1, 'disk-1', 1, 104857600, 0, 1),
				       ($2, 'disk-10', 10, 104857600, 0, 1);
			`, newTestUUID(), newTestUUID())
			require.NoError(t, err)

			// 插入归属不明的存量孤儿 (parent_ino = 0，无 entries)
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (99, 0, $1, 0, 1000, 1000, 0);
			`, int32(0o100644))
			require.NoError(t, err)

			// 执行迁移应被拒绝
			_, err = tx.Exec(ctx, upSQL)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "migration blocked: found inodes with ambiguous or unresolvable root_ino ownership")
		})

		t.Run("多根环境下存在跨磁盘硬链接导致多根可达歧义时阻止迁移", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			require.NoError(t, err)
			defer func() {
				_ = tx.Rollback(context.Background())
			}()

			_, err = tx.Exec(ctx, "DELETE FROM entries; DELETE FROM inodes; DELETE FROM disks;")
			require.NoError(t, err)

			// 插入根盘 1 与子盘 10
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (1, 1, $1, 2, 1000, 1000, 0),
				       (10, 10, $1, 2, 1000, 1000, 0);
			`, int32(0o040755))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
				VALUES ($1, 'disk-1', 1, 104857600, 0, 1),
				       ($2, 'disk-10', 10, 104857600, 0, 1);
			`, newTestUUID(), newTestUUID())
			require.NoError(t, err)

			// 插入跨盘硬链接文件 50（无唯一定义父节点，parent_ino = 0），分别在盘 1 和盘 10 下拥有 entry
			_, err = tx.Exec(ctx, `
				INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
				VALUES (50, 0, $1, 2, 1000, 1000, 0);
			`, int32(0o100644))
			require.NoError(t, err)

			_, err = tx.Exec(ctx, `
				INSERT INTO entries (parent_ino, name, ino)
				VALUES (1, 'link_in_root1', 50),
				       (10, 'link_in_disk10', 50);
			`)
			require.NoError(t, err)

			// 执行迁移应因跨盘硬链接歧义被拒绝
			_, err = tx.Exec(ctx, upSQL)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "migration blocked: found inodes with ambiguous or unresolvable root_ino ownership")
		})
	})

	t.Run("Down 回滚保护测试：保留 disks 存量记录与表结构，仅还原约束与新增列", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM disks WHERE disk_id = $1", diskID)
		})

		// 插入存量磁盘数据
		_, err := pool.Exec(ctx, `
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
			VALUES ($1, 'rollback-safe-disk', $2, 50000000, 1024, 2);
		`, diskID, rootIno)
		require.NoError(t, err)

		// 执行 Down 回滚逻辑（在隔离事务中验证，验证后回滚该事务以保持测试库处于最新 Up 状态）
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = tx.Rollback(context.Background())
		}()

		_, err = tx.Exec(ctx, downSQL)
		require.NoError(t, err)

		// 验证 disks 表依然存在，且存量记录完整保留
		var name string
		var maxBytes, usedBytes int64
		err = tx.QueryRow(ctx, "SELECT name, max_bytes, used_bytes FROM disks WHERE disk_id = $1", diskID).Scan(&name, &maxBytes, &usedBytes)
		require.NoError(t, err)
		assert.Equal(t, "rollback-safe-disk", name)
		assert.Equal(t, int64(50000000), maxBytes)
		assert.Equal(t, int64(1024), usedBytes)

		// 验证 used_inodes 列已被删除
		var columnExists bool
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_name = 'disks' AND column_name = 'used_inodes'
			);
		`).Scan(&columnExists)
		require.NoError(t, err)
		assert.False(t, columnExists, "Down 回滚后 used_inodes 列应不存在")
	})
}
