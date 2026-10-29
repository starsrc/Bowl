package meta_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// testSliceGCAdvisoryLockKey 是跨测试包隔离 slice_refs 全表 GC 与并发切片测试的固定 64 位 PostgreSQL 咨询锁键。
const testSliceGCAdvisoryLockKey int64 = 0x424f574c5f474301

// getStorageTestDSN 解析并返回测试 PostgreSQL DSN，供尚未迁移至 setupMetaTestDB 的包内历史测试文件调用。
func getStorageTestDSN() string {
	for _, env := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "DATABASE_URL", "BOWL_TEST_POSTGRES_DSN", "TEST_DB_DSN"} {
		if val := strings.TrimSpace(os.Getenv(env)); val != "" {
			return val
		}
	}

	candidates := []string{
		"postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable",
		"postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable",
	}

	for _, candidate := range candidates {
		probeCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		pool, err := pgxpool.New(probeCtx, candidate)
		if err == nil {
			pingErr := pool.Ping(probeCtx)
			pool.Close()
			cancel()
			if pingErr == nil {
				return candidate
			}
		} else {
			cancel()
		}
	}

	return "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"
}

// resolveAndConnectTestDB 统一解析并连接 PostgreSQL 测试库，严格遵循显式环境变量配置故障硬失败契约。
func resolveAndConnectTestDB(ctx context.Context, fatalf func(string, ...any), skipf func(string, ...any)) (*pgxpool.Pool, string) {
	for _, envName := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "DATABASE_URL", "BOWL_TEST_POSTGRES_DSN", "TEST_DB_DSN"} {
		rawVal := strings.TrimSpace(os.Getenv(envName))
		if rawVal != "" {
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			poolCfg, err := pgxpool.ParseConfig(rawVal)
			if err != nil {
				fatalf("显式指定的数据库 DSN (%s) 配置解析失败: %v", envName, err)
				return nil, ""
			}
			pool, err := pgxpool.NewWithConfig(probeCtx, poolCfg)
			if err != nil {
				fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
				return nil, ""
			}
			if err := pool.Ping(probeCtx); err != nil {
				pool.Close()
				fatalf("显式指定的数据库 DSN (%s) 连接失败: %v", envName, err)
				return nil, ""
			}
			return pool, rawVal
		}
	}

	candidates := []string{
		"postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable",
		"postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable",
	}

	for _, cand := range candidates {
		probeCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		poolCfg, err := pgxpool.ParseConfig(cand)
		if err == nil {
			pool, err := pgxpool.NewWithConfig(probeCtx, poolCfg)
			if err == nil {
				if pingErr := pool.Ping(probeCtx); pingErr == nil {
					cancel()
					return pool, cand
				}
				pool.Close()
			}
		}
		cancel()
	}

	if strings.TrimSpace(os.Getenv("BOWL_FORMAL_REQUIRE_DB")) == "1" {
		fatalf("未探测到可用 PostgreSQL 实例 (9432/5432 均不可达) 且设置了 BOWL_FORMAL_REQUIRE_DB=1")
		return nil, ""
	}

	skipf("跳过 PostgreSQL 测试 (未显式配置 DSN 且 127.0.0.1:9432 和 127.0.0.1:5432 均不可达)")
	return nil, ""
}

// setupMetaTestDB 建立当前测试用例专用的 PostgreSQL 连接池，并在测试结束时自动回收。
func setupMetaTestDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	pool, _ := resolveAndConnectTestDB(ctx, t.Fatalf, t.Skipf)
	if pool == nil {
		return nil, nil
	}
	t.Cleanup(func() {
		pool.Close()
	})
	return pool, ctx
}

// setupIsolatedMetaTestDB 为需要验证全表扫描或空表分页的用例创建独立的临时数据库，
// 自动应用全部迁移，并在测试结束时按 LIFO 顺序先关闭测试连接池、再强制销毁临时数据库。
func setupIsolatedMetaTestDB(t *testing.T, prefix string) (*pgxpool.Pool, context.Context) {
	t.Helper()
	adminPool, ctx := setupMetaTestDB(t)

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	_, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err, "创建独立临时数据库 %s 失败", dbName)

	cfg := adminPool.Config().Copy()
	cfg.ConnConfig.Database = dbName

	isoPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _ = adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		cancel()
		require.NoError(t, err, "连接独立临时数据库 %s 失败", dbName)
	}

	// LIFO 顺序：先注册 DROP DATABASE（在 isoPool.Close 之后、adminPool.Close 之前执行）
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, dropErr := adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		require.NoError(t, dropErr, "销毁独立临时数据库 %s 失败", dbName)
	})

	// 后注册 isoPool.Close()，使其在 DROP DATABASE 及所有更早注册的 t.Cleanup 之前或按序执行
	t.Cleanup(func() {
		isoPool.Close()
	})

	applyAllStorageMigrations(t, ctx, isoPool)
	return isoPool, ctx
}

// applyAllStorageMigrations 读取并按文件名升序在目标连接池上执行 services/storage/db/migrations 下的全部 Up 迁移。
func applyAllStorageMigrations(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	migrationsDir := "../../db/migrations"
	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err, "读取迁移目录 %s 失败", migrationsDir)

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	slices.Sort(files)

	for _, name := range files {
		raw, err := os.ReadFile(filepath.Join(migrationsDir, name))
		require.NoError(t, err, "读取迁移文件 %s 失败", name)
		parts := strings.Split(string(raw), "-- +goose Down")
		upPart := parts[0]
		if idx := strings.Index(upPart, "-- +goose Up"); idx >= 0 {
			upPart = upPart[idx+len("-- +goose Up"):]
		}
		_, err = pool.Exec(ctx, upPart)
		require.NoError(t, err, "执行迁移 %s 失败", name)
	}
}

// acquireTestSliceGCAdvisoryLock 在专用连接上获取全表 GC 咨询锁，测试退出时自动释放连接与锁。
func acquireTestSliceGCAdvisoryLock(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	conn, err := pool.Acquire(context.Background())
	require.NoError(t, err, "获取专用连接以加 GC 咨询锁失败")

	_, err = conn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", testSliceGCAdvisoryLockKey)
	require.NoError(t, err, "加会话级 GC 咨询锁失败")

	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", testSliceGCAdvisoryLockKey)
		conn.Release()
	})
}

// cleanupTestInos 清理测试创建的 Inode 与关联的 Chunks 及 SliceRefs。
func cleanupTestInos(ctx context.Context, q db.Querier, inos ...int64) {
	if len(inos) == 0 {
		return
	}
	_ = meta.DeleteAllChunksByInos(ctx, q, inos)
	for _, ino := range inos {
		_ = q.DeleteInode(ctx, ino)
	}
}

// TestExplicitInvalidDSNFailsFast 验证显式配置错误 DSN 时立即触发 fatalf 硬失败，绝不静默 Skip 或回退尝试其他端口。
func TestExplicitInvalidDSNFailsFast(t *testing.T) {
	t.Setenv("STORAGE_TEST_DSN", "")
	t.Setenv("TEST_DB_DSN", "")
	t.Setenv("STORAGE_DB_DSN", "postgres://postgres:postgres@127.0.0.1:1/bowl?sslmode=disable&connect_timeout=1")

	var fatalCalls, skipCalls int
	fatalf := func(format string, args ...any) {
		fatalCalls++
	}
	skipf := func(format string, args ...any) {
		skipCalls++
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	pool, _ := resolveAndConnectTestDB(ctx, fatalf, skipf)
	if pool != nil {
		pool.Close()
	}

	assert.Equal(t, 1, fatalCalls, "显式指定的非法 DSN 必须触发且仅触发 1 次 fatalf")
	assert.Equal(t, 0, skipCalls, "显式指定的非法 DSN 严禁触发 skipf")
}
