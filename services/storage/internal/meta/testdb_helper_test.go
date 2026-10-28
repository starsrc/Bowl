package meta_test

import (
	"context"
	"os"
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

// resolveAndConnectTestDB 统一解析并连接 PostgreSQL 测试库，严格遵循显式环境变量配置故障硬失败契约。
func resolveAndConnectTestDB(ctx context.Context, fatalf func(string, ...any), skipf func(string, ...any)) (*pgxpool.Pool, string) {
	for _, envName := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "TEST_DB_DSN"} {
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
