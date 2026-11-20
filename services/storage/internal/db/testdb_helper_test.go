package db_test

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const defaultMetaTestDSN = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"

var testSchemaSeq atomic.Int64

func resolveMetaTestDSN() string {
	return cmp.Or(
		os.Getenv("STORAGE_TEST_DSN"),
		os.Getenv("STORAGE_DB_DSN"),
		os.Getenv("DATABASE_URL"),
		os.Getenv("BOWL_TEST_POSTGRES_DSN"),
		os.Getenv("TEST_DB_DSN"),
		defaultMetaTestDSN,
	)
}

func setupMetaTestDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	dsn := resolveMetaTestDSN()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err, "解析测试数据库 DSN 失败: %s", dsn)

	adminPool, err := pgxpool.NewWithConfig(ctx, adminCfg)
	require.NoError(t, err, "创建测试数据库管理连接池失败")
	t.Cleanup(adminPool.Close)
	require.NoError(t, adminPool.Ping(ctx), "连接测试数据库失败（请确认 PostgreSQL 与迁移已就绪）: %s", dsn)

	schema := fmt.Sprintf("test_db_%d_%d", time.Now().UnixNano(), testSchemaSeq.Add(1))
	_, err = adminPool.Exec(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err, "创建隔离 schema %s 失败", schema)
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, dropErr := adminPool.Exec(dropCtx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
		require.NoError(t, dropErr, "清理隔离 schema %s 失败", schema)
	})

	cloneDDL := fmt.Sprintf(`
		CREATE TABLE %[1]s.disk_lineages (LIKE public.disk_lineages INCLUDING ALL);
		CREATE TABLE %[1]s.disk_commits (LIKE public.disk_commits INCLUDING ALL);
		CREATE TABLE %[1]s.disks (LIKE public.disks INCLUDING ALL);
		CREATE TABLE %[1]s.disk_mounts (LIKE public.disk_mounts INCLUDING ALL);
		ALTER TABLE %[1]s.disk_commits
			ADD CONSTRAINT disk_commits_lineage_id_fkey FOREIGN KEY (lineage_id) REFERENCES %[1]s.disk_lineages(lineage_id) ON DELETE CASCADE,
			ADD CONSTRAINT fk_disk_commits_parent FOREIGN KEY (lineage_id, parent_commit_id) REFERENCES %[1]s.disk_commits(lineage_id, commit_id),
			ADD CONSTRAINT fk_disk_commits_merge_parent FOREIGN KEY (lineage_id, merge_parent_commit_id) REFERENCES %[1]s.disk_commits(lineage_id, commit_id);
		ALTER TABLE %[1]s.disks
			ADD CONSTRAINT fk_disks_lineage FOREIGN KEY (lineage_id) REFERENCES %[1]s.disk_lineages(lineage_id),
			ADD CONSTRAINT fk_disks_base_commit FOREIGN KEY (lineage_id, base_commit_id) REFERENCES %[1]s.disk_commits(lineage_id, commit_id);
		ALTER TABLE %[1]s.disk_mounts
			ADD CONSTRAINT disk_mounts_disk_id_fkey FOREIGN KEY (disk_id) REFERENCES %[1]s.disks(disk_id) ON DELETE CASCADE;
	`, schema)
	_, err = adminPool.Exec(ctx, cloneDDL)
	require.NoError(t, err, "在 schema %s 克隆表结构失败", schema)

	isoCfg := adminPool.Config().Copy()
	if isoCfg.ConnConfig.RuntimeParams == nil {
		isoCfg.ConnConfig.RuntimeParams = make(map[string]string)
	}
	isoCfg.ConnConfig.RuntimeParams["search_path"] = schema

	isoPool, err := pgxpool.NewWithConfig(ctx, isoCfg)
	require.NoError(t, err, "创建隔离连接池失败")
	t.Cleanup(isoPool.Close)
	require.NoError(t, isoPool.Ping(ctx), "探活隔离连接池失败")

	return isoPool, ctx
}
