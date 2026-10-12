package db_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

func getTestDSN() string {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	return dsn
}

func TestMigrateUp_Success(t *testing.T) {
	dsn := getTestDSN()

	err := db.MigrateUp(dsn)
	require.NoError(t, err, "goose migrations up should succeed on local postgres")

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	// Verify tables exist by querying information_schema
	var tableName string
	err = pool.QueryRow(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'disks'").Scan(&tableName)
	require.NoError(t, err)
	assert.Equal(t, "disks", tableName)

	err = pool.QueryRow(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'disk_attachments'").Scan(&tableName)
	require.NoError(t, err)
	assert.Equal(t, "disk_attachments", tableName)

	// 验证 disk_cleanup_tasks 数据表已成功创建
	err = pool.QueryRow(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'disk_cleanup_tasks'").Scan(&tableName)
	require.NoError(t, err)
	assert.Equal(t, "disk_cleanup_tasks", tableName)

	// Verify indexes exist on disks table
	var idxStatusCreatedAt string
	err = pool.QueryRow(ctx, "SELECT indexname FROM pg_indexes WHERE tablename = 'disks' AND indexname = 'idx_disks_status_created_at'").Scan(&idxStatusCreatedAt)
	require.NoError(t, err)
	assert.Equal(t, "idx_disks_status_created_at", idxStatusCreatedAt)

	var idxCreatedAt string
	err = pool.QueryRow(ctx, "SELECT indexname FROM pg_indexes WHERE tablename = 'disks' AND indexname = 'idx_disks_created_at'").Scan(&idxCreatedAt)
	require.NoError(t, err)
	assert.Equal(t, "idx_disks_created_at", idxCreatedAt)

	// 验证 disk_cleanup_tasks 表的 status 索引已成功创建
	var idxCleanupStatus string
	err = pool.QueryRow(ctx, "SELECT indexname FROM pg_indexes WHERE tablename = 'disk_cleanup_tasks' AND indexname = 'idx_disk_cleanup_tasks_status'").Scan(&idxCleanupStatus)
	require.NoError(t, err)
	assert.Equal(t, "idx_disk_cleanup_tasks_status", idxCleanupStatus)
}
