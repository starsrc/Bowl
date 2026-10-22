package disk_test

import (
	"os"
	"path/filepath"
	"testing"

	_ "github.com/jackc/pgx/v5"
	_ "github.com/starsrc/Aether/pkg/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "go.uber.org/fx"
	_ "google.golang.org/genproto/googleapis/rpc/status"
	_ "google.golang.org/grpc"
)

func TestDiskModuleConfiguration(t *testing.T) {
	// 验证 services/disk 模块元数据定义
	goModBytes, err := os.ReadFile("go.mod")
	require.NoError(t, err)
	assert.Contains(t, string(goModBytes), "module github.com/starsrc/bowl/services/disk")

	// 验证 sqlc 配置文件与代码生成规则
	sqlcBytes, err := os.ReadFile("sqlc.yaml")
	require.NoError(t, err)
	assert.Contains(t, string(sqlcBytes), "package: \"db\"")
	assert.Contains(t, string(sqlcBytes), "out: \"internal/db\"")

	// 验证 mockery 配置文件与变参展开规范
	mockeryBytes, err := os.ReadFile(".mockery.yml")
	require.NoError(t, err)
	assert.Contains(t, string(mockeryBytes), "unroll-variadic: true")
}

func TestDisksGooseMigration(t *testing.T) {
	migrationPath := filepath.Join("db", "migrations", "00001_create_disks.sql")
	contentBytes, err := os.ReadFile(migrationPath)
	require.NoError(t, err)

	content := string(contentBytes)

	// 验证 Up 迁移语句与核心约束
	assert.Contains(t, content, "-- +goose Up")
	assert.Contains(t, content, "CREATE TABLE IF NOT EXISTS disks")
	assert.Contains(t, content, "disk_id UUID PRIMARY KEY")
	assert.Contains(t, content, "root_ino BIGINT NOT NULL UNIQUE")
	assert.Contains(t, content, "max_bytes BIGINT NOT NULL")
	assert.Contains(t, content, "used_bytes BIGINT NOT NULL DEFAULT 0")
	assert.Contains(t, content, "CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0)")
	assert.Contains(t, content, "CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)")

	// 验证 Down 迁移语句
	assert.Contains(t, content, "-- +goose Down")
	assert.Contains(t, content, "DROP TABLE IF EXISTS disks;")
}
