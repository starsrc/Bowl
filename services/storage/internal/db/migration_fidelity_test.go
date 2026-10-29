package db_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectAdminPostgres 连接 postgres 默认管理库，遵循 resolveAndConnectTestDB 的显式 DSN 硬失败契约。
func connectAdminPostgres(ctx context.Context, fatalf func(string, ...any), skipf func(string, ...any)) (*pgxpool.Pool, string, string) {
	basePool, rawDSN := resolveAndConnectTestDB(ctx, fatalf, skipf)
	if basePool == nil {
		return nil, "", ""
	}
	basePool.Close()

	adminCfg, err := pgxpool.ParseConfig(rawDSN)
	if err != nil {
		fatalf("解析数据库 DSN %q 失败: %v", rawDSN, err)
		return nil, "", ""
	}
	host := fmt.Sprintf("%s:%d", adminCfg.ConnConfig.Host, adminCfg.ConnConfig.Port)
	adminCfg.ConnConfig.Database = "postgres"

	adminPool, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		fatalf("无法连接管理数据库 %s: %v", rawDSN, err)
		return nil, "", ""
	}
	if err := adminPool.Ping(ctx); err != nil {
		adminPool.Close()
		fatalf("管理数据库 %s ping 失败: %v", rawDSN, err)
		return nil, "", ""
	}
	return adminPool, rawDSN, host
}

// createUniqueTempDB 为迁移保真度测试创建独立的临时数据库并在测试结束时强制销毁。
func createUniqueTempDB(t *testing.T, prefix string) (*pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	adminPool, rawDSN, _ := connectAdminPostgres(ctx, t.Fatalf, t.Skipf)
	if adminPool == nil {
		return nil, ""
	}

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	_, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName))
	if err != nil {
		adminPool.Close()
		require.NoError(t, err, "创建独立临时数据库 %s 失败", dbName)
	}

	testCfg, err := pgxpool.ParseConfig(rawDSN)
	if err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		cleanupCancel()
		adminPool.Close()
		require.NoError(t, err, "解析测试数据库配置 %s 失败", dbName)
	}
	testCfg.ConnConfig.Database = dbName

	testPool, err := pgxpool.NewWithConfig(ctx, testCfg)
	if err != nil {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, _ = adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		cleanupCancel()
		adminPool.Close()
		require.NoError(t, err, "连接临时测试数据库 %s 失败", dbName)
	}

	// 注册第一个 t.Cleanup（后进先出，在测试结束且测试连接池关闭后最后执行）
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		defer adminPool.Close()
		_, dropErr := adminPool.Exec(cleanupCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		require.NoError(t, dropErr, "删除独立临时数据库 %s 失败", dbName)
	})

	// 注册第二个 t.Cleanup（后进先出，最先执行，释放所有测试连接）
	t.Cleanup(func() {
		testPool.Close()
	})

	return testPool, dbName
}

// extractGooseSection 提取 migration 文件中的 Up 或 Down 部分。
func extractGooseSection(t *testing.T, filename string, isUp bool) string {
	t.Helper()
	migrationsDir := "../../db/migrations"
	content, err := os.ReadFile(filepath.Join(migrationsDir, filename))
	require.NoError(t, err, "读取迁移文件 %s 失败", filename)

	parts := strings.Split(string(content), "-- +goose Down")
	require.Len(t, parts, 2, "迁移文件 %s 缺少 -- +goose Down 分隔符", filename)

	var target string
	if isUp {
		target = parts[0]
	} else {
		target = parts[1]
	}

	target = strings.ReplaceAll(target, "-- +goose Up", "")
	target = strings.ReplaceAll(target, "-- +goose StatementBegin", "")
	target = strings.ReplaceAll(target, "-- +goose StatementEnd", "")
	return strings.TrimSpace(target)
}

// runMigrationTxInPool 在事务中执行迁移 SQL。
func runMigrationTxInPool(ctx context.Context, pool *pgxpool.Pool, sqlText string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, sqlText); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// TestMigration_PUACharacterPreservedOnUp 验证 G-05：
// BYTEA 迁移脚本 Up 函数严禁误解码 Unicode 私用区字符 (U+E000..U+E0FF)，必须保持原始 UTF-8 字节。
func TestMigration_PUACharacterPreservedOnUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_pua")

	// 1. 依次执行前 3 个迁移的 Up SQL
	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 插入包含合法 Unicode 私用区字符 U+E041 (UTF-8 编码为 \xee\x81\x81，对应 chr(57409))
	setupSQL := `
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev) VALUES (1, 16877, 4096, 2, 0, 0, 0);
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, symlink_target) VALUES (2, 41453, 3, 1, 0, 0, 0, chr(57409));
		INSERT INTO entries (parent_ino, name, ino) VALUES (1, chr(57409), 2);
	`
	_, err := testPool.Exec(ctx, setupSQL)
	require.NoError(t, err)

	// 确认升级前 UTF-8 编码十六进制为 ee8181 (3 字节)
	var beforeHex string
	err = testPool.QueryRow(ctx, "SELECT encode(convert_to(name, 'UTF8'), 'hex') FROM entries WHERE ino = 2;").Scan(&beforeHex)
	require.NoError(t, err)
	require.Equal(t, "ee8181", beforeHex, "升级前 entries.name 必须为 3 字节 UTF-8 码值 ee8181")

	// 3. 执行 20261017010000 与 20261017020000 的 Up SQL
	upSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSymlink)
	require.NoError(t, err, "执行 20261017010000 Up 迁移失败")

	upEntries := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upEntries)
	require.NoError(t, err, "执行 20261017020000 Up 迁移失败")

	// 4. 正确性断言（触发真红）：
	// 升级为 BYTEA 后，原始 UTF-8 字节必须无损保持 "ee8181"！
	// 当前迁移脚本误将 U+E041 判定为回滚转义字符并扣减 57344，被静默篡改为 ASCII 'A' ("41")，断言直接报红！
	var gotNameHex, gotSymHex string
	err = testPool.QueryRow(ctx, "SELECT encode(name, 'hex') FROM entries WHERE ino = 2;").Scan(&gotNameHex)
	require.NoError(t, err)
	err = testPool.QueryRow(ctx, "SELECT encode(symlink_target, 'hex') FROM inodes WHERE ino = 2;").Scan(&gotSymHex)
	require.NoError(t, err)

	assert.Equal(t, "ee8181", gotNameHex, "G-05: 迁移升级后 entries.name 应保持原始 UTF-8 字节 ee8181")
	assert.Equal(t, "ee8181", gotSymHex, "G-05: 迁移升级后 inodes.symlink_target 应保持原始 UTF-8 字节 ee8181")
}

// TestMigration_SymlinkTargetDownUpRoundtripNoLoss 验证 G-06：
// symlink_target Down 回滚函数严禁截断超 4096 UTF-8 字节的编码字符串造成不可逆数据丢失。
func TestMigration_SymlinkTargetDownUpRoundtripNoLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_trunc")

	// 1. 依次执行前 3 个迁移的 Up SQL
	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 执行 20261017010000 的 Up SQL
	upSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", true)
	downSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", false)
	err := runMigrationTxInPool(ctx, testPool, upSymlink)
	require.NoError(t, err, "执行 20261017010000 Up 迁移失败")

	// 3. 插入长度为 2000 字节的合法非 UTF-8 符号链接目标 (2000 个 \x80，完全符合 PATH_MAX 契约)
	insertSQL := `
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, symlink_target)
		VALUES (3, 41453, 2000, 1, 0, 0, 0, decode(repeat('80', 2000), 'hex'));
	`
	_, err = testPool.Exec(ctx, insertSQL)
	require.NoError(t, err)

	var initialLen int64
	err = testPool.QueryRow(ctx, "SELECT octet_length(symlink_target) FROM inodes WHERE ino = 3;").Scan(&initialLen)
	require.NoError(t, err)
	require.Equal(t, int64(2000), initialLen, "初始符号链接目标长度必须为 2000 字节")

	// 4. 执行 Down SQL
	err = runMigrationTxInPool(ctx, testPool, downSymlink)
	require.NoError(t, err, "执行 20261017010000 Down 迁移失败")

	// 5. 重新执行 Up SQL 完成往返
	err = runMigrationTxInPool(ctx, testPool, upSymlink)
	require.NoError(t, err, "重新执行 Up SQL 失败")

	// 6. 断言（触发真红）：
	// 往返后符号链接目标字节长度必须保持 2000 字节无损！
	// 当前生产脚本截断至 1365 字节，断言直接报红！
	var gotLen int64
	err = testPool.QueryRow(ctx, "SELECT octet_length(symlink_target) FROM inodes WHERE ino = 3;").Scan(&gotLen)
	require.NoError(t, err)
	assert.Equal(t, int64(2000), gotLen, "G-06: Down -> Up 往返后符号链接目标字节长度必须保持 2000 字节无损")

	var gotHex string
	err = testPool.QueryRow(ctx, "SELECT encode(symlink_target, 'hex') FROM inodes WHERE ino = 3;").Scan(&gotHex)
	require.NoError(t, err)
	assert.Equal(t, strings.Repeat("80", 2000), gotHex, "G-06: Down -> Up 往返后符号链接目标的完整字节内容必须 100% 保真")
}

// TestMigration_NestedDiskDetachesCrossDiskEntryOnUp 验证原 E2E H9 存量嵌套磁盘迁移场景：
// 20261023000000_add_disks_and_inode_root_ino.sql 在 Up 时自动解除父盘指向子盘根的跨盘 entries 记录、
// 递减父目录 nlink，并将子盘根 inodes.parent_ino 修正为自指 ino，防止存量嵌套子盘在父盘 Rmdir 时被误摧毁。
func TestMigration_NestedDiskDetachesCrossDiskEntryOnUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_nested")

	// 1. 依次执行前置迁移
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
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 模拟升级前存量环境：disks 表已存在，父盘挂载子盘根目录项
	setupSQL := `
		CREATE TABLE IF NOT EXISTS disks (
			disk_id UUID PRIMARY KEY,
			name VARCHAR(255) NOT NULL DEFAULT '',
			root_ino BIGINT NOT NULL UNIQUE,
			max_bytes BIGINT NOT NULL,
			used_bytes BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
			CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)
		);

		-- 父盘根节点 (ino=1, parent_ino=1, nlink=3，包含子目录项 subdisk)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (1, 16877, 4096, 3, 0, 0, 0, 1);
		INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('11111111-1111-1111-1111-111111111111', 'parent-disk', 1, 104857600);

		-- 存量嵌套子盘根节点 (ino=10, 错误关联到父盘 parent_ino=1, nlink=2)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (10, 16877, 4096, 2, 0, 0, 0, 1);
		INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('22222222-2222-2222-2222-222222222222', 'nested-child-disk', 10, 52428800);

		-- 父盘中指向子盘根的跨盘目录项
		INSERT INTO entries (parent_ino, name, ino)
		VALUES (1, convert_to('subdisk', 'UTF8'), 10);

		-- 子盘内部文件 (ino=11)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (11, 33188, 1024, 1, 0, 0, 0, 10);
		INSERT INTO entries (parent_ino, name, ino)
		VALUES (10, convert_to('child_file.txt', 'UTF8'), 11);
	`
	_, err := testPool.Exec(ctx, setupSQL)
	require.NoError(t, err)

	// 3. 执行 20261023000000 Up 迁移
	upSQL := extractGooseSection(t, "20261023000000_add_disks_and_inode_root_ino.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSQL)
	require.NoError(t, err, "执行 20261023000000 Up 迁移失败")

	// 4. 断言跨盘 entries 已被清理
	var crossEntryCount int64
	err = testPool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE parent_ino = 1 AND ino = 10;").Scan(&crossEntryCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), crossEntryCount, "父盘指向嵌套子盘根的 entries 必须在 Up 迁移中被自动解除")

	// 5. 断言父盘根 nlink 扣减 1 (3 -> 2)
	var parentNlink int32
	err = testPool.QueryRow(ctx, "SELECT nlink FROM inodes WHERE ino = 1;").Scan(&parentNlink)
	require.NoError(t, err)
	assert.Equal(t, int32(2), parentNlink, "解开子盘目录项后父盘根目录 nlink 必须从 3 减为 2")

	// 6. 断言子盘根 parent_ino 已被修正为自指 (10 -> 10)
	var childParentIno int64
	err = testPool.QueryRow(ctx, "SELECT parent_ino FROM inodes WHERE ino = 10;").Scan(&childParentIno)
	require.NoError(t, err)
	assert.Equal(t, int64(10), childParentIno, "子盘根 parent_ino 必须被自动修正为自指 ino=10")

	// 7. 断言 root_ino 回填正确且互不污染
	var rootIno1, rootIno10, rootIno11 int64
	err = testPool.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = 1;").Scan(&rootIno1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rootIno1)

	err = testPool.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = 10;").Scan(&rootIno10)
	require.NoError(t, err)
	assert.Equal(t, int64(10), rootIno10)

	err = testPool.QueryRow(ctx, "SELECT root_ino FROM inodes WHERE ino = 11;").Scan(&rootIno11)
	require.NoError(t, err)
	assert.Equal(t, int64(10), rootIno11)

	// 8. 断言 used_inodes 统计
	var usedInodes1, usedInodes2 int64
	err = testPool.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE root_ino = 1;").Scan(&usedInodes1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), usedInodes1)

	err = testPool.QueryRow(ctx, "SELECT used_inodes FROM disks WHERE root_ino = 10;").Scan(&usedInodes2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), usedInodes2)
}

// TestMigration_DisksAndInodeRootIno_UpDownUpFidelity 验证 G-03：
// 嵌套磁盘迁移在同一语句中对中间层磁盘根 Inode 更新两次导致修复丢失。
// 构造两层嵌套磁盘结构：根盘 1 (ino=1) -> 中间层子盘 A (ino=10, parent_ino=1, nlink=3) -> 孙盘 B (ino=20, parent_ino=10, nlink=2)。
// 执行 20261023000000_add_disks_and_inode_root_ino.sql 的 Up 迁移后，
// 中间层子盘 A (ino=10) 必须同时满足：
// 1) parent_ino == 10（自指根，因为自身也是独立磁盘根）
// 2) nlink == 2（因为原本挂载在其下的孙盘 B 跨盘目录项被删除，nlink 必须从 3 减为 2）。
// 当前 SQL 在同一 WITH ... UPDATE 语句中同时通过 updated_parents 和顶层 UPDATE 更新 ino=10，必然丢失其中一项修改而触发 FAIL。
func TestMigration_DisksAndInodeRootIno_UpDownUpFidelity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_nest_g03")

	// 1. 依次执行前置迁移
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
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 模拟升级前存量环境：创建 disks 表，并构造两层嵌套磁盘结构：
	// 根盘 1 (ino=1) -> 中间层子盘 A (ino=10, parent_ino=1, nlink=3) -> 孙盘 B (ino=20, parent_ino=10, nlink=2)
	setupSQL := `
		CREATE TABLE IF NOT EXISTS disks (
			disk_id UUID PRIMARY KEY,
			name VARCHAR(255) NOT NULL DEFAULT '',
			root_ino BIGINT NOT NULL UNIQUE,
			max_bytes BIGINT NOT NULL,
			used_bytes BIGINT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
			CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
			CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)
		);

		-- 根盘 1 (ino=1, parent_ino=1, nlink=3，挂载了 subdisk A)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (1, 16877, 4096, 3, 0, 0, 0, 1);
		INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('11111111-1111-1111-1111-111111111111', 'root-disk', 1, 104857600);

		-- 中间层子盘 A (ino=10, 挂在根盘 parent_ino=1 下，自身挂载了孙盘 B 故 nlink=3)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (10, 16877, 4096, 3, 0, 0, 0, 1);
		INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('22222222-2222-2222-2222-222222222222', 'child-disk-a', 10, 52428800);

		-- 孙盘 B (ino=20, 挂在中间层子盘 parent_ino=10 下，nlink=2)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (20, 16877, 4096, 2, 0, 0, 0, 10);
		INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('33333333-3333-3333-3333-333333333333', 'grandchild-disk-b', 20, 26214400);

		-- entries 跨盘挂载记录
		INSERT INTO entries (parent_ino, name, ino) VALUES (1, convert_to('subdisk_a', 'UTF8'), 10);
		INSERT INTO entries (parent_ino, name, ino) VALUES (10, convert_to('granddisk_b', 'UTF8'), 20);

		-- 孙盘内部正常文件 (ino=21)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino)
		VALUES (21, 33188, 1024, 1, 0, 0, 0, 20);
		INSERT INTO entries (parent_ino, name, ino) VALUES (20, convert_to('file_in_b.txt', 'UTF8'), 21);
	`
	_, err := testPool.Exec(ctx, setupSQL)
	require.NoError(t, err)

	// 3. 执行 20261023000000 Up 迁移
	upSQL := extractGooseSection(t, "20261023000000_add_disks_and_inode_root_ino.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSQL)
	require.NoError(t, err, "执行 20261023000000 Up 迁移失败")

	// 4. 断言中间层子盘 A (ino=10)：必须同时满足 parent_ino == 10 且 nlink == 2！
	var childParentIno int64
	var childNlink int32
	err = testPool.QueryRow(ctx, "SELECT parent_ino, nlink FROM inodes WHERE ino = 10;").Scan(&childParentIno, &childNlink)
	require.NoError(t, err)

	assert.Equal(t, int64(10), childParentIno, "G-03: 中间层磁盘根 A (ino=10) parent_ino 必须被自动修正为自指 10")
	assert.Equal(t, int32(2), childNlink, "G-03: 解除孙盘 B 跨盘目录项后中间层磁盘根 A (ino=10) nlink 必须从 3 减为 2")

	// 5. 校验跨盘 entries 已被完全解除
	var crossEntriesCount int64
	err = testPool.QueryRow(ctx, "SELECT COUNT(*) FROM entries WHERE (parent_ino = 1 AND ino = 10) OR (parent_ino = 10 AND ino = 20);").Scan(&crossEntriesCount)
	require.NoError(t, err)
	assert.Equal(t, int64(0), crossEntriesCount, "所有跨盘 entries 必须被自动清除")

	// 6. 校验根盘 1 与孙盘 B
	var rootNlink int32
	err = testPool.QueryRow(ctx, "SELECT nlink FROM inodes WHERE ino = 1;").Scan(&rootNlink)
	require.NoError(t, err)
	assert.Equal(t, int32(2), rootNlink, "根盘 ino=1 nlink 必须从 3 减为 2")

	var grandParentIno int64
	err = testPool.QueryRow(ctx, "SELECT parent_ino FROM inodes WHERE ino = 20;").Scan(&grandParentIno)
	require.NoError(t, err)
	assert.Equal(t, int64(20), grandParentIno, "孙盘 ino=20 parent_ino 必须自指 20")
}

// TestMigration_EntriesNameToBytea_NonUTF8Preserved 验证 G-04：
// entries 名称 BYTEA 迁移的 Down -> Up 往返保真、同目录多硬链接及编码碰撞。
// 补充执行 20261017020000_convert_entries_name_to_bytea.sql 的 Down 迁移再执行 Up 迁移的往返（Round-trip）测试，
// 并在回滚前插入以下合法目录项数据：
// 1) 同一父目录下指向同一个文件 Inode 的两个不同名称硬链接（例如非 UTF-8 名称 []byte{0xfe, 0x01} 与 []byte{0xfe, 0x02} 指向同一 ino）；
// 2) 同一父目录下指向不同 Inode 的单字节 []byte{0xff} 与 ASCII 字面量 []byte("\\377")；
// 3) 同一父目录下前 255 个转义字符相同但尾部不同的两个长非 UTF-8 名称。
// 断言 Down 与随后的 Up 迁移均不报错（require.NoError），且 Up 完成后所有目录项的 (parent_ino, name, ino) 与回滚前 100% 字节级一致。
func TestMigration_EntriesNameToBytea_NonUTF8Preserved(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_g04")

	// 1. 依次执行前置迁移并执行 20261017020000 Up 迁移
	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
		"20261017010000_convert_symlink_target_to_bytea.sql",
		"20261017020000_convert_entries_name_to_bytea.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 插入测试用的父目录 inode 与各个目标文件 inodes
	setupInodesSQL := `
		-- 父目录 ino=1
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (1, 16877, 4096, 2, 0, 0, 0);

		-- 目标文件 inodes:
		-- ino=100: 用于同父目录多硬链接测试 (nlink=2)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (100, 33188, 1024, 2, 0, 0, 0);

		-- ino=101, 102: 用于编码碰撞测试 (\xff 与 \377)
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (101, 33188, 1024, 1, 0, 0, 0),
		       (102, 33188, 1024, 1, 0, 0, 0);

		-- ino=103, 104: 用于长名称截断碰撞测试
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (103, 33188, 1024, 1, 0, 0, 0),
		       (104, 33188, 1024, 1, 0, 0, 0);
	`
	_, err := testPool.Exec(ctx, setupInodesSQL)
	require.NoError(t, err)

	// 3. 构造 3 类特定测试目录项数据：
	// a. 同父目录下指向同一个文件 Inode (100) 的两个不同名称硬链接：非 UTF-8 名称 []byte{0xfe, 0x01} 与 []byte{0xfe, 0x02}
	linkNameA := []byte{0xfe, 0x01}
	linkNameB := []byte{0xfe, 0x02}

	// b. 同父目录下指向不同 Inode 的单字节 []byte{0xff} 与 ASCII 字面量 []byte("\\377")
	colNameByte := []byte{0xff}
	colNameLit := []byte("\\377")

	// c. 同父目录下前 255 个转义字符相同但尾部不同的两个长非 UTF-8 名称
	// 70 个 0x80 会转义为 70*4=280 字符，取 left(..., 255) 时前 255 字符完全相同
	longNameA := append(bytes.Repeat([]byte{0x80}, 70), 0x01)
	longNameB := append(bytes.Repeat([]byte{0x80}, 70), 0x02)

	type expectedEntry struct {
		parentIno int64
		name      []byte
		ino       int64
	}

	expectedList := []expectedEntry{
		{parentIno: 1, name: linkNameA, ino: 100},
		{parentIno: 1, name: linkNameB, ino: 100},
		{parentIno: 1, name: colNameByte, ino: 101},
		{parentIno: 1, name: colNameLit, ino: 102},
		{parentIno: 1, name: longNameA, ino: 103},
		{parentIno: 1, name: longNameB, ino: 104},
	}

	for _, entry := range expectedList {
		_, err := testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", entry.parentIno, entry.name, entry.ino)
		require.NoError(t, err, "插入测试目录项失败: ino=%d, name=%x", entry.ino, entry.name)
	}

	// 4. 执行 20261017020000 Down 迁移
	downSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", false)
	err = runMigrationTxInPool(ctx, testPool, downSQL)
	require.NoError(t, err, "执行 20261017020000 Down 迁移失败")

	// 5. 重新执行 20261017020000 Up 迁移完成往返
	upSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSQL)
	require.NoError(t, err, "重新执行 20261017020000 Up 迁移失败")

	// 6. 验证往返后所有 entries 的 (parent_ino, name, ino) 100% 字节级一致
	rows, err := testPool.Query(ctx, "SELECT parent_ino, name, ino FROM entries ORDER BY ino, name;")
	require.NoError(t, err)
	defer rows.Close()

	var gotList []expectedEntry
	for rows.Next() {
		var e expectedEntry
		err := rows.Scan(&e.parentIno, &e.name, &e.ino)
		require.NoError(t, err)
		gotList = append(gotList, e)
	}
	require.NoError(t, rows.Err())

	require.Len(t, gotList, len(expectedList), "往返后目录项总数必须完全一致")

	gotMap := make(map[string]bool)
	for _, g := range gotList {
		key := fmt.Sprintf("%d:%d:%x", g.parentIno, g.ino, g.name)
		gotMap[key] = true
	}
	for _, exp := range expectedList {
		key := fmt.Sprintf("%d:%d:%x", exp.parentIno, exp.ino, exp.name)
		assert.True(t, gotMap[key], "往返后必须保真存在目录项: parent_ino=%d, ino=%d, name(hex)=%x", exp.parentIno, exp.ino, exp.name)
	}
}

// TestMigration_EntriesBackupIno_ReplacedEntryNotOverwrittenOnUp 验证 P-6：
// 20261017020000_convert_entries_name_to_bytea.sql 迁移回滚后若同名目录项被删除并替换为指向新 Inode，
// 再次 Up 迁移时不得错误匹配并用已被删除旧 Inode 的 raw_name 覆写新 Inode 的目录项。
func TestMigration_EntriesBackupIno_ReplacedEntryNotOverwrittenOnUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_p6")

	// 1. 依次执行前置迁移并执行 20261017020000 Up 迁移
	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
		"20261017010000_convert_symlink_target_to_bytea.sql",
		"20261017020000_convert_entries_name_to_bytea.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 插入测试用 Inodes
	// 父目录 ino=1
	// 旧文件 old_ino=100
	// 保留文件 keep_ino=101 (用于验证未被替换的正常目录项仍能恢复原始非 UTF-8 字节)
	// 新文件 new_ino=200
	setupInodesSQL := `
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (1, 16877, 4096, 2, 0, 0, 0),
		       (100, 33188, 1024, 1, 0, 0, 0),
		       (101, 33188, 1024, 1, 0, 0, 0),
		       (200, 33188, 1024, 1, 0, 0, 0);
	`
	_, err := testPool.Exec(ctx, setupInodesSQL)
	require.NoError(t, err)

	// 3. 在 Up 状态 (BYTEA 模式) 下插入两个非 UTF-8 原始字节名的目录项
	oldRawName := []byte{0xfe, 0x01}
	keepRawName := []byte{0xfe, 0x02}

	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 1, oldRawName, 100)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 1, keepRawName, 101)
	require.NoError(t, err)

	// 4. 执行 Down 迁移回滚到 VARCHAR 模式
	downSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", false)
	err = runMigrationTxInPool(ctx, testPool, downSQL)
	require.NoError(t, err, "执行 20261017020000 Down 迁移失败")

	// 5. 获取回滚后的转义名称 rollback_name
	var rollbackName string
	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 1 AND ino = 100;").Scan(&rollbackName)
	require.NoError(t, err)
	require.NotEmpty(t, rollbackName)

	var keepRollbackName string
	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 1 AND ino = 101;").Scan(&keepRollbackName)
	require.NoError(t, err)
	require.NotEmpty(t, keepRollbackName)

	// 6. 模拟回滚期间（VARCHAR 模式下）：
	// 删除旧文件 old_ino=100 的目录项，并插入同名 rollback_name 但指向新文件 new_ino=200 的新目录项
	_, err = testPool.Exec(ctx, "DELETE FROM entries WHERE parent_ino = 1 AND ino = 100;")
	require.NoError(t, err)

	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, ino, name) VALUES ($1, $3, $2);", 1, rollbackName, 200)
	require.NoError(t, err)

	// 7. 再次执行 Up 迁移重新升级为 BYTEA 模式
	upSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSQL)
	require.NoError(t, err, "再次执行 20261017020000 Up 迁移失败")

	// 8. 校验结果
	// (a) 对于未替换的 keep_ino=101，应正常恢复非 UTF-8 原始字节 keepRawName
	var gotKeepName []byte
	err = testPool.QueryRow(ctx, "SELECT name::bytea FROM entries WHERE parent_ino = 1 AND ino = 101;").Scan(&gotKeepName)
	require.NoError(t, err)
	assert.Equal(t, keepRawName, gotKeepName, "未替换的目录项 ino=101 应正常恢复原始非 UTF-8 字节")

	// (b) 对于新文件 new_ino=200，其名称必须保持为新插入的字符串 UTF-8 编码字节 convert_to(rollback_name, 'UTF8')，
	// 严禁被已被删除的 old_ino=100 旧文件原始非 UTF-8 字节篡改！
	var gotNewInoName []byte
	err = testPool.QueryRow(ctx, "SELECT name::bytea FROM entries WHERE parent_ino = 1 AND ino = 200;").Scan(&gotNewInoName)
	require.NoError(t, err)

	expectedNewInoName := []byte(rollbackName)
	assert.Equal(t, expectedNewInoName, gotNewInoName, "指向 new_ino=200 的新目录项名称必须保持为新插入名称，不得被旧文件 old_ino=100 的 raw_name 覆写")
}

// TestMigration002_ArbitraryByteaRoundTripAndCollisionIsolation 验证 G1-3：
// entries 名称 BYTEA 迁移在 Down -> Up 往返过程中处理链式回滚名称置换场景时，
// 严禁因单条 UPDATE 执行时的物理更新顺序触发 pk_entries (parent_ino, name) 的 23505 (unique_violation) 错误。
//
// 场景设定：
// 同一 parent_ino 下存在两个条目：
// 1) 非 UTF-8 名称 A（如 []byte{0xff, 0xfe}，ino=102），Down 时被编码为 B = "__bowl_rb_" + sha256(A)；
// 2) 合法 ASCII 名称恰好为 B（ino=101），Down 时因匹配 __bowl_rb_ 前缀被编码为 C = "__bowl_rb_" + sha256(B)。
// Down 完成后，两行在 entries 中的 name 分别为 B (ino=102) 和 C (ino=101)。
// 再次执行 Up 时，若 PostgreSQL 先更新 C -> B，而 B -> A 尚未完成，就会触发 (parent_ino, name) 唯一约束冲突。
// 本测试覆盖不同物理插入与存储顺序（先 B 后 A，以及先 A 后 B），验证 Up -> Down -> Up 完整往返必须成功且数据无损还原。
func TestMigration002_ArbitraryByteaRoundTripAndCollisionIsolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_002")

	// 1. 执行前置迁移至 20261017020000 Up
	earlyMigrations := []string{
		"20261015165500_create_metadata_tables.sql",
		"20261016180000_add_symlink_target_to_inodes.sql",
		"20261017000000_create_file_handles_table.sql",
		"20261017010000_convert_symlink_target_to_bytea.sql",
		"20261017020000_convert_entries_name_to_bytea.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	// 2. 插入测试用 parent_ino 和文件 inodes
	setupSQL := `
		INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev)
		VALUES (10, 16877, 4096, 2, 0, 0, 0),
		       (20, 16877, 4096, 2, 0, 0, 0),
		       (101, 33188, 1024, 1, 0, 0, 0),
		       (102, 33188, 1024, 1, 0, 0, 0),
		       (201, 33188, 1024, 1, 0, 0, 0),
		       (202, 33188, 1024, 1, 0, 0, 0);
	`
	_, err := testPool.Exec(ctx, setupSQL)
	require.NoError(t, err)

	// 构造名称对：
	// A: 非 UTF-8 原始字节
	rawNameA := []byte{0xff, 0xfe}
	shaA := sha256.Sum256(rawNameA)
	nameBStr := fmt.Sprintf("__bowl_rb_%x", shaA)
	rawNameB := []byte(nameBStr)

	// 场景 1 (parent_ino=10): 先插入 B (ino=101)，后插入 A (ino=102)
	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 10, rawNameB, 101)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 10, rawNameA, 102)
	require.NoError(t, err)

	// 场景 2 (parent_ino=20): 先插入 A (ino=201)，后插入 B (ino=202)
	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 20, rawNameA, 201)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, "INSERT INTO entries (parent_ino, name, ino) VALUES ($1, $2, $3);", 20, rawNameB, 202)
	require.NoError(t, err)

	// 3. 执行 Down 迁移回滚为 VARCHAR(255)
	downSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", false)
	err = runMigrationTxInPool(ctx, testPool, downSQL)
	require.NoError(t, err, "执行 20261017020000 Down 迁移失败")

	// 4. 执行 Up 迁移重新升级为 BYTEA 并恢复原始名称
	upSQL := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", true)
	err = runMigrationTxInPool(ctx, testPool, upSQL)
	require.NoError(t, err, "重新执行 20261017020000 Up 迁移失败")

	// 5. 校验两组 entries 均已 100% 保真还原
	var gotName101, gotName102 []byte
	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 10 AND ino = 101;").Scan(&gotName101)
	require.NoError(t, err)
	assert.Equal(t, rawNameB, gotName101, "parent 10 ino 101 名称必须保真还原为 rawNameB")

	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 10 AND ino = 102;").Scan(&gotName102)
	require.NoError(t, err)
	assert.Equal(t, rawNameA, gotName102, "parent 10 ino 102 名称必须保真还原为 rawNameA")

	var gotName201, gotName202 []byte
	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 20 AND ino = 201;").Scan(&gotName201)
	require.NoError(t, err)
	assert.Equal(t, rawNameA, gotName201, "parent 20 ino 201 名称必须保真还原为 rawNameA")

	err = testPool.QueryRow(ctx, "SELECT name FROM entries WHERE parent_ino = 20 AND ino = 202;").Scan(&gotName202)
	require.NoError(t, err)
	assert.Equal(t, rawNameB, gotName202, "parent 20 ino 202 名称必须保真还原为 rawNameB")
}

// TestMigration_AddDiskIDToClientSessions_UpAndDown 验证 20261029010000_add_disk_id_to_client_sessions.sql 的 Up 与 Down 往返正确性及 ON DELETE CASCADE 约束。
func TestMigration_AddDiskIDToClientSessions_UpAndDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testPool, _ := createUniqueTempDB(t, "bowl_mig_sess_disk")

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
		"20261023000000_add_disks_and_inode_root_ino.sql",
		"20261028000000_add_chunks_and_slice_refs.sql",
	}
	for _, f := range earlyMigrations {
		upSQL := extractGooseSection(t, f, true)
		err := runMigrationTxInPool(ctx, testPool, upSQL)
		require.NoError(t, err, "执行前置迁移 %s 失败", f)
	}

	upSQL := extractGooseSection(t, "20261029010000_add_disk_id_to_client_sessions.sql", true)
	downSQL := extractGooseSection(t, "20261029010000_add_disk_id_to_client_sessions.sql", false)

	// 1. 执行 Up 迁移
	require.NoError(t, runMigrationTxInPool(ctx, testPool, upSQL))

	// 验证列与索引存在
	var colExists, idxExists bool
	err := testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'client_sessions' AND column_name = 'disk_id'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	assert.True(t, colExists, "Up 迁移后 client_sessions.disk_id 列必须存在")

	err = testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE tablename = 'client_sessions' AND indexname = 'idx_client_sessions_disk_expires'
		)
	`).Scan(&idxExists)
	require.NoError(t, err)
	assert.True(t, idxExists, "Up 迁移后 idx_client_sessions_disk_expires 索引必须存在")

	// 验证外键 ON DELETE CASCADE：删除磁盘时自动级联清理关联会话
	diskID := "33333333-3333-7333-8333-333333333333"
	_, err = testPool.Exec(ctx, `
		INSERT INTO inodes (ino, parent_ino, mode, nlink, uid, gid, root_ino)
		VALUES (100, 100, 16877, 2, 0, 0, 100);
	`)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
		VALUES ($1, 'cascade-disk', 100, 104857600, 0, 1);
	`, diskID)
	require.NoError(t, err)
	_, err = testPool.Exec(ctx, `
		INSERT INTO client_sessions (session_id, expires_at, updated_at, created_at, disk_id)
		VALUES (9001, clock_timestamp() + interval '10 minutes', clock_timestamp(), clock_timestamp(), $1);
	`, diskID)
	require.NoError(t, err)

	_, err = testPool.Exec(ctx, `DELETE FROM disks WHERE disk_id = $1`, diskID)
	require.NoError(t, err)

	var sessCount int64
	err = testPool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = 9001`).Scan(&sessCount)
	require.NoError(t, err)
	assert.Zero(t, sessCount, "删除磁盘时 ON DELETE CASCADE 必须自动删除关联的 client_sessions 记录")

	// 2. 执行 Down 迁移
	require.NoError(t, runMigrationTxInPool(ctx, testPool, downSQL))

	err = testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'client_sessions' AND column_name = 'disk_id'
		)
	`).Scan(&colExists)
	require.NoError(t, err)
	assert.False(t, colExists, "Down 迁移后 client_sessions.disk_id 列必须已被删除")

	err = testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE tablename = 'client_sessions' AND indexname = 'idx_client_sessions_disk_expires'
		)
	`).Scan(&idxExists)
	require.NoError(t, err)
	assert.False(t, idxExists, "Down 迁移后 idx_client_sessions_disk_expires 索引必须已被删除")
}

func quoteLibpqValue(v string) string {
	escaped := strings.ReplaceAll(v, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `'`, `\'`)
	return "'" + escaped + "'"
}

func buildTestURLDSN(host string, port uint16, user, password, dbName string) string {
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, password),
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     "/" + dbName,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func buildTestKeywordDSN(host string, port uint16, user, password, dbName string) string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		quoteLibpqValue(host),
		port,
		quoteLibpqValue(dbName),
		quoteLibpqValue(user),
		quoteLibpqValue(password),
	)
}

// TestMigration_ConnectAdminAndCreateTempDB_URLAndKeywordDSN 验证 connectAdminPostgres 与 createUniqueTempDB
// 对等价 URL DSN 与 libpq keyword/value DSN（host=... port=... dbname=...）均能正确解析、连接管理库并创建临时库，
// 同时非法端口 DSN 仍保持硬失败。
func TestMigration_ConnectAdminAndCreateTempDB_URLAndKeywordDSN(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	probePool, _ := resolveAndConnectTestDB(ctx, t.Fatalf, t.Skipf)
	if probePool == nil {
		return
	}
	baseCfg := probePool.Config().ConnConfig
	host := baseCfg.Host
	port := baseCfg.Port
	user := baseCfg.User
	password := baseCfg.Password
	dbName := baseCfg.Database
	probePool.Close()

	urlDSN := buildTestURLDSN(host, port, user, password, dbName)
	keywordDSN := buildTestKeywordDSN(host, port, user, password, dbName)

	clearDSNEnvs := func(t *testing.T) {
		for _, env := range []string{"STORAGE_TEST_DSN", "STORAGE_DB_DSN", "DATABASE_URL", "BOWL_TEST_POSTGRES_DSN", "TEST_DB_DSN"} {
			t.Setenv(env, "")
		}
	}

	t.Run("URL DSN 建库与查询保真", func(t *testing.T) {
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", urlDSN)

		tempPool, tempDBName := createUniqueTempDB(t, "bowl_mig_url_dsn")
		require.NotNil(t, tempPool)
		var currentDB string
		require.NoError(t, tempPool.QueryRow(ctx, "SELECT current_database()").Scan(&currentDB))
		assert.Equal(t, tempDBName, currentDB)
	})

	t.Run("Keyword/Value DSN 建库与查询保真", func(t *testing.T) {
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", keywordDSN)

		tempPool, tempDBName := createUniqueTempDB(t, "bowl_mig_kw_dsn")
		require.NotNil(t, tempPool)
		var currentDB string
		require.NoError(t, tempPool.QueryRow(ctx, "SELECT current_database()").Scan(&currentDB))
		assert.Equal(t, tempDBName, currentDB)
	})

	verifySpecialCredentialRoleAndDSNs := func(t *testing.T, creatorDSN string, dropPool *pgxpool.Pool, rolePrefix string) {
		t.Helper()
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", creatorDSN)

		creatorPool, _, _ := connectAdminPostgres(ctx, t.Fatalf, t.Skipf)
		require.NotNil(t, creatorPool)
		t.Cleanup(func() {
			creatorPool.Close()
		})

		roleName := fmt.Sprintf("%s_%d", rolePrefix, time.Now().UnixNano())
		specialPass := `r3 pass'word\test:@#?`
		_, err := creatorPool.Exec(ctx, fmt.Sprintf(
			"CREATE ROLE %s WITH LOGIN CREATEDB PASSWORD E%s",
			pgx.Identifier{roleName}.Sanitize(),
			quoteLibpqValue(specialPass),
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, dropErr := dropPool.Exec(cleanCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{roleName}.Sanitize()))
			require.NoError(t, dropErr)
		})

		specialURLDSN := buildTestURLDSN(host, port, roleName, specialPass, dbName)
		specialKeywordDSN := buildTestKeywordDSN(host, port, roleName, specialPass, dbName)

		t.Run("Special_URL_DSN", func(t *testing.T) {
			clearDSNEnvs(t)
			t.Setenv("STORAGE_TEST_DSN", specialURLDSN)
			tempPool, tempDBName := createUniqueTempDB(t, "bowl_mig_sp_url")
			require.NotNil(t, tempPool)
			var currentDB, currentUser string
			require.NoError(t, tempPool.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&currentDB, &currentUser))
			assert.Equal(t, tempDBName, currentDB)
			assert.Equal(t, roleName, currentUser)
		})

		t.Run("Special_Keyword_DSN", func(t *testing.T) {
			clearDSNEnvs(t)
			t.Setenv("STORAGE_TEST_DSN", specialKeywordDSN)
			tempPool, tempDBName := createUniqueTempDB(t, "bowl_mig_sp_kw")
			require.NotNil(t, tempPool)
			var currentDB, currentUser string
			require.NoError(t, tempPool.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&currentDB, &currentUser))
			assert.Equal(t, tempDBName, currentDB)
			assert.Equal(t, roleName, currentUser)
		})
	}

	t.Run("含空格与特殊字符凭据的 URL 与 Keyword DSN 建库保真", func(t *testing.T) {
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", urlDSN)
		adminPool, _, _ := connectAdminPostgres(ctx, t.Fatalf, t.Skipf)
		require.NotNil(t, adminPool)
		t.Cleanup(func() {
			adminPool.Close()
		})
		verifySpecialCredentialRoleAndDSNs(t, urlDSN, adminPool, "bowl_r3_role")
	})

	t.Run("非超级用户角色（NOSUPERUSER）执行建库与特殊凭据保真", func(t *testing.T) {
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", urlDSN)

		adminPool, _, _ := connectAdminPostgres(ctx, t.Fatalf, t.Skipf)
		require.NotNil(t, adminPool)
		t.Cleanup(func() {
			adminPool.Close()
		})

		nonSuperAdminRole := fmt.Sprintf("bowl_r4_nonsuper_admin_%d", time.Now().UnixNano())
		nonSuperAdminPass := "r4_nonsuper_pass"
		_, err := adminPool.Exec(ctx, fmt.Sprintf(
			"CREATE ROLE %s WITH LOGIN CREATEDB CREATEROLE NOSUPERUSER PASSWORD E%s",
			pgx.Identifier{nonSuperAdminRole}.Sanitize(),
			quoteLibpqValue(nonSuperAdminPass),
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, dropErr := adminPool.Exec(cleanCtx, fmt.Sprintf("DROP ROLE IF EXISTS %s", pgx.Identifier{nonSuperAdminRole}.Sanitize()))
			require.NoError(t, dropErr)
		})

		nonSuperURLDSN := buildTestURLDSN(host, port, nonSuperAdminRole, nonSuperAdminPass, dbName)
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", nonSuperURLDSN)

		nonSuperPool, _, _ := connectAdminPostgres(ctx, t.Fatalf, t.Skipf)
		require.NotNil(t, nonSuperPool)
		t.Cleanup(func() {
			nonSuperPool.Close()
		})

		var isSuper bool
		require.NoError(t, nonSuperPool.QueryRow(ctx, "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&isSuper))
		require.False(t, isSuper, "测试连接角色必须为非超级用户 NOSUPERUSER")

		verifySpecialCredentialRoleAndDSNs(t, nonSuperURLDSN, adminPool, "bowl_r4_sub_role")
	})

	t.Run("非法端口 Keyword DSN 仍硬失败不跳过", func(t *testing.T) {
		clearDSNEnvs(t)
		t.Setenv("STORAGE_TEST_DSN", "host=127.0.0.1 port=1 dbname=bowl user=postgres password=postgres sslmode=disable connect_timeout=1")

		var fatalCalls, skipCalls int
		adminPool, _, _ := connectAdminPostgres(
			ctx,
			func(string, ...any) { fatalCalls++ },
			func(string, ...any) { skipCalls++ },
		)
		if adminPool != nil {
			adminPool.Close()
		}
		assert.Equal(t, 1, fatalCalls, "非法端口 Keyword DSN 必须触发 fatalf")
		assert.Zero(t, skipCalls, "非法端口 Keyword DSN 严禁触发 skipf")
	})
}
