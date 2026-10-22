package db_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
)

var testInoSeq int64 = 9_000_000_000

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
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("跳过 PostgreSQL 测试 (连接初始化失败): %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("跳过 PostgreSQL 测试 (ping 失败): %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return db.NewStore(pool), pool
}

// ==========================================
// 单元与接口测试 (Unit & Interface Tests)
// ==========================================

func TestNewStore(t *testing.T) {
	store := db.NewStore(nil)
	assert.NotNil(t, store)

	var _ db.Store = store
}

func TestModule(t *testing.T) {
	assert.NotNil(t, db.Module)
}

func TestQuerierInterface(t *testing.T) {
	var _ db.Querier = (*db.Queries)(nil)
	var _ db.Querier = (db.Store)(nil)
	var _ tx.Transactor[db.Querier] = (db.Store)(nil)
}

func TestMockStore_Behavior(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	diskID := newTestUUID()

	expectedDisk := db.Disk{
		DiskID:    diskID,
		Name:      "test-disk",
		RootIno:   1001,
		MaxBytes:  1024 * 1024,
		UsedBytes: 0,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("CreateDisk 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		params := db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "test-disk",
			RootIno:  1001,
			MaxBytes: 1024 * 1024,
		}

		mockStore.EXPECT().
			CreateDisk(mock.Anything, params).
			Return(expectedDisk, nil).
			Once()

		result, err := store.CreateDisk(ctx, params)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, result)
	})

	t.Run("GetDisk 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			GetDisk(mock.Anything, diskID).
			Return(expectedDisk, nil).
			Once()

		result, err := store.GetDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, result)
	})

	t.Run("GetDiskForUpdate 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			GetDiskForUpdate(mock.Anything, diskID).
			Return(expectedDisk, nil).
			Once()

		result, err := store.GetDiskForUpdate(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, expectedDisk, result)
	})

	t.Run("UpdateDisk 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		updateParams := db.UpdateDiskParams{
			DiskID:   diskID,
			Name:     "renamed-disk",
			MaxBytes: 2048 * 1024,
		}

		updatedDisk := expectedDisk
		updatedDisk.Name = "renamed-disk"
		updatedDisk.MaxBytes = 2048 * 1024

		mockStore.EXPECT().
			UpdateDisk(mock.Anything, updateParams).
			Return(updatedDisk, nil).
			Once()

		result, err := store.UpdateDisk(ctx, updateParams)
		require.NoError(t, err)
		assert.Equal(t, updatedDisk, result)
	})

	t.Run("UpdateDiskUsedBytes 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		updateUsedParams := db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 512 * 1024,
		}

		updatedDisk := expectedDisk
		updatedDisk.UsedBytes = 512 * 1024

		mockStore.EXPECT().
			UpdateDiskUsedBytes(mock.Anything, updateUsedParams).
			Return(updatedDisk, nil).
			Once()

		result, err := store.UpdateDiskUsedBytes(ctx, updateUsedParams)
		require.NoError(t, err)
		assert.Equal(t, updatedDisk, result)
	})

	t.Run("DeleteDisk 算子契约调用与参数透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			DeleteDisk(mock.Anything, diskID).
			Return(int64(1), nil).
			Once()

		rows, err := store.DeleteDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, int64(1), rows)
	})

	t.Run("错误透明透传", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		expectedErr := errors.New("db error")
		mockStore.EXPECT().
			GetDisk(mock.Anything, diskID).
			Return(db.Disk{}, expectedErr).
			Once()

		_, err := store.GetDisk(ctx, diskID)
		require.ErrorIs(t, err, expectedErr)
	})
}

func TestMockStore_TransactorBehavior(t *testing.T) {
	ctx := context.Background()

	t.Run("ExecTxWithOptions 事务执行无变参", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var store db.Store = mockStore

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		err := store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			return nil
		})
		require.NoError(t, err)
	})
}

func TestMockQuerier_Behavior(t *testing.T) {
	ctx := context.Background()
	diskID := newTestUUID()
	mockQuerier := dbmock.NewMockQuerier(t)
	var q db.Querier = mockQuerier

	expectedDisk := db.Disk{DiskID: diskID, Name: "mock-q", RootIno: 2002, MaxBytes: 1000}
	mockQuerier.EXPECT().GetDisk(ctx, diskID).Return(expectedDisk, nil).Once()

	res, err := q.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, expectedDisk, res)
}

// ==========================================
// 数据库持久化集成测试 (Database Integration Tests)
// ==========================================

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
		Name:     "integration-disk-1",
		RootIno:  rootIno,
		MaxBytes: 100 * 1024 * 1024,
	}

	// 1. 创建磁盘实体
	created, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)
	assert.Equal(t, diskID, created.DiskID)
	assert.Equal(t, "integration-disk-1", created.Name)
	assert.Equal(t, rootIno, created.RootIno)
	assert.Equal(t, int64(100*1024*1024), created.MaxBytes)
	assert.Equal(t, int64(0), created.UsedBytes)
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

	// 3. 查询不存在的磁盘实体返回 pgx.ErrNoRows
	nonExistentID := newTestUUID()
	_, err = store.GetDisk(ctx, nonExistentID)
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
		Name:     "for-update-disk",
		RootIno:  rootIno,
		MaxBytes: 50 * 1024 * 1024,
	}

	_, err := store.CreateDisk(ctx, params)
	require.NoError(t, err)

	// 悲观排他锁读取
	err = store.ExecTx(ctx, func(q db.Querier) error {
		locked, getErr := q.GetDiskForUpdate(ctx, diskID)
		require.NoError(t, getErr)
		assert.Equal(t, diskID, locked.DiskID)
		assert.Equal(t, "for-update-disk", locked.Name)
		return nil
	})
	require.NoError(t, err)

	// 查询不存在的磁盘返回 pgx.ErrNoRows
	_, err = store.GetDiskForUpdate(ctx, newTestUUID())
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

	createParams := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "before-update",
		RootIno:  rootIno,
		MaxBytes: 10 * 1024 * 1024,
	}
	created, err := store.CreateDisk(ctx, createParams)
	require.NoError(t, err)

	// 等待微小时间片确保 clock_timestamp 变更
	time.Sleep(2 * time.Millisecond)

	updateParams := db.UpdateDiskParams{
		DiskID:   diskID,
		Name:     "after-update",
		MaxBytes: 20 * 1024 * 1024,
	}
	updated, err := store.UpdateDisk(ctx, updateParams)
	require.NoError(t, err)
	assert.Equal(t, "after-update", updated.Name)
	assert.Equal(t, int64(20*1024*1024), updated.MaxBytes)
	assert.Equal(t, int64(0), updated.UsedBytes)
	assert.True(t, updated.UpdatedAt.Time.After(created.UpdatedAt.Time) || updated.UpdatedAt.Time.Equal(created.UpdatedAt.Time))

	// 重新读取验证持久化
	persisted, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, "after-update", persisted.Name)
	assert.Equal(t, int64(20*1024*1024), persisted.MaxBytes)

	// 更新不存在记录返回 pgx.ErrNoRows
	_, err = store.UpdateDisk(ctx, db.UpdateDiskParams{
		DiskID:   newTestUUID(),
		Name:     "ghost",
		MaxBytes: 100,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_UpdateDiskUsedBytes(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID)
	})

	createParams := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "used-bytes-test",
		RootIno:  rootIno,
		MaxBytes: 10 * 1024 * 1024,
	}
	_, err := store.CreateDisk(ctx, createParams)
	require.NoError(t, err)

	// 更新已用容量
	updateUsedParams := db.UpdateDiskUsedBytesParams{
		DiskID:    diskID,
		UsedBytes: 4 * 1024 * 1024,
	}
	updated, err := store.UpdateDiskUsedBytes(ctx, updateUsedParams)
	require.NoError(t, err)
	assert.Equal(t, int64(4*1024*1024), updated.UsedBytes)

	// 重新读取验证持久化
	persisted, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(4*1024*1024), persisted.UsedBytes)

	// 更新不存在记录返回 pgx.ErrNoRows
	_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
		DiskID:    newTestUUID(),
		UsedBytes: 100,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_Integration_DeleteDisk(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID := newTestUUID()
	rootIno := nextTestIno()

	createParams := db.CreateDiskParams{
		DiskID:   diskID,
		Name:     "to-delete",
		RootIno:  rootIno,
		MaxBytes: 10 * 1024 * 1024,
	}
	_, err := store.CreateDisk(ctx, createParams)
	require.NoError(t, err)

	// 首次删除成功返回受影响行数为 1
	rows, err := store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)

	// 查询已被删除的磁盘记录
	_, err = store.GetDisk(ctx, diskID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 重复删除返回受影响行数为 0
	rows, err = store.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestStore_Integration_UniqueRootInoConstraint(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	diskID1 := newTestUUID()
	diskID2 := newTestUUID()
	sharedRootIno := nextTestIno()

	t.Cleanup(func() {
		_, _ = store.DeleteDisk(context.Background(), diskID1)
		_, _ = store.DeleteDisk(context.Background(), diskID2)
	})

	// 插入首条记录
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:   diskID1,
		Name:     "first-disk",
		RootIno:  sharedRootIno,
		MaxBytes: 10 * 1024 * 1024,
	})
	require.NoError(t, err)

	// 插入冲突的重复 root_ino 记录，应触发唯一约束异常 (SQLSTATE 23505)
	_, err = store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:   diskID2,
		Name:     "conflicting-disk",
		RootIno:  sharedRootIno,
		MaxBytes: 20 * 1024 * 1024,
	})
	require.Error(t, err)

	var pgErr *pgconn.PgError
	require.ErrorAs(t, err, &pgErr)
	assert.Equal(t, "23505", pgErr.Code)
}

func TestStore_Integration_CheckConstraints(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	t.Run("chk_disks_root_ino 约束: root_ino <= 0 被拒绝", func(t *testing.T) {
		// root_ino = 0
		diskIDZero := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskIDZero,
			Name:     "zero-root-ino",
			RootIno:  0,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_root_ino", pgErr.ConstraintName)

		// root_ino = -1
		diskIDNeg := newTestUUID()
		_, err = store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskIDNeg,
			Name:     "neg-root-ino",
			RootIno:  -1,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.Error(t, err)
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_root_ino", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: max_bytes <= 0 被拒绝", func(t *testing.T) {
		// max_bytes = 0
		diskIDZero := newTestUUID()
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskIDZero,
			Name:     "zero-max-bytes",
			RootIno:  nextTestIno(),
			MaxBytes: 0,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)

		// max_bytes = -100
		diskIDNeg := newTestUUID()
		_, err = store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskIDNeg,
			Name:     "neg-max-bytes",
			RootIno:  nextTestIno(),
			MaxBytes: -100,
		})
		require.Error(t, err)
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})

	t.Run("chk_disks_capacity 约束: used_bytes < 0 与 used_bytes > max_bytes 被拒绝", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     "capacity-check",
			RootIno:  rootIno,
			MaxBytes: 1000,
		})
		require.NoError(t, err)

		// used_bytes < 0
		_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: -1,
		})
		require.Error(t, err)
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)

		// used_bytes > max_bytes
		_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 1001,
		})
		require.Error(t, err)
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)

		// 将 used_bytes 合法设置为 500
		_, err = store.UpdateDiskUsedBytes(ctx, db.UpdateDiskUsedBytesParams{
			DiskID:    diskID,
			UsedBytes: 500,
		})
		require.NoError(t, err)

		// 更新 max_bytes < used_bytes (缩容低于已用) 触发约束冲突
		_, err = store.UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   diskID,
			Name:     "capacity-check",
			MaxBytes: 400,
		})
		require.Error(t, err)
		require.ErrorAs(t, err, &pgErr)
		assert.Equal(t, "23514", pgErr.Code)
		assert.Equal(t, "chk_disks_capacity", pgErr.ConstraintName)
	})
}

func TestStore_Integration_TransactionRollbackAndCommit(t *testing.T) {
	store, _ := setupTestDB(t)
	ctx := context.Background()

	t.Run("事务成功提交后持久化生效", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "tx-commit-disk",
				RootIno:  rootIno,
				MaxBytes: 5000,
			})
			return createErr
		})
		require.NoError(t, err)

		persisted, err := store.GetDisk(ctx, diskID)
		require.NoError(t, err)
		assert.Equal(t, diskID, persisted.DiskID)
	})

	t.Run("事务发生错误时原子回滚", func(t *testing.T) {
		diskID := newTestUUID()
		rootIno := nextTestIno()

		t.Cleanup(func() {
			_, _ = store.DeleteDisk(context.Background(), diskID)
		})

		rollbackErr := errors.New("simulated tx failure")
		err := store.ExecTx(ctx, func(q db.Querier) error {
			_, createErr := q.CreateDisk(ctx, db.CreateDiskParams{
				DiskID:   diskID,
				Name:     "tx-rollback-disk",
				RootIno:  rootIno,
				MaxBytes: 5000,
			})
			if createErr != nil {
				return createErr
			}
			return rollbackErr
		})
		require.ErrorIs(t, err, rollbackErr)

		// 验证回滚后记录不存在
		_, err = store.GetDisk(ctx, diskID)
		require.ErrorIs(t, err, pgx.ErrNoRows)
	})
}
