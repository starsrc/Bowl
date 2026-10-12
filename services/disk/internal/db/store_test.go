package db_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/db"
)

func setupTestStore(t *testing.T) (db.Store, func()) {
	t.Helper()
	dsn := getTestDSN()

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	store := db.NewStore(pool)

	cleanup := func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disks, disk_cleanup_tasks CASCADE")
		store.Close()
	}

	return store, cleanup
}

func TestStore_CreateAndGetDisk(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	name := "test-volume-01"
	capacity := int64(10 * 1024 * 1024 * 1024) // 10 GB
	inodesQuota := int64(1000000)
	status := int16(2) // READY

	created, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          name,
		CapacityBytes: capacity,
		InodesQuota:   inodesQuota,
		Status:        status,
	})
	require.NoError(t, err)
	assert.Equal(t, diskID, uuid.UUID(created.DiskID.Bytes))
	assert.Equal(t, name, created.Name)
	assert.Equal(t, capacity, created.CapacityBytes)
	assert.Equal(t, int64(0), created.UsedBytes)
	assert.Equal(t, inodesQuota, created.InodesQuota)
	assert.Equal(t, int64(0), created.InodesUsed)
	assert.Equal(t, status, created.Status)
	assert.WithinDuration(t, time.Now(), created.CreatedAt.Time, 5*time.Second)
	assert.WithinDuration(t, time.Now(), created.UpdatedAt.Time, 5*time.Second)

	// Query via GetDisk
	retrieved, err := store.GetDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, created.DiskID, retrieved.DiskID)
	assert.Equal(t, created.Name, retrieved.Name)
	assert.Equal(t, created.CapacityBytes, retrieved.CapacityBytes)
	assert.Equal(t, created.Status, retrieved.Status)

	// Query non-existent disk
	_, err = store.GetDisk(ctx, pgtype.UUID{Bytes: uuid.New(), Valid: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_UpdateDiskStatus(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "status-test-volume",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2, // READY
	})
	require.NoError(t, err)

	updated, err := store.UpdateDiskStatus(ctx, db.UpdateDiskStatusParams{
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: 3, // ATTACHED
	})
	require.NoError(t, err)
	assert.Equal(t, int16(3), updated.Status)
}

func TestStore_UpdateDisk(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	created, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "initial-name",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2, // READY
	})
	require.NoError(t, err)

	updated, err := store.UpdateDisk(ctx, db.UpdateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "updated-name",
		CapacityBytes: 2048,
		InodesQuota:   200,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated-name", updated.Name)
	assert.Equal(t, int64(2048), updated.CapacityBytes)
	assert.Equal(t, int64(200), updated.InodesQuota)
	assert.True(t, updated.UpdatedAt.Time.After(created.UpdatedAt.Time) || updated.UpdatedAt.Time.Equal(created.UpdatedAt.Time))
}

func TestStore_DiskAttachments_OWMR(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "owmr-test-volume",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2,
	})
	require.NoError(t, err)

	diskUUID := pgtype.UUID{Bytes: diskID, Valid: true}

	// 1. Attach writer client-1 (read_only = false)
	writerAtt, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-1",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "writer-client-1", writerAtt.ClientID)
	assert.False(t, writerAtt.ReadOnly)

	// 2. Attach reader client-2 (read_only = true) -> succeeds
	readerAtt, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "reader-client-2",
		ReadOnly: true,
	})
	require.NoError(t, err)
	assert.Equal(t, "reader-client-2", readerAtt.ClientID)
	assert.True(t, readerAtt.ReadOnly)

	// 3. Attach another writer client-3 (read_only = false) -> fails due to OWMR partial unique index idx_disk_attachments_writer
	_, err = store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-3",
		ReadOnly: false,
	})
	require.Error(t, err)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, "idx_disk_attachments_writer", pgErr.ConstraintName)

	// 4. Attach same client reader-client-2 again -> fails due to idx_disk_attachments_client
	_, err = store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "reader-client-2",
		ReadOnly: true,
	})
	require.Error(t, err)
	require.True(t, errors.As(err, &pgErr))
	assert.Equal(t, "23505", pgErr.Code)
	assert.Equal(t, "idx_disk_attachments_client", pgErr.ConstraintName)

	// 5. Count attachments
	count, err := store.CountDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)

	// 6. List attachments
	attachments, err := store.ListDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Len(t, attachments, 2)

	// 7. Delete writer attachment
	deletedRows, err := store.DeleteDiskAttachment(ctx, db.DeleteDiskAttachmentParams{
		DiskID:   diskUUID,
		ClientID: "writer-client-1",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedRows)

	// 8. Now client-3 can attach as writer!
	writerAtt3, err := store.CreateDiskAttachment(ctx, db.CreateDiskAttachmentParams{
		ID:       pgtype.UUID{Bytes: uuid.New(), Valid: true},
		DiskID:   diskUUID,
		ClientID: "writer-client-3",
		ReadOnly: false,
	})
	require.NoError(t, err)
	assert.Equal(t, "writer-client-3", writerAtt3.ClientID)

	// 9. Delete all attachments for disk
	deletedAll, err := store.DeleteDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), deletedAll) // client-2 and client-3

	countAfter, err := store.CountDiskAttachmentsByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, int64(0), countAfter)
}

func TestStore_ListDisksAndCountDisks(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// Create 5 disks with varying statuses and distinct creation times
	// Statuses: READY (2), ATTACHED (3), READY (2), ERROR (4), READY (2)
	statuses := []int16{2, 3, 2, 4, 2}
	createdIDs := make([]uuid.UUID, len(statuses))
	for i, st := range statuses {
		id := uuid.New()
		createdIDs[i] = id
		_, err := store.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:        pgtype.UUID{Bytes: id, Valid: true},
			Name:          "list-disk-" + string(rune('a'+i)),
			CapacityBytes: 1024 * int64(i+1),
			InodesQuota:   100,
			Status:        st,
		})
		require.NoError(t, err)
		time.Sleep(5 * time.Millisecond) // guarantee strictly increasing created_at
	}

	// 1. Total count without filter (0 means unspecified / all)
	totalCount, err := store.CountDisks(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(5), totalCount)

	// 2. Count with READY filter (status = 2)
	readyCount, err := store.CountDisks(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(3), readyCount)

	// 3. Count with ATTACHED filter (status = 3)
	attachedCount, err := store.CountDisks(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, int64(1), attachedCount)

	// 4. List first page (limit = 2, offset = 0) ordered by created_at DESC
	page1, err := store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: 0,
		LimitVal:     2,
		OffsetVal:    0,
	})
	require.NoError(t, err)
	require.Len(t, page1, 2)
	// Most recently created disk was index 4 (last one created)
	assert.Equal(t, createdIDs[4], uuid.UUID(page1[0].DiskID.Bytes))
	assert.Equal(t, createdIDs[3], uuid.UUID(page1[1].DiskID.Bytes))

	// 5. List second page (limit = 2, offset = 2)
	page2, err := store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: 0,
		LimitVal:     2,
		OffsetVal:    2,
	})
	require.NoError(t, err)
	require.Len(t, page2, 2)
	assert.Equal(t, createdIDs[2], uuid.UUID(page2[0].DiskID.Bytes))
	assert.Equal(t, createdIDs[1], uuid.UUID(page2[1].DiskID.Bytes))

	// 6. List third page (limit = 2, offset = 4)
	page3, err := store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: 0,
		LimitVal:     2,
		OffsetVal:    4,
	})
	require.NoError(t, err)
	require.Len(t, page3, 1)
	assert.Equal(t, createdIDs[0], uuid.UUID(page3[0].DiskID.Bytes))

	// 7. List out-of-range offset returns empty list
	pageEmpty, err := store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: 0,
		LimitVal:     2,
		OffsetVal:    10,
	})
	require.NoError(t, err)
	assert.Empty(t, pageEmpty)

	// 8. Filtered list for READY (status = 2)
	readyList, err := store.ListDisks(ctx, db.ListDisksParams{
		StatusFilter: 2,
		LimitVal:     10,
		OffsetVal:    0,
	})
	require.NoError(t, err)
	require.Len(t, readyList, 3)
	for _, d := range readyList {
		assert.Equal(t, int16(2), d.Status)
	}
}

func TestStore_DeleteDisk(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	_, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID:        pgtype.UUID{Bytes: diskID, Valid: true},
		Name:          "delete-test-disk",
		CapacityBytes: 1024,
		InodesQuota:   100,
		Status:        2,
	})
	require.NoError(t, err)

	// Verify disk exists
	_, err = store.GetDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.NoError(t, err)

	// Delete disk
	rows, err := store.DeleteDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, int64(1), rows)

	// Verify disk no longer exists
	_, err = store.GetDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, pgx.ErrNoRows)

	// Deleting again returns 0 rows affected
	rows, err = store.DeleteDisk(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, int64(0), rows)
}

func TestStore_DiskCleanupTask_Lifecycle_Completed(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	taskID := uuid.New()
	diskID := uuid.New()

	// 1. 创建处于 PENDING 状态的任务
	taskUUID := pgtype.UUID{Bytes: taskID, Valid: true}
	diskUUID := pgtype.UUID{Bytes: diskID, Valid: true}
	created, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: taskUUID,
		DiskID: diskUUID,
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)
	assert.Equal(t, taskID, uuid.UUID(created.TaskID.Bytes))
	assert.Equal(t, diskID, uuid.UUID(created.DiskID.Bytes))
	assert.Equal(t, db.CleanupTaskStatusPending, created.Status)
	assert.Equal(t, int32(0), created.Attempts)
	assert.False(t, created.LastError.Valid)
	assert.WithinDuration(t, time.Now(), created.CreatedAt.Time, 5*time.Second)
	assert.WithinDuration(t, time.Now(), created.UpdatedAt.Time, 5*time.Second)

	// 2. 状态流转: PENDING -> RUNNING
	running, err := store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
		TaskID: taskUUID,
		Status: db.CleanupTaskStatusRunning,
	})
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusRunning, running.Status)
	assert.Equal(t, int32(0), running.Attempts)
	assert.True(t, running.UpdatedAt.Time.After(created.UpdatedAt.Time) || running.UpdatedAt.Time.Equal(created.UpdatedAt.Time))

	// 3. 状态流转: RUNNING -> COMPLETED
	completed, err := store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
		TaskID: taskUUID,
		Status: db.CleanupTaskStatusCompleted,
	})
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusCompleted, completed.Status)

	// 4. 通过 GetCleanupTask 获取并断言最终持久化状态
	retrieved, err := store.GetCleanupTask(ctx, taskUUID)
	require.NoError(t, err)
	assert.Equal(t, taskUUID, retrieved.TaskID)
	assert.Equal(t, diskUUID, retrieved.DiskID)
	assert.Equal(t, db.CleanupTaskStatusCompleted, retrieved.Status)
	assert.Equal(t, int32(0), retrieved.Attempts)
}

func TestStore_DiskCleanupTask_Lifecycle_FailedWithRetry(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	taskID := uuid.New()
	diskID := uuid.New()
	taskUUID := pgtype.UUID{Bytes: taskID, Valid: true}
	diskUUID := pgtype.UUID{Bytes: diskID, Valid: true}

	// 1. 创建初始 PENDING 任务
	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: taskUUID,
		DiskID: diskUUID,
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	// 2. 流转为 RUNNING
	_, err = store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
		TaskID: taskUUID,
		Status: db.CleanupTaskStatusRunning,
	})
	require.NoError(t, err)

	// 3. 记录第 1 次执行尝试失败 (重试中，状态切回 PENDING)
	attempt1Err := "s3 connection timeout"
	attempt1, err := store.RecordCleanupTaskAttempt(ctx, db.RecordCleanupTaskAttemptParams{
		TaskID:    taskUUID,
		LastError: pgtype.Text{String: attempt1Err, Valid: true},
		Status:    db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), attempt1.Attempts)
	assert.Equal(t, db.CleanupTaskStatusPending, attempt1.Status)
	assert.True(t, attempt1.LastError.Valid)
	assert.Equal(t, attempt1Err, attempt1.LastError.String)

	// 4. 工作器再次认领为 RUNNING
	_, err = store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
		TaskID: taskUUID,
		Status: db.CleanupTaskStatusRunning,
	})
	require.NoError(t, err)

	// 5. 遭遇不可恢复错误，直接置为 FAILED 并递增 attempts
	finalErr := "permanent s3 bucket authorization error"
	failed, err := store.FailCleanupTask(ctx, db.FailCleanupTaskParams{
		TaskID:    taskUUID,
		LastError: pgtype.Text{String: finalErr, Valid: true},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), failed.Attempts)
	assert.Equal(t, db.CleanupTaskStatusFailed, failed.Status)
	assert.True(t, failed.LastError.Valid)
	assert.Equal(t, finalErr, failed.LastError.String)

	// 6. 验证按 disk_id 查询最新任务
	taskByDisk, err := store.GetCleanupTaskByDiskID(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, taskUUID, taskByDisk.TaskID)
	assert.Equal(t, db.CleanupTaskStatusFailed, taskByDisk.Status)
	assert.Equal(t, int32(2), taskByDisk.Attempts)
	assert.Equal(t, finalErr, taskByDisk.LastError.String)
}

func TestStore_DiskCleanupTask_GetNextPendingAndList(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	// 1. 顺序创建 3 个清理任务
	taskID1 := uuid.New()
	taskID2 := uuid.New()
	taskID3 := uuid.New()
	diskID1 := uuid.New()
	diskID2 := uuid.New()
	diskID3 := uuid.New()

	// 任务 1: PENDING (最早创建)
	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID1, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID1, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)

	// 任务 2: RUNNING
	_, err = store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID2, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID2, Valid: true},
		Status: db.CleanupTaskStatusRunning,
	})
	require.NoError(t, err)
	time.Sleep(5 * time.Millisecond)

	// 任务 3: PENDING (较晚创建)
	_, err = store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID3, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID3, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	// 2. 测试 GetNextPendingCleanupTask: 应返回最早创建的 PENDING 任务 (taskID1)
	nextTask, err := store.GetNextPendingCleanupTask(ctx)
	require.NoError(t, err)
	assert.Equal(t, taskID1, uuid.UUID(nextTask.TaskID.Bytes))
	assert.Equal(t, db.CleanupTaskStatusPending, nextTask.Status)

	// 3. 测试 GetPendingCleanupTasks(limit = 10): 应按 created_at ASC 返回 taskID1 与 taskID3
	pendingList, err := store.GetPendingCleanupTasks(ctx, 10)
	require.NoError(t, err)
	require.Len(t, pendingList, 2)
	assert.Equal(t, taskID1, uuid.UUID(pendingList[0].TaskID.Bytes))
	assert.Equal(t, taskID3, uuid.UUID(pendingList[1].TaskID.Bytes))

	// 4. 测试 ListCleanupTasks 条件过滤
	// 全部任务
	allTasks, err := store.ListCleanupTasks(ctx, db.ListCleanupTasksParams{
		StatusFilter: 0,
		LimitVal:     10,
		OffsetVal:    0,
	})
	require.NoError(t, err)
	assert.Len(t, allTasks, 3)

	// 仅过滤 RUNNING 任务
	runningTasks, err := store.ListCleanupTasks(ctx, db.ListCleanupTasksParams{
		StatusFilter: db.CleanupTaskStatusRunning,
		LimitVal:     10,
		OffsetVal:    0,
	})
	require.NoError(t, err)
	require.Len(t, runningTasks, 1)
	assert.Equal(t, taskID2, uuid.UUID(runningTasks[0].TaskID.Bytes))

	// 5. 按 disk_id 列出任务
	tasksByDisk, err := store.ListCleanupTasksByDiskID(ctx, pgtype.UUID{Bytes: diskID1, Valid: true})
	require.NoError(t, err)
	require.Len(t, tasksByDisk, 1)
	assert.Equal(t, taskID1, uuid.UUID(tasksByDisk[0].TaskID.Bytes))

	// 6. 测试 DeleteCleanupTask
	deletedRows, err := store.DeleteCleanupTask(ctx, pgtype.UUID{Bytes: taskID1, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deletedRows)

	// 删除后查询返回 ErrNoRows
	_, err = store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID1, Valid: true})
	require.Error(t, err)
	assert.ErrorIs(t, err, pgx.ErrNoRows)
}

func TestStore_DiskCleanupTask_TxAtomicity(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()
	diskID := uuid.New()
	taskID := uuid.New()
	diskUUID := pgtype.UUID{Bytes: diskID, Valid: true}
	taskUUID := pgtype.UUID{Bytes: taskID, Valid: true}

	// 1. 事务内发生错误回滚，验证元数据与清理任务均未持久化
	simulatedErr := errors.New("simulated error during transaction")
	err := store.ExecTx(ctx, func(q db.Querier) error {
		_, createDiskErr := q.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:        diskUUID,
			Name:          "tx-rollback-disk",
			CapacityBytes: 1024,
			InodesQuota:   100,
			Status:        2,
		})
		if createDiskErr != nil {
			return createDiskErr
		}

		_, createTaskErr := q.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
			TaskID: taskUUID,
			DiskID: diskUUID,
			Status: db.CleanupTaskStatusPending,
		})
		if createTaskErr != nil {
			return createTaskErr
		}

		return simulatedErr
	})
	require.ErrorIs(t, err, simulatedErr)

	// 验证未写入
	_, err = store.GetDisk(ctx, diskUUID)
	require.ErrorIs(t, err, pgx.ErrNoRows)
	_, err = store.GetCleanupTask(ctx, taskUUID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 2. 事务成功提交，验证元数据与清理任务原子持久化
	err = store.ExecTx(ctx, func(q db.Querier) error {
		_, createDiskErr := q.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:        diskUUID,
			Name:          "tx-commit-disk",
			CapacityBytes: 1024,
			InodesQuota:   100,
			Status:        2,
		})
		if createDiskErr != nil {
			return createDiskErr
		}

		_, createTaskErr := q.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
			TaskID: taskUUID,
			DiskID: diskUUID,
			Status: db.CleanupTaskStatusPending,
		})
		return createTaskErr
	})
	require.NoError(t, err)

	// 验证已成功写入
	disk, err := store.GetDisk(ctx, diskUUID)
	require.NoError(t, err)
	assert.Equal(t, "tx-commit-disk", disk.Name)

	task, err := store.GetCleanupTask(ctx, taskUUID)
	require.NoError(t, err)
	assert.Equal(t, taskUUID, task.TaskID)
	assert.Equal(t, db.CleanupTaskStatusPending, task.Status)
}


