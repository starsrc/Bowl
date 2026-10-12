package worker_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/disk/internal/db"
	"github.com/starsrc/bowl/services/disk/internal/worker"
)

// mockObjectStoragePurger 为清理工作器测试提供线程安全的 ObjectStoragePurger Mock 实现。
type mockObjectStoragePurger struct {
	mu      sync.Mutex
	calls   []uuid.UUID
	purgeFn func(ctx context.Context, diskID uuid.UUID) error
}

func (m *mockObjectStoragePurger) PurgeDisk(ctx context.Context, diskID uuid.UUID) error {
	m.mu.Lock()
	m.calls = append(m.calls, diskID)
	fn := m.purgeFn
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, diskID)
	}
	return nil
}

func (m *mockObjectStoragePurger) getCalls() []uuid.UUID {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]uuid.UUID, len(m.calls))
	copy(res, m.calls)
	return res
}

func getTestDSN() string {
	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
	}
	return dsn
}

func setupTestStore(t *testing.T) (db.Store, func()) {
	t.Helper()
	dsn := getTestDSN()

	err := db.MigrateUp(dsn)
	require.NoError(t, err)

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)

	_, _ = pool.Exec(ctx, "TRUNCATE TABLE disks, disk_cleanup_tasks CASCADE")

	store := db.NewStore(pool)

	cleanup := func() {
		_, _ = pool.Exec(ctx, "TRUNCATE TABLE disks, disk_cleanup_tasks CASCADE")
		store.Close()
	}

	return store, cleanup
}

// TestDiskCleanupWorker_Notify_ImmediateSuccess 验证通过 Notify 发送通知后任务立即执行并流转至 COMPLETED。
func TestDiskCleanupWorker_Notify_ImmediateSuccess(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID := uuid.New()
	taskID := uuid.New()

	// 1. 初始化数据库中的待处理清理任务 (PENDING)
	createdTask, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusPending, createdTask.Status)

	purger := &mockObjectStoragePurger{}

	// 2. 初始化并启动 DiskCleanupWorker
	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(1*time.Minute), // 长轮询间隔，确保优先由 Notify 触发
		worker.WithBaseBackoff(10*time.Millisecond),
		worker.WithMaxAttempts(3),
	)

	w.Start(ctx)
	defer w.Stop()

	// 3. 发送 Notify 通知
	w.Notify(diskID)

	// 4. 等待并断言任务状态流转为 COMPLETED
	require.Eventually(t, func() bool {
		task, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID, Valid: true})
		if err != nil {
			return false
		}
		return task.Status == db.CleanupTaskStatusCompleted
	}, 3*time.Second, 20*time.Millisecond, "任务状态应在 Notify 后流转为 COMPLETED")

	// 5. 验证 purger 确实被正确调用
	calls := purger.getCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, diskID, calls[0])
}

// TestDiskCleanupWorker_Poll_PendingRecovery 验证后台周期性轮询能够在启动与重启时恢复处理积压/遗留的 PENDING 任务。
func TestDiskCleanupWorker_Poll_PendingRecovery(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID1 := uuid.New()
	taskID1 := uuid.New()
	diskID2 := uuid.New()
	taskID2 := uuid.New()

	// 1. 在 Worker 启动前预先在数据库写入两个 PENDING 任务（模拟节点崩溃后重启或未通知到的遗留任务）
	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID1, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID1, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	_, err = store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID2, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID2, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	purger := &mockObjectStoragePurger{}

	// 2. 初始化 Worker，配置短轮询周期 (50ms)
	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(50*time.Millisecond),
		worker.WithBaseBackoff(10*time.Millisecond),
		worker.WithMaxAttempts(3),
	)

	// 3. 启动 Worker（不调用 Notify，纯依赖后台轮询或启动补偿）
	w.Start(ctx)
	defer w.Stop()

	// 4. 等待两个任务全部被轮询发现并流转为 COMPLETED
	require.Eventually(t, func() bool {
		task1, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID1, Valid: true})
		if err != nil {
			return false
		}
		task2, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID2, Valid: true})
		if err != nil {
			return false
		}
		return task1.Status == db.CleanupTaskStatusCompleted && task2.Status == db.CleanupTaskStatusCompleted
	}, 3*time.Second, 20*time.Millisecond, "遗留任务应全部被轮询处理并流转至 COMPLETED")

	// 5. 校验 purger 调用记录包含了两个 diskID
	calls := purger.getCalls()
	assert.Contains(t, calls, diskID1)
	assert.Contains(t, calls, diskID2)
	assert.Len(t, calls, 2)
}

// TestDiskCleanupWorker_ExponentialRetry_AndFailure 验证物理清理持续失败时，工作器执行指数退避重试并在达到 maxAttempts 后流转至 FAILED。
func TestDiskCleanupWorker_ExponentialRetry_AndFailure(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID := uuid.New()
	taskID := uuid.New()

	// 1. 初始化待处理任务 (PENDING)
	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	expectedErr := errors.New("simulated object storage connection error")
	callTimes := make([]time.Time, 0)
	var callMu sync.Mutex

	purger := &mockObjectStoragePurger{
		purgeFn: func(ctx context.Context, dID uuid.UUID) error {
			callMu.Lock()
			callTimes = append(callTimes, time.Now())
			callMu.Unlock()
			return expectedErr
		},
	}

	maxAttempts := 3
	baseBackoff := 30 * time.Millisecond

	// 2. 初始化 Worker: maxAttempts = 3, baseBackoff = 30ms
	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(1*time.Minute),
		worker.WithBaseBackoff(baseBackoff),
		worker.WithMaxAttempts(maxAttempts),
	)

	w.Start(ctx)
	defer w.Stop()

	startTime := time.Now()
	w.Notify(diskID)

	// 3. 等待任务达到终态 FAILED
	require.Eventually(t, func() bool {
		task, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID, Valid: true})
		if err != nil {
			return false
		}
		return task.Status == db.CleanupTaskStatusFailed
	}, 4*time.Second, 20*time.Millisecond, "持续失败后任务状态应流转为 FAILED")

	elapsed := time.Since(startTime)

	// 4. 断言数据库持久化状态与错误记录
	finalTask, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusFailed, finalTask.Status)
	assert.Equal(t, int32(maxAttempts), finalTask.Attempts)
	assert.True(t, finalTask.LastError.Valid)
	assert.Contains(t, finalTask.LastError.String, "simulated object storage connection error")

	// 5. 校验调用次数及指数退避耗时
	callMu.Lock()
	actualCalls := len(callTimes)
	callMu.Unlock()
	assert.Equal(t, maxAttempts, actualCalls, "总调用次数应与 maxAttempts 一致")

	// 理论退避间隔: 30ms (attempt 1) + 60ms (attempt 2) = 90ms
	expectedMinElapsed := baseBackoff + 2*baseBackoff
	assert.True(t, elapsed >= expectedMinElapsed, "总执行时间 (%v) 应大于退避时间之和 (%v)", elapsed, expectedMinElapsed)
}

// TestDiskCleanupWorker_RetrySuccess_TransitionsToCompleted 验证首次失败后在重试阶段成功清理并流转为 COMPLETED。
func TestDiskCleanupWorker_RetrySuccess_TransitionsToCompleted(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	diskID := uuid.New()
	taskID := uuid.New()

	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	var callCount int
	var countMu sync.Mutex

	purger := &mockObjectStoragePurger{
		purgeFn: func(ctx context.Context, dID uuid.UUID) error {
			countMu.Lock()
			callCount++
			current := callCount
			countMu.Unlock()

			if current == 1 {
				return errors.New("transient s3 503 slowdown")
			}
			return nil
		},
	}

	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(1*time.Minute),
		worker.WithBaseBackoff(20*time.Millisecond),
		worker.WithMaxAttempts(3),
	)

	w.Start(ctx)
	defer w.Stop()

	w.Notify(diskID)

	// 等待重试成功并流转为 COMPLETED
	require.Eventually(t, func() bool {
		task, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID, Valid: true})
		if err != nil {
			return false
		}
		return task.Status == db.CleanupTaskStatusCompleted
	}, 3*time.Second, 20*time.Millisecond)

	finalTask, err := store.GetCleanupTask(ctx, pgtype.UUID{Bytes: taskID, Valid: true})
	require.NoError(t, err)
	assert.Equal(t, db.CleanupTaskStatusCompleted, finalTask.Status)
	assert.Equal(t, int32(1), finalTask.Attempts, "重试成功前应记录 1 次失败尝试")

	countMu.Lock()
	assert.Equal(t, 2, callCount, "总共应调用 2 次 (1次失败 + 1次成功)")
	countMu.Unlock()
}

// TestDiskCleanupWorker_GracefulShutdown 验证工作器在处理在途任务时能正确响应 Stop，等待任务退出且无死锁或泄漏。
func TestDiskCleanupWorker_GracefulShutdown(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx := context.Background()

	diskID := uuid.New()
	taskID := uuid.New()

	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: db.CleanupTaskStatusPending,
	})
	require.NoError(t, err)

	startedPurge := make(chan struct{})
	purgerStopped := make(chan struct{})

	purger := &mockObjectStoragePurger{
		purgeFn: func(pCtx context.Context, dID uuid.UUID) error {
			close(startedPurge)
			// 阻塞等待外部 context 取消信号
			<-pCtx.Done()
			close(purgerStopped)
			return pCtx.Err()
		},
	}

	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(1*time.Minute),
		worker.WithBaseBackoff(10*time.Millisecond),
		worker.WithMaxAttempts(3),
	)

	w.Start(ctx)
	w.Notify(diskID)

	// 等待在途任务进入 PurgeDisk 逻辑
	select {
	case <-startedPurge:
	case <-time.After(3 * time.Second):
		t.Fatal("超时：未进入在途任务 PurgeDisk 执行")
	}

	// 执行 Stop，此时应能触发 context 取消并等待 PurgeDisk 退出
	stopDone := make(chan struct{})
	go func() {
		w.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Stop 执行超时，可能存在未释放的锁或未退出的协程")
	}

	// 确认 purger 也已正常响应 context 退出
	select {
	case <-purgerStopped:
	case <-time.After(1 * time.Second):
		t.Fatal("在途任务未在 Stop 后正常退出")
	}

	// 验证在 Stop 后重复调用 Stop 和 Notify 不会 panic 且安全返回
	assert.NotPanics(t, func() {
		w.Stop()
		w.Notify(uuid.New())
	}, "多次 Stop 或已停止后的 Notify 必须保持安全与幂等")
}

// TestDiskCleanupWorker_Notify_NilOrNotFound 验证传入 uuid.Nil 或数据库不存在任务时的鲁棒性。
func TestDiskCleanupWorker_Notify_NilOrNotFound(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	purger := &mockObjectStoragePurger{}
	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithNotifyChanCapacity(10),
		worker.WithPollInterval(1*time.Minute),
	)

	w.Start(ctx)
	defer w.Stop()

	// 传入 uuid.Nil
	assert.NotPanics(t, func() {
		w.Notify(uuid.Nil)
	})

	// 传入不存在的 diskID
	nonExistentDiskID := uuid.New()
	assert.NotPanics(t, func() {
		w.Notify(nonExistentDiskID)
	})

	// 等待一小段时间，确保 purger 未被调用
	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, purger.getCalls())
}

// TestDiskCleanupWorker_DeduplicationAndCompletedSkip 验证已完成任务被跳过，以及同一 diskID 并发通知的去重机制。
func TestDiskCleanupWorker_DeduplicationAndCompletedSkip(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	diskID := uuid.New()
	taskID := uuid.New()

	// 创建已处于 COMPLETED 状态的任务
	_, err := store.CreateCleanupTask(ctx, db.CreateCleanupTaskParams{
		TaskID: pgtype.UUID{Bytes: taskID, Valid: true},
		DiskID: pgtype.UUID{Bytes: diskID, Valid: true},
		Status: db.CleanupTaskStatusCompleted,
	})
	require.NoError(t, err)

	purger := &mockObjectStoragePurger{}
	w := worker.NewDiskCleanupWorker(
		store,
		purger,
		worker.WithPollInterval(1*time.Minute),
	)

	w.Start(ctx)
	defer w.Stop()

	// 多次通知已完成的任务
	w.Notify(diskID)
	w.Notify(diskID)

	time.Sleep(50 * time.Millisecond)
	assert.Empty(t, purger.getCalls(), "已处于 COMPLETED 状态的任务不应再次触发清理")
}




