package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/disk/internal/db"
	"github.com/starsrc/bowl/services/disk/internal/storage"
)

// WorkerOption 定义 DiskCleanupWorker 的配置选项函数。
type WorkerOption func(*DiskCleanupWorker)

// WithPollInterval 配置后台周期性补偿轮询的间隔时间。
func WithPollInterval(d time.Duration) WorkerOption {
	return func(w *DiskCleanupWorker) {
		if d > 0 {
			w.pollInterval = d
		}
	}
}

// WithBaseBackoff 配置指数退避重试的基准等待时间。
func WithBaseBackoff(d time.Duration) WorkerOption {
	return func(w *DiskCleanupWorker) {
		if d > 0 {
			w.baseBackoff = d
		}
	}
}

// WithMaxAttempts 配置任务执行的最大尝试次数（超过后流转至 FAILED）。
func WithMaxAttempts(n int) WorkerOption {
	return func(w *DiskCleanupWorker) {
		if n > 0 {
			w.maxAttempts = n
		}
	}
}

// WithNotifyChanCapacity 配置通知缓冲通道的容量。
func WithNotifyChanCapacity(cap int) WorkerOption {
	return func(w *DiskCleanupWorker) {
		if cap > 0 {
			w.notifyChan = make(chan uuid.UUID, cap)
		}
	}
}

// DiskCleanupWorker 是负责异步级联清理已删除虚拟磁盘底层物理数据（S3 对象与本地切片）的常驻调度引擎。
type DiskCleanupWorker struct {
	store        db.Store
	purger       storage.ObjectStoragePurger
	notifyChan   chan uuid.UUID
	pollInterval time.Duration
	baseBackoff  time.Duration
	maxAttempts  int
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	startOnce sync.Once
	stopOnce  sync.Once
	activeMu  sync.Mutex
	active    map[uuid.UUID]struct{}
}

// NewDiskCleanupWorker 实例化常驻磁盘物理清理工作器。
func NewDiskCleanupWorker(store db.Store, purger storage.ObjectStoragePurger, opts ...WorkerOption) *DiskCleanupWorker {
	w := &DiskCleanupWorker{
		store:        store,
		purger:       purger,
		notifyChan:   make(chan uuid.UUID, 100),
		pollInterval: 1 * time.Minute,
		baseBackoff:  2 * time.Second,
		maxAttempts:  5,
		active:       make(map[uuid.UUID]struct{}),
	}

	for _, opt := range opts {
		opt(w)
	}

	return w
}

// Start 启动后台事件循环协程，开始接收即时通知与周期性轮询调度。
func (w *DiskCleanupWorker) Start(ctx context.Context) {
	w.startOnce.Do(func() {
		w.ctx, w.cancel = context.WithCancel(ctx)
		w.wg.Add(1)
		go w.run()
	})
}

// Stop 优雅停止后台事件循环，并等待所有正在执行的在途物理清理任务完成。
func (w *DiskCleanupWorker) Stop() {
	w.stopOnce.Do(func() {
		if w.cancel != nil {
			w.cancel()
		}
		w.wg.Wait()
	})
}

// Notify 向工作器发送磁盘清理通知（非阻塞安全发送）。
func (w *DiskCleanupWorker) Notify(diskID uuid.UUID) {
	if diskID == uuid.Nil {
		return
	}
	if w.ctx != nil && w.ctx.Err() != nil {
		return
	}
	select {
	case w.notifyChan <- diskID:
	default:
		// 通道已满时非阻塞丢弃，依赖后台周期补偿轮询拉取
		log.Printf("清理任务通知通道已满，diskID: %s 将由后台轮询恢复", diskID)
	}
}

// run 运行常驻后台事件循环。
func (w *DiskCleanupWorker) run() {
	defer w.wg.Done()

	// 启动或重启时立即执行一次补偿轮询，恢复未完成的孤儿任务
	w.pollPendingTasks()

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case diskID := <-w.notifyChan:
			w.dispatch(diskID)
		case <-ticker.C:
			w.pollPendingTasks()
		}
	}
}

// pollPendingTasks 批量拉取数据库中的待处理任务进行补偿调度。
func (w *DiskCleanupWorker) pollPendingTasks() {
	if w.ctx == nil || w.ctx.Err() != nil {
		return
	}

	tasks, err := w.store.GetPendingCleanupTasks(w.ctx, 50)
	if err != nil {
		if errors.Is(err, context.Canceled) || (w.ctx != nil && w.ctx.Err() != nil) {
			return
		}
		log.Printf("拉取待处理清理任务失败: %v", err)
		return
	}

	for _, t := range tasks {
		if !t.DiskID.Valid {
			continue
		}
		diskID := uuid.UUID(t.DiskID.Bytes)
		w.dispatch(diskID)
	}
}

// dispatch 将指定磁盘的分发调度至异步协程，并防范同一磁盘的并发重复执行。
func (w *DiskCleanupWorker) dispatch(diskID uuid.UUID) {
	if diskID == uuid.Nil {
		return
	}
	if w.ctx == nil || w.ctx.Err() != nil {
		return
	}

	w.activeMu.Lock()
	if _, ok := w.active[diskID]; ok {
		w.activeMu.Unlock()
		return
	}
	w.active[diskID] = struct{}{}
	w.activeMu.Unlock()

	w.wg.Add(1)
	go func() {
		defer func() {
			w.activeMu.Lock()
			delete(w.active, diskID)
			w.activeMu.Unlock()
			w.wg.Done()
		}()

		if err := w.processDisk(w.ctx, diskID); err != nil {
			if !errors.Is(err, context.Canceled) && (w.ctx == nil || w.ctx.Err() == nil) {
				log.Printf("执行磁盘物理清理失败 (diskID: %s): %v", diskID, err)
			}
		}
	}()
}

// processDisk 处理单个虚拟磁盘的底层物理资源级联清理及指数退避重试状态流转。
func (w *DiskCleanupWorker) processDisk(ctx context.Context, diskID uuid.UUID) error {
	task, err := w.store.GetCleanupTaskByDiskID(ctx, pgtype.UUID{Bytes: diskID, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("查询磁盘清理任务失败 (diskID: %s): %w", diskID, err)
	}

	// 若任务已处于终态，则无需重复执行
	if task.Status == db.CleanupTaskStatusCompleted || task.Status == db.CleanupTaskStatusFailed {
		return nil
	}

	// 任务执行循环与指数退避重试
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// 1. 将任务状态流转为 RUNNING
		task, err = w.store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
			TaskID: task.TaskID,
			Status: db.CleanupTaskStatusRunning,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("更新任务状态为 RUNNING 失败: %w", err)
		}

		// 2. 调用物理存储驱动执行清理
		purgeErr := w.purger.PurgeDisk(ctx, diskID)
		if purgeErr == nil {
			// 清理成功：流转至 COMPLETED
			_, err = w.store.UpdateCleanupTaskStatus(ctx, db.UpdateCleanupTaskStatusParams{
				TaskID: task.TaskID,
				Status: db.CleanupTaskStatusCompleted,
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("更新任务状态为 COMPLETED 失败: %w", err)
			}
			return nil
		}

		// 若由于 context 取消引发错误，直接退出
		if errors.Is(purgeErr, context.Canceled) || ctx.Err() != nil {
			return ctx.Err()
		}

		// 3. 清理失败：判断是否达到最大重试次数上限
		if int(task.Attempts) >= w.maxAttempts {
			// 达到最大重试次数，流转至 FAILED
			_, err = w.store.FailCleanupTask(ctx, db.FailCleanupTaskParams{
				TaskID:    task.TaskID,
				LastError: pgtype.Text{String: purgeErr.Error(), Valid: true},
			})
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("标记任务状态为 FAILED 失败: %w", err)
			}
			return purgeErr
		}

		// 未达到最大次数：递增 attempts，恢复为 PENDING 状态
		task, err = w.store.RecordCleanupTaskAttempt(ctx, db.RecordCleanupTaskAttemptParams{
			TaskID:    task.TaskID,
			LastError: pgtype.Text{String: purgeErr.Error(), Valid: true},
			Status:    db.CleanupTaskStatusPending,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("记录任务尝试失败: %w", err)
		}

		// 4. 计算指数退避时间: baseBackoff * 2^(attempts-1) (例如 2s, 4s, 8s, 16s, 32s)
		shift := task.Attempts - 1
		if shift < 0 {
			shift = 0
		}
		backoff := w.baseBackoff * time.Duration(1<<shift)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}
