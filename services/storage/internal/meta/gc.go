package meta

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

const (
	// DefaultGCBatchLimit 定义单批次物理垃圾回收扫描的最大切片记录数。
	DefaultGCBatchLimit int32 = 100
	// DefaultGCInterval 定义后台 GC Worker 扫描轮询周期的默认时间间隔。
	DefaultGCInterval time.Duration = time.Minute
	// DefaultGCOrphanGracePeriod 定义后台 GC 对账回收未登记孤儿物理块文件的默认安全宽限期。
	DefaultGCOrphanGracePeriod time.Duration = time.Hour
)

// CollectGarbage 执行单批次物理垃圾回收。
//  1. 扫描 slice_refs 表中引用计数已归零 (refs <= 0) 的物理切片，调用 BlockStore.DeleteBlock 物理删除本地 NVMe 文件，
//     在该切片的所有关联 Block 文件均物理删除成功（或文件本就不存在）后，从数据库中清除 slice_refs 行。
//  2. 若底层 BlockStore 实现了 BlockLister，进一步扫描修改时间超过安全宽限期（默认 1 小时）的物理块文件并与 slice_refs 对账，
//     仅对在 slice_refs 中无记录（pgx.ErrNoRows）的未登记孤儿物理块文件执行物理回收。
//
// 若 limit <= 0，默认采用 DefaultGCBatchLimit (100)。
// 若某切片的物理块删除发生错误，跳过清除该切片的 slice_refs 记录以保留至下次 GC 重试，并不使事务整体回滚已成功回收的其他切片。
func (m *manager) CollectGarbage(ctx context.Context, limit int32) (int, error) {
	if limit <= 0 {
		limit = DefaultGCBatchLimit
	}

	var totalReclaimed int
	var capturedBlockErrs []error

	bs := m.getBlockStore()

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		totalReclaimed = 0
		capturedBlockErrs = nil

		rows, err := q.ListGarbageSliceRefs(ctx, limit)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}

		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}

			// 非法 ID 或零大小异常切片，直接从数据库元数据中移除
			if row.SliceID <= 0 || row.Size <= 0 {
				if err := q.DeleteSliceRef(ctx, row.SliceID); err != nil {
					return err
				}
				totalReclaimed++
				continue
			}

			blocks := SliceBlocks(uint32(row.Size))
			var deleteFailed bool
			for _, b := range blocks {
				if err := bs.DeleteBlock(ctx, uint64(row.SliceID), b.BlockIdx, b.BlockSize); err != nil {
					capturedBlockErrs = append(capturedBlockErrs, err)
					deleteFailed = true
					break
				}
			}

			if deleteFailed {
				// 某个 Block 文件物理删除遇到错误：跳过删除该 slice_refs 记录，
				// 不回滚已成功删除的其他切片，保留在数据库中供下次 GC 重试。
				continue
			}

			if err := q.DeleteSliceRef(ctx, row.SliceID); err != nil {
				return err
			}
			totalReclaimed++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}

	if lister, ok := bs.(BlockLister); ok && m.store != nil {
		gracePeriod := m.gcOrphanGracePeriod
		if gracePeriod <= 0 {
			gracePeriod = DefaultGCOrphanGracePeriod
		}
		cutoff := time.Now().Add(-gracePeriod)
		candidates, listErr := lister.ListBlocksBefore(ctx, cutoff, int(limit))
		if listErr != nil {
			capturedBlockErrs = append(capturedBlockErrs, listErr)
		} else {
			for _, cand := range candidates {
				if err := ctx.Err(); err != nil {
					capturedBlockErrs = append(capturedBlockErrs, err)
					break
				}
				if cand.SliceID == 0 || cand.SliceID > math.MaxInt64 {
					continue
				}
				_, getErr := m.store.GetSliceRef(ctx, int64(cand.SliceID))
				if getErr == nil {
					// 在 slice_refs 中存在记录（无论 refs > 0 还是 refs <= 0），绝不越权删除
					continue
				}
				if !errors.Is(getErr, pgx.ErrNoRows) {
					capturedBlockErrs = append(capturedBlockErrs, getErr)
					continue
				}
				if delErr := bs.DeleteBlock(ctx, cand.SliceID, cand.BlockIdx, cand.BlockSize); delErr != nil {
					capturedBlockErrs = append(capturedBlockErrs, delErr)
					continue
				}
				totalReclaimed++
			}
		}
	}

	if len(capturedBlockErrs) > 0 {
		return totalReclaimed, errors.Join(capturedBlockErrs...)
	}

	return totalReclaimed, nil
}

// startGCWorkerWithDone 在后台启动周期性物理垃圾回收协程并返回 doneCh 通道。
// 协程完全退出排空后关闭 doneCh。
// 若 interval <= 0 则采用 DefaultGCInterval；若 batchLimit <= 0 则采用 DefaultGCBatchLimit。
func (m *manager) startGCWorkerWithDone(ctx context.Context, interval time.Duration, batchLimit int32) <-chan struct{} {
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	if batchLimit <= 0 {
		batchLimit = DefaultGCBatchLimit
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for {
					if ctx.Err() != nil {
						return
					}
					n, err := m.CollectGarbage(ctx, batchLimit)
					if err != nil || n < int(batchLimit) {
						break
					}
				}
			}
		}
	}()

	return doneCh
}

// StartGCWorker 在后台启动周期性物理垃圾回收协程。
// 当 ctx 取消时自动平滑退出。
// 若 interval <= 0 则采用 DefaultGCInterval；若 batchLimit <= 0 则采用 DefaultGCBatchLimit。
func (m *manager) StartGCWorker(ctx context.Context, interval time.Duration, batchLimit int32) {
	_ = m.startGCWorkerWithDone(ctx, interval, batchLimit)
}
