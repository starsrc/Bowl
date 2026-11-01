package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type writeBatchTask struct {
	ctx           context.Context
	req           *WriteRequest
	dataLen       int
	sliceID       int64
	stagedDirs    []string
	cleanupBlocks func()
	doneCh        chan struct{}

	resp             *WriteResponse
	err              error
	bizErr           error
	actualOffset     uint64
	evolvedSizeAfter int64
	sliceRefs        int32
}

type handleCacheKey struct {
	ino       uint64
	fh        uint64
	sessionID pgtype.Int8
}

type handleCacheEntry struct {
	handle db.FileHandle
	bizErr error
}

type sessionTouchKey struct {
	sessionID       uint64
	handleCreatedAt int64
	valid           bool
}

type batchChunkAppendStat struct {
	ino      uint64
	indx     int64
	octetLen int64
}

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
// 遵循 ADR-0019、ADR-0022 与 ADR-0024 RocksDB 式 WriteThread 两阶段流水线 Group Commit 架构：
// 1. 静态边界校验：参数非空、(ino, fh, offset) 在正数范围内、len(data) <= 4 MiB、offset + len(data) <= MaxInt64；
// 2. 零字节写入：在事务内锁定 Inode 校验常规文件类型与写句柄有效性，续期会话，不分配 slice_id 且不修改任何元数据；
// 3. 两阶段流水线落盘与 Group Commit：
//   - 阶段 1（无锁并行写入页缓存）：从内存号段分配器领取 slice_id，将 data 写入底层物理块存储（StageBlock 写入同目录临时文件并 Rename 至正式路径，发布到 OS Page Cache）；失败立即清理物理块返回 ErrIO；
//   - 阶段 2（WriteThread Leader-Follower 单事务 Group Commit）：
//   - 步骤 2a：在单个 PostgreSQL 事务内启用 SET LOCAL synchronous_commit = off，按 Inode 升序加锁、按句柄/会话去重校验、按 RootIno 升序锁盘并在内存中依次推演各请求偏移（含 O_APPEND）与配额增量、按 (ino, indx) 拼接追加 Slice、登记 slice_refs、更新 Inode 属性并全组共享 1 次事务 COMMIT；
//   - 步骤 2b：对批次内被业务规则隔离剔除或事务回滚未提交的任务立即调用 DeleteBlock 清理物理块（零孤儿块残留），触发异步 Compaction 并批量唤醒 Follower。
func (m *manager) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh == 0 || req.Fh > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	dataLen := len(req.Data)
	if dataLen > BlockSize {
		return nil, ErrInvalidArgument
	}
	if req.Offset > uint64(math.MaxInt64)-uint64(dataLen) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}

	// 零字节写入：校验 Inode 与句柄有效性并续期会话，直接返回，不触发物理落盘与元数据修改
	if dataLen == 0 {
		return m.writeZeroBytes(ctx, req)
	}

	m.writeInFlightStage1.Add(1)

	// 阶段 1：行锁外从内存号段分配器领取 slice_id 与并发落盘物理块
	sliceID, err := m.allocSliceID(ctx)
	if err != nil {
		m.onStage1Aborted()
		return nil, fmt.Errorf("next slice id: %w", err)
	}

	bs := m.getBlockStore()
	stagedBS, hasStagedBS := bs.(StagedBlockStore)
	blocks := SliceBlocks(uint32(dataLen))
	writtenBlocks := make([]SliceBlockInfo, 0, len(blocks))
	var stagedDirs []string
	if hasStagedBS {
		stagedDirs = make([]string, 0, len(blocks))
	}

	var cleanupOnce sync.Once
	cleanupBlocks := func() {
		cleanupOnce.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, b := range writtenBlocks {
				_ = bs.DeleteBlock(cleanupCtx, uint64(sliceID), b.BlockIdx, b.BlockSize)
			}
		})
	}

	for _, b := range blocks {
		start := b.BlockIdx * BlockSize
		end := start + b.BlockSize
		blockData := req.Data[start:end]
		if hasStagedBS {
			dir, err := stagedBS.StageBlock(ctx, uint64(sliceID), b.BlockIdx, blockData)
			if err != nil {
				cleanupBlocks()
				m.onStage1Aborted()
				return nil, fmt.Errorf("%w: put block (slice_id=%d, block_idx=%d): %v", ErrIO, sliceID, b.BlockIdx, err)
			}
			writtenBlocks = append(writtenBlocks, b)
			if dir != "" {
				stagedDirs = append(stagedDirs, dir)
			}
		} else {
			if err := bs.PutBlock(ctx, uint64(sliceID), b.BlockIdx, blockData); err != nil {
				cleanupBlocks()
				m.onStage1Aborted()
				return nil, fmt.Errorf("%w: put block (slice_id=%d, block_idx=%d): %v", ErrIO, sliceID, b.BlockIdx, err)
			}
			writtenBlocks = append(writtenBlocks, b)
		}
	}

	task := &writeBatchTask{
		ctx:           ctx,
		req:           req,
		dataLen:       dataLen,
		sliceID:       sliceID,
		stagedDirs:    stagedDirs,
		cleanupBlocks: cleanupBlocks,
		doneCh:        make(chan struct{}),
	}

	return m.submitWriteTask(task)
}

func (m *manager) notifyWriteCoalesce() {
	if m.writeCoalesceCh == nil {
		return
	}
	select {
	case m.writeCoalesceCh <- struct{}{}:
	default:
	}
}

func (m *manager) onStage1Aborted() {
	m.writeInFlightStage1.Add(-1)
	m.notifyWriteCoalesce()
}

func (m *manager) submitWriteTask(task *writeBatchTask) (*WriteResponse, error) {
	m.writeGroupMu.Lock()
	m.writeInFlightStage1.Add(-1)
	m.writePendingQueue = append(m.writePendingQueue, task)
	m.notifyWriteCoalesce()
	if m.writeLeaderActive {
		m.writeGroupMu.Unlock()
		<-task.doneCh
		return task.resp, task.err
	}
	m.writeLeaderActive = true
	m.writeGroupMu.Unlock()

	m.runWriteLeaderLoop()
	<-task.doneCh
	return task.resp, task.err
}

func (m *manager) runWriteLeaderLoop() {
	maxBatch := m.writeGroupMaxBatchSize
	if maxBatch <= 0 {
		maxBatch = DefaultWriteGroupMaxBatchSize
	}

	for {
		m.writeGroupMu.Lock()
		if len(m.writePendingQueue) == 0 {
			m.writeLeaderActive = false
			m.writeGroupMu.Unlock()
			return
		}

		// 若队列中仅有 1 个任务且无显式在途阶段 1，让出一次调度器时间片以接纳同波并发启动的协程
		if len(m.writePendingQueue) == 1 && m.writeInFlightStage1.Load() == 0 && maxBatch > 1 {
			m.writeGroupMu.Unlock()
			runtime.Gosched()
			m.writeGroupMu.Lock()
		}

		// 当存在其他并发 Write 请求正处于阶段 1 落盘途中且当前批次未满时，短暂汇合等待同批请求进入阶段 2
		if len(m.writePendingQueue) < maxBatch && m.writeInFlightStage1.Load() > 0 && m.writeCoalesceCh != nil {
			m.writeGroupMu.Unlock()
			timer := time.NewTimer(5 * time.Millisecond)
			for {
				m.writeGroupMu.Lock()
				ready := len(m.writePendingQueue) >= maxBatch || m.writeInFlightStage1.Load() == 0
				m.writeGroupMu.Unlock()
				if ready {
					break
				}
				select {
				case <-m.writeCoalesceCh:
				case <-timer.C:
					goto drainBatch
				}
			}
		drainBatch:
			timer.Stop()
			m.writeGroupMu.Lock()
		}

		batchLen := len(m.writePendingQueue)
		if batchLen == 0 {
			m.writeLeaderActive = false
			m.writeGroupMu.Unlock()
			return
		}
		if batchLen > maxBatch {
			batchLen = maxBatch
		}
		batch := make([]*writeBatchTask, batchLen)
		copy(batch, m.writePendingQueue[:batchLen])
		for i := 0; i < batchLen; i++ {
			m.writePendingQueue[i] = nil
		}
		m.writePendingQueue = m.writePendingQueue[batchLen:]
		m.writeGroupMu.Unlock()

		m.executeWriteBatch(batch)
	}
}

func buildBatchWriteContext(tasks []*writeBatchTask) (context.Context, context.CancelFunc) {
	if len(tasks) == 0 {
		return context.Background(), func() {}
	}
	if len(tasks) == 1 {
		return tasks[0].ctx, func() {}
	}
	baseCtx := context.WithoutCancel(tasks[0].ctx)
	for _, t := range tasks {
		if t.ctx.Done() == nil {
			// 至少有一个任务不可取消，整批事务无需挂载取消监听
			return baseCtx, func() {}
		}
	}

	batchCtx, cancel := context.WithCancel(baseCtx)
	stopCh := make(chan struct{})
	go func() {
		for _, t := range tasks {
			select {
			case <-t.ctx.Done():
			case <-stopCh:
				return
			}
		}
		cancel()
	}()
	return batchCtx, func() {
		close(stopCh)
		cancel()
	}
}

func (m *manager) executeWriteBatch(batch []*writeBatchTask) {
	defer func() {
		for _, t := range batch {
			close(t.doneCh)
		}
	}()

	// 单任务快速路径：直接使用原请求 ctx 执行，100% 保持单请求行为与错误文本
	if len(batch) == 1 {
		t := batch[0]
		t.resp, t.err = m.executeSingleWriteTask(t)
		return
	}

	// 多任务批次路径：先隔离过滤在进入阶段 2 前已被调用方取消的任务
	validTasks := make([]*writeBatchTask, 0, len(batch))
	for _, t := range batch {
		if err := t.ctx.Err(); err != nil {
			t.cleanupBlocks()
			t.err = fmt.Errorf("write commit tx: %w", err)
			continue
		}
		validTasks = append(validTasks, t)
	}
	if len(validTasks) == 0 {
		return
	}
	if len(validTasks) == 1 {
		t := validTasks[0]
		t.resp, t.err = m.executeSingleWriteTask(t)
		return
	}

	batchCtx, batchCancel := buildBatchWriteContext(validTasks)
	defer batchCancel()

	m.executeBatchWriteTasks(batchCtx, validTasks)
}

func mapWriteBizError(err error) error {
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return ErrInvalidArgument
	case errors.Is(err, ErrNotFound):
		return ErrNotFound
	case errors.Is(err, ErrIsDir):
		return ErrIsDir
	case errors.Is(err, ErrBadFileHandle):
		return ErrBadFileHandle
	case errors.Is(err, ErrNoSpace):
		return ErrNoSpace
	case errors.Is(err, ErrIO):
		return err
	default:
		return fmt.Errorf("write commit tx: %w", err)
	}
}

func (m *manager) executeSingleWriteTask(t *writeBatchTask) (*WriteResponse, error) {
	ctx := t.ctx
	req := t.req
	dataLen := t.dataLen
	sliceID := t.sliceID

	var committedStats []chunkAppendStat
	var everReachedCommit bool
	var resp *WriteResponse
	var lastTouchRollback func()
	txErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if lastTouchRollback != nil {
			lastTouchRollback()
			lastTouchRollback = nil
		}
		everReachedCommit = false
		committedStats = nil
		if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r != nil && r.RawDBTX() != nil {
			if _, err := r.RawDBTX().Exec(ctx, "SET LOCAL synchronous_commit = off"); err != nil {
				return fmt.Errorf("%w: set local synchronous_commit: %v", ErrIO, err)
			}
		}
		inode, err := q.GetInodeForUpdate(ctx, int64(req.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", req.Ino, err)
		}

		mode := uint32(inode.Mode)
		if (mode & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}
		if (mode & S_IFMT) != S_IFREG {
			return ErrInvalidArgument
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(req.Fh),
			Ino:       int64(req.Ino),
			SessionID: toPgSessionID(req.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", req.Fh, req.Ino, err)
		}

		accMode := uint32(handle.Flags) & O_ACCMODE
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if req.SessionID != nil {
			rollbackTouch, err := m.touchWriteClientSessionForHandle(ctx, q, *req.SessionID, handle.CreatedAt)
			if err != nil {
				return err
			}
			lastTouchRollback = rollbackTouch
		}

		actualOffset := req.Offset
		if (uint32(handle.Flags) & O_APPEND) != 0 {
			actualOffset = uint64(inode.Size)
		}
		if actualOffset > uint64(math.MaxInt64)-uint64(dataLen) {
			return ErrInvalidArgument
		}

		newEnd := int64(actualOffset) + int64(dataLen)
		newSize := inode.Size
		if newEnd > newSize {
			newSize = newEnd
			if inode.RootIno > 0 {
				deltaBytes := newEnd - inode.Size
				if err := m.reserveQuotaBytes(ctx, q, inode.RootIno, deltaBytes); err != nil {
					return err
				}
			}
		}

		startIndx := int64(actualOffset / ChunkSize)
		endIndx := int64((actualOffset + uint64(dataLen) - 1) / ChunkSize)

		if startIndx == endIndx {
			sliceDesc := Slice{
				Pos:  uint32(actualOffset % ChunkSize),
				ID:   uint64(sliceID),
				Size: uint32(dataLen),
				Off:  0,
				Len:  uint32(dataLen),
			}
			sliceBytes := EncodeSlice(sliceDesc)

			octetLen, err := q.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
				Ino:    int64(req.Ino),
				Indx:   startIndx,
				Slices: sliceBytes,
			})
			if err != nil {
				return fmt.Errorf("append chunk slice (ino=%d, indx=%d): %w", req.Ino, startIndx, err)
			}
			committedStats = append(committedStats, chunkAppendStat{indx: startIndx, octetLen: octetLen})

			if err := q.InsertSliceRef(ctx, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    int32(dataLen),
				Refs:    1,
			}); err != nil {
				return fmt.Errorf("insert slice ref (slice_id=%d): %w", sliceID, err)
			}
		} else {
			firstPos := uint32(actualOffset % ChunkSize)
			firstLen := uint32(ChunkSize) - firstPos
			secondLen := uint32(dataLen) - firstLen

			firstSliceBytes := EncodeSlice(Slice{
				Pos:  firstPos,
				ID:   uint64(sliceID),
				Size: uint32(dataLen),
				Off:  0,
				Len:  firstLen,
			})
			octetLen1, err := q.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
				Ino:    int64(req.Ino),
				Indx:   startIndx,
				Slices: firstSliceBytes,
			})
			if err != nil {
				return fmt.Errorf("append chunk slice first chunk (ino=%d, indx=%d): %w", req.Ino, startIndx, err)
			}
			committedStats = append(committedStats, chunkAppendStat{indx: startIndx, octetLen: octetLen1})

			secondSliceBytes := EncodeSlice(Slice{
				Pos:  0,
				ID:   uint64(sliceID),
				Size: uint32(dataLen),
				Off:  firstLen,
				Len:  secondLen,
			})
			octetLen2, err := q.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
				Ino:    int64(req.Ino),
				Indx:   startIndx + 1,
				Slices: secondSliceBytes,
			})
			if err != nil {
				return fmt.Errorf("append chunk slice second chunk (ino=%d, indx=%d): %w", req.Ino, startIndx+1, err)
			}
			committedStats = append(committedStats, chunkAppendStat{indx: startIndx + 1, octetLen: octetLen2})

			if err := q.InsertSliceRef(ctx, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    int32(dataLen),
				Refs:    2,
			}); err != nil {
				return fmt.Errorf("insert slice ref (slice_id=%d): %w", sliceID, err)
			}
		}
		newMode := int32(uint32(inode.Mode) &^ (S_ISUID | S_ISGID))

		updatedInode, err := q.UpdateInodeSizeAndTouchForWrite(ctx, db.UpdateInodeSizeAndTouchForWriteParams{
			Ino:  int64(req.Ino),
			Size: newSize,
			Mode: newMode,
		})
		if err != nil {
			return fmt.Errorf("update inode size and touch for write (ino=%d): %w", req.Ino, err)
		}

		resp = &WriteResponse{
			Size:  uint32(dataLen),
			Inode: m.newInode(updatedInode),
		}
		everReachedCommit = true
		return nil
	})

	if txErr != nil {
		if lastTouchRollback != nil {
			lastTouchRollback()
			lastTouchRollback = nil
		}
		// 当 everReachedCommit 为 false 时，事务未进入提交阶段，安全清理在途写入物理块；
		// 若已进入 COMMIT 阶段（everReachedCommit 为 true），提交结果未知或在途，
		// 必须保守保留物理块，杜绝误删即将提交完成的数据，未引用的残留块由后台 GC 回收。
		if !everReachedCommit {
			t.cleanupBlocks()
		}

		m.handleBadFileHandleSessionCleanup(ctx, req.SessionID, txErr)
		return nil, mapWriteBizError(txErr)
	}

	for _, stat := range committedStats {
		m.maybeTriggerAsyncCompact(req.Ino, stat.indx, stat.octetLen)
	}

	return resp, nil
}

// executeBatchWriteTasks 在单个 PostgreSQL 事务内合并提交多个 Write 请求，并隔离剔除单条业务违规请求。
func (m *manager) executeBatchWriteTasks(batchCtx context.Context, validTasks []*writeBatchTask) {
	var committedStats []batchChunkAppendStat
	var everReachedCommit bool
	var touchRollbacks []func()

	txErr := m.execWriteTxWithRetry(batchCtx, func(q db.Querier) error {
		for i := len(touchRollbacks) - 1; i >= 0; i-- {
			if touchRollbacks[i] != nil {
				touchRollbacks[i]()
			}
		}
		touchRollbacks = nil
		everReachedCommit = false
		committedStats = nil
		if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r != nil && r.RawDBTX() != nil {
			if _, err := r.RawDBTX().Exec(batchCtx, "SET LOCAL synchronous_commit = off"); err != nil {
				return fmt.Errorf("%w: set local synchronous_commit: %v", ErrIO, err)
			}
		}

		for _, t := range validTasks {
			t.resp = nil
			t.err = nil
			t.bizErr = nil
			t.actualOffset = 0
			t.evolvedSizeAfter = 0
			t.sliceRefs = 0
		}

		// 1. 提取批次内唯一 Inode 编号并按升序加排他锁（杜绝跨文件并发写死锁）
		uniqueInos := make([]uint64, 0, len(validTasks))
		seenInos := make(map[uint64]struct{}, len(validTasks))
		for _, t := range validTasks {
			if _, ok := seenInos[t.req.Ino]; !ok {
				seenInos[t.req.Ino] = struct{}{}
				uniqueInos = append(uniqueInos, t.req.Ino)
			}
		}
		slices.Sort(uniqueInos)

		inodes := make(map[uint64]db.Inode, len(uniqueInos))
		inodeNotFound := make(map[uint64]bool)
		evolvedSize := make(map[uint64]int64, len(uniqueInos))

		for _, ino := range uniqueInos {
			inode, err := q.GetInodeForUpdate(batchCtx, int64(ino))
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					inodeNotFound[ino] = true
					continue
				}
				return fmt.Errorf("get inode %d for update: %w", ino, err)
			}
			inodes[ino] = inode
			evolvedSize[ino] = inode.Size
		}

		// 2. 校验 Inode 类型、按 (ino, fh, sessionID) 去重校验 FileHandle 与 ClientSession
		handleCache := make(map[handleCacheKey]handleCacheEntry, len(validTasks))
		sessionTouchCache := make(map[sessionTouchKey]error)
		seenRootInos := make(map[int64]struct{})
		var uniqueRootInos []int64

		for _, t := range validTasks {
			ino := t.req.Ino
			if inodeNotFound[ino] {
				t.bizErr = ErrNotFound
				continue
			}
			inode := inodes[ino]
			mode := uint32(inode.Mode)
			if (mode & S_IFMT) == S_IFDIR {
				t.bizErr = ErrIsDir
				continue
			}
			if (mode & S_IFMT) != S_IFREG {
				t.bizErr = ErrInvalidArgument
				continue
			}

			hKey := handleCacheKey{
				ino:       ino,
				fh:        t.req.Fh,
				sessionID: toPgSessionID(t.req.SessionID),
			}
			hEntry, cached := handleCache[hKey]
			if !cached {
				handle, err := q.GetFileHandle(batchCtx, db.GetFileHandleParams{
					Fh:        int64(t.req.Fh),
					Ino:       int64(ino),
					SessionID: hKey.sessionID,
				})
				if err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						hEntry = handleCacheEntry{bizErr: ErrBadFileHandle}
						handleCache[hKey] = hEntry
					} else {
						return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", t.req.Fh, ino, err)
					}
				} else {
					accMode := uint32(handle.Flags) & O_ACCMODE
					if accMode != O_WRONLY && accMode != O_RDWR {
						hEntry = handleCacheEntry{handle: handle, bizErr: ErrBadFileHandle}
					} else if t.req.SessionID != nil {
						sKey := sessionTouchKey{
							sessionID:       *t.req.SessionID,
							handleCreatedAt: handle.CreatedAt.Time.UnixNano(),
							valid:           handle.CreatedAt.Valid,
						}
						if touchErr, sCached := sessionTouchCache[sKey]; sCached {
							hEntry = handleCacheEntry{handle: handle, bizErr: touchErr}
						} else {
							rollbackTouch, touchErr := m.touchWriteClientSessionForHandle(batchCtx, q, *t.req.SessionID, handle.CreatedAt)
							if touchErr != nil {
								if errors.Is(touchErr, ErrBadFileHandle) {
									sessionTouchCache[sKey] = touchErr
									hEntry = handleCacheEntry{handle: handle, bizErr: touchErr}
								} else {
									return touchErr
								}
							} else {
								if rollbackTouch != nil {
									touchRollbacks = append(touchRollbacks, rollbackTouch)
								}
								sessionTouchCache[sKey] = nil
								hEntry = handleCacheEntry{handle: handle}
							}
						}
					} else {
						hEntry = handleCacheEntry{handle: handle}
					}
					handleCache[hKey] = hEntry
				}
			}

			if hEntry.bizErr != nil {
				t.bizErr = hEntry.bizErr
				continue
			}

			// 若该合法写入任务可能扩容且所属 Inode 关联了 Disk (RootIno > 0)，登记待锁定的 RootIno
			if inode.RootIno > 0 {
				isAppend := (uint32(hEntry.handle.Flags) & O_APPEND) != 0
				if isAppend {
					if uint64(inode.Size) <= uint64(math.MaxInt64)-uint64(t.dataLen) {
						if _, ok := seenRootInos[inode.RootIno]; !ok {
							seenRootInos[inode.RootIno] = struct{}{}
							uniqueRootInos = append(uniqueRootInos, inode.RootIno)
						}
					}
				} else if int64(t.req.Offset)+int64(t.dataLen) > inode.Size {
					if _, ok := seenRootInos[inode.RootIno]; !ok {
						seenRootInos[inode.RootIno] = struct{}{}
						uniqueRootInos = append(uniqueRootInos, inode.RootIno)
					}
				}
			}
		}

		// 3. 按 RootIno 升序校验并预留涉及扩容任务的内存配额账本
		slices.Sort(uniqueRootInos)
		hasDisk := make(map[int64]bool, len(uniqueRootInos))
		for _, rootIno := range uniqueRootInos {
			_, found, err := m.ensureDiskQuotaLoadedInTx(batchCtx, q, rootIno)
			if err != nil {
				return err
			}
			if found {
				hasDisk[rootIno] = true
			}
		}

		// 4. 按批次内任务到达顺序依次推演偏移、文件大小演进与内存配额预留
		acceptedTasks := make([]*writeBatchTask, 0, len(validTasks))
		hasAcceptedForIno := make(map[uint64]bool, len(uniqueInos))

		for _, t := range validTasks {
			if t.bizErr != nil {
				continue
			}
			ino := t.req.Ino
			inode := inodes[ino]
			hEntry := handleCache[handleCacheKey{
				ino:       ino,
				fh:        t.req.Fh,
				sessionID: toPgSessionID(t.req.SessionID),
			}]

			actualOffset := t.req.Offset
			if (uint32(hEntry.handle.Flags) & O_APPEND) != 0 {
				actualOffset = uint64(evolvedSize[ino])
			}
			if actualOffset > uint64(math.MaxInt64)-uint64(t.dataLen) {
				t.bizErr = ErrInvalidArgument
				continue
			}

			newEnd := int64(actualOffset) + int64(t.dataLen)
			curSize := evolvedSize[ino]
			var taskDelta int64
			if newEnd > curSize {
				taskDelta = newEnd - curSize
			}

			if taskDelta > 0 && inode.RootIno > 0 && hasDisk[inode.RootIno] {
				if err := m.reserveQuotaBytes(batchCtx, q, inode.RootIno, taskDelta); err != nil {
					if errors.Is(err, ErrNoSpace) {
						t.bizErr = ErrNoSpace
						continue
					}
					return err
				}
			}

			if newEnd > curSize {
				evolvedSize[ino] = newEnd
			}
			t.actualOffset = actualOffset
			t.evolvedSizeAfter = evolvedSize[ino]
			hasAcceptedForIno[ino] = true
			acceptedTasks = append(acceptedTasks, t)
		}

		if len(acceptedTasks) == 0 {
			everReachedCommit = true
			return nil
		}

		// 5. 批量落库修改：按 (ino, indx) 合并追加 Slice -> 登记 slice_refs -> 按 ino 更新 Inode

		chunkSlicesMap := make(map[chunkKey][]byte, len(acceptedTasks))
		chunkKeyOrder := make([]chunkKey, 0, len(acceptedTasks))
		appendSliceToMap := func(ck chunkKey, sliceBytes []byte) {
			if _, exists := chunkSlicesMap[ck]; !exists {
				chunkKeyOrder = append(chunkKeyOrder, ck)
			}
			chunkSlicesMap[ck] = append(chunkSlicesMap[ck], sliceBytes...)
		}

		for _, t := range acceptedTasks {
			startIndx := int64(t.actualOffset / ChunkSize)
			endIndx := int64((t.actualOffset + uint64(t.dataLen) - 1) / ChunkSize)
			if startIndx == endIndx {
				sliceBytes := EncodeSlice(Slice{
					Pos:  uint32(t.actualOffset % ChunkSize),
					ID:   uint64(t.sliceID),
					Size: uint32(t.dataLen),
					Off:  0,
					Len:  uint32(t.dataLen),
				})
				appendSliceToMap(chunkKey{ino: t.req.Ino, indx: startIndx}, sliceBytes)
				t.sliceRefs = 1
			} else {
				firstPos := uint32(t.actualOffset % ChunkSize)
				firstLen := uint32(ChunkSize) - firstPos
				secondLen := uint32(t.dataLen) - firstLen

				firstSliceBytes := EncodeSlice(Slice{
					Pos:  firstPos,
					ID:   uint64(t.sliceID),
					Size: uint32(t.dataLen),
					Off:  0,
					Len:  firstLen,
				})
				appendSliceToMap(chunkKey{ino: t.req.Ino, indx: startIndx}, firstSliceBytes)

				secondSliceBytes := EncodeSlice(Slice{
					Pos:  0,
					ID:   uint64(t.sliceID),
					Size: uint32(t.dataLen),
					Off:  firstLen,
					Len:  secondLen,
				})
				appendSliceToMap(chunkKey{ino: t.req.Ino, indx: startIndx + 1}, secondSliceBytes)
				t.sliceRefs = 2
			}
		}

		for _, ck := range chunkKeyOrder {
			octetLen, err := q.AppendChunkSlice(batchCtx, db.AppendChunkSliceParams{
				Ino:    int64(ck.ino),
				Indx:   ck.indx,
				Slices: chunkSlicesMap[ck],
			})
			if err != nil {
				return fmt.Errorf("append chunk slice (ino=%d, indx=%d): %w", ck.ino, ck.indx, err)
			}
			committedStats = append(committedStats, batchChunkAppendStat{
				ino:      ck.ino,
				indx:     ck.indx,
				octetLen: octetLen,
			})
		}

		for _, t := range acceptedTasks {
			if err := q.InsertSliceRef(batchCtx, db.InsertSliceRefParams{
				SliceID: t.sliceID,
				Size:    int32(t.dataLen),
				Refs:    t.sliceRefs,
			}); err != nil {
				return fmt.Errorf("insert slice ref (slice_id=%d): %w", t.sliceID, err)
			}
		}

		updatedInodes := make(map[uint64]db.Inode, len(uniqueInos))
		for _, ino := range uniqueInos {
			if !hasAcceptedForIno[ino] {
				continue
			}
			inode := inodes[ino]
			newMode := int32(uint32(inode.Mode) &^ (S_ISUID | S_ISGID))
			updatedInode, err := q.UpdateInodeSizeAndTouchForWrite(batchCtx, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: evolvedSize[ino],
				Mode: newMode,
			})
			if err != nil {
				return fmt.Errorf("update inode size and touch for write (ino=%d): %w", ino, err)
			}
			updatedInodes[ino] = updatedInode
		}

		for _, t := range acceptedTasks {
			snap := updatedInodes[t.req.Ino]
			snap.Size = t.evolvedSizeAfter
			t.resp = &WriteResponse{
				Size:  uint32(t.dataLen),
				Inode: m.newInode(snap),
			}
		}

		everReachedCommit = true
		return nil
	})

	if txErr != nil {
		for i := len(touchRollbacks) - 1; i >= 0; i-- {
			if touchRollbacks[i] != nil {
				touchRollbacks[i]()
			}
		}
		touchRollbacks = nil

		cleanedSessions := make(map[uint64]struct{})
		for _, t := range validTasks {
			if !everReachedCommit || t.bizErr != nil {
				t.cleanupBlocks()
			}
			if errors.Is(t.bizErr, ErrBadFileHandle) && t.req.SessionID != nil {
				if _, ok := cleanedSessions[*t.req.SessionID]; !ok {
					cleanedSessions[*t.req.SessionID] = struct{}{}
					m.handleBadFileHandleSessionCleanup(t.ctx, t.req.SessionID, t.bizErr)
				}
			}
			t.resp = nil
			t.err = mapWriteBizError(txErr)
		}
		return
	}

	cleanedSessions := make(map[uint64]struct{})
	for _, t := range validTasks {
		if t.bizErr != nil {
			t.cleanupBlocks()
			if errors.Is(t.bizErr, ErrBadFileHandle) && t.req.SessionID != nil {
				if _, ok := cleanedSessions[*t.req.SessionID]; !ok {
					cleanedSessions[*t.req.SessionID] = struct{}{}
					m.handleBadFileHandleSessionCleanup(t.ctx, t.req.SessionID, t.bizErr)
				}
			}
			t.resp = nil
			t.err = mapWriteBizError(t.bizErr)
		}
	}

	for _, stat := range committedStats {
		m.maybeTriggerAsyncCompact(stat.ino, stat.indx, stat.octetLen)
	}
}

// writeZeroBytes 处理 0 字节写入探针。
func (m *manager) writeZeroBytes(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	var resp *WriteResponse
	var lastTouchRollback func()
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		if lastTouchRollback != nil {
			lastTouchRollback()
			lastTouchRollback = nil
		}
		inode, err := q.GetInodeForUpdate(ctx, int64(req.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", req.Ino, err)
		}

		mode := uint32(inode.Mode)
		if (mode & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}
		if (mode & S_IFMT) != S_IFREG {
			return ErrInvalidArgument
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(req.Fh),
			Ino:       int64(req.Ino),
			SessionID: toPgSessionID(req.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", req.Fh, req.Ino, err)
		}

		accMode := uint32(handle.Flags) & O_ACCMODE
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if req.SessionID != nil {
			rollbackTouch, err := m.touchWriteClientSessionForHandle(ctx, q, *req.SessionID, handle.CreatedAt)
			if err != nil {
				return err
			}
			lastTouchRollback = rollbackTouch
		}

		resp = &WriteResponse{
			Size:  0,
			Inode: m.newInode(inode),
		}
		return nil
	})

	if err != nil {
		if lastTouchRollback != nil {
			lastTouchRollback()
			lastTouchRollback = nil
		}
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			m.handleBadFileHandleSessionCleanup(ctx, req.SessionID, err)
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("write zero bytes tx: %w", err)
		}
	}

	return resp, nil
}
