package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// ProjectChunkSlices 计算当前 Chunk 在 [chunkReadStart, chunkReadEnd) 区间内命中的投影切片列表。
// 内部通过 BuildSlice 构建紧致视图，并对有效交集切片做区间裁剪计算。
func ProjectChunkSlices(rawSlices []Slice, chunkReadStart, chunkReadEnd uint32) []Slice {
	view := BuildSlice(rawSlices)
	var result []Slice
	for _, s := range view {
		overlapStart := max(s.Pos, chunkReadStart)
		overlapEnd := min(s.Pos+s.Len, chunkReadEnd)
		if overlapStart >= overlapEnd {
			continue
		}
		overlapLen := overlapEnd - overlapStart
		sliceOff := s.Off + (overlapStart - s.Pos)
		result = append(result, Slice{
			Pos:  overlapStart,
			ID:   s.ID,
			Size: s.Size,
			Off:  sliceOff,
			Len:  overlapLen,
		})
	}
	return result
}

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
// 遵循 ADR-0019 数据层架构与 POSIX pread(2) 语义：
//  1. 静态边界校验：参数非空、(ino, fh, offset) 在合法正数范围内、size <= BlockSize (4 MiB)、session_id 合法；
//  2. 权限与类型校验：在事务内校验目标 Inode 必须为常规文件 S_IFREG（目录返回 ErrIsDir，其他非常规文件返回 ErrInvalidArgument），
//     校验 (ino, fh) 对应的 FileHandle 存在且具备读权限（O_RDONLY 或 O_RDWR；若是 O_WRONLY、句柄不存在或 ino 不匹配均返回 ErrBadFileHandle）；
//  3. 快照原子读取：使用单条语句 GetInodeSizeAndChunksInRange 一次性原子获取同一快照时刻的 (fileSize, chunks)，
//     杜绝跨 Chunk 循环多次 GetChunk 造成的并发写快照读撕裂；
//  4. 会话续期与访问时间：在传入有效 session_id 时续期会话租约；遵循配置的 AtimeMode（noatime/relatime/strictatime）决定是否更新 inodes.atime；
//  5. EOF 与截断读取：当 size == 0 或 offset >= inode.Size 时，直接返回空字节流（len(data) == 0）；
//     当 offset + size > inode.Size 时，自动截断实际读取长度 readLen = uint32(inode.Size - offset)；
//  6. 在途读生命周期保护与物理块拼接：
//     - 对各 Chunk 调用 ProjectChunkSlices 计算本次读取区间实际命中的物理片段，提取非零物理 slice_id 并去重升序；
//     - 若存在非零 slice_id，在同一事务内调用 LockValidSliceRefsKeyShare 锁定 FOR KEY SHARE；
//     若在亚微秒竞争窗口中锁定数量不足，在有界重试内重新执行快照查询与锁定；
//     - 在持有 FOR KEY SHARE 读保护事务期间，并发 CollectGarbage 自动跳过被锁切片（SKIP LOCKED）；
//     - 从 BlockStore 读取物理块并拼入返回缓冲区，提交事务释放锁。
func (m *manager) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
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
	if req.Size > BlockSize {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}

	var resp *ReadResponse
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		startIndx := int32(req.Offset / ChunkSize)
		endIndx := startIndx
		if req.Size > 0 {
			endIndx = int32((req.Offset + uint64(req.Size) - 1) / ChunkSize)
		}

		rows, err := q.GetInodeSizeAndChunksInRange(ctx, db.GetInodeSizeAndChunksInRangeParams{
			Ino:       int64(req.Ino),
			StartIndx: startIndx,
			EndIndx:   endIndx,
		})
		if err != nil {
			return fmt.Errorf("get inode size and chunks (ino=%d): %w", req.Ino, err)
		}
		if len(rows) == 0 {
			return ErrNotFound
		}

		firstRow := rows[0]
		mode := uint32(firstRow.Mode)
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
		if accMode != O_RDONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if req.SessionID != nil {
			if err := m.touchActiveClientSessionForHandle(ctx, q, *req.SessionID, handle.CreatedAt); err != nil {
				return err
			}
		}

		// 到达 EOF 或请求大小为 0 时直接返回空字节流
		if req.Size == 0 || req.Offset >= uint64(firstRow.Size) {
			resp = &ReadResponse{Data: []byte{}}
			return nil
		}

		type chunkPlan struct {
			indx      uint32
			slices    []Slice
			bufOffset uint32
			readStart uint32
			readEnd   uint32
		}

		const maxLockRetries = 8
		var (
			finalPlans  []chunkPlan
			finalBuf    []byte
			latestInode db.GetInodeSizeAndChunksInRangeRow
		)

		currentRows := rows
		for attempt := 0; attempt < maxLockRetries; attempt++ {
			if attempt > 0 {
				currentRows, err = q.GetInodeSizeAndChunksInRange(ctx, db.GetInodeSizeAndChunksInRangeParams{
					Ino:       int64(req.Ino),
					StartIndx: startIndx,
					EndIndx:   endIndx,
				})
				if err != nil {
					return fmt.Errorf("re-get inode size and chunks (ino=%d): %w", req.Ino, err)
				}
				if len(currentRows) == 0 {
					return ErrNotFound
				}
			}

			latestInode = currentRows[0]
			if req.Offset >= uint64(latestInode.Size) {
				resp = &ReadResponse{Data: []byte{}}
				return nil
			}

			readLen := req.Size
			if req.Offset+uint64(req.Size) > uint64(latestInode.Size) {
				readLen = uint32(uint64(latestInode.Size) - req.Offset)
			}

			actualStart := req.Offset
			actualEnd := req.Offset + uint64(readLen)
			actualStartIndx := uint32(actualStart / ChunkSize)
			actualEndIndx := uint32((actualEnd - 1) / ChunkSize)

			chunksMap := make(map[uint32][]byte)
			for _, r := range currentRows {
				if r.ChunkIndx.Valid {
					chunksMap[uint32(r.ChunkIndx.Int32)] = r.ChunkSlices
				}
			}

			var plans []chunkPlan
			var sliceIDs []int64
			var decodeErr error

			for indx := actualStartIndx; indx <= actualEndIndx; indx++ {
				chunkGlobalStart := uint64(indx) * ChunkSize
				chunkGlobalEnd := chunkGlobalStart + ChunkSize

				partGlobalStart := max(actualStart, chunkGlobalStart)
				partGlobalEnd := min(actualEnd, chunkGlobalEnd)
				if partGlobalStart >= partGlobalEnd {
					continue
				}

				chunkReadStart := uint32(partGlobalStart - chunkGlobalStart)
				chunkReadEnd := uint32(partGlobalEnd - chunkGlobalStart)
				bufOffset := uint32(partGlobalStart - actualStart)

				rawBytes := chunksMap[indx]
				if len(rawBytes) == 0 {
					continue
				}

				rawSlices, err := DecodeSlices(rawBytes)
				if err != nil {
					decodeErr = fmt.Errorf("decode slices (ino=%d, indx=%d): %w", req.Ino, indx, err)
					break
				}

				projSlices := ProjectChunkSlices(rawSlices, chunkReadStart, chunkReadEnd)
				for _, ps := range projSlices {
					if ps.ID > 0 {
						sliceIDs = append(sliceIDs, int64(ps.ID))
					}
				}

				plans = append(plans, chunkPlan{
					indx:      indx,
					slices:    projSlices,
					bufOffset: bufOffset,
					readStart: chunkReadStart,
					readEnd:   chunkReadEnd,
				})
			}

			if decodeErr != nil {
				return decodeErr
			}

			slices.Sort(sliceIDs)
			sliceIDs = slices.Compact(sliceIDs)

			if len(sliceIDs) > 0 {
				lockedIDs, err := q.LockValidSliceRefsKeyShare(ctx, sliceIDs)
				if err != nil {
					return fmt.Errorf("lock slice refs key share: %w", err)
				}
				if len(lockedIDs) < len(sliceIDs) {
					if attempt == maxLockRetries-1 {
						return fmt.Errorf("%w: failed to lock all slice refs after %d retries", ErrIO, maxLockRetries)
					}
					continue
				}
			}

			finalPlans = plans
			finalBuf = make([]byte, readLen)
			break
		}

		bs := m.getBlockStore()
		for _, cp := range finalPlans {
			for _, s := range cp.slices {
				if s.ID == 0 {
					continue
				}

				bufStart := cp.bufOffset + (s.Pos - cp.readStart)
				locs, err := LocateBlocks(s.Size, s.Off, s.Len)
				if err != nil {
					return fmt.Errorf("locate blocks (slice_id=%d, size=%d, off=%d, len=%d): %w", s.ID, s.Size, s.Off, s.Len, err)
				}

				curOverlapOff := uint32(0)
				for _, loc := range locs {
					dstStart := bufStart + curOverlapOff
					dstEnd := dstStart + loc.Length
					if err := bs.GetBlock(ctx, s.ID, loc.BlockIdx, loc.BlockSize, loc.BlockOff, finalBuf[dstStart:dstEnd]); err != nil {
						return fmt.Errorf("%w: get block (slice_id=%d, block_idx=%d): %v", ErrIO, s.ID, loc.BlockIdx, err)
					}
					curOverlapOff += loc.Length
				}
			}
		}

		if m.shouldTouchAtime(latestInode.Atime.Time, latestInode.Mtime.Time, latestInode.Ctime.Time, time.Now()) {
			if err := q.TouchInodeAtime(ctx, int64(req.Ino)); err != nil {
				return fmt.Errorf("touch inode atime %d: %w", req.Ino, err)
			}
		}

		resp = &ReadResponse{Data: finalBuf}
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			if errors.Is(err, errExpiredSessionBadHandle) && req.SessionID != nil {
				m.cleanExpiredSessionByID(ctx, *req.SessionID)
			}
			return nil, ErrBadFileHandle
		case errors.Is(err, ErrIO):
			return nil, err
		default:
			return nil, fmt.Errorf("read tx: %w", err)
		}
	}

	return resp, nil
}
