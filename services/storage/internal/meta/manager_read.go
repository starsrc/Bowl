package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
// 遵循 ADR-0019 数据层架构与 POSIX pread(2) 语义：
//  1. 静态边界校验：参数非空、(ino, fh, offset) 在合法正数范围内、size <= BlockSize (4 MiB)、session_id 合法；
//  2. 权限与类型校验：在事务内校验目标 Inode 必须为常规文件 S_IFREG（目录返回 ErrIsDir，其他非常规文件返回 ErrInvalidArgument），
//     校验 (ino, fh) 对应的 FileHandle 存在且具备读权限（O_RDONLY 或 O_RDWR；若是 O_WRONLY、句柄不存在或 ino 不匹配均返回 ErrBadFileHandle）；
//  3. 会话续期与访问时间：在传入有效 session_id 时续期会话租约；遵循配置的 AtimeMode（noatime/relatime/strictatime）决定是否更新 inodes.atime；
//  4. EOF 与截断读取：当 size == 0 或 offset >= inode.Size 时，直接返回空字节流（len(data) == 0）；
//     当 offset + size > inode.Size 时，自动截断实际读取长度 readLen = uint32(inode.Size - offset)；
//  5. 跨 4 MiB Block 与跨 64 MiB Chunk 边界无缝拼接：
//     - 当读取区间 [req.Offset, req.Offset + readLen) 跨越 64 MiB Chunk 边界时，按 startIndx 到 endIndx 分别加载各自的 chunks.slices；
//     - 若某侧 Chunk 不存在（pgx.ErrNoRows）或为空，视为稀疏空洞保持 0x00 填充；
//     - 若存在，通过 BuildSlice 裁剪合并有效切片视图；
//     - 对有效 Slice 区间若跨越 BlockSize，按 block_idx = slice_off / BlockSize 与 block_off = slice_off % BlockSize 准确定位并从 BlockStore 拼接连续字节流。
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
		inode, err := q.GetInode(ctx, int64(req.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d: %w", req.Ino, err)
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
		if accMode != O_RDONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		if err := m.renewOptionalClientSession(ctx, q, req.SessionID); err != nil {
			return err
		}

		// 到达 EOF 或请求大小为 0 时直接返回空字节流
		if req.Size == 0 || req.Offset >= uint64(inode.Size) {
			resp = &ReadResponse{Data: []byte{}}
			return nil
		}

		if m.shouldTouchAtime(inode.Atime.Time, inode.Mtime.Time, inode.Ctime.Time, time.Now()) {
			if err := q.TouchInodeAtime(ctx, int64(req.Ino)); err != nil {
				return fmt.Errorf("touch inode atime %d: %w", req.Ino, err)
			}
		}

		readLen := req.Size
		if req.Offset+uint64(req.Size) > uint64(inode.Size) {
			readLen = uint32(uint64(inode.Size) - req.Offset)
		}

		buf := make([]byte, readLen)
		actualStart := req.Offset
		actualEnd := req.Offset + uint64(readLen)
		startIndx := uint32(actualStart / ChunkSize)
		endIndx := uint32((actualEnd - 1) / ChunkSize)

		bs := m.getBlockStore()

		for indx := startIndx; indx <= endIndx; indx++ {
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

			chunk, err := q.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(req.Ino),
				Indx: int32(indx),
			})
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					// 该 Chunk 为稀疏未写入空洞，对应缓冲区区间保持 0x00，不触发物理盘 IO
					continue
				}
				return fmt.Errorf("get chunk (ino=%d, indx=%d): %w", req.Ino, indx, err)
			}

			if len(chunk.Slices) == 0 {
				continue
			}

			slices, err := DecodeSlices(chunk.Slices)
			if err != nil {
				return fmt.Errorf("decode slices (ino=%d, indx=%d): %w", req.Ino, indx, err)
			}

			view := BuildSlice(slices)

			for _, s := range view {
				overlapStart := max(s.Pos, chunkReadStart)
				overlapEnd := min(s.Pos+s.Len, chunkReadEnd)
				if overlapStart >= overlapEnd {
					continue
				}

				if s.ID == 0 {
					continue
				}

				overlapLen := overlapEnd - overlapStart
				bufStart := bufOffset + (overlapStart - chunkReadStart)
				sliceOff := s.Off + (overlapStart - s.Pos)

				locs, err := LocateBlocks(s.Size, sliceOff, overlapLen)
				if err != nil {
					return fmt.Errorf("locate blocks (slice_id=%d, size=%d, off=%d, len=%d): %w", s.ID, s.Size, sliceOff, overlapLen, err)
				}

				curOverlapOff := uint32(0)
				for _, loc := range locs {
					dstStart := bufStart + curOverlapOff
					dstEnd := dstStart + loc.Length
					if err := bs.GetBlock(ctx, s.ID, loc.BlockIdx, loc.BlockSize, loc.BlockOff, buf[dstStart:dstEnd]); err != nil {
						return fmt.Errorf("%w: get block (slice_id=%d, block_idx=%d): %v", ErrIO, s.ID, loc.BlockIdx, err)
					}
					curOverlapOff += loc.Length
				}
			}
		}

		resp = &ReadResponse{Data: buf}
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
			return nil, ErrBadFileHandle
		case errors.Is(err, ErrIO):
			return nil, err
		default:
			return nil, fmt.Errorf("read tx: %w", err)
		}
	}

	return resp, nil
}
