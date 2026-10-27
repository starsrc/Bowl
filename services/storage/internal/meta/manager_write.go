package meta

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
// 遵循 ADR-0019 两阶段落盘架构：
// 1. 静态边界校验：参数非空、(ino, fh, offset) 在正数范围内、len(data) <= 4 MiB、offset + len(data) <= MaxInt64；
// 2. 零字节写入：在事务内锁定 Inode 校验常规文件类型与写句柄有效性，续期会话，不分配 slice_id 且不修改任何元数据；
// 3. 两阶段落盘写入：
//   - 阶段 1（锁外落盘）：申请 slice_id，将 data 按 BlockSize 切分原子写入底层物理块存储；失败立即清理物理块返回 ErrIO；
//   - 阶段 2（事务提交）：事务内锁定 Inode 校验类型与写句柄有效性，续期会话，追加 24 字节 Slice 描述符至 chunks 表，
//     插入 slice_refs 引用，更新 Inode 大小（扩容则更新，覆写保持）、剥离 S_ISUID/S_ISGID 特权位并刷新 mtime/ctime；
//   - 事务回滚补偿：若阶段 2 事务失败，在事务外立即调用 DeleteBlock 清理阶段 1 已落盘物理块，保证零孤儿块残留。
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

	// 阶段 1：行锁外申请 slice_id 与落盘物理块
	sliceID, err := m.store.NextSliceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("next slice id: %w", err)
	}

	bs := m.getBlockStore()
	blocks := SliceBlocks(uint32(dataLen))
	writtenBlocks := make([]SliceBlockInfo, 0, len(blocks))
	cleanupBlocks := func() {
		for _, b := range writtenBlocks {
			_ = bs.DeleteBlock(ctx, uint64(sliceID), b.BlockIdx, b.BlockSize)
		}
	}

	for _, b := range blocks {
		start := b.BlockIdx * BlockSize
		end := start + b.BlockSize
		blockData := req.Data[start:end]
		if err := bs.PutBlock(ctx, uint64(sliceID), b.BlockIdx, blockData); err != nil {
			cleanupBlocks()
			return nil, fmt.Errorf("%w: put block (slice_id=%d, block_idx=%d): %v", ErrIO, sliceID, b.BlockIdx, err)
		}
		writtenBlocks = append(writtenBlocks, b)
	}

	// 阶段 2：事务提交追加 Slice 元数据与更新 Inode
	var resp *WriteResponse
	txErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
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

		if err := m.renewOptionalClientSession(ctx, q, req.SessionID); err != nil {
			return err
		}

		newEnd := int64(req.Offset) + int64(dataLen)
		newSize := inode.Size
		if newEnd > newSize {
			newSize = newEnd
			if inode.RootIno > 0 {
				deltaBytes := uint64(newEnd - inode.Size)
				diskRow, err := q.GetDiskByRootInoForUpdate(ctx, inode.RootIno)
				if err != nil {
					if !errors.Is(err, pgx.ErrNoRows) {
						return fmt.Errorf("get disk by root_ino %d for update: %w", inode.RootIno, err)
					}
				} else {
					if uint64(diskRow.UsedBytes) > uint64(diskRow.MaxBytes) || deltaBytes > uint64(diskRow.MaxBytes)-uint64(diskRow.UsedBytes) {
						return ErrNoSpace
					}
					if err := q.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
						RootIno:     inode.RootIno,
						DeltaBytes:  int64(deltaBytes),
						DeltaInodes: 0,
					}); err != nil {
						return fmt.Errorf("add disk quota by root_ino %d: %w", inode.RootIno, err)
					}
				}
			}
		}

		indx := uint32(req.Offset / ChunkSize)
		pos := uint32(req.Offset % ChunkSize)

		sliceDesc := Slice{
			Pos:  pos,
			ID:   uint64(sliceID),
			Size: uint32(dataLen),
			Off:  0,
			Len:  uint32(dataLen),
		}
		sliceBytes := EncodeSlice(sliceDesc)

		if err := q.AppendChunkSlice(ctx, db.AppendChunkSliceParams{
			Ino:    int64(req.Ino),
			Indx:   int32(indx),
			Slices: sliceBytes,
		}); err != nil {
			return fmt.Errorf("append chunk slice (ino=%d, indx=%d): %w", req.Ino, indx, err)
		}

		if err := q.InsertSliceRef(ctx, db.InsertSliceRefParams{
			SliceID: sliceID,
			Size:    int32(dataLen),
			Refs:    1,
		}); err != nil {
			return fmt.Errorf("insert slice ref (slice_id=%d): %w", sliceID, err)
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
		return nil
	})

	if txErr != nil {
		// 阶段 2 失败时清理物理块，确保零残留
		cleanupBlocks()

		switch {
		case errors.Is(txErr, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(txErr, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(txErr, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(txErr, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		case errors.Is(txErr, ErrNoSpace):
			return nil, ErrNoSpace
		default:
			return nil, fmt.Errorf("write commit tx: %w", txErr)
		}
	}

	return resp, nil
}

// writeZeroBytes 处理 0 字节写入探针。
func (m *manager) writeZeroBytes(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	var resp *WriteResponse
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
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

		if err := m.renewOptionalClientSession(ctx, q, req.SessionID); err != nil {
			return err
		}

		resp = &WriteResponse{
			Size:  0,
			Inode: m.newInode(inode),
		}
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
		default:
			return nil, fmt.Errorf("write zero bytes tx: %w", err)
		}
	}

	return resp, nil
}
