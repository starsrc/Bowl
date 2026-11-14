package fusefs

import (
	"context"
	"errors"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Read 实现常规文件数据读取，对应 Linux FUSE FUSE_READ 协议。
// 校验本地句柄与访问模式，并在服务端请求中省略旧版句柄：
//  1. 映射内核 Inode 为服务端 Inode，并校验本地文件句柄；
//  2. 目标读取长度以 input.Size 与传入非空 buf 长度二者较小值为准；长度为 0 时立即返回空数据；
//  3. 在同一文件句柄上维护顺序读检测与 4 MiB 对齐预读缓冲：顺序读与内核并发预读流量统一按 4 MiB 对齐窗口
//     向服务端单飞拉取，并在确认连续顺序流后并发维持 W_read=4 个 4 MiB 预取窗口；
//     非连续随机读保持按请求尺寸直通读取，不产生额外放大，且不清空落在窗口内的在途拉取；
//     窗口未到文件末尾且请求跨窗口边界时，用下一 4 MiB 对齐窗口补齐；窗口已到 EOF 时直接返回剩余字节；
//  4. 当 want > MaxRPCChunkSize 时按 4 MiB 顺序循环分片读取；
//  5. 遇到 EOF 或短读立即停止分片循环并返回已读数据；中途分片报错但已有数据读取成功时按短读返回 fuse.OK，首片失败返回对应 errno。
func (fs *RawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (res fuse.ReadResult, status fuse.Status) {
	start := fs.metrics.beginOp(OpRead)
	opCtx, endSpan := fs.startFuseOp(OpRead)
	defer func() {
		endSpan(status)
		var n uint64
		if status == fuse.OK && res != nil {
			n = uint64(res.Size())
		}
		fs.metrics.endOp(OpRead, start, status, n)
	}()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if fs.isVirtualStatsIno(input.NodeId) {
		return fs.readVirtualStats(input, buf)
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return nil, status
	}
	if !handle.CanRead() {
		return nil, fuse.EBADF
	}

	want := int(input.Size)
	if len(buf) > 0 && len(buf) < want {
		want = len(buf)
	}
	if want == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}

	dest := buf
	if len(dest) < want {
		dest = make([]byte, want)
	} else {
		dest = dest[:want]
	}

	if n, local, st := fs.tmpMgr.Read(input.NodeId, input.Offset, dest); local {
		return fuse.ReadResultData(dest[:n]), st
	}

	baseCtx, cancelTimeout := context.WithTimeout(opCtx, DefaultRPCTimeout)
	ctx, cancelChan := rpc.ContextWithCancelChan(baseCtx, cancel)
	defer func() {
		cancelChan()
		cancelTimeout()
	}()

	fetchChunk := func(fetchCtx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		reqCtx := rpc.WithRequestID(fetchCtx, fs.NextRequestID())
		resp, rpcErr := fs.client.Read(reqCtx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ino,
			Fh:     0,
			Offset: offset,
			Size:   length,
			DiskId: fs.cfg.DiskID,
		}))
		if rpcErr != nil {
			return rpc.ReadPayload{}, rpcErr
		}
		return rpc.TakeReadPayload(resp.Msg), nil
	}

	if fs.stage.HasPending(ino) {
		n, handled, stageErr := fs.stage.Read(ctx, ino, input.Offset, dest, fetchChunk)
		if stageErr != nil {
			if errno, ok := errors.AsType[syscall.Errno](stageErr); ok {
				return nil, fuse.Status(errno)
			}
			return nil, fs.ToStatus(stageErr, cancel)
		}
		if handled {
			return fuse.ReadResultData(dest[:n]), fuse.OK
		}
	}

	n, err := fs.readAhead.Read(ctx, input.Fh, ino, input.Offset, dest, fetchChunk)
	if err != nil {
		return nil, fs.ToStatus(err, cancel)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

// Write 实现常规文件数据写入，对应 Linux FUSE FUSE_WRITE 协议。
// 匿名文件在具名化前写入宿主机独占文件，常规文件按权限与特性分流至本地 Stage 暂存或同步直通：
// 1. --mode ro 只读挂载下直接拦截返回 EROFS；
// 2. 映射 Inode 并校验本地文件句柄，立即同步失效目标 Inode 的预读缓冲；
// 3. 当 len(data) <= MaxRPCChunkSize 且满足本地暂存条件（非 O_SYNC/O_APPEND/0字节/特权标志且已通过能力探测）时写入本地 Stage 分片；
// 4. 需同步直通或 len(data) > MaxRPCChunkSize 时排空本地 Stage 并按 4 MiB 分片直写服务端；
// 5. 遵循 POSIX 短写语义：若中途分片报错（如 ENOSPC）且已有数据写入成功，返回已写入字节数与 fuse.OK；首片即失败返回对应 errno。
func (fs *RawFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, status fuse.Status) {
	start := fs.metrics.beginOp(OpWrite)
	opCtx, endSpan := fs.startFuseOp(OpWrite)
	defer func() {
		endSpan(status)
		var n uint64
		if status == fuse.OK {
			n = uint64(written)
		}
		fs.metrics.endOp(OpWrite, start, status, n)
	}()
	return fs.writeData(opCtx, cancel, input, data)
}

func (fs *RawFS) writeData(opCtx context.Context, cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, status fuse.Status) {

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	if status = fs.CheckWritable(); status != fuse.OK {
		return 0, status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return 0, fuse.EACCES
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return 0, status
	}
	if !handle.CanWrite() {
		return 0, fuse.EBADF
	}

	appendMode := handle.Flags&syscall.O_APPEND != 0
	if n, local, st := fs.tmpMgr.Write(input.NodeId, input.Offset, data, appendMode); local {
		return n, st
	}

	dataLen := len(data)
	syncMode := handle.Flags&(syscall.O_SYNC|syscall.O_DSYNC) != 0
	specialInode := fs.isInodeSpecial(input.NodeId)

	switch {
	case appendMode:
		// 同一 Inode 的 O_APPEND 必须在“排空异步写 -> 读取当前大小 -> 提交 Write”之间保持定序，
		// 否则并发追加会读到相同旧大小而互相覆盖。
		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()
		if flushErr := fs.stage.FlushAndResetInodeForHandle(opCtx, input.Fh, ino); flushErr != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.stageErrToStatus(flushErr)
		}
		cached, st := fs.getInodeWithContext(context.WithoutCancel(opCtx), nil, input.NodeId)
		if st != fuse.OK {
			return 0, st
		}
		copyInput := *input
		copyInput.Offset = cached.inode.GetAttr().GetSize()
		input = &copyInput
	case fs.stage == nil || syncMode || specialInode || !fs.caps.knownAbsent(input.NodeId) || dataLen == 0 || dataLen > MaxRPCChunkSize:
		if flushErr := fs.stage.FlushAndResetInodeForHandle(opCtx, input.Fh, ino); flushErr != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.stageErrToStatus(flushErr)
		}
	default:
		fs.readAhead.InvalidateInode(ino)
		if stWritten, handled, stErr := fs.stage.WriteForHandle(opCtx, cancel, input.Fh, ino, input.NodeId, input.Offset, data, epoch); handled {
			if stErr != nil {
				if !errors.Is(stErr, rpc.ErrKernelInterrupted) && !errors.Is(stErr, syscall.EINTR) {
					fs.cache.invalidateInode(input.NodeId)
				}
				return 0, fs.stageErrToStatus(stErr)
			}
			fs.readAhead.InvalidateInode(ino)
			return stWritten, fuse.OK
		}
	}

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

	// Write 为非幂等变更（尤其在 O_APPEND 下），一旦在途发出 RPC 则忽略中途 FUSE_INTERRUPT（如 Go SIGURG 抢占信号），
	// 避免服务端已提交追加但客户端因中断返回 EINTR 触发调用方重试造成重复追加。
	// 使用 context.WithoutCancel(opCtx) 保留调用链 SpanContext，同时屏蔽上游取消对非幂等落盘的半途中断。
	detachedCtx := context.WithoutCancel(opCtx)

	if dataLen <= MaxRPCChunkSize {
		ctx, cancelFunc := fs.RPCContextFrom(detachedCtx, nil)
		defer cancelFunc()
		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ino,
			Fh:     0,
			Offset: input.Offset,
			Data:   data,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.ToStatus(err, nil)
		}
		fs.markDirty(ino)
		fs.recordInodeSpecial(input.NodeId, resp.Msg.GetInode())
		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		written = resp.Msg.GetSize()
		fs.metrics.addStagePassthrough(uint64(written))
		fs.stage.ObserveServerInode(ino, resp.Msg.GetInode(), false)
		return written, fuse.OK
	}

	var totalWritten uint32
	for totalWritten < uint32(dataLen) {
		if totalWritten > 0 && cancel != nil {
			select {
			case <-cancel:
				return totalWritten, fuse.OK
			default:
			}
		}

		chunkSize := min(uint32(dataLen)-totalWritten, MaxRPCChunkSize)
		chunk := data[totalWritten : totalWritten+chunkSize]
		curOffset := input.Offset + uint64(totalWritten)

		chunkCtx, chunkCancel := fs.RPCContextFrom(detachedCtx, nil)
		resp, err := fs.client.Write(chunkCtx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ino,
			Fh:     0,
			Offset: curOffset,
			Data:   chunk,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		chunkCancel()
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			if totalWritten > 0 {
				return totalWritten, fuse.OK
			}
			return 0, fs.ToStatus(err, nil)
		}

		fs.recordInodeSpecial(input.NodeId, resp.Msg.GetInode())
		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		n := resp.Msg.GetSize()
		fs.markDirty(ino)
		fs.metrics.addStagePassthrough(uint64(n))
		fs.stage.ObserveServerInode(ino, resp.Msg.GetInode(), false)
		totalWritten += n
		if n < chunkSize {
			break
		}
	}

	return totalWritten, fuse.OK
}

// Writev 实现聚集向量写入，消除 go-fuse 默认返回 ENOSYS 导致的首次写入回退往返开销：
// 空段传 nil、单段零拷贝直传 data[0]、多段连续拼接后委托共享写路径 writeData 处理，避免触发 OpWrite 双计。
func (fs *RawFS) Writev(cancel <-chan struct{}, input *fuse.WriteIn, data [][]byte) (written uint32, status fuse.Status) {
	start := fs.metrics.beginOp(OpWritev)
	opCtx, endSpan := fs.startFuseOp(OpWritev)
	defer func() {
		endSpan(status)
		var n uint64
		if status == fuse.OK {
			n = uint64(written)
		}
		fs.metrics.endOp(OpWritev, start, status, n)
	}()

	switch len(data) {
	case 0:
		return fs.writeData(opCtx, cancel, input, nil)
	case 1:
		return fs.writeData(opCtx, cancel, input, data[0])
	default:
		var total int
		for _, seg := range data {
			total += len(seg)
		}
		merged := make([]byte, 0, total)
		for _, seg := range data {
			merged = append(merged, seg...)
		}
		return fs.writeData(opCtx, cancel, input, merged)
	}
}
