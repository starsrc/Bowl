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
			Branch: fs.cfg.Branch,
		}))
		if rpcErr != nil {
			return rpc.ReadPayload{}, rpcErr
		}
		return rpc.TakeReadPayload(resp.Msg), nil
	}

	if fs.IsFenced() {
		n, err := readDirectChunks(ctx, input.Offset, dest, fetchChunk)
		if err != nil {
			return nil, fs.ToStatus(err)
		}
		return fuse.ReadResultData(dest[:n]), fuse.OK
	}

	if fs.stage.HasPending(ino) {
		g0 := fs.metaClock.Load()
		var baseSize uint64
		var hasBaseSize bool
		if info, ok := fs.lookupKnownInode(input.NodeId); ok && info.hasServerSize {
			baseSize = info.serverSize
			hasBaseSize = true
		}
		n, handled, stageErr := fs.stage.Read(
			ctx,
			ino,
			input.Offset,
			dest,
			baseSize,
			hasBaseSize,
			func() bool { return fs.inodeMutatedSince(ino, g0) },
			fetchChunk,
		)
		if stageErr != nil {
			if errno, ok := errors.AsType[syscall.Errno](stageErr); ok {
				return nil, fuse.Status(errno)
			}
			return nil, fs.ToStatus(stageErr)
		}
		if handled {
			return fuse.ReadResultData(dest[:n]), fuse.OK
		}
	}

	n, err := fs.readAhead.Read(ctx, input.Fh, ino, input.Offset, dest, fetchChunk)
	if err != nil {
		return nil, fs.ToStatus(err)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

func readDirectChunks(ctx context.Context, offset uint64, dest []byte, fetch ChunkFetcher) (int, error) {
	want := len(dest)
	var totalRead int
	for totalRead < want {
		chunkReqSize := min(want-totalRead, MaxRPCChunkSize)
		payload, err := fetch(ctx, offset+uint64(totalRead), uint32(chunkReqSize))
		if err != nil {
			if totalRead > 0 {
				return totalRead, nil
			}
			return 0, err
		}
		if len(payload.Data) == 0 {
			payload.Release()
			break
		}
		n := min(chunkReqSize, len(payload.Data))
		copy(dest[totalRead:totalRead+n], payload.Data[:n])
		payload.Release()
		totalRead += n
		if n < chunkReqSize {
			break
		}
	}
	return totalRead, nil
}

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
	if status = fs.CheckWritable(); status != fuse.OK {
		return 0, status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return 0, fuse.EACCES
	}

	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return 0, st
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return 0, status
	}
	if !handle.CanWrite() {
		return 0, fuse.EBADF
	}

	appendMode := handle.Flags&syscall.O_APPEND != 0
	dataLen := len(data)
	syncMode := handle.Flags&(syscall.O_SYNC|syscall.O_DSYNC) != 0

	var unlock func()
	defer func() {
		if unlock != nil {
			unlock()
		}
	}()
	exclusiveLocked := false
	if appendMode || (fs.stage != nil && (syncMode || dataLen == 0 || dataLen > MaxRPCChunkSize)) {
		unlock = fs.inodeLocks.lock(input.NodeId)
		exclusiveLocked = true
	} else {
		unlock = fs.inodeLocks.rlock(input.NodeId)
	}

	if n, updatedInode, local, st := fs.tmpMgr.Write(input.NodeId, input.Offset, data, appendMode); local {
		if st == fuse.OK && len(data) > 0 {
			fs.caps.markAbsent(input.NodeId)
			fs.recordKnownInodeAuthoritative(input.NodeId, updatedInode)
		}
		return n, st
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	specialInode := fs.isInodeSpecial(input.NodeId)
	capKnownAbsent := fs.caps.knownAbsent(input.NodeId)
	if !exclusiveLocked && fs.stage != nil && (specialInode || !capKnownAbsent) {
		unlock()
		unlock = nil
		unlock = fs.inodeLocks.lock(input.NodeId)
	}

	switch {
	case appendMode:
		if flushErr := fs.flushAndResetInodeForHandle(opCtx, cancel, input.Fh, ino); flushErr != nil {
			return 0, fs.stageErrToStatus(flushErr)
		}
		inode, _, st := fs.getAttrInodeWithContext(context.WithoutCancel(opCtx), nil, input.NodeId)
		if st != fuse.OK {
			return 0, st
		}
		copyInput := *input
		copyInput.Offset = inode.GetAttr().GetSize()
		input = &copyInput
	case fs.stage == nil || syncMode || specialInode || !capKnownAbsent || dataLen == 0 || dataLen > MaxRPCChunkSize:
		if flushErr := fs.flushAndResetInodeForHandle(opCtx, cancel, input.Fh, ino); flushErr != nil {
			return 0, fs.stageErrToStatus(flushErr)
		}
	default:
		fs.readAhead.InvalidateInode(ino)
		if stWritten, handled, stErr := fs.stage.WriteForHandle(opCtx, cancel, input.Fh, ino, input.NodeId, input.Offset, data); handled {
			if stErr != nil {
				return 0, fs.stageErrToStatus(stErr)
			}
			fs.readAhead.InvalidateInode(ino)
			return stWritten, fuse.OK
		}
	}

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

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
			Branch: fs.cfg.Branch,
			Epoch:  fs.cfg.Epoch,
		}))
		if err != nil {
			return 0, fs.ToStatus(err)
		}
		fs.markMetaDirty()
		fs.recordKnownInodeMonotonic(input.NodeId, resp.Msg.GetInode())
		fs.caps.markAbsent(input.NodeId)
		written = resp.Msg.GetSize()
		fs.metrics.addStagePassthrough(uint64(written))
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
			Branch: fs.cfg.Branch,
			Epoch:  fs.cfg.Epoch,
		}))
		chunkCancel()
		if err != nil {
			fs.fenceIfStaleEpoch(err)
			if totalWritten > 0 {
				return totalWritten, fuse.OK
			}
			return 0, fs.ToStatus(err)
		}

		fs.recordKnownInodeMonotonic(input.NodeId, resp.Msg.GetInode())
		fs.caps.markAbsent(input.NodeId)
		n := resp.Msg.GetSize()
		fs.markMetaDirty()
		fs.metrics.addStagePassthrough(uint64(n))
		totalWritten += n
		if n < chunkSize {
			break
		}
	}

	return totalWritten, fuse.OK
}

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
