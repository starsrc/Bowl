package store

import (
	"context"
	"errors"
	"math"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type writeDataOp struct {
	ino     uint64
	offset  uint64
	dataLen uint32
	blocks  []writeBlockSpec

	// Phase A 校验缓存至 Phase B
	target     Inode
	newSize    uint64
	bytesDelta uint64
	attr       Attribute
}

func isFullBlockAlignedWrite(offset uint64, data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return offset%BlobSize == 0 && len(data)%BlobSize == 0
}

func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// Write 执行跨 256 KiB 逻辑块的追加式数据写入流水线：
//   - 满 256 KiB 对齐整块写：阶段 1 在不持 inodeLocks 的状态下向活跃 .pack 追加写入满块数据，随后获取 s.inodeLocks[ino&0xfff] 投递进 Actor 双阶段队列；
//   - 局部或非对齐跨块写：全程持 s.inodeLocks[ino&0xfff]，若命中活跃 .pack 最末尾同块则走 ExtendTail 原地顺延写指针，否则读取旧块合并后通过 Append 追加至活跃 .pack；
//   - 在 Actor Phase B 的同一个 IndexedBatch 中原子更新 InodeKey 与涉及的全部 DataKey -> 16B BlobRef{PackID, Offset, Length}。
func (s *StoreActor) Write(ctx context.Context, ino, offset uint64, data []byte, epoch ...uint64) (uint32, Attribute, error) {
	if s.readOnly {
		return 0, Attribute{}, ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return 0, Attribute{}, s.submitStaleEpoch(ctx, opWrite, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return 0, Attribute{}, err
	}
	if offset > MaxFileSize || uint64(len(data)) > MaxFileSize-offset {
		return 0, Attribute{}, ErrFileTooLarge
	}
	if len(data) > math.MaxUint32 {
		return 0, Attribute{}, ErrInvalid
	}

	if len(data) == 0 {
		return s.writeZeroBytes(ctx, ino, offset, ep)
	}

	if isFullBlockAlignedWrite(offset, data) {
		return s.writeFullBlocks(ctx, ino, offset, data, ep)
	}

	return s.writePartialData(ctx, ino, offset, data, ep)
}

func (s *StoreActor) writeZeroBytes(ctx context.Context, ino, offset, epoch uint64) (uint32, Attribute, error) {
	op := &writeDataOp{
		ino:     ino,
		offset:  offset,
		dataLen: 0,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.epoch = epoch
	cmd.req = op
	if err := s.submit(ctx, cmd); err != nil {
		return 0, Attribute{}, err
	}
	return 0, op.attr, nil
}

func (s *StoreActor) writeFullBlocks(ctx context.Context, ino, offset uint64, data []byte, epoch uint64) (uint32, Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return 0, Attribute{}, err
	}
	defer unlock()

	blobs := s.blobs
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	numBlocks := len(data) / BlobSize
	specs := make([]writeBlockSpec, numBlocks)
	var staged []BlobRef

	// 阶段 1：在不持有 inodeLocks 的状态下向活跃 .pack 批量追加写入满块数据。
	var blobSpan trace.Span
	if isSpanRecording(ctx) {
		_, blobSpan = startChildSpan(ctx, "store.BlobWrite",
			attribute.Int64("inode.ino", int64(ino)),
			attribute.Int("write.bytes", len(data)),
			attribute.String("write.mode", "full_block"),
		)
	}
	refs, writeErr := s.stageAppendBlocks(ctx, blobs, ino, data, &staged)
	finishSpan(blobSpan, writeErr)
	if writeErr != nil {
		s.unpinStagedBlobs(staged)
		return 0, Attribute{}, writeErr
	}
	s.metrics.recordBlobWrite(writeModeFullBlock, uint64(len(refs)), uint64(len(data)))

	baseBlockIdx := offset / BlobSize
	for i, ref := range refs {
		specs[i] = writeBlockSpec{
			blockIdx: baseBlockIdx + uint64(i),
			newRef:   ref,
			keep:     true,
		}
	}

	// 预写成功后获取 per-Inode 定序锁，确保持有至阶段 3 db.Apply 完成（<-cmd.done 唤醒）后释放。
	inoLock := s.lockInode(ctx, ino)
	defer inoLock.Unlock()

	op := &writeDataOp{
		ino:     ino,
		offset:  offset,
		dataLen: uint32(len(data)),
		blocks:  specs,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.epoch = epoch
	cmd.req = op
	cmd.stagedBlobs = staged
	if err := s.enqueueAndWait(ctx, cmd); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) writePartialData(ctx context.Context, ino, offset uint64, data []byte, epoch uint64) (uint32, Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return 0, Attribute{}, err
	}
	defer unlock()

	blobs := s.blobs
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	inoLock := s.lockInode(ctx, ino)
	defer inoLock.Unlock()

	var (
		specs    []writeBlockSpec
		staged   []BlobRef
		blobSpan trace.Span
		stageCtx = ctx
	)
	if isSpanRecording(ctx) {
		stageCtx, blobSpan = startChildSpan(ctx, "store.BlobWrite",
			attribute.Int64("inode.ino", int64(ino)),
			attribute.Int("write.bytes", len(data)),
			attribute.String("write.mode", "partial"),
		)
	}

	var prepErr error
	for pos, consumed := offset, 0; consumed < len(data); {
		blockIdx := pos / BlobSize
		inBlock := int(pos % BlobSize)
		length := min(len(data)-consumed, BlobSize-inBlock)
		chunk := data[consumed : consumed+length]

		spec, err := s.preparePartialWriteBlock(stageCtx, blobs, ino, blockIdx, inBlock, chunk, &staged)
		if err != nil {
			prepErr = err
			break
		}
		specs = append(specs, spec)

		pos += uint64(length)
		consumed += length
	}
	finishSpan(blobSpan, prepErr)
	if prepErr != nil {
		s.unpinStagedBlobs(staged)
		return 0, Attribute{}, prepErr
	}

	op := &writeDataOp{
		ino:     ino,
		offset:  offset,
		dataLen: uint32(len(data)),
		blocks:  specs,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.epoch = epoch
	cmd.req = op
	cmd.stagedBlobs = staged
	if err := s.enqueueAndWait(ctx, cmd); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) preparePartialWriteBlock(ctx context.Context, blobs *blobStore, ino, blockIdx uint64, inBlock int, chunk []byte, staged *[]BlobRef) (writeBlockSpec, error) {
	// 跨块局部写中可能包含中间满块对齐整块：直接追加新块，无需读旧块。
	if inBlock == 0 && len(chunk) == BlobSize {
		if isAllZero(chunk) {
			s.metrics.recordBlobWrite(writeModeZeroElide, 1, uint64(len(chunk)))
			return writeBlockSpec{
				blockIdx: blockIdx,
				keep:     false,
			}, nil
		}
		ref, err := s.stageAppend(ctx, blobs, ino, chunk, staged)
		if err != nil {
			return writeBlockSpec{}, err
		}
		s.metrics.recordBlobWrite(writeModeFullBlock, 1, uint64(len(chunk)))
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   ref,
			keep:     true,
		}, nil
	}

	oldRef, oldOK, err := s.readDataBlobRefFromDB(ctx, ino, blockIdx)
	if err != nil {
		return writeBlockSpec{}, err
	}

	newLen := inBlock + len(chunk)
	if oldOK && int(oldRef.Length) > newLen {
		newLen = int(oldRef.Length)
	}

	// 尝试活跃 .pack 尾部顺延扩展
	if oldOK && inBlock >= int(oldRef.Length) {
		if extRef, ok, extErr := s.stageExtendTail(blobs, ino, oldRef, uint32(inBlock), chunk, uint32(newLen), staged); extErr != nil {
			return writeBlockSpec{}, extErr
		} else if ok {
			var addedBytes uint64
			if extRef.Length >= oldRef.Length {
				addedBytes = uint64(extRef.Length - oldRef.Length)
			}
			s.metrics.recordBlobWrite(writeModeExtendTail, 1, addedBytes)
			return writeBlockSpec{
				blockIdx: blockIdx,
				newRef:   extRef,
				keep:     true,
				sameRef:  extRef == oldRef,
			}, nil
		}
	}

	if !oldOK && isAllZero(chunk) {
		s.metrics.recordBlobWrite(writeModeZeroElide, 1, uint64(len(chunk)))
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, nil
	}

	merged := make([]byte, newLen)
	if oldOK {
		var readStart time.Time
		if s.metrics != nil {
			readStart = time.Now()
		}
		err = blobs.readAtContext(ctx, oldRef, 0, merged[:oldRef.Length])
		if s.metrics != nil {
			s.metrics.observeBlobRMWRead(time.Since(readStart))
		}
		if err != nil {
			return writeBlockSpec{}, err
		}
	}
	copy(merged[inBlock:], chunk)
	if isAllZero(merged) {
		s.metrics.recordBlobWrite(writeModeZeroElide, 1, uint64(len(merged)))
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, nil
	}

	ref, err := s.stageAppend(ctx, blobs, ino, merged, staged)
	if err != nil {
		return writeBlockSpec{}, err
	}
	if !oldOK && inBlock == 0 {
		s.metrics.recordBlobWrite(writeModeExtendTail, 1, uint64(len(merged)))
	} else {
		s.metrics.recordBlobWrite(writeModeRMW, 1, uint64(len(merged)))
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   ref,
		keep:     true,
	}, nil
}

func (s *StoreActor) validateWritableFileInode(reader pebbleReader, ino, offset uint64, dataLen uint32) (Inode, error) {
	inode, err := s.readInode(reader, ino)
	if err != nil {
		return Inode{}, err
	}
	if err := s.checkInodeLive(reader, &inode); err != nil {
		return Inode{}, err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return Inode{}, ErrIsDir
	case S_IFREG:
	default:
		return Inode{}, ErrInvalid
	}
	if inode.Flags&FlagImmutable != 0 {
		return Inode{}, ErrPerm
	}
	if dataLen > 0 && inode.Flags&FlagAppend != 0 && offset != inode.Size {
		return Inode{}, ErrPerm
	}
	return inode, nil
}

// validateWriteCmd 执行阶段 2 Phase A 纯内存只读预校验（绝不触碰 s.batch 写接口）。
func (s *StoreActor) validateWriteCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*writeDataOp)

	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.validateWritableFileInode(s.batch, op.ino, op.offset, op.dataLen)
	if err != nil {
		return err
	}
	if op.dataLen == 0 {
		op.target = inode
		op.newSize = inode.Size
		op.attr = inode.ToAttribute()
		return nil
	}

	endOffset := op.offset + uint64(op.dataLen)
	newSize := max(inode.Size, endOffset)
	var bytesDelta uint64
	if newSize > inode.Size {
		bytesDelta = newSize - inode.Size
		if bytesDelta > math.MaxUint64-s.stat.LogicalBytes {
			return ErrNoSpace
		}
		if err := s.checkQuota(bytesDelta, 0); err != nil {
			return err
		}
	}

	if err := s.resolveWriteBlockSpecs(s.batch, op.ino, inode.Size, op.blocks); err != nil {
		return err
	}

	op.target = inode
	op.newSize = newSize
	op.bytesDelta = bytesDelta
	return nil
}

// mutateWriteCmd 执行阶段 2 Phase B 内存记账与零拷贝批次编码。
func (s *StoreActor) mutateWriteCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*writeDataOp)
	if op.dataLen == 0 {
		s.overlayAtime(&op.target)
		op.attr = op.target.ToAttribute()
		return
	}

	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	inode := op.target
	inode.Size = op.newSize
	inode.Mode &^= S_ISUID | S_ISGID
	inode.Mtime = nowNano
	inode.Ctime = nowNano
	inode.WriteSeq = seq

	s.stat.LogicalBytes += op.bytesDelta

	_ = BatchSetInodeDeferred(s.batch, inode)
	BatchDeleteXattrDeferred(s.batch, op.ino, securityCapabilityXattr)

	s.applyWriteBlockSpecs(op.ino, op.blocks)

	s.overlayAtime(&inode)
	op.attr = inode.ToAttribute()
}

// Read 在队列外执行 100% 无锁并发直读：
//   - 单块读直接调 1 次 s.db.Get(DataKey)，跨块读调 s.db.NewIter(nil) + SeekPrefixGE(DataKey(ino, firstBlock))；
//   - 稀疏空洞直接填零；
//   - 通过 16B BlobRef{PackID, Offset, Length} 优先从 hotShard 拷贝，未命中直接对常驻 .pack 句柄执行 ReadAt（0 open/close、0 撕裂读、无 ENOENT 重试）。
func (s *StoreActor) Read(ctx context.Context, ino, offset uint64, size uint32) ([]byte, error) {
	return s.ReadInto(ctx, ino, offset, size, nil)
}

// ReadInto 在队列外执行无锁并发直读；当 cap(dst) >= readLen 时直接复用调用方提供的池化缓冲，
// 且仅对真实稀疏空洞区间执行精确清零，彻底消除对即将被物理块数据覆盖缓冲区的冗余清零开销。
func (s *StoreActor) ReadInto(ctx context.Context, ino, offset uint64, size uint32, dst []byte) ([]byte, error) {
	if s.metrics != nil {
		opStart := time.Now()
		defer func() {
			s.metrics.observeReadOp(readOpRead, time.Since(opStart))
		}()
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if err := validateNodeIno(ino); err != nil {
		return nil, err
	}
	if offset > MaxFileSize {
		return nil, ErrInvalid
	}

	unlock, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer unlock()

	inode, err := s.readInodeFromDB(ctx, ino)
	if err != nil {
		return nil, err
	}
	if err := s.checkInodeLiveFromDB(ctx, &inode); err != nil {
		return nil, err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return nil, ErrIsDir
	case S_IFREG:
	default:
		return nil, ErrInvalid
	}

	if size == 0 || offset >= inode.Size {
		return []byte{}, nil
	}

	readLen := min(uint64(size), inode.Size-offset)
	var (
		out       []byte
		preZeroed bool
	)
	if uint64(cap(dst)) >= readLen {
		out = dst[:readLen]
	} else {
		out = make([]byte, readLen)
		preZeroed = true
	}
	end := offset + readLen

	firstBlock := offset / BlobSize
	lastBlock := (end - 1) / BlobSize

	if firstBlock == lastBlock {
		if err := s.readSingleBlock(ctx, ino, firstBlock, offset, end, offset, out, preZeroed); err != nil {
			return nil, err
		}
	} else {
		if err := s.readMultiBlocks(ctx, ino, firstBlock, lastBlock, offset, end, out, preZeroed); err != nil {
			return nil, err
		}
	}

	s.touchReadAtime(&inode)
	return out, nil
}

func (s *StoreActor) readSingleBlock(ctx context.Context, ino, blockIdx, rangeStart, rangeEnd, baseOffset uint64, out []byte, preZeroed bool) error {
	ref, ok, err := s.readDataBlobRefFromDB(ctx, ino, blockIdx)
	if err != nil {
		return err
	}
	if !ok {
		if !preZeroed {
			dstStart := rangeStart - baseOffset
			clear(out[dstStart : dstStart+(rangeEnd-rangeStart)])
		}
		return nil
	}
	return s.readBlockSlice(ctx, ref, rangeStart, rangeEnd, baseOffset, out, preZeroed)
}

type scannedDataBlock struct {
	blockIdx uint64
	ref      BlobRef
}

func (s *StoreActor) scanDataBlockRefs(ctx context.Context, ino, firstBlock, lastBlock uint64) (hits []scannedDataBlock, err error) {
	var span trace.Span
	if isSpanRecording(ctx) {
		_, span = startChildSpan(ctx, "store.KVScan",
			attribute.String("scan.kind", "data_blocks"),
			attribute.Int64("inode.ino", int64(ino)),
		)
	}
	var scanStart time.Time
	if s.metrics != nil {
		scanStart = time.Now()
	}
	defer func() {
		if s.metrics != nil {
			s.metrics.observeKVPrefixScan(time.Since(scanStart))
		}
		if span != nil {
			span.SetAttributes(attribute.Int("scan.hits", len(hits)))
			finishSpan(span, err)
		}
	}()

	iter, openErr := s.db.NewIter(nil)
	if openErr != nil {
		return nil, wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
		if err != nil {
			hits = nil
		}
	}()

	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, firstBlock)

	capHint := int(min(lastBlock-firstBlock+1, uint64(32)))
	hits = make([]scannedDataBlock, 0, capHint)
	for valid := iter.SeekPrefixGE(startKey[:]); valid; valid = iter.Next() {
		keyIno, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		if keyIno != ino || blockIdx > lastBlock {
			break
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		hits = append(hits, scannedDataBlock{
			blockIdx: blockIdx,
			ref:      ref,
		})
	}
	return hits, nil
}

type coalescedDiskRead struct {
	active     bool
	packID     uint64
	packOffset uint64
	outStart   uint64
	length     uint64
	lastBlock  uint64
	lastRef    BlobRef
	blockCount uint64
}

func (s *StoreActor) readMultiBlocks(ctx context.Context, ino, firstBlock, lastBlock, offset, end uint64, out []byte, preZeroed bool) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	blobs := s.blobs
	if blobs == nil {
		return ErrIO
	}
	hits, err := s.scanDataBlockRefs(ctx, ino, firstBlock, lastBlock)
	if err != nil {
		return err
	}

	var run coalescedDiskRead
	flushRun := func() error {
		if !run.active {
			return nil
		}
		cur := run
		run = coalescedDiskRead{}
		return blobs.readRangeAtContext(ctx, cur.packID, int64(cur.packOffset), out[cur.outStart:cur.outStart+cur.length], cur.blockCount)
	}

	filledEnd := offset
	for _, h := range hits {
		blockStart := h.blockIdx * BlobSize
		blockEnd := blockStart + BlobSize
		rangeStart := max(offset, blockStart)
		rangeEnd := min(end, blockEnd)
		if rangeStart >= rangeEnd {
			continue
		}
		if rangeStart > filledEnd {
			if err := flushRun(); err != nil {
				return err
			}
			if !preZeroed {
				clear(out[filledEnd-offset : rangeStart-offset])
			}
		}

		inBlockStart := rangeStart % BlobSize
		reqLen := rangeEnd - rangeStart
		dstStart := rangeStart - offset
		dst := out[dstStart : dstStart+reqLen]

		if inBlockStart >= uint64(h.ref.Length) {
			if err := flushRun(); err != nil {
				return err
			}
			if !preZeroed {
				clear(dst)
			}
			filledEnd = rangeEnd
			continue
		}

		if h.ref.PackID == 0 || h.ref.Length == 0 || h.ref.Length > BlobSize || uint64(h.ref.Offset)+uint64(h.ref.Length) > PackMaxSize {
			if err := flushRun(); err != nil {
				return err
			}
			return ErrInvalidValue
		}

		n := min(reqLen, uint64(h.ref.Length)-inBlockStart)
		// 一致性守卫：仍在 hotShard 内存热缓存中的块（活跃包尾块或部分覆写块）优先命中最新内存字节，
		// 仅对不在 hotShard 中的相邻物理连续冷块区间合并执行单次底层 pread。
		if blobs.tryReadHot(h.ref, int64(inBlockStart), dst[:n]) {
			if err := flushRun(); err != nil {
				return err
			}
			if !preZeroed && n < reqLen {
				clear(dst[n:])
			}
			filledEnd = rangeEnd
			continue
		}

		canMerge := run.active &&
			h.blockIdx == run.lastBlock+1 &&
			h.ref.PackID == run.packID &&
			run.lastRef.Length == BlobSize &&
			uint64(run.lastRef.Offset)+uint64(run.lastRef.Length) == uint64(h.ref.Offset) &&
			inBlockStart == 0 &&
			run.packOffset+run.length == uint64(h.ref.Offset) &&
			run.outStart+run.length == dstStart
		if canMerge {
			run.length += n
			run.lastBlock = h.blockIdx
			run.lastRef = h.ref
			run.blockCount++
		} else {
			if err := flushRun(); err != nil {
				return err
			}
			run = coalescedDiskRead{
				active:     true,
				packID:     h.ref.PackID,
				packOffset: uint64(h.ref.Offset) + inBlockStart,
				outStart:   dstStart,
				length:     n,
				lastBlock:  h.blockIdx,
				lastRef:    h.ref,
				blockCount: 1,
			}
		}

		if !preZeroed && n < reqLen {
			clear(dst[n:])
		}
		filledEnd = rangeEnd
	}

	if err := flushRun(); err != nil {
		return err
	}
	if !preZeroed && filledEnd < end {
		clear(out[filledEnd-offset : end-offset])
	}
	return nil
}

// readBlockSlice 从 ref 指向的物理封包区间读取 [rangeStart, rangeEnd) 填入 out，
// 仅对块内尾部稀疏未落盘区间执行清零，避免对有效物理载荷区间的双重写入。
func (s *StoreActor) readBlockSlice(ctx context.Context, ref BlobRef, rangeStart, rangeEnd, baseOffset uint64, out []byte, preZeroed bool) error {
	blobs := s.blobs
	if blobs == nil {
		return ErrIO
	}
	inBlockStart := rangeStart % BlobSize
	reqLen := rangeEnd - rangeStart
	dstStart := rangeStart - baseOffset
	dst := out[dstStart : dstStart+reqLen]
	if inBlockStart >= uint64(ref.Length) {
		if !preZeroed {
			clear(dst)
		}
		return nil
	}
	n := min(reqLen, uint64(ref.Length)-inBlockStart)
	if err := blobs.readAtContext(ctx, ref, int64(inBlockStart), dst[:n]); err != nil {
		return err
	}
	if !preZeroed && n < reqLen {
		clear(dst[n:])
	}
	return nil
}
