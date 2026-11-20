package store

import (
	"context"
	"math"
	"time"
)

func (s *StoreActor) setAttrWithSize(ctx context.Context, ino uint64, req SetAttrReq) (Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return Attribute{}, err
	}
	defer unlock()

	blobs := s.blobs
	if blobs == nil {
		return Attribute{}, ErrIO
	}

	inoLock := s.lockInode(ctx, ino)
	defer inoLock.Unlock()

	newSize := *req.Size
	var (
		tailSpecs []writeBlockSpec
		staged    []BlobRef
	)

	if newSize > 0 {
		tailBlock := (newSize - 1) / BlobSize
		tailLen := uint32(newSize - tailBlock*BlobSize)
		if tailLen < BlobSize {
			oldRef, oldOK, err := s.readDataBlobRefFromDB(ctx, ino, tailBlock)
			if err != nil {
				return Attribute{}, err
			}
			if oldOK && oldRef.Length > tailLen {
				spec, err := s.prepareTruncateTailBlock(ctx, blobs, ino, tailBlock, oldRef, tailLen, &staged)
				if err != nil {
					s.unpinStagedBlobs(staged)
					return Attribute{}, err
				}
				tailSpecs = []writeBlockSpec{spec}
			}
		}
	}

	op := &setAttrOp{
		ino:       ino,
		req:       req,
		tailSpecs: tailSpecs,
	}
	cmd := s.acquireCmd()
	cmd.op = opSetAttr
	cmd.epoch = req.Epoch
	cmd.req = op
	cmd.stagedBlobs = staged
	if err := s.enqueueAndWait(ctx, cmd); err != nil {
		return Attribute{}, err
	}
	return op.attr, nil
}

func (s *StoreActor) prepareTruncateTailBlock(ctx context.Context, blobs *blobStore, ino, blockIdx uint64, oldRef BlobRef, tailLen uint32, staged *[]BlobRef) (writeBlockSpec, error) {
	data := make([]byte, tailLen)
	var readStart time.Time
	if s.metrics != nil {
		readStart = time.Now()
	}
	err := blobs.readAtContext(ctx, oldRef, 0, data)
	if s.metrics != nil {
		s.metrics.observeBlobRMWRead(time.Since(readStart))
	}
	if err != nil {
		return writeBlockSpec{}, err
	}
	if isAllZero(data) {
		s.metrics.recordBlobWrite(writeModeZeroElide, 1, uint64(len(data)))
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, nil
	}
	ref, err := s.stageAppend(ctx, blobs, ino, data, staged)
	if err != nil {
		return writeBlockSpec{}, err
	}
	s.metrics.recordBlobWrite(writeModeRMW, 1, uint64(len(data)))
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   ref,
		keep:     true,
	}, nil
}

type fallocateDataOp struct {
	ino          uint64
	offset       uint64
	length       uint64
	mode         uint32
	zeroRange    bool
	partialSpecs []writeBlockSpec

	target         Inode
	newSize        uint64
	bytesDelta     uint64
	fullDeleteKeys []dataKeyEntry
}

func (s *StoreActor) Fallocate(ctx context.Context, ino, offset, length uint64, mode uint32, epoch ...uint64) error {
	if s.readOnly {
		return ErrReadOnly
	}
	ep := resolveEpoch(epoch)
	if s.isDefinitelyStaleEpoch(ep) {
		return s.submitStaleEpoch(ctx, opFallocate, ep)
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}
	if length == 0 || offset > MaxFileSize || length > MaxFileSize-offset {
		return ErrInvalid
	}
	if mode == FallocPunchHole {
		return ErrInvalid
	}
	switch mode {
	case 0, FallocKeepSize, FallocPunchHole | FallocKeepSize, FallocZeroRange, FallocZeroRange | FallocKeepSize:
	default:
		return ErrNotSupported
	}

	unlock, err := s.enter()
	if err != nil {
		return err
	}
	defer unlock()

	blobs := s.blobs
	if blobs == nil {
		return ErrIO
	}

	inoLock := s.lockInode(ctx, ino)
	defer inoLock.Unlock()

	zeroRange := mode&FallocPunchHole != 0 || mode&FallocZeroRange != 0
	var (
		partialSpecs []writeBlockSpec
		staged       []BlobRef
	)

	if zeroRange {

		end := offset + length
		firstBlock := offset / BlobSize
		lastBlock := (end - 1) / BlobSize

		candidates := []uint64{firstBlock}
		if lastBlock != firstBlock {
			candidates = append(candidates, lastBlock)
		}
		for _, blockIdx := range candidates {
			blockStart := blockIdx * BlobSize
			blockEnd := blockStart + BlobSize
			rStart := max(offset, blockStart)
			rEnd := min(end, blockEnd)
			inBlock := int(rStart - blockStart)
			clearLen := int(rEnd - rStart)
			if inBlock == 0 && clearLen == BlobSize {

				continue
			}
			spec, hasSpec, err := s.preparePunchPartialBlock(ctx, blobs, ino, blockIdx, inBlock, clearLen, &staged)
			if err != nil {
				s.unpinStagedBlobs(staged)
				return err
			}
			if hasSpec {
				partialSpecs = append(partialSpecs, spec)
			}
		}
	}

	op := &fallocateDataOp{
		ino:          ino,
		offset:       offset,
		length:       length,
		mode:         mode,
		zeroRange:    zeroRange,
		partialSpecs: partialSpecs,
	}
	cmd := s.acquireCmd()
	cmd.op = opFallocate
	cmd.epoch = ep
	cmd.req = op
	cmd.stagedBlobs = staged
	return s.enqueueAndWait(ctx, cmd)
}

func (s *StoreActor) preparePunchPartialBlock(ctx context.Context, blobs *blobStore, ino, blockIdx uint64, inBlock, clearLen int, staged *[]BlobRef) (writeBlockSpec, bool, error) {
	oldRef, oldOK, err := s.readDataBlobRefFromDB(ctx, ino, blockIdx)
	if err != nil || !oldOK {
		return writeBlockSpec{}, false, err
	}
	if inBlock >= int(oldRef.Length) {
		return writeBlockSpec{}, false, nil
	}
	clearEnd := min(inBlock+clearLen, int(oldRef.Length))

	data := make([]byte, oldRef.Length)
	var readStart time.Time
	if s.metrics != nil {
		readStart = time.Now()
	}
	err = blobs.readAtContext(ctx, oldRef, 0, data)
	if s.metrics != nil {
		s.metrics.observeBlobRMWRead(time.Since(readStart))
	}
	if err != nil {
		return writeBlockSpec{}, false, err
	}
	if isAllZero(data[inBlock:clearEnd]) {
		return writeBlockSpec{}, false, nil
	}
	clear(data[inBlock:clearEnd])

	if isAllZero(data) {
		s.metrics.recordBlobWrite(writeModeZeroElide, 1, uint64(len(data)))
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, true, nil
	}

	ref, err := s.stageAppend(ctx, blobs, ino, data, staged)
	if err != nil {
		return writeBlockSpec{}, false, err
	}
	s.metrics.recordBlobWrite(writeModeFallocZero, 1, uint64(len(data)))
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   ref,
		keep:     true,
	}, true, nil
}

func (s *StoreActor) validateFallocateCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*fallocateDataOp)

	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, &inode); err != nil {
		return err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return ErrIsDir
	case S_IFREG:
	default:
		return ErrNotSupported
	}
	if inode.Flags&(FlagImmutable|FlagAppend) != 0 {
		return ErrPerm
	}

	end := op.offset + op.length
	newSize := inode.Size
	if op.mode&FallocKeepSize == 0 && end > newSize {
		newSize = end
	}

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

	if op.zeroRange {

		fullStart := (op.offset + BlobSize - 1) / BlobSize
		var fullEnd uint64
		hasFullRange := false
		if end >= BlobSize {
			fullEnd = (end / BlobSize) - 1
			if fullStart <= fullEnd {
				hasFullRange = true
			}
		}
		if hasFullRange {
			keys, err := s.collectDataKeysInRange(s.batch, op.ino, fullStart, fullEnd)
			if err != nil {
				return err
			}
			op.fullDeleteKeys = keys
		}

		if err := s.resolveWriteBlockSpecs(s.batch, op.ino, inode.Size, op.partialSpecs); err != nil {
			return err
		}
	}

	op.target = inode
	op.newSize = newSize
	op.bytesDelta = bytesDelta
	return nil
}

func (s *StoreActor) mutateFallocateCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*fallocateDataOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	inode := op.target
	oldSize := inode.Size
	inode.Size = op.newSize
	if op.zeroRange {
		inode.Mode &^= S_ISUID | S_ISGID
	}
	if op.zeroRange || op.newSize > oldSize {
		inode.Mtime = nowNano
	}
	inode.Ctime = nowNano
	inode.WriteSeq = seq

	s.stat.LogicalBytes += op.bytesDelta

	_ = BatchSetInodeDeferred(s.batch, inode)
	BatchDeleteXattrDeferred(s.batch, op.ino, securityCapabilityXattr)

	if op.zeroRange {
		s.applyBlockMutations(op.ino, op.partialSpecs, op.fullDeleteKeys)
	}
}
