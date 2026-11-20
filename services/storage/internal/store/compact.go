package store

import (
	"bytes"
	"context"
	"errors"
	"slices"
)

type relocateBlockSpec struct {
	ino      uint64
	blockIdx uint64
	oldRef   BlobRef
	newRef   BlobRef

	matched bool
}

type compactRelocateOp struct {
	specs []relocateBlockSpec
}

func (s *StoreActor) CompactPacks(ctx context.Context) error {
	if s.readOnly {
		return ErrReadOnly
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

	candidates, err := s.selectLowWatermarkPacks(ctx)
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return s.FlushWAL()
	}

	candSet := make(map[uint64]struct{}, len(candidates))
	for _, id := range candidates {
		candSet[id] = struct{}{}
	}

	liveBlocks, err := s.scanLiveBlocksInPacks(candSet)
	if err != nil {
		return err
	}

	if len(liveBlocks) > 0 {

		s.pinPackIDs(candidates)

		var staged []BlobRef
		specs := make([]relocateBlockSpec, 0, len(liveBlocks))
		readBuf := make([]byte, BlobSize)
		for _, blk := range liveBlocks {
			if blk.oldRef.Length == 0 || blk.oldRef.Length > BlobSize {
				s.unpinStagedBlobs(staged)
				s.unpinPackIDs(candidates)
				return ErrInvalidValue
			}
			data := readBuf[:blk.oldRef.Length]
			if readErr := blobs.readAtContext(ctx, blk.oldRef, 0, data); readErr != nil {
				s.unpinStagedBlobs(staged)
				s.unpinPackIDs(candidates)
				return readErr
			}
			newRef, writeErr := s.stageAppend(ctx, blobs, blk.ino, data, &staged)
			if writeErr != nil {
				s.unpinStagedBlobs(staged)
				s.unpinPackIDs(candidates)
				return writeErr
			}
			specs = append(specs, relocateBlockSpec{
				ino:      blk.ino,
				blockIdx: blk.blockIdx,
				oldRef:   blk.oldRef,
				newRef:   newRef,
			})
		}

		if s.beforeCompactRelocateHook != nil {
			s.beforeCompactRelocateHook()
		}

		op := &compactRelocateOp{specs: specs}
		cmd := s.acquireCmd()
		cmd.op = opCompactRelocate
		cmd.req = op
		cmd.stagedBlobs = staged

		relocErr := s.enqueueAndWait(ctx, cmd)
		if relocErr != nil {
			s.unpinPackIDs(candidates)
			return relocErr
		}

		syncErr := blobs.sync()
		s.unpinPackIDs(candidates)
		if syncErr != nil {
			return syncErr
		}
	}

	if err := s.reapZeroLiveCandidatePacks(ctx, candidates); err != nil {
		return err
	}
	return s.FlushWAL()
}

func (s *StoreActor) selectLowWatermarkPacks(ctx context.Context) ([]uint64, error) {
	var candidates []uint64
	cmd := s.acquireCmd()
	cmd.op = opCompactSelect
	cmd.req = &candidates
	if err := s.enqueueAndWait(ctx, cmd); err != nil {
		return nil, err
	}
	return candidates, nil
}

func (s *StoreActor) validateCompactSelectCmd(cmd *writeCmd) error {
	out, _ := cmd.req.(*[]uint64)
	activeID := s.activePackID()
	for packID, meta := range s.packStats {
		if packID == 0 || packID == activeID {
			continue
		}
		if meta.LiveBytes > 0 && meta.LiveBytes*2 <= meta.TotalBytes {
			*out = append(*out, packID)
		}
	}
	slices.Sort(*out)
	return nil
}

type liveBlockCandidate struct {
	ino      uint64
	blockIdx uint64
	oldRef   BlobRef
}

func (s *StoreActor) scanLiveBlocksInPacks(candSet map[uint64]struct{}) (out []liveBlockCandidate, err error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	for valid := iter.SeekGE(dataPrefixBytes); valid && bytes.HasPrefix(iter.Key(), dataPrefixBytes); valid = iter.Next() {
		ino, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		if _, ok := candSet[ref.PackID]; ok {
			out = append(out, liveBlockCandidate{
				ino:      ino,
				blockIdx: blockIdx,
				oldRef:   ref,
			})
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return out, nil
}

func (s *StoreActor) validateCompactRelocateCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*compactRelocateOp)
	for i := range op.specs {
		spec := &op.specs[i]
		spec.matched = false

		inode, err := s.readInode(s.batch, spec.ino)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err = s.checkInodeLive(s.batch, &inode); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return err
		}
		if inode.Mode&S_IFMT != S_IFREG {
			continue
		}

		if spec.blockIdx*BlobSize >= inode.Size {
			continue
		}

		curRef, ok, err := s.readDataBlobRef(s.batch, spec.ino, spec.blockIdx)
		if err != nil {
			return err
		}
		if ok && curRef == spec.oldRef {
			spec.matched = true
		}
	}
	return nil
}

func (s *StoreActor) mutateCompactRelocateCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*compactRelocateOp)
	for i := range op.specs {
		spec := &op.specs[i]
		if !spec.matched {
			s.recordDeadBlobWrite(spec.newRef)
			continue
		}
		s.removeBlobRef(spec.oldRef)
		s.addBlobRef(spec.newRef)
		BatchSetDataDeferred(s.batch, spec.ino, spec.blockIdx, spec.newRef)
	}
}

func (s *StoreActor) reapZeroLiveCandidatePacks(ctx context.Context, candidates []uint64) error {
	if len(candidates) == 0 {
		return nil
	}
	cmd := s.acquireCmd()
	cmd.op = opCompactReap
	cmd.req = candidates
	return s.enqueueAndWait(ctx, cmd)
}

func (s *StoreActor) mutateCompactReapCmd(cmd *writeCmd) {
	ids, _ := cmd.req.([]uint64)
	activeID := s.activePackID()
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if meta, ok := s.packStats[id]; ok {
			if meta.LiveBytes == 0 {
				s.dirtyPackStats[id] = struct{}{}
			}
		} else if id != activeID && !s.isPackPinned(id) {
			s.addPendingDeadPacks([]uint64{id})
		}
	}
}
