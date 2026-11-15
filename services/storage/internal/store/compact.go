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

	// Phase A CAS 校验结果（true 表示节点仍存活且 curRef == oldRef）
	matched bool
}

type compactRelocateOp struct {
	specs []relocateBlockSpec
}

// CompactPacks 执行基于 ShardStore (SOSP '21) 的低水位已封口 .pack 搬迁回收：
//  1. 筛选存活率低于水位线（srcPackID != activePackID && LiveBytes > 0 && LiveBytes * 2 <= TotalBytes）的已封口 .pack；
//  2. 扫描当前分支 Pebble DataKey 找出仍指向候选 srcPackID 的活块 (ino, blockIdx, oldRef)，在队列外从源包读取块内容并追加写入活跃 .pack 获得 newRef；
//  3. 投递 opCompactRelocate 至 StoreActor：
//     - Phase A：通过 s.batch.Get(DataKey(ino, blockIdx)) 原子 CAS 校验目标节点仍存活且 curRef == oldRef；若已被前台并发覆写、截断或删除，跳过指针更新并将预写 newRef 记为死空间（不计入 LiveBytes）；
//     - Phase B：仅对通过 CAS 校验的块将 DataKey 更新为 newRef，扣减 srcPackID 的 LiveBytes 并累加 newRef.PackID 的 LiveBytes；
//  4. 当 srcPackID 的 LiveBytes 归零后，强制先完成目标新 .pack 的 fdatasync（blobs.sync()）与 Pebble WAL Sync（FlushWAL()）双重持久化屏障，随后删除旧 .pack 文件。
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

	// 1. 扫描当前分支 Pebble DataKey，找出所有指向低水位候选包的活块
	liveBlocks, err := s.scanLiveBlocksInPacks(candSet)
	if err != nil {
		return err
	}

	if len(liveBlocks) > 0 {
		// 先钉住所有源候选包，使 Actor Step 3 不会在目标新包完成 fdatasync 前提前触发 RemovePack。
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

		// 4. Soft Updates 双重持久化屏障：
		// 先将目标新 .pack 持久化落盘（fdatasync），再释放源包钉住并在 Actor 内归零回收，最后同步 Pebble WAL 并调用 removePack。
		syncErr := blobs.sync()
		s.unpinPackIDs(candidates)
		if syncErr != nil {
			return syncErr
		}
	}

	// 触发已归零候选封口包的 sys:pack 删除与 WAL Sync + RemovePack
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
	iter, openErr := s.db.NewIter(nil)
	if openErr != nil {
		return nil, wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
		if err != nil {
			out = nil
		}
	}()

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
	return out, nil
}

// validateCompactRelocateCmd 执行 Phase A 原子 CAS 校验（防御 ShardStore 论文 Bug #1）：
// 严格通过 s.batch.Get(DataKey(ino, blockIdx)) 校验目标节点仍存活且 curRef == oldRef。
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
		// 校验该逻辑块仍在当前文件 Size 范围内
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

// mutateCompactRelocateCmd 执行 Phase B 原子指针切换与 sys:pack 记账更新：
// 仅对 Phase A 通过 CAS 校验的块更新 DataKey -> newRef；未通过 CAS 校验的预写块仅推进目标包的 TotalBytes（LiveBytes 不增加）。
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
