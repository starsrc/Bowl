package store

import (
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

type dataKeyEntry struct {
	key []byte
	ref BlobRef
}

type writeBlockSpec struct {
	blockIdx uint64
	newRef   BlobRef
	keep     bool
	sameRef  bool

	// Phase A 校验时从 s.batch 读取的实时旧块状态
	oldRef BlobRef
	oldOK  bool
}

func (s *StoreActor) readDataBlobRef(reader pebbleReader, ino, blockIdx uint64) (BlobRef, bool, error) {
	var keyBuf [DataKeyLen]byte
	putDataKey(keyBuf[:], ino, blockIdx)
	val, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return BlobRef{}, false, nil
	}
	if err != nil {
		return BlobRef{}, false, wrapIOErr(err)
	}
	ref, decErr := DecodeBlobRef(val)
	if closeErr := wrapIOErr(closer.Close()); decErr != nil || closeErr != nil {
		return BlobRef{}, false, errors.Join(decErr, closeErr)
	}
	return ref, true, nil
}

func (s *StoreActor) readDataBlobRefFromDB(ctx context.Context, ino, blockIdx uint64) (BlobRef, bool, error) {
	var keyBuf [DataKeyLen]byte
	putDataKey(keyBuf[:], ino, blockIdx)
	val, closer, err := s.dbGetPoint(ctx, keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return BlobRef{}, false, nil
	}
	if err != nil {
		return BlobRef{}, false, wrapIOErr(err)
	}
	ref, decErr := DecodeBlobRef(val)
	if closeErr := wrapIOErr(closer.Close()); decErr != nil || closeErr != nil {
		return BlobRef{}, false, errors.Join(decErr, closeErr)
	}
	return ref, true, nil
}

// collectDataKeys 通过 reader.NewIter(nil) + iter.SeekPrefixGE(DataPrefix(ino)) 激活 14B Prefix Bloom Filter，
// 收集指定节点的全部 DataKey 及对应 BlobRef。
func (s *StoreActor) collectDataKeys(reader pebbleReader, ino uint64) ([]dataKeyEntry, error) {
	return s.collectDataKeysInRange(reader, ino, 0, math.MaxUint64)
}

// collectDataKeysFrom 扫描指定节点从 startBlockIdx 起（闭区间 [startBlockIdx, +∞)）的所有 DataKey 及对应 BlobRef。
func (s *StoreActor) collectDataKeysFrom(reader pebbleReader, ino, startBlockIdx uint64) ([]dataKeyEntry, error) {
	return s.collectDataKeysInRange(reader, ino, startBlockIdx, math.MaxUint64)
}

// collectDataKeysInRange 扫描指定节点在闭区间 [startBlockIdx, endBlockIdx] 内的全部 DataKey 及对应 BlobRef。
func (s *StoreActor) collectDataKeysInRange(reader pebbleReader, ino, startBlockIdx, endBlockIdx uint64) (entries []dataKeyEntry, err error) {
	if startBlockIdx > endBlockIdx {
		return nil, nil
	}
	iter, openErr := reader.NewIter(nil)
	if openErr != nil {
		return nil, wrapIOErr(openErr)
	}
	defer func() {
		err = errors.Join(err, wrapIOErr(iter.Error()), wrapIOErr(iter.Close()))
		if err != nil {
			entries = nil
		}
	}()

	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, startBlockIdx)

	for valid := iter.SeekPrefixGE(startKey[:]); valid; valid = iter.Next() {
		keyIno, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		if keyIno != ino || blockIdx > endBlockIdx {
			break
		}
		if blockIdx < startBlockIdx {
			continue
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		entries = append(entries, dataKeyEntry{
			key: append([]byte(nil), iter.Key()...),
			ref: ref,
		})
	}
	return entries, nil
}

// resolveWriteBlockSpecs 在 Stage 2 Phase A 中统一从 s.batch 读取各目标逻辑块的实时旧引用状态；
// 对于 blockIdx * BlobSize >= inodeSize 的尾部越界新块，根据存储引擎不变量直接判定无旧引用（0 次点查）。
func (s *StoreActor) resolveWriteBlockSpecs(reader pebbleReader, ino, inodeSize uint64, specs []writeBlockSpec) error {
	for i := range specs {
		if specs[i].blockIdx*BlobSize >= inodeSize {
			specs[i].oldRef = BlobRef{}
			specs[i].oldOK = false
			continue
		}
		oldRef, oldOK, err := s.readDataBlobRef(reader, ino, specs[i].blockIdx)
		if err != nil {
			return err
		}
		specs[i].oldRef = oldRef
		specs[i].oldOK = oldOK
	}
	return nil
}

// deleteDataKeyEntries 在 Stage 2 Phase B 中统一扣减被移除块的存活字节并从批次中删除 DataKey。
func (s *StoreActor) deleteDataKeyEntries(entries []dataKeyEntry) {
	for _, entry := range entries {
		s.removeBlobRef(entry.ref)
		BatchDeleteDeferred(s.batch, entry.key)
	}
}

// applyWriteBlockSpecs 在 Stage 2 Phase B 中按 256 KiB 逻辑块网格计划统一更新存活字节账本与 DataKey。
func (s *StoreActor) applyWriteBlockSpecs(ino uint64, specs []writeBlockSpec) {
	for i := range specs {
		b := &specs[i]
		if b.keep {
			if !b.sameRef {
				if b.oldOK {
					s.removeBlobRef(b.oldRef)
				}
				s.addBlobRef(b.newRef)
				BatchSetDataDeferred(s.batch, ino, b.blockIdx, b.newRef)
			}
		} else if b.oldOK {
			s.removeBlobRef(b.oldRef)
			BatchDeleteDataDeferred(s.batch, ino, b.blockIdx)
		}
	}
}

// applyBlockMutations 在 Stage 2 Phase B 中统一执行整块范围删除与边界/覆写块更新。
func (s *StoreActor) applyBlockMutations(ino uint64, specs []writeBlockSpec, deleted []dataKeyEntry) {
	s.deleteDataKeyEntries(deleted)
	s.applyWriteBlockSpecs(ino, specs)
}
