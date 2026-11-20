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
		return BlobRef{}, false, err
	}
	ref, decErr := DecodeBlobRef(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
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
		return BlobRef{}, false, err
	}
	ref, decErr := DecodeBlobRef(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return BlobRef{}, false, errors.Join(decErr, closeErr)
	}
	return ref, true, nil
}

func (s *StoreActor) collectDataKeys(reader pebbleReader, ino uint64) ([]dataKeyEntry, error) {
	return s.collectDataKeysInRange(reader, ino, 0, math.MaxUint64)
}

func (s *StoreActor) collectDataKeysFrom(reader pebbleReader, ino, startBlockIdx uint64) ([]dataKeyEntry, error) {
	return s.collectDataKeysInRange(reader, ino, startBlockIdx, math.MaxUint64)
}

func (s *StoreActor) collectDataKeysInRange(reader pebbleReader, ino, startBlockIdx, endBlockIdx uint64) (entries []dataKeyEntry, err error) {
	if startBlockIdx > endBlockIdx {
		return nil, nil
	}
	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

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
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return entries, nil
}

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

func (s *StoreActor) deleteDataKeyEntries(entries []dataKeyEntry) {
	for _, entry := range entries {
		s.removeBlobRef(entry.ref)
		BatchDeleteDeferred(s.batch, entry.key)
	}
}

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

func (s *StoreActor) applyBlockMutations(ino uint64, specs []writeBlockSpec, deleted []dataKeyEntry) {
	s.deleteDataKeyEntries(deleted)
	s.applyWriteBlockSpecs(ino, specs)
}
