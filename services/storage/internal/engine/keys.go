package engine

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

const (
	DiskPrefixLen        = 19
	CatalogIdxPrefixLen  = 8
	CatalogIdxKeyLen     = 33
	LayerPrefixLen       = 11
	LayerInodeKeyLen     = 25
	LayerEntryPrefixLen  = 26
	LayerXattrPrefixLen  = 26
	LayerOrphanKeyLen    = 26
	LayerDataPrefixLen   = 25
	LayerDataKeyLen      = 33
	LayerParentPrefixLen = 27
	LayerParentKeyLen    = 35
	MetaCommitKeyLen     = 20
	LayerDataSubPrefixOf = 17
)

var (
	catalogIdxPrefixBytes  = []byte("cat:idx:")
	metaDiskKeyBytes       = []byte("meta:disk")
	metaBranchPrefixBytes  = []byte("meta:branch:")
	metaCommitPrefixBytes  = []byte("meta:commit:")
	metaMountPrefixBytes   = []byte("meta:mount:")
	layerKeyPrefixBytes    = []byte("l:")
	layerInodeTagBytes     = []byte(":inode:")
	layerEntryTagBytes     = []byte(":entry:")
	layerXattrTagBytes     = []byte(":xattr:")
	layerOrphanTagBytes    = []byte(":orphan:")
	layerDataTagBytes      = []byte(":data:")
	layerDataSubPrefixOnly = []byte(":data:")
	layerParentTagBytes    = []byte(":parent:")
)

func NodeSplitKey(key []byte) int {
	if len(key) < DiskPrefixLen+LayerDataPrefixLen || key[0] != 'd' || key[1] != ':' || key[DiskPrefixLen-1] != ':' {
		return len(key)
	}
	sub := key[DiskPrefixLen:]
	if len(sub) < LayerDataPrefixLen || sub[0] != 'l' || sub[1] != ':' || sub[10] != ':' {
		return len(key)
	}
	if len(sub) >= LayerParentPrefixLen && sub[26] == ':' && bytes.Equal(sub[10:18], layerParentTagBytes) {
		return DiskPrefixLen + LayerParentPrefixLen
	}
	if len(sub) >= LayerEntryPrefixLen && sub[25] == ':' {
		if bytes.Equal(sub[10:17], layerEntryTagBytes) || bytes.Equal(sub[10:17], layerXattrTagBytes) {
			return DiskPrefixLen + LayerEntryPrefixLen
		}
	}
	if len(sub) >= LayerDataPrefixLen && sub[24] == ':' && bytes.Equal(sub[10:16], layerDataTagBytes) {
		return DiskPrefixLen + LayerDataPrefixLen
	}
	return len(key)
}

func PrefixUpperBound(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func DiskKeyPrefix(diskID uuid.UUID) []byte {
	dk := newDiskKeys(diskID)
	return dk.Prefix()
}

func CatalogIdxPrefix() []byte {
	return append([]byte(nil), catalogIdxPrefixBytes...)
}

func createdAtMicroFromNano(ns int64) int64 {
	return time.Unix(0, ns).UTC().Truncate(time.Microsecond).UnixMicro()
}

func CatalogIdxKey(createdAtMicro int64, diskID uuid.UUID) []byte {
	out := make([]byte, CatalogIdxKeyLen)
	copy(out, catalogIdxPrefixBytes)
	invTS := ^(uint64(createdAtMicro) ^ (uint64(1) << 63))
	binary.BigEndian.PutUint64(out[8:16], invTS)
	out[16] = ':'
	for i := range 16 {
		out[17+i] = ^diskID[i]
	}
	return out
}

func DecodeCatalogIdxKey(key []byte) (int64, uuid.UUID, error) {
	if len(key) != CatalogIdxKeyLen || !bytes.HasPrefix(key, catalogIdxPrefixBytes) || key[16] != ':' {
		return 0, uuid.Nil, ErrInvalidKey
	}
	invTS := binary.BigEndian.Uint64(key[8:16])
	us := int64((^invTS) ^ (uint64(1) << 63))
	var id uuid.UUID
	for i := range 16 {
		id[i] = ^key[17+i]
	}
	return us, id, nil
}

type diskKeys struct {
	prefix [DiskPrefixLen]byte
}

func newDiskKeys(diskID uuid.UUID) diskKeys {
	var dk diskKeys
	dk.prefix[0] = 'd'
	dk.prefix[1] = ':'
	copy(dk.prefix[2:18], diskID[:])
	dk.prefix[18] = ':'
	return dk
}

func (dk diskKeys) Prefix() []byte {
	out := make([]byte, DiskPrefixLen)
	copy(out, dk.prefix[:])
	return out
}

func (dk diskKeys) RangeBounds() (start, end []byte) {
	start = dk.Prefix()
	end = PrefixUpperBound(start)
	return start, end
}

func (dk diskKeys) stripPrefix(key []byte) ([]byte, bool) {
	if len(key) < DiskPrefixLen || !bytes.Equal(key[:DiskPrefixLen], dk.prefix[:]) {
		return nil, false
	}
	return key[DiskPrefixLen:], true
}

func (dk diskKeys) setLayerID(key []byte, layerID uint64) {
	binary.BigEndian.PutUint64(key[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
}

func (dk diskKeys) MetaDiskKey() []byte {
	out := make([]byte, DiskPrefixLen+len(metaDiskKeyBytes))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaDiskKeyBytes)
	return out
}

func (dk diskKeys) MetaBranchPrefix() []byte {
	out := make([]byte, DiskPrefixLen+len(metaBranchPrefixBytes))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaBranchPrefixBytes)
	return out
}

func (dk diskKeys) MetaBranchKey(name string) []byte {
	out := make([]byte, DiskPrefixLen+len(metaBranchPrefixBytes)+len(name))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaBranchPrefixBytes)
	copy(out[DiskPrefixLen+len(metaBranchPrefixBytes):], name)
	return out
}

func (dk diskKeys) MetaCommitPrefix() []byte {
	out := make([]byte, DiskPrefixLen+len(metaCommitPrefixBytes))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaCommitPrefixBytes)
	return out
}

func (dk diskKeys) MetaCommitKey(id uint64) []byte {
	out := make([]byte, DiskPrefixLen+MetaCommitKeyLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaCommitPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+12:DiskPrefixLen+20], id)
	return out
}

func (dk diskKeys) DecodeMetaCommitKey(key []byte) (uint64, error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) != MetaCommitKeyLen || !bytes.HasPrefix(sub, metaCommitPrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(sub[12:20]), nil
}

func (dk diskKeys) MetaMountPrefix() []byte {
	out := make([]byte, DiskPrefixLen+len(metaMountPrefixBytes))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaMountPrefixBytes)
	return out
}

func (dk diskKeys) MetaMountKey(id string) []byte {
	out := make([]byte, DiskPrefixLen+len(metaMountPrefixBytes)+len(id))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], metaMountPrefixBytes)
	copy(out[DiskPrefixLen+len(metaMountPrefixBytes):], id)
	return out
}

func (dk diskKeys) LayerRangeBounds(layerID uint64) (start, end []byte) {
	start = make([]byte, DiskPrefixLen+LayerPrefixLen)
	copy(start, dk.prefix[:])
	copy(start[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(start[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	start[DiskPrefixLen+10] = ':'

	end = make([]byte, DiskPrefixLen+LayerPrefixLen)
	copy(end, start)
	end[DiskPrefixLen+10] = ';'
	return start, end
}

func (dk diskKeys) LayerDataAllPrefix(layerID uint64) []byte {
	out := make([]byte, DiskPrefixLen+16)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+16], layerDataSubPrefixOnly)
	return out
}

func (dk diskKeys) LayerInodePrefix(layerID uint64) []byte {
	out := make([]byte, DiskPrefixLen+17)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerInodeTagBytes)
	return out
}

func (dk diskKeys) LayerInodeKey(layerID, ino uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerInodeKeyLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerInodeTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+17:DiskPrefixLen+25], ino)
	return out
}

func (dk diskKeys) DecodeLayerInodeKey(key []byte) (layerID, ino uint64, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) != LayerInodeKeyLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:17], layerInodeTagBytes) {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[17:25]), nil
}

func (dk diskKeys) LayerEntryPrefix(layerID, parent uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerEntryPrefixLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerEntryTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+17:DiskPrefixLen+25], parent)
	out[DiskPrefixLen+25] = ':'
	return out
}

func (dk diskKeys) LayerEntryKey(layerID, parent uint64, name []byte) []byte {
	out := make([]byte, DiskPrefixLen+LayerEntryPrefixLen+len(name))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerEntryTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+17:DiskPrefixLen+25], parent)
	out[DiskPrefixLen+25] = ':'
	copy(out[DiskPrefixLen+26:], name)
	return out
}

func (dk diskKeys) DecodeLayerEntryKey(key []byte) (layerID, parent uint64, name []byte, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) < LayerEntryPrefixLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:17], layerEntryTagBytes) || sub[25] != ':' {
		return 0, 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), sub[26:]...)
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[17:25]), name, nil
}

func (dk diskKeys) LayerXattrPrefix(layerID, ino uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerXattrPrefixLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerXattrTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+17:DiskPrefixLen+25], ino)
	out[DiskPrefixLen+25] = ':'
	return out
}

func (dk diskKeys) LayerXattrKey(layerID, ino uint64, name []byte) []byte {
	out := make([]byte, DiskPrefixLen+LayerXattrPrefixLen+len(name))
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+17], layerXattrTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+17:DiskPrefixLen+25], ino)
	out[DiskPrefixLen+25] = ':'
	copy(out[DiskPrefixLen+26:], name)
	return out
}

func (dk diskKeys) DecodeLayerXattrKey(key []byte) (layerID, ino uint64, name []byte, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) < LayerXattrPrefixLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:17], layerXattrTagBytes) || sub[25] != ':' {
		return 0, 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), sub[26:]...)
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[17:25]), name, nil
}

func (dk diskKeys) LayerOrphanPrefix(layerID uint64) []byte {
	out := make([]byte, DiskPrefixLen+18)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+18], layerOrphanTagBytes)
	return out
}

func (dk diskKeys) LayerOrphanKey(layerID, ino uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerOrphanKeyLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+18], layerOrphanTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+18:DiskPrefixLen+26], ino)
	return out
}

func (dk diskKeys) DecodeLayerOrphanKey(key []byte) (layerID, ino uint64, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) != LayerOrphanKeyLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:18], layerOrphanTagBytes) {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[18:26]), nil
}

func (dk diskKeys) LayerDataPrefix(layerID, ino uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerDataPrefixLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+16], layerDataTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+16:DiskPrefixLen+24], ino)
	out[DiskPrefixLen+24] = ':'
	return out
}

func (dk diskKeys) LayerDataKey(layerID, ino, blockIdx uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerDataKeyLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+16], layerDataTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+16:DiskPrefixLen+24], ino)
	out[DiskPrefixLen+24] = ':'
	binary.BigEndian.PutUint64(out[DiskPrefixLen+25:DiskPrefixLen+33], blockIdx)
	return out
}

func (dk diskKeys) DecodeLayerDataKey(key []byte) (layerID, ino, blockIdx uint64, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) != LayerDataKeyLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:16], layerDataTagBytes) || sub[24] != ':' {
		return 0, 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[16:24]), binary.BigEndian.Uint64(sub[25:33]), nil
}

func (dk diskKeys) LayerParentPrefix(layerID, ino uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerParentPrefixLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+18], layerParentTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+18:DiskPrefixLen+26], ino)
	out[DiskPrefixLen+26] = ':'
	return out
}

func (dk diskKeys) LayerParentKey(layerID, ino, parentDirIno uint64) []byte {
	out := make([]byte, DiskPrefixLen+LayerParentKeyLen)
	copy(out, dk.prefix[:])
	copy(out[DiskPrefixLen:], layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+2:DiskPrefixLen+10], layerID)
	copy(out[DiskPrefixLen+10:DiskPrefixLen+18], layerParentTagBytes)
	binary.BigEndian.PutUint64(out[DiskPrefixLen+18:DiskPrefixLen+26], ino)
	out[DiskPrefixLen+26] = ':'
	binary.BigEndian.PutUint64(out[DiskPrefixLen+27:DiskPrefixLen+35], parentDirIno)
	return out
}

func (dk diskKeys) DecodeLayerParentKey(key []byte) (layerID, ino, parentDirIno uint64, err error) {
	sub, ok := dk.stripPrefix(key)
	if !ok || len(sub) != LayerParentKeyLen || !bytes.HasPrefix(sub, layerKeyPrefixBytes) || !bytes.Equal(sub[10:18], layerParentTagBytes) || sub[26] != ':' {
		return 0, 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(sub[2:10]), binary.BigEndian.Uint64(sub[18:26]), binary.BigEndian.Uint64(sub[27:35]), nil
}
