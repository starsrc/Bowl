package engine

import (
	"bytes"
	"encoding/binary"
)

const (
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

func SplitKey(key []byte) int {
	if len(key) < LayerDataPrefixLen || key[0] != 'l' || key[1] != ':' || key[10] != ':' {
		return len(key)
	}
	if len(key) >= LayerParentPrefixLen && key[26] == ':' && bytes.Equal(key[10:18], layerParentTagBytes) {
		return LayerParentPrefixLen
	}
	if len(key) >= LayerEntryPrefixLen && key[25] == ':' {
		if bytes.Equal(key[10:17], layerEntryTagBytes) || bytes.Equal(key[10:17], layerXattrTagBytes) {
			return LayerEntryPrefixLen
		}
	}
	if len(key) >= LayerDataPrefixLen && key[24] == ':' && bytes.Equal(key[10:16], layerDataTagBytes) {
		return LayerDataPrefixLen
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

func MetaDiskKey() []byte {
	return append([]byte(nil), metaDiskKeyBytes...)
}

func MetaBranchPrefix() []byte {
	return append([]byte(nil), metaBranchPrefixBytes...)
}

func MetaBranchKey(name string) []byte {
	out := make([]byte, len(metaBranchPrefixBytes)+len(name))
	copy(out, metaBranchPrefixBytes)
	copy(out[len(metaBranchPrefixBytes):], name)
	return out
}

func MetaCommitPrefix() []byte {
	return append([]byte(nil), metaCommitPrefixBytes...)
}

func MetaCommitKey(id uint64) []byte {
	out := make([]byte, MetaCommitKeyLen)
	copy(out, metaCommitPrefixBytes)
	binary.BigEndian.PutUint64(out[12:20], id)
	return out
}

func DecodeMetaCommitKey(key []byte) (uint64, error) {
	if len(key) != MetaCommitKeyLen || !bytes.HasPrefix(key, metaCommitPrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[12:20]), nil
}

func MetaMountPrefix() []byte {
	return append([]byte(nil), metaMountPrefixBytes...)
}

func MetaMountKey(id string) []byte {
	out := make([]byte, len(metaMountPrefixBytes)+len(id))
	copy(out, metaMountPrefixBytes)
	copy(out[len(metaMountPrefixBytes):], id)
	return out
}

func LayerRangeBounds(layerID uint64) (start, end []byte) {
	start = make([]byte, LayerPrefixLen)
	copy(start, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(start[2:10], layerID)
	start[10] = ':'

	end = make([]byte, LayerPrefixLen)
	copy(end, start)
	end[10] = ';'
	return start, end
}

func LayerDataAllPrefix(layerID uint64) []byte {
	out := make([]byte, 16)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:16], layerDataSubPrefixOnly)
	return out
}

func LayerInodeKey(layerID, ino uint64) []byte {
	out := make([]byte, LayerInodeKeyLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:17], layerInodeTagBytes)
	binary.BigEndian.PutUint64(out[17:25], ino)
	return out
}

func DecodeLayerInodeKey(key []byte) (layerID, ino uint64, err error) {
	if len(key) != LayerInodeKeyLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:17], layerInodeTagBytes) {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[17:25]), nil
}

func LayerEntryPrefix(layerID, parent uint64) []byte {
	out := make([]byte, LayerEntryPrefixLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:17], layerEntryTagBytes)
	binary.BigEndian.PutUint64(out[17:25], parent)
	out[25] = ':'
	return out
}

func LayerEntryKey(layerID, parent uint64, name []byte) []byte {
	out := make([]byte, LayerEntryPrefixLen+len(name))
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:17], layerEntryTagBytes)
	binary.BigEndian.PutUint64(out[17:25], parent)
	out[25] = ':'
	copy(out[26:], name)
	return out
}

func DecodeLayerEntryKey(key []byte) (layerID, parent uint64, name []byte, err error) {
	if len(key) < LayerEntryPrefixLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:17], layerEntryTagBytes) || key[25] != ':' {
		return 0, 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[26:]...)
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[17:25]), name, nil
}

func LayerXattrPrefix(layerID, ino uint64) []byte {
	out := make([]byte, LayerXattrPrefixLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:17], layerXattrTagBytes)
	binary.BigEndian.PutUint64(out[17:25], ino)
	out[25] = ':'
	return out
}

func LayerXattrKey(layerID, ino uint64, name []byte) []byte {
	out := make([]byte, LayerXattrPrefixLen+len(name))
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:17], layerXattrTagBytes)
	binary.BigEndian.PutUint64(out[17:25], ino)
	out[25] = ':'
	copy(out[26:], name)
	return out
}

func DecodeLayerXattrKey(key []byte) (layerID, ino uint64, name []byte, err error) {
	if len(key) < LayerXattrPrefixLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:17], layerXattrTagBytes) || key[25] != ':' {
		return 0, 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[26:]...)
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[17:25]), name, nil
}

func LayerOrphanPrefix(layerID uint64) []byte {
	out := make([]byte, 18)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:18], layerOrphanTagBytes)
	return out
}

func LayerOrphanKey(layerID, ino uint64) []byte {
	out := make([]byte, LayerOrphanKeyLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:18], layerOrphanTagBytes)
	binary.BigEndian.PutUint64(out[18:26], ino)
	return out
}

func DecodeLayerOrphanKey(key []byte) (layerID, ino uint64, err error) {
	if len(key) != LayerOrphanKeyLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:18], layerOrphanTagBytes) {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[18:26]), nil
}

func LayerDataPrefix(layerID, ino uint64) []byte {
	out := make([]byte, LayerDataPrefixLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:16], layerDataTagBytes)
	binary.BigEndian.PutUint64(out[16:24], ino)
	out[24] = ':'
	return out
}

func LayerDataKey(layerID, ino, blockIdx uint64) []byte {
	out := make([]byte, LayerDataKeyLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:16], layerDataTagBytes)
	binary.BigEndian.PutUint64(out[16:24], ino)
	out[24] = ':'
	binary.BigEndian.PutUint64(out[25:33], blockIdx)
	return out
}

func DecodeLayerDataKey(key []byte) (layerID, ino, blockIdx uint64, err error) {
	if len(key) != LayerDataKeyLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:16], layerDataTagBytes) || key[24] != ':' {
		return 0, 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[16:24]), binary.BigEndian.Uint64(key[25:33]), nil
}

func LayerParentPrefix(layerID, ino uint64) []byte {
	out := make([]byte, LayerParentPrefixLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:18], layerParentTagBytes)
	binary.BigEndian.PutUint64(out[18:26], ino)
	out[26] = ':'
	return out
}

func LayerParentKey(layerID, ino, parentDirIno uint64) []byte {
	out := make([]byte, LayerParentKeyLen)
	copy(out, layerKeyPrefixBytes)
	binary.BigEndian.PutUint64(out[2:10], layerID)
	copy(out[10:18], layerParentTagBytes)
	binary.BigEndian.PutUint64(out[18:26], ino)
	out[26] = ':'
	binary.BigEndian.PutUint64(out[27:35], parentDirIno)
	return out
}

func DecodeLayerParentKey(key []byte) (layerID, ino, parentDirIno uint64, err error) {
	if len(key) != LayerParentKeyLen || !bytes.HasPrefix(key, layerKeyPrefixBytes) || !bytes.Equal(key[10:18], layerParentTagBytes) || key[26] != ':' {
		return 0, 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[2:10]), binary.BigEndian.Uint64(key[18:26]), binary.BigEndian.Uint64(key[27:35]), nil
}
