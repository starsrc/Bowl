package store

import (
	"bytes"
	"encoding/binary"
)

const (
	StatKeyLen        = 8
	PackStatPrefixLen = 9
	PackStatKeyLen    = 17
	InodeKeyLen       = 14
	EntryPrefixLen    = 15
	XattrPrefixLen    = 15
	OrphanKeyLen      = 15
	DataPrefixLen     = 14
	DataKeyLen        = 22
)

var (
	statKeyBytes        = []byte("sys:stat")
	packStatPrefixBytes = []byte("sys:pack:")
	inodePrefixBytes    = []byte("inode:")
	entryPrefixBytes    = []byte("entry:")
	xattrPrefixBytes    = []byte("xattr:")
	orphanPrefixBytes   = []byte("orphan:")
	dataPrefixBytes     = []byte("data:")
)

func SplitKey(key []byte) int {
	if len(key) >= EntryPrefixLen && key[14] == ':' {
		if bytes.HasPrefix(key, entryPrefixBytes) || bytes.HasPrefix(key, xattrPrefixBytes) {
			return EntryPrefixLen
		}
	}
	if len(key) >= DataPrefixLen && key[13] == ':' && bytes.HasPrefix(key, dataPrefixBytes) {
		return DataPrefixLen
	}
	return len(key)
}

func StatKey() []byte {
	out := make([]byte, StatKeyLen)
	putStatKey(out)
	return out
}

func putStatKey(dst []byte) {
	copy(dst, statKeyBytes)
}

func PackStatPrefix() []byte {
	return append([]byte(nil), packStatPrefixBytes...)
}

func PackStatKey(packID uint64) []byte {
	out := make([]byte, PackStatKeyLen)
	putPackStatKey(out, packID)
	return out
}

func putPackStatKey(dst []byte, packID uint64) {
	copy(dst, packStatPrefixBytes)
	binary.BigEndian.PutUint64(dst[9:17], packID)
}

func DecodePackStatKey(key []byte) (uint64, error) {
	if len(key) != PackStatKeyLen || !bytes.HasPrefix(key, packStatPrefixBytes) {
		return 0, ErrInvalidKey
	}
	packID := binary.BigEndian.Uint64(key[9:17])
	if packID == 0 {
		return 0, ErrInvalidKey
	}
	return packID, nil
}

func InodeKey(ino uint64) []byte {
	out := make([]byte, InodeKeyLen)
	putInodeKey(out, ino)
	return out
}

func putInodeKey(dst []byte, ino uint64) {
	copy(dst, inodePrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], ino)
}

func DecodeInodeKey(key []byte) (uint64, error) {
	if len(key) != InodeKeyLen || !bytes.HasPrefix(key, inodePrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[6:14]), nil
}

func EntryPrefix(parent uint64) []byte {
	out := make([]byte, EntryPrefixLen)
	copy(out, entryPrefixBytes)
	binary.BigEndian.PutUint64(out[6:14], parent)
	out[14] = ':'
	return out
}

func EntryKey(parent uint64, name []byte) []byte {
	out := make([]byte, EntryPrefixLen+len(name))
	putEntryKey(out, parent, name)
	return out
}

func putEntryKey(dst []byte, parent uint64, name []byte) {
	copy(dst, entryPrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], parent)
	dst[14] = ':'
	copy(dst[15:], name)
}

func DecodeEntryKey(key []byte) (parent uint64, name []byte, err error) {
	if len(key) < EntryPrefixLen || !bytes.HasPrefix(key, entryPrefixBytes) || key[14] != ':' {
		return 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[15:]...)
	return binary.BigEndian.Uint64(key[6:14]), name, nil
}

func XattrPrefix(ino uint64) []byte {
	out := make([]byte, XattrPrefixLen)
	copy(out, xattrPrefixBytes)
	binary.BigEndian.PutUint64(out[6:14], ino)
	out[14] = ':'
	return out
}

func XattrKey(ino uint64, name []byte) []byte {
	out := make([]byte, XattrPrefixLen+len(name))
	putXattrKey(out, ino, name)
	return out
}

func putXattrKey(dst []byte, ino uint64, name []byte) {
	copy(dst, xattrPrefixBytes)
	binary.BigEndian.PutUint64(dst[6:14], ino)
	dst[14] = ':'
	copy(dst[15:], name)
}

func DecodeXattrKey(key []byte) (ino uint64, name []byte, err error) {
	if len(key) < XattrPrefixLen || !bytes.HasPrefix(key, xattrPrefixBytes) || key[14] != ':' {
		return 0, nil, ErrInvalidKey
	}
	name = append([]byte(nil), key[15:]...)
	return binary.BigEndian.Uint64(key[6:14]), name, nil
}

func OrphanPrefix() []byte {
	return append([]byte(nil), orphanPrefixBytes...)
}

func OrphanKey(ino uint64) []byte {
	out := make([]byte, OrphanKeyLen)
	putOrphanKey(out, ino)
	return out
}

func putOrphanKey(dst []byte, ino uint64) {
	copy(dst, orphanPrefixBytes)
	binary.BigEndian.PutUint64(dst[7:15], ino)
}

func DecodeOrphanKey(key []byte) (uint64, error) {
	if len(key) != OrphanKeyLen || !bytes.HasPrefix(key, orphanPrefixBytes) {
		return 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[7:15]), nil
}

func DataPrefix(ino uint64) []byte {
	out := make([]byte, DataPrefixLen)
	copy(out, dataPrefixBytes)
	binary.BigEndian.PutUint64(out[5:13], ino)
	out[13] = ':'
	return out
}

func DataKey(ino, blockIdx uint64) []byte {
	out := make([]byte, DataKeyLen)
	putDataKey(out, ino, blockIdx)
	return out
}

func putDataKey(dst []byte, ino, blockIdx uint64) {
	copy(dst, dataPrefixBytes)
	binary.BigEndian.PutUint64(dst[5:13], ino)
	dst[13] = ':'
	binary.BigEndian.PutUint64(dst[14:22], blockIdx)
}

func DecodeDataKey(key []byte) (ino, blockIdx uint64, err error) {
	if len(key) != DataKeyLen || !bytes.HasPrefix(key, dataPrefixBytes) || key[13] != ':' {
		return 0, 0, ErrInvalidKey
	}
	return binary.BigEndian.Uint64(key[5:13]), binary.BigEndian.Uint64(key[14:22]), nil
}
