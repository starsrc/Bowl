package store

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble/v2"
)

const (
	BranchStatSize  = 41
	PackMetaSize    = 17
	EntryValueSize  = 13
	InodeHeaderSize = 93
	BlobRefSize     = 16
)

const codecVersion byte = 1

const (
	OrphanKindUnlinked         byte = 0x01
	OrphanKindTmpfileExclusive byte = 0x02
	OrphanKindTmpfileLinkable  byte = 0x03
)

func DecodeOrphanKind(b []byte) (byte, error) {
	if len(b) != 1 {
		return 0, ErrInvalidValue
	}
	switch b[0] {
	case OrphanKindUnlinked, OrphanKindTmpfileExclusive, OrphanKindTmpfileLinkable:
		return b[0], nil
	default:
		return 0, ErrInvalidValue
	}
}

type BranchStat struct {
	LogicalBytes uint64
	Inodes       uint64
	NextWriteSeq uint64
	WriterEpoch  uint64
	MaxBytes     uint64
}

func EncodeBranchStat(s BranchStat) []byte {
	out := make([]byte, BranchStatSize)
	putBranchStat(out, s)
	return out
}

func putBranchStat(dst []byte, s BranchStat) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], s.LogicalBytes)
	binary.BigEndian.PutUint64(dst[9:17], s.Inodes)
	binary.BigEndian.PutUint64(dst[17:25], s.NextWriteSeq)
	binary.BigEndian.PutUint64(dst[25:33], s.WriterEpoch)
	binary.BigEndian.PutUint64(dst[33:41], s.MaxBytes)
}

func DecodeBranchStat(b []byte) (BranchStat, error) {
	if len(b) != BranchStatSize || b[0] != codecVersion {
		return BranchStat{}, ErrInvalidValue
	}
	return BranchStat{
		LogicalBytes: binary.BigEndian.Uint64(b[1:9]),
		Inodes:       binary.BigEndian.Uint64(b[9:17]),
		NextWriteSeq: binary.BigEndian.Uint64(b[17:25]),
		WriterEpoch:  binary.BigEndian.Uint64(b[25:33]),
		MaxBytes:     binary.BigEndian.Uint64(b[33:41]),
	}, nil
}

type Entry struct {
	Ino  uint64
	Mode uint32
}

func EncodeEntry(e Entry) []byte {
	out := make([]byte, EntryValueSize)
	putEntry(out, e)
	return out
}

func putEntry(dst []byte, e Entry) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], e.Ino)
	binary.BigEndian.PutUint32(dst[9:13], e.Mode)
}

func DecodeEntry(b []byte) (Entry, error) {
	if len(b) != EntryValueSize || b[0] != codecVersion {
		return Entry{}, ErrInvalidValue
	}
	return Entry{
		Ino:  binary.BigEndian.Uint64(b[1:9]),
		Mode: binary.BigEndian.Uint32(b[9:13]),
	}, nil
}

type Inode struct {
	Ino           uint64
	ParentIno     uint64
	Mode          uint32
	Nlink         uint32
	UID           uint32
	GID           uint32
	Rdev          uint32
	Size          uint64
	Atime         int64
	Mtime         int64
	Ctime         int64
	Flags         uint32
	Generation    uint64
	WriteSeq      uint64
	SymlinkTarget []byte
}

type Attribute struct {
	Ino        uint64
	ParentIno  uint64
	Generation uint64
	WriteSeq   uint64
	Mode       uint32
	Nlink      uint32
	UID        uint32
	GID        uint32
	Rdev       uint32
	Size       uint64
	Blksize    uint32
	Blocks     uint64
	Atime      int64
	Mtime      int64
	Ctime      int64
	Flags      uint32
}

func (i *Inode) ToAttribute() Attribute {
	size := i.Size
	if i.Mode&S_IFMT == S_IFDIR && size == 0 {
		size = DefaultDirSize
	}
	var blocks uint64
	switch i.Mode & S_IFMT {
	case S_IFDIR, S_IFREG, S_IFLNK:
		blocks = (size + 511) / 512
	}
	var rdev uint32
	if i.Mode&S_IFMT == S_IFCHR || i.Mode&S_IFMT == S_IFBLK {
		rdev = i.Rdev
	}
	return Attribute{
		Ino:        i.Ino,
		ParentIno:  i.ParentIno,
		Generation: i.Generation,
		WriteSeq:   i.WriteSeq,
		Mode:       i.Mode,
		Nlink:      i.Nlink,
		UID:        i.UID,
		GID:        i.GID,
		Rdev:       rdev,
		Size:       size,
		Blksize:    DefaultBlockSize,
		Blocks:     blocks,
		Atime:      i.Atime,
		Mtime:      i.Mtime,
		Ctime:      i.Ctime,
		Flags:      i.Flags,
	}
}

func EncodeInode(i Inode) ([]byte, error) {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return nil, ErrInvalidValue
	}
	out := make([]byte, InodeHeaderSize+len(i.SymlinkTarget))
	putInode(out, &i)
	return out, nil
}

func putInode(dst []byte, i *Inode) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], i.Ino)
	binary.BigEndian.PutUint64(dst[9:17], i.ParentIno)
	binary.BigEndian.PutUint32(dst[17:21], i.Mode)
	binary.BigEndian.PutUint32(dst[21:25], i.Nlink)
	binary.BigEndian.PutUint32(dst[25:29], i.UID)
	binary.BigEndian.PutUint32(dst[29:33], i.GID)
	binary.BigEndian.PutUint32(dst[33:37], i.Rdev)
	binary.BigEndian.PutUint64(dst[37:45], i.Size)
	binary.BigEndian.PutUint64(dst[45:53], uint64(i.Atime))
	binary.BigEndian.PutUint64(dst[53:61], uint64(i.Mtime))
	binary.BigEndian.PutUint64(dst[61:69], uint64(i.Ctime))
	binary.BigEndian.PutUint32(dst[69:73], i.Flags)
	binary.BigEndian.PutUint64(dst[73:81], i.Generation)
	binary.BigEndian.PutUint64(dst[81:89], i.WriteSeq)
	binary.BigEndian.PutUint32(dst[89:93], uint32(len(i.SymlinkTarget)))
	copy(dst[93:], i.SymlinkTarget)
}

func DecodeInode(b []byte) (Inode, error) {
	if len(b) < InodeHeaderSize || b[0] != codecVersion {
		return Inode{}, ErrInvalidValue
	}
	targetLen := binary.BigEndian.Uint32(b[89:93])
	if uint64(len(b)-InodeHeaderSize) != uint64(targetLen) {
		return Inode{}, ErrInvalidValue
	}
	var target []byte
	if targetLen > 0 {
		target = append([]byte(nil), b[93:]...)
	}
	return Inode{
		Ino:           binary.BigEndian.Uint64(b[1:9]),
		ParentIno:     binary.BigEndian.Uint64(b[9:17]),
		Mode:          binary.BigEndian.Uint32(b[17:21]),
		Nlink:         binary.BigEndian.Uint32(b[21:25]),
		UID:           binary.BigEndian.Uint32(b[25:29]),
		GID:           binary.BigEndian.Uint32(b[29:33]),
		Rdev:          binary.BigEndian.Uint32(b[33:37]),
		Size:          binary.BigEndian.Uint64(b[37:45]),
		Atime:         int64(binary.BigEndian.Uint64(b[45:53])),
		Mtime:         int64(binary.BigEndian.Uint64(b[53:61])),
		Ctime:         int64(binary.BigEndian.Uint64(b[61:69])),
		Flags:         binary.BigEndian.Uint32(b[69:73]),
		Generation:    binary.BigEndian.Uint64(b[73:81]),
		WriteSeq:      binary.BigEndian.Uint64(b[81:89]),
		SymlinkTarget: target,
	}, nil
}

type BlobRef struct {
	PackID uint64
	Offset uint32
	Length uint32
}

func EncodeBlobRef(ref BlobRef) []byte {
	out := make([]byte, BlobRefSize)
	putBlobRef(out, ref)
	return out
}

func putBlobRef(dst []byte, ref BlobRef) {
	binary.BigEndian.PutUint64(dst[0:8], ref.PackID)
	binary.BigEndian.PutUint32(dst[8:12], ref.Offset)
	binary.BigEndian.PutUint32(dst[12:16], ref.Length)
}

func DecodeBlobRef(b []byte) (BlobRef, error) {
	if len(b) != BlobRefSize {
		return BlobRef{}, ErrInvalidValue
	}
	ref := BlobRef{
		PackID: binary.BigEndian.Uint64(b[0:8]),
		Offset: binary.BigEndian.Uint32(b[8:12]),
		Length: binary.BigEndian.Uint32(b[12:16]),
	}
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > BlobSize || uint64(ref.Offset)+uint64(ref.Length) > PackMaxSize {
		return BlobRef{}, ErrInvalidValue
	}
	return ref, nil
}

type PackMeta struct {
	LiveBytes  uint64
	TotalBytes uint64
}

func EncodePackMeta(m PackMeta) []byte {
	out := make([]byte, PackMetaSize)
	putPackMeta(out, m)
	return out
}

func putPackMeta(dst []byte, m PackMeta) {
	dst[0] = codecVersion
	binary.BigEndian.PutUint64(dst[1:9], m.LiveBytes)
	binary.BigEndian.PutUint64(dst[9:17], m.TotalBytes)
}

func DecodePackMeta(b []byte) (PackMeta, error) {
	if len(b) != PackMetaSize || b[0] != codecVersion {
		return PackMeta{}, ErrInvalidValue
	}
	m := PackMeta{
		LiveBytes:  binary.BigEndian.Uint64(b[1:9]),
		TotalBytes: binary.BigEndian.Uint64(b[9:17]),
	}
	if m.LiveBytes > m.TotalBytes || m.TotalBytes > PackMaxSize {
		return PackMeta{}, ErrInvalidValue
	}
	return m, nil
}

type StatFSResult struct {
	TotalBytes uint64
	UsedBytes  uint64
	AvailBytes uint64
	Bsize      uint32
	Frsize     uint32
	Blocks     uint64
	Bfree      uint64
	Bavail     uint64
	Files      uint64
	Ffree      uint64
	Namelen    uint32
}

func BatchSetStatDeferred(b *pebble.Batch, s BranchStat) {
	op := b.SetDeferred(StatKeyLen, BranchStatSize)
	putStatKey(op.Key)
	putBranchStat(op.Value, s)
	_ = op.Finish()
}

func BatchSetPackMetaDeferred(b *pebble.Batch, packID uint64, m PackMeta) {
	op := b.SetDeferred(PackStatKeyLen, PackMetaSize)
	putPackStatKey(op.Key, packID)
	putPackMeta(op.Value, m)
	_ = op.Finish()
}

func BatchDeletePackMetaDeferred(b *pebble.Batch, packID uint64) {
	op := b.DeleteDeferred(PackStatKeyLen)
	putPackStatKey(op.Key, packID)
	_ = op.Finish()
}

func BatchSetInodeDeferred(b *pebble.Batch, i Inode) error {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return ErrInvalidValue
	}
	op := b.SetDeferred(InodeKeyLen, InodeHeaderSize+len(i.SymlinkTarget))
	putInodeKey(op.Key, i.Ino)
	putInode(op.Value, &i)
	_ = op.Finish()
	return nil
}

func BatchSetEntryDeferred(b *pebble.Batch, parent uint64, name []byte, e Entry) {
	op := b.SetDeferred(EntryPrefixLen+len(name), EntryValueSize)
	putEntryKey(op.Key, parent, name)
	putEntry(op.Value, e)
	_ = op.Finish()
}

func BatchSetXattrDeferred(b *pebble.Batch, ino uint64, name, val []byte) {
	op := b.SetDeferred(XattrPrefixLen+len(name), len(val))
	putXattrKey(op.Key, ino, name)
	copy(op.Value, val)
	_ = op.Finish()
}

func BatchSetOrphanKindDeferred(b *pebble.Batch, ino uint64, kind byte) {
	op := b.SetDeferred(OrphanKeyLen, 1)
	putOrphanKey(op.Key, ino)
	op.Value[0] = kind
	_ = op.Finish()
}

func BatchSetOrphanDeferred(b *pebble.Batch, ino uint64) {
	BatchSetOrphanKindDeferred(b, ino, OrphanKindUnlinked)
}

func BatchSetDataDeferred(b *pebble.Batch, ino, blockIdx uint64, ref BlobRef) {
	op := b.SetDeferred(DataKeyLen, BlobRefSize)
	putDataKey(op.Key, ino, blockIdx)
	putBlobRef(op.Value, ref)
	_ = op.Finish()
}

func BatchDeleteDeferred(b *pebble.Batch, key []byte) {
	op := b.DeleteDeferred(len(key))
	copy(op.Key, key)
	_ = op.Finish()
}

func BatchDeleteInodeDeferred(b *pebble.Batch, ino uint64) {
	op := b.DeleteDeferred(InodeKeyLen)
	putInodeKey(op.Key, ino)
	_ = op.Finish()
}

func BatchDeleteEntryDeferred(b *pebble.Batch, parent uint64, name []byte) {
	op := b.DeleteDeferred(EntryPrefixLen + len(name))
	putEntryKey(op.Key, parent, name)
	_ = op.Finish()
}

func BatchDeleteXattrDeferred(b *pebble.Batch, ino uint64, name []byte) {
	op := b.DeleteDeferred(XattrPrefixLen + len(name))
	putXattrKey(op.Key, ino, name)
	_ = op.Finish()
}

func BatchDeleteOrphanDeferred(b *pebble.Batch, ino uint64) {
	op := b.DeleteDeferred(OrphanKeyLen)
	putOrphanKey(op.Key, ino)
	_ = op.Finish()
}

func BatchDeleteDataDeferred(b *pebble.Batch, ino, blockIdx uint64) {
	op := b.DeleteDeferred(DataKeyLen)
	putDataKey(op.Key, ino, blockIdx)
	_ = op.Finish()
}
