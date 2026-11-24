package engine

import (
	"encoding/binary"
	"math"
)

const codecVersion byte = 1

const (
	EntryValueSize  = 13
	InodeHeaderSize = 93
	BlobRefSize     = 16
)

type DiskMeta struct {
	DiskID        string
	Name          string
	DefaultBranch string
	MaxBytes      uint64
	BlockSize     uint32
	ReservedIno   uint64
	ReservedLayer uint64
	ReservedCmt   uint64
	CreatedAt     int64
	UpdatedAt     int64
}

type BranchMeta struct {
	Name           string
	HeadCommitID   uint64
	WorkLayerID    uint64
	WriterEpoch    uint64
	WorkOwnedBytes uint64
	LogicalBytes   uint64
	CreatedAt      int64
	UpdatedAt      int64
}

type CommitMeta struct {
	ID            uint64
	ParentID      uint64
	MergeParentID uint64
	LayerID       uint64
	Kind          CommitKind
	OwnedBytes    uint64
	LogicalBytes  uint64
	Message       string
	CreatedAt     int64
}

type MountMeta struct {
	ID          string
	BranchName  string
	ReadOnly    bool
	WriterEpoch uint64
	MountedAt   int64
	ExpiresAt   int64
}

type EvictMountsResult struct {
	EvictedCount    uint32
	EvictedMountIDs []string
	UpdatedBranches []BranchMeta
}

type ControlSnapshot struct {
	Disk      DiskMeta
	UsedBytes uint64
	Branches  []BranchMeta
	Commits   []CommitMeta
	Mounts    []MountMeta
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

type Entry struct {
	Ino  uint64
	Mode uint32
}

type DirEntry struct {
	Name       string
	Ino        uint64
	Mode       uint32
	NextOffset uint64
}

type DirEntryPlus struct {
	Name       string
	Ino        uint64
	Mode       uint32
	NextOffset uint64
	Inode      Inode
}

type CommitNodeResult struct {
	Mode          uint32
	FileSize      uint64
	Data          []byte
	EOF           bool
	SymlinkTarget []byte
}

type BlobRef struct {
	PackID uint64
	Offset uint32
	Length uint32
}

type BlockRefLocation struct {
	LayerID    uint64
	BranchName string
	CommitID   uint64
	IsCommit   bool
	Ino        uint64
	BlockIdx   uint64
}

type GCStats struct {
	PacksScanned    int
	PacksDeleted    int
	BlocksRelocated int
	BlocksSkipped   int
	BytesReclaimed  uint64
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

func EncodeLiveEnvelope(payload []byte) []byte {
	out := make([]byte, 1+len(payload))
	out[0] = TagLive
	copy(out[1:], payload)
	return out
}

func EncodeTombstone() []byte {
	return []byte{TagTombstone}
}

func DecodeEnvelope(raw []byte) (tag byte, payload []byte, err error) {
	if len(raw) == 0 {
		return 0, nil, ErrInvalidValue
	}
	switch raw[0] {
	case TagTombstone:
		if len(raw) != 1 {
			return 0, nil, ErrInvalidValue
		}
		return TagTombstone, nil, nil
	case TagLive:
		if len(raw) == 1 {
			return TagLive, nil, nil
		}
		return TagLive, raw[1:], nil
	default:
		return 0, nil, ErrInvalidValue
	}
}

func EncodeDiskMeta(m DiskMeta) []byte {
	diskIDLen := len(m.DiskID)
	nameLen := len(m.Name)
	defBranchLen := len(m.DefaultBranch)
	out := make([]byte, 1+4+diskIDLen+4+nameLen+4+defBranchLen+52)
	off := 0
	out[off] = codecVersion
	off++
	binary.BigEndian.PutUint32(out[off:off+4], uint32(diskIDLen))
	off += 4
	copy(out[off:off+diskIDLen], m.DiskID)
	off += diskIDLen
	binary.BigEndian.PutUint32(out[off:off+4], uint32(nameLen))
	off += 4
	copy(out[off:off+nameLen], m.Name)
	off += nameLen
	binary.BigEndian.PutUint32(out[off:off+4], uint32(defBranchLen))
	off += 4
	copy(out[off:off+defBranchLen], m.DefaultBranch)
	off += defBranchLen
	binary.BigEndian.PutUint64(out[off:off+8], m.MaxBytes)
	off += 8
	binary.BigEndian.PutUint32(out[off:off+4], m.BlockSize)
	off += 4
	binary.BigEndian.PutUint64(out[off:off+8], m.ReservedIno)
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], m.ReservedLayer)
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], m.ReservedCmt)
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], uint64(m.CreatedAt))
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], uint64(m.UpdatedAt))
	return out
}

func DecodeDiskMeta(b []byte) (DiskMeta, error) {
	if len(b) < 1+4+4+4+52 || b[0] != codecVersion {
		return DiskMeta{}, ErrInvalidValue
	}
	off := 1
	diskIDLen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if diskIDLen < 0 || off+diskIDLen+4 > len(b) {
		return DiskMeta{}, ErrInvalidValue
	}
	diskID := string(b[off : off+diskIDLen])
	off += diskIDLen
	nameLen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if nameLen < 0 || off+nameLen+4 > len(b) {
		return DiskMeta{}, ErrInvalidValue
	}
	name := string(b[off : off+nameLen])
	off += nameLen
	defBranchLen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if defBranchLen < 0 || off+defBranchLen+52 != len(b) {
		return DiskMeta{}, ErrInvalidValue
	}
	defBranch := string(b[off : off+defBranchLen])
	off += defBranchLen
	m := DiskMeta{
		DiskID:        diskID,
		Name:          name,
		DefaultBranch: defBranch,
		MaxBytes:      binary.BigEndian.Uint64(b[off : off+8]),
		BlockSize:     binary.BigEndian.Uint32(b[off+8 : off+12]),
		ReservedIno:   binary.BigEndian.Uint64(b[off+12 : off+20]),
		ReservedLayer: binary.BigEndian.Uint64(b[off+20 : off+28]),
		ReservedCmt:   binary.BigEndian.Uint64(b[off+28 : off+36]),
		CreatedAt:     int64(binary.BigEndian.Uint64(b[off+36 : off+44])),
		UpdatedAt:     int64(binary.BigEndian.Uint64(b[off+44 : off+52])),
	}
	return m, nil
}

func EncodeBranchMeta(m BranchMeta) []byte {
	nameLen := len(m.Name)
	out := make([]byte, 1+4+nameLen+56)
	off := 0
	out[off] = codecVersion
	off++
	binary.BigEndian.PutUint32(out[off:off+4], uint32(nameLen))
	off += 4
	copy(out[off:off+nameLen], m.Name)
	off += nameLen
	binary.BigEndian.PutUint64(out[off:off+8], m.HeadCommitID)
	binary.BigEndian.PutUint64(out[off+8:off+16], m.WorkLayerID)
	binary.BigEndian.PutUint64(out[off+16:off+24], m.WriterEpoch)
	binary.BigEndian.PutUint64(out[off+24:off+32], m.WorkOwnedBytes)
	binary.BigEndian.PutUint64(out[off+32:off+40], m.LogicalBytes)
	binary.BigEndian.PutUint64(out[off+40:off+48], uint64(m.CreatedAt))
	binary.BigEndian.PutUint64(out[off+48:off+56], uint64(m.UpdatedAt))
	return out
}

func DecodeBranchMeta(b []byte) (BranchMeta, error) {
	if len(b) < 1+4+56 || b[0] != codecVersion {
		return BranchMeta{}, ErrInvalidValue
	}
	nameLen := int(binary.BigEndian.Uint32(b[1:5]))
	if 5+nameLen+56 != len(b) {
		return BranchMeta{}, ErrInvalidValue
	}
	off := 5 + nameLen
	return BranchMeta{
		Name:           string(b[5:off]),
		HeadCommitID:   binary.BigEndian.Uint64(b[off : off+8]),
		WorkLayerID:    binary.BigEndian.Uint64(b[off+8 : off+16]),
		WriterEpoch:    binary.BigEndian.Uint64(b[off+16 : off+24]),
		WorkOwnedBytes: binary.BigEndian.Uint64(b[off+24 : off+32]),
		LogicalBytes:   binary.BigEndian.Uint64(b[off+32 : off+40]),
		CreatedAt:      int64(binary.BigEndian.Uint64(b[off+40 : off+48])),
		UpdatedAt:      int64(binary.BigEndian.Uint64(b[off+48 : off+56])),
	}, nil
}

func EncodeCommitMeta(m CommitMeta) []byte {
	msgLen := len(m.Message)
	out := make([]byte, 1+8+8+8+8+1+8+8+8+4+msgLen)
	out[0] = codecVersion
	binary.BigEndian.PutUint64(out[1:9], m.ID)
	binary.BigEndian.PutUint64(out[9:17], m.ParentID)
	binary.BigEndian.PutUint64(out[17:25], m.MergeParentID)
	binary.BigEndian.PutUint64(out[25:33], m.LayerID)
	out[33] = byte(m.Kind)
	binary.BigEndian.PutUint64(out[34:42], m.OwnedBytes)
	binary.BigEndian.PutUint64(out[42:50], m.LogicalBytes)
	binary.BigEndian.PutUint64(out[50:58], uint64(m.CreatedAt))
	binary.BigEndian.PutUint32(out[58:62], uint32(msgLen))
	copy(out[62:], m.Message)
	return out
}

func DecodeCommitMeta(b []byte) (CommitMeta, error) {
	if len(b) < 62 || b[0] != codecVersion {
		return CommitMeta{}, ErrInvalidValue
	}
	msgLen := int(binary.BigEndian.Uint32(b[58:62]))
	if 62+msgLen != len(b) {
		return CommitMeta{}, ErrInvalidValue
	}
	return CommitMeta{
		ID:            binary.BigEndian.Uint64(b[1:9]),
		ParentID:      binary.BigEndian.Uint64(b[9:17]),
		MergeParentID: binary.BigEndian.Uint64(b[17:25]),
		LayerID:       binary.BigEndian.Uint64(b[25:33]),
		Kind:          CommitKind(b[33]),
		OwnedBytes:    binary.BigEndian.Uint64(b[34:42]),
		LogicalBytes:  binary.BigEndian.Uint64(b[42:50]),
		CreatedAt:     int64(binary.BigEndian.Uint64(b[50:58])),
		Message:       string(b[62:]),
	}, nil
}

func EncodeMountMeta(m MountMeta) []byte {
	idLen := len(m.ID)
	branchLen := len(m.BranchName)
	out := make([]byte, 1+4+idLen+4+branchLen+25)
	off := 0
	out[off] = codecVersion
	off++
	binary.BigEndian.PutUint32(out[off:off+4], uint32(idLen))
	off += 4
	copy(out[off:off+idLen], m.ID)
	off += idLen
	binary.BigEndian.PutUint32(out[off:off+4], uint32(branchLen))
	off += 4
	copy(out[off:off+branchLen], m.BranchName)
	off += branchLen
	if m.ReadOnly {
		out[off] = 1
	}
	off++
	binary.BigEndian.PutUint64(out[off:off+8], m.WriterEpoch)
	binary.BigEndian.PutUint64(out[off+8:off+16], uint64(m.MountedAt))
	binary.BigEndian.PutUint64(out[off+16:off+24], uint64(m.ExpiresAt))
	return out
}

func DecodeMountMeta(b []byte) (MountMeta, error) {
	if len(b) < 1+4+4+25 || b[0] != codecVersion {
		return MountMeta{}, ErrInvalidValue
	}
	off := 1
	idLen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if idLen < 0 || off+idLen+4 > len(b) {
		return MountMeta{}, ErrInvalidValue
	}
	id := string(b[off : off+idLen])
	off += idLen
	branchLen := int(binary.BigEndian.Uint32(b[off : off+4]))
	off += 4
	if branchLen < 0 || off+branchLen+25 != len(b) {
		return MountMeta{}, ErrInvalidValue
	}
	branchName := string(b[off : off+branchLen])
	off += branchLen
	if b[off] != 0 && b[off] != 1 {
		return MountMeta{}, ErrInvalidValue
	}
	readOnly := b[off] == 1
	off++
	return MountMeta{
		ID:          id,
		BranchName:  branchName,
		ReadOnly:    readOnly,
		WriterEpoch: binary.BigEndian.Uint64(b[off : off+8]),
		MountedAt:   int64(binary.BigEndian.Uint64(b[off+8 : off+16])),
		ExpiresAt:   int64(binary.BigEndian.Uint64(b[off+16 : off+24])),
	}, nil
}

func EncodeEntry(e Entry) []byte {
	out := make([]byte, EntryValueSize)
	out[0] = codecVersion
	binary.BigEndian.PutUint64(out[1:9], e.Ino)
	binary.BigEndian.PutUint32(out[9:13], e.Mode)
	return out
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

func EncodeInode(i Inode) ([]byte, error) {
	if uint64(len(i.SymlinkTarget)) > math.MaxUint32 {
		return nil, ErrInvalidValue
	}
	out := make([]byte, InodeHeaderSize+len(i.SymlinkTarget))
	out[0] = codecVersion
	binary.BigEndian.PutUint64(out[1:9], i.Ino)
	binary.BigEndian.PutUint64(out[9:17], i.ParentIno)
	binary.BigEndian.PutUint32(out[17:21], i.Mode)
	binary.BigEndian.PutUint32(out[21:25], i.Nlink)
	binary.BigEndian.PutUint32(out[25:29], i.UID)
	binary.BigEndian.PutUint32(out[29:33], i.GID)
	binary.BigEndian.PutUint32(out[33:37], i.Rdev)
	binary.BigEndian.PutUint64(out[37:45], i.Size)
	binary.BigEndian.PutUint64(out[45:53], uint64(i.Atime))
	binary.BigEndian.PutUint64(out[53:61], uint64(i.Mtime))
	binary.BigEndian.PutUint64(out[61:69], uint64(i.Ctime))
	binary.BigEndian.PutUint32(out[69:73], i.Flags)
	binary.BigEndian.PutUint64(out[73:81], i.Generation)
	binary.BigEndian.PutUint64(out[81:89], i.WriteSeq)
	binary.BigEndian.PutUint32(out[89:93], uint32(len(i.SymlinkTarget)))
	copy(out[93:], i.SymlinkTarget)
	return out, nil
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

func EncodeBlobRef(ref BlobRef) []byte {
	out := make([]byte, BlobRefSize)
	binary.BigEndian.PutUint64(out[0:8], ref.PackID)
	binary.BigEndian.PutUint32(out[8:12], ref.Offset)
	binary.BigEndian.PutUint32(out[12:16], ref.Length)
	return out
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
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > MaxBlockSize || uint64(ref.Offset)+uint64(ref.Length) > uint64(DefaultMaxPackSize) {
		return BlobRef{}, ErrInvalidValue
	}
	return ref, nil
}
