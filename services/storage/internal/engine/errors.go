package engine

import (
	"errors"
	"math"
)

var (
	ErrClosed               = errors.New("engine: disk closed")
	ErrNotFound             = errors.New("engine: not found")
	ErrExist                = errors.New("engine: already exists")
	ErrNotDir               = errors.New("engine: not a directory")
	ErrIsDir                = errors.New("engine: is a directory")
	ErrNotEmpty             = errors.New("engine: directory not empty")
	ErrInvalid              = errors.New("engine: invalid argument")
	ErrInvalidKey           = errors.New("engine: invalid key")
	ErrInvalidValue         = errors.New("engine: invalid value")
	ErrPerm                 = errors.New("engine: operation not permitted")
	ErrNotSupported         = errors.New("engine: operation not supported")
	ErrNoData               = errors.New("engine: no data available")
	ErrNoSpace              = errors.New("engine: no space left on device")
	ErrStaleEpoch           = errors.New("engine: stale writer epoch")
	ErrNameTooLong          = errors.New("engine: name too long")
	ErrCommitInUse          = errors.New("engine: commit reachable from live branch")
	ErrDefaultBranch        = errors.New("engine: cannot delete default branch")
	ErrTooManyLinks         = errors.New("engine: too many links")
	ErrFileTooLarge         = errors.New("engine: file too large")
	ErrInodeExhausted       = errors.New("engine: inode numbers exhausted")
	ErrDirtyWorkLayer       = errors.New("engine: dirty work layer")
	ErrAmbiguousLCA         = errors.New("engine: ambiguous common ancestor")
	ErrDiskMounted          = errors.New("engine: disk or branch has active mount")
	ErrQuotaShrinkBelowUsed = errors.New("engine: quota cannot shrink below used bytes")
)

const (
	ModeTypeMask uint32 = 0o170000
	ModeFIFO     uint32 = 0o010000
	ModeChar     uint32 = 0o020000
	ModeDir      uint32 = 0o040000
	ModeBlock    uint32 = 0o060000
	ModeRegular  uint32 = 0o100000
	ModeSymlink  uint32 = 0o120000
	ModeSocket   uint32 = 0o140000
	ModeSetUID   uint32 = 0o4000
	ModeSetGID   uint32 = 0o2000
	ModeSticky   uint32 = 0o1000
	ModePermMask uint32 = 0o7777
)

const (
	RootIno                uint64 = 1
	DefaultBranchName             = "main"
	DefaultBlockSize       uint32 = 256 << 10
	MinBlockSize           uint32 = 4 << 10
	MaxBlockSize           uint32 = 256 << 10
	DefaultMaxPackSize     uint32 = 64 << 20
	DefaultDirSize         uint64 = 4096
	DefaultStatFSBlockSize uint32 = 4096
	MaxFileNameBytes              = 255
	MaxSymlinkTargetBytes         = 4095
	MaxNlink               uint32 = 65000
	MaxFileSize            uint64 = math.MaxInt64
	MaxXattrNameBytes             = 255
	MaxXattrValueBytes            = 65536
	MaxXattrListBytes             = 65536
	MaxReadDirLimit        uint32 = 4096
	IDSegmentSize          uint64 = 4096
)

const (
	FlagImmutable       uint32 = 0x1
	FlagAppend          uint32 = 0x2
	InodeFlagEverLinked uint32 = 1 << 31
)

const (
	OrphanUnlinked        byte = 0x01
	OrphanTmpfileExcl     byte = 0x02
	OrphanTmpfileLinkable byte = 0x03
)

const (
	RenameNoReplace uint32 = 0x1
	RenameExchange  uint32 = 0x2
	RenameWhiteout  uint32 = 0x4
)

const (
	FallocKeepSize  uint32 = 0x01
	FallocPunchHole uint32 = 0x02
	FallocZeroRange uint32 = 0x10
)

const (
	XattrCreate  uint32 = 0x1
	XattrReplace uint32 = 0x2
)

const (
	TagTombstone byte = 0x00
	TagLive      byte = 0x01
)

type CommitKind uint8

const (
	CommitKindInit   CommitKind = 1
	CommitKindManual CommitKind = 2
	CommitKindFork   CommitKind = 3
	CommitKindMerge  CommitKind = 4
)
