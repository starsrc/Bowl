// Package store 实现基于 Pebble 与不可变块文件的 StoreActor 存储引擎。
package store

import (
	"errors"
	"math"
)

// 领域哨兵错误定义。
var (
	ErrClosed         = errors.New("store: instance closed")
	ErrReadOnly       = errors.New("store: read-only instance")
	ErrNotFound       = errors.New("store: not found")
	ErrExist          = errors.New("store: entry already exists")
	ErrNotDir         = errors.New("store: not a directory")
	ErrIsDir          = errors.New("store: is a directory")
	ErrNotEmpty       = errors.New("store: directory not empty")
	ErrInvalid        = errors.New("store: invalid argument")
	ErrInvalidKey     = errors.New("store: invalid key")
	ErrInvalidValue   = errors.New("store: invalid value")
	ErrPerm           = errors.New("store: operation not permitted")
	ErrNoData         = errors.New("store: no data available")
	ErrNoSpace        = errors.New("store: no space left on device")
	ErrInodeExhausted = errors.New("store: inode numbers exhausted")
	ErrNameTooLong    = errors.New("store: file name too long")
	ErrTooManyLinks   = errors.New("store: too many links")
	ErrFileTooLarge   = errors.New("store: file too large")
	ErrNotSupported   = errors.New("store: operation not supported")
	ErrRange          = errors.New("store: numerical result out of range")
	ErrIO             = errors.New("store: input/output error")
	ErrStaleEpoch     = errors.New("store: stale writer epoch")
	// ErrWriterLeaseActive 表示磁盘当前持有未过期的活跃写租约，非 force 获取应拒绝。
	ErrWriterLeaseActive    = errors.New("store: writer lease active")
	ErrQuotaShrinkBelowUsed = errors.New("store: max_bytes cannot be less than logical_bytes")
)

// POSIX 文件类型掩码与权限模式常量。
//
//revive:disable:var-naming
const (
	S_IFMT   uint32 = 0o170000
	S_IFIFO  uint32 = 0o010000
	S_IFCHR  uint32 = 0o020000
	S_IFDIR  uint32 = 0o040000
	S_IFBLK  uint32 = 0o060000
	S_IFREG  uint32 = 0o100000
	S_IFLNK  uint32 = 0o120000
	S_IFSOCK uint32 = 0o140000
	S_ISUID  uint32 = 0o4000
	S_ISGID  uint32 = 0o2000
	S_ISVTX  uint32 = 0o1000
	PermMask uint32 = 0o7777
)

//revive:enable:var-naming

// 核心节点与文件系统规格常量。
const (
	RootIno                uint64 = 1
	InodeSegment           uint64 = 4096
	BlobSize                      = 256 << 10 // 256 KiB 固定逻辑分块网格
	PackMaxSize                   = 64 << 20  // 64 MiB 单分支 .pack 封包容量上限
	MaxWriteBytes                 = 1 << 20   // 1 MiB 单次最大写入长度
	DefaultBlockSize       uint32 = 65536
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
)

// Inode 文件系统标志位常量（FS_*_FL）。
const (
	FlagImmutable uint32 = 0x1
	FlagAppend    uint32 = 0x2
)

// POSIX 打开标志位常量（平台无关固定值）。
//
//revive:disable:var-naming
const (
	O_CREAT   = 0o100
	O_EXCL    = 0o200
	O_TMPFILE = 0o20000000
)

//revive:enable:var-naming

// Xattr 操作标志位常量。
const (
	XattrCreate  uint32 = 0x1
	XattrReplace uint32 = 0x2
)

// Rename2 操作标志位常量。
const (
	RenameNoreplace uint32 = 0x1
	RenameExchange  uint32 = 0x2
	RenameWhiteout  uint32 = 0x4
)

// Fallocate 操作标志位常量。
const (
	FallocKeepSize  uint32 = 0x01
	FallocPunchHole uint32 = 0x02
	FallocZeroRange uint32 = 0x10
)

// AtimeMode 定义访问时间（atime）更新策略。
type AtimeMode int

// 访问时间（atime）更新策略枚举值。
const (
	AtimeModeNoAtime     AtimeMode = 0
	AtimeModeRelAtime    AtimeMode = 1
	AtimeModeStrictAtime AtimeMode = 2
)
