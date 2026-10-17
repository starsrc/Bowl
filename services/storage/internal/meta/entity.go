package meta

import (
	"errors"
	"time"
)

// POSIX 文件类型掩码与模式常量。
const (
	S_IFMT                uint32 = 0o170000
	S_IFIFO               uint32 = 0o010000
	S_IFCHR               uint32 = 0o020000
	S_IFDIR               uint32 = 0o040000
	S_IFBLK               uint32 = 0o060000
	S_IFREG               uint32 = 0o100000
	S_IFLNK               uint32 = 0o120000
	S_IFSOCK              uint32 = 0o140000
	S_ISUID               uint32 = 0o4000
	S_ISGID               uint32 = 0o2000
	PermMask              uint32 = 0o7777
	MaxFileNameBytes             = 255
	MaxSymlinkTargetBytes        = 4095
)

// Linux FUSE / POSIX 打开标志位常量（平台无关固定值，严禁依赖宿主机 syscall.O_*）。
const (
	O_ACCMODE = 0o3
	O_RDONLY  = 0o0
	O_WRONLY  = 0o1
	O_RDWR    = 0o2
	O_CREAT   = 0o100
	O_EXCL    = 0o200
	O_TRUNC   = 0o1000
	O_APPEND  = 0o2000
)

// Linux FUSE / POSIX fallocate 模式标志位常量（平台无关固定值）。
const (
	FallocFlKeepSize  uint32 = 0x01
	FallocFlPunchHole uint32 = 0x02
)

// Linux 扩展属性（xattr）规格常量与操作标志位。
const (
	MaxXattrNameBytes  = 255
	MaxXattrValueBytes = 65536
	MaxXattrListBytes  = 65536

	XattrCreate  uint32 = 0x1
	XattrReplace uint32 = 0x2
)

// SetAttrInput 定义修改 Inode 属性的输入参数。
type SetAttrInput struct {
	Mode     *uint32
	UID      *uint32
	GID      *uint32
	Size     *uint64
	Atime    *time.Time
	Mtime    *time.Time
	AtimeNow bool
	MtimeNow bool
}

// MknodParams 定义创建节点的输入参数。
type MknodParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
}

// MkdirParams 定义创建子目录的输入参数。
type MkdirParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
}

// SymlinkParams 定义创建符号链接的输入参数。
type SymlinkParams struct {
	ParentIno uint64
	Name      []byte
	Target    []byte
	UID       uint32
	GID       uint32
}

// LinkParams 定义创建硬链接的输入参数。
type LinkParams struct {
	Ino          uint64
	NewParentIno uint64
	NewName      []byte
}

// RenameParams 定义重命名与移动操作的输入参数。
type RenameParams struct {
	OldParentIno uint64
	OldName      []byte
	NewParentIno uint64
	NewName      []byte
}

// CreateParams 定义原子创建常规文件并分配初始文件句柄的输入参数。
type CreateParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Flags     uint32
}

// CreateResult 包含新建文件的 Inode 元数据、初始分配的 FileHandle 与打开响应标志。
type CreateResult struct {
	Inode     *Inode
	Fh        uint64
	OpenFlags uint32
}

// OpenParams 定义打开文件并分配文件句柄的输入参数。
type OpenParams struct {
	Ino   uint64
	Flags uint32
}

// OpenResult 包含打开文件分配的 FileHandle 与打开响应标志。
type OpenResult struct {
	Fh        uint64
	OpenFlags uint32
}

// FlushParams 定义会话句柄冲刷操作的输入参数。
type FlushParams struct {
	Ino       uint64
	Fh        uint64
	LockOwner *uint64
}

// FsyncParams 定义文件同步操作的输入参数。
type FsyncParams struct {
	Ino      uint64
	Fh       uint64
	Datasync bool
}

// ReleaseParams 定义文件句柄销毁与孤儿回收操作的输入参数。
type ReleaseParams struct {
	Ino uint64
	Fh  uint64
}

// OpenDirParams 定义打开目录并分配目录句柄的输入参数。
type OpenDirParams struct {
	Ino   uint64
	Flags uint32
}

// OpenDirResult 包含打开目录分配的 DirHandle 与打开响应标志。
type OpenDirResult struct {
	Fh        uint64
	OpenFlags uint32
}

// ReleaseDirParams 定义目录句柄销毁与孤儿回收操作的输入参数。
type ReleaseDirParams struct {
	Ino uint64
	Fh  uint64
}

// 目录遍历最大单次条目配额限制。
const MaxReadDirLimit uint32 = 4096

// 默认内核条目与属性缓存时效（秒）。
const (
	DefaultEntryTimeoutSeconds float64 = 1.0
	DefaultAttrTimeoutSeconds  float64 = 1.0
)

// DirEntry 定义 ReadDir 返回的基本目录项实体。
type DirEntry struct {
	Ino        uint64
	Name       []byte
	Mode       uint32
	NextOffset uint64
}

// ReadDirParams 定义目录项流式遍历输入参数。
type ReadDirParams struct {
	Ino    uint64
	Fh     uint64
	Offset uint64
	Limit  uint32
}

// DirEntryPlus 定义 ReadDirPlus 返回的扩展目录项实体（包含完整 Inode 属性与内核缓存时效）。
type DirEntryPlus struct {
	Ino                 uint64
	Name                []byte
	NextOffset          uint64
	Inode               *Inode
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// ReadDirPlusParams 定义扩展目录项流式遍历输入参数。
type ReadDirPlusParams struct {
	Ino    uint64
	Fh     uint64
	Offset uint64
	Limit  uint32
}

// FallocateParams 定义文件空间预分配与打洞操作的输入参数。
type FallocateParams struct {
	Ino    uint64
	Fh     uint64
	Offset uint64
	Length uint64
	Mode   uint32
}

// SetXattrParams 定义设置扩展属性的输入参数。
type SetXattrParams struct {
	Ino   uint64
	Name  []byte
	Value []byte
	Flags uint32
}

// 领域哨兵错误。
var (
	ErrInvalidArgument = errors.New("meta: invalid argument")
	ErrNotFound        = errors.New("meta: not found")
	ErrNotDir          = errors.New("meta: not a directory")
	ErrIsDir           = errors.New("meta: is a directory")
	ErrPerm            = errors.New("meta: operation not permitted")
	ErrAlreadyExists   = errors.New("meta: entry already exists")
	ErrNotEmpty        = errors.New("meta: directory not empty")
	ErrBadFileHandle   = errors.New("meta: bad file handle")
	ErrNotSupported    = errors.New("meta: operation not supported")
	ErrNameTooLong     = errors.New("meta: file name too long")
	ErrRange           = errors.New("meta: numerical result out of range")
	ErrTooBig          = errors.New("meta: argument list too long")
	ErrNoData          = errors.New("meta: no data available")
	ErrNoSpace         = errors.New("meta: no space left on device")
)

// Inode 表示文件系统节点的领域实体。
type Inode struct {
	Ino        uint64
	Generation uint64
	Attr       Attribute
}

// Attribute 表示节点的 POSIX 物理属性。
type Attribute struct {
	Mode    uint32
	Size    uint64
	Nlink   uint32
	UID     uint32
	GID     uint32
	Rdev    uint32
	Blksize uint32
	Blocks  uint64
	Atime   time.Time
	Mtime   time.Time
	Ctime   time.Time
}
