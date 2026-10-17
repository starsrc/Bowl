package meta

import (
	"errors"
	"time"
)

// POSIX 文件类型掩码与模式常量。
const (
	S_IFMT           uint32 = 0o170000
	S_IFIFO          uint32 = 0o010000
	S_IFCHR          uint32 = 0o020000
	S_IFDIR          uint32 = 0o040000
	S_IFBLK          uint32 = 0o060000
	S_IFREG          uint32 = 0o100000
	S_IFLNK          uint32 = 0o120000
	S_IFSOCK         uint32 = 0o140000
	S_ISUID          uint32 = 0o4000
	S_ISGID          uint32 = 0o2000
	PermMask              uint32 = 0o7777
	MaxFileNameBytes             = 255
	MaxSymlinkTargetBytes        = 4096
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
	Name      string
	Mode      uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
}

// MkdirParams 定义创建子目录的输入参数。
type MkdirParams struct {
	ParentIno uint64
	Name      string
	Mode      uint32
	UID       uint32
	GID       uint32
}

// SymlinkParams 定义创建符号链接的输入参数。
type SymlinkParams struct {
	ParentIno uint64
	Name      string
	Target    string
	UID       uint32
	GID       uint32
}

// LinkParams 定义创建硬链接的输入参数。
type LinkParams struct {
	Ino          uint64
	NewParentIno uint64
	NewName      string
}

// RenameParams 定义重命名与移动操作的输入参数。
type RenameParams struct {
	OldParentIno uint64
	OldName      string
	NewParentIno uint64
	NewName      string
}

// CreateParams 定义原子创建常规文件并分配初始文件句柄的输入参数。
type CreateParams struct {
	ParentIno uint64
	Name      string
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
