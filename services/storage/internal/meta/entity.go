package meta

import (
	"errors"
	"time"
)

// POSIX 文件类型掩码与目录常量。
const (
	S_IFMT  uint32 = 0o170000
	S_IFDIR uint32 = 0o040000
)

// 领域哨兵错误。
var (
	ErrInvalidArgument = errors.New("meta: invalid argument")
	ErrNotFound        = errors.New("meta: not found")
	ErrNotDir          = errors.New("meta: not a directory")
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
