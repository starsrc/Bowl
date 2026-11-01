package meta

import (
	"errors"
	"fmt"
	"math"
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
	S_ISVTX               uint32 = 0o1000
	PermMask              uint32 = 0o7777
	MaxFileNameBytes             = 255
	MaxSymlinkTargetBytes        = 4095
)

// 根节点编号。
const (
	RootIno uint64 = 1
)

// 节点链接与大小上限。
const (
	MaxNlink    uint32 = 65000
	MaxFileSize uint64 = math.MaxInt64
)

// 默认块大小与目录默认物理大小。
const (
	DefaultBlockSize uint32 = 65536
	DefaultDirSize   uint64 = 4096
)

// 数据层核心常量（ADR-0019 / ADR-0022）。
const (
	ChunkSize                     = 1 << 26 // 64 MiB (67,108,864 字节)
	BlockSize                     = 1 << 22 // 4 MiB (4,194,304 字节)
	SliceBytes                    = 24      // 单个 Slice 二进制元数据描述符字节大小
	CompactSliceThreshold         = 64      // 单个 Chunk 累积 Slice 数量触发 Compaction 的阈值
	DefaultSliceIDBatchSize       = 64      // 服务端内存预取 slice_id 号段的默认步长
	DefaultWriteGroupMaxBatchSize = 64      // 服务端 Write 阶段 2 Group Commit 单批次最大聚合请求数
)

// 磁盘与 StatFS 常量。
const (
	// MaxDiskNameBytes 定义磁盘名称最大字节长度（255 字节）。
	MaxDiskNameBytes = 255
	// DefaultStatFSBlockSize 定义 StatFS 默认换算块大小（4096 字节）。
	DefaultStatFSBlockSize = 4096
	// DefaultListDisksPageSize 定义 ListDisks 默认单页最大返回条数（50）。
	DefaultListDisksPageSize uint32 = 50
	// MaxListDisksPageSize 定义 ListDisks 单页最大返回条数上限（1000）。
	MaxListDisksPageSize uint32 = 1000
)

// Inode 文件系统标志位常量（对齐 Linux chattr / FS_*_FL）。
const (
	FlagImmutable uint32 = 0x1 // FS_IMMUTABLE_FL
	FlagAppend    uint32 = 0x2 // FS_APPEND_FL
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
	O_TMPFILE = 0o20000000
)

// FUSE Open 响应标志常量（平台无关固定值）。
const (
	FOPEN_DIRECT_IO   uint32 = 1 << 0
	FOPEN_KEEP_CACHE  uint32 = 1 << 1
	FOPEN_NONSEEKABLE uint32 = 1 << 2
	FOPEN_CACHE_DIR   uint32 = 1 << 3
)

// Linux FUSE / POSIX fallocate 模式标志位常量（平台无关固定值）。
const (
	FallocFlKeepSize      uint32 = 0x01
	FallocFlPunchHole     uint32 = 0x02
	FallocFlCollapseRange uint32 = 0x08
	FallocFlZeroRange     uint32 = 0x10
	FallocFlInsertRange   uint32 = 0x20
)

// Linux FUSE_RENAME2 (45) 重命名标志位常量（平台无关固定值，严禁依赖宿主机 syscall）。
const (
	RenameNoReplace uint32 = 0x1
	RenameExchange  uint32 = 0x2
	RenameWhiteout  uint32 = 0x4
)

// AtimeMode 定义最后访问时间更新策略枚举。
type AtimeMode int

const (
	AtimeModeNoAtime     AtimeMode = 0 // 默认：不更新 atime
	AtimeModeRelAtime    AtimeMode = 1 // relatime：仅当 atime <= mtime 或 atime 超过 24h 时更新
	AtimeModeStrictAtime AtimeMode = 2 // strictatime：每次读取均更新 atime
)

// Linux 扩展属性（xattr）规格常量与操作标志位。
const (
	MaxXattrNameBytes  = 255
	MaxXattrValueBytes = 65536
	MaxXattrListBytes  = 65536

	XattrCreate  uint32 = 0x1
	XattrReplace uint32 = 0x2
)

// Linux FUSE 文件锁类型、标志位、最大偏移量与会话租约常量（平台无关固定值，严禁依赖宿主机 syscall.F_*）。
const (
	F_RDLCK uint32 = 0
	F_WRLCK uint32 = 1
	F_UNLCK uint32 = 2

	FuseLkFlock            uint32 = 0x1
	FuseReleaseFlush       uint32 = 0x1
	FuseReleaseFlockUnlock uint32 = 0x2

	OffsetMax uint64 = math.MaxInt64

	DefaultSessionLeaseTTL           = 60 * time.Second
	DefaultSessionProbeTimeout       = 1500 * time.Millisecond
	DefaultWriteSessionTouchInterval = 5 * time.Second

	WaiterStatusPending   int16 = 1
	WaiterStatusGranted   int16 = 2
	WaiterStatusCancelled int16 = 3
)

// SetAttrInput 定义修改 Inode 属性的输入参数。
type SetAttrInput struct {
	Mode      *uint32
	UID       *uint32
	GID       *uint32
	Size      *uint64
	Atime     *time.Time
	Mtime     *time.Time
	AtimeNow  bool
	MtimeNow  bool
	Fh        *uint64
	SessionID *uint64
	CtimeNow  bool
	Flags     *uint32
	CallerUID uint32
	CallerGID uint32
}

// MknodParams 定义创建节点的输入参数。
type MknodParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Umask     uint32
}

// MkdirParams 定义创建子目录的输入参数。
type MkdirParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Umask     uint32
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

// RenameParams 定义重命名、移动与交换操作的输入参数。
type RenameParams struct {
	OldParentIno uint64
	OldName      []byte
	NewParentIno uint64
	NewName      []byte
	Flags        uint32
}

// CreateParams 定义原子创建常规文件并分配初始文件句柄的输入参数。
// 严禁在 Flags 中携带 O_TMPFILE 标志位（匿名临时文件必须通过 TmpFile 接口创建）。
type CreateParams struct {
	ParentIno uint64
	Name      []byte
	Mode      uint32
	UID       uint32
	GID       uint32
	Flags     uint32
	SessionID *uint64
	Umask     uint32
}

// CreateResult 包含新建文件的 Inode 元数据、初始分配的 FileHandle 与打开响应标志。
type CreateResult struct {
	Inode               *Inode
	Fh                  uint64
	OpenFlags           uint32
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// TmpFileParams 定义原子创建匿名临时常规文件并分配初始文件句柄的输入参数。
type TmpFileParams struct {
	ParentIno uint64
	Mode      uint32
	UID       uint32
	GID       uint32
	Flags     uint32
	SessionID *uint64
	Umask     uint32
}

// OpenParams 定义打开文件并分配文件句柄的输入参数。
// 严禁在 Flags 中携带 O_TMPFILE 标志位（严防伪造 O_TMPFILE 绕过独占限制或非法复活孤儿节点）。
type OpenParams struct {
	Ino       uint64
	Flags     uint32
	SessionID *uint64
}

// OpenResult 包含打开文件分配的 FileHandle 与打开响应标志。
type OpenResult struct {
	Fh        uint64
	OpenFlags uint32
}

// FlushParams 定义会话句柄冲刷与进程级 POSIX 记录锁清理操作的输入参数。
type FlushParams struct {
	Ino       uint64
	Fh        uint64
	LockOwner *uint64
	SessionID *uint64
}

// FsyncParams 定义文件同步操作的输入参数。
type FsyncParams struct {
	Ino       uint64
	Fh        uint64
	Datasync  bool
	SessionID *uint64
}

// ReleaseParams 定义文件句柄销毁、BSD flock 句柄锁清理与孤儿回收操作的输入参数。
type ReleaseParams struct {
	Ino          uint64
	Fh           uint64
	LockOwner    *uint64
	ReleaseFlags uint32
	SessionID    *uint64
}

// OpenDirParams 定义打开目录并分配目录句柄的输入参数。
type OpenDirParams struct {
	Ino       uint64
	Flags     uint32
	SessionID *uint64
}

// OpenDirResult 包含打开目录分配的 FileHandle 与打开响应标志。
type OpenDirResult struct {
	Fh        uint64
	OpenFlags uint32
}

// FsyncDirParams 定义目录句柄同步操作的输入参数。
type FsyncDirParams struct {
	Ino       uint64
	Fh        uint64
	Datasync  bool
	SessionID *uint64
}

// ReleaseDirParams 定义目录句柄销毁与孤儿回收操作的输入参数。
type ReleaseDirParams struct {
	Ino       uint64
	Fh        uint64
	SessionID *uint64
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
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Limit     uint32
	SessionID *uint64
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
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Limit     uint32
	SessionID *uint64
}

// FallocateParams 定义文件空间预分配与打洞操作的输入参数。
type FallocateParams struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Length    uint64
	Mode      uint32
	SessionID *uint64
}

// SetXattrParams 定义设置扩展属性的输入参数。
type SetXattrParams struct {
	Ino   uint64
	Name  []byte
	Value []byte
	Flags uint32
}

// FileLock 表示 FUSE 文件锁领域实体。
type FileLock struct {
	Start uint64
	End   uint64
	Type  uint32
	PID   uint32
}

// GetLkParams 定义 FUSE_GETLK (31) 冲突锁探测操作的输入参数。
type GetLkParams struct {
	Ino       uint64
	Fh        uint64
	LockOwner uint64
	Lk        FileLock
	LkFlags   uint32
	SessionID uint64
}

// SetLkParams 定义 FUSE_SETLK (32) 非阻塞加解锁操作的输入参数。
type SetLkParams struct {
	Ino       uint64
	Fh        uint64
	LockOwner uint64
	Lk        FileLock
	LkFlags   uint32
	SessionID uint64
}

// SetLkwParams 定义 FUSE_SETLKW (33) 阻塞等待加解锁操作的输入参数。
type SetLkwParams struct {
	Ino       uint64
	Fh        uint64
	LockOwner uint64
	Lk        FileLock
	LkFlags   uint32
	SessionID uint64
	RequestID uint64
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
	ErrAgain           = errors.New("meta: resource temporarily unavailable")
	ErrDeadlock        = errors.New("meta: resource deadlock avoided")
	ErrInterrupted     = errors.New("meta: interrupted system call")
	ErrTooManyLinks    = errors.New("meta: too many links")
	ErrFileTooLarge    = errors.New("meta: file too large")
	ErrIllegalSeek     = errors.New("meta: illegal seek")
	ErrIO              = errors.New("meta: input/output error")
	ErrAccess          = errors.New("meta: permission denied")
	ErrInvalidShrink   = fmt.Errorf("%w: cannot shrink disk capacity below current usage", ErrInvalidArgument)
	ErrCrossDevice     = errors.New("meta: invalid cross-device link")
	ErrBusy            = errors.New("meta: device or resource busy")
)

// Inode 表示文件系统节点的领域实体。
type Inode struct {
	Ino                 uint64
	Generation          uint64
	Attr                Attribute
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// Attribute 表示节点的 POSIX 物理属性。
type Attribute struct {
	Mode      uint32
	Size      uint64
	Nlink     uint32
	UID       uint32
	GID       uint32
	Rdev      uint32
	Blksize   uint32
	Blocks    uint64
	Atime     time.Time
	Mtime     time.Time
	Ctime     time.Time
	Flags     uint32
	ParentIno uint64
}

// GetXattrParams 定义获取扩展属性的输入参数。
type GetXattrParams struct {
	Ino  uint64
	Name []byte
	Size uint32
}

// ListXattrParams 定义列出扩展属性名称的输入参数。
type ListXattrParams struct {
	Ino  uint64
	Size uint32
}

// RenewSessionResult 包含续期后刷新得到的会话租约截止时间与是否为新创建或过期重建标识。
type RenewSessionResult struct {
	ExpiresAt        time.Time
	SessionRecreated bool
}

// Disk 描述虚拟磁盘领域实体。
type Disk struct {
	DiskID     string
	Name       string
	RootIno    uint64
	MaxBytes   uint64
	UsedBytes  uint64
	UsedInodes uint64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// CreateDiskParams 描述创建虚拟磁盘入参。
type CreateDiskParams struct {
	Name     string
	MaxBytes uint64
}

// ListDisksParams 描述分页查询虚拟磁盘列表入参。
type ListDisksParams struct {
	PageSize  uint32
	PageToken string
}

// ListDisksResult 描述分页查询虚拟磁盘列表返回结果。
type ListDisksResult struct {
	Disks         []*Disk
	NextPageToken string
}

// UpdateDiskParams 描述更新虚拟磁盘入参。
type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

// StatFSResult 描述磁盘文件系统统计状态（对接 Linux FUSE FUSE_STATFS = 17 协议）。
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

// ReadRequest 定义文件数据流读取请求参数。
type ReadRequest struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Size      uint32
	SessionID *uint64
}

// ReadResponse 包含文件数据流读取返回的数据载荷。
type ReadResponse struct {
	Data []byte
}

// WriteRequest 定义文件数据流写入请求参数。
type WriteRequest struct {
	Ino       uint64
	Fh        uint64
	Offset    uint64
	Data      []byte
	SessionID *uint64
}

// WriteResponse 包含文件数据流写入返回的实际写入大小与更新后的 Inode 元数据。
type WriteResponse struct {
	Size  uint32
	Inode *Inode
}
