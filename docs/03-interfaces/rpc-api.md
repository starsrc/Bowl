<!-- 本文档由 `task docs:gen`（`docs/scripts/gen-rpc-doc.mjs`）从 `proto/storage/v1/storage.proto` 自动生成，请勿手动修改。 -->

# RPC 接口契约

> 本文档基于 `storage/v1/storage.proto`（包名 `storage.v1`）通过 `buf build --exclude-imports -o -#format=json` 自动提取生成，修改契约后请执行 `task docs:gen` 同步更新。

## 服务定义：StorageService

StorageService 提供针对分布式文件系统的元数据操作与 FUSE 协议对接能力。

共包含 **37** 个 RPC 方法：

| # | RPC 方法 | 请求类型 | 响应类型 | 调用模式 |
| --- | --- | --- | --- | --- |
| 1 | [Lookup](#lookup) | [LookupRequest](#lookuprequest) | [LookupResponse](#lookupresponse) | 一元调用 (Unary) |
| 2 | [GetAttr](#getattr) | [GetAttrRequest](#getattrrequest) | [GetAttrResponse](#getattrresponse) | 一元调用 (Unary) |
| 3 | [SetAttr](#setattr) | [SetAttrRequest](#setattrrequest) | [SetAttrResponse](#setattrresponse) | 一元调用 (Unary) |
| 4 | [Mknod](#mknod) | [MknodRequest](#mknodrequest) | [MknodResponse](#mknodresponse) | 一元调用 (Unary) |
| 5 | [Mkdir](#mkdir) | [MkdirRequest](#mkdirrequest) | [MkdirResponse](#mkdirresponse) | 一元调用 (Unary) |
| 6 | [Symlink](#symlink) | [SymlinkRequest](#symlinkrequest) | [SymlinkResponse](#symlinkresponse) | 一元调用 (Unary) |
| 7 | [Readlink](#readlink) | [ReadlinkRequest](#readlinkrequest) | [ReadlinkResponse](#readlinkresponse) | 一元调用 (Unary) |
| 8 | [Link](#link) | [LinkRequest](#linkrequest) | [LinkResponse](#linkresponse) | 一元调用 (Unary) |
| 9 | [Unlink](#unlink) | [UnlinkRequest](#unlinkrequest) | [UnlinkResponse](#unlinkresponse) | 一元调用 (Unary) |
| 10 | [Rmdir](#rmdir) | [RmdirRequest](#rmdirrequest) | [RmdirResponse](#rmdirresponse) | 一元调用 (Unary) |
| 11 | [Rename](#rename) | [RenameRequest](#renamerequest) | [RenameResponse](#renameresponse) | 一元调用 (Unary) |
| 12 | [Create](#create) | [CreateRequest](#createrequest) | [CreateResponse](#createresponse) | 一元调用 (Unary) |
| 13 | [Read](#read) | [ReadRequest](#readrequest) | [ReadResponse](#readresponse) | 一元调用 (Unary) |
| 14 | [Write](#write) | [WriteRequest](#writerequest) | [WriteResponse](#writeresponse) | 一元调用 (Unary) |
| 15 | [Fsync](#fsync) | [FsyncRequest](#fsyncrequest) | [FsyncResponse](#fsyncresponse) | 一元调用 (Unary) |
| 16 | [FsyncDir](#fsyncdir) | [FsyncDirRequest](#fsyncdirrequest) | [FsyncDirResponse](#fsyncdirresponse) | 一元调用 (Unary) |
| 17 | [ReadDir](#readdir) | [ReadDirRequest](#readdirrequest) | [ReadDirResponse](#readdirresponse) | 一元调用 (Unary) |
| 18 | [ReadDirPlus](#readdirplus) | [ReadDirPlusRequest](#readdirplusrequest) | [ReadDirPlusResponse](#readdirplusresponse) | 一元调用 (Unary) |
| 19 | [Fallocate](#fallocate) | [FallocateRequest](#fallocaterequest) | [FallocateResponse](#fallocateresponse) | 一元调用 (Unary) |
| 20 | [SetXattr](#setxattr) | [SetXattrRequest](#setxattrrequest) | [SetXattrResponse](#setxattrresponse) | 一元调用 (Unary) |
| 21 | [GetXattr](#getxattr) | [GetXattrRequest](#getxattrrequest) | [GetXattrResponse](#getxattrresponse) | 一元调用 (Unary) |
| 22 | [ListXattr](#listxattr) | [ListXattrRequest](#listxattrrequest) | [ListXattrResponse](#listxattrresponse) | 一元调用 (Unary) |
| 23 | [RemoveXattr](#removexattr) | [RemoveXattrRequest](#removexattrrequest) | [RemoveXattrResponse](#removexattrresponse) | 一元调用 (Unary) |
| 24 | [MountDisk](#mountdisk) | [MountDiskRequest](#mountdiskrequest) | [MountDiskResponse](#mountdiskresponse) | 一元调用 (Unary) |
| 25 | [RenewMount](#renewmount) | [RenewMountRequest](#renewmountrequest) | [RenewMountResponse](#renewmountresponse) | 一元调用 (Unary) |
| 26 | [UnmountDisk](#unmountdisk) | [UnmountDiskRequest](#unmountdiskrequest) | [UnmountDiskResponse](#unmountdiskresponse) | 一元调用 (Unary) |
| 27 | [CreateDisk](#createdisk) | [CreateDiskRequest](#creatediskrequest) | [CreateDiskResponse](#creatediskresponse) | 一元调用 (Unary) |
| 28 | [GetDisk](#getdisk) | [GetDiskRequest](#getdiskrequest) | [GetDiskResponse](#getdiskresponse) | 一元调用 (Unary) |
| 29 | [ListDisks](#listdisks) | [ListDisksRequest](#listdisksrequest) | [ListDisksResponse](#listdisksresponse) | 一元调用 (Unary) |
| 30 | [UpdateDisk](#updatedisk) | [UpdateDiskRequest](#updatediskrequest) | [UpdateDiskResponse](#updatediskresponse) | 一元调用 (Unary) |
| 31 | [DeleteDisk](#deletedisk) | [DeleteDiskRequest](#deletediskrequest) | [DeleteDiskResponse](#deletediskresponse) | 一元调用 (Unary) |
| 32 | [ForkDisk](#forkdisk) | [ForkDiskRequest](#forkdiskrequest) | [ForkDiskResponse](#forkdiskresponse) | 一元调用 (Unary) |
| 33 | [StatFS](#statfs) | [StatFSRequest](#statfsrequest) | [StatFSResponse](#statfsresponse) | 一元调用 (Unary) |
| 34 | [ReclaimOrphan](#reclaimorphan) | [ReclaimOrphanRequest](#reclaimorphanrequest) | [ReclaimOrphanResponse](#reclaimorphanresponse) | 一元调用 (Unary) |
| 35 | [DiffDisk](#diffdisk) | [DiffDiskRequest](#diffdiskrequest) | [DiffDiskResponse](#diffdiskresponse) | 一元调用 (Unary) |
| 36 | [ReadCommitNode](#readcommitnode) | [ReadCommitNodeRequest](#readcommitnoderequest) | [ReadCommitNodeResponse](#readcommitnoderesponse) | 一元调用 (Unary) |
| 37 | [MergeDisk](#mergedisk) | [MergeDiskRequest](#mergediskrequest) | [MergeDiskResponse](#mergediskresponse) | 一元调用 (Unary) |

### RPC 方法详述

#### Lookup

- **请求**: [LookupRequest](#lookuprequest)　|　**响应**: [LookupResponse](#lookupresponse)　|　**模式**: 一元调用 (Unary)

Lookup 执行单级目录项查找。

根据父目录 Inode 与目标子项名称获取对应的 Inode 元数据及内核缓存时效。

若父节点或目标子项不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

#### GetAttr

- **请求**: [GetAttrRequest](#getattrrequest)　|　**响应**: [GetAttrResponse](#getattrresponse)　|　**模式**: 一元调用 (Unary)

GetAttr 获取指定 Inode 的属性与内核缓存时效。

遵循 Linux FUSE FUSE_GETATTR 规范，支持已解链但仍打开的孤儿节点 (nlink == 0)。

若指定 Inode 不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

#### SetAttr

- **请求**: [SetAttrRequest](#setattrrequest)　|　**响应**: [SetAttrResponse](#setattrresponse)　|　**模式**: 一元调用 (Unary)

SetAttr 修改指定 Inode 的元数据属性（对接 Linux FUSE FUSE_SETATTR 协议）。

遵循按需部分更新语义。若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)；

若违反 POSIX 语义（如对目录设置大小），必须返回 gRPC 标准状态码 InvalidArgument 或相应错误。

#### Mknod

- **请求**: [MknodRequest](#mknodrequest)　|　**响应**: [MknodResponse](#mknodresponse)　|　**模式**: 一元调用 (Unary)

Mknod 在指定父目录下创建常规文件或特殊设备节点。

若同名文件或目录项已存在，必须返回 gRPC 标准状态码 AlreadyExists (映射至 POSIX EEXIST)。

若父节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

#### Mkdir

- **请求**: [MkdirRequest](#mkdirrequest)　|　**响应**: [MkdirResponse](#mkdirresponse)　|　**模式**: 一元调用 (Unary)

Mkdir 在指定父目录下创建子目录。

若同名文件或目录项已存在，必须返回 gRPC 标准状态码 AlreadyExists (映射至 POSIX EEXIST)。

若父节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

#### Symlink

- **请求**: [SymlinkRequest](#symlinkrequest)　|　**响应**: [SymlinkResponse](#symlinkresponse)　|　**模式**: 一元调用 (Unary)

Symlink 在指定父目录下创建指向目标路径的符号链接节点。

若同名文件或目录项已存在，必须返回 gRPC 标准状态码 AlreadyExists (映射至 POSIX EEXIST)。

若父节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

#### Readlink

- **请求**: [ReadlinkRequest](#readlinkrequest)　|　**响应**: [ReadlinkResponse](#readlinkresponse)　|　**模式**: 一元调用 (Unary)

Readlink 获取符号链接节点指向的目标路径。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若节点存在但非符号链接类型，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### Link

- **请求**: [LinkRequest](#linkrequest)　|　**响应**: [LinkResponse](#linkresponse)　|　**模式**: 一元调用 (Unary)

Link 为已有非目录 Inode 在目标父目录下创建硬链接。

若同名文件或目录项已存在，必须返回 gRPC 标准状态码 AlreadyExists (映射至 POSIX EEXIST)。

若父节点不存在或源节点不存在/已解链，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

若源节点为目录，必须返回 gRPC 标准状态码 PermissionDenied (映射至 POSIX EPERM)。

#### Unlink

- **请求**: [UnlinkRequest](#unlinkrequest)　|　**响应**: [UnlinkResponse](#unlinkresponse)　|　**模式**: 一元调用 (Unary)

Unlink 删除指定父目录下的非目录项（解链）。

若目标节点为目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX EISDIR)。

若父节点不存在或目标目录项不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

对 "." 或 ".."，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### Rmdir

- **请求**: [RmdirRequest](#rmdirrequest)　|　**响应**: [RmdirResponse](#rmdirresponse)　|　**模式**: 一元调用 (Unary)

Rmdir 移除指定父目录下的空目录项。

若目标节点非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

若目标目录非空，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTEMPTY)。

若父节点不存在或目标目录项不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

对 "." 返回 InvalidArgument (EINVAL)；对 ".." 返回 FailedPrecondition (ENOTEMPTY)。

#### Rename

- **请求**: [RenameRequest](#renamerequest)　|　**响应**: [RenameResponse](#renameresponse)　|　**模式**: 一元调用 (Unary)

Rename 执行原子文件/目录重命名、移动或双向交换（支持 FUSE_RENAME2 的 RENAME_NOREPLACE 与 RENAME_EXCHANGE）。

当 flags 为 0 时，执行传统覆盖重命名；若源与目标为同一路径或同一 Inode 的硬链接，执行无副作用短路成功返回。

当 flags 为 RENAME_NOREPLACE (0x1) 时，禁止覆盖已存在的目标项（包含同一路径或同一 Inode 的不同硬链接），若目标已存在则返回 AlreadyExists (EEXIST)。

当 flags 为 RENAME_EXCHANGE (0x2) 时，要求源与目标均存在（任一不存在返回 NotFound / ENOENT），原子交换两个目录项的 Inode 绑定并执行双向祖先环检测。

若传入互斥组合（RENAME_NOREPLACE | RENAME_EXCHANGE）、RENAME_WHITEOUT (0x4) 或未知标志位，返回 InvalidArgument (EINVAL)。

若源项或父目录不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父目录存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

当 flags 为 0 覆盖目标时：若源为目录而目标为非目录返回 FailedPrecondition (ENOTDIR)；若源为非目录而目标为目录返回 FailedPrecondition (EISDIR)；若源为目录而目标为非空目录返回 FailedPrecondition (ENOTEMPTY)。

若尝试将目录移动或交换为其自身或子孙目录，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

对 "." 或 ".." 的重命名，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### Create

- **请求**: [CreateRequest](#createrequest)　|　**响应**: [CreateResponse](#createresponse)　|　**模式**: 一元调用 (Unary)

Create 在指定父目录下创建常规文件，文件句柄由客户端管理。

若同名文件或目录项已存在，必须返回 gRPC 标准状态码 AlreadyExists (映射至 POSIX EEXIST)。

若父节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若父节点存在但非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

仅允许创建常规文件 (mode & S_IFMT 必须为 0 或 S_IFREG)。

#### Read

- **请求**: [ReadRequest](#readrequest)　|　**响应**: [ReadResponse](#readresponse)　|　**模式**: 一元调用 (Unary)

Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。

遵循 POSIX pread(2) 语义，支持最大 4 MiB 单次读取。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若目标为目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX EISDIR)。

#### Write

- **请求**: [WriteRequest](#writerequest)　|　**响应**: [WriteResponse](#writeresponse)　|　**模式**: 一元调用 (Unary)

Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。

遵循 POSIX pwrite(2) 语义，支持最大 4 MiB 单次写入。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若目标为目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX EISDIR)。

#### Fsync

- **请求**: [FsyncRequest](#fsyncrequest)　|　**响应**: [FsyncResponse](#fsyncresponse)　|　**模式**: 一元调用 (Unary)

Fsync 同步并持久化文件节点与句柄关联的状态。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

#### FsyncDir

- **请求**: [FsyncDirRequest](#fsyncdirrequest)　|　**响应**: [FsyncDirResponse](#fsyncdirresponse)　|　**模式**: 一元调用 (Unary)

FsyncDir 同步并持久化目录节点与目录句柄关联的状态（对接 Linux FUSE FUSE_FSYNCDIR = 30 协议）。

若目标非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

支持对处于 rmdir-whilst-open 状态的孤儿目录 (nlink == 0) 幂等同步成功。

#### ReadDir

- **请求**: [ReadDirRequest](#readdirrequest)　|　**响应**: [ReadDirResponse](#readdirresponse)　|　**模式**: 一元调用 (Unary)

ReadDir 读取指定目录的子项流（基本条目）。

若目标非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

处于 rmdir-whilst-open 状态的孤儿目录直接返回空列表。

#### ReadDirPlus

- **请求**: [ReadDirPlusRequest](#readdirplusrequest)　|　**响应**: [ReadDirPlusResponse](#readdirplusresponse)　|　**模式**: 一元调用 (Unary)

ReadDirPlus 读取指定目录的子项流并附带完整 Inode 属性与缓存时效（扩展条目）。

若目标非目录，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX ENOTDIR)。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

处于 rmdir-whilst-open 状态的孤儿目录直接返回空列表。

#### Fallocate

- **请求**: [FallocateRequest](#fallocaterequest)　|　**响应**: [FallocateResponse](#fallocateresponse)　|　**模式**: 一元调用 (Unary)

Fallocate 为常规文件预分配物理空间或打洞去分配空间（对接 Linux FUSE FUSE_FALLOCATE 协议）。

遵循 Linux fallocate(2) 语义，支持空间预分配、保留大小分配及打洞释放空间。

#### SetXattr

- **请求**: [SetXattrRequest](#setxattrrequest)　|　**响应**: [SetXattrResponse](#setxattrresponse)　|　**模式**: 一元调用 (Unary)

SetXattr 设置指定 Inode 的扩展属性（对接 Linux FUSE FUSE_SETXATTR 协议）。

遵循 POSIX setxattr(2) 语义，支持 XATTR_CREATE (0x1) 与 XATTR_REPLACE (0x2) 标志位状态机。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若对 user.* 属性操作非常规文件、非目录或非套接字节点，必须返回 gRPC 标准状态码 PermissionDenied (映射至 POSIX EPERM)。

若违反标志位约束，分别返回 AlreadyExists (EEXIST) 或 NotFound (ENODATA)。

若新增属性超出单节点 65536 字节配额，必须返回 gRPC 标准状态码 ResourceExhausted (映射至 POSIX ENOSPC)。

#### GetXattr

- **请求**: [GetXattrRequest](#getxattrrequest)　|　**响应**: [GetXattrResponse](#getxattrresponse)　|　**模式**: 一元调用 (Unary)

GetXattr 获取指定 Inode 的扩展属性值（对接 Linux FUSE FUSE_GETXATTR 协议）。

遵循 POSIX getxattr(2) 语义。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若扩展属性不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENODATA)。

若对 user.* 属性操作非常规文件、非目录或非套接字节点，直接返回 gRPC 标准状态码 NotFound (映射至 POSIX ENODATA)。

#### ListXattr

- **请求**: [ListXattrRequest](#listxattrrequest)　|　**响应**: [ListXattrResponse](#listxattrresponse)　|　**模式**: 一元调用 (Unary)

ListXattr 列出指定 Inode 的全部扩展属性名称列表（对接 Linux FUSE FUSE_LISTXATTR 协议）。

遵循 POSIX listxattr(2) 语义。

属性名称按原始字节序升序（BYTEA ASC）返回；不按调用方过滤（对齐 Linux 内核 FUSE 行为）。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若无属性，返回空列表。

#### RemoveXattr

- **请求**: [RemoveXattrRequest](#removexattrrequest)　|　**响应**: [RemoveXattrResponse](#removexattrresponse)　|　**模式**: 一元调用 (Unary)

RemoveXattr 移除指定 Inode 的扩展属性（对接 Linux FUSE FUSE_REMOVEXATTR 协议）。

遵循 POSIX removexattr(2) 语义。

若节点不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若扩展属性不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENODATA)。

若对 user.* 属性操作非常规文件、非目录或非套接字节点，直接返回 gRPC 标准状态码 PermissionDenied (映射至 POSIX EPERM)。

#### MountDisk

- **请求**: [MountDiskRequest](#mountdiskrequest)　|　**响应**: [MountDiskResponse](#mountdiskresponse)　|　**模式**: 一元调用 (Unary)

MountDisk 执行单盘挂载握手。

可写挂载 (read_only = false) 原子递增并持久化 WriterEpoch，清理残留孤儿节点并返回新世代号与根 Inode。

只读挂载 (read_only = true) 校验目标磁盘存在并返回 epoch = 0 与根 Inode。

#### RenewMount

- **请求**: [RenewMountRequest](#renewmountrequest)　|　**响应**: [RenewMountResponse](#renewmountresponse)　|　**模式**: 一元调用 (Unary)

RenewMount 为已建立的在线挂载软租约执行周期续期。

#### UnmountDisk

- **请求**: [UnmountDiskRequest](#unmountdiskrequest)　|　**响应**: [UnmountDiskResponse](#unmountdiskresponse)　|　**模式**: 一元调用 (Unary)

UnmountDisk 幂等释放指定的在线挂载软租约。

#### CreateDisk

- **请求**: [CreateDiskRequest](#creatediskrequest)　|　**响应**: [CreateDiskResponse](#creatediskresponse)　|　**模式**: 一元调用 (Unary)

CreateDisk 创建新的虚拟磁盘实体并原子创建其根目录 Inode。

分配全局唯一 UUIDv7 磁盘标识符与初始配额上限，创建根目录 Inode 并将其与磁盘一对一强绑定。

若指定参数非法（如 name 超过 255 字节或包含 '\x00'，或 max_bytes 为 0/超出 int64 最大值），必须返回 InvalidArgument (映射至 POSIX EINVAL) 或相应错误。

#### GetDisk

- **请求**: [GetDiskRequest](#getdiskrequest)　|　**响应**: [GetDiskResponse](#getdiskresponse)　|　**模式**: 一元调用 (Unary)

GetDisk 查询指定 disk_id 的虚拟磁盘实体详情与配额状态。

若目标磁盘不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若 disk_id 格式非法或为全零 Nil UUID，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### ListDisks

- **请求**: [ListDisksRequest](#listdisksrequest)　|　**响应**: [ListDisksResponse](#listdisksresponse)　|　**模式**: 一元调用 (Unary)

ListDisks 分页列出系统中的虚拟磁盘实体（按创建时间倒序排列）。

#### UpdateDisk

- **请求**: [UpdateDiskRequest](#updatediskrequest)　|　**响应**: [UpdateDiskResponse](#updatediskresponse)　|　**模式**: 一元调用 (Unary)

UpdateDisk 更新指定虚拟磁盘的名称或容量配额上限。

支持按需单独或同时更新 name 与 max_bytes。

若新 max_bytes 小于当前已用空间 (used_bytes)，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX EINVAL 缩容保护)。

若目标磁盘不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若参数校验不通过（如 disk_id 格式非法、name 与 max_bytes 均未传、name 超过 255 字节或包含 '\x00'、max_bytes 为 0/超出 int64 最大值），必须返回 InvalidArgument (映射至 POSIX EINVAL)。

#### DeleteDisk

- **请求**: [DeleteDiskRequest](#deletediskrequest)　|　**响应**: [DeleteDiskResponse](#deletediskresponse)　|　**模式**: 一元调用 (Unary)

DeleteDisk 级联删除指定虚拟磁盘实体并强制回收其全部元数据节点。

若目标磁盘不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若 disk_id 格式非法或为全零 Nil UUID，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### ForkDisk

- **请求**: [ForkDiskRequest](#forkdiskrequest)　|　**响应**: [ForkDiskResponse](#forkdiskresponse)　|　**模式**: 一元调用 (Unary)

ForkDisk 基于指定源虚拟磁盘当前已提交状态零拷贝派生新的虚拟磁盘实体。

自动将源磁盘当前状态固化为不可变共同祖先切面，并在不中断源磁盘在线读写的前提下克隆出独立子磁盘。

若源磁盘不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若显式指定的 max_bytes 小于孤儿节点净化后的初始已用空间，必须返回 gRPC 标准状态码 FailedPrecondition (映射至 POSIX EINVAL 缩容保护)。

若参数校验不通过（如 source_disk_id 格式非法或为全零 Nil UUID、name 超过 255 字节或包含 '\x00'、max_bytes 为 0/超出 int64 最大值），必须返回 InvalidArgument (映射至 POSIX EINVAL 或 ENAMETOOLONG)。

#### StatFS

- **请求**: [StatFSRequest](#statfsrequest)　|　**响应**: [StatFSResponse](#statfsresponse)　|　**模式**: 一元调用 (Unary)

StatFS 查询指定 Inode 所属虚拟磁盘的字节容量与 POSIX 4096 字节块统计信息（对接 Linux FUSE FUSE_STATFS = 17 协议）。

接收目标节点的 uint64 ino，查询所属 Disk 配额并返回 4096 字节块及已用 Inode 统计信息。

若目标 Inode 或其所属虚拟磁盘不存在，必须返回 gRPC 标准状态码 NotFound (映射至 POSIX ENOENT)。

若 ino 为 0 或超出 int64 正数最大值，必须返回 gRPC 标准状态码 InvalidArgument (映射至 POSIX EINVAL)。

#### ReclaimOrphan

- **请求**: [ReclaimOrphanRequest](#reclaimorphanrequest)　|　**响应**: [ReclaimOrphanResponse](#reclaimorphanresponse)　|　**模式**: 一元调用 (Unary)

ReclaimOrphan 回收最后一个本地句柄关闭的孤儿节点。

#### DiffDisk

- **请求**: [DiffDiskRequest](#diffdiskrequest)　|　**响应**: [DiffDiskResponse](#diffdiskresponse)　|　**模式**: 一元调用 (Unary)

DiffDisk 计算源虚拟磁盘相对最近共同祖先与目标虚拟磁盘之间的差异与冲突列表。

#### ReadCommitNode

- **请求**: [ReadCommitNodeRequest](#readcommitnoderequest)　|　**响应**: [ReadCommitNodeResponse](#readcommitnoderesponse)　|　**模式**: 一元调用 (Unary)

ReadCommitNode 读取指定不可变提交切面中某个节点的元数据与文件内容切片或符号链接目标。

#### MergeDisk

- **请求**: [MergeDiskRequest](#mergediskrequest)　|　**响应**: [MergeDiskResponse](#mergediskresponse)　|　**模式**: 一元调用 (Unary)

MergeDisk 将源虚拟磁盘的变更三路合并至目标虚拟磁盘（支持 dry_run 预览与冲突解决策略）。

## 枚举类型定义

### DiffKind

DiffKind 表示目录树条目在三路或两路差异比较中的变更类型。

| 枚举值 | 数值 | 说明 |
| --- | --- | --- |
| `DIFF_KIND_UNSPECIFIED` | `0` | 未指定变更类型。 |
| `DIFF_KIND_ADDED` | `1` | 新增条目。 |
| `DIFF_KIND_MODIFIED` | `2` | 修改条目。 |
| `DIFF_KIND_DELETED` | `3` | 删除条目。 |

### ConflictKind

ConflictKind 表示三路合并或差异检测中识别出的冲突类型。

| 枚举值 | 数值 | 说明 |
| --- | --- | --- |
| `CONFLICT_KIND_UNSPECIFIED` | `0` | 无冲突。 |
| `CONFLICT_KIND_ADD_ADD` | `1` | 双边新增同名条目冲突（Add/Add）。 |
| `CONFLICT_KIND_CONTENT` | `2` | 常规文件内容双边修改冲突（Content）。 |
| `CONFLICT_KIND_MODIFY_DELETE` | `3` | 目标盘修改而源盘删除冲突（Modify/Delete）。 |
| `CONFLICT_KIND_DELETE_MODIFY` | `4` | 目标盘删除而源盘修改冲突（Delete/Modify）。 |
| `CONFLICT_KIND_TREE_DELETE_MODIFY` | `5` | 目录树删除与子树修改冲突（Tree Delete/Modify）。 |
| `CONFLICT_KIND_TYPE` | `6` | 节点类型不匹配冲突（Type）。 |
| `CONFLICT_KIND_DIR_FILE` | `7` | 目录与文件同名冲突（Dir/File）。 |
| `CONFLICT_KIND_ATTR` | `8` | POSIX 元数据属性冲突（Attr）。 |
| `CONFLICT_KIND_XATTR` | `9` | 扩展属性冲突（Xattr）。 |
| `CONFLICT_KIND_XATTR_QUOTA` | `10` | 扩展属性配额或容量限制冲突（Xattr Quota）。 |
| `CONFLICT_KIND_SECURITY_ATTR` | `11` | 安全扩展属性冲突（Security Xattr）。 |
| `CONFLICT_KIND_HARDLINK` | `12` | 硬链接拓扑冲突（Hardlink）。 |

### MergeStrategy

MergeStrategy 表示磁盘合并时对冲突条目的解决策略。

| 枚举值 | 数值 | 说明 |
| --- | --- | --- |
| `MERGE_STRATEGY_UNSPECIFIED` | `0` | 未指定策略（服务端按 FAIL 语义处理）。 |
| `MERGE_STRATEGY_FAIL` | `1` | 遇到任何未显式覆盖的冲突即中止并报告冲突（默认语义）。 |
| `MERGE_STRATEGY_OURS` | `2` | 冲突时保留目标盘（ours）版本。 |
| `MERGE_STRATEGY_THEIRS` | `3` | 冲突时采用源盘（theirs）版本。 |

## 消息结构定义

### LookupRequest

LookupRequest 定义 Lookup 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。根目录通常为 1。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `maxLen=255` | 待查找的子项名称（文件或目录名）。&lt;br&gt;长度至多 255 字节，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `disk_id` | `string` | singular | CEL `lookup_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### LookupResponse

LookupResponse 包含查找命中的节点元数据以及 FUSE 内核缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 命中的目标 Inode 实体元数据。 |
| 2 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 3 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### GetAttrRequest

GetAttrRequest 定义 GetAttr 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点的 Inode 编号。&lt;br&gt;必须大于 0 且小于等于 int64 最大值 (9223372036854775807)，防止向数据库 BIGINT 转换时发生整型溢出。 |
| 2 | `disk_id` | `string` | singular | CEL `getattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### GetAttrResponse

GetAttrResponse 包含目标节点元数据以及 FUSE 属性缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 目标 Inode 实体元数据。 |
| 2 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### SetAttrRequest

SetAttrRequest 定义 SetAttr 接口输入参数。

遵循按需部分更新语义，未显式设置的属性保持原值不变。

**消息级校验规则（protovalidate）：**

- CEL `setattr_atime_conflict`: `!(has(this.atime) && this.atime_now)`（atime and atime_now cannot both be set）
- CEL `setattr_mtime_conflict`: `!(has(this.mtime) && this.mtime_now)`（mtime and mtime_now cannot both be set）
- CEL `setattr_has_update`: `has(this.mode) || has(this.uid) || has(this.gid) || has(this.size) || has(this.atime) || has(this.mtime) || this.atime_now || this.mtime_now || this.ctime_now || has(this.flags)`（at least one attribute must be updated）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 待修改属性的目标 Inode 编号。 |
| 2 | `mode` | `uint32` | optional | — | 待修改的文件模式与权限位。 |
| 3 | `uid` | `uint32` | optional | `uint32`: `lte=2147483647` | 待修改的属主用户 ID。 |
| 4 | `gid` | `uint32` | optional | `uint32`: `lte=2147483647` | 待修改的属组 ID。 |
| 5 | `size` | `uint64` | optional | `uint64`: `lte=9223372036854775807` | 待修改的文件物理大小（字节数）。 |
| 6 | `atime` | `google.protobuf.Timestamp` | optional | — | 待修改的最后访问时间。 |
| 7 | `mtime` | `google.protobuf.Timestamp` | optional | — | 待修改的最后修改时间。 |
| 8 | `atime_now` | `bool` | singular | — | 是否将最后访问时间设置为当前服务端时间。与 atime 互斥。 |
| 9 | `mtime_now` | `bool` | singular | — | 是否将最后修改时间设置为当前服务端时间。与 mtime 互斥。 |
| 10 | `fh` | `uint64` | optional | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；句柄由客户端在本地管理。 |
| 11 | `disk_id` | `string` | singular | CEL `setattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 12 | `ctime_now` | `bool` | singular | — | 是否将状态变更时间设置为当前服务端时间。 |
| 13 | `flags` | `uint32` | optional | `uint32`: `lte=2147483647` | 可选的文件系统标志位（如 chattr 所设）。 |
| 14 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### SetAttrResponse

SetAttrResponse 包含更新后的 Inode 实体元数据及内核属性缓存时长。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 更新后的目标 Inode 实体元数据。 |
| 2 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### MknodRequest

MknodRequest 定义 Mknod 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待创建的新节点名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `mode` | `uint32` | singular | — | 节点模式位（包含文件类型及权限位）。 |
| 4 | `uid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属用户 ID。 |
| 5 | `gid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属组 ID。 |
| 6 | `rdev` | `uint32` | singular | — | 特殊设备号（仅当创建字符/块设备文件时有效，其余节点为 0）。 |
| 7 | `disk_id` | `string` | singular | CEL `mknod_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 8 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### MknodResponse

MknodResponse 包含新创建节点的元数据以及 FUSE 内核缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 新创建的 Inode 实体元数据。 |
| 2 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 3 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### MkdirRequest

MkdirRequest 定义 Mkdir 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待创建的新目录名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `mode` | `uint32` | singular | — | 目录模式位（包含可选的 S_IFDIR 文件类型位及权限位）。 |
| 4 | `uid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属用户 ID。 |
| 5 | `gid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属组 ID。 |
| 6 | `disk_id` | `string` | singular | CEL `mkdir_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 7 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### MkdirResponse

MkdirResponse 包含新创建目录的元数据以及 FUSE 内核缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 新创建的 Inode 实体元数据。 |
| 2 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 3 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### SymlinkRequest

SymlinkRequest 定义 Symlink 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待创建的符号链接名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `target` | `bytes` | singular | CEL `symlink_request_target_no_null`: `!this.contains(b'\x00')`（symlink target must not contain null byte）<br>`bytes`: `minLen=1`, `maxLen=4095` | 符号链接指向的目标路径。&lt;br&gt;长度必须在 1 到 4095 字节之间，且严禁包含空字节 '\x00'（允许包含路径分隔符 '/'）。 |
| 4 | `uid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属用户 ID。 |
| 5 | `gid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属组 ID。 |
| 6 | `disk_id` | `string` | singular | CEL `symlink_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 7 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### SymlinkResponse

SymlinkResponse 包含新创建符号链接的元数据以及 FUSE 内核缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 新创建的 Inode 实体元数据。 |
| 2 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 3 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### ReadlinkRequest

ReadlinkRequest 定义 Readlink 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标符号链接的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `disk_id` | `string` | singular | CEL `readlink_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### ReadlinkResponse

ReadlinkResponse 包含符号链接指向的目标路径。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `target` | `bytes` | singular | CEL `readlink_response_target_no_null`: `!this.contains(b'\x00')`（readlink target must not contain null byte）<br>`bytes`: `minLen=1`, `maxLen=4095` | 目标路径。 |

### LinkRequest

LinkRequest 定义 Link 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 待创建硬链接的源节点 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `new_parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 3 | `new_name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待创建的新目录项名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 4 | `disk_id` | `string` | singular | CEL `link_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 5 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### LinkResponse

LinkResponse 包含硬链接创建后更新的源节点元数据及内核缓存时长。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 硬链接计数递增后的 Inode 实体元数据。 |
| 2 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 3 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### UnlinkRequest

UnlinkRequest 定义 Unlink 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `maxLen=255` | 待删除的非目录项名称。&lt;br&gt;长度至多 255 字节，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `disk_id` | `string` | singular | CEL `unlink_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 4 | `open_in_client` | `bool` | optional | — | 是否仍有本地打开句柄；省略时按未打开处理。 |
| 5 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### UnlinkResponse

UnlinkResponse 定义 Unlink 接口返回参数。

*无字段（空消息）。*

### RmdirRequest

RmdirRequest 定义 Rmdir 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `maxLen=255` | 待移除的空目录项名称。&lt;br&gt;长度至多 255 字节，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `disk_id` | `string` | singular | CEL `rmdir_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 4 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### RmdirResponse

RmdirResponse 定义 Rmdir 接口返回参数。

*无字段（空消息）。*

### RenameRequest

RenameRequest 定义 Rename 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `old_parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 源父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `old_name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `maxLen=255` | 待重命名的源项名称。&lt;br&gt;长度至多 255 字节，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `new_parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 4 | `new_name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 目标项新名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 5 | `flags` | `uint32` | singular | — | 重命名标志位。 |
| 6 | `disk_id` | `string` | singular | CEL `rename_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 7 | `target_open_in_client` | `bool` | optional | — | 被覆盖的目标是否仍有本地打开句柄；省略时按未打开处理。 |
| 8 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### RenameResponse

RenameResponse 定义 Rename 接口返回参数。

*无字段（空消息）。*

### CreateRequest

CreateRequest 定义 Create 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `parent_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 父目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待创建的新常规文件名称。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `mode` | `uint32` | singular | — | 节点模式位（包含可选的 S_IFREG 文件类型位及权限位）。 |
| 4 | `uid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属用户 ID。 |
| 5 | `gid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属组 ID。 |
| 6 | `flags` | `uint32` | singular | — | 打开标志位 (open flags, 如 O_CREAT, O_RDWR, O_WRONLY 等，严禁携带 O_TMPFILE = 0o20000000)。 |
| 7 | `disk_id` | `string` | singular | CEL `create_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 8 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### CreateResponse

CreateResponse 包含新创建常规文件的元数据和 FUSE 内核缓存有效时间。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `inode` | [Inode](#inode) | singular | `required` | 新创建的 Inode 实体元数据。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `gt=0` | 保留的兼容字段；客户端自行分配文件句柄。 |
| 3 | `open_flags` | `uint32` | singular | — | 打开响应标志位 (open flags)。 |
| 4 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项 (Entry) 在 FUSE 内核中的有效缓存时长（秒）。 |
| 5 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性 (attr) 在 FUSE 内核中的有效缓存时长（秒）。 |

### ReadRequest

ReadRequest 定义 Read 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标常规文件的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；文件句柄由客户端在本地管理。 |
| 3 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 读取起始偏移量（字节）。必须在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 4 | `size` | `uint32` | singular | `uint32`: `lte=4194304` | 读取字节数（最大 4194304，即 4 MiB）。 |
| 5 | `disk_id` | `string` | singular | CEL `read_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### ReadResponse

ReadResponse 定义 Read 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `data` | `bytes` | singular | — | 读取到的数据载荷。 |

### WriteRequest

WriteRequest 定义 Write 接口输入参数。

**消息级校验规则（protovalidate）：**

- CEL `write_request_offset_len_overflow`: `uint(this.offset) <= 9223372036854775807u - uint(size(this.data))`（offset + len(data) must not exceed MaxInt64 (9223372036854775807)）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标常规文件的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；文件句柄由客户端在本地管理。 |
| 3 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 写入起始偏移量（字节）。必须在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 4 | `data` | `bytes` | singular | `bytes`: `maxLen=4194304` | 写入的数据载荷（最大 4194304 字节，即 4 MiB）。 |
| 5 | `disk_id` | `string` | singular | CEL `write_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 6 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### WriteResponse

WriteResponse 定义 Write 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `size` | `uint32` | singular | `uint32`: `lte=4194304` | 实际写入的字节数。 |
| 2 | `inode` | [Inode](#inode) | singular | `required` | 写入后更新的 Inode 实体元数据。 |

### FsyncRequest

FsyncRequest 定义 Fsync 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标文件节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；句柄由客户端在本地管理。 |
| 3 | `datasync` | `bool` | singular | — | 是否仅同步数据内容 (fdatasync 语义)。若为 true，仅同步文件数据而不强制刷写非必要元数据。 |
| 4 | `disk_id` | `string` | singular | CEL `fsync_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 5 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### FsyncResponse

FsyncResponse 定义 Fsync 接口响应结果。

*无字段（空消息）。*

### FsyncDirRequest

FsyncDirRequest 定义 FsyncDir 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标目录节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 待同步的目录 FileHandle。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 3 | `datasync` | `bool` | singular | — | 是否仅同步数据内容 (fdatasync 语义)。若为 true，仅同步目录数据而不强制刷写非必要元数据。 |
| 4 | `disk_id` | `string` | singular | CEL `fsyncdir_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 5 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### FsyncDirResponse

FsyncDirResponse 定义 FsyncDir 接口响应结果。

*无字段（空消息）。*

### DirEntry

DirEntry 定义 ReadDir 返回的基本目录项。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `gt=0` | 子项对应的 Inode 编号。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 子项名称（支持 "." 与 ".."）。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `mode` | `uint32` | singular | `uint32`: `gt=0` | 文件的模式位（包含文件类型与权限位）。 |
| 4 | `next_offset` | `uint64` | singular | `uint64`: `gt=0` | 下一个条目的游标偏移量（单调递增，大于 0）。 |

### ReadDirRequest

ReadDirRequest 定义 ReadDir 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；目录句柄由客户端在本地管理。 |
| 3 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 读取起始游标偏移量。0 表示从头开始读取（包含 "." 与 ".."）。 |
| 4 | `limit` | `uint32` | singular | `uint32`: `lte=4096`, `gt=0` | 单次最大读取条目数配额（最大 4096）。 |
| 5 | `disk_id` | `string` | singular | CEL `readdir_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### ReadDirResponse

ReadDirResponse 定义 ReadDir 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `entries` | [DirEntry](#direntry) | repeated | — | 目录项列表。 |

### DirEntryPlus

DirEntryPlus 定义 ReadDirPlus 返回的扩展目录项（内联属性与缓存时效）。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `gt=0` | 子项对应的 Inode 编号。 |
| 2 | `name` | `bytes` | singular | CEL `bytes_name_no_slash_or_nul`: `!this.contains(b'\x00') && !this.contains(b'/')`（name cannot contain '/' or NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 子项名称（支持 "." 与 ".."）。&lt;br&gt;长度必须在 1 到 255 字节之间，且严禁包含路径分隔符 '/' 与空字节 '\x00'。 |
| 3 | `next_offset` | `uint64` | singular | `uint64`: `gt=0` | 下一个条目的游标偏移量（单调递增，大于 0）。 |
| 4 | `inode` | [Inode](#inode) | singular | `required` | 子项的完整 Inode 元数据。 |
| 5 | `entry_timeout_seconds` | `double` | singular | `double`: `gte=0` | 目录项在内核中的缓存有效时间（秒）。对 "." 与 ".." 固定为 0。 |
| 6 | `attr_timeout_seconds` | `double` | singular | `double`: `gte=0` | 属性在内核中的缓存有效时间（秒）。 |

### ReadDirPlusRequest

ReadDirPlusRequest 定义 ReadDirPlus 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标目录的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；目录句柄由客户端在本地管理。 |
| 3 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 读取起始游标偏移量。0 表示从头开始读取（包含 "." 与 ".."）。 |
| 4 | `limit` | `uint32` | singular | `uint32`: `lte=4096`, `gt=0` | 单次最大读取条目数配额（最大 4096）。 |
| 5 | `disk_id` | `string` | singular | CEL `readdirplus_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### ReadDirPlusResponse

ReadDirPlusResponse 定义 ReadDirPlus 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `entries` | [DirEntryPlus](#direntryplus) | repeated | — | 扩展目录项列表。 |

### FallocateRequest

FallocateRequest 定义 Fallocate 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标常规文件的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `fh` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 保留的兼容字段；文件句柄由客户端在本地管理。 |
| 3 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 操作起始偏移量（字节）。必须在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 4 | `length` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 操作区间长度（字节）。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 5 | `mode` | `uint32` | singular | — | fallocate 模式位标志，支持 0、FALLOC_FL_KEEP_SIZE、FALLOC_FL_PUNCH_HOLE \| FALLOC_FL_KEEP_SIZE、FALLOC_FL_ZERO_RANGE 与 FALLOC_FL_ZERO_RANGE \| FALLOC_FL_KEEP_SIZE；单独传入 FALLOC_FL_PUNCH_HOLE 返回 InvalidArgument 与 EINVAL，其余不支持模式返回 Unimplemented 与 EOPNOTSUPP。 |
| 6 | `disk_id` | `string` | singular | CEL `fallocate_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 7 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### FallocateResponse

FallocateResponse 定义 Fallocate 接口响应结果。

*无字段（空消息）。*

### SetXattrRequest

SetXattrRequest 定义 SetXattr 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `xattr_name_no_nul`: `!this.contains(b'\x00')`（xattr name cannot contain NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 扩展属性名称。&lt;br&gt;长度必须在 1 到 255 字节之间：空名称（0 字节）或包含空字节 '\x00' 由校验器显式映射为 ErrorInfo.reason = "EINVAL"；&lt;br&gt;名称超过 255 字节显式映射为 ErrorInfo.reason = "ERANGE"。 |
| 3 | `value` | `bytes` | singular | `bytes`: `maxLen=65536` | 扩展属性值。&lt;br&gt;长度最大允许 65536 字节（超过 65536 字节由校验器显式映射为 ErrorInfo.reason = "ERANGE"）。 |
| 4 | `flags` | `uint32` | singular | `uint32`: `lte=3` | 扩展属性标志位 (0: 默认覆盖或创建, 1: XATTR_CREATE, 2: XATTR_REPLACE, 3: XATTR_CREATE \| XATTR_REPLACE)。&lt;br&gt;flags = 3 允许进入领域层按目标属性存在性分别返回 EEXIST 或 ENODATA；flags &gt; 3 在协议入口拒绝并返回 EINVAL。 |
| 5 | `disk_id` | `string` | singular | CEL `setxattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 6 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### SetXattrResponse

SetXattrResponse 定义 SetXattr 接口响应结果。

*无字段（空消息）。*

### GetXattrRequest

GetXattrRequest 定义 GetXattr 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `xattr_name_no_nul`: `!this.contains(b'\x00')`（xattr name cannot contain NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 扩展属性名称。&lt;br&gt;长度必须在 1 到 255 字节之间：空名称（0 字节）或包含空字节 '\x00' 由校验器显式映射为 ErrorInfo.reason = "EINVAL"；&lt;br&gt;名称超过 255 字节显式映射为 ErrorInfo.reason = "ERANGE"。 |
| 3 | `disk_id` | `string` | singular | CEL `getxattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### GetXattrResponse

GetXattrResponse 定义 GetXattr 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `value` | `bytes` | singular | `bytes`: `maxLen=65536` | 扩展属性值。&lt;br&gt;长度最大允许 65536 字节（超长违规由校验器显式映射为 ErrorInfo.reason = "ERANGE"）。 |

### ListXattrRequest

ListXattrRequest 定义 ListXattr 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `disk_id` | `string` | singular | CEL `listxattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### ListXattrResponse

ListXattrResponse 定义 ListXattr 接口响应结果。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `names` | `bytes` | repeated | `repeated`: 元素: CEL `xattr_name_no_nul`: `!this.contains(b'\x00')`（xattr name cannot contain NUL byte）; `bytes`: `minLen=1`, `maxLen=255` | 节点绑定的扩展属性名称列表，按原始字节序升序（BYTEA ASC）排列。&lt;br&gt;每一项名称长度必须在 1 到 255 字节之间，且严禁包含空字节 '\x00'。 |

### RemoveXattrRequest

RemoveXattrRequest 定义 RemoveXattr 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点的 Inode 编号。必须大于 0 且在 int64 正数范围内 (&lt;= 9223372036854775807)。 |
| 2 | `name` | `bytes` | singular | CEL `xattr_name_no_nul`: `!this.contains(b'\x00')`（xattr name cannot contain NUL byte）<br>`bytes`: `minLen=1`, `maxLen=255` | 待移除的扩展属性名称。&lt;br&gt;长度必须在 1 到 255 字节之间：空名称（0 字节）或包含空字节 '\x00' 由校验器显式映射为 ErrorInfo.reason = "EINVAL"；&lt;br&gt;名称超过 255 字节显式映射为 ErrorInfo.reason = "ERANGE"。 |
| 3 | `disk_id` | `string` | singular | CEL `removexattr_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 4 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 写挂载世代号栅栏（必须大于 0 且在 int64 正数范围内）。 |

### RemoveXattrResponse

RemoveXattrResponse 定义 RemoveXattr 接口响应结果。

*无字段（空消息）。*

### MountDiskRequest

MountDiskRequest 定义 MountDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `mount_disk.disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | — |
| 2 | `read_only` | `bool` | singular | — | — |

### MountDiskResponse

MountDiskResponse 定义 MountDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `epoch` | `uint64` | singular | — | read_only=true 时为 0；read_only=false 时为递增后的权威 WriterEpoch (&gt; 0) |
| 2 | `root_inode` | [Inode](#inode) | singular | — | — |
| 3 | `entry_timeout_seconds` | `double` | singular | — | — |
| 4 | `attr_timeout_seconds` | `double` | singular | — | — |
| 5 | `mount_id` | `string` | singular | — | 本次在线挂载实例的唯一标识符（UUID 格式）。 |
| 6 | `lease_ttl_seconds` | `uint32` | singular | — | 在线挂载软租约的有效时长（秒）。 |

### RenewMountRequest

RenewMountRequest 定义 RenewMount 接口输入参数。

**消息级校验规则（protovalidate）：**

- CEL `renew_mount_epoch_matches_mode`: `this.read_only ? this.epoch == 0u : this.epoch > 0u`（epoch must be 0 when read_only is true, and &gt; 0 when read_only is false）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `renew_mount_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `mount_id` | `string` | singular | CEL `renew_mount_mount_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（mount_id must not be nil UUID）<br>`string`: `uuid=true` | 挂载实例唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 3 | `read_only` | `bool` | singular | — | 是否为只读挂载。 |
| 4 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 写挂载世代号（只读挂载必须为 0；可写挂载必须大于 0 且在 int64 正数范围内）。 |

### RenewMountResponse

RenewMountResponse 定义 RenewMount 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `lease_ttl_seconds` | `uint32` | singular | — | 续期后的在线挂载软租约有效时长（秒）。 |

### UnmountDiskRequest

UnmountDiskRequest 定义 UnmountDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `unmount_disk_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `mount_id` | `string` | singular | CEL `unmount_disk_mount_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（mount_id must not be nil UUID）<br>`string`: `uuid=true` | 挂载实例唯一标识符（UUID 格式，非全零 nil UUID）。 |

### UnmountDiskResponse

UnmountDiskResponse 定义 UnmountDisk 接口输出参数。

*无字段（空消息）。*

### Inode

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `gt=0` | 节点的全局唯一持久化标识。 |
| 2 | `generation` | `uint64` | singular | — | Inode 代数版本，用于标识 Inode 复用生命周期，防止陈旧引用。 |
| 3 | `attr` | [InodeAttribute](#inodeattribute) | singular | `required` | 节点的 POSIX 物理属性集合。 |

### InodeAttribute

InodeAttribute 包含遵循 POSIX 规范的节点物理属性。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `mode` | `uint32` | singular | `uint32`: `gt=0` | 文件的模式位（包含文件类型 S_IFREG, S_IFDIR 及权限位 0755, 0644 等）。 |
| 2 | `size` | `uint64` | singular | — | 文件物理大小（字节数）。 |
| 3 | `nlink` | `uint32` | singular | — | 硬链接数量。文件默认为 1；目录通常至少为 2；已解链但仍处于打开状态的孤儿节点允许为 0。 |
| 4 | `uid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属用户 ID。 |
| 5 | `gid` | `uint32` | singular | `uint32`: `lte=2147483647` | 所属组 ID。 |
| 6 | `rdev` | `uint32` | singular | — | 特殊设备号（仅当为设备文件时有效，常规文件为 0）。 |
| 7 | `blksize` | `uint32` | singular | — | 块大小（字节数，默认 65536 即 0x10000）。 |
| 8 | `blocks` | `uint64` | singular | — | 占用的 512 字节块数量。 |
| 9 | `atime` | `google.protobuf.Timestamp` | singular | `required` | 最后访问时间。 |
| 10 | `mtime` | `google.protobuf.Timestamp` | singular | `required` | 最后修改时间。 |
| 11 | `ctime` | `google.protobuf.Timestamp` | singular | `required` | 状态变更时间。 |
| 12 | `flags` | `uint32` | singular | `uint32`: `lte=2147483647` | 文件系统标志位（如 chattr 所设）。 |

### Disk

Disk 描述虚拟磁盘实体及其配额状态。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `disk_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 磁盘全局唯一标识符（UUID 字符串格式，非全零 nil UUID）。 |
| 2 | `name` | `string` | singular | CEL `disk_name_no_nul`: `!this.contains('\x00')`（name cannot contain NUL byte）<br>`string`: `maxBytes=255` | 磁盘名称（最大 255 字节，严禁包含空字节 '\x00'）。 |
| 3 | `root_ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 一对一绑定的根目录 Inode 编号（必须在 int64 正数区间内）。 |
| 4 | `max_bytes` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 容量配额上限字节数（必须在 int64 正数区间内）。 |
| 5 | `used_bytes` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 磁盘实时已用逻辑字节数。 |
| 6 | `used_inodes` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 磁盘实时有效 Inode 数量。 |
| 7 | `created_at` | `google.protobuf.Timestamp` | singular | `required` | 磁盘创建时间戳（必填）。 |
| 8 | `updated_at` | `google.protobuf.Timestamp` | singular | `required` | 磁盘最后更新时间戳（必填）。 |
| 9 | `parent_disk_id` | `string` | optional | CEL `disk_parent_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（parent_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 派生源父磁盘唯一标识符（根磁盘未设置；派生磁盘为合法非全零 nil UUID）。 |

### CreateDiskRequest

CreateDiskRequest 定义 CreateDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `name` | `string` | singular | CEL `create_disk_name_no_nul`: `!this.contains('\x00')`（name cannot contain NUL byte）<br>`string`: `maxBytes=255` | 磁盘名称（最大 255 字节，严禁包含空字节 '\x00'）。 |
| 2 | `max_bytes` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 容量配额上限字节数（必须在 int64 正数区间内）。 |

### CreateDiskResponse

CreateDiskResponse 定义 CreateDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk` | [Disk](#disk) | singular | `required` | 创建成功的虚拟磁盘实体。 |

### GetDiskRequest

GetDiskRequest 定义 GetDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `get_disk_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### GetDiskResponse

GetDiskResponse 定义 GetDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk` | [Disk](#disk) | singular | `required` | 查询到的虚拟磁盘实体。 |

### ListDisksRequest

ListDisksRequest 分页查询虚拟磁盘列表请求。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `page_size` | `uint32` | singular | `uint32`: `lte=1000` | 单页最大返回条数（为 0 时默认取 50，最大不超过 1000）。 |
| 2 | `page_token` | `string` | singular | — | 游标分页令牌（为空表示从第一页开始查询）。 |

### ListDisksResponse

ListDisksResponse 分页查询虚拟磁盘列表响应。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disks` | [Disk](#disk) | repeated | — | 当前页虚拟磁盘列表（按 created_at DESC, disk_id DESC 排序）。 |
| 2 | `next_page_token` | `string` | singular | — | 下一页游标令牌（为空字符串表示已到达最后一页）。 |

### UpdateDiskRequest

UpdateDiskRequest 定义 UpdateDisk 接口输入参数。

**消息级校验规则（protovalidate）：**

- CEL `update_disk_has_update`: `has(this.name) || has(this.max_bytes)`（at least one of name or max_bytes must be set）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `update_disk_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `name` | `string` | optional | CEL `update_disk_name_no_nul`: `!this.contains('\x00')`（name cannot contain NUL byte）<br>`string`: `maxBytes=255` | 待更新的磁盘名称（最大 255 字节，严禁包含空字节 '\x00'）。 |
| 3 | `max_bytes` | `uint64` | optional | `uint64`: `lte=9223372036854775807`, `gt=0` | 待更新的容量配额上限（必须在 int64 正数区间内）。 |

### UpdateDiskResponse

UpdateDiskResponse 定义 UpdateDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk` | [Disk](#disk) | singular | `required` | 更新后的虚拟磁盘实体。 |

### DeleteDiskRequest

DeleteDiskRequest 定义 DeleteDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `delete_disk_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### DeleteDiskResponse

DeleteDiskResponse 定义 DeleteDisk 接口输出参数。

*无字段（空消息）。*

### ForkDiskRequest

ForkDiskRequest 定义 ForkDisk 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `source_disk_id` | `string` | singular | CEL `fork_disk_source_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（source_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 派生源虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `name` | `string` | singular | CEL `fork_disk_name_no_nul`: `!this.contains('\x00')`（name cannot contain NUL byte）<br>`string`: `maxBytes=255` | 新建派生磁盘名称（最大 255 字节，严禁包含空字节 '\x00'）。 |
| 3 | `max_bytes` | `uint64` | optional | `uint64`: `lte=9223372036854775807`, `gt=0` | 可选的新盘容量配额上限字节数（省略时继承源盘配额；指定时必须在 int64 正数区间内）。 |

### ForkDiskResponse

ForkDiskResponse 定义 ForkDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk` | [Disk](#disk) | singular | `required` | 派生成功的虚拟磁盘实体。 |

### StatFSRequest

StatFSRequest 定义 StatFS 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点 Inode 编号（必须在 int64 正数区间内）。 |
| 2 | `disk_id` | `string` | singular | CEL `statfs_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |

### StatFSResponse

StatFSResponse 定义 StatFS 接口输出参数，对齐 Linux FUSE FUSE_STATFS = 17 协议与 4096 字节块统计语义。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `total_bytes` | `uint64` | singular | — | 磁盘总容量字节数。 |
| 2 | `used_bytes` | `uint64` | singular | — | 当前已使用容量字节数。 |
| 3 | `avail_bytes` | `uint64` | singular | — | 当前剩余可用容量字节数。 |
| 4 | `bsize` | `uint32` | singular | — | 文件系统基础块大小（字节，固定为 4096）。 |
| 5 | `frsize` | `uint32` | singular | — | 文件系统基本分配单元大小（字节，固定为 4096）。 |
| 6 | `blocks` | `uint64` | singular | — | 总数据块数量（基于 4096 字节块换算，至少为 1）。 |
| 7 | `bfree` | `uint64` | singular | — | 空闲数据块数量。 |
| 8 | `bavail` | `uint64` | singular | — | 非特权用户可用数据块数量。 |
| 9 | `files` | `uint64` | singular | — | 当前已使用的 Inode 数量（反映 used_inodes）。 |
| 10 | `ffree` | `uint64` | singular | — | 空闲 Inode 数量（不设限时填 0）。 |
| 11 | `namelen` | `uint32` | singular | — | 文件名最大长度（字节，固定为 255）。 |

### ReclaimOrphanRequest

ReclaimOrphanRequest 定义 ReclaimOrphan 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `reclaim_orphan_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | — |
| 2 | `epoch` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | — |
| 3 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | — |

### ReclaimOrphanResponse

ReclaimOrphanResponse 定义 ReclaimOrphan 接口输出参数。

*无字段（空消息）。*

### DiffNodeSummary

DiffNodeSummary 描述差异或冲突条目的节点关键元数据摘要。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `ino` | `uint64` | singular | — | 节点 Inode 编号。 |
| 2 | `mode` | `uint32` | singular | — | POSIX 文件模式位（包含文件类型与权限位）。 |
| 3 | `size` | `uint64` | singular | — | 节点逻辑字节大小。 |

### DiffEntry

DiffEntry 描述单条路径的差异与冲突详情。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `path` | `bytes` | singular | — | 变更或冲突项的相对路径字节序列。 |
| 2 | `kind` | [DiffKind](#diffkind) | singular | — | 变更类型。 |
| 3 | `conflict` | [ConflictKind](#conflictkind) | singular | — | 冲突类型（无冲突时为 CONFLICT_KIND_UNSPECIFIED）。 |
| 4 | `base` | [DiffNodeSummary](#diffnodesummary) | optional | — | 共同祖先切面中的节点摘要（若存在）。 |
| 5 | `target` | [DiffNodeSummary](#diffnodesummary) | optional | — | 目标磁盘中的节点摘要（若存在）。 |
| 6 | `source` | [DiffNodeSummary](#diffnodesummary) | optional | — | 源磁盘中的节点摘要（若存在）。 |

### DiffSummary

DiffSummary 汇总差异比较的各项条目统计计数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `added` | `uint64` | singular | — | 新增条目数量。 |
| 2 | `modified` | `uint64` | singular | — | 修改条目数量。 |
| 3 | `deleted` | `uint64` | singular | — | 删除条目数量。 |
| 4 | `conflicts` | `uint64` | singular | — | 冲突条目数量。 |

### DiffDiskRequest

DiffDiskRequest 定义 DiffDisk 接口输入参数。

**消息级校验规则（protovalidate）：**

- CEL `diff_disk_source_not_equal_target`: `has(this.target_disk_id) ? this.source_disk_id != this.target_disk_id : true`（source_disk_id and target_disk_id must be different）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `source_disk_id` | `string` | singular | CEL `diff_disk_source_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（source_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 源虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `target_disk_id` | `string` | optional | CEL `diff_disk_target_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（target_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 可选的目标虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID；省略时仅做两路差异或相对祖先比较）。 |
| 3 | `path_prefix` | `bytes` | singular | CEL `diff_disk_path_prefix_no_nul`: `!this.contains(b'\x00')`（path_prefix cannot contain NUL byte）<br>`bytes`: `maxLen=4096` | 可选的路径前缀过滤条件（最大 4096 字节，严禁包含空字节 '\x00'）。 |
| 4 | `conflicts_only` | `bool` | singular | — | 是否仅返回存在冲突的条目。 |
| 5 | `page_size` | `uint32` | singular | `uint32`: `lte=1000` | 单页最大返回条数（最大不超过 1000）。 |
| 6 | `page_token` | `string` | singular | CEL `diff_disk_page_token_no_nul`: `!this.contains('\x00')`（page_token cannot contain NUL byte）<br>`string`: `maxLen=4096` | 游标分页令牌（最大 4096 字符，严禁包含空字节 '\x00'）。 |

### DiffDiskResponse

DiffDiskResponse 定义 DiffDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `lca_commit_id` | `string` | singular | — | 最近共同祖先提交标识。 |
| 2 | `summary` | [DiffSummary](#diffsummary) | singular | — | 差异与冲突统计摘要。 |
| 3 | `entries` | [DiffEntry](#diffentry) | repeated | — | 当前页差异条目列表。 |
| 4 | `next_page_token` | `string` | singular | — | 下一页游标令牌（为空字符串表示已到达最后一页）。 |

### ReadCommitNodeRequest

ReadCommitNodeRequest 定义 ReadCommitNode 接口输入参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk_id` | `string` | singular | CEL `read_commit_node_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（disk_id must not be nil UUID）<br>`string`: `uuid=true` | 所属虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `commit_id` | `string` | singular | CEL `read_commit_node_commit_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（commit_id must not be nil UUID）<br>`string`: `uuid=true` | 目标不可变提交切面唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 3 | `ino` | `uint64` | singular | `uint64`: `lte=9223372036854775807`, `gt=0` | 目标节点 Inode 编号（必须在 int64 正数区间内）。 |
| 4 | `offset` | `uint64` | singular | `uint64`: `lte=9223372036854775807` | 读取起始偏移量（必须在 int64 非负区间内）。 |
| 5 | `size` | `uint32` | singular | `uint32`: `lte=4194304` | 单次请求读取的字节大小上限（最大 4 MiB，即 4194304 字节）。 |

### ReadCommitNodeResponse

ReadCommitNodeResponse 定义 ReadCommitNode 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `mode` | `uint32` | singular | — | 节点 POSIX 文件模式位。 |
| 2 | `file_size` | `uint64` | singular | — | 文件总字节大小。 |
| 3 | `data` | `bytes` | singular | — | 本次读取的文件内容字节切片。 |
| 4 | `eof` | `bool` | singular | — | 是否已读取至文件末尾。 |
| 5 | `symlink_target` | `bytes` | singular | — | 若目标节点为符号链接，返回其目标路径字节序列。 |

### MergeConflict

MergeConflict 描述合并过程中发现的单个路径冲突详情。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `path` | `bytes` | singular | — | 冲突路径字节序列。 |
| 2 | `kind` | [ConflictKind](#conflictkind) | singular | — | 冲突类型。 |
| 3 | `base` | [DiffNodeSummary](#diffnodesummary) | optional | — | 共同祖先切面中的节点摘要（若存在）。 |
| 4 | `target` | [DiffNodeSummary](#diffnodesummary) | optional | — | 目标磁盘中的节点摘要（若存在）。 |
| 5 | `source` | [DiffNodeSummary](#diffnodesummary) | optional | — | 源磁盘中的节点摘要（若存在）。 |

### MergeConflictReport

MergeConflictReport 汇总合并冲突报告。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `lca_commit_id` | `string` | singular | — | 最近共同祖先提交标识。 |
| 2 | `total_conflicts` | `uint64` | singular | — | 冲突总数（可能大于当前响应携带的截断列表长度）。 |
| 3 | `conflicts` | [MergeConflict](#mergeconflict) | repeated | — | 冲突明细列表。 |
| 4 | `conflicts_truncated` | `bool` | singular | — | 冲突列表是否因数量过多而被截断。 |

### MergeDiskRequest

MergeDiskRequest 定义 MergeDisk 接口输入参数。

**消息级校验规则（protovalidate）：**

- CEL `merge_disk_source_not_equal_target`: `this.source_disk_id != this.target_disk_id`（source_disk_id and target_disk_id must be different）

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `source_disk_id` | `string` | singular | CEL `merge_disk_source_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（source_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 源虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID）。 |
| 2 | `target_disk_id` | `string` | singular | CEL `merge_disk_target_disk_id_not_nil`: `this != '00000000-0000-0000-0000-000000000000'`（target_disk_id must not be nil UUID）<br>`string`: `uuid=true` | 目标虚拟磁盘唯一标识符（UUID 格式，非全零 nil UUID，且不能与 source_disk_id 相同）。 |
| 3 | `strategy` | [MergeStrategy](#mergestrategy) | singular | `enum`: `definedOnly=true` | 全局默认冲突解决策略（UNSPECIFIED 服务端按 FAIL 语义处理）。 |
| 4 | `path_strategies` | map&lt;`string`, [MergeStrategy](#mergestrategy)&gt; | map | `map`: `max_pairs=4096`, Key: CEL `merge_disk_path_strategies_key_no_nul`: `!this.contains('\x00')`（path_strategies key cannot contain NUL byte）; `string`: `minLen=1`, `maxLen=4096`, Value: `enum`: `definedOnly=true`, `notIn=[0]` | 按路径前缀或精确路径覆盖的冲突解决策略映射。 |
| 5 | `dry_run` | `bool` | singular | — | 是否仅执行预演检查而不实际提交合并结果。 |
| 6 | `max_bytes` | `uint64` | optional | `uint64`: `lte=9223372036854775807`, `gt=0` | 可选的合并后目标盘容量配额上限字节数（指定时必须在 int64 正数区间内）。 |
| 7 | `force` | `bool` | singular | — | 是否忽略在线挂载软租约门禁强制执行合并。 |

### MergeDiskResponse

MergeDiskResponse 定义 MergeDisk 接口输出参数。

| 编号 | 字段名 | 类型 | 标签 | 校验规则 | 说明 |
| --- | --- | --- | --- | --- | --- |
| 1 | `disk` | [Disk](#disk) | optional | — | 合并后更新的目标虚拟磁盘实体（dry_run 或冲突中止时可能为空）。 |
| 2 | `lca_commit_id` | `string` | singular | — | 最近共同祖先提交标识。 |
| 3 | `merge_commit_id` | `string` | singular | — | 合并产生的新提交标识（dry_run 或冲突中止时为空）。 |
| 4 | `estimated_used_bytes` | `uint64` | singular | — | 预估或实际合并后的目标盘已用字节数。 |
| 5 | `target_max_bytes` | `uint64` | singular | — | 合并后目标盘的容量配额上限字节数。 |
| 6 | `conflict_report` | [MergeConflictReport](#mergeconflictreport) | optional | — | 冲突报告（无冲突或非冲突失败时可能为空）。 |

