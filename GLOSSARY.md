# Bowl 分布式存储系统

Bowl 负责管理分布式文件系统的 POSIX 元数据目录树、文件分块数据读写、商业化磁盘实例生命周期与容量配额核算。

## Storage 元数据引擎

**Inode (索引节点)**:
文件系统中标识文件或目录实体的唯一持久化数字标识符与元数据载体。
_Avoid_: FileID, NodeID, ObjectId, VNode

**Entry (目录项)**:
父目录下将名称映射到目标 Inode 的单向命名绑定关系。
_Avoid_: Dentry, FileItem, DirRecord, Record

**Lookup (路径查找)**:
根据父级 Inode 与目标名称，解析并获取对应 Entry 及其 Inode 属性的元数据检索操作。
_Avoid_: Find, Search, GetFile, Query

**GetAttr (属性获取)**:
根据目标 Inode 编号，直接检索并获取该 Inode 及其 Attribute 的元数据检索操作。
_Avoid_: Stat, GetFileInfo, FetchAttr, DescribeNode

**SetAttr (属性设置)**:
根据目标 Inode 编号（及可选关联 FileHandle），按需修改该 Inode 的可变 Attribute（如权限模式、文件大小、所属用户与组、访问与修改时间戳、文件系统标志位）并刷新状态变更时间的元数据变更操作。
_Avoid_: Chmod, Chown, Truncate, UpdateAttr, ModifyNode, PatchInode

**Mknod (节点创建)**:
在指定父目录 Inode 下创建非目录文件节点并绑定同名 Entry 的元数据变更操作。
_Avoid_: CreateNode, MakeNode, Touch, CreateFile

**Mkdir (目录创建)**:
在指定父目录 Inode 下创建目录节点、绑定同名 Entry 并递增父目录硬链接计数的元数据变更操作。
_Avoid_: CreateDir, MakeDirectory, NewFolder

**Unlink (目录项解链)**:
从指定父目录移除非目录 Entry 并递减目标 Inode 硬链接计数的元数据变更操作。
_Avoid_: DeleteFile, RemoveFile, Erase, DestroyNode

**Rmdir (空目录移除)**:
从指定父目录移除空目录 Entry、将目标目录硬链接计数归零并递减父目录硬链接计数的元数据变更操作。
_Avoid_: DeleteDir, RemoveFolder, PurgeDir

**Rename (目录项重命名)**:
在同一或不同父目录间原子性地移动、覆盖或交换 Entry 绑定关系（支持禁止覆盖与双向交换模式）的元数据变更操作。
_Avoid_: Move, Mv, Relocate, Rebind

**Link (硬链接创建)**:
在指定父目录下为已存在的非目录 Inode（或未携带独占标志的活跃匿名临时文件）绑定新 Entry 并递增其硬链接计数的元数据变更操作。
_Avoid_: HardLink, AddLink, AliasNode

**Symlink (符号链接创建)**:
在指定父目录下创建存储目标路径字节序列的符号链接 Inode 并绑定同名 Entry 的元数据变更操作。
_Avoid_: SoftLink, CreateShortcut, MakeAlias

**Readlink (符号链接读取)**:
根据目标符号链接 Inode 编号，直接读取其目标路径字节序列的元数据检索操作。
_Avoid_: GetSymlink, ResolveLink, ReadTarget

**Orphan Inode (孤儿节点)**:
所有 Entry 均已解链或自创建起未绑定 Entry（硬链接计数为零）但尚未被物理回收的 Inode 实体。
_Avoid_: ZombieNode, DanglingInode, DeletedFile

**Attribute (文件属性)**:
描述 Inode 的物理状态与内核控制参数的集合（如大小、权限模式、硬链接数、时间戳等）。
_Avoid_: Stat, Meta, Properties, Info

**Generation (代数)**:
用于标识 Inode 复用周期的递增版本号，防止客户端缓存引用已释放后重新分配的 Inode。
_Avoid_: Version, Revision, Epoch

**FileHandle (文件句柄)**:
文件系统为单次文件或目录打开会话分配的、受客户端会话租约管辖的唯一持久化数字标识符，用于在打开生命周期内关联后续的遍历、冲刷、同步与释放操作。
_Avoid_: Fd, FileDescriptor, SessionID, HandleID, StreamID

**Create (文件创建并打开)**:
在指定父目录下原子性地创建常规文件 Inode、绑定同名 Entry 并分配初始 FileHandle 的元数据变更与会话建立操作。
_Avoid_: CreateAndOpen, TouchAndOpen, NewFile

**TmpFile (匿名临时文件创建)**:
在指定父目录下直接创建未绑定 Entry（硬链接计数为零）的匿名常规文件 Inode 并分配初始 FileHandle 的元数据变更与会话建立操作。
_Avoid_: CreateTemp, MakeAnonFile, OpenTmpFile

**Open (文件打开)**:
针对已存在的非目录 Inode 建立打开会话并分配 FileHandle（可按需伴随文件截断）的生命周期操作。
_Avoid_: OpenFile, AcquireHandle, ConnectFile

**Flush (句柄冲刷)**:
在单个文件描述符关闭时触发、校验目标 Inode 与活跃 FileHandle 归属有效性的会话同步操作（不销毁 FileHandle）。
_Avoid_: Close, SyncHandle, Drain

**Fsync (文件同步)**:
确认目标 Inode 及其活跃 FileHandle 关联状态（支持完整同步与仅数据同步语义）已持久化的同步操作。
_Avoid_: Sync, CommitFile, FlushDisk

**Release (句柄释放)**:
销毁指定 FileHandle 并在目标 Inode 无剩余活跃 FileHandle 且处于 Orphan Inode 状态时触发物理回收的会话终止操作。
_Avoid_: CloseFile, DestroyHandle, FreeHandle

**OpenDir (目录打开)**:
针对已存在的目录 Inode 建立只读打开会话并分配 FileHandle 的生命周期操作。
_Avoid_: OpenDirectory, AcquireDirHandle, StartDirStream

**ReadDir (目录读取)**:
根据目录 Inode、活跃 FileHandle 与游标偏移量，合成 `.` / `..` 并分页拉取子项 Entry 列表的元数据检索操作。
_Avoid_: ListDir, ReadEntries, FetchDir, ScanDir

**ReadDirPlus (增强目录读取)**:
在 ReadDir 基础之上，内联预取并返回子项完整 Inode 属性与内核缓存时效的批量元数据检索操作。
_Avoid_: ReadDirWithAttr, BatchLookup, ScanDirPlus

**FsyncDir (目录同步)**:
确认目标目录 Inode 及其活跃目录 FileHandle 关联状态（支持完整同步与仅数据同步语义）已持久化的同步操作。
_Avoid_: SyncDir, CommitDir, FlushDir

**ReleaseDir (目录句柄释放)**:
销毁指定目录 FileHandle 并在目标目录 Inode 无剩余活跃 FileHandle 且处于 Orphan Inode 状态时触发物理回收的会话终止操作。
_Avoid_: CloseDir, DestroyDirHandle, FreeDirHandle

**Fallocate (空间预分配与打洞)**:
针对已打开常规文件 Inode 的活跃写句柄，执行空间预留扩展或指定区间零字节打洞并联动权限与时间戳的元数据操作。
_Avoid_: AllocSpace, PunchHole, Preallocate, ReserveBlocks

**Xattr (扩展属性)**:
关联于 Inode 的持久化名称-二进制值键值对元数据。
_Avoid_: ExtendedAttr, CustomMeta, Tag, Property, UserAttr

**SetXattr (扩展属性设置)**:
根据目标 Inode 编号、属性名称、二进制值与创建或替换语义标志，新增或更新目标 Inode 的 Xattr 并刷新状态变更时间的元数据变更操作。
_Avoid_: PutXattr, WriteXattr, UpdateXattr, AddTag

**GetXattr (扩展属性获取)**:
根据目标 Inode 编号与属性名称，检索并获取对应 Xattr 二进制值的元数据检索操作。
_Avoid_: ReadXattr, FetchXattr, QueryXattr

**ListXattr (扩展属性列表获取)**:
根据目标 Inode 编号，检索并按名称字节序升序返回该 Inode 绑定的全部 Xattr 名称列表的元数据检索操作。
_Avoid_: GetXattrs, ScanXattr, EnumXattr

**RemoveXattr (扩展属性移除)**:
根据目标 Inode 编号与属性名称，删除指定 Xattr 并刷新目标 Inode 状态变更时间的元数据变更操作。
_Avoid_: DeleteXattr, DelXattr, UnsetXattr, DropXattr

**FileLock (文件锁)**:
受会话租约管辖、关联于目标 Inode 的运行期同步契约，涵盖 POSIX 字节区间记录锁与 BSD `flock` 整文件锁，描述锁定字节区间 `[start, end]`、锁类型（读锁/写锁/解锁）及持锁进程标识符（`pid`）。
_Avoid_: RecordLock, RangeLock, Flock, LockEntry

**LockOwner (锁拥有者)**:
FUSE 内核为锁持有主体（POSIX 进程文件表或 BSD `flock` 打开文件表项）分配的、在会话生命周期内有效的 64 位不透明令牌。
_Avoid_: OwnerID, LockHolder, ClientID, HolderID

**GetLk (文件锁冲突探测)**:
根据目标 Inode、活跃 FileHandle、LockOwner 与期望的 FileLock 描述符，在对应锁命名空间内检索是否存在冲突锁并返回首个冲突锁详情（无冲突时返回解锁状态）的同步状态检索操作。
_Avoid_: QueryLock, CheckLock, TestLock, InspectLock

**SetLk (非阻塞文件锁设置)**:
根据目标 Inode、活跃 FileHandle、LockOwner 与目标 FileLock 描述符，在对应锁命名空间内非阻塞地申请、转换、裂解合并或释放文件锁，遇冲突立即拒绝的同步操作。
_Avoid_: TryLock, AcquireLock, LockNonBlock, UpdateLock

**SetLkw (阻塞文件锁设置)**:
在 SetLk 语义基础上，遇冲突锁时阻塞等待直至锁可用、检测到同节点死锁或收到中断信号的同步操作。
_Avoid_: WaitLock, LockBlock, AcquireLockWait

**RenewSession (会话续期)**:
客户端显式延长其会话租约有效期的保活操作，同步触发已超期会话清理与无剩余活跃 FileHandle 的 Orphan Inode 物理回收。
_Avoid_: Heartbeat, PingSession, KeepAlive, TouchSession

**CloseSession (会话关闭)**:
客户端在正常卸载退出时显式注销其 ClientSession 租约，并同步释放关联活跃 FileHandle、FileLock 与 Orphan Inode 的生命周期终止操作。
_Avoid_: EndSession, DestroySession, LogoutSession, DisconnectSession

**WatchSession (会话探活流)**:
客户端在挂载存续期间与服务端维持的双向长连接流，用于即时感知客户端进程终止并承载服务端按需发起的存活探测问答。
_Avoid_: PingStream, SessionChannel, MonitorSession, KeepAliveStream

**Disk (磁盘)**:
对外交付的商业化独立文件系统实例，由全局唯一 UUID 标识，一对一绑定一个独立的根目录 Inode，并承载容量配额上限与已用空间及节点数核算。
_Avoid_: Volume, Drive, MountPoint, Partition, Bucket

**CreateDisk (磁盘创建)**:
创建商业化独立 Disk 实例、分配全局唯一标识符与初始容量配额上限，并原子创建一对一绑定的根目录 Inode 的生命周期建立操作。
_Avoid_: NewDisk, InitVolume, ProvisionDisk, MakeDisk

**GetDisk (磁盘查询)**:
根据目标 Disk 唯一标识符，检索并获取其名称、根目录 Inode、容量配额上限、已用空间与节点数及时间戳详情的状态检索操作。
_Avoid_: DescribeDisk, FetchDisk, StatDisk, InspectDisk

**ListDisks (磁盘列表查询)**:
按创建时间倒序与游标分页规则，批量检索 Disk 实例及其配额状态列表的状态检索操作。
_Avoid_: ScanDisks, GetAllDisks, EnumDisks, QueryDisks

**UpdateDisk (磁盘更新)**:
根据目标 Disk 唯一标识符，按需修改其名称或调整容量配额上限（支持在线扩容与不低于当前已用空间的缩容）的生命周期变更操作。
_Avoid_: ResizeDisk, ExpandDisk, ShrinkDisk, ModifyDisk, PatchDisk

**DeleteDisk (磁盘删除)**:
在确认目标 Disk 无存活客户端挂载会话（对未超期会话经双向流探活无响应后回收）的前提下，级联销毁该 Disk 实例及其下属全部 Inode、FileHandle、FileLock 与 Slice 引用的生命周期释放操作。
_Avoid_: RemoveDisk, DestroyDisk, DropVolume, PurgeDisk

**StatFS (文件系统状态查询)**:
检索目标文件系统实例的总容量、已用空间、可用空间、Inode 节点统计及派生 POSIX 文件系统块指标的状态检索操作。
_Avoid_: GetDiskStat, StatVFS, DiskInfo, GetCapacity

## Storage 数据层

**Chunk (逻辑分块)**:
将常规文件连续字节空间按固定跨度切分的逻辑寻址单元，用于隔离并索引文件局部区间内的写入切片序列。
_Avoid_: Segment, Stripe, Partition, Part

**Slice (写入切片)**:
针对单个 Chunk 发起单次连续数据写入或空洞标记所生成的不可变逻辑区间描述符，按追加顺序叠加并在读取时裁剪合并为有效数据视图。
_Avoid_: Extent, Patch, Fragment, Delta

**Block (物理数据块)**:
将 Slice 的实际字节载荷按固定上限切分后持久化至底层存储介质的不可变物理存储单元。
_Avoid_: Blob, Page, Cell, Sector, ObjectFile

**Read (文件数据读取)**:
根据目标常规文件 Inode、活跃可读 FileHandle、字节偏移量与期望长度，合并目标区间内的 Slice 视图并组装返回对应字节流的读取操作。
_Avoid_: FetchData, GetBytes, ReadFile, PullStream

**Write (文件数据写入)**:
根据目标常规文件 Inode、活跃可写 FileHandle、字节偏移量与待写字节序列，将载荷持久化为底层 Block、追加 Slice 元数据映射并同步更新文件大小与配额的写入操作。
_Avoid_: PutData, WriteBytes, AppendFile, PushStream

**Compaction (切片合并)**:
将单个 Chunk 内累积的多层重叠或碎片化 Slice 读取重整为连续新 Slice 并替换旧映射序列的后台碎片整理操作。
_Avoid_: Defrag, MergeSlices, Squash, Pack

## Client 客户端挂载层

**ClientSession (客户端会话)**:
单次客户端挂载实例向服务端注册并按租约周期保活的运行期身份上下文，用于统一归属该挂载实例持有的全部活跃 FileHandle 与 FileLock。
_Avoid_: MountSession, Connection, ClientLease, PeerSession

**Mount (磁盘挂载)**:
根据目标 Disk 唯一标识符建立 ClientSession、将磁盘根目录重写映射为本地 FUSE 根节点并挂载至宿主机目标目录的生命周期建立操作。
_Avoid_: Attach, BindDisk, ConnectVolume

**Umount (磁盘卸载)**:
解除宿主机目标目录上的 FUSE 文件系统挂载绑定并终止对应客户端守护进程的生命周期释放操作（CLI 兼容 `unmount` 别名）。
_Avoid_: Detach, UnbindDisk, DisconnectVolume

