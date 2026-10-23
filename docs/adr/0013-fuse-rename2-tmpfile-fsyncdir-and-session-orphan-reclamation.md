# 0013. FUSE Rename2、TmpFile、FsyncDir 与会话级孤儿节点兜底回收设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成目录树拓扑（ADR-0008）、文件句柄生命周期（ADR-0009）、目录流遍历（ADR-0010）、扩展属性（ADR-0011）与文件锁及会话租约（ADR-0012）后，对照 Linux 内核 FUSE 协议及主流生产级分布式文件系统（JuiceFS、Ceph）的元数据层边界，还需闭环最后四项纯元数据核心能力：
1. **`FUSE_RENAME2 (45)` 增强原子重命名**：现有 `Rename` 仅支持 `flags == 0` 的传统覆盖语义（ADR-0008），尚未支持 Linux `renameat2(2)` 的 `RENAME_NOREPLACE`（禁止覆盖）与 `RENAME_EXCHANGE`（双向原子交换），且两者在同路径/同 `Inode` 硬链接短路及祖先环检测上与传统 `Rename` 存在关键差异。
2. **`FUSE_TMPFILE (51)` 匿名临时文件创建与 `Link` 具名化闭环**：现代编译器、包管理器与事务型存储广泛使用 `open(O_TMPFILE)` 创建无目录项的匿名文件，写入完成后再通过 `linkat`（触发 `FUSE_LINK`）原子发布到目录树。需避免伪名 `Create + Unlink` 方案带来的二次 RPC 与父目录时间戳污染，并在支持非 `O_EXCL` 临时文件具名化的同时，防止已 `Unlink` 解链的孤儿文件被非法复活（演进 ADR-0008）。
3. **客户端崩溃后的 `Orphan Inode` 会话级兜底回收**：ADR-0012 已通过 `client_sessions` 租约表解决了客户端崩溃后的 `FileLock` 泄漏，但 ADR-0009 的 `file_handles` 表尚未关联 `session_id`。若客户端在持有 `Orphan Inode`（`unlink-whilst-open`、`rmdir-whilst-open` 或 `TmpFile`）句柄期间异常断连而未发送 `Release` / `ReleaseDir`，孤儿节点将永久驻留；同时在写事务内顺带清理过期会话孤儿节点时，必须防止跨 `Inode` 逆序加锁引发数据库死锁。
4. **`FUSE_FSYNCDIR (30)` 目录句柄同步**：应用在修改目录树后常对父目录 `fd` 调用 `fsync(2)`，需在目录句柄体系（ADR-0010）中补齐与文件 `Fsync` 对称的 `FsyncDir` 契约。

## 决策
1. **在 `Rename` 中内联扩展 `FUSE_RENAME2 (45)`（`RENAME_NOREPLACE`、`RENAME_EXCHANGE` 与 `RENAME_WHITEOUT`）**：
   - **契约与标志位白名单**：不拆分独立 RPC，直接在 `RenameRequest` 与 `meta.RenameParams` 中新增 `uint32 flags = 5`。定义平台无关常量 `RenameNoReplace = 0x1`（`1 << 0`）、`RenameExchange = 0x2`（`1 << 1`）、`RenameWhiteout = 0x4`（`1 << 2`）。传入 `RenameNoReplace | RenameExchange` 互斥组合或任何未知标志位静态返回 `ErrInvalidArgument`（`EINVAL`）；传入 `RenameWhiteout (0x4)` 返回 `ErrNotSupported`（`EOPNOTSUPP`）。
   - **同路径与同 `Inode` 硬链接边界（演进 ADR-0008）**：
     - 当 `flags == RenameNoReplace` 时：只要源与目标 `Entry` 均存在——**包括源与目标为同一路径（`old_parent_ino == new_parent_ino && old_name == new_name`）或指向同一 `Inode` 的不同硬链接（`old_ino == new_ino`）**——一律返回 `ErrAlreadyExists`（`EEXIST`），不执行无副作用成功短路。
     - 当 `flags == RenameExchange` 时：要求源与目标 `Entry` 均必须存在，任一不存在即返回 `ErrNotFound`（`ENOENT`）；当两者均存在且为同一路径或指向同一 `Inode` 时，无副作用直接返回成功。
   - **`RenameExchange` 异节点原子交换状态机**：
     - **全局升序加锁**：预读源与目标 `Entry`（任一不存在则在锁定校验新旧父目录后返回 `ErrNotFound`），将 `old_parent_ino`、`new_parent_ino`、`old_ino`、`new_ino` 去重后按全局 `ino` 严格升序加 `FOR UPDATE` 行锁，并二次校验 `Entry` 防并发漂移。
     - **双向 `WITH RECURSIVE` 祖先环检测**：当 `old_parent_ino != new_parent_ino` 时，若 `old_ino` 为目录，校验 `new_parent_ino` 不是 `old_ino` 及其子孙；**若 `new_ino` 为目录，同步反向校验 `old_parent_ino` 不是 `new_ino` 及其子孙**，命中环路即返回 `ErrInvalidArgument`（`EINVAL`）。
     - **`entries` 原地互换与时间戳/`nlink`/`parent_ino` 矩阵**：不限定源与目标的文件类型组合，亦不要求目录为空；原地将 `(old_parent_ino, old_name)` 的 `ino` 更新为 `new_ino`、将 `(new_parent_ino, new_name)` 的 `ino` 更新为 `old_ino`；同时刷新 `old_ino` 与 `new_ino` 的 `ctime`（跨父目录交换目录时同步更新被移动目录的 `inodes.parent_ino`，双方自身 `nlink` 不变）。若同父目录或跨父目录但双方同为目录/同为非目录，新旧父目录 `nlink` 均不变，仅刷新 `mtime` 与 `ctime`；若跨父目录且一方为目录、另一方为非目录，则移入目录的父目录需校验 `nlink < MaxNlink`（否则返回 `ErrTooManyLinks`），随后移出目录的父目录 `nlink - 1`、移入目录的父目录 `nlink + 1`，并刷新各自 `mtime` 与 `ctime`。

2. **原生匿名 `TmpFile`（`FUSE_TMPFILE = 51`）直建与一次性 `Link` 具名化（演进 ADR-0008）**：
   - **独立 `TmpFile` RPC 与入参不变量**：新增 `TmpFile(TmpFileRequest) returns (TmpFileResponse)`（请求含 `parent_ino`、`mode`、`uid`、`gid`、`flags`、`optional session_id`；响应与 `CreateResponse` 对齐）。强制要求 `flags & O_ACCMODE` 为 `O_WRONLY` 或 `O_RDWR`（传入 `O_RDONLY` 返回 `ErrInvalidArgument` / `EINVAL`）；`mode & S_IFMT` 仅允许为 `0` 或 `S_IFREG`，归一化为 `S_IFREG | (mode & 0o7777)`。
   - **严禁 `Create` 与 `Open` 伪造 `O_TMPFILE` 来源**：`Create` 与 `Open` 在入参校验阶段强制拒绝包含 `O_TMPFILE (0o20000000)` 标志位的 `flags`（直接返回 `ErrInvalidArgument` / `EINVAL`），确保 `file_handles.flags` 中的 `O_TMPFILE` 位仅能由 `TmpFile` 唯一写入，杜绝普通具名文件通过伪造 `O_TMPFILE` 在 `Unlink` 后被 `Link` 非法复活，以及杜绝 `TmpFile(O_EXCL)` 节点通过二次 `Open(O_TMPFILE)` 绕过独占限制。
   - **单事务直建 `nlink = 0` 节点**：在读写事务中锁定父目录 `parent_ino`（校验存在、为 `S_IFDIR` 且 `nlink > 0`；若父目录含 `S_ISGID` 则继承父目录 `gid`），直接插入 `nlink = 0` 的常规文件 `Inode`（**不写入 `entries` 表，亦不刷新父目录 `mtime` / `ctime`**），并在 `file_handles` 表分配初始句柄（落库 `flags` 强制置位平台无关常量 `O_TMPFILE = 0o20000000`，即 `params.Flags | O_TMPFILE`）。
   - **`Link` 具名化准入与防二次复活**：当 `Link` 锁定源 `Inode` 发现 `nlink == 0` 时，查询该 `Inode` 是否存在有效（所属会话未过期）的 `FileHandle` 满足 `(flags & O_TMPFILE) != 0 && (flags & O_EXCL) == 0`：
     - 若不满足（如普通被 `Unlink` 解链的孤儿文件、创建时携带了 `O_EXCL` 的 `TmpFile`、或句柄已关闭/过期），继续返回 `ErrNotFound`（`ENOENT`）；
     - 若满足，则允许插入目标 `Entry`、将源 `Inode` 的 `nlink` 递增为 `1` 并刷新 `ctime`，**且在同一事务内清除该 `Inode` 关联 `file_handles.flags` 中的 `O_TMPFILE` 位（`flags = flags & ~O_TMPFILE`）**。一旦具名化后再次被 `Unlink` 降回 `nlink == 0`，其 `O_TMPFILE` 资格已被消费，严禁二次复活。

3. **`file_handles` 会话租约对称绑定、独立 `RenewSession` 续期与 `FOR UPDATE SKIP LOCKED` 无死锁孤儿回收（演进 ADR-0009 / ADR-0012）**：
   - **数据库迁移与向后兼容契约**：新增迁移为 `file_handles` 表添加可空外键列 `session_id BIGINT REFERENCES client_sessions(session_id) ON DELETE CASCADE` 及索引 `idx_file_handles_session_id ON file_handles(session_id)`。在 `Create`、`TmpFile`、`Open`、`OpenDir`、`Fsync`、`FsyncDir`、`ReadDir`、`ReadDirPlus`、`ReleaseDir`、`Fallocate` 请求中增加 `optional uint64 session_id`（未传入时落库 `NULL`，保持存量无会话调用 100% 向后兼容）。
   - **句柄会话归属严格对称匹配**：`GetFileHandle` 与 `DeleteFileHandle` 强制要求请求的 `req_session_id` 与句柄持久化的 `fh.session_id` 严格对称匹配（`fh.session_id IS NOT DISTINCT FROM req_session_id`）：当传入非空 `req_session_id` 时，要求 `fh.session_id = req_session_id` 且关联 `client_sessions.expires_at > clock_timestamp()`；当未传 `req_session_id`（`NULL`）时，仅允许操作 `fh.session_id IS NULL` 的无会话句柄。任何跨会话访问、有会话访问无会话句柄、或无会话访问有会话句柄均返回 `ErrBadFileHandle`（`EBADF`）。
   - **读写续期顺序、只读过滤与独立 `RenewSession` 保活**：
     - 分配新 `FileHandle` 的写接口（`Create`、`TmpFile`、`Open`、`OpenDir`）携带非空 `session_id` 时，先调用 `renewClientSession` 确保会话记录存在，再插入 `file_handles`；
     - 操作已有 `FileHandle` 的写接口（`Flush`、`Release`、`ReleaseDir`、`Fallocate`、`SetLk`、`SetLkw`）携带非空 `session_id` 时，**必须先通过 `GetFileHandle` / `DeleteFileHandle` 校验句柄存在、归属当前 `session_id` 且会话未过期，校验通过后再调用 `renewClientSession` 续期**，严防已过期会话通过操作失效句柄“诈尸”自救；
     - 只读路径接口（`Fsync`、`FsyncDir`、`ReadDir`、`ReadDirPlus`、`GetLk`）保持只读事务不写库续期，通过 `GetFileHandle` 校验句柄归属与会话未过期（过期或不匹配返回 `ErrBadFileHandle` / `EBADF`）；
     - 新增独立 `RenewSession(RenewSessionRequest) returns (RenewSessionResponse)` RPC（入参 `uint64 session_id`，响应 `google.protobuf.Timestamp expires_at`），供长期只读或空闲持有句柄的客户端显式续期租约并顺带触发过期会话与孤儿节点清理。
   - **`FOR UPDATE SKIP LOCKED` 非阻塞物理回收**：由于 `renewClientSession` 调用点可能已持有当前请求目标 `Inode` 的行级排他锁，在清理过期 `client_sessions`（级联删除过期 `file_handles`）后，通过 `SELECT i.ino FROM inodes i WHERE i.nlink = 0 AND NOT EXISTS (...) FOR UPDATE SKIP LOCKED` 非阻塞锁定所有无剩余有效 `FileHandle` 的孤儿 `Inode` 并执行物理删除。被其他并发事务锁定的孤儿节点直接跳过，从根源上消除跨 `Inode` 锁等待环（SQLSTATE `40P01`）。

4. **`FUSE_FSYNCDIR (30)` 目录同步契约**：
   - 新增 `FsyncDir(FsyncDirRequest) returns (FsyncDirResponse)` RPC（字段：`uint64 ino`、`uint64 fh`、`bool datasync`、`optional uint64 session_id`）。
   - 在 `RepeatableRead` 只读事务中依次校验：`GetInode(ino)`（不存在返回 `ErrNotFound` / `ENOENT`；非 `S_IFDIR` 返回 `ErrNotDir` / `ENOTDIR`，与文件 `Fsync` 对目录返回 `ErrIsDir` 严格对称）$\rightarrow$ `GetFileHandle(fh, ino)`（不存在或所属会话已过期返回 `ErrBadFileHandle` / `EBADF`）。校验通过（含 `nlink == 0` 的 `rmdir-whilst-open` 孤儿目录）后幂等返回成功。

## 结果
- **积极影响**：在不破坏任何存量 RPC 签名与测试契约的前提下，完整闭环了 `FUSE_RENAME2 (45)`、`FUSE_TMPFILE (51)`、`FUSE_FSYNCDIR (30)` 及客户端断连后的 `Orphan Inode` 自动物理回收；通过 `O_TMPFILE` 标志位一次性擦除机制与 `FOR UPDATE SKIP LOCKED` 非阻塞回收，同时保障了 POSIX 链接安全不变量与高并发无死锁特性。
- **代价考量**：`GetFileHandle` 与 `HasFileHandlesByIno` 增加了对 `client_sessions` 的主键 `LEFT JOIN`，且 `renewClientSession` 增加了对无句柄 `nlink == 0` 孤儿节点的 `SKIP LOCKED` 清理查询；配合 `inodes(ino) WHERE nlink = 0` 部分索引（Partial Index），扫描范围严格收敛于极少数孤儿节点，运行时开销极低。
