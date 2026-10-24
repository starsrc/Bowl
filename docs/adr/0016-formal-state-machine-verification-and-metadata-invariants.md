# 0016. 元数据层形式化状态机对账验证、数学不变量契约与双状态机边界设计

## 状态
已采纳 (accepted) — 配套验证 [ADR-0008](0008-fuse-directory-topology-locking-and-orphan-lifecycle.md)、[ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md)、[ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md)

## 背景
Storage 元数据引擎已构建起涵盖目录树拓扑、硬链接计数（`nlink`）、匿名临时文件（`TmpFile`）具名化、孤儿节点（`Orphan Inode`）延迟回收、客户端会话租约、文件锁（POSIX 字节区间锁与 BSD `flock`）以及商业化磁盘容量配额账本（`Disk`）的状态机。在推进生产级可靠性验证时，面临三项核心权衡：
1. **固定剧本单测的组合盲区与外部建模语言的脱节风险**：传统手工单元测试难以覆盖“跨目录 `Rename`（含 `RENAME_EXCHANGE`）+ `TmpFile` 一次性 `Link` + `Open(O_TRUNC)` + 会话超期由 `RenewSession` 同步触发孤儿物理回收”等深层多步组合与并发交错。若采用独立于代码的外部建模语言（如 Coq 或 TLA+），验证对象是手工抽象公式而非真实运行的 Go 事务与 PostgreSQL SQL 语句，无法拦截 SQL 查询条件遗漏或底表脏数据残留。
2. **数据面解耦与状态空间控制**：在元数据状态机验证中，文件数据面（Chunk/Slice）可完全抽象为常规文件 `Inode` 的 `size` 标量属性，无需等待数据层落地即可独立验证元数据与配额账本的一致性。
3. **36 个公开接口的状态域正交切分**：`StorageService` 共包含 36 个公开 RPC 接口。若对纯只读接口或单节点简单键值接口（`Xattr`）盲目建模，或将正交的“目录树与配额账本”和“文件锁区间与等待队列”混入同一状态机，会降低随机碰撞密度并增加故障定位成本。

## 决策
1. **基于纯 Go 内存参考模型（Reference Oracle）与底表全量审计的状态机属性验证**：
   - 基于 Go 标准库 `math/rand/v2` 构建零外部依赖的确定性随机状态机驱动器，支持固定 64 位随机种子（`Seed`）重放与中文操作轨迹输出。
   - 采用**“公开接口返回值对拍 + PostgreSQL 底表快照不变量扫描”**双层核验机制：每步随机操作同时作用于内存单线程参考模型与真实 PostgreSQL `meta.Manager`；除核对返回值与 POSIX 错误码一致外，直接按测试所属 `root_ino` 扫描数据库底表（`disks`、`inodes`、`entries`、`file_handles`、`client_sessions`、`file_locks`、`file_lock_waiters`），校验下述数学不变量。

2. **36 个公开 RPC 接口的三档分流与双正交状态机边界**：
   - **主状态机（目录树拓扑、孤儿生命周期与磁盘配额账本）**：
     - **核心写接口（19 个）**：`CreateDisk`、`UpdateDisk`、`DeleteDisk`、`Mkdir`、`Mknod`、`Symlink`、`Create`、`TmpFile`、`Link`、`Unlink`、`Rmdir`、`Rename`、`Open`、`OpenDir`、`SetAttr`、`Fallocate`、`Release`、`ReleaseDir`、`RenewSession`。
     - **只读与同步查账探针（10 个）**：`Lookup`、`GetAttr`、`Readlink`、`ReadDir`、`ReadDirPlus`、`GetDisk`、`StatFS`、`Fsync`、`FsyncDir`，以及不携带 `lock_owner` 且不携带 `session_id` 的只读 `Flush`。
   - **副状态机（文件锁区间裂解合并、双命名空间与死锁检测）**：
     - **锁写操作路径（4 条，含 2 个专属 RPC 与 2 个复用 RPC）**：专属 RPC `SetLk`、`SetLkw`，以及复用主状态机的带 `lock_owner`（或 `session_id`）的 `Flush` 与带 `FuseReleaseFlockUnlock` 的 `Release`。
     - **锁只读查账探针（1 个）**：`GetLk`。
   - **豁免范围（4 个）**：单节点扩展属性接口（`SetXattr`、`GetXattr`、`ListXattr`、`RemoveXattr`）仅涉及单 `Inode` 内部 64KB 键值配额，与目录树、磁盘配额及文件锁正交，维持现有单元测试覆盖。（注：19 主写 + 10 主探针 + 2 专属锁写 + 1 锁探针 + 4 豁免 = 36 个公开 RPC 接口）。

3. **主状态机的 6 条核心数学不变量（Metadata & Quota Invariants）**：
   在单步状态机每一步提交后及多协程并发压测收敛后，数据库底表必须满足以下 6 条不变量：
   - **INV-1（目录树无环与父子双向一致）**：
     - 根目录（`ino == root_ino`）满足 `inodes.parent_ino == root_ino`，且在 `entries` 表中不存在指向 `root_ino` 的记录；
     - 任意非根存活目录（`ino != root_ino && nlink > 0`）在 `entries` 表中恰有唯一父目录记录，满足 `entries.parent_ino == inodes.parent_ino`，且沿 `entries` 向上回溯必在有限步内唯一到达其所属 `Disk` 的 `root_ino`，路径无环。
   - **INV-2（硬链接计数守恒）**：
     - 对任意非目录 `Inode`：`inodes.nlink == count(entries WHERE ino = i.ino)`；
     - 对任意目录 `Inode`：若为存活目录（含 `root_ino`），则 `inodes.nlink == 2 + count(子目录 entries)`；若为已移除的孤儿目录（`rmdir-whilst-open`），则 `inodes.nlink == 0` 且名下不存在任何子 `entries`。
   - **INV-3（孤儿节点生命周期与清扫收敛）**：
     - 任意 `inodes.nlink == 0` 的 Orphan Inode 在解链或 `TmpFile` 创建事务提交时刻，必在 `file_handles` 表中存在至少一条关联句柄（无会话句柄 `session_id IS NULL`，或绑定未超期会话 `s.expires_at > clock_timestamp()`）；
     - 考虑到会话过期采用惰性清理（Lazy Reclamation）且 `DeleteUnreferencedOrphanInodes` 使用 `FOR UPDATE SKIP LOCKED` 规避跨节点锁等待，在无并发事务持锁且显式执行 `RenewSession` 同步清扫后，底表中不存在任何无有效句柄（即既无 `session_id IS NULL` 句柄、亦无未过期会话句柄）的 `nlink == 0` 孤儿节点。
   - **INV-4（磁盘容量与节点配额账本守恒）**：对任意存活 `Disk`（绑定 `root_ino`）：
     - `disks.used_bytes == sum(inodes.size WHERE root_ino = disks.root_ino AND (mode & S_IFMT) == S_IFREG)`（仅统计常规文件 `S_IFREG` 的字节大小，涵盖具名常规文件与尚未物理销毁的孤儿常规文件；符号链接 `S_IFLNK` 的 `size` 不计入 `used_bytes`）；
     - `disks.used_inodes == count(inodes WHERE root_ino = disks.root_ino)`（涵盖根目录、所有存活节点与尚未物理销毁的孤儿节点）；
     - 恒满足 `0 <= disks.used_bytes <= disks.max_bytes` 与 `disks.used_inodes >= 1`；`UpdateDisk` 当目标 `max_bytes < used_bytes` 时必拒绝缩容并返回 `ErrInvalidArgument`（`EINVAL`）；当 `DeleteDisk` 执行后，底表中归属该 `root_ino` 的 `inodes`、`entries`、`file_handles`、`file_locks` 记录数恒为 `0`。
   - **INV-5（跨盘拓扑隔离）**：对 `entries` 表中的任意绑定关系 `(parent_ino, child_ino)`，恒满足 `parent.root_ino == child.root_ino`；对非目录源节点发起跨 `Disk` 的 `Link`（注：目录源节点优先按 POSIX 拦截返回 `EPERM`），或发起跨 `Disk` 的 `Rename`（含 `RENAME_NOREPLACE` 与 `RENAME_EXCHANGE`），必被拦截并返回 `ErrCrossDevice`（`EXDEV`）。
   - **INV-6（匿名临时文件一次性具名化）**：常规文件句柄的 `O_TMPFILE` 标志位仅能由 `TmpFile` 写入（`Create` 与 `Open` 强制拒绝携带 `O_TMPFILE` 的入参）；一旦非 `O_EXCL` 的 `TmpFile` 通过 `Link` 成功具名化（`nlink: 0 -> 1`），其关联 `file_handles.flags` 中的 `O_TMPFILE` 位必在同一事务内被清零；任何曾具名化后被 `Unlink` 解链至 `nlink == 0` 的节点，后续调用 `Link` 必返回 `ErrNotFound`（`ENOENT`）。

4. **副状态机的 4 条文件锁数学不变量（File Lock Invariants）**：
   - **LOCK-INV-1（POSIX 字节区间规整与互斥）**：在同一 `(ino, lk_flags = 0)` 下，任意两条不同 `(session_id, lock_owner)` 的有效活跃锁（所属会话未过期）若字节区间 `[start, end]` 相交，则两者必同为读锁（`F_RDLCK`）；同一 `(session_id, ino, lock_owner)` 下的活跃锁区间必定互不相交，且不存在相邻（`prev.end < OffsetMax && prev.end + 1 == next.start`）且同类型的未合并碎片区间。
   - **LOCK-INV-2（POSIX 与 BSD `flock` 双命名空间正交）**：在所属会话未过期且目标 `Inode` 未被物理销毁的前提下，`lk_flags == 0` 与 `lk_flags == FuseLkFlock` 的加锁、冲突检测与显式解锁互不干扰：携带 `lock_owner` 的 `Flush` 仅清除对应 `(session_id, ino, lock_owner)` 的 POSIX 记录锁而不删除 `flock`；携带 `FuseReleaseFlockUnlock` 的 `Release` 仅清除对应 `(session_id, ino, fh)` 的 `flock` 而不删除其他句柄持有的 POSIX 记录锁。
   - **LOCK-INV-3（同命名空间有界死锁检测）**：在同一 `(ino, lk_flags)` 锁命名空间下，当阻塞锁请求 `SetLkw` 遇到活跃锁或更早等待者冲突需进入等待队列时，若基于当前活跃锁持有者与 `PENDING` 等待者构建的局部等待有向图存在从请求持有者回到自身的环路（`hasDeadlockCycle`），必立即拒绝等待并返回 `ErrDeadlock`（`EDEADLK`），不将该成环请求写入 `file_lock_waiters`。
   - **LOCK-INV-4（取消清理与租约级联收敛）**：在数据库连接正常的前提下，任何因上下文取消（`ErrInterrupted` / `EINTR`）退出的 `SetLkw` 请求在完成取消清理（`cleanupCancelledWaiter`）后，在 `file_lock_waiters` 中不残留该 `(session_id, request_id)` 的 `PENDING` 记录，且不在 `file_locks` 中产生未授锁的悬挂锁；当会话过期并经 `RenewSession` 同步清理后，该过期会话名下的 `file_locks` 与 `file_lock_waiters` 记录数恒为 `0`。

5. **短租约断连模拟、并发零死锁压测与双档位运行机制**：
   - **毫秒级短租约模拟崩溃**：在状态机测试中通过 `WithSessionLeaseTTL(20ms)` 配置短租约，在随机操作序列中注入等待与 `RenewSession` 调用，高频验证“客户端断连 -> 会话过期 -> `RenewSession` 同步触发孤儿节点物理回收与磁盘配额退还”的完整闭环。
   - **多协程并发冲突压测**：启动多个并发协程对共享目录树随机执行跨目录 `Rename`（覆盖与 `RENAME_EXCHANGE`）、`Link`、`Unlink`、`Open`/`Release`、`RenewSession` 及文件锁争抢，断言全程无未恢复的数据库死锁失败，且并发结束后执行一次 `RenewSession` 清扫，底表满足上述全部不变量。
   - **双档位命令接入**：
     - **日常轻量档（`task test` 默认执行）**：在 2~3 秒内完成多组固定步数的随机状态机对账与并发冲突检验；
     - **深度穷举档（`task test:formal`）**：通过环境变量开启多轮次长序列随机对账与高并发压测，用于发版前及专项回归验证。

## 结果
- **积极影响**：在不引入外部建模语言维护负担的前提下，直接对真实 Go 与 PostgreSQL 事务代码建立起可复现的状态机不变量防线；双状态机隔离设计兼顾了碰撞密度与可调试性；`ADR-0016` 为代码实现与后续审查提供了与源码严格对齐的不变量规范。
- **代价考量**：在 `services/storage/internal/meta/` 下新增状态机参考模型与底表审计测试代码，日常 `task test` 增加约 2~3 秒的 PostgreSQL 随机状态机回归耗时。
