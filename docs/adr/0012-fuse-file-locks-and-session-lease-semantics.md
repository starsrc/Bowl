# 0012. FUSE 文件锁（GetLk/SetLk/SetLkw）、双命名空间隔离、会话租约与有界死锁检测设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成扩展属性（Xattr）与结构化错误原因契约（ADR-0011）后，需推进 Linux FUSE 文件锁相关的三项核心操作码：`FUSE_GETLK (31)`、`FUSE_SETLK (32)` 与 `FUSE_SETLKW (33)`，并联动补齐 `FUSE_FLUSH` 与 `FUSE_RELEASE` 的锁清理闭环。在对齐 POSIX `fcntl(2)`、BSD `flock(2)` 及 Linux 内核 FUSE 协议语义时，面临六项关键架构权衡：
1. **POSIX 记录锁与 BSD `flock` 的命名空间隔离及模式转换差异**：Linux FUSE 通过 `fuse_lk_in.lk_flags`（`0` vs `FUSE_LK_FLOCK = 0x1`）复用同一套锁操作码。在 Linux VFS 原生语义中，POSIX 字节区间锁与 BSD `flock` 整文件锁处于完全正交的冲突域（互不阻塞）；POSIX 锁要求校验打开句柄的读/写模式位（`EBADF`）且在升级冲突时保留原读锁，而 BSD `flock` 允许对只读句柄加排他锁（`LOCK_EX`）且遵循“先释放旧锁、再申请新锁”的非原子转换语义（升级冲突时原锁已丢失）。此外，标准 Linux FUSE ABI 缺少传递 OFD（Open File Description）锁专用生命周期的独立通道，不宜对标准连接作出虚假的完整 OFD 兼容承诺。
2. **文件锁与 `FileHandle`（`fh`）、`Flush` 及 `Release` 的归属解耦**：POSIX 规范强制要求同一进程关闭文件的**任意一个**文件描述符（触发 `FUSE_FLUSH` 并携带 `lock_owner`）时，必须立即释放该进程（`lock_owner`）在该 Inode 上持有的全部 POSIX 记录锁，即便这些锁是通过其他 `fh` 申请或跨多个 `fh` 合并而成的；而 BSD `flock` 绑定于打开文件表项（`fh`），仅在句柄最后引用关闭（`FUSE_RELEASE` 且 `release_flags` 携带 `FUSE_RELEASE_FLOCK_UNLOCK = 0x2`）时释放。若将 POSIX 锁外键级联绑定至 `file_handles(fh) ON DELETE CASCADE`，不仅合并区间无法指派单一 `fh`，关闭未直接加锁的句柄也会漏删锁或误删锁。
3. **分布式客户端崩溃防护（Client Session 租约与 Fencing）**：文件锁属于运行期同步状态而非永久磁盘元数据。不同客户端内核独立生成的 64 位 `lock_owner` 哈希存在跨节点碰撞可能，且客户端崩溃或网络分区未下发 `Flush` / `Release` 时会导致持久化锁永久泄漏。
4. **数据库事务隔离级别与并发快照可见性**：此前只读或部分校验路径使用了 `REPEATABLE READ` 隔离级别。但在加锁写路径中，若事务在 `SELECT ... FOR UPDATE` 阻塞等待前已建立 `REPEATABLE READ` 快照，获取行锁后仍会读取旧快照或触发序列化失败，无法感知持锁事务刚刚提交的锁状态变更。
5. **`SetLkw`（`FUSE_SETLKW`）阻塞等待的悬挂锁风险、同节点死锁（`EDEADLK`）与写锁饥饿**：在网络 RPC 与异步中断（`FUSE_INTERRUPT`）并发到达时，若服务端数据库事务刚提交授锁而上下文恰好取消，缺乏状态机原子裁决会产生无人认领的“悬挂锁（Orphaned Lock）”；同时，同一 Inode 上双读锁同时阻塞升级写锁会形成直接死锁。
6. **目录锁的内核拦截边界与 Inode 时间戳不变量**：Linux VFS 虽允许对目录 fd 调用 `flock(2)`，但 Linux 内核 FUSE 驱动（`fs/fuse/dir.c`）的 `fuse_dir_operations` 未注册任何锁回调，目录锁由内核本地处理而绝不下发用户态；同时文件锁操作属于运行期同步，严禁污染 Inode 的 `atime`、`mtime` 或 `ctime`。

## 决策
1. **三接口 1:1 拆分、`FileLock` 完整契约与 `lock_owner` 64 位无损编码**：
   - 在 `StorageService` 与 `meta.Manager` 中提供 `GetLk`（`FUSE_GETLK = 31`）、`SetLk`（`FUSE_SETLK = 32`）、`SetLkw`（`FUSE_SETLKW = 33`）三个独立方法，共用 `FileLock` 结构（包含 `uint64 start`、`uint64 end`、`uint32 type`、`uint32 pid`）。
   - 在 `meta` 领域层定义平台无关的 Linux FUSE 常量：`F_RDLCK = 0`、`F_WRLCK = 1`、`F_UNLCK = 2`（严禁依赖宿主机 `syscall.F_*`）；锁标志位 `FuseLkFlock = 0x1`（`1 << 0`）；释放标志位 `FuseReleaseFlush = 0x1`（`1 << 0`）与 `FuseReleaseFlockUnlock = 0x2`（`1 << 1`）；最大字节偏移量 `OffsetMax = math.MaxInt64`（`0x7fffffffffffffff`）。
   - **区间与 `lock_owner` 约束**：锁区间遵循闭区间 `0 <= start <= end <= OffsetMax`（`end == OffsetMax` 代表延伸至无穷远，区间裂解与相邻合并计算时必须先判 `< OffsetMax` 防止 `+ 1` 溢出）。由于 Linux 内核 `fuse_lock_owner_id()` 生成的 64 位无符号哈希最高位可能为 `1`，Protobuf 中的 `lock_owner` 允许全范围 `uint64`（不设 `lte = MaxInt64`），在存入 PostgreSQL `BIGINT` 时通过 `int64(lock_owner)` 二进制补码无损重解释。
2. **POSIX 记录锁与 BSD `flock` 双命名空间隔离及独立状态机**：
   - **冲突域完全正交**：`lk_flags == 0`（POSIX 记录锁）与 `lk_flags == FuseLkFlock (0x1)`（BSD `flock`）在冲突检测、查询与清理时严格按 `lk_flags` 隔离，互不阻塞。暂不对标准连接承诺完整 OFD 锁跨句柄共享语义。
   - **打开句柄模式位校验分流**：
     - POSIX 记录锁（`lk_flags == 0`）：`SetLk` / `SetLkw` 申请 `F_RDLCK` 时要求 `fh` 具备读权限（`flags & O_ACCMODE` 为 `O_RDONLY` 或 `O_RDWR`），申请 `F_WRLCK` 时要求具备写权限（`O_WRONLY` 或 `O_RDWR`），否则返回 `ErrBadFileHandle`（`EBADF`）；`F_UNLCK` 与 `GetLk` 仅校验 `fh` 存在且归属目标 `ino`，不限制读写模式位。
     - BSD `flock`（`lk_flags == FuseLkFlock`）：强制要求 `start == 0 && end == OffsetMax`（否则返回 `ErrInvalidArgument` / `EINVAL`）；校验 `fh` 归属目标 `ino`，但**不校验 `fh` 的读写打开模式位**（允许对只读句柄加排他锁 `F_WRLCK` / `LOCK_EX`）。
   - **转换与裂解合并算法分流**：
     - **POSIX 原子区间算法**：属于同一 `(session_id, ino, lock_owner)` 的锁在外部冲突时直接拒绝（`SetLk` 返回 `ErrAgain`），原锁完整保留；无外部冲突时，对相交的同拥有者锁执行覆盖或左右裂解（裂解出的左右残段保留原记录的 `pid`），对相交或首尾紧邻（`prev.end < OffsetMax && prev.end + 1 == next.start`）且同类型的同拥有者锁合并为单条连续区间（合并后记录的 `pid` 更新为新请求的 `pid`）。
     - **BSD `flock` 非原子转换算法**：属于同一 `(session_id, ino, fh)` 的 `flock` 在请求模式转换（如 `F_RDLCK -> F_WRLCK`）时，**先在事务内删除该 `(session_id, ino, fh)` 已持有的旧 `flock` 记录**，再检测与其他 `fh` 的冲突；若存在冲突，`SetLk` 仍提交“删除旧锁”的变更并返回 `ErrAgain`（`EAGAIN`），`SetLkw` 则提交旧锁删除并登记 `PENDING` 等待，严格还原 Linux `flock_lock_inode` 的非原子升级语义。
3. **会话租约（Client Session Lease）、持久化表结构与 `READ COMMITTED` 事务协议**：
   - **数据库表结构**：
     - `client_sessions` 表：`session_id BIGINT PRIMARY KEY`、`expires_at TIMESTAMPTZ NOT NULL`、`updated_at TIMESTAMPTZ NOT NULL`。每次锁操作或携带 `session_id` 的 `Flush` / `Release` 自动通过 `UPSERT` 续期会话租约（默认 `DefaultSessionLeaseTTL = 60s`）。
     - `file_locks` 表：`id BIGSERIAL PRIMARY KEY`、`session_id BIGINT NOT NULL REFERENCES client_sessions(session_id) ON DELETE CASCADE`、`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`fh BIGINT NOT NULL`（仅作为 `flock` 绑定键，**严禁设置 `REFERENCES file_handles(fh) ON DELETE CASCADE`**）、`lock_owner BIGINT NOT NULL`、`lk_flags INTEGER NOT NULL`、`start_offset BIGINT NOT NULL`、`end_offset BIGINT NOT NULL`、`lock_type SMALLINT NOT NULL`、`pid INTEGER NOT NULL`、`created_at TIMESTAMPTZ NOT NULL`。
     - `file_lock_waiters` 表：`request_id BIGINT NOT NULL`、`session_id BIGINT NOT NULL REFERENCES client_sessions(session_id) ON DELETE CASCADE`、`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`fh BIGINT NOT NULL`、`lock_owner BIGINT NOT NULL`、`lk_flags INTEGER NOT NULL`、`start_offset BIGINT NOT NULL`、`end_offset BIGINT NOT NULL`、`lock_type SMALLINT NOT NULL`、`pid INTEGER NOT NULL`、`status SMALLINT NOT NULL`（`1 = PENDING, 2 = GRANTED, 3 = CANCELLED`）、`created_at TIMESTAMPTZ NOT NULL`，主键为 `(session_id, request_id)`。
   - **写路径 `READ COMMITTED` 三步事务协议**：`SetLk`、`SetLkw`、携带 `lock_owner` 的 `Flush` 以及携带 `FUSE_RELEASE_FLOCK_UNLOCK` 的 `Release` 统一使用默认 `READ COMMITTED` 隔离级别：
     1. 调用 `GetInodeForUpdate(ino)` 获取目标 Inode 行级排他锁（校验节点存在且非目录），随后清理已超期的 `client_sessions`（`expires_at <= clock_timestamp()`，级联清除过期锁与等待者）并续期当前 `session_id`；
     2. 执行独立 SQL 语句扫描当前 `ino` 在对应 `lk_flags` 命名空间下的活跃锁与 `PENDING` 等待队列（确保读取行锁等待结束后的最新已提交快照）；
     3. 执行死锁判定、冲突判定、区间裂解合并或锁清理，并在提交后通知唤醒等待队列。
   - **读路径轻量化**：`GetLk` 采用只读事务直查主库（校验 `Inode` 存在且非目录、`fh` 有效，过滤 `s.expires_at > clock_timestamp()`），不加行级排他锁；若 `req.lk.type == F_UNLCK` 返回 `ErrInvalidArgument`（`EINVAL`）；存在多个冲突锁时按 `ORDER BY start_offset ASC, end_offset ASC, id ASC LIMIT 1` 确定性返回首个冲突锁，无冲突时返回 `{start: req.start, end: req.end, type: F_UNLCK, pid: 0}`。
4. **`Flush` 与 `Release` 的职责边界与清理解耦**：
   - **`Flush`（清理进程级 POSIX 记录锁）**：当 `Flush` 携带 `lock_owner` 时，在 `READ COMMITTED` 写事务内锁定 `Inode`、校验 `(ino, fh)` 合法后，按 `(session_id, ino, lock_owner)`（若存量调用未传 `session_id` 则按 `(ino, lock_owner)`）删除全部 `lk_flags == 0` 的 POSIX 记录锁并唤醒等待者；`Flush` 不销毁 `fh`，亦不清理 `flock`。当未携带 `lock_owner` 时维持只读校验。
   - **`Release`（销毁句柄与按标志位清理 `flock`）**：`ReleaseRequest` 扩展 `optional uint64 lock_owner`、`uint32 release_flags` 与 `optional uint64 session_id`。在 `READ COMMITTED` 写事务中锁定 `Inode` 并删除 `file_handles(fh)`；仅当 `(release_flags & FuseReleaseFlockUnlock) != 0` 时，清理该 `(session_id, ino, fh)`（未传 `session_id` 时按 `(ino, fh)`）绑定的 `lk_flags == FuseLkFlock` 锁并唤醒等待者，不越权清理 POSIX 锁；最后若 `nlink == 0` 且无剩余活跃句柄，执行孤儿节点物理回收（`ON DELETE CASCADE` 自动兜底清理孤儿节点残留锁）。
5. **`SetLkw` 原子状态机、同 Inode 有界死锁检测（`EDEADLK`）与防写锁饥饿**：
   - **状态机防悬挂锁**：`SetLkw` 以 `(session_id, request_id)` 驱动 `PENDING / GRANTED / CANCELLED` 状态机。每次在 `GetInodeForUpdate(ino)` 事务内尝试授锁前，必须先检查 `ctx.Err()` 与 waiter 状态；若已取消则将状态收敛为 `CANCELLED`（删除 waiter）并返回 `ErrInterrupted`（`EINTR`），绝不写入 `file_locks`；若可授锁，则在同一事务内原子写入 `file_locks` 并完成 waiter 状态收敛。若事务外部等待期间收到 `ctx.Done()`，立即开启清理事务将 `PENDING` 置为 `CANCELLED`（若发现并发事务已将其置为 `GRANTED`，则回滚撤销已授锁或按原子状态裁决，杜绝无人认领的悬挂锁）。
   - **同一 Inode 内的有界死锁检测**：在 `GetInodeForUpdate(ino)` 保护下，当请求者 $O_{\text{req}}$ 遇冲突需等待时，基于该 `ino` 当前同命名空间的活跃锁集合与 `PENDING` 等待者集合构建局部等待有向图（若等待者 $W$ 的请求与持锁者 $H \neq W$ 的活跃锁冲突，则存在有向边 $W \to H$）。若引入 $O_{\text{req}}$ 的等待边后存在从 $O_{\text{req}}$ 回到 $O_{\text{req}}$ 的有向环（如双读锁同时阻塞升级写锁 $A \leftrightarrow B$ 或多方环），立即拒绝等待并返回 `ErrDeadlock`（`EDEADLK`）。不维护跨 Inode 全局死锁检测图。
   - **可靠唤醒与防写锁饥饿排队**：短事务登记 `PENDING` 后立即提交并释放 Inode 行锁，在事务外通过进程内条件广播 / PostgreSQL `NOTIFY` 配合带抖动的指数退避轮询等待。当新到达的加锁请求者当前在该 `ino` 同命名空间下**未持有任何活跃锁**，且其请求与队列中更早的 `PENDING` 等待者存在冲突（如写锁等待期间新到达的读锁）时，不可插队越过等待者（`SetLk` 返回 `ErrAgain`，`SetLkw` 排入队列尾部），防止写锁永久饥饿；若请求者已持有该节点活跃锁（如重入、降级、拆分或升级），则不触发排队拦截以免引入附加死锁。
6. **目录防御边界、时间戳不变量与 `ErrorInfo` 映射扩展**：
   - 虽然 Linux VFS 允许目录 `flock(2)`，但内核 FUSE 驱动从不向用户态下发目录锁请求；若服务端收到针对目录 Inode（`S_IFDIR`）的 `GetLk` / `SetLk` / `SetLkw`，统一防御性返回 `ErrIsDir`（`EISDIR`）。处于 `unlink-whilst-open`（`nlink == 0` 且持有活跃 `fh`）的孤儿节点正常支持全部文件锁操作。
   - `GetLk`、`SetLk`、`SetLkw` 严格不修改 `Inode` 的 `atime`、`mtime` 或 `ctime`。
   - 扩展 ADR-0011 的领域哨兵错误映射表（`domain = "bowl.storage"`）：
     - `ErrAgain` $\rightarrow$ `ResourceExhausted` + `reason: "EAGAIN"`
     - `ErrDeadlock` $\rightarrow$ `Aborted` + `reason: "EDEADLK"`
     - `ErrInterrupted` $\rightarrow$ `Canceled` + `reason: "EINTR"`

## 结果
- **积极影响**：完整闭环了 `FUSE_GETLK (31)`、`FUSE_SETLK (32)`、`FUSE_SETLKW (33)` 及 `Flush` / `Release` 的锁生命周期，精准区分了 POSIX 字节区间裂解合并与 BSD `flock` 非原子转换语义，通过 `client_sessions` 租约消除了客户端异常断连导致的死锁泄漏，并通过 `request_id` 原子状态机与同节点局部等待图环检测根治了 `SetLkw` 悬挂锁与同区间升级死锁问题。
- **代价考量**：引入 `client_sessions`、`file_locks` 与 `file_lock_waiters` 三张运行期状态表后，加锁与带锁关闭操作需在 `READ COMMITTED` 事务内执行行级排他锁与区间计算，但由于单文件活跃锁与等待者数量通常极小且均命中 `(ino, lk_flags)` 复合索引，事务内计算开销可控。
