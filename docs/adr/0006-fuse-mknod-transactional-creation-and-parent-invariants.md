# 0006. FUSE Mknod 读写事务节点创建与父目录不变量设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在完成 `Lookup`、`GetAttr` 与 `SetAttr` 的设计后，需要实现对接 Linux FUSE 内核 `FUSE_MKNOD` 协议与 POSIX `mknod(2)` 语义的 `Mknod` 能力，用于在指定父目录下创建非目录文件节点并绑定同名 `Entry`。在遵循强 FUSE 与 POSIX 规范时，面临四项核心设计权衡：
1. **文件类型职责边界与参数净化**：POSIX 与 FUSE 中，目录（`S_IFDIR`）与符号链接（`S_IFLNK`）分别由独立的 `Mkdir` 与 `Symlink` 协议负责，且特殊设备号 `rdev` 仅对字符设备（`S_IFCHR`）与块设备（`S_IFBLK`）有效；同时 FUSE 请求携带的 `umask` 需明确客户端与服务端的计算边界。
2. **保留目录项名称与同名冲突错误语义**：POSIX `mknod(2)` 规定当目标名称已存在（含每个目录天然存在的 `.` 与 `..`）时必须返回 `EEXIST`，且该判定需在确认父节点存在且为目录之后进行。
3. **跨表写入并发安全与父目录状态不变量**：`Mknod` 涉及 `inodes` 与 `entries` 两张表的写入。并发场景下需防止父目录在创建子项瞬间被并发解链（`rmdir`），拦截向已解链孤儿目录（`nlink == 0`）写入子项，并严格落实父目录 `S_ISGID` 组继承及父目录 `mtime`/`ctime` 联动刷新。
4. **SQL 算子复用性**：后续 `Mkdir`、`Symlink`、`Link` 均涉及节点插入、目录项绑定与父目录时间戳刷新，需避免编写仅服务于单一 RPC 的复杂耦合 SQL。

## 决策
1. **RPC 契约与权限掩码边界**：
   - `umask` 由客户端 FUSE 适配层在发起 RPC 前完成权限掩码合成，`MknodRequest` 仅接收合成后的目标 `mode` 及 `parent_ino`、`name`、`uid`、`gid`、`rdev`；`MknodResponse` 与 `LookupResponse` 对齐，返回新建 `Inode` 及 `entry_timeout_seconds`、`attr_timeout_seconds`。
   - 对 `parent_ino` 施加 `> 0 && <= math.MaxInt64` 约束，对 `mode` 施加 `> 0 && <= 0o177777`（`65535`）约束，对 `name` 施加 `1..255` 字节且不含 `/` 与 `\x00` 约束。
2. **严格显式文件类型白名单与 `rdev` 归一化**：
   - `Mknod` 仅允许显式传入五种非目录、非软链文件类型：`S_IFREG (0o100000)`、`S_IFCHR (0o020000)`、`S_IFBLK (0o060000)`、`S_IFIFO (0o010000)`、`S_IFSOCK (0o140000)`；传入 `0`、`S_IFDIR`、`S_IFLNK` 或未知高位类型统一返回 `ErrInvalidArgument`（`EINVAL`）。
   - 仅当类型为 `S_IFCHR` 或 `S_IFBLK` 时持久化请求传入的 `rdev`；其余类型强制将落库 `rdev` 归零为 `0`。新建节点初始 `size = 0`、`nlink = 1`、`generation = 0`，时间戳统一取事务 `NOW()`。
3. **父目录 `FOR UPDATE` 悲观锁、孤儿目录拦截与属性联动**：
   - `meta.Manager.Mknod(ctx, MknodParams)` 开启读写事务，先调用 `GetInodeForUpdate(parent_ino)` 加行级排他锁：若父节点不存在或 `nlink == 0`（已解链孤儿目录）返回 `ErrNotFound`（`ENOENT`）；若父节点非目录返回 `ErrNotDir`（`ENOTDIR`）。
   - 父目录校验通过后，若 `name` 为 `"."` 或 `".."`，直接返回 `ErrAlreadyExists`（`EEXIST`）。
   - 若父目录 `mode` 包含 `S_ISGID (0o2000)` 位，新建节点的 `gid` 强制继承父目录 `gid`。
4. **正交原子 `sqlc` 算子组合与 `23505` 冲突捕获**：
   - 在 `sqlc` 层定义三个正交原子操作：`CreateInode`、`CreateEntry`、`TouchInodeTimestamps`（更新父目录 `mtime = NOW(), ctime = NOW()`）。
   - 正常路径下不执行额外的 `SELECT entries` 预查；当 `CreateEntry` 触发 PostgreSQL 唯一约束冲突（SQLSTATE `23505` `unique_violation`）时，在领域层捕获并映射为 `ErrAlreadyExists`（`EEXIST`）并回滚事务。

## 结果
- **积极影响**：完整覆盖 POSIX / FUSE `mknod` 的类型边界、权限组继承、孤儿目录防护与时间戳联动语义；正交化的 `CreateInode`、`CreateEntry`、`TouchInodeTimestamps` 算子可直接复用于后续 `Mkdir`、`Symlink`、`Link` 操作。
- **代价考量**：并发同名冲突下回滚事务会消耗一个 `inode_id_seq` 序列号，但在 FUSE VFS 前置 `Lookup` 机制下冲突仅发生于极端并发竞态，省去了正常创建路径上的一次冗余索引点查。
