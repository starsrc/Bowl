# 0005. FUSE SetAttr 读写事务悲观锁部分更新与 POSIX 属性不变量设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在支持 `Lookup` 与 `GetAttr` 后，需要实现对接 Linux FUSE 内核 `FUSE_SETATTR` 协议的 `SetAttr` 能力（涵盖 `chmod`、`chown`、`truncate`、`utimens` 等系统调用的元数据修改）。在遵循强 FUSE 与 POSIX 规范时，面临三项核心架构权衡：
1. **并发部分更新与状态依赖校验的原子性**：`SetAttr` 属于按需部分更新（Partial Update），且多项 POSIX 约束强依赖目标 `Inode` 当前的 `mode` 状态（例如：禁止对目录修改 `size` 并返回 `EISDIR`、禁止对非常规文件修改 `size` 并返回 `EINVAL`、修改 `mode` 时禁止篡改高位文件类型 `S_IFMT`、非显式修改 `mode` 而变更常规文件的 `uid`/`gid`/`size` 时需自动剥离 `S_ISUID`/`S_ISGID` 特权位）。若采用无锁先查后改或单条复杂 `UPDATE`，既容易在并发写入下产生 TOCTOU 竞态，也难以区分 `ENOENT`（节点不存在）、`EISDIR`（目标是目录）与 `EINVAL`（非法参数）。
2. **`ctime` 管控与时间戳联动规则**：标准 FUSE `FUSE_SETATTR` 下，`ctime` 由文件系统服务端权威维护，不允许客户端指定修改为任意时间；且 `truncate`（修改 `size`）在未显式指定 `mtime` 时需联动刷新 `mtime`。
3. **契约层部分更新表达与 64 位有符号整型边界**：Protobuf 需要清晰表达各可选修改字段及 `ATIME_NOW` / `MTIME_NOW` 策略，同时防止空更新请求、冲突时间策略以及 `uint64`（`ino`、`size`）向 PostgreSQL `BIGINT` 转换时的符号位溢出。

## 决策
1. **Proto3 `optional` 契约与 CEL 复合校验**：
   - 在 `SetAttrRequest` 中采用 `optional` 标量与消息字段（`mode`、`uid`、`gid`、`size`、`atime`、`mtime`、`flags`、`fh`、`session_id`）配合布尔标志（`atime_now`、`mtime_now`、`ctime_now`）表达部分更新。
   - 对 `ino`（`> 0 && <= math.MaxInt64`）、`size`（`<= math.MaxInt64`）、`mode`（`<= 0o177777` 即 `65535`）及 `flags`（`<= math.MaxInt32`）施加边界约束。
   - 通过消息级 CEL 规则强制要求：至少指定一项变更属性（含仅设置 `ctime_now` 或 `flags`）；`atime` 与 `atime_now` 互斥；`mtime` 与 `mtime_now` 互斥。
2. **读写事务 + `SELECT ... FOR UPDATE` 悲观锁两步执行与可选句柄校验**：
   - 在 `sqlc` 查询层新增 `GetInodeForUpdate`（`SELECT ... FROM inodes WHERE ino = $1 FOR UPDATE`）与 `UpdateInodeAttr`（更新 `mode, size, uid, gid, atime, mtime, flags` 并将 `ctime` 强制设为 `clock_timestamp()`，`RETURNING` 完整记录）。
   - `meta.Manager.SetAttr` 通过 `Store.ExecTxWithOptions(ctx, ...)` 开启读写事务：先以 `GetInodeForUpdate` 锁定并读取目标 `Inode`（不存在则返回 `ErrNotFound`）；若请求携带 `fh`，通过 `GetFileHandle` 校验句柄存在且归属该 `(ino, session_id)`（不匹配返回 `ErrBadFileHandle` / `EBADF`），且当同时修改 `size`（`ftruncate`）时强制要求句柄具备写权限（`O_WRONLY` 或 `O_RDWR`）；若携带有效 `session_id` 则续期客户端会话；当未显式传入 `flags` 时默认保留 `current.Flags`（在 `SetAttr`、`Open(O_TRUNC)` 与 `Fallocate` 中一致生效），最后调用 `UpdateInodeAttr` 完成持久化并动态推导 `Blksize` 与 `Blocks`。
3. **POSIX 属性不变量与特权位安全剥离**：
   - **文件类型不可变与符号链接权限固定**：若目标为符号链接（`S_IFLNK`）且请求修改 `mode`，直接返回 `ErrInvalidArgument`（POSIX 符号链接权限固定为 `0777` 且不可通过 `chmod` 修改）；若传入 `mode` 携带非零文件类型位（`mode & S_IFMT != 0`）且与原 `Inode` 的 `mode & S_IFMT` 不一致，返回 `ErrInvalidArgument`；最终写入的 `mode` 始终保留原文件类型位 `(old_mode & S_IFMT) | (new_mode & 0o7777)`。
   - **常规文件 `size` 变更限制**：当传入 `size` 时，若目标为目录（`S_IFDIR`）则返回领域错误 `ErrIsDir`（对应 `EISDIR`）；若为其他非常规文件（`!= S_IFREG`）则返回 `ErrInvalidArgument`（对应 `EINVAL`）。
   - **特权位自动清理**：当未显式传入 `mode`，但对常规文件（`S_IFREG`）修改了 `uid`、`gid` 或 `size` 时，自动清除原 `mode` 中的 `S_ISUID (0o4000)` 与 `S_ISGID (0o2000)` 位。
   - **时间戳权威刷新**：每次 `SetAttr` 更新总是将 `ctime` 刷新为事务当前时间 `NOW()`；当修改了 `size` 且未显式指定 `mtime` 或 `mtime_now` 时，自动将 `mtime` 同步刷新为 `NOW()`。

## 结果
- **积极影响**：通过行级排他锁消除了并发 `SetAttr` 的竞态覆盖风险，领域层清晰承载了完整的 POSIX / FUSE 校验与错误映射（`ErrInvalidArgument`、`ErrNotFound`、`ErrIsDir`），数据库 SQL 保持简洁正交。
- **代价考量**：每次 `SetAttr` 在同一读写事务内执行一次主键加锁查询与一次主键更新，但在单节点元数据事务内两次主键操作开销极低，换取了严格的 POSIX 语义一致性。
