# 0008. FUSE 目录树拓扑变更锁序、孤儿节点生命周期与 POSIX 不变量设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在推进 `Mkdir`、`Link`、`Unlink`、`Rmdir` 与 `Rename` 五项目录树拓扑变更能力时，涉及多父目录、多子节点的跨表并发读写。在遵循严格 POSIX 与 Linux FUSE 协议规范时，面临四项核心设计权衡：
1. **多节点并发加锁与死锁防御**：跨目录 `Rename(A/x -> B/y)` 与反向 `Rename(B/u -> A/v)`、以及并发 `Link` / `Unlink` 会同时锁定多个目录与文件 `Inode`。若无全局一致的悲观锁获取顺序，高并发下极易触发数据库行锁死锁（SQLSTATE `40P01`）。
2. **解链节点的生命周期与孤儿节点（`Orphan Inode`）边界**：ADR-0004 确立了对 `unlink-whilst-open`（`nlink == 0`）节点的属性读取支持。需明确 `Unlink`、`Rmdir` 与 `Rename`（覆盖目标）在移除最后一个 `Entry` 时是否保留 `inodes` 行，以及 `Link` 面对 `nlink == 0` 孤儿节点时的防御语义。
3. **目录硬链接计数（`nlink`）与空目录判定**：传统 POSIX 中目录初始 `nlink = 2` 且随子目录增减联动变化父目录 `nlink`，但包含常规文件的目录其 `nlink` 仍为 `2`，不能仅凭 `nlink == 2` 判定目录为空。
4. **`Rename` 的同节点硬链接保护与目录环检测**：POSIX `rename(2)` 规定当源与目标指向同一 `Inode` 的不同硬链接时必须无副作用成功返回；当跨目录移动目录时，必须阻止将目录移入其自身的子孙目录中（`EINVAL`）。

## 决策
1. **全局两级升序悲观锁协议（Two-Tier Ascending Lock Order）**：
   - 所有目录树变更事务统一遵循 **“第一级：所有涉及的父目录节点按 `ino` 升序加锁 $\rightarrow$ 第二级：所有涉及的目标子节点按 `ino` 升序加锁”** 的 `SELECT ... FOR UPDATE` 顺序：
     - `Mkdir`：锁定 `parent_ino`。
     - `Link`：先锁定父目录 `new_parent_ino`，再锁定目标非目录节点 `ino`（若 `ino == new_parent_ino` 则直接判定类型非法返回 `ErrPerm` 或 `ErrNotDir`）。
     - `Unlink` / `Rmdir`：先锁定 `parent_ino`，解析 `Entry` 后再锁定目标 `child_ino`。
     - `Rename`：先按 `min(old_parent_ino, new_parent_ino)` $\rightarrow$ `max(old_parent_ino, new_parent_ino)` 锁定父目录；解析源与目标 `Entry` 后，再按 `ino` 升序锁定 `old_ino` 与（若存在且异于 `old_ino` 的）`new_ino`。
2. **孤儿节点（`Orphan Inode`）保留与 `Link` 复活拦截**：
   - `Unlink`、`Rmdir` 与 `Rename` 覆盖目标时，仅从 `entries` 表删除对应记录，并更新目标 `Inode` 的 `nlink`（非目录递减 `nlink - 1`，目录直接置 `nlink = 0`）及刷新 `ctime = NOW()`，**不物理删除 `inodes` 表记录**，以完整支撑 `unlink-whilst-open` 句柄操作及 `parent.Nlink == 0` 孤儿目录写入拦截。
   - `Link` 在锁定源 `Inode` 后，若发现其为目录则返回 `ErrPerm`（`EPERM`）；若发现 `nlink == 0`（已解链孤儿节点）则返回 `ErrNotFound`（`ENOENT`），严禁复活孤儿节点。
3. **目录 `nlink` 联动、空目录 `EXISTS` 校验与 `S_ISGID` 继承**：
   - `Mkdir` 接收 `mode` 并合成为 `S_IFDIR | (mode & 0o7777)`（若携带非零且非 `S_IFDIR` 的类型位则返回 `ErrInvalidArgument`）；若父目录含 `S_ISGID (0o2000)`，新目录同时继承父目录 `gid` 与 `S_ISGID` 模式位。新目录初始 `nlink = 2`，父目录 `nlink + 1`。
   - `Rmdir` 与 `Rename`（覆盖目录）统一通过 `EXISTS (SELECT 1 FROM entries WHERE parent_ino = $1)` 权威判定目标目录是否为空，非空时返回 `ErrNotEmpty`（`ENOTEMPTY`）。`Rmdir` 成功后父目录 `nlink - 1`。
   - 对 `"."` 与 `".."` 的操作：在确认父目录有效后，创建类操作（`Mkdir` / `Link` / `Symlink`）返回 `ErrAlreadyExists`（`EEXIST`）；`Rmdir("..")` 返回 `ErrNotEmpty`（`ENOTEMPTY`）；其余删除与重命名操作返回 `ErrInvalidArgument`（`EINVAL`）。
4. **`Rename` 同节点硬链接无副作用返回、覆盖矩阵与 `WITH RECURSIVE` 环检测**：
   - **同路径或同节点短路**：在确认新旧父目录有效且源 `Entry` 存在后，若 `(old_parent_ino == new_parent_ino && old_name == new_name)` 或目标 `Entry` 已存在且 `old_ino == new_ino`（同一 `Inode` 的硬链接），直接无副作用返回成功。
   - **异节点覆盖矩阵**：当 `new_ino` 存在且 `old_ino != new_ino` 时，源为目录而目标非目录返回 `ErrNotDir`；源非目录而目标为目录返回 `ErrIsDir`；两者皆为目录时校验目标目录为空（否则 `ErrNotEmpty`）后将目标目录置为孤儿（`nlink = 0`）。跨父目录移动目录时，源父目录 `nlink - 1`，若未覆盖目标目录则目标父目录 `nlink + 1`（若覆盖了目标空目录则目标父目录 `nlink` 不变）。
   - **祖先链环检测**：当源节点为目录且 `old_parent_ino != new_parent_ino` 时，通过 PostgreSQL `WITH RECURSIVE` 沿 `entries` 自 `new_parent_ino` 向上回溯祖先链（设深度上限保护），若命中 `old_ino` 则返回 `ErrInvalidArgument`（`EINVAL`）。

## 结果
- **积极影响**：通过严格的两级升序锁协议从根源消除了并发拓扑变更死锁；孤儿节点状态机与 ADR-0004、ADR-0006 形成严密闭环；完整覆盖了 POSIX 在硬链接自覆盖、空目录校验与子孙目录环检测上的全部边界约束。
- **代价考量**：跨目录移动目录时需额外执行一次 `WITH RECURSIVE` 祖先链查询，但因禁止目录硬链接，每个目录至多只有一个父节点，递归路径退化为单链点查，数据库开销极小。
