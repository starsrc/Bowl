# 0003. POSIX Lookup 只读事务两步校验、特殊目录项解析与块属性动态推导设计

## 状态
已采纳 (accepted)

## 背景
在实现 Storage 元数据引擎的 `Lookup` 能力时，需要满足以下核心契约：
1. **严格遵循 POSIX `lookup(parent_ino, name)` 语义**：准确区分“父节点或目标子项不存在”（`ENOENT`）与“父节点存在但不是目录”（`ENOTDIR`）。
2. **精简数据库持久化存储与块属性隔离推导**：`inodes` 表仅持久化核心物理状态（`mode`, `size`, `nlink`, `uid`, `gid`, `rdev`, 时间戳），而未冗余存储 POSIX `struct stat` 中的 `blksize`（块大小）与 `blocks`（占用的 512 字节块数）。
3. **支持特殊目录项与孤儿父目录拦截**：处理 `"."` 与 `".."` 解析（含根目录自指与孤儿目录 `rmdir-whilst-open` 下通过 `inodes.parent_ino` 寻找原父目录），拦截已解链孤儿父目录（`parent.Nlink == 0`）对常规子项的查找。
4. **对齐 JuiceFS 空名称与内核缓存时效语义**：对 `len(name) == 0` 先进入事务校验父目录再返回 `ENOENT`；支持目录（`dirEntryTimeout`）与非目录（`entryTimeout`）独立配置 dentry 缓存超时，以及属性缓存超时（`attrTimeout`）。

## 决策
1. **数据访问与事务架构**：
   - 采用 `sqlc` + `pgx/v5` 生成类型安全的 `Querier` 接口，并结合 `github.com/starsrc/Aether/pkg/tx` 的泛型 `Transactor[Querier]` 构建 `Store` 抽象，确保编译期类型安全与事务生命周期安全。
2. **只读可重复读事务校验与特殊项解析顺序**：
   - 入参静态防御校验：`parent_ino == 0 || parent_ino > math.MaxInt64` 返回 `ErrInvalidArgument`；`len(name) > MaxFileNameBytes` 返回 `ErrNameTooLong`；`name` 包含 `'/'` 或 `'\x00'` 时返回 `ErrInvalidArgument`。对 `len(name) == 0` 不在静态阶段拦截。
   - 开启只读可重复读事务（`RepeatableRead`）：
     1. 查询父节点 `GetInode(parent_ino)`：不存在返回 `ErrNotFound`（`ENOENT`）；非目录返回 `ErrNotDir`（`ENOTDIR`）。
     2. 优先解析 `".."`（在 `parent.Nlink == 0` 拦截前执行）：
        - 若 `parentIno == RootIno (1)`，根目录自指返回根目录属性。
        - 若 `parent.ParentIno > 0 && parent.ParentIno != parentIno`，直接点查原父节点属性；若 `pgx.ErrNoRows` 则回退调用 `GetParentInode` 反查，再无记录则返回 `ErrNotFound`。
        - 若 `parent.ParentIno <= 0`，调用 `GetParentInode` 反查；若未命中且 `parent.Nlink > 0` 则回退为自身，否则返回 `ErrNotFound`。
     3. 孤儿父目录拦截：若 `parent.Nlink == 0`，返回 `ErrNotFound`（`ENOENT`），杜绝已解链父目录下的无效子项点查。
     4. 解析 `"."`：直接返回当前父节点自身属性。
     5. 解析空名称 `""`：父目录校验合法后，按 JuiceFS 规范返回 `ErrNotFound`（`ENOENT`）。
     6. 常规目录项点查：调用 `GetInodeByEntry(parent_ino, name)`，不存在返回 `ErrNotFound`（`ENOENT`）。
3. **块属性与文件类型隔离推导**：
   - 固定 `Blksize = 65536` (`0x10000`, 64 KiB)，对标 JuiceFS `attrToStat`。
   - 目录节点默认 `Size = 4096`，计算 `Blocks = 8`（512 字节块）。
   - 常规文件与符号链接按 `(Size + 511) / 512` 计算 `Blocks`。
   - 特殊文件（`S_IFCHR`, `S_IFBLK`, `S_IFIFO`, `S_IFSOCK`）强制隔离 `Size = 0` 与 `Blocks = 0`；仅设备节点（`S_IFCHR`, `S_IFBLK`）暴露 `Rdev`，其余非设备节点强制 `Rdev = 0`。
4. **内核缓存超时与独立配置**：
   - `meta.Manager` 维护独立配置：`dirEntryTimeout`（目录）、`entryTimeout`（非目录）与 `attrTimeout`（属性）。
   - 在 `Lookup` 返回的 `Inode` 实体与 `ReadDirPlus` 返回的扩展目录项中统一暴露 `EntryTimeoutSeconds` 与 `AttrTimeoutSeconds`。

## 结果
- **积极影响**：
  - `Lookup` 完整覆盖 POSIX 与 JuiceFS 的特殊目录项解析（`.`、`..`、空名称 `""`）及孤儿父目录拦截边界。
  - 完美解决 `rmdir-whilst-open` 状态下进程在被删目录中执行 `cd ..` 错误自指的死循环 Bug。
  - `Blksize = 65536` 避免用户态标准库退化为 4 KiB 小块 I/O，大幅提升读写吞吐性能。
  - 目录与非目录节点具备独立的内核缓存时效控制。
- **代价考量**：事务内包含多级防御判断，但均为主键点查，开销微乎其微。
