# 0003. POSIX Lookup 只读事务两步校验与块属性动态推导设计

## 状态
已采纳 (accepted)

## 背景
在实现 Storage 元数据引擎的 `Lookup` 能力时，需要同时满足两项核心诉求：
1. **严格遵循 POSIX `lookup(parent_ino, name)` 错误语义**：必须准确区分“父节点或目标子项不存在”（`ENOENT`）与“父节点存在但不是目录”（`ENOTDIR`）。若仅使用单条 `entries JOIN inodes` 查询，未命中时无法区分是父节点非目录还是子项不存在。
2. **精简数据库持久化存储**：`inodes` 表仅持久化核心物理状态（`mode`, `size`, `nlink`, `uid`, `gid`, `rdev`, 时间戳），而未冗余存储 POSIX `struct stat` 中的 `blksize`（块大小）与 `blocks`（占用的 512 字节块数）。

## 决策
1. **数据访问与事务架构**：
   - 采用 `sqlc` + `pgx/v5` 生成类型安全的 `Querier` 接口，并结合 `github.com/starsrc/Aether/pkg/tx` 的泛型 `Transactor[Querier]` 构建 `Store` 抽象，确保编译期类型安全与事务生命周期安全。
2. **只读可重复读事务两步校验**：
   - `Lookup` 在进入数据库前先执行入参防御校验（`parent_ino > 0`、`name` 字节长度 `1..255` 且不含 `'/'` 与 `'\x00'`），非法时返回 `ErrInvalidArgument`。
   - 通过 `ExecTxWithOptions(ctx, ..., tx.WithReadOnly(), tx.WithIsoLevel(tx.RepeatableRead))` 开启只读可重复读事务，冻结单一事务快照并按序执行两步查询：
     1. 查询父节点 `GetInode(parent_ino)`：若不存在则返回 `ErrNotFound`（`ENOENT`）；若存在但 `(mode & S_IFMT) != S_IFDIR`（`0o170000` / `0o040000`），立即返回 `ErrNotDir`（`ENOTDIR`）。
     2. 查询目标目录项对应的节点 `GetInodeByEntry(parent_ino, name)`：若不存在则返回 `ErrNotFound`（`ENOENT`）。
3. **块属性内存动态推导**：
   - `inodes` 表不新增 `blksize` 与 `blocks` 列。
   - 领域层在构造 `Inode` 的 `Attribute` 时，固定 `Blksize = 4096`，并按 `Blocks = (Size + 511) / 512` 动态计算 512 字节块数。

## 结果
- **积极影响**：`Lookup` 行为与 POSIX 文件系统标准完全一致；只读事务保障了父目录状态检查与目录项解析在同一快照下完成；数据库表结构保持无冗余正交设计。
- **代价考量**：每次 `Lookup` 在事务内包含两次主键/唯一索引点查，但由于均命中主键 B-Tree 索引且处于同一数据库会话事务内，开销极小且换取了严格的语义正确性。
