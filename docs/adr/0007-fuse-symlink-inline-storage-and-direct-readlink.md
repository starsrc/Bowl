# 0007. FUSE Symlink 单表内联存储与 Readlink 单语句直查设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在实现符号链接创建（`Symlink`）与读取（`Readlink`）能力时，需要持久化符号链接指向的目标路径字符串（`target`）。在现有 `inodes` 与 `entries` 双表模型下，面临三项关键架构权衡：
1. **目标路径的持久化布局**：是新建独立的 `symlinks (ino, target)` 关联表，还是在 `inodes` 表内联可空列 `symlink_target TEXT`？独立关联表虽然使 `inodes` 表保持纯数值列，但在 `Symlink` 写入时引入跨三表操作，且可通过应用层与数据库 `CHECK` 约束消除单表可空列的类型歧义。
2. **符号链接的 POSIX 属性不变量**：POSIX 与 FUSE 规范要求符号链接的权限模式固定为 `S_IFLNK | 0o777`（`0o120777`），其物理大小 `size` 等于目标路径字符串的字节长度（不含结尾空字节），且目标路径允许包含 `'/'` 但严禁包含空字节 `'\x00'`。
3. **`Readlink` 的事务开销与读取边界**：与 `GetAttr` 类似，`Readlink` 仅根据主键 `ino` 读取单个符号链接节点的目标路径；同时需明确对非符号链接节点（`!= S_IFLNK`）及已解链孤儿节点（`Orphan Inode`，`nlink == 0`）的访问语义。

## 决策
1. **`inodes` 表内联 `symlink_target` 列与一致性约束**：
   - 新增数据库迁移，在 `inodes` 表添加可空列 `symlink_target TEXT`，并施加 `CHECK` 约束：当 `(mode & 61440) = 40960`（即 `(mode & 0o170000) = 0o120000`，`S_IFLNK`）时 `symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096`，其余文件类型必须为 `symlink_target IS NULL`。
   - `meta.Inode` 与 Protobuf `InodeAttribute` 保持纯粹的 POSIX `stat` 属性结构，不混入 `symlink_target` 字段；目标路径仅通过 `Readlink` 接口返回。
2. **`Symlink` 创建契约与父目录联动**：
   - `SymlinkRequest` 包含 `parent_ino`、`name`、`target`、`uid`、`gid`（不设 `mode` 字段）。`target` 字节长度限制为 `1..4096` 且禁止包含 `'\x00'`。
   - 在读写事务中先以 `GetInodeForUpdate(parent_ino)` 锁定父目录（校验存在、为目录且 `nlink > 0`；若 `name` 为 `"."` 或 `".."` 返回 `ErrAlreadyExists`）。
   - 新建符号链接 `Inode` 的 `mode` 固定设为 `S_IFLNK | 0o777`（`0o120777`），`size` 设为 `len(target)`，`nlink = 1`；若父目录含 `S_ISGID (0o2000)` 位，新节点 `gid` 自动继承父目录 `gid`。随后绑定 `Entry`（捕获 `23505` 唯一冲突映射为 `ErrAlreadyExists`）并刷新父目录 `mtime` 与 `ctime`。
3. **`Readlink` 单语句直查与类型校验**：
   - `meta.Manager.Readlink` 不开启显式事务，直接按主键 `ino` 单语句查询 `inodes` 记录（兼容 `nlink == 0` 的 `Orphan Inode`）。
   - 若节点不存在返回 `ErrNotFound`（`ENOENT`）；若节点存在但 `(mode & S_IFMT) != S_IFLNK`，返回 `ErrInvalidArgument`（`EINVAL`）。

## 结果
- **积极影响**：`Symlink` 与 `Readlink` 无需维护额外的关联表或执行跨表 JOIN，`Readlink` 具备与 `GetAttr` 相同的单语句低延迟读取性能，同时通过数据库 `CHECK` 约束保障了非软链节点无脏数据残留。
- **代价考量**：`inodes` 表增加了一个变长可空列 `symlink_target`，但在 PostgreSQL 中 `NULL` 值仅占用行头位图（null bitmap）中的 1 bit，对常规文件与目录不产生额外存储膨胀。
