# 0007. FUSE Symlink 单表内联存储、Readlink 单语句直查与损坏目标防御设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在实现符号链接创建（`Symlink`）与读取（`Readlink`）能力时，需要持久化符号链接指向的目标路径字符串（`target`）。在现有 `inodes` 与 `entries` 双表模型下，面临以下关键架构权衡：
1. **目标路径的持久化布局**：在 `inodes` 表内联可空列 `symlink_target TEXT`，并通过应用层与数据库 `CHECK` 约束确保只有 `S_IFLNK` 节点持久化目标路径。
2. **符号链接的 POSIX 属性不变量**：权限模式固定为 `S_IFLNK | 0o777`（`0o120777`），物理大小 `size` 等于目标路径字节长度，目标路径允许包含 `'/'` 但严禁包含空字节 `'\x00'`。
3. **`Readlink` 损坏目标防御与错误映射 (STARSRC-313)**：当底层数据库记录出现异常导致符号链接节点的 `symlink_target` 为空时，属于服务端内部数据损坏，必须返回内部存储错误（`EIO` / `codes.Internal`），严禁向客户端伪装为参数错误 `EINVAL`。
4. **`atime` 访问时间策略 (STARSRC-256, STARSRC-339)**：默认遵循 `noatime` 读路径免更新策略；在配置 `relatime` 或 `strictatime` 时按规则触发 `TouchInodeAtime`。

## 决策
1. **`inodes` 表内联 `symlink_target` 列与一致性约束**：
   - 在 `inodes` 表添加可空列 `symlink_target TEXT`，施加 `CHECK` 约束：当 `(mode & S_IFMT) = S_IFLNK` 时 `symlink_target IS NOT NULL AND length(symlink_target) BETWEEN 1 AND 4096`，其余文件类型必须为 `symlink_target IS NULL`。
2. **`Symlink` 创建契约与父目录联动**：
   - 目标路径长度校验 `1..4095` 且禁止包含 `'\x00'`。
   - 在读写事务中锁定父目录（校验存在、为目录且 `nlink > 0`）。
   - 新建符号链接 `Inode` 模式为 `S_IFLNK | 0o777`，`size` 为 `len(target)`，`nlink = 1`；继承父目录 `S_ISGID`。
3. **`Readlink` 直查、类型校验与 EIO 防御 (STARSRC-313)**：
   - 入参防御校验 `ino == 0 || ino > math.MaxInt64`（返回 `ErrInvalidArgument`）。
   - 单语句读取 `GetSymlinkTarget(ctx, ino)`：不存在返回 `ErrNotFound`（`ENOENT`）。
   - 类型校验：若 `(mode & S_IFMT) != S_IFLNK`，返回 `ErrInvalidArgument`（`EINVAL`）。
   - 损坏目标防御：若目标节点确认为 `S_IFLNK` 但 `len(row.SymlinkTarget) == 0`，返回 `ErrIO`（映射为 `codes.Internal` + `reason: "EIO"`），明确区分客户端入参错误与服务端存储损坏。
   - `atime` 联动：默认 `AtimeModeNoAtime` 不触发写库；当配置 `RelAtime` 或 `StrictAtime` 且满足刷新条件时，调用 `TouchInodeAtime` 刷新 `atime`。

## 结果
- **积极影响**：
  - `Readlink` 保持单语句点查极低开销，同时严格遵循 POSIX 错误分类，底层记录损坏时如实暴露 `EIO`。
  - 完善了访问时间策略支持，与 JuiceFS 语义完整对齐。
- **代价考量**：对损坏数据增加了防御分支判断，开销忽略不计。
