# 0004. FUSE GetAttr 单语句直查、孤儿节点活跃句柄校验与属性映射设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务对接 Linux FUSE 内核 `FUSE_GETATTR` 协议的 `GetAttr` 能力，面临以下关键设计权衡：
1. **单节点读取的事务开销**：`GetAttr` 仅根据主键 `ino` 查询单行 `inodes` 记录，无需包裹显式只读事务，直接利用 PostgreSQL 单条 `SELECT` 语句天然具备的语句级快照以消除事务往返延迟。
2. **`unlink-whilst-open` 孤儿节点生命周期校验 (STARSRC-279)**：POSIX 规范允许已解链节点（`nlink == 0`）在持有打开文件句柄期间继续执行 `fstat`。但对于已解链且**无任何活跃打开句柄**的孤儿节点，必须严格拦截其属性访问并返回 `ENOENT`，杜绝未回收孤儿节点的脏读。
3. **内核缓存超时与属性隔离对齐**：返回实体统一暴露 `EntryTimeoutSeconds` 与 `AttrTimeoutSeconds`，并对齐 `Blksize = 65536` 及文件类型隔离。

## 决策
1. **单语句直查跳过显式事务**：
   - `meta.Manager.GetAttr` 入参校验 `ino == 0 || ino > math.MaxInt64`（返回 `ErrInvalidArgument`）。
   - 直接调用 `Store.GetInode(ctx, int64(ino))`，若记录不存在返回 `ErrNotFound`（`ENOENT`）。
2. **孤儿节点活跃句柄严格校验 (STARSRC-279)**：
   - 当读取到 `target.Nlink == 0` 时，调用 `Store.HasFileHandlesByIno(ctx, int64(ino))` 探测是否存在活跃打开句柄。
   - 若不存在活跃句柄（`!hasHandles`），立即返回 `ErrNotFound`（`ENOENT`）。
   - 仅当存在打开句柄时，允许正常返回 `st_nlink = 0` 的属性。
3. **统一属性推导与缓存超时暴露**：
   - 使用 `m.newInode(target)` 进行实体构建，继承全局配置的 `attrTimeout` 以及针对目录（`dirEntryTimeout`）/非目录（`entryTimeout`）的条目缓存有效期。
   - 对齐 `Blksize = 65536` (`0x10000`)；目录默认 `Size = 4096`、`Blocks = 8`；特殊文件（`S_IFCHR`, `S_IFBLK`, `S_IFIFO`, `S_IFSOCK`）隔离 `Size = 0` 与 `Blocks = 0`。
4. **全链路 gRPC Status 与 POSIX errno 契约映射**：
   - 全领域 22 个哨兵错误在 `ToStatus` 中映射为对应的 gRPC Code 并在 `google.rpc.ErrorInfo` 中注入 `reason`（如 `ENOENT`, `EINVAL`, `ENOTDIR`, `EIO`, `EACCES`）与 `domain: bowl.storage`。

## 结果
- **积极影响**：
  - 杜绝了无句柄孤儿节点在异步 GC 延迟回收期间被外部 `GetAttr` 访问的数据不一致问题。
  - 保证持有活跃打开句柄的孤儿文件合法 `fstat` 正常工作。
  - 统一暴露内核属性与条目缓存超时参数，支持客户端灵活配置。
- **代价考量**：对 `nlink == 0` 的孤儿节点增加了一次轻量句柄表点查（`HasFileHandlesByIno`），但仅在已解链文件访问时触发，对常规热点文件零额外开销。
