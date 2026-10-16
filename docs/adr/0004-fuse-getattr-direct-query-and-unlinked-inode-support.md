# 0004. FUSE GetAttr 单语句直查与已解链孤儿节点属性支持设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在完成 `Lookup` 后，需要实现对接 Linux FUSE 内核 `FUSE_GETATTR` 协议的 `GetAttr` 能力。在遵循强 FUSE 规范时，面临两项关键设计权衡：
1. **单节点读取的事务开销**：`Lookup` 因涉及“父节点类型校验 + 子目录项解析”两步读取，在 ADR-0003 中采用了 `RepeatableRead` 只读事务以冻结一致性快照。而 `GetAttr` 仅根据主键 `ino` 查询单行 `inodes` 记录，若同样包裹显式只读事务，会在高频 `getattr` 调用下产生不必要的 `BEGIN READ ONLY` / `COMMIT` 往返开销。
2. **`unlink-whilst-open`（已删除但仍打开的文件）属性读取与整型边界**：POSIX 与 FUSE 规范允许进程在打开文件后执行 `unlink`，此时目录项已移除且 Inode 的硬链接数 `nlink` 归零，但在文件描述符关闭（`FUSE_RELEASE`）前，内核仍会针对该 `ino` 发起 `FUSE_GETATTR`（如 `fstat`）并要求正常返回 `st_nlink = 0` 的属性。此外，FUSE 协议中 `fuse_ino_t` 为无符号 64 位整数（含哨兵值 `FUSE_UNKNOWN_INO = 0xffffffffffffffffULL`），而 PostgreSQL `inodes.ino` 为有符号 64 位整数 `BIGINT`。

## 决策
1. **单语句直查跳过显式事务**：
   - `meta.Manager.GetAttr` 直接调用 `Store.GetInode(ctx, int64(ino))`，不开启 `ExecTxWithOptions` 显式事务，直接利用 PostgreSQL 单条 `SELECT` 语句天然具备的语句级一致性快照。
2. **支持 `nlink == 0` 孤儿节点并补齐时间戳必填契约**：
   - 移除 `proto/storage/v1/storage.proto` 中 `InodeAttribute.nlink` 的 `gt = 0` 校验约束，允许 `GetAttr` 正常返回 `nlink = 0` 的未释放 Inode。
   - 为 `InodeAttribute` 的 `atime`、`mtime`、`ctime` 补齐 `[(buf.validate.field).required = true]` 约束，严格对齐 FUSE `struct fuse_attr` 非空时间戳语义与数据库 `NOT NULL` 约束。
3. **`uint64` 至 `BIGINT` 的上界溢出防御**：
   - 在 `GetAttrRequest.ino` 的 Protobuf 校验规则中同时声明 `gt = 0` 与 `lte = 9223372036854775807`（`math.MaxInt64`），并在 `meta.Manager.GetAttr` 入口拦截 `ino == 0 || ino > math.MaxInt64` 返回 `ErrInvalidArgument`，防止无符号 64 位整数向 `int64` 转换时发生符号位溢出。

## 结果
- **积极影响**：`GetAttr` 消除了多余的数据库事务往返，完整兼容 FUSE `unlink-whilst-open` 语义，并在契约与领域双层阻断了 64 位整型溢出风险。
- **代价考量**：`GetAttr` 与 `Lookup` 在 `meta` 层的 `Store` 调用形态不同（直调 `GetInode` vs `ExecTxWithOptions`），测试打桩需分别针对单方法调用与事务闭包进行断言。
