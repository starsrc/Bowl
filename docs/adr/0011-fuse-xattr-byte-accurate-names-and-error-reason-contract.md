# 0011. FUSE 扩展属性（Xattr）、全量路径名与符号链接字节化及结构化错误原因契约设计

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成目录流遍历与 `Fallocate`（ADR-0010）后，需推进 Linux FUSE 扩展属性相关的四项核心操作码：`FUSE_SETXATTR (22)`、`FUSE_GETXATTR (23)`、`FUSE_LISTXATTR (24)` 与 `FUSE_REMOVEXATTR (25)`。在对齐 Linux VFS 与 FUSE 协议语义时，面临六项关键架构权衡：
1. **非 UTF-8 原始字节序列的无损传输与存储**：Linux VFS 中目录项名称（`dentry`）、扩展属性名称（`xattr name`）及符号链接目标路径（`symlink target`）均为除 `\x00`（目录项另禁 `/`）外的任意字节流，不要求合法 UTF-8。此前 `entries.name`（`VARCHAR(255)`，ADR-0001）、`inodes.symlink_target`（`TEXT`，ADR-0007）及 Protobuf `string` 字段会拒绝非 UTF-8 字节序列；且 `protovalidate` 的 `bytes.pattern` 规则在底层 CEL 中执行 `string(this)` 强转，遇到非 UTF-8 字节会直接抛出运行时异常。
2. **Xattr 持久化模型、空值语义与单节点名称列表配额**：POSIX `setxattr(2)` 允许 `0` 字节空属性值（不代表删除），单属性值上限为 `XATTR_SIZE_MAX = 65536` 字节；同时 Linux `listxattr(2)` 的单次返回缓冲区上限为 `XATTR_LIST_MAX = 65536` 字节。若不对单 Inode 累计属性名长度设限，属性虽能持续写入，但一旦 `Σ(len(name)+1) > 65536`，后续 `listxattr` 将永久返回 `E2BIG` 导致属性列表不可读。
3. **RPC 契约边界与 FUSE 双模态 `size` 探测解耦**：Linux FUSE 的 `FUSE_GETXATTR` 与 `FUSE_LISTXATTR` 请求携带 `size` 参数（`size == 0` 探测长度，`size > 0` 读取数据并校验 `ERANGE`），需界定该缓冲区协议细节由服务端 RPC 处理还是由客户端 FUSE 适配层本地完成。
4. **VFS 与 Storage 的权限校验分工、安全前提及 `user.*` 文件类型边界**：Linux VFS `xattr_permission` 既包含依赖调用方凭证的检查（粘滞位目录属主、`trusted.*` 的 `CAP_SYS_ADMIN` 及常规 DAC 权限检查），也包含不依赖凭证的文件类型检查（`user.*` 的节点类型限制），需明确客户端挂载前提、网络信任边界及内核版本兼容基线。
5. **POSIX ACL 与 `FUSE_HANDLE_KILLPRIV` / `FUSE_HANDLE_KILLPRIV_V2` 的协商策略**：扩展属性涉及 `system.posix_acl_access`、`system.posix_acl_default` 与 `security.capability`，需明确 Storage 是否解析 ACL 或在元数据变更时联动清理 `security.capability`。
6. **gRPC 状态码向 POSIX `errno` 的无损还原与名称超长错误区分**：仅靠 gRPC 标准状态码无法一对一还原 `errno`（如 `NotFound` 同时对应 `ENOENT` 与 `ENODATA`；`FailedPrecondition` 同时对应 `ENOTDIR`、`EISDIR`、`ENOTEMPTY`、`EBADF`；`InvalidArgument` 同时对应 `EINVAL`、`ERANGE`、`E2BIG`、`ENAMETOOLONG`），目录项/符号链接超长（`ENAMETOOLONG`）与 Xattr 名称超长（`ERANGE`）遵循不同的内核错误码规范，且 `Unimplemented` 若被客户端误还原为 `ENOSYS` 会导致内核永久禁用对应 FUSE 操作码。

## 决策
1. **全项目名称与符号链接目标统一字节化（`bytes` / `BYTEA`）、`CASE` 约束防护及 `protovalidate` 预编译**：
   - **数据库迁移、存量边界收敛与非 UTF-8 可逆回滚编码**：
     - 新建 `inode_xattrs` 表：`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`name BYTEA NOT NULL`、`value BYTEA NOT NULL`，主键为 `(ino, name)`；附加约束 `chk_inode_xattrs_name CHECK (octet_length(name) BETWEEN 1 AND 255 AND position('\x00'::bytea IN name) = 0)` 与 `chk_inode_xattrs_value CHECK (octet_length(value) <= 65536)`。
     - 将 `entries.name` 从 `VARCHAR(255)` 迁移为 `BYTEA NOT NULL`，附加约束 `chk_entries_name CHECK (octet_length(name) BETWEEN 1 AND 255 AND position('\x00'::bytea IN name) = 0 AND position('\x2f'::bytea IN name) = 0)`（更新 ADR-0001 中关于 `VARCHAR(255)` 的定义）；回滚（`Down`）时对非 UTF-8 原始字节采用 Unicode 私用区（PUA `U+E000..U+E0FF`）单字节一字符可逆编码（`Up` 时自动解码还原），确保回滚兼容非 UTF-8 存量数据且不超出 `VARCHAR(255)` 字符上限。
     - 将 `inodes.symlink_target` 从 `TEXT` 迁移为 `BYTEA`，在添加新 `CHECK` 约束前先将存量 `> 4095` 字节（旧版允许 `4096` 字节）的符号链接目标平滑截断至 `4095` 字节并同步收敛 `size = LEAST(size, 4095)`，随后将最大有效载荷上限调整为 `4095` 字节（对齐 Linux `PATH_MAX - 1`，预留末尾 NUL 字节，更新 ADR-0007 中关于 `TEXT` 与 `4096` 上限的定义）；回滚（`Down`）同样采用 PUA 可逆编码；使用基于 `(mode & 61440) = 40960` 的 `CASE` 完整结构更新 `chk_inodes_symlink_target` 约束，防止 SQL 三值逻辑下的 `NULL` 穿透：
       ```sql
       ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target CHECK (
           CASE
               WHEN (mode & 61440) = 40960 THEN
                   symlink_target IS NOT NULL
                   AND octet_length(symlink_target) BETWEEN 1 AND 4095
                   AND position('\x00'::bytea IN symlink_target) = 0
               ELSE
                   symlink_target IS NULL
           END
       );
       ```
   - **时间戳统一采用 `clock_timestamp()`**：
     - 在 `TouchInodeCtime`、`TouchInodeTimestamps` 等 SQL 语句中，统一使用 `clock_timestamp()` 替代 `NOW()`（`transaction_timestamp()`），确保同一事务或高频操作内 `ctime` / `mtime` 单调推进。
   - **Protobuf 契约、`protovalidate` CEL 字节规则与违规 `errno` 显式转换**：
     - 全项目请求与响应中的目录项名称（`Lookup`、`Mknod`、`Mkdir`、`Symlink`、`Link`、`Unlink`、`Rmdir`、`Rename`、`Create`、`DirEntry`、`DirEntryPlus`）、扩展属性名称（`SetXattr`、`GetXattr`、`RemoveXattr`、`ListXattrResponse.names`）及符号链接路径（`SymlinkRequest.target`、`ReadlinkResponse.target`）统一定义为 `bytes`（Go 领域层对应 `[]byte`）。
     - 弃用会触发 UTF-8 强转的 `bytes.pattern`，改用 `bytes.min_len` / `bytes.max_len` 配合字段级 CEL 原生字节函数 `!this.contains(b'\\x00')`（目录项名称额外叠加 `&& !this.contains(b'/')`），并在 `ListXattrResponse.names` 上配置 `repeated.items` 约束；服务启动阶段通过 `protovalidate.WithMessages(...)` 预编译全部消息校验器，并在 `pkg/validator` 中将字段边界违规按领域层求值顺序显式转换为携带 `google.rpc.ErrorInfo`（`ERANGE`、`E2BIG`、`ENAMETOOLONG`、`EINVAL`）的 gRPC `InvalidArgument` 状态错误。
   - **`nil` 空字节切片写库防御**：由于 `protobuf-go` 将 0 字节 `bytes` 解码为 `nil` 且 `pgx` 会将 `nil []byte` 序列化为 SQL `NULL`，`meta.Manager` 在写入 `inode_xattrs.value` 前必须将 `nil` 归一化为非空切片 `[]byte{}`，确保 0 字节属性值正常持久化。
2. **结构化 Xattr RPC 契约与确定性字节序列表**：
   - `GetXattr` 接收 `(ino, name)` 并返回 `bytes value`；`ListXattr` 接收 `(ino)` 并按 `ORDER BY name ASC`（`BYTEA` 原始字节序升序，作为 Bowl 确定性输出约定）返回 `repeated bytes names`。
   - FUSE 内核协议中的 `size == 0` 缓冲区长度探测、`\0` 分隔字节流拼接以及 `size < actual_len` 时的 `ERANGE` 判定，均由客户端 FUSE 适配层本地完成，不在元数据 RPC 层耦合 `size` 参数。
3. **VFS 与 Storage 职责边界、安全前提、求值顺序及 Bowl 内部配额策略**：
   - **安全前提与职责切分**：
     - **强制挂载前提与信任边界**：FUSE 客户端挂载时**必须开启 `-o default_permissions`**，由客户端 Linux 内核 VFS 权威执行基于调用方凭证的 DAC 权限检查、粘滞位（Sticky Bit）目录属主检查以及 `trusted.*` 命名空间的 `CAP_SYS_ADMIN` 能力检查；Storage RPC 严格限定为内网受信 FUSE 客户端代理调用的后端服务，不向非受信终端直接暴露。
     - **Storage 实施策略 A（不依赖凭证的 `user.*` 文件类型限制，对齐 Linux 7.1+ 基线）**：当 `name` 以 `"user."` 开头时，仅允许目标 Inode 为常规文件（`S_IFREG`）、目录（`S_IFDIR`）或套接字（`S_IFSOCK`，对齐 Linux 7.1+ 兼容基线）；对符号链接、字符设备、块设备与 FIFO 执行 `user.*` 的 `SetXattr` / `RemoveXattr` 返回 `ErrPerm`（`EPERM`），执行 `GetXattr` 返回 `ErrNoData`（`ENODATA`）。`ListXattr` 不按调用方过滤，与内核 FUSE 行为一致。所有 Inode 类型及孤儿节点（`nlink == 0`）均支持扩展属性操作。
   - **求值顺序与并发锁协议**：
     1. **入参静态校验（不查库）**：
        - `ino == 0 || ino > math.MaxInt64` 返回 `ErrInvalidArgument`（`EINVAL`）；
        - `SetXattr` 中 `flags &^ 0x3 != 0` 返回 `ErrInvalidArgument`（`EINVAL`）；
        - Xattr 名称 `len(name) == 0 || len(name) > 255` 返回 `ErrRange`（`ERANGE`，对齐 Linux `import_xattr_name`），而目录项名称 `len(name) > 255` 或符号链接目标 `len(target) > 4095` 则返回 `ErrNameTooLong`（`ENAMETOOLONG`）；
        - 名称包含 `\x00`（`bytes.IndexByte(name, 0) >= 0`）返回 `ErrInvalidArgument`（`EINVAL`）；
        - `SetXattr` 中 `len(value) > 65536` 返回 `ErrTooBig`（`E2BIG`）。
     2. **Inode 锁与存在性校验**：`SetXattr` 与 `RemoveXattr` 在读写事务内调用 `GetInodeForUpdate(ino)` 加行级排他锁；`GetXattr` 与 `ListXattr` 在 `RepeatableRead` 只读事务内调用 `GetInode(ino)`；节点不存在统一返回 `ErrNotFound`（`ENOENT`）。
     3. **`user.*` 文件类型准入校验**：先于属性存在性查询执行，不满足文件类型要求时按操作类型返回 `ErrPerm` 或 `ErrNoData`。
     4. **`XATTR_CREATE (0x1)` / `XATTR_REPLACE (0x2)` 独立判定与 Bowl `XATTR_LIST_MAX (65536)` 配额**：`SetXattr` 查询 `(ino, name)` 是否已存在——若已存在且含 `XATTR_CREATE` 返回 `ErrAlreadyExists`（`EEXIST`）；若不存在且含 `XATTR_REPLACE` 返回 `ErrNoData`（`ENODATA`）（因此 `flags == 0x3` 在属性存在时返回 `EEXIST`、不存在时返回 `ENODATA`，而非 `EINVAL`）。仅当 `(ino, name)` 尚不存在（新增属性名）时，在持有 Inode `FOR UPDATE` 行锁下计算该节点现有 `Σ(octet_length(name) + 1)`；若新增后总长超过 `65536` 字节（Bowl 内部配额策略，对齐 ext4 / tmpfs），返回 `ErrNoSpace`（`ENOSPC`）；覆盖已有同名属性时不触发配额拦截。
     5. **时间戳联动**：`SetXattr` 与 `RemoveXattr` 成功后仅调用 `TouchInodeCtime(ino)` 刷新 `ctime = clock_timestamp()`，不修改 `mtime` / `atime`。
4. **不协商 `FUSE_POSIX_ACL`、同时禁用 `FUSE_HANDLE_KILLPRIV` 与 `FUSE_HANDLE_KILLPRIV_V2`**：
   - **不协商 `FUSE_POSIX_ACL`**：`system.posix_acl_access` 与 `system.posix_acl_default` 作为普通扩展属性透传存储；Storage 不解析 ACL 二进制结构、不同步 `Inode.mode`、不剥离 `S_ISGID`、不执行默认 ACL 继承，内核亦不执行 ACL 权限检查。限制说明：当客户端在非初始用户命名空间（non-init user namespace）中挂载时，Linux 内核会对 ACL 相关操作直接返回 `EOPNOTSUPP`（`fuse_no_acl`）。
   - **同时禁用 `FUSE_HANDLE_KILLPRIV` 与 `FUSE_HANDLE_KILLPRIV_V2`**：写入、截断或 `chown` 时的特权位与能力剥离完全由 Linux 内核负责（内核自行下发 `FUSE_SETATTR` 剥离 `S_ISUID` / `S_ISGID`，并下发 `FUSE_REMOVEXATTR` 删除 `security.capability`）。Storage 无需在 `SetAttr`、`Open(O_TRUNC)` 或 `Fallocate` 中隐式删除 `security.capability`（ADR-0005、ADR-0009、ADR-0010 中已有的 `S_ISUID` / `S_ISGID` 剥离逻辑与内核行为幂等一致，予以保留）。
5. **基于 `google.rpc.ErrorInfo` 的全项目 `errno` 精确映射契约**：
   - 全项目统一在 gRPC 错误详情 `google.rpc.ErrorInfo` 中设置 `domain = "bowl.storage"`，并在 `reason` 字段中直接携带标准 POSIX `errno` 名称，FUSE 客户端适配层优先读取 `ErrorInfo.reason` 还原内核错误码，gRPC 状态码仅作为大类回退。
   - **严禁还原为 `ENOSYS`**：FUSE 客户端必须将 `(Unimplemented, "EOPNOTSUPP")` 还原为 `EOPNOTSUPP`，严禁将 gRPC `Unimplemented` 映射为 `ENOSYS`，防止 Linux 内核因收到 `ENOSYS` 而永久禁用对应 FUSE 操作码。
   - 全量 14 个领域哨兵错误映射关系固定如下：
     - `ErrInvalidArgument` $\rightarrow$ `InvalidArgument` + `reason: "EINVAL"`
     - `ErrNameTooLong` $\rightarrow$ `InvalidArgument` + `reason: "ENAMETOOLONG"`
     - `ErrRange` $\rightarrow$ `InvalidArgument` + `reason: "ERANGE"`
     - `ErrTooBig` $\rightarrow$ `InvalidArgument` + `reason: "E2BIG"`
     - `ErrNotFound` $\rightarrow$ `NotFound` + `reason: "ENOENT"`
     - `ErrNoData` $\rightarrow$ `NotFound` + `reason: "ENODATA"`
     - `ErrAlreadyExists` $\rightarrow$ `AlreadyExists` + `reason: "EEXIST"`
     - `ErrPerm` $\rightarrow$ `PermissionDenied` + `reason: "EPERM"`
     - `ErrNotDir` $\rightarrow$ `FailedPrecondition` + `reason: "ENOTDIR"`
     - `ErrIsDir` $\rightarrow$ `FailedPrecondition` + `reason: "EISDIR"`
     - `ErrNotEmpty` $\rightarrow$ `FailedPrecondition` + `reason: "ENOTEMPTY"`
     - `ErrBadFileHandle` $\rightarrow$ `FailedPrecondition` + `reason: "EBADF"`
     - `ErrNoSpace` $\rightarrow$ `ResourceExhausted` + `reason: "ENOSPC"`
     - `ErrNotSupported` $\rightarrow$ `Unimplemented` + `reason: "EOPNOTSUPP"`

## 结果
- **积极影响**：彻底打通了非 UTF-8 原始字节在目录项、符号链接与扩展属性全链路中的无损存取，通过 `CASE` 约束消除了 `NULL` 穿透风险，通过 Inode 行级排他锁与 `XATTR_LIST_MAX` 配额消除了 `listxattr` 永久 `E2BIG` 死锁隐患，并通过 `google.rpc.ErrorInfo`（`domain = "bowl.storage"`）精确区分了 `ENAMETOOLONG` 与 `ERANGE`、`ENOENT` 与 `ENODATA` 等同大类 POSIX `errno`。
- **代价考量**：将存量 `entries.name` 与 `inodes.symlink_target` 从字符串类型迁移为 `BYTEA` 并区分 `ErrNameTooLong` 与 `ErrInvalidArgument`，需要在契约层（Protobuf）、查询层（`sqlc`）与领域层（`meta`）同步更新存量签名与测试，在同一迭代内一步到位完成可避免后续二次破坏性变更。
