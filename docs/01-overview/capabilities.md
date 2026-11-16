# 系统能力与边界

**Bowl** 是一个面向云原生沙箱与开发环境的远程 POSIX 虚拟磁盘系统。系统由独立的存储服务端（`services/storage`）与 Linux FUSE 客户端（`client`）组成，通过 ConnectRPC over HTTP/2 通信，提供接近本地文件系统的 POSIX 语义以及磁盘级即时 Fork、三路结构化 Diff 与 Merge 能力。

## 核心设计目标

1. **完整远程 POSIX 语义**：在客户端通过 Linux `/dev/fuse` 暴露标准挂载点，支持常规文件随机读写、硬链接/符号链接、扩展属性（`xattr`）、`fallocate` 空间预分配/打洞（`punch hole`）、`O_TMPFILE` 匿名临时文件及建议锁（`flock` / `fcntl`）。
2. **血缘感知与写时复制（CoW）**：每块虚拟磁盘具备持久化状态与独立局部提交树。`ForkDisk` 瞬间从源盘派生写时复制分支并记录全局血缘 DAG；`DiffDisk` 与 `MergeDisk` 基于最近公共祖先（LCA）完成目录树级比对与三路合并。
3. **单写者 Epoch 栅栏**：同一磁盘支持多节点并发只读挂载（`epoch = 0`），而读写挂载通过控制面单调递增的 `writer_epoch` 与软租约续期实现严格脑裂隔离。
4. **控制面与数据面解耦**：PostgreSQL 18 仅持久化全局盘元数据、血缘关系与 Commit DAG；单盘内部所有 Inode、目录项、扩展属性与数据块全部自治于本地 `StoreActor`（Pebble KV + `.pack` 追加封包）。

## 已支持能力全景矩阵

| 能力域 | 已实现特性 | 关键约束与说明 | 源码依据 |
| :--- | :--- | :--- | :--- |
| **磁盘生命周期** | 创建、查询、分页列表、在线扩容/改名、删除 | 在线扩容仅支持增大容量（不允许低于 `used_bytes`）；删除挂载中的磁盘返回 `EBUSY` | `services/storage/internal/control/control.go` |
| **分支、对比与合并** | `ForkDisk`、`DiffDisk`（含 `--patch`、`--conflicts-only`）、`MergeDisk`（`fail`/`ours`/`theirs` 及路径级覆盖 `--resolve`、`--dry-run`） | `diff --patch` 对单文件超过 `32 MiB` 自动跳过文本补丁；`merge` 遇到冲突默认中止并返回退出码 `1` | `services/storage/internal/control/{control,merge_diff}.go` |
| **挂载与并发控制** | 读写挂载（`WriterEpoch` 栅栏 + `RenewMount` 心跳，默认 `--mode staged` 本地磁盘暂存）、多节点只读挂载（`--mode ro`）、后台守护进程与优雅卸载 | 默认租约 `30s`，客户端每 `TTL/3`（最低 `1s`）续约一次；读写挂载仅允许单活跃写者 | `services/storage/internal/control/{control,mount_lease}.go`、`client/internal/cli/mount.go` |
| **POSIX 文件与目录** | `Lookup`、`GetAttr`、`SetAttr`、`Read`/`Write`、`Create`/`Mkdir`/`Mknod`/`Symlink`/`Link`/`Unlink`/`Rmdir`/`Rename`、`ReadDir`/`ReadDirPlus`、`StatFS` | `Rename` 支持 `0`、`RENAME_NOREPLACE`（`0x1`）、`RENAME_EXCHANGE`（`0x2`）；`Mknod` 支持常规文件（`S_IFREG`）、字符/块设备（`S_IFCHR`/`S_IFBLK`，含 `rdev`）、FIFO（`S_IFIFO`）与 Socket（`S_IFSOCK`） | `services/storage/internal/store/namespace.go`、`client/internal/fusefs/` |
| **高级存储特性** | `Fallocate`（默认分配与 `FALLOC_FL_PUNCH_HOLE` / `FALLOC_FL_KEEP_SIZE`）、`xattr`（增删改查列表）、宿主机磁盘 `O_TMPFILE` + `linkat` 物化、顺序预读与有界本地磁盘暂存异步上传 | `user.*` 扩展属性仅允许设置在常规文件、目录与 Socket 上 | `services/storage/internal/store/{xattr_setattr,data_rw}.go`、`client/internal/fusefs/` |
| **传输与可观测性** | ConnectRPC HTTP/2（`h2c` / HTTPS）、客户端 1 条元信息连接 + 8 条数据分片连接池、双端口 Prometheus + OTel 链路追踪 + 客户端 `.stats` 虚拟文件 | `--token` / `$BOWL_TOKEN` 在客户端注入 `Authorization: Bearer` 头 | `client/internal/rpc/client.go`、`services/storage/internal/server/` |

## 硬性边界与配额表

下表所有数值均直接取自 `proto/storage/v1/storage.proto`、服务端配置及存储引擎常量定义：

| 边界项 | 精确上限 / 规格 | 单位 / 约束说明 | 源码依据 |
| :--- | :--- | :--- | :--- |
| **单次 RPC 读写载荷上限** | `4 MiB`（`4194304` 字节） | 适用于 `Read`、`Write`、`ReadCommitNode`；客户端 FUSE 分片与协议层共同约束（注：`store/errors.go` 保留有历史常量 `MaxWriteBytes = 1 MiB`，实际读写网关以 `4 MiB` 为准） | `proto/storage/v1/storage.proto`、`services/storage/internal/server/handler_data.go` |
| **HTTP 服务端请求体默认上限** | `8 MiB`（`8388608` 字节） | 由 `SERVER_READ_MAX_BYTES` 控制；`deploy/dev` 与 `deploy/prod` 中均显式覆盖为 `16 MiB`（`16777216` 字节） | `services/storage/internal/config/config.go`、`deploy/dev/storage.env` |
| **HTTP/2 流控与分片** | 单帧 `1 MiB` / 单流 `8 MiB` | 服务端单连接接收窗口 `128 MiB`；客户端维护 1 条元信息连接（`32 MiB` 窗口）+ `8` 条数据分片连接（每条 `64 MiB` 窗口） | `services/storage/internal/server/server.go`、`client/internal/rpc/client.go` |
| **逻辑分块网格（Block Grid）** | `256 KiB`（`262144` 字节） | `BlobSize = 256 << 10`，文件数据按 `256 KiB` 对齐切块并支持稀疏空洞 | `services/storage/internal/store/block_grid.go`、`blob.go` |
| **`.pack` 封包文件上限** | `64 MiB`（`67108864` 字节） | `PackMaxSize = 64 << 20`，追加写入，达到阈值后封存并滚动新建封包 | `services/storage/internal/store/blob.go` |
| **Pebble 与热块缓存默认配置** | 共享 BlockCache `64 MiB`、热块缓存 `64 MiB` | 热块缓存分 `16` 分片 × `64` 槽位（每分片 `4 MiB`）；MemTable `16 MiB`、WAL 同步 `4 MiB`、最大打开文件数 `128` | `services/storage/internal/store/{actor,blob}.go` |
| **文件名与磁盘名上限** | 文件名 `1 .. 255 B`；磁盘名 `0 .. 255 B` | 文件名禁止 `/` 与空字符 `\x00`；磁盘名允许空字符串，仅禁止 `\x00`（不限制 `/`） | `services/storage/internal/store/errors.go`、`services/storage/internal/control/control.go`、`proto/storage/v1/storage.proto` |
| **符号链接目标上限** | `1 .. 4095 B` | `MaxSymlinkTargetBytes = 4095`，禁止空字符 `\x00` | `services/storage/internal/store/errors.go`、`proto/storage/v1/storage.proto` |
| **单 Inode 硬链接上限** | `65000` | `MaxNlink = 65000`，超出返回 `EMLINK` | `services/storage/internal/store/errors.go` |
| **扩展属性（`xattr`）配额** | 键名 `1 .. 255 B`；单值 `64 KiB`（`65536 B`）；单 Inode 键名列表配额 `64 KiB`（`65536 B`） | 键名禁止空字符；每条属性在键名列表中计入 `len(name) + 1` 字节；不支持 `RENAME` 时自动迁移以外的隐式配额绕过 | `services/storage/internal/store/{errors,xattr_setattr}.go` |
| **目录列举与分页上限** | `ReadDir` / `ReadDirPlus`: `1 .. 4096` 条；`ListDisks` / `DiffDisk`: `page_size <= 1000` | `ListDisks` 未传 `page_size` 时服务端默认 `50`；`DiffDisk` `--prefix` 与 `--page-token` 上限 `4096 B` | `proto/storage/v1/storage.proto`、`services/storage/internal/store/errors.go` |
| **Merge 路径覆盖与 Diff Patch 上限** | `--resolve` 最多 `4096` 项；`--patch` 单文件上限 `32 MiB` | 每条覆盖路径 `1 .. 4096 B`；Myers 文本 Diff 最大编辑距离 `2000` | `proto/storage/v1/storage.proto`、`client/internal/cli/disk_diff.go` |
| **数值边界** | `ino`、`epoch`、`size`、`offset`、`max_bytes`、`used_*` 上限 `9223372036854775807`（`int64` 最大值）；`uid`/`gid`/`flags` 上限 `2147483647`（`int32` 最大值） | 兼容 POSIX 64 位偏移与 32 位属主标识边界 | `proto/storage/v1/storage.proto` |

## 明确的非目标与当前边界

以下是经源码逐项核实后确认的**不支持项或有意限制**，阅读与集成时请勿假设系统具备这些行为：

1. **不支持多节点并发读写同一磁盘**
   - 读写挂载采用**单写者 Epoch 栅栏**（`WriterEpoch > 0`），后发起的读写挂载会递增 `epoch` 并立即 fenced 掉旧写者，不存在多写者共享锁或分布式缓存一致性协议。
   - 多节点共享同一数据集请通过只读挂载（`--mode ro`，`epoch = 0`）或 `bowl disk fork` 派生独立分支再 `merge` 的模式实现。
2. **文件句柄、文件锁与暂存状态仅驻留客户端本地**
   - 客户端的 `HandleTable`（已打开文件描述符映射）、`LockTable`（`GetLk` / `SetLk` / `SetLkw` 建议锁）、`StageCoordinator`（本地磁盘暂存分片状态）、`TmpFileManager`（宿主机磁盘 `O_TMPFILE` 状态）、`NamespaceCache` 与 `capabilityCache` 均为单个客户端实例的本地状态。
   - 因此跨主机挂载同一磁盘时，POSIX `fcntl` 字节范围锁与 `flock` 锁**不会**在多台客户端之间同步，仅对同一挂载点内的本地进程生效。
3. **不支持 `RENAME_WHITEOUT`**
   - `Rename` 仅支持标志位 `0`、`RENAME_NOREPLACE`（`0x1`）与 `RENAME_EXCHANGE`（`0x2`）；传入 `RENAME_WHITEOUT`（`0x4`）、组合标志或未知标志位会直接返回 `EINVAL`（参见 `client/internal/fusefs/fs_rename.go` 与 `services/storage/internal/store/unlink_rename.go`）。
4. **FUSE 挂载强依赖 Linux 内核环境**
   - 控制面 CLI（`bowl disk ...`）与存储服务端（`services/storage`）可跨平台编译运行，但 `bowl mount` 与 `bowl umount` 依赖 Linux `/dev/fuse` 设备节点以及 `fusermount3` / `fusermount` / `umount` 工具，无法在未提供 FUSE 内核模块的 macOS 宿主机直接执行。
5. **服务端未内置 Token 校验拦截器**
   - 客户端在配置 `--token` 或 `$BOWL_TOKEN` 时会在每个 ConnectRPC 请求上注入 `Authorization: Bearer <token>` 请求头（参见 `client/internal/rpc/client.go`），但当前 `services/storage/internal/server/` 中尚未实现服务端鉴权拦截器，生产部署需依赖内网隔离或前端反向代理/TLS 终止。
6. **默认服务端元数据缓存策略偏保守**
   - 服务端默认启用 `STORAGE_NOATIME=true`（避免读取刷写 `atime` 放大写负载），同时默认关闭内核页缓存与目录缓存（`STORAGE_OPEN_KEEP_CACHE=false`、`STORAGE_OPEN_DIR_CACHE=false`），属性/目录项超时为 `1.0s`（参见 `services/storage/internal/config/config_meta.go`）。
7. **暂不支持跨存储节点分片与自动副本**
   - 单盘的 `StoreActor`、Pebble 实例与 `.pack` 文件均落在单个 `services/storage` 节点的数据目录（`DATA_DIR`）下，控制面不支持将单盘条带化分散至多个存储节点。

## 延伸阅读

- [系统架构与仓库地图](/01-overview/architecture) —— 了解双层存储拓扑与 Go 多模块组织方式。
- [快速开始](/01-overview/quickstart) —— 本地启动服务并跑通从创建到合并的完整闭环。
- [挂载与 Epoch 栅栏](/02-features/mount-and-fencing) —— 深入理解 `WriterEpoch`、租约续期与脑裂防护机制。
