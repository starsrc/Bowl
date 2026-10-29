# 0020. Linux FUSE 客户端无缓存直通架构、会话自愈生命周期与容器化测试边界

## 状态
已采纳 (accepted) — 演进 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md)、[ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md) 与 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)

## 背景
在 `StorageService` 完成 38 项 POSIX 元数据、商业 `Disk` 配额核算与基于不可变 `Chunk-Slice-Block` 的数据读写闭环（ADR-0001 至 ADR-0019）后，系统需要交付面向 Linux 环境的 `bowl` CLI 客户端，通过 FUSE 将指定 `Disk` 挂载为本地标准文件系统（支持多客户端并发共享挂载同一 `Disk`）。在确定客户端架构边界时，面临五项核心权衡：
1. **客户端缓存深度与服务端不可变切片封装的冲突**：以 JuiceFS 为代表的胖客户端之所以能在本地磁盘安全缓存数据块，是因为客户端直接持有全局不可变的物理 `sliceId` 直连对象存储；而 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 已将 `Slice` 与 `Block` 内聚隐藏于 `StorageService` 内部，客户端仅能按可变的 `(ino, offset)` 调用 `Read` / `Write`。在多客户端并发共享挂载同一 `Disk` 的前提下，若过早引入客户端本地读写缓存或异步写回（Writeback），不仅会带来跨节点脏读风险，还会破坏服务端单事务持久化与配额实时扣减承诺。
2. **多用户 POSIX 权限执行边界**：`StorageService` 持久化了每个 `Inode` 的 `mode`、`uid` 与 `gid`，但 `Lookup`、`Open`、`Unlink` 等数据面 RPC 不携带调用进程身份，服务端自身不执行传统 POSIX `rwx` 属主权限拦截。
3. **分布式会话租约（`ClientSession`）超期后的锁安全恢复**：当网络分区导致客户端租约在服务端超期回收时，该会话此前打开的全部 `FileHandle` 与持有的 `FileLock` 已被服务端级联销毁。网络恢复后需在“避免挂载点永久瘫痪”与“严防业务进程在不知情丢失文件锁的情况下无锁并发写坏数据”之间确立明确状态机。
4. **底层 Go FUSE 库维护边界与 `FUSE_TMPFILE` 取舍**：上游官方 `github.com/hanwen/go-fuse/v2` 的 `RawFileSystem` 已覆盖 `Rename2`、`ReadDirPlus`、`FsyncDir`、`Fallocate`、`Xattr` 及 `GetLk/SetLk/SetLkw` 等全部常规操作码，但未注册 `FUSE_TMPFILE (51)` 回调，需权衡是否为此维护私有底层库 Fork。
5. **纯 Linux FUSE 特权依赖与开发测试容器隔离**：客户端仅面向 Linux 交付，而真实 `/dev/fuse` 挂载需要 `CAP_SYS_ADMIN` 与设备透传，既不能强求非 Linux 开发宿主机原生运行，也不能破坏生产 `storage-service` 容器的无特权安全基线。

## 决策
1. **独立 `client/` 子模块、顶层 `mount`/`umount` 命令与无 PID 文件守护生命周期**：
   - 在仓库根目录建立独立 Go 模块 `client/`（`module github.com/starsrc/bowl/client`，纯 Linux 构建、零 CGO，通过本地 `replace` 复用根模块 `gen/go` ConnectRPC 契约）。
   - 提供顶层一级命令 `bowl mount <disk-id> <mountpoint>` 与 `bowl umount <mountpoint>`（内置 `unmount` 等价别名），将 `bowl disk ...` 命名空间留作未来的控制面资源管理。客户端通过 `--endpoint` / `BOWL_STORAGE_ENDPOINT`（自适应 `h2c` 与 `HTTPS`）直连 `StorageService`，并预留可选 `--token` / `BOWL_TOKEN` 注入 Bearer 认证头。
   - **默认后台守护与同步握手**：`bowl mount` 先在前台校验挂载点有效且未重复挂载、调用 `GetDisk` 解析 `root_ino` 并注册初始 `ClientSession`，随后派生后台守护进程执行 `/dev/fuse` 挂载，父进程经管道握手确认挂载就绪（或接收具体失败原因）后方才退出（同时支持 `-f / --foreground` 前台托管模式）；后台日志默认写入 `/var/log/bowl/mount-<disk-id>.log`（无权限时回退至 `~/.local/state/bowl/logs/mount-<disk-id>.log`）。
   - **内核事件驱动卸载**：不维护易失步残留的本地 PID 文件；`bowl umount`（支持 `-f / --force` 懒卸载）按序调用系统 `fusermount3 -u` / `fusermount -u` / `umount`，待内核冲刷完活跃句柄的 `Flush` 与 `Release` / `ReleaseDir` 并关闭 `/dev/fuse` 后，守护进程在 `OnUnmount` 中终止会话续期并自退出。
2. **用户态零缓存直通、内核态缓存透传与 `default_permissions` 权限闭环**：
   - **用户态零缓存直通**：第一阶段客户端进程不引入任何用户态内存或本地磁盘缓存，所有 FUSE 操作直通 `StorageService`；挂载时禁用内核异步 `writeback_cache` 以确保每次写入同步直达服务端，协商 `MaxWrite = 1 MiB` 并在读写超 `4 MiB` 时按 `4 MiB` 边界分片调用 RPC。
   - **透传服务端内核缓存指令以保留 `mmap`**：客户端原样透传服务端响应中的 `entry_timeout_seconds`、`attr_timeout_seconds` 与 `open_flags`（服务端默认 `OpenKeepCache = false` 实现跨客户端打开时刷新），默认不强制置位 `FOPEN_DIRECT_IO` 以确保 `mmap` 与挂载点可执行二进制文件正常运行（另提供 `--direct-io` 可选开关）。
   - **根 `Inode` 双向重写与本地内核权限强制校验**：在 FUSE 适配边界对所有请求入参与目录遍历（含 `.` / `..`）结果执行内核根节点 `1` 与服务端 `Disk.root_ino` 的双向转换；默认强制开启 FUSE 内核 `default_permissions` 由 Linux VFS 本地执行标准 POSIX `rwx` 及 Sticky Bit 校验，创建类操作透传请求头真实 `uid / gid`，暴露 `--allow-other` 与 `--ro` 选项，并通过 `Ioctl` 将 `FS_IOC_GETFLAGS` / `FS_IOC_SETFLAGS` 桥接至 `GetAttr` / `SetAttr(flags)`。
3. **`ClientSession` 自适应续期、超期会话轮换、有界网络重试与待释放句柄异步重试队列**：
   - **自适应心跳与会话轮换自愈**：客户端启动时生成随机 63 位正整数 `session_id` 调用 `RenewSession`，并按服务端返回的 `expires_at` 剩余时长 `1/3` 动态调度续期心跳。若网络中断恢复后检测到原租约已超期回收（`session_recreated == true`），立即记录告警并原子切换为全新 `session_id`：保障后续新打开的文件立即恢复正常服务，同时令断网前已丢失服务端 `FileLock` 与句柄的老文件描述符诚实返回 `EBADF`（严禁静默伪造重开句柄掩盖锁丢失）。
   - **错误无损提取与有界重试**：解析 ConnectRPC 错误详情中的 `google.rpc.ErrorInfo.Reason` 映射为 Linux `syscall.Errno`；仅对传输层未送达的连接瞬断及只读/幂等 RPC 执行总长不超过 `5s` 的指数退避重试（收到内核 `cancel` 中断时立即返回 `EINTR`），重试耗尽返回 `EIO`，防止服务端停机时宿主机进程永久挂死。
   - **待释放句柄异步重试队列**：由于 FUSE `Release` 与 `ReleaseDir` 回调不向内核返回错误码，客户端在同步解绑本地句柄会话映射的同时，维护轻量待释放句柄重试队列：当 `Release` / `ReleaseDir` 因网络闪断、超时或中断错误失败时入队由后台协程周期重试（在 `OnUnmount` 卸载时停止后台循环并尽力冲刷队列），将服务端返回的 `EBADF` / `ENOENT` / `EINVAL` 视为已完成销毁直接出队，并在 `ClientSession` 发生超期轮换时自动丢弃旧会话的待释放项，避免长稳活跃会话下瞬断导致服务端句柄与关联 BSD `flock` 或孤儿节点延迟回收。
4. **直接使用上游官方 `github.com/hanwen/go-fuse/v2`**：
   - 直接依赖上游官方稳定版 `github.com/hanwen/go-fuse/v2`，不为低频匿名 `open(O_TMPFILE)` 维护私有底层库 Fork（内核下发 `FUSE_TMPFILE` 时按标准未实现操作码降级返回 `EOPNOTSUPP`，常规具名临时文件如 `mkstemp` 不受影响）。
5. **独立特权测试容器与 `Taskfile.yml` 闭环验证**：
   - 保持 [`docker-compose.yaml`](../../docker-compose.yaml) 生产 `storage-service` 容器无特权不变；为 `client/` 构建包含 Go 1.26 与 `fuse3` 的专用 Linux 测试容器镜像，通过 `task test:client` 执行容器内静态检查与单元测试，通过 `task test:client:e2e`（按需授予 `/dev/fuse` 与 `CAP_SYS_ADMIN` 并接入 `bowl-net`）运行包含多挂载点并发共享读写与锁互斥在内的真实 FUSE 挂载/卸载黑盒验证。

## 结果
- **积极影响**：在不破坏 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 服务端单事务持久化与商业配额强一致边界的前提下，以极简、零私有 Fork 的薄客户端打通了完整的 Linux POSIX FUSE 挂载闭环；`default_permissions` 补齐了多用户权限安全边界；会话超期自动轮换兼顾了挂载点长稳自愈与分布式文件锁语义安全；独立测试容器在非 Linux 开发机上也能一键完成真实内核级 FUSE 端到端测试。
- **代价考量**：因第一阶段未引入用户态数据块缓存，跨客户端重开大文件或内核页回收后的重复读取均会发起内网 `StorageService.Read` RPC；后续若面临超大规模只读数据集复用瓶颈，可在不改变 CLI 契约的前提下按 `(disk_id, ino, generation, mtime, size)` 版本键渐进扩展可选的客户端只读块缓存。
