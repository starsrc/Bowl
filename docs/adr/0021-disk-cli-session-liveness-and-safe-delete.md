# 0021. Disk 控制面 CLI、会话挂载绑定与基于双向流探活的安全删盘机制

## 状态
已采纳 (accepted) — 演进 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 与 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)

## 背景
在 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md) 交付 Linux FUSE 数据面命令（`bowl mount` / `bowl umount`）后，客户端预留的 `bowl disk` 控制面命名空间需补齐完整的磁盘生命周期管理子命令（创建、列表查询、详情查询、更新/扩缩容、删除）。在设计控制面 CLI 与服务端协同机制时，面临四项核心架构问题：
1. **容量调整语义与双层水位线校验**：由于 `Disk` 同时支持在线扩容与缩容（只要目标容量不低于当前已用空间 `used_bytes`），若子命令命名为单向 `expand` 会产生语义冲突；同时需在 CLI 与服务端双层保障缩容水位线安全。
2. **磁盘列表分页扩展性**：`StorageService` 此前仅提供单盘 `GetDisk`，缺乏批量枚举接口；随着商业实例数量增长，需提供稳定、无漂移的分页查询契约。
3. **活跃挂载下的安全删盘保护（演进 ADR-0015）**：[ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 早期将 `DeleteDisk` 设定为无条件强制级联删除。但在真实使用场景中，若某块 `Disk` 仍处于本地或远端活跃挂载状态，误执行 `bowl disk delete` 会导致整盘数据瞬间不可逆销毁且活跃挂载点变为失效僵尸挂载。然而，当时 `client_sessions` 表未关联 `disk_id`（空闲挂载无打开句柄时服务端无法得知会话挂载在哪块盘上），且 `bowl umount` 退出时未主动通知服务端销毁会话。
4. **客户端异常死机（`kill -9` / 断电）与活跃挂载的精确区分**：若仅依赖 `client_sessions` 的 60s 租约 TTL 拦截删盘，一旦客户端被 `kill -9` 或宿主机断电，在 60s 租约过期窗口内执行 `DeleteDisk` 会被误拒；而若由服务端按客户端上报的 `IP:Port` 反向发起 TCP 拨号探活，在容器网桥、NAT 或防火墙环境下会因入站连接被拦截而把**活着的客户端误判为死机**并误删磁盘（详见 [`docs/research/session-liveness-and-disk-delete.md`](../research/session-liveness-and-disk-delete.md)）。

## 决策
1. **统一 `bowl disk` 控制面子命令体系与双层容量校验**：
   - 在纯 Linux 客户端模块 `client/` 中提供 `bowl disk <create|list|get|update|delete>` 五个子命令（`update` 兼容 `resize` 别名），共享 `--endpoint`（`$BOWL_STORAGE_ENDPOINT`）与 `--token`（`$BOWL_TOKEN`）连接标志：
     - `bowl disk create --size <size> [--name <name>] [-q|--quiet] [--json]`
     - `bowl disk list [--limit <n>] [--page-token <token>] [--json]`
     - `bowl disk get <disk-id> [--json]`
     - `bowl disk update <disk-id> [--size <size>] [--name <name>] [--json]`
     - `bowl disk delete <disk-id> [-y|--yes] [--json]`
   - **容量单位与双层校验**：`--size` 支持不区分大小写的 1024 二进制幂单位（`K/M/G/T`、`KiB/MiB/GiB/TiB`、`KB/MB/GB/TB`）及纯数字字节。执行 `bowl disk update` 调整容量时，CLI 先调用 `GetDisk` 前置校验 `new_max_bytes >= current_used_bytes`（且 `new_max_bytes > 0`），不满足时立即输出含当前已用容量的人类可读报错；通过后再由服务端 `UpdateDisk` 在 `FOR UPDATE` 行锁事务内执行终审兜底。
2. **新增 `StorageService.ListDisks` 游标分页契约**：
   - 在 `proto/storage/v1/storage.proto` 中新增 `ListDisks(ListDisksRequest) returns (ListDisksResponse)`，支持 `page_size`（默认 `50`，最大 `1000`）与 `page_token` 游标分页，按创建时间倒序（`created_at DESC, disk_id DESC`）稳定返回 `repeated Disk disks` 与 `next_page_token`。
3. **`ClientSession` 绑定 `Disk` 与正常卸载 `CloseSession` 即时注销**：
   - 在 PostgreSQL `client_sessions` 表中新增 `disk_id UUID REFERENCES disks(disk_id) ON DELETE CASCADE` 列与 `(disk_id, expires_at)` 索引；在 `RenewSessionRequest` 中新增 `optional string disk_id`，由 `bowl mount` 在激活与续期会话时传入。
   - 在 `StorageService` 中新增 `CloseSession(CloseSessionRequest) returns (CloseSessionResponse)`：当 `bowl umount` 触发守护进程退出时，客户端在 `session.Manager.Close()` 中主动调用 `CloseSession` 立即注销云端会话并释放关联句柄、文件锁及孤儿节点，确保正常 `umount` 后可 0 延迟执行 `bowl disk delete`。
4. **基于 `WatchSession` 双向流断连感知与 `1.5s` 按需 Ping-Pong 验活的安全删盘机制**：
   - **出站双向流连接反转**：在 `StorageService` 中新增 `WatchSession(stream WatchSessionRequest) returns (stream WatchSessionResponse)` 双向流。客户端在 `bowl mount` 存续期间主动维持该出站流（100% 穿透 NAT、Docker 容器与防火墙），平时零轮询开销；当客户端进程被 `kill -9` 或容器销毁时，宿主机 OS 内核立即发送 TCP `FIN/RST`，服务端流即时感知断开并自动清理该残留会话。
   - **删盘多重防护与按需 Ping-Pong 探活**：
     1. **客户端本机防护**：`bowl disk delete <disk-id>` 先扫描本机 `/proc/self/mountinfo`，若目标磁盘在本机处于挂载态则立即报错拒绝并提示先执行 `bowl umount`；未传 `-y / --yes` 时要求终端交互确认。
     2. **服务端按需 Ping-Pong 验活**：`StorageService.DeleteDisk` 执行时，先清理已超期会话；若目标 `disk_id` 下仍存在未超期的 `client_sessions`，服务端遍历这些会话并通过 `WatchSession` 流下发 `Ping(ping_seq)`，等待最多 `1.5s` 的 `Pong(pong_seq)` 应答：
        - 若任一会话在 `1.5s` 内回复 `Pong`（确认客户端存活），立即拒绝删除并返回 gRPC `FailedPrecondition`（携带 `ErrorInfo{Reason: "EBUSY", Domain: "bowl.storage"}`）；
        - 若某会话无活跃 `WatchSession` 流或在 `1.5s` 内未回复 `Pong`（确认客户端已死机/断电半开连接），服务端当场回收该僵尸会话；待全部未超期会话均确认为死会话并清理后，在数据库事务内加锁复核无活跃会话与活跃句柄，原子执行 `Disk` 与下属 `Inode`/`Chunk` 级联删除。

## 结果
- **积极影响**：通过单一 `bowl` CLI 打通了「建盘 → 查询/列表 → 挂载 → 在线扩缩容 → 卸载 → 安全删盘」的完整闭环；借助客户端出站 `WatchSession` 双向流与按需 `Ping-Pong` 探活，在不要求客户端暴露入站端口的前提下，同时实现了活跃挂载防误删（`EBUSY`）、正常 `umount` 0 延迟删盘、`kill -9` 毫秒级感知与断电 1.5s 死会话自动回收。
- **代价考量**：`StorageService` 需在内存中维护活跃 `WatchSession` 流注册表以派发按需 `Ping-Pong` 探测，且原 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 中“无视活跃会话强制删除磁盘”的旧测试需更新为验证活跃会话拦截（`EBUSY`）与死会话探活回收。
