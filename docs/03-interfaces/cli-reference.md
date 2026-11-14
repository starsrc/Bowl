# CLI 参考手册

`bowl` 是 Bowl 的统一命令行入口（源码位于 `client/internal/cli/`），提供磁盘控制面管理（`bowl disk`）、Linux FUSE 挂载与卸载（`bowl mount` / `bowl umount`）以及客户端运行时状态查询（`bowl stats`）。

## 全局解析规则

### 服务端端点（Endpoint）与鉴权令牌（Token）

所有需要访问存储服务端的命令（`bowl disk *` 与 `bowl mount`）按以下优先级解析连接参数（参见 `client/internal/rpc/client.go`）：

| 参数 | 优先级（高 → 低） | 合法格式与约束 |
| :--- | :--- | :--- |
| **服务端地址** | `--endpoint` 标志 → `$BOWL_STORAGE_ENDPOINT` 环境变量 → 默认 `http://127.0.0.1:9000` | 仅接受 `http://` 或 `https://` 且包含有效主机名；尾部斜杠自动裁剪；格式非法时以退出码 `2` 终止 |
| **Bearer 令牌** | `--token` 标志 → `$BOWL_TOKEN` 环境变量 → 空（不发送鉴权头） | 非空时在每个 ConnectRPC 请求头自动注入 `Authorization: Bearer <token>` |

### 容量单位解析与格式化

`--size` 支持十进制风格后缀，底层严格按 **1024 二进制幂** 换算为字节（参见 `client/internal/cli/size.go`）：

| 输入后缀（不区分大小写） | 换算系数 | 示例与换算结果 |
| :--- | :--- | :--- |
| 无后缀 / `B` | `1`（$2^0$） | `4096` → `4096 B` |
| `K` / `KB` / `KIB` | `1024`（$2^{10}$） | `512K` → `524288 B` |
| `M` / `MB` / `MIB` | `1048576`（$2^{20}$） | `256M` → `268435456 B` |
| `G` / `GB` / `GIB` | `1073741824`（$2^{30}$） | `10G` → `10737418240 B` |
| `T` / `TB` / `TIB` | `1099511627776`（$2^{40}$） | `1T` → `1099511627776 B` |
| `P` / `PB` / `PIB` | `1125899906842624`（$2^{50}$） | `1P` → `1125899906842624 B` |

> [!IMPORTANT]
> `--size` 仅接受严格大于 `0` 且不超过 `math.MaxInt64`（`9223372036854775807`）的整数值，不支持小数（如 `1.5G`）或负值。人类可读输出统一格式化为 `%.2f KiB/MiB/GiB/TiB/PiB`（小于 `1 KiB` 显示整数 `B`）。

## 顶层命令速查

```text
bowl <command> [flags] [arguments]
```

| 命令 | 别名 | 核心功能 |
| :--- | :--- | :--- |
| `bowl disk <subcommand>` | — | 磁盘生命周期管理（`create` / `fork` / `list` / `get` / `update` / `delete` / `diff` / `merge`） |
| `bowl mount <disk-id> <mountpoint>` | — | 将指定虚拟磁盘挂载到 Linux 本地目录（默认后台守护进程） |
| `bowl umount <mountpoint>` | `unmount` | 卸载指定挂载点并等待守护进程优雅退出 |
| `bowl stats [mountpoint]` | — | 查询客户端挂载实例的实时 FUSE / RPC / 缓存统计 |
| `bowl version` | `--version` / `-v` | 打印客户端版本号（`bowl version <Version>`）并退出 `0` |
| `bowl help` | `--help` / `-h` | 打印帮助信息并退出 `0` |

## `bowl disk` 子命令全集

所有 `bowl disk` 子命令均支持通用连接与输出标志：`--endpoint <url>`、`--token <token>`、`--json`、`-h/--help`。

### `-q / --quiet` 使用契约

- **仅允许用于** `create`、`fork`、`merge` 三个子命令：成功后仅在标准输出打印单行 `<disk_id>`（`merge` 输出目标盘 ID），便于 Shell 变量捕获；在 `merge --dry-run` 模式下 `-q` 无额外输出但仍保留退出码语义。
- **`--quiet` 与 `--json` 互斥**，同时传入直接返回退出码 `2`。
- 在 `list`、`get`、`update`/`resize`、`delete`、`diff` 上传入 `-q` 或 `--quiet` 会立即报错并返回退出码 `2`。

### `bowl disk create`

创建一块新的空白虚拟磁盘（RPC 超时 `10s`）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `--size <size>` | 是 | — | 磁盘逻辑容量上限（如 `10G`、`512MiB`） |
| `--name <name>` | 否 | `""` | 磁盘可读名称（不超过 `255 B`） |
| `-q`, `--quiet` | 否 | `false` | 仅输出新创建的 `<disk_id>` |
| `--json` | 否 | `false` | 输出 `diskJSON` |

### `bowl disk fork`

从已有源盘创建写时复制分支盘（RPC 超时 `30s`）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<source-disk-id>` | 是（位置参数） | — | 源磁盘 UUID |
| `--name <name>` | 否 | `""` | 新分支磁盘名称（不超过 `255 B`） |
| `--size <size>` | 否 | 继承源盘 | 可选覆盖分支盘容量上限 |
| `-q`, `--quiet` | 否 | `false` | 仅输出新分支 `<disk_id>` |
| `--json` | 否 | `false` | 输出 `diskJSON` |

### `bowl disk list`

分页列出磁盘（RPC 超时 `10s`）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `--limit <n>` | 否 | 服务端默认 `50` | 每页返回条数，范围 `1 .. 1000` |
| `--page-token <token>` | 否 | `""` | 下一页游标（来自上次输出的 `next_page_token`） |
| `--json` | 否 | `false` | 输出 `{"disks": [<diskJSON>, ...], "next_page_token": "..."}` |

### `bowl disk get`

查询单块磁盘元数据与当前容量占用（RPC 超时 `10s`）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<disk-id>` | 是（位置参数） | — | 目标磁盘 UUID |
| `--json` | 否 | `false` | 输出 `diskJSON` |

### `bowl disk update`（别名 `resize`）

修改磁盘名称或在线调整容量上限（RPC 超时 `10s`）。至少需提供 `--size` 或 `--name` 之一。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<disk-id>` | 是（位置参数） | — | 目标磁盘 UUID |
| `--size <size>` | 否 | — | 新容量上限；客户端先调用 `GetDisk` 预检是否小于 `used_bytes`，服务端同时在事务内强制校验 |
| `--name <name>` | 否 | — | 新名称（允许传 `--name ""` 清空名称） |
| `--json` | 否 | `false` | 输出更新后的 `diskJSON` |

### `bowl disk delete`

删除磁盘及其独占历史状态（RPC 超时 `15s`）。删除前客户端会检查 `/proc/self/mountinfo`，若本机正在挂载该磁盘则直接失败；若服务端检测到活跃挂载或依赖会返回 `EBUSY`。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<disk-id>` | 是（位置参数） | — | 待删除磁盘 UUID |
| `-y`, `--yes` | 否 | `false` | 跳过交互式 `y/N` 确认（未传时用户在终端拒绝确认将返回退出码 `1`） |
| `--json` | 否 | `false` | 输出 `{"disk_id": "...", "deleted": true}` |

### `bowl disk diff`

基于全局血缘树与 LCA 计算磁盘变更清单（RPC 超时 `60s`）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<source-disk-id>` | 是（位置参数） | — | 源磁盘 UUID |
| `--target <target-disk-id>` | 否 | 分叉父节点 Commit | 目标对比磁盘 UUID；省略时与最近父提交对比 |
| `--prefix <path>` | 否 | `""` | 按路径前缀过滤（最长 `4096 B`，禁止空字符） |
| `--conflicts-only` | 否 | `false` | 仅返回存在三路冲突的条目 |
| `-p`, `--patch` | 否 | `false` | 为文本文件生成类 Unified Diff 补丁（超过 `32 MiB` 或 Myers 编辑距离大于 `2000` 自动跳过补丁内容） |
| `--limit <n>` | 否 | 服务端默认 | 分页条数，范围 `1 .. 1000` |
| `--page-token <token>` | 否 | `""` | 分页游标（最长 `4096 B`，禁止空字符） |
| `--json` | 否 | `false` | 输出结构化 Diff 报告（字段见下节 Schema） |

### `bowl disk merge`

将源分支盘的变更三路合并入目标盘（RPC 超时 `60s`）。非 `--dry-run` 且未传 `--force` 时，客户端先检查 `/proc/self/mountinfo`，仅当源盘或目标盘在本机处于读写挂载状态时拦截并提示先卸载（本机只读挂载不拦截合并）。

| 标志 / 参数 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- |
| `<source-disk-id>` | 是（位置参数） | - | 源分支盘 UUID |
| `--into <target-disk-id>` | 是 | - | 合入目标盘 UUID |
| `--strategy <fail\|ours\|theirs>` | 否 | `fail` | 默认冲突裁决策略 |
| `--resolve <path>=<ours\|theirs>` | 否 | - | 针对单个路径的冲突覆盖策略（可重复传参，最多 `4096` 项，路径 `1 .. 4096 B`） |
| `--dry-run` | 否 | `false` | 仅做冲突检测与容量估算，不修改目标盘状态 |
| `-f`, `--force` | 否 | `false` | 跳过本地 `/proc/self/mountinfo` 读写挂载预检并向服务端请求强制合入 |
| `--size <size>` | 否 | - | 合并同时显式扩容目标盘 |
| `-q`, `--quiet` | 否 | `false` | 合并成功后仅输出目标盘 ID（冲突时仍退出 `1`） |
| `--json` | 否 | `false` | 输出 `merge` 结构化 JSON |

## `--json` 输出 Schema 说明

### 基础磁盘对象 `diskJSON`

`create`、`fork`、`get`、`update`/`resize` 直接输出以下对象，`list` 将其包裹在 `{"disks": [...], "next_page_token": "..."}` 中：

```json
{
  "disk_id": "0195...",
  "parent_disk_id": "0195...",
  "name": "app-main",
  "root_ino": 1,
  "max_bytes": 1073741824,
  "used_bytes": 4096,
  "used_inodes": 2,
  "created_at": "2026-11-14T00:00:00Z",
  "updated_at": "2026-11-14T00:00:00Z"
}
```

- `parent_disk_id` 仅在非空（Fork 派生盘）时出现（`omitempty`）。

### `diff --json` Schema

```json
{
  "source_disk_id": "0195...",
  "target_disk_id": "0195...",
  "lca_commit_id": "0195...",
  "summary": {
    "added": 1,
    "modified": 1,
    "deleted": 0,
    "conflicts": 0
  },
  "entries": [
    {
      "path": "/config.env",
      "kind": "modified",
      "conflict": "",
      "base": { "ino": 2, "mode": 33188, "size": 12 },
      "target": { "ino": 2, "mode": 33188, "size": 12 },
      "source": { "ino": 2, "mode": 33188, "size": 19 },
      "patch": "@@ -1 +1 @@\n-v1=baseline\n+v2=feature-enabled\n"
    }
  ],
  "next_page_token": ""
}
```

### `merge --json` Schema

```json
{
  "source_disk_id": "0195...",
  "target_disk_id": "0195...",
  "dry_run": false,
  "merged": true,
  "lca_commit_id": "0195...",
  "merge_commit_id": "0195...",
  "estimated_used_bytes": 8192,
  "target_max_bytes": 1073741824,
  "disk": { "...": "同 diskJSON，仅成功合入时返回" },
  "conflict_report": {
    "lca_commit_id": "0195...",
    "total_conflicts": 1,
    "conflicts_truncated": false,
    "conflicts": [
      {
        "path": "/config.env",
        "kind": "modify-modify",
        "base": { "ino": 2, "mode": 33188, "size": 12 },
        "target": { "ino": 2, "mode": 33188, "size": 15 },
        "source": { "ino": 2, "mode": 33188, "size": 19 }
      }
    ]
  }
}
```

> [!WARNING]
> 当 `merge` 检测到未决冲突（服务端返回 `ECONFLICT` 或报告包含冲突条目）时，即使传入了 `--json`，CLI 仍会在输出 `merged: false` 与 `conflict_report` 后以**退出码 `1`** 退出，确保 CI/自动化脚本不会误判合入成功。

## `bowl mount` / `bowl umount` / `bowl stats`

### `bowl mount <disk-id> <mountpoint> [flags]`

将虚拟磁盘通过 Linux `/dev/fuse` 挂载到 `<mountpoint>`。默认以**后台守护进程**模式启动：主进程通过文件描述符 `fd 3` 管道等待子进程完成 `MountDisk`、FUSE 挂载与状态文件注册并收到 `OK` 握手后才返回 `0`。

| 标志 | 默认值 / 环境变量 | 说明 |
| :--- | :--- | :--- |
| `--endpoint <url>` | `$BOWL_STORAGE_ENDPOINT` / `http://127.0.0.1:9000` | 存储服务端 URL |
| `--token <token>` | `$BOWL_TOKEN` | Bearer 鉴权令牌 |
| `-f`, `--foreground` | `false` | 前台运行，直接响应 `SIGINT` / `SIGTERM` 退出 |
| `--log-file <path>` | `/var/log/bowl/mount-<disk-id>.log`（无权限回退 `$XDG_STATE_HOME/bowl/logs/` 或 `~/.local/state/bowl/logs/`） | 仅后台守护模式生效；与 `-f` 同时传入会报参数错误（退出 `2`） |
| `--allow-other` | `false` | 允许非挂载用户访问挂载点（需 `/etc/fuse.conf` 开启 `user_allow_other`） |
| `--ro` | `false` | 无状态只读挂载（`epoch = 0`，不占用写租约，允许多节点并发挂载，不可与 `--force` 同时传入） |
| `--force` | `false` | 强制抢占当前持有未过期写租约的在线读写挂载，递增 `WriterEpoch` 并使旧写者失租约自动下线（不可与 `--ro` 同时传入，无 `-f` 短标志） |
| `--direct-io` | `false` | 强制 FUSE 绕过内核页缓存直通存储服务 |
| `--debug` | `false` | 开启 `go-fuse` 底层协议调试日志 |
| `--admin-addr <host:port\|off>` | `$BOWL_ADMIN_ADDR` / `$OBS_ADMIN_ADDR` / `127.0.0.1:0` | 本地回环 HTTP 管理端口；设 `"off"` 完全关闭管理端口 |
| `--stats-file` | `false` | 在挂载点根目录暴露只读虚拟文件 `.stats`（与 `.jfs.stats` 别名），固定 Inode `(1<<63)-1` |
| `--log-level <level>` | `$BOWL_LOG_LEVEL` / `$LOG_LEVEL` / `info` | 日志级别（`debug` / `info` / `warn` / `error`） |
| `--log-format <json\|text>` | `$BOWL_LOG_FORMAT` / `$LOG_FORMAT` / `json` | 结构化日志格式 |
| `--trace-sample-ratio <float>` | `$OTEL_TRACES_SAMPLER_ARG` / `0.001` | 客户端 Trace 采样率（`0.0 .. 1.0`） |

挂载成功后，客户端会在 `/run/bowl/mounts/<sha256_16B>.json`（无权限时回退至 `$TMPDIR/bowl-mounts/`）写入挂载发现元数据，记录 `mountpoint`、`pid`、`disk_id`、`admin_addr` 与 `started_at`。

### `bowl umount <mountpoint> [-f|--force]`（别名 `unmount`）

1. 检查 `<mountpoint>` 是否处于挂载状态；
2. 依次尝试 `fusermount3 -u` → `fusermount -u` → `umount`（携带 `-f` 时使用 `-uz` / `-l` 懒卸载）；
3. 默认模式（未传 `-f`）会以 `20ms` 间隔最多等待 `5s`，直到元信息文件被清理或对应 `bowl mount` 守护进程 PID 退出，避免进程残留锁或孤儿进程。

### `bowl stats [mountpoint] [--json | --raw | --admin-addr <host:port>]`

向挂载进程暴露的**本地回环 TCP HTTP 管理端口**发起 `GET http://<admin_addr>/debug/stats?format=<text|json|stats>` 请求，实时输出 FUSE 41 项操作延迟/计数、RPC 重试、缓存命中率与队列水位：

- **定位目标进程**：传入 `<mountpoint>` 时，按挂载点路径哈希读取 `/run/bowl/mounts/<sha256_16B>.json`（或 `$TMPDIR/bowl-mounts/`）获取 `admin_addr`；若直接省略参数或需要跨环境指定，可通过 `--admin-addr <host:port>` 直连。
- **输出格式**：默认人类可读文本表格（`format=text`）；`--json` 输出完整 JSON（`format=json`）；`--raw` 输出两列空白分隔原始指标文本（`format=stats`，与挂载点 `.stats` 虚拟文件同源）。`--json` 与 `--raw` 互斥（退出码 `2`）。

## 退出码契约

| 退出码 | 语义分类 | 典型触发场景 |
| :--- | :--- | :--- |
| **`0`** | 成功 | 命令正常执行完毕；`--help` / `version`；`merge --dry-run` 且冲突数为 `0` |
| **`1`** | 运行时 / 业务错误 | RPC 超时或网络不可达；服务端返回配额超限 / `EBUSY` / `ECONFLICT`（含 `merge` 冲突）；本地挂载预检失败；`delete` 用户交互取消；`mount` / `umount` / `stats` 执行失败 |
| **`2`** | 参数或语法校验失败 | 未知命令或未知标志；缺少必填位置参数或多余参数；UUID / `--size` / `--endpoint` / `--limit` / `--admin-addr` 格式错误；互斥标志同时出现（`-q` + `--json`、`--json` + `--raw`、`-f` + `--log-file`、`--ro` + `--force`）；在 `delete`/`diff`/`merge` 上触发服务端 `InvalidArgument` |

## 延伸阅读

- [RPC 接口契约](/03-interfaces/rpc-api) —— 查看 CLI 底层调用的 `StorageService` 37 个 Protobuf 接口。
- [可观测性手册](/03-interfaces/observability) —— 深入了解 `/debug/stats`、`.stats` 虚拟文件及后台守护日志路径。
