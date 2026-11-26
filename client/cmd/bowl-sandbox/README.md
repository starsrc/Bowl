# bowl-sandbox

`bowl-sandbox` 在本机用 Cloud Hypervisor 跑 MicroVM 沙箱。它不连 Bowl 存储服务:每个沙箱的系统盘是状态目录里的一个 ext4 镜像文件,建沙箱时从模板镜像复制一份。

## 要求

- Linux,有 `/dev/kvm`,以 root 运行。
- PATH 里有 `cloud-hypervisor`、`iptables`、`cp`(GNU coreutils);做模板还要 Docker、`mkfs.ext4` 和 loop 设备。
- 状态目录(默认 `/var/lib/bowl-sandbox`,用 `--state-dir` 改)里放好虚机内核 `vmlinux` 和 Linux 版的 `bowl-sandbox-guest`。
- 状态目录最好在 XFS 或 btrfs 上。`cp --reflink` 在这两种文件系统上不复制数据;在 ext4 上每建一个沙箱都要整份复制模板镜像。

构建:

```bash
cd client
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bowl-sandbox ./cmd/bowl-sandbox
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bowl-sandbox-guest ./cmd/bowl-sandbox-guest
```

## 命令

| 命令 | 做什么 |
| --- | --- |
| `template create <image> [--name N] [--size 4GiB] [--prewarm CMD]` | 把容器镜像解进一个 ext4 镜像,放入 `bowl-sandbox-guest`,再启动一次拍内存快照。`--prewarm` 的命令在拍快照前执行 |
| `template list` / `template delete <name>` | 列出、删除模板。已经建好的沙箱不受影响 |
| `create --template N [--cpus 1] [--memory 1024] [--timeout 30m] [-q]` | 建沙箱,打印 ID。规格和快照一致时从快照恢复,否则冷启动 |
| `list` / `info <id>` | 状态、模板、IP、规格、到期时间 |
| `exec [-b] [--cwd DIR] <id> -- <cmd...>` | 前台执行并以命令的退出码退出;`-b` 后台执行,打印运行 ID |
| `logs <id> [--exec RUN] [-f]` / `wait <id> <run>` | 读后台运行的输出、等它结束 |
| `shell <id>` | 交互 shell(有 bash 用 bash) |
| `cp <src> <dst>` | 一边写成 `<id>:<path>`,在本机和沙箱之间复制文件或目录 |
| `pause` / `resume <id>` | 冻结 vCPU,内存保留 |
| `stop` / `start <id>` | 停虚机释放 CPU 和内存,盘留着;`start` 从这块盘冷启动 |
| `timeout <id> <duration>` | 从现在起多久后删掉沙箱,`0` 取消 |
| `kill <id>` / `kill --all` | 删沙箱和它的盘 |
| `serve [--listen 127.0.0.1:7070] [--api-key K]` | 提供 SDK 用的 HTTP 接口。监听回环以外的地址必须带 `--api-key` 或 `$BOWL_SANDBOX_API_KEY` |

模板名里的 `/` 和 `:` 存成 `_`,所以 `template list` 里 `node:22-slim` 显示为 `node_22-slim`,两种写法都能用。

## 网络

每个沙箱一块 tap 网卡和一个 `172.30.<n>.0/24`,沙箱的 IP 是 `172.30.<n>.2`,宿主机可以直接连它监听的端口。出网走宿主机 NAT。沙箱连不到宿主机本身、私有网段(10/8、172.16/12、192.168/16)和 169.254/16(云主机元数据服务在这里)。

## HTTP 接口

| 请求 | 说明 |
| --- | --- |
| `POST /sandboxes` | `{"template", "timeout_seconds", "cpus", "memory_mib"}` |
| `GET /sandboxes`、`GET /sandboxes/{id}`、`DELETE /sandboxes/{id}` | 列出、查看、删除 |
| `POST /sandboxes/{id}/commands` | `{"cmd"}` 走 `sh -c`,或 `{"argv"}`;可带 `cwd`、`envs`、`stdin`、`background`、`timeout_seconds` |
| `GET /sandboxes/{id}/commands/{run}` | 等后台运行结束,返回退出码和输出 |
| `GET` / `PUT /sandboxes/{id}/files?path=` | 读写文件,写时自动建父目录 |
| `POST /sandboxes/{id}/timeout` | `{"timeout_seconds"}`,`0` 取消 |

Python SDK 在 [`sdk/python`](../../../sdk/python)。
