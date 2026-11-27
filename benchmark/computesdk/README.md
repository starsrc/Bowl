# 用 computesdk/benchmarks 量 sandbox 这一层

[computesdk/benchmarks](https://github.com/computesdk/benchmarks) 的 sandbox TTI 量的是:从发出创建请求,到 sandbox 里第一条命令(`node -v`)成功返回。这里把 `bowl-sandbox` 接成它的一个 provider。

量的是 MicroVM 这一层,不经过 Bowl:每个 sandbox 的系统盘是状态目录里的一个镜像文件,从模板镜像复制而来。

## 怎么跑

需要一台有 `/dev/kvm` 的 Linux 机器,root,装好 Cloud Hypervisor、Docker、Node.js 24 和 pnpm。状态目录要放在支持 reflink 的文件系统上(XFS 或 btrfs)。

```bash
S="--state-dir /var/lib/bowl-local"          # 一个 XFS 上的目录,里面放 vmlinux 和 bowl-sandbox-guest

bowl-sandbox $S template create node:22-slim --prewarm "node -v"
bowl-sandbox $S serve &                       # 127.0.0.1:7070

bash benchmark/computesdk/setup.sh ~/csdk-bench
cd ~/csdk-bench
BOWL_SANDBOX_URL=http://127.0.0.1:7070 pnpm exec tsx packages/benchsdk-runner/dist/bin.js \
  run benchmarks/sandbox/tti.bench.ts --provider bowl --iterations 30 --no-ingest
```

`setup.sh` 克隆那个仓库,把 [`bowl.ts`](bowl.ts) 放进去并登记成 provider。[`workloads.ts`](workloads.ts) 和 [`hold.ts`](hold.ts) 是另外两个驱动脚本,量 sandbox 里的负载和空闲内存,用之前手工拷到那个仓库的 `benchmarks/sandbox/` 下,用 `tsx` 跑。`--no-ingest` 表示结果只留在本机,不上传到它的平台。

## 结果

机器是 Azure D2s_v5:2 核 8G,嵌套虚拟化。sandbox 都是 1 核 1G,镜像都是 `node:22-slim`。跑 benchmark 的进程和 sandbox 在同一台机器上。

同一台机器上还装了 CubeSandbox v0.8.0(一键安装,默认配置),用同一个 benchmark 量。它兼容 E2B 的 SDK,所以走的是 benchmark 里现成的 `e2b` provider,把 `E2B_API_URL` 指到本机。

### 创建到第一条命令返回

单位毫秒。单并发每组跑了两轮,各 30 次,表里是两轮的范围。

| 对象 | 并发 | 次数 | 成功 | 中位数 | P95 |
| --- | --- | --- | --- | --- | --- |
| bowl-sandbox,模板预热过 `node -v` | 1 | 30 × 2 | 全部 | 186 到 190 | 218 到 225 |
| bowl-sandbox,模板没预热 | 1 | 30 × 2 | 全部 | 289 到 299 | 363 到 391 |
| CubeSandbox | 1 | 30 × 2 | 全部 | 284 到 297 | 320 到 416 |
| E2B 云服务(经公网) | 1 | 20 | 全部 | 567 | 1007 |
| bowl-sandbox,预热 | 5 | 30 | 全部 | 279 | 805 |
| bowl-sandbox,没预热 | 5 | 30 | 全部 | 568 | 1644 |
| CubeSandbox | 5 | 30 | 全部 | 1396 | 1738 |
| E2B 云服务(经公网) | 5 | 20 | 全部 | 590 | 904 |
| bowl-sandbox,预热 | 10 | 40 | 全部 | 391 | 2135 |
| CubeSandbox | 10 | 40 | 全部 | 3257 | 4232 |

怎么读:

- **和 CubeSandbox 比要看"没预热"那一行。** 它的模板没有预热 `node -v`,这一行两边条件一样,单并发基本打平(289 到 299 对 284 到 297)。预热后我们快约 100 毫秒,它也支持预热模板,这次没给它做。
- **并发时 CubeSandbox 慢很多,多半是这台机器太小。** 它每个 sandbox 有一个管理进程,还有七八个常驻组件,2 核不是它的目标配置。它自己公布的 50 并发平均 276 毫秒是在裸金属上测的。
- **CubeSandbox 默认配额在这台机器上只够开 8 个左右 1G 的 sandbox**,超过就返回 `no more resource`。并发那几行是把 Cubelet 的 `host.quota` 调大之后量的。
- **E2B 那两行多了公网往返,机器也不是同一台**,只能看个量级。它用的是 E2B 默认的 `base` 模板。
- 第一轮里 bowl-sandbox 两组各有一次超过 1.1 秒(1201 和 1158 毫秒),第二轮没有,原因没查。

### 空闲时每个 sandbox 占多少内存

同时开 20 个不跑东西的 sandbox,看宿主机可用内存(`MemAvailable`)平均每个少多少。先建一个再删掉,让模板的页缓存不算进去。

| 对象 | 每个 sandbox |
| --- | --- |
| bowl-sandbox | 3.8 到 4.5 MB |
| CubeSandbox | 10.6 MB |

CubeSandbox 自己在 1000 个 sandbox 上量的是 21 到 26 MB(2 核 2G 的规格)。

### sandbox 里的 CPU 和文件读写

用的是那个 benchmark 仓库自带的两个负载,各在一个 sandbox 里跑一次:CPU 是对 1 GiB 的零做 SHA-256,文件读写是一套 SQLite 操作。驱动脚本是 [`workloads.ts`](workloads.ts)。单位毫秒,越小越好。

| 负载 | bowl-sandbox | CubeSandbox |
| --- | --- | --- |
| CPU,单线程 | 5790 | 5620 |
| SQLite,默认(WAL) | 3622 | 3320 |
| SQLite,每次写都 fsync | 1706 | 1180 |
| SQLite,大数据集 | 17321 | 15428 |

只跑了一次,差 3% 到 12% 的几项不能下结论。fsync 那项差 31%,值得再查:两边的系统盘都是 XFS 上的写时复制克隆,经 virtio-blk 给虚机;我们虚机里是 ext4 直接当根,它是 overlay。

### 做模板

`node:22-slim`:bowl-sandbox 在镜像已经拉到本机时是 7 到 12 秒(含拍快照),第一次要拉镜像时 36 秒;CubeSandbox 约 10 秒,含拉镜像。

## 两个会影响数字的条件

- **状态目录不在 reflink 文件系统上时很慢。** 建 sandbox 要复制一份模板镜像。在 ext4 上这是一次整份复制,虚机第一次刷盘会把整份镜像落盘,建一个 sandbox 要 3.5 到 5.8 秒。XFS 上复制不占时间。
- **`--prewarm` 跑过的命令,结果留在每个 sandbox 的内存里。** 上面的模板预热了 `node -v`,所以 node 的可执行文件已经在虚机的页缓存里。不预热时第一条命令要从盘上读。
