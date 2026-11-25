# 用 computesdk/benchmarks 量 sandbox 这一层

[computesdk/benchmarks](https://github.com/computesdk/benchmarks) 的 sandbox TTI 量的是:从发出创建请求,到 sandbox 里第一条命令(`node -v`)成功返回。这里把 `bowl-sandbox` 接成它的一个 provider。

量的是 MicroVM 这一层,不经过 Bowl:`bowl-sandbox --local` 把系统盘放成本机的镜像文件。fork、diff、merge、move 要 Bowl 盘,这个模式下没有。

## 怎么跑

需要一台有 `/dev/kvm` 的 Linux 机器,root,装好 Cloud Hypervisor、Docker、Node.js 24 和 pnpm。状态目录要放在支持 reflink 的文件系统上(XFS 或 btrfs)。

```bash
export BOWL_SANDBOX_STORAGE=local
S="--state-dir /var/lib/bowl-local"          # 一个 XFS 上的目录,里面放 vmlinux 和 bowl-sandbox-guest

bowl-sandbox $S template create node:22-slim --prewarm "node -v"
bowl-sandbox $S serve &                       # 127.0.0.1:7070

bash benchmark/computesdk/setup.sh ~/csdk-bench
cd ~/csdk-bench
BOWL_SANDBOX_URL=http://127.0.0.1:7070 pnpm exec tsx packages/benchsdk-runner/dist/bin.js \
  run benchmarks/sandbox/tti.bench.ts --provider bowl --iterations 30 --no-ingest
```

`setup.sh` 克隆那个仓库,把 [`bowl.ts`](bowl.ts) 放进去并登记成 provider。`--no-ingest` 表示结果只留在本机,不上传到它的平台。

## 结果

机器是 Azure D2s_v5:2 核 8G,嵌套虚拟化。sandbox 是 1 核 1G。跑 benchmark 的进程和 sandbox 在同一台机器上,没有网络延迟;它榜单上的云厂商是从另一台机器经公网调用的,所以这里的数字不能直接和榜单比。

| 做法 | 并发 | 次数 | 成功 | 中位数 | P95 | 最大 |
| --- | --- | --- | --- | --- | --- | --- |
| 每次启动内核 | 1 | 20 | 20 | 约 480 ms | 约 570 ms | 656 ms |
| 从模板快照恢复 | 1 | 30 | 30 | 165 ms | 218 ms | 232 ms |
| 从模板快照恢复 | 5 | 30 | 30 | 233 ms | 884 ms | 1167 ms |

并发 5 时慢,是因为 `serve` 里建 sandbox 是一个一个排队做的,机器也只有 2 核。

内存:同时开 20 个刚建好、没跑东西的 sandbox,宿主机可用内存平均每个少 3.7 MB,每个虚机进程的 Pss 是 6.9 MB。快照的内存文件(1 GiB)在宿主机页缓存里只占一份,所有虚机共用,不算在每个 sandbox 头上。sandbox 里跑起东西之后,它改写过的内存页归它自己,这个数会涨。

## 两个会影响数字的条件

- **状态目录不在 reflink 文件系统上时很慢。** 建 sandbox 要复制一份模板镜像。在 ext4 上这是一次整份复制,虚机第一次刷盘会把整份镜像落盘,建一个 sandbox 要 3.5 到 5.8 秒。XFS 上复制不占时间。
- **`--prewarm` 跑过的命令,结果留在每个 sandbox 的内存里。** 上面的模板预热了 `node -v`,所以 node 的可执行文件已经在虚机的页缓存里。不预热时第一条命令要从盘上读。
