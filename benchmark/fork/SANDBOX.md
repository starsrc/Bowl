# sandbox 一级的 fork

这里量的是 `bowl-sandbox fork`,不是 `bowl disk fork`。分两个数字:

- **盘 fork**:暂停源 sandbox、把它的写入刷到存储节点、调 ForkDisk、恢复源 sandbox。源 sandbox 停住的时间就是这一段。
- **到就绪**:从发出 fork 到新 sandbox 能执行第一条命令,包含上一段和新 sandbox 的启动。

源 sandbox 里一直有一个进程每 50 毫秒写一次计数。每次 fork 之后核对:fork 里的值比源 sandbox 当前的值小,说明 fork 拿到的是 fork 那一刻的状态,源 sandbox 也没被打断。

脚本是 [`sandbox_fork.sh`](sandbox_fork.sh),用当前的 `bowl-sandbox` 配置跑:

```bash
TEMPLATE=python:3.12-slim RUNS=5 bash benchmark/fork/sandbox_fork.sh
```

## 结果

| 后端 | 系统盘 | 机器 | 次数 | 盘 fork | 到就绪 |
| --- | --- | --- | --- | --- | --- |
| microvm | 整块盘经 virtio-fs | Azure D2s_v5(2 核,嵌套虚拟化) | 5 | 109 到 174 ms | 591 到 776 ms |
| microvm | 盘里的块设备镜像 | 同上 | 3 | 194 到 250 ms | 1.63 到 1.75 s |
| container | 整块盘当根,chroot | macOS,Docker Desktop,Apple 芯片 | 5 | 75 到 146 ms | 894 到 1106 ms |

所有次数里 fork 都拿到了 fork 那一刻的计数。

几点说明:

- 存储服务和 sandbox 在同一台机器上,没有网络延迟。
- Azure 那台是嵌套虚拟化的 2 核机器,不是裸机,虚机启动比裸机慢。
- 块设备那一行的盘 fork 更慢,因为暂停前要先在虚机里 `sync`;到就绪更慢,因为新虚机要挂 ext4。换来的是装包、编译这类小文件负载快 3 到 7 倍。
- 只 fork 盘,不 fork 内存:新 sandbox 是冷启动,源 sandbox 里的进程不会出现在 fork 里。
