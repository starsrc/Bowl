---
layout: home

hero:
  name: Bowl Developer Book
  text: 分布式虚拟磁盘系统开发手册
  tagline: 面向服务端控制面、Pebble 存储引擎与 Linux FUSE 客户端的架构、契约与扩展指南
  actions:
    - theme: brand
      text: 快速开始
      link: /01-overview/quickstart
    - theme: alt
      text: 能力全景
      link: /01-overview/capabilities

features:
  - title: POSIX 文件系统与 Linux FUSE 客户端
    details: 基于 hanwen/go-fuse/v2 实现目录树、硬链接/符号链接、rename2、fallocate、xattr、孤儿文件及守护进程化挂载。
  - title: 零拷贝派生与三路合并
    details: 支持基于不可变提交切面的 ForkDisk 零拷贝派生、DiffDisk 差异对比，以及带 dry_run 预览与冲突策略的 MergeDisk 三路合并。
  - title: 单写者世代栅栏与持久化租约
    details: 通过 Pebble 持久化的 WriterEpoch 栅栏与 WriterExpiresAt 写租约、MountDisk/RenewMount/UnmountDisk 续期抢占及配额校验保障多端挂载一致性。
  - title: 分离式控制面与引擎架构
    details: 控制面基于 Uber Fx、ConnectRPC、protovalidate 与 PostgreSQL；数据面采用单盘 StoreActor、Pebble 键值索引与 64 MiB Append-Only Pack 容器。
---

## 四卷阅读引导

| 卷目 | 定位 | 推荐阅读路径 |
| --- | --- | --- |
| 第一卷：全局认知 | 建立整体心智模型与本地开发运行环境 | [能力全景](/01-overview/capabilities) → [系统架构](/01-overview/architecture) → [快速开始](/01-overview/quickstart) |
| 第二卷：功能规格 | 理解虚拟磁盘核心业务语义与边界条件 | [磁盘生命周期](/02-features/disk-lifecycle) → [派生、差异与三路合并](/02-features/fork-diff-merge) → [挂载租约与写栅栏](/02-features/mount-and-fencing) → [POSIX 语义与扩展属性](/02-features/posix-semantics) |
| 第三卷：接口契约 | 查阅 CLI 参数、全量 RPC 契约与可观测指标 | [CLI 命令参考](/03-interfaces/cli-reference) → [RPC 接口契约](/03-interfaces/rpc-api) → [可观测性与运维指标](/03-interfaces/observability) |
| 第四卷：内部实现与扩展 | 深入核心模块源码并按规范开展二次开发 | [Storage 控制面实现](/04-internals/storage-control-plane) → [Storage 存储引擎](/04-internals/storage-store-engine) → [Client 与 FUSE 流水线](/04-internals/client-fuse-pipeline) → [二次开发与扩展指南](/04-internals/extending-bowl) |
