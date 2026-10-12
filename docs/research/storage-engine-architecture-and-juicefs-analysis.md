# 调研报告：Bowl Storage 存储引擎架构与 JuiceFS 依赖解耦分析

- **文档编号**: RES-BOWL-0002
- **关联组件**: `services/storage` (Bowl Storage 服务端数据面)
- **分析日期**: 2026-10-12
- **状态**: 完成 (Completed)

---

## 1. 核心结论 (Executive Summary)

针对用户关于“系统是否使用 JuiceFS 以及未引用代码时切片与 S3 存储如何实现”的问题，基于第一性原理与代码库事实调查，结论如下：

1. **未直接引入 JuiceFS 代码库**：`services/storage/go.mod` 中**没有**引入官方模块 `github.com/juicedata/juicefs`。
2. **完全采纳并自主实现 JuiceFS 存储模型**：Bowl 摒弃了直接引用官方库的做法，但在内部（`services/storage/internal/chunk`）以纯 Go 方式**百分之百对齐并实现了 JuiceFS 规范的数据切片模型与对象存储规约**（包括 64MB 逻辑 Chunk、4MB 物理 Block、24 字节大端序 Slice 紧凑二进制编码、NVMe Staging 缓存暂存与 S3 异步推流管道）。
3. **早期文档提及 JuiceFS 的原因**：早期的 ADR（如 ADR 0003, ADR 0006, ADR 0008, ADR 0012）在设计构想阶段曾探讨直接复用 JuiceFS 核心库或委派锁语义。但在前期原型调研（Ticket STARSRC-110）中，发现官方 JuiceFS 存在致命的 CGO 强依赖与依赖单包膨胀问题。因此在 Milestone 2（Commit `45e3254`，STARSRC-114）中，团队决定自主实现协议兼容的纯 Go 存储引擎。

---

## 2. 为什么没有直接引用 JuiceFS 依赖库？

通过排查 Git 调研记录与官方依赖关系，确认排除官方库的根本技术约束如下：

1. **CGO 强依赖破坏纯 Go 跨平台编译**：
   - 官方 `github.com/juicedata/juicefs/pkg/chunk` 强依赖 `pkg/compress`。
   - `pkg/compress` 默认引入 CGO 库 `github.com/DataDog/zstd` 和 `github.com/hungys/go-lz4`。
   - 在 `CGO_ENABLED=0` 环境下构建会立即失败，无法满足服务端轻量化静态交叉编译的要求。
2. **依赖严重膨胀（1,500+ 个外部包）**：
   - 官方 `github.com/juicedata/juicefs/pkg/object` 将全部 40+ 种存储后端驱动（含 Ceph、HDFS、TiKV、SQLite3 等）耦合在单个包中。
   - 引入该包将导致工程间接引入 **1,590 个依赖包**，包含强制 CGO 的 `go-sqlite3` 以及在 macOS 编译不兼容的底层库。
3. **租约与多租户架构不适配**：
   - JuiceFS 官方设计假定单一全局数据库与单文件系统挂载；
   - Bowl 采用虚拟磁盘（Virtual Disk）模型，每个磁盘拥有独立的 PostgreSQL Schema（`disk_<uuid>`）与专属的 OWMR（One-Writer Multi-Reader）租约生命周期，与官方单体元数据驱动存在本质冲突。

---

## 3. 切片与 S3 对象存储的完整自研实现

Bowl 在 `services/storage` 中通过模块化驱动架构自主实现了切片与 S3 存储流水线，代码结构位于 `services/storage/internal/chunk` 与 `services/storage/internal/service`：

```
services/storage/
├── internal/
│   ├── chunk/
│   │   ├── slice.go       # 24 字节大端序 Slice 紧凑编解码与 64MB/4MB 常量定义
│   │   ├── staging.go     # NVMe 本地磁盘暂存写入管理器 (Write-through 缓存)
│   │   ├── batcher.go     # 4MB 物理 Block 划分与 JuiceFS 标准 S3 Key 生成
│   │   ├── uploader.go    # 基于 minio-go 的后台异步多协程 S3 推流管道与重试
│   │   ├── reader.go      # NVMe 优先读取与 S3 HTTP Range 冷数据透明回源
│   │   ├── overlay.go     # 单 Chunk 内多切片图层覆盖重叠重组与稀疏空洞填充
│   │   ├── recovery.go    # 节点重启时 NVMe 未决切片自动扫描与 S3 重放
│   │   └── gc.go          # 切片引用计数追踪与 S3 物理对象异步垃圾回收
│   └── service/
│       ├── chunk_driver.go # FileStorageDriver 接口实现，串联 PostgreSQL 与切片引擎
│       ├── storage.go      # per-disk chunks 与 slice_refs 数据表 DDL 管理
│       └── driver.go       # FileStorageDriver 抽象定义与 LegacyFileDriver
```

### 3.1 核心数据结构与 24 字节切片编码 (`slice.go`)

在 `internal/chunk/slice.go` 中，Bowl 实现了与 JuiceFS 完全兼容的切片数据结构：
- **逻辑 Chunk 大小**：64 MiB (`ChunkSize = 64 * 1024 * 1024`)。
- **物理 Block 大小**：4 MiB (`BlockSize = 4 * 1024 * 1024`)。
- **切片二进制格式**：每个切片序列化为固定 **24 字节大端序（Big-Endian）** 字节流：
  - `Pos` (`uint32`, 4 字节)：切片在 64MB Chunk 内的起始偏移 `[0, ChunkSize)`；
  - `ID` (`uint64`, 8 字节)：全局单调递增的唯一切片标识符；
  - `Size` (`uint32`, 4 字节)：切片实际物理载荷的总字节数；
  - `Off` (`uint32`, 4 字节)：有效数据在切片载荷内的起始偏移；
  - `Len` (`uint32`, 4 字节)：有效数据区间长度。

### 3.2 PostgreSQL 元数据存储模型 (`storage.go` & `chunk_driver.go`)

每个虚拟磁盘创建独立的 PostgreSQL Schema，并在其中维护切片元数据表：
1. **`chunks` 表**：
   ```sql
   CREATE TABLE IF NOT EXISTS "<schema>".chunks (
       ino BIGINT NOT NULL,
       indx INT NOT NULL,
       slices BYTEA NOT NULL,
       PRIMARY KEY (ino, indx)
   );
   ```
2. **`slice_refs` 表**：
   ```sql
   CREATE TABLE IF NOT EXISTS "<schema>".slice_refs (
       id BIGINT PRIMARY KEY,
       chunk_indx INT NOT NULL DEFAULT 0,
       size INT NOT NULL,
       refs INT NOT NULL DEFAULT 1
   );
   ```
3. **极速 Append 追加语义**：
   客户端发起 `Write` RPC 时，`ChunkDriver` 计算所属 Chunk 编号，为新数据分配唯一 `sliceID`，并将 24 字节二进制切片直接追加至 PostgreSQL：
   ```sql
   INSERT INTO "<schema>".chunks (ino, indx, slices)
   VALUES ($1, $2, $3)
   ON CONFLICT (ino, indx)
   DO UPDATE SET slices = chunks.slices || EXCLUDED.slices;
   ```
   采用字节拼接（`BYTEA || BYTEA`）避免了全表扫描与行膨胀，实现亚毫秒级元数据追加。

### 3.3 本地 NVMe Staging 缓存 (`staging.go`)

在执行 `Write` RPC 时：
1. `StagingManager` 负责将客户端传入的切片物理载荷写入本地 NVMe 高速缓存文件：
   `<STAGING_DIR>/<disk_id>/slice_<slice_id>.bin`
2. 写入成功后立即返回，保证了 FUSE/Connect-RPC 写入操作的超低延迟。

### 3.4 4MB 物理分块与 S3 对象键格式 (`batcher.go`)

当切片需要持久化到对象存储时，`BlockBatcher` 将切片数据切分为 4MB 物理分块，并按照 JuiceFS 规范生成标准的 S3 对象键：
```
chunks/<disk_id>/<chunk_prefix>/<chunk_indx>/<slice_id>_<block_indx>
```
其中：
- `chunk_prefix` 计算规则为 `fmt.Sprintf("%03d", chunk_indx/1000)`，用于在大规模文件场景下打散 S3 对象存储前缀热点；
- `block_indx` 为 4MB 块的递增序号。

### 3.5 后台异步 S3 上传管道与 Fsync 屏障 (`uploader.go` & `chunk_driver.go`)

1. **异步推流**：
   - `S3Uploader` 基于轻量纯 Go 的 MinIO 官方 SDK（`github.com/minio/minio-go/v7`）开发；
   - 内部启动常驻 Worker 协程池，消费 `UploadTask` 队列；
   - 支持网络抖动下的指数退避重试（Exponential Backoff）。
2. **持久化屏障（Durability Barrier）**：
   - 客户端调用 `Flush` 或 `Fsync` RPC 时，`ChunkDriver.syncInodeBarrier` 会调用 `S3Uploader.WaitForIno(ctx, diskID, ino)`；
   - 服务端阻塞等待该 Inode 的全部在途上传切片成功提交至 S3 后才向客户端返回成功，确保 POSIX 数据落盘语义。

### 3.6 透明读穿透与多切片覆盖重组 (`reader.go` & `overlay.go`)

当客户端发起 `Read` RPC 时：
1. **元数据读取**：根据 `offset` 计算涉及的 64MB Chunk，从 PostgreSQL `chunks` 表读取对应的 `slices` 字节流，反序列化为 `[]Slice`；
2. **多切片图层覆盖（ReconstituteRangeTo）**：
   按写入时间的先后顺序对重叠区间进行图层叠加覆盖，未被切片覆盖的区间自动作为稀疏空洞填充为全零（`0x00`）；
3. **读分层穿透（S3Reader）**：
   - **热路径**：优先检查本地 NVMe Staging 目录，若命中直接读取本地文件；
   - **冷路径**：本地缺失时，自动降级至 S3/MinIO，通过 HTTP Range 请求按需拉取指定 4MB Block 的局部字节区间。

---

## 4. 结论与总结

- **系统有没有用 JuiceFS？**：系统**没有**直接引入 JuiceFS 的 Go 源码包，但**采纳并实现了 JuiceFS 的分块分片核心架构规范**。
- **为什么代码库找不到 juicefs import？**：为避免 CGO 强制依赖和 1500+ 个无用包膨胀，Bowl 在保持架构规范兼容的前提下，使用纯 Go 自研实现了协议层与存储驱动。
- **代码在何处实现？**：核心实现集中在 `services/storage/internal/chunk/`（分块、暂存、S3上传、冷读回源）和 `services/storage/internal/service/chunk_driver.go`（存储驱动门面与元数据集成）。
