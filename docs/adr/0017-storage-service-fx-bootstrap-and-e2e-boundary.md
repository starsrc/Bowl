# 0017. Storage 服务 Fx 启动装配、POSIX ErrorInfo 拦截契约与黑盒 E2E 测试边界

## 状态
已采纳 (accepted)

## 背景
`StorageService` 的 36 个元数据与磁盘配额领域操作已在 `services/storage/internal/meta` 中完备实现，请求静态校验器已在 `pkg/validator` 中就绪，但缺少服务端可运行入口（`cmd/main.go`）、配置加载（`internal/config`）、数据库连接池生命周期托管（`internal/db`）与 ConnectRPC 服务端协议承载层（`internal/server`）。此外，在搭建可运行服务与端到端测试基础设施时，面临三项核心架构权衡：
1. **ConnectRPC 协议版本与跨仓可观测性套件复用**：仓库生成的 Connect 桩代码此前使用了 `connectrpc.com/connect/v2` alpha 版本，而 `Aether` 共享的 `pkg/observability/{slogx,telemetry}` 及社区 `otelconnect` 拦截器均构建于 `connectrpc.com/connect` v1 稳定版之上；同时需明确 `StorageService` 作为内网存储服务是否需要暴露公网 REST 转码。
2. **POSIX 细粒度 `ErrorInfo.Reason` 在 ConnectRPC 拦截器链中的无损透传**：通用 `connectrpc.com/validate` 拦截器会将所有 Protobuf 校验错误笼统转换为不带业务错误详情的 `CodeInvalidArgument`，导致 FUSE 客户端无法区分 `ENAMETOOLONG`、`ERANGE`、`E2BIG` 与 `EINVAL`；同时领域层 `meta.ToStatus` 产出的 24 类 POSIX 哨兵错误详情也需在 ConnectRPC 响应中无损挂载并对未知内部错误脱敏。
3. **本地多项目容器端口隔离与黑盒 E2E 测试边界**：本地开发环境同时运行 `Aether` 容器组（占用 `8000`、`8080`、`8081` 等 8000 段端口），且需确立纯黑盒容器级端到端验证的模块边界与构建标签规范。

## 决策
1. **基于 `uber-go/fx` 的内网 ConnectRPC v1 服务装配（无 REST 转码）**：
   - 将 `buf.gen.yaml` 与模块依赖统一锁定至 `connectrpc.com/connect` v1 稳定版，直接复用 `github.com/starsrc/Aether/pkg/observability/{slogx,telemetry}` 与 `github.com/starsrc/Aether/pkg/tx`。
   - `StorageService` 定位为集群内网 RPC 服务（暂不挂载公网 JWT `authn` 认证拦截器，亦不引入 Vanguard REST 转码），通过标准 `http.NewServeMux` 挂载 ConnectRPC 处理器，并在 `http.Server` 上同时启用 HTTP/1.1 与非加密 HTTP/2（`h2c`）。
   - 采用 `go.uber.org/fx` 组织 `config.Module`、`telemetry.Module`、`slogx.Module`、`db.Module`、`meta.Module` 与 `server.Module` 的依赖注入与优雅启停生命周期；在 `config.Module` 中除暴露 `ServerConfig`、`DBPoolConfig`、`TelemetryConfig`、`LogConfig` 外，暴露 `MetaConfig`（支持配置 `SessionLeaseTTL`、`AtimeMode` 与 FUSE 内核缓存超时），注入 `meta.New`。
2. **专属 `pkg/validator` 拦截器与 POSIX `ErrorInfo` 转换契约**：
   - 拦截器链按洋葱模型由外向内组装为：`slogx.NewMaskInterceptor`（最外层脱敏 `CodeInternal`/`CodeUnknown` 原始错误文本并保留 `Details`） -> `otelconnect.NewInterceptor`（链路追踪） -> `slogx.NewConnectInterceptor`（结构化访问日志） -> 基于 `pkg/validator.NewStorageValidator()` 的专属请求校验拦截器。
   - 在专属校验拦截器与 `StorageServiceHandler` 实现层，统一将 `validator.ToStatus(err)` 与 `meta.ToStatus(err)` 产出的 gRPC `*status.Status`（含状态码与 `google.rpc.ErrorInfo{Reason, Domain: "bowl.storage"}`）转换为 `*connect.Error` 并通过 `connect.NewErrorDetail` 挂载详情，确保客户端通过 `connect.Error.Details()` 可无损提取 POSIX `errno` 原因码。
3. **9000+ 端口段容器编排与根模块 `e2e/` 黑盒测试边界**：
   - 摒弃 `integration` 命名，统一采用 `e2e`：所有针对真实运行容器的黑盒端到端测试均置于根模块 `e2e/` 目录下（由根 `go.mod` 管辖），测试文件统一声明 `//go:build e2e`，辅助包 `e2e/testutil` 不加构建标签以便日常 `go vet ./...` 静态检查覆盖；`e2e/` 严禁导入 `services/storage/internal/...` 内部包。
   - Bowl 容器编排统一使用 `9000+` 端口段（`storage-service` 默认监听并映射 `9000:9000`），提供自包含的 `docker-compose.yaml`（含 `bowl-postgres`、`bowl-migration`、`bowl-storage-service`、`bowl-jaeger`）与 `Taskfile.yml` 编排任务（`task build`、`task run`、`task down`、`task test:e2e`）。

## 结果
- **积极影响**：打通 `StorageService` 从配置加载、数据库连接池、领域状态机到 ConnectRPC 服务端监听的完整运行闭环；FUSE 客户端与黑盒 E2E 测试可通过标准 ConnectRPC 协议无损获取 24 类领域 POSIX 错误与请求校验错误详情；9000+ 端口段与 `//go:build e2e` 彻底消除了与本地 `Aether` 容器的端口冲突及离线单测对外部容器的耦合。
- **代价考量**：需要将根模块与 `services/storage` 的 ConnectRPC 依赖统一对齐至 `connectrpc.com/connect` v1 并重新生成 `gen/go` 协议代码。
