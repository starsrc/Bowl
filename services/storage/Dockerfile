# ==============================================================================
# Build Stage
# ==============================================================================
FROM golang:1.26-alpine3.24 AS builder

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

# 利用 Docker 构建缓存：先复制 Go 依赖描述文件
COPY go.mod go.sum ./
COPY services/storage/go.mod services/storage/go.sum ./services/storage/

# 下载 Go 模块依赖
WORKDIR /src/services/storage
RUN go mod download

# 复制协议生成文件、通用共享包与服务源码
WORKDIR /src
COPY gen ./gen
COPY pkg ./pkg
COPY services/storage ./services/storage

# 编译静态二进制文件（CGO_ENABLED=0，剥离调试符号减小体积）
WORKDIR /src/services/storage
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /bin/storage-service \
    ./cmd/main.go

# ==============================================================================
# Production Runtime Stage
# ==============================================================================
FROM alpine:3.24 AS runtime

# 安装运行基础证书和时区数据
RUN apk --no-cache add ca-certificates tzdata

# 创建非 root 用户与组（UID/GID 10001），符合安全基线要求
RUN addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

WORKDIR /app

# 从构建镜像复制二进制文件
COPY --from=builder /bin/storage-service /app/storage-service

# 切换至非 root 用户
USER appuser:appgroup

# 声明监听端口
EXPOSE 9000

ENTRYPOINT ["/app/storage-service"]
