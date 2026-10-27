# ==============================================================================
# Build Stage
# ==============================================================================
FROM golang:1.26-alpine3.24 AS builder

RUN apk --no-cache add ca-certificates tzdata git

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}

WORKDIR /src

# 复制根模块描述与共享协议代码
COPY go.mod go.sum ./
COPY gen ./gen
COPY pkg ./pkg
COPY services/storage ./services/storage

# 编译静态二进制文件（CGO_ENABLED=0，剥离调试符号减小体积）
WORKDIR /src/services/storage
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-w -s" \
    -o /bin/storage-service \
    ./cmd

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
