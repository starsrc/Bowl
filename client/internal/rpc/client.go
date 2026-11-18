// Package rpc 封装 StorageService ConnectRPC 客户端、重试拦截器与错误映射。
package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/starsrc/pkg/serverx"
	"go.opentelemetry.io/otel/metric"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	// DefaultEndpoint 为未显式配置 endpoint 且未设置环境变量时的默认 StorageService 地址。
	DefaultEndpoint = "http://127.0.0.1:9000"

	// EnvStorageEndpoint 为配置 StorageService 接入地址的环境变量名。
	EnvStorageEndpoint = "BOWL_STORAGE_ENDPOINT"

	// EnvToken 为配置鉴权访问令牌的环境变量名。
	EnvToken = "BOWL_TOKEN"

	// HTTP2MaxReadFrameSize 为客户端 HTTP/2 接收帧大小上限 (1 MiB)。
	HTTP2MaxReadFrameSize = 1 << 20

	// HTTP2MaxReceiveBufferPerStream 为客户端单流 HTTP/2 接收流控窗口上限 (8 MiB)，
	// 确保单次 4 MiB + Protobuf 信封载荷无需流级 WINDOW_UPDATE 停顿即可完整接收。
	HTTP2MaxReceiveBufferPerStream = 8 << 20

	// HTTP2ControlMaxReceiveBufferPerConnection sets the per-connection HTTP/2 receive window to 8 MiB for the control transport.
	HTTP2ControlMaxReceiveBufferPerConnection = 8 << 20

	// HTTP2MetaMaxReceiveBufferPerConnection sets the per-connection HTTP/2 receive window to 32 MiB for the metadata transport.
	HTTP2MetaMaxReceiveBufferPerConnection = 32 << 20

	// HTTP2DataMaxReceiveBufferPerConnection 为每条大块数据条带连接的 HTTP/2 接收流控窗口上限 (64 MiB)。
	HTTP2DataMaxReceiveBufferPerConnection = 64 << 20

	// HTTP2DataConnectionShards 为客户端并发大块读写（Read/Write）条带化的独立 H2C/TLS 连接数。
	HTTP2DataConnectionShards = 8

	// HTTP2SendPingTimeout sets the idle read period before HTTP/2 sends a PING frame to 3 seconds.
	HTTP2SendPingTimeout = 3 * time.Second

	// HTTP2PingTimeout sets the wait duration for a PING acknowledgment before closing the connection to 2 seconds.
	HTTP2PingTimeout = 2 * time.Second
)

// ClientConfig 定义 StorageServiceClient 的连接配置。
type ClientConfig struct {
	// Endpoint 为目标 StorageService 地址（例如 http://127.0.0.1:9000 或 https://storage.bowl.com）。
	// 若为空则通过 ResolveEndpoint 自动回退。
	Endpoint string

	// Token 为访问令牌。若为空则通过 ResolveToken 自动回退。
	Token string

	// HTTPClient 为可选的自定义 http.Client（测试或定制传输使用）。
	// 若未指定，则根据 endpoint scheme 自动配置 h2c 或 TLS HTTP/2 客户端。
	HTTPClient *http.Client

	// TLSConfig 为可选的自定义 TLS 配置（例如自签名证书信任）。
	TLSConfig *tls.Config

	// RetryConfig 为可选的重试策略配置。若为 nil 则默认使用 30s 有界指数退避策略。
	RetryConfig *RetryConfig

	// Logger 为可选的结构化日志记录器。若为 nil 则静默不输出；配置后输出关键异常与慢调用结构化日志。
	Logger *slog.Logger

	// Metrics 为可选的客户端 RPC 自定义指标收集器。对 nil 安全。
	Metrics *Metrics
}

// ResolveEndpoint 按照显式参数 -> BOWL_STORAGE_ENDPOINT -> 默认 http://127.0.0.1:9000 的回退链解析 endpoint，
// 并校验 URL scheme 必须为 http 或 https，且 host 不可为空。
func ResolveEndpoint(endpoint string) (string, error) {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = strings.TrimSpace(os.Getenv(EnvStorageEndpoint))
	}
	if endpoint == "" {
		endpoint = DefaultEndpoint
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid storage endpoint URL %q: %w", endpoint, err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("unsupported storage endpoint scheme %q: only http and https are supported", u.Scheme)
	}

	if u.Host == "" {
		return "", fmt.Errorf("storage endpoint %q missing host", endpoint)
	}

	return strings.TrimRight(endpoint, "/"), nil
}

// ResolveToken 按照显式参数 -> BOWL_TOKEN 回退链解析访问令牌。
func ResolveToken(token string) string {
	if strings.TrimSpace(token) != "" {
		return strings.TrimSpace(token)
	}
	return strings.TrimSpace(os.Getenv(EnvToken))
}

// newHTTPTransport 根据已校验的 endpoint URL scheme 与指定连接级接收窗口构建独立 HTTP/2 Transport。
func newHTTPTransport(endpoint string, tlsConfig *tls.Config, maxRecvBufPerConn int) (*http.Transport, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint %q: %w", endpoint, err)
	}
	if maxRecvBufPerConn <= 0 {
		maxRecvBufPerConn = HTTP2DataMaxReceiveBufferPerConnection
	}

	h2Cfg := &http.HTTP2Config{
		MaxReadFrameSize:              HTTP2MaxReadFrameSize,
		MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
		MaxReceiveBufferPerConnection: maxRecvBufPerConn,
		SendPingTimeout:               HTTP2SendPingTimeout,
		PingTimeout:                   HTTP2PingTimeout,
	}

	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	if strings.EqualFold(u.Scheme, "http") {
		// 明文 HTTP/2 (h2c) 并发多路复用传输：显式开启 TCP_NODELAY 并合并单次 Unary 请求的 HEADERS+DATA 帧，
		// 消除 Go h2 双次 Flush 在 Linux tcp_autocorking + virtio-net + Delayed ACK 下触发的 ~10ms 死锁。
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		return &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, dialErr := dialer.DialContext(ctx, network, addr)
				if dialErr != nil {
					return nil, dialErr
				}
				if tc, ok := conn.(*net.TCPConn); ok {
					_ = tc.SetNoDelay(true)
				}
				return newH2CoalesceConn(conn), nil
			},
			Protocols: &protocols,
			HTTP2:     h2Cfg,
		}, nil
	}

	// 标准 TLS HTTP/2 传输
	tlsTransport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, dialErr := dialer.DialContext(ctx, network, addr)
			if dialErr != nil {
				return nil, dialErr
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			return conn, nil
		},
		ForceAttemptHTTP2: true,
		HTTP2:             h2Cfg,
	}
	if tlsConfig != nil {
		tlsTransport.TLSClientConfig = tlsConfig
	}
	return tlsTransport, nil
}

func matchesProcedure(urlPath string, procedures ...string) bool {
	for _, proc := range procedures {
		if urlPath == proc || strings.HasSuffix(urlPath, proc) {
			return true
		}
	}
	return false
}

// isControlProcedure reports whether urlPath matches a mount lifecycle RPC routed to the dedicated control transport.
func isControlProcedure(urlPath string) bool {
	return matchesProcedure(
		urlPath,
		storagev1connect.StorageServiceMountDiskProcedure,
		storagev1connect.StorageServiceRenewMountProcedure,
		storagev1connect.StorageServiceUnmountDiskProcedure,
	)
}

// isBulkDataProcedure reports whether urlPath matches a bulk chunk transfer RPC routed across the 8 data shards.
func isBulkDataProcedure(urlPath string) bool {
	return matchesProcedure(
		urlPath,
		storagev1connect.StorageServiceReadProcedure,
		storagev1connect.StorageServiceWriteProcedure,
		storagev1connect.StorageServiceReadCommitNodeProcedure,
	)
}

type idleConnCloser interface {
	CloseIdleConnections()
}

// shardedRoundTripper isolates client HTTP/2 traffic across 1 control transport, 1 metadata transport, and 8 bulk data shards.
// MountDisk, RenewMount, and UnmountDisk route to controlTransport, Read, Write, and ReadCommitNode round-robin across
// 8 dataTransports, and all remaining metadata and disk RPCs route to metaTransport.
type shardedRoundTripper struct {
	controlTransport http.RoundTripper
	metaTransport    http.RoundTripper
	dataTransports   [HTTP2DataConnectionShards]http.RoundTripper
	nextDataShard    atomic.Uint64
}

func newShardedRoundTripper(endpoint string, tlsConfig *tls.Config) (*shardedRoundTripper, error) {
	rt := &shardedRoundTripper{}
	controlTr, err := newHTTPTransport(endpoint, tlsConfig, HTTP2ControlMaxReceiveBufferPerConnection)
	if err != nil {
		return nil, err
	}
	rt.controlTransport = controlTr

	metaTr, err := newHTTPTransport(endpoint, tlsConfig, HTTP2MetaMaxReceiveBufferPerConnection)
	if err != nil {
		rt.CloseIdleConnections()
		return nil, err
	}
	rt.metaTransport = metaTr

	for i := range HTTP2DataConnectionShards {
		dataTr, trErr := newHTTPTransport(endpoint, tlsConfig, HTTP2DataMaxReceiveBufferPerConnection)
		if trErr != nil {
			rt.CloseIdleConnections()
			return nil, trErr
		}
		rt.dataTransports[i] = dataTr
	}
	return rt, nil
}

func (rt *shardedRoundTripper) selectTransport(req *http.Request) http.RoundTripper {
	if req != nil && req.URL != nil {
		urlPath := req.URL.Path
		if isControlProcedure(urlPath) {
			return rt.controlTransport
		}
		if isBulkDataProcedure(urlPath) {
			idx := (rt.nextDataShard.Add(1) - 1) % HTTP2DataConnectionShards
			return rt.dataTransports[idx]
		}
	}
	return rt.metaTransport
}

func (rt *shardedRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return rt.selectTransport(req).RoundTrip(req)
}

// CloseIdleConnections forwards idle connection cleanup to the control, metadata, and all 8 bulk data transports.
func (rt *shardedRoundTripper) CloseIdleConnections() {
	if closer, ok := rt.controlTransport.(idleConnCloser); ok {
		closer.CloseIdleConnections()
	}
	if closer, ok := rt.metaTransport.(idleConnCloser); ok {
		closer.CloseIdleConnections()
	}
	for _, tr := range rt.dataTransports {
		if closer, ok := tr.(idleConnCloser); ok {
			closer.CloseIdleConnections()
		}
	}
}

// NewClientFromConfig 根据传入的 ClientConfig 构建 StorageServiceClient。
// 支持 h2c (明文 HTTP/2) 与 HTTPS TLS 传输，并在 token 非空时注入 Bearer 鉴权头。
// 底层 http.Client 不设置固定 Timeout，连接生命周期完全由 context.Context 控制。
func NewClientFromConfig(cfg ClientConfig, opts ...connect.ClientOption) (storagev1connect.StorageServiceClient, error) {
	return newClientWithClock(cfg, realClock{}, opts...)
}

func newClientWithClock(cfg ClientConfig, clk rpcClock, opts ...connect.ClientOption) (storagev1connect.StorageServiceClient, error) {
	if clk == nil {
		clk = realClock{}
	}
	endpoint, err := ResolveEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	token := ResolveToken(cfg.Token)

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		tr, trErr := newShardedRoundTripper(endpoint, cfg.TLSConfig)
		if trErr != nil {
			return nil, trErr
		}
		httpClient = &http.Client{
			Transport: tr,
		}
	}

	var interceptors []connect.Interceptor

	// 1. 最外层装配重试与逻辑总耗时拦截器（包裹在 otelconnect 外层，使每次网络重试独立计入单次尝试 Span 与客户端延迟分布，
	// 同时在最外层记录含退避睡眠的逻辑调用总耗时 bowl_client_rpc_logical_duration_seconds）
	retryCfg := RetryConfig{}
	if cfg.RetryConfig != nil {
		retryCfg = *cfg.RetryConfig
	}
	interceptors = append(interceptors,
		newRetryInterceptor(retryCfg, cfg.Logger, cfg.Metrics, clk),
		newRequestObservabilityInterceptor(cfg.Logger, cfg.Metrics, DefaultSlowRPCThreshold, clk),
	)

	// 3. OTel ConnectRPC 客户端拦截器（自动基于全局 TracerProvider / MeterProvider 注入 W3C traceparent 并以亚毫秒桶采集客户端 RPC 指标）
	// 4. 错误原因标注拦截器（提取服务端 ErrorInfo.Reason 同步写入客户端 Span rpc.error_reason 与 otelconnect.Labeler）
	otelInterceptor, err := otelconnect.NewInterceptor(
		otelconnect.WithDurationHistogramOptions(
			metric.WithExplicitBucketBoundaries(SubMillisecondDurationBuckets...),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create otelconnect interceptor: %w", err)
	}
	interceptors = append(interceptors, otelInterceptor, serverx.NewErrorReasonInterceptor())

	// 5. Bearer 令牌鉴权拦截器与 Accept-Encoding: identity 拦截器
	if token != "" {
		authInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				req.Header().Set("Authorization", "Bearer "+token)
				return next(ctx, req)
			}
		})
		interceptors = append(interceptors, authInterceptor)
	}

	// Connect 一元响应默认不协商压缩，避免大块读取受 gzip 的 CPU 开销限制。
	// 保留压缩编解码器和请求压缩；调用方后续传入的拦截器仍可覆盖响应协商。
	interceptors = append(interceptors, connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Accept-Encoding", "identity")
			return next(ctx, req)
		}
	}))

	clientOpts := []connect.ClientOption{
		connect.WithCodec(newProtoBinaryCodec()),
	}
	if len(interceptors) > 0 {
		clientOpts = append(clientOpts, connect.WithInterceptors(interceptors...))
	}

	if len(opts) > 0 {
		clientOpts = append(clientOpts, opts...)
	}

	return storagev1connect.NewStorageServiceClient(httpClient, endpoint, clientOpts...), nil
}

// NewClient 为 NewClientFromConfig 的便捷形式，支持 endpoint / token 与额外 connect.ClientOption。
func NewClient(endpoint, token string, opts ...connect.ClientOption) (storagev1connect.StorageServiceClient, error) {
	return NewClientFromConfig(ClientConfig{
		Endpoint: endpoint,
		Token:    token,
	}, opts...)
}
