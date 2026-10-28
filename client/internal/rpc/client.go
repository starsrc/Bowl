package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"

	"connectrpc.com/connect"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"golang.org/x/net/http2"
)

const (
	// DefaultEndpoint 为未显式配置 endpoint 且未设置环境变量时的默认 StorageService 地址。
	DefaultEndpoint = "http://127.0.0.1:9000"

	// EnvStorageEndpoint 为配置 StorageService 接入地址的环境变量名。
	EnvStorageEndpoint = "BOWL_STORAGE_ENDPOINT"

	// EnvToken 为配置鉴权访问令牌的环境变量名。
	EnvToken = "BOWL_TOKEN"
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

	// RetryConfig 为可选的重试策略配置。若为 nil 则默认使用 5s 有界指数退避策略。
	RetryConfig *RetryConfig
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

// NewClientFromConfig 根据传入的 ClientConfig 构建 StorageServiceClient。
// 支持 h2c (明文 HTTP/2) 与 HTTPS TLS 传输，并在 token 非空时注入 Bearer 鉴权头。
// 底层 http.Client 不设置固定 Timeout，连接生命周期完全由 context.Context 控制。
func NewClientFromConfig(cfg ClientConfig, opts ...connect.ClientOption) (storagev1connect.StorageServiceClient, error) {
	endpoint, err := ResolveEndpoint(cfg.Endpoint)
	if err != nil {
		return nil, err
	}

	token := ResolveToken(cfg.Token)

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		u, err := url.Parse(endpoint)
		if err != nil {
			return nil, fmt.Errorf("parse endpoint %q: %w", endpoint, err)
		}

		if strings.EqualFold(u.Scheme, "http") {
			// 明文 HTTP/2 (h2c) 并发多路复用传输
			h2cTransport := &http2.Transport{
				AllowHTTP: true,
				DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, network, addr)
				},
			}
			httpClient = &http.Client{
				Transport: h2cTransport,
			}
		} else {
			// 标准 TLS HTTP/2 传输
			tlsTransport := &http.Transport{
				Proxy:             http.ProxyFromEnvironment,
				ForceAttemptHTTP2: true,
			}
			if cfg.TLSConfig != nil {
				tlsTransport.TLSClientConfig = cfg.TLSConfig
			}
			httpClient = &http.Client{
				Transport: tlsTransport,
			}
		}
	}

	var interceptors []connect.Interceptor

	// 1. 默认装配重试拦截器（位于最外层，包裹鉴权与底层网络传输）
	retryCfg := RetryConfig{}
	if cfg.RetryConfig != nil {
		retryCfg = *cfg.RetryConfig
	}
	if !retryCfg.Disable {
		interceptors = append(interceptors, NewRetryInterceptor(retryCfg))
	}

	// 2. 装配 Bearer 令牌鉴权拦截器
	if token != "" {
		authInterceptor := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				req.Header().Set("Authorization", "Bearer "+token)
				return next(ctx, req)
			}
		})
		interceptors = append(interceptors, authInterceptor)
	}

	var clientOpts []connect.ClientOption
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
