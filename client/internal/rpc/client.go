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
	DefaultEndpoint = "http://127.0.0.1:9000"

	EnvStorageEndpoint = "BOWL_STORAGE_ENDPOINT"

	HTTP2MaxReadFrameSize = 1 << 20

	HTTP2MaxReceiveBufferPerStream = 8 << 20

	HTTP2ControlMaxReceiveBufferPerConnection = 8 << 20

	HTTP2MetaMaxReceiveBufferPerConnection = 32 << 20

	HTTP2DataMaxReceiveBufferPerConnection = 64 << 20

	HTTP2DataConnectionShards = 8

	HTTP2SendPingTimeout = 3 * time.Second

	HTTP2PingTimeout = 2 * time.Second

	HTTP2TLSHandshakeTimeout = 10 * time.Second

	HTTP2IdleConnTimeout = 90 * time.Second
)

type ClientConfig struct {
	Endpoint string

	HTTPClient *http.Client

	TLSConfig *tls.Config

	RetryConfig *RetryConfig

	Logger *slog.Logger

	Metrics *Metrics
}

func ResolveEndpoint(endpoint string) (string, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
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

	if u.Hostname() == "" {
		return "", fmt.Errorf("storage endpoint %q missing host", endpoint)
	}
	if u.User != nil {
		return "", fmt.Errorf("storage endpoint %q must not contain userinfo", endpoint)
	}
	if u.RawQuery != "" || u.ForceQuery {
		return "", fmt.Errorf("storage endpoint %q must not contain query parameters", endpoint)
	}
	if u.Fragment != "" || strings.Contains(endpoint, "#") {
		return "", fmt.Errorf("storage endpoint %q must not contain a fragment", endpoint)
	}

	return strings.TrimRight(endpoint, "/"), nil
}

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
			IdleConnTimeout: HTTP2IdleConnTimeout,
			Protocols:       &protocols,
			HTTP2:           h2Cfg,
		}, nil
	}

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
		TLSHandshakeTimeout: HTTP2TLSHandshakeTimeout,
		IdleConnTimeout:     HTTP2IdleConnTimeout,
		ForceAttemptHTTP2:   true,
		HTTP2:               h2Cfg,
	}
	if tlsConfig != nil {
		tlsTransport.TLSClientConfig = tlsConfig.Clone()
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

func isControlProcedure(urlPath string) bool {
	return matchesProcedure(
		urlPath,
		storagev1connect.StorageServiceMountBranchProcedure,
		storagev1connect.StorageServiceRenewMountProcedure,
		storagev1connect.StorageServiceUnmountBranchProcedure,
	)
}

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

type ownedTransportClient struct {
	storagev1connect.StorageServiceClient
	closer idleConnCloser
}

func (c *ownedTransportClient) CloseIdleConnections() {
	if c != nil && c.closer != nil {
		c.closer.CloseIdleConnections()
	}
}

func CloseIdleConnections(client storagev1connect.StorageServiceClient) {
	if client == nil {
		return
	}
	if closer, ok := client.(idleConnCloser); ok {
		closer.CloseIdleConnections()
	}
}

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

	var ownedCloser idleConnCloser
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		tr, trErr := newShardedRoundTripper(endpoint, cfg.TLSConfig)
		if trErr != nil {
			return nil, trErr
		}
		ownedCloser = tr
		httpClient = &http.Client{
			Transport: tr,
		}
	}

	var interceptors []connect.Interceptor

	retryCfg := RetryConfig{}
	if cfg.RetryConfig != nil {
		retryCfg = *cfg.RetryConfig
	}
	interceptors = append(interceptors,
		newRetryInterceptor(retryCfg, cfg.Logger, cfg.Metrics, clk),
		newRequestObservabilityInterceptor(cfg.Logger, cfg.Metrics, DefaultSlowRPCThreshold, clk),
	)

	otelInterceptor, err := otelconnect.NewInterceptor(
		otelconnect.WithDurationHistogramOptions(
			metric.WithExplicitBucketBoundaries(SubMillisecondDurationBuckets...),
		),
	)
	if err != nil {
		if ownedCloser != nil {
			ownedCloser.CloseIdleConnections()
		}
		return nil, fmt.Errorf("create otelconnect interceptor: %w", err)
	}
	interceptors = append(
		interceptors,
		otelInterceptor,
		serverx.NewErrorReasonInterceptor(),
		connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				req.Header().Set("Accept-Encoding", "identity")
				return next(ctx, req)
			}
		}),
	)

	clientOpts := []connect.ClientOption{
		connect.WithCodec(newProtoBinaryCodec()),
	}
	if len(interceptors) > 0 {
		clientOpts = append(clientOpts, connect.WithInterceptors(interceptors...))
	}

	if len(opts) > 0 {
		clientOpts = append(clientOpts, opts...)
	}

	client := storagev1connect.NewStorageServiceClient(httpClient, endpoint, clientOpts...)
	if ownedCloser != nil {
		return &ownedTransportClient{
			StorageServiceClient: client,
			closer:               ownedCloser,
		}, nil
	}
	return client, nil
}
