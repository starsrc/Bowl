package rpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func buildTestControlIsolatedRoundTripper() (
	rt *controlIsolatedRoundTripper,
	controlRT *recordingRoundTripper,
	businessRT *recordingRoundTripper,
) {
	controlRT = &recordingRoundTripper{}
	businessRT = &recordingRoundTripper{}
	rt = &controlIsolatedRoundTripper{
		controlTransport:  controlRT,
		businessTransport: businessRT,
	}
	return rt, controlRT, businessRT
}

func dispatchRoundTrip(t *testing.T, rt http.RoundTripper, path string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:9000"+path, http.NoBody)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

func requireDialWrapsH2CoalesceConn(t *testing.T, name string, tr *http.Transport) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		if c, accErr := ln.Accept(); accErr == nil {
			_ = c.Close()
		}
	}()
	dialedConn, dialErr := tr.DialContext(context.Background(), "tcp", ln.Addr().String())
	require.NoError(t, dialErr, "%s dial must succeed", name)
	_, isCoalesce := dialedConn.(*h2CoalesceConn)
	require.True(t, isCoalesce, "%s http:// transport must wrap connections in *h2CoalesceConn, got %T", name, dialedConn)
	_ = dialedConn.Close()
	<-acceptDone
}

func requireTransportPingTimeouts(t *testing.T, name string, rt http.RoundTripper, wantSendPing, wantPingTimeout time.Duration) {
	t.Helper()
	tr, ok := rt.(*http.Transport)
	require.True(t, ok, "%s must be *http.Transport", name)
	require.NotNil(t, tr.HTTP2, "%s must set HTTP2Config", name)
	require.Equal(t, wantSendPing, tr.HTTP2.SendPingTimeout, "%s SendPingTimeout mismatch", name)
	require.Equal(t, wantPingTimeout, tr.HTTP2.PingTimeout, "%s PingTimeout mismatch", name)
}

func TestControlIsolatedRoundTripper_GivenControlProcedures_RoutesToDedicatedControlTransport(t *testing.T) {
	rt, controlRT, businessRT := buildTestControlIsolatedRoundTripper()

	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceMountBranchProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceRenewMountProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceUnmountBranchProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceMountBranchProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceRenewMountProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceUnmountBranchProcedure)

	require.Equal(t, int64(6), controlRT.calls.Load())
	require.Zero(t, businessRT.calls.Load())
}

func TestControlIsolatedRoundTripper_GivenDataAndMetadataProcedures_RoutesToSharedBusinessTransport(t *testing.T) {
	rt, controlRT, businessRT := buildTestControlIsolatedRoundTripper()

	procedures := []string{
		storagev1connect.StorageServiceReadProcedure,
		"/prefix" + storagev1connect.StorageServiceWriteProcedure,
		storagev1connect.StorageServiceReadCommitNodeProcedure,
		"/prefix" + storagev1connect.StorageServiceReadCommitNodeProcedure,
		storagev1connect.StorageServiceLookupProcedure,
		storagev1connect.StorageServiceGetAttrProcedure,
		storagev1connect.StorageServiceCreateProcedure,
		storagev1connect.StorageServiceUnlinkProcedure,
		storagev1connect.StorageServiceFsyncProcedure,
		storagev1connect.StorageServiceReadDirPlusProcedure,
		storagev1connect.StorageServiceStatFSProcedure,
		storagev1connect.StorageServiceCreateDiskProcedure,
		storagev1connect.StorageServiceGetDiskProcedure,
		storagev1connect.StorageServiceListDisksProcedure,
		storagev1connect.StorageServiceUpdateDiskProcedure,
		storagev1connect.StorageServiceDeleteDiskProcedure,
		storagev1connect.StorageServiceForkBranchProcedure,
		storagev1connect.StorageServiceReclaimOrphanProcedure,
		storagev1connect.StorageServiceDiffBranchesProcedure,
		storagev1connect.StorageServiceMergeBranchProcedure,
	}
	for _, proc := range procedures {
		dispatchRoundTrip(t, rt, proc)
	}

	require.Zero(t, controlRT.calls.Load())
	require.Equal(t, int64(len(procedures)), businessRT.calls.Load())
}

func TestControlIsolatedRoundTripper_WhenCloseIdleConnectionsCalled_ForwardsToControlAndBusinessTransports(t *testing.T) {
	rt, controlRT, businessRT := buildTestControlIsolatedRoundTripper()
	httpCli := &http.Client{Transport: rt}

	httpCli.CloseIdleConnections()

	require.Equal(t, int64(1), controlRT.idleClosed.Load())
	require.Equal(t, int64(1), businessRT.idleClosed.Load())
}

func TestNewClientFromConfig_GivenEndpointAndBowlTokenEnv_ResolvesEndpointAndOmitsAuthorizationHeader(t *testing.T) {
	t.Setenv(EnvStorageEndpoint, "http://10.0.0.5:9001/")
	t.Setenv("BOWL_TOKEN", "env-secret")
	resolvedEP, epErr := ResolveEndpoint("")
	require.NoError(t, epErr)
	require.Equal(t, "http://10.0.0.5:9001", resolvedEP)

	_, badSchemeErr := ResolveEndpoint("ftp://127.0.0.1:9000")
	require.Error(t, badSchemeErr)
	_, missingHostErr := ResolveEndpoint("http://")
	require.Error(t, missingHostErr)

	handler := &scriptedReadHandler{}
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000/",
		HTTPClient: newInMemoryHTTPClient(handler),
	})
	require.NoError(t, err)
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.NoError(t, err)
	require.Empty(t, handler.lastAuth)
	require.Equal(t, "identity", handler.lastAcceptEnc)
}

func TestNewControlIsolatedRoundTripper_GivenHTTPAndHTTPS_ConfiguresIsolatedH2BuffersPingTimeoutsAndH2Coalesce(t *testing.T) {
	h2cRT, err := newControlIsolatedRoundTripper("http://127.0.0.1:9000", nil)
	require.NoError(t, err)
	defer h2cRT.CloseIdleConnections()

	require.Equal(t, 8<<20, HTTP2ControlMaxReceiveBufferPerConnection)
	require.Equal(t, 64<<20, HTTP2DataMaxReceiveBufferPerConnection)
	require.Equal(t, 3*time.Second, HTTP2SendPingTimeout)
	require.Equal(t, 2*time.Second, HTTP2PingTimeout)
	require.Equal(t, 90*time.Second, HTTP2IdleConnTimeout)
	require.Equal(t, 10*time.Second, HTTP2TLSHandshakeTimeout)

	controlH2C, ok := h2cRT.controlTransport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, controlH2C.HTTP2.MaxReceiveBufferPerConnection)
	require.Equal(t, HTTP2IdleConnTimeout, controlH2C.IdleConnTimeout)
	require.NotNil(t, controlH2C.Protocols)
	require.True(t, controlH2C.Protocols.UnencryptedHTTP2())
	requireTransportPingTimeouts(t, "controlTransport", controlH2C, HTTP2SendPingTimeout, HTTP2PingTimeout)
	requireDialWrapsH2CoalesceConn(t, "controlTransport", controlH2C)

	businessH2C, ok := h2cRT.businessTransport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, HTTP2DataMaxReceiveBufferPerConnection, businessH2C.HTTP2.MaxReceiveBufferPerConnection)
	require.Equal(t, HTTP2IdleConnTimeout, businessH2C.IdleConnTimeout)
	require.NotNil(t, businessH2C.Protocols)
	require.True(t, businessH2C.Protocols.UnencryptedHTTP2())
	requireTransportPingTimeouts(t, "businessTransport", businessH2C, HTTP2SendPingTimeout, HTTP2PingTimeout)
	requireDialWrapsH2CoalesceConn(t, "businessTransport", businessH2C)

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	tlsRT, err := newControlIsolatedRoundTripper("https://storage.bowl.local:9443", tlsCfg)
	require.NoError(t, err)
	defer tlsRT.CloseIdleConnections()

	controlTLS, ok := tlsRT.controlTransport.(*http.Transport)
	require.True(t, ok)
	require.True(t, controlTLS.ForceAttemptHTTP2)
	require.Equal(t, HTTP2TLSHandshakeTimeout, controlTLS.TLSHandshakeTimeout)
	require.Equal(t, HTTP2IdleConnTimeout, controlTLS.IdleConnTimeout)
	require.NotNil(t, controlTLS.TLSClientConfig)
	require.NotSame(t, tlsCfg, controlTLS.TLSClientConfig)
	require.Equal(t, uint16(tls.VersionTLS13), controlTLS.TLSClientConfig.MinVersion)
	require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, controlTLS.HTTP2.MaxReceiveBufferPerConnection)
	requireTransportPingTimeouts(t, "controlTransportTLS", controlTLS, HTTP2SendPingTimeout, HTTP2PingTimeout)

	businessTLS, ok := tlsRT.businessTransport.(*http.Transport)
	require.True(t, ok)
	require.True(t, businessTLS.ForceAttemptHTTP2)
	require.Equal(t, HTTP2TLSHandshakeTimeout, businessTLS.TLSHandshakeTimeout)
	require.Equal(t, HTTP2IdleConnTimeout, businessTLS.IdleConnTimeout)
	require.NotNil(t, businessTLS.TLSClientConfig)
	require.NotSame(t, tlsCfg, businessTLS.TLSClientConfig)
	require.NotSame(t, controlTLS.TLSClientConfig, businessTLS.TLSClientConfig)
	require.Equal(t, uint16(tls.VersionTLS13), businessTLS.TLSClientConfig.MinVersion)
	require.Equal(t, HTTP2DataMaxReceiveBufferPerConnection, businessTLS.HTTP2.MaxReceiveBufferPerConnection)
	requireTransportPingTimeouts(t, "businessTransportTLS", businessTLS, HTTP2SendPingTimeout, HTTP2PingTimeout)
}

type tierTestStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
}

func (h *tierTestStorageHandler) MountBranch(_ context.Context, _ *connect.Request[storagev1.MountBranchRequest]) (*connect.Response[storagev1.MountBranchResponse], error) {
	return connect.NewResponse(&storagev1.MountBranchResponse{MountId: "mount-1"}), nil
}

func (h *tierTestStorageHandler) RenewMount(_ context.Context, _ *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	return connect.NewResponse(&storagev1.RenewMountResponse{LeaseTtlSeconds: 30}), nil
}

func (h *tierTestStorageHandler) UnmountBranch(_ context.Context, _ *connect.Request[storagev1.UnmountBranchRequest]) (*connect.Response[storagev1.UnmountBranchResponse], error) {
	return connect.NewResponse(&storagev1.UnmountBranchResponse{}), nil
}

func (h *tierTestStorageHandler) Lookup(_ context.Context, _ *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	return connect.NewResponse(&storagev1.LookupResponse{}), nil
}

func (h *tierTestStorageHandler) GetAttr(_ context.Context, _ *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	return connect.NewResponse(&storagev1.GetAttrResponse{}), nil
}

func (h *tierTestStorageHandler) CreateDisk(_ context.Context, _ *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	return connect.NewResponse(&storagev1.CreateDiskResponse{}), nil
}

func (h *tierTestStorageHandler) StatFS(_ context.Context, _ *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	return connect.NewResponse(&storagev1.StatFSResponse{}), nil
}

func (h *tierTestStorageHandler) Read(_ context.Context, _ *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
}

func (h *tierTestStorageHandler) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
}

func (h *tierTestStorageHandler) ReadCommitNode(_ context.Context, _ *connect.Request[storagev1.ReadCommitNodeRequest]) (*connect.Response[storagev1.ReadCommitNodeResponse], error) {
	return connect.NewResponse(&storagev1.ReadCommitNodeResponse{Data: []byte("ok")}), nil
}

type recordedProcedureAddrs struct {
	mu     sync.Mutex
	byPath map[string]map[string]struct{}
}

func newRecordedProcedureAddrs() *recordedProcedureAddrs {
	return &recordedProcedureAddrs{
		byPath: make(map[string]map[string]struct{}),
	}
}

func (r *recordedProcedureAddrs) Record(path, remoteAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	addrs, ok := r.byPath[path]
	if !ok {
		addrs = make(map[string]struct{})
		r.byPath[path] = addrs
	}
	addrs[remoteAddr] = struct{}{}
}

func (r *recordedProcedureAddrs) AddrSet(paths ...string) map[string]struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]struct{})
	for _, p := range paths {
		for addr := range r.byPath[p] {
			out[addr] = struct{}{}
		}
	}
	return out
}

func startH2CStorageServer(t *testing.T, handler storagev1connect.StorageServiceHandler, onRequest func(r *http.Request)) string {
	t.Helper()
	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		handler,
		connect.WithCodec(newProtoBinaryCodec()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if onRequest != nil {
			onRequest(r)
		}
		mux.ServeHTTP(w, r)
	})
	return startH2CHTTPServer(t, rootHandler)
}

func requireDisjointAddrSets(t *testing.T, nameA string, setA map[string]struct{}, nameB string, setB map[string]struct{}) {
	t.Helper()
	for addr := range setA {
		_, overlap := setB[addr]
		require.False(t, overlap, "%s and %s must not share TCP connection %s", nameA, nameB, addr)
	}
}

func runRepeatedBulkCalls(t *testing.T, ctx context.Context, client storagev1connect.StorageServiceClient, count int) {
	t.Helper()
	for range count {
		_, readErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
		require.NoError(t, readErr)
		_, writeErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("ab")}))
		require.NoError(t, writeErr)
		_, commitNodeErr := client.ReadCommitNode(ctx, connect.NewRequest(&storagev1.ReadCommitNodeRequest{Ino: 1, Size: 4}))
		require.NoError(t, commitNodeErr)
	}
}

func TestNewClient_GivenControlMetaAndBulkTraffic_IsolatesControlFromSharedBusinessTransport(t *testing.T) {
	recorder := newRecordedProcedureAddrs()
	endpoint := startH2CStorageServer(t, &tierTestStorageHandler{}, func(r *http.Request) {
		recorder.Record(r.URL.Path, r.RemoteAddr)
	})
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:       endpoint,
		RetryConfig:    &RetryConfig{Disable: true},
		IsolateControl: true,
	})
	require.NoError(t, err)
	defer CloseIdleConnections(client)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{DiskId: "disk-1", Branch: "main"}))
	require.NoError(t, err)
	_, err = client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{DiskId: "disk-1", Branch: "main", MountId: "mount-1"}))
	require.NoError(t, err)
	_, err = client.UnmountBranch(ctx, connect.NewRequest(&storagev1.UnmountBranchRequest{DiskId: "disk-1", MountId: "mount-1"}))
	require.NoError(t, err)

	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("a")}))
	require.NoError(t, err)
	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: 1}))
	require.NoError(t, err)
	_, err = client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "disk-2", MaxBytes: 1024}))
	require.NoError(t, err)
	_, err = client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: 1}))
	require.NoError(t, err)

	runRepeatedBulkCalls(t, ctx, client, 8)

	controlAddrs := recorder.AddrSet(
		storagev1connect.StorageServiceMountBranchProcedure,
		storagev1connect.StorageServiceRenewMountProcedure,
		storagev1connect.StorageServiceUnmountBranchProcedure,
	)
	businessAddrs := recorder.AddrSet(
		storagev1connect.StorageServiceLookupProcedure,
		storagev1connect.StorageServiceGetAttrProcedure,
		storagev1connect.StorageServiceCreateDiskProcedure,
		storagev1connect.StorageServiceStatFSProcedure,
		storagev1connect.StorageServiceReadProcedure,
		storagev1connect.StorageServiceWriteProcedure,
		storagev1connect.StorageServiceReadCommitNodeProcedure,
	)

	require.Len(t, controlAddrs, 1, "control RPCs must share one dedicated TCP connection")
	require.Len(t, businessAddrs, 1, "all data and metadata RPCs must share one business TCP connection")
	requireDisjointAddrSets(t, "control", controlAddrs, "business", businessAddrs)
}

type tcpBlackholeProxy struct {
	ln                    net.Listener
	backendAddr           string
	dials                 atomic.Int64
	blackholeFirstConn    atomic.Bool
	firstConnClientClosed chan struct{}
	firstConnCloseOnce    sync.Once
	closed                atomic.Bool
	wg                    sync.WaitGroup
	mu                    sync.Mutex
	conns                 []net.Conn
}

func startTCPBlackholeProxy(t *testing.T, backendAddr string) *tcpBlackholeProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := &tcpBlackholeProxy{
		ln:                    ln,
		backendAddr:           backendAddr,
		firstConnClientClosed: make(chan struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

func (p *tcpBlackholeProxy) Addr() string {
	return p.ln.Addr().String()
}

func (p *tcpBlackholeProxy) Dials() int64 {
	return p.dials.Load()
}

func (p *tcpBlackholeProxy) EnableBlackholeOnFirstConn() {
	p.blackholeFirstConn.Store(true)
}

func (p *tcpBlackholeProxy) WaitFirstConnEvicted(timeout time.Duration) (bool, time.Duration) {
	start := time.Now()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-p.firstConnClientClosed:
		return true, time.Since(start)
	case <-timer.C:
		return false, time.Since(start)
	}
}

func (p *tcpBlackholeProxy) trackConn(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return false
	}
	p.conns = append(p.conns, c)
	return true
}

func (p *tcpBlackholeProxy) acceptLoop() {
	defer p.wg.Done()
	dialer := &net.Dialer{Timeout: 2 * time.Second}
	for {
		clientConn, err := p.ln.Accept()
		if err != nil {
			return
		}
		backendConn, dialErr := dialer.Dial("tcp", p.backendAddr)
		if dialErr != nil {
			_ = clientConn.Close()
			continue
		}
		if !p.trackConn(clientConn) || !p.trackConn(backendConn) {
			_ = clientConn.Close()
			_ = backendConn.Close()
			return
		}

		dialNum := p.dials.Add(1)
		p.wg.Add(2)
		go p.relayClientToBackend(dialNum, clientConn, backendConn)
		go p.relayBackendToClient(dialNum, clientConn, backendConn)
	}
}

func (p *tcpBlackholeProxy) isBlackholed(dialNum int64) bool {
	return dialNum == 1 && p.blackholeFirstConn.Load()
}

func (p *tcpBlackholeProxy) relayClientToBackend(dialNum int64, clientConn, backendConn net.Conn) {
	defer p.wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, readErr := clientConn.Read(buf)
		if n > 0 && !p.isBlackholed(dialNum) {
			if _, writeErr := writeFull(backendConn, buf[:n]); writeErr != nil {
				_ = clientConn.Close()
				_ = backendConn.Close()
				return
			}
		}
		if readErr != nil {
			if dialNum == 1 {
				p.firstConnCloseOnce.Do(func() {
					close(p.firstConnClientClosed)
				})
			}
			_ = backendConn.Close()
			return
		}
	}
}

func (p *tcpBlackholeProxy) relayBackendToClient(dialNum int64, clientConn, backendConn net.Conn) {
	defer p.wg.Done()
	buf := make([]byte, 32*1024)
	for {
		n, readErr := backendConn.Read(buf)
		if n > 0 && !p.isBlackholed(dialNum) {
			if _, writeErr := writeFull(clientConn, buf[:n]); writeErr != nil {
				_ = clientConn.Close()
				_ = backendConn.Close()
				return
			}
		}
		if readErr != nil {
			if !p.isBlackholed(dialNum) {
				_ = clientConn.Close()
			}
			return
		}
	}
}

func (p *tcpBlackholeProxy) Close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	_ = p.ln.Close()
	p.mu.Lock()
	conns := make([]net.Conn, len(p.conns))
	copy(conns, p.conns)
	p.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
	p.wg.Wait()
}

func TestNewClient_GivenBlackholedTCPConnectionAfterWarmup_EvictsViaPingWithin5sAndRedials(t *testing.T) {
	backendURL := startH2CStorageServer(t, &tierTestStorageHandler{}, nil)
	proxy := startTCPBlackholeProxy(t, strings.TrimPrefix(backendURL, "http://"))
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    "http://" + proxy.Addr(),
		RetryConfig: &RetryConfig{Disable: true},
	})
	require.NoError(t, err)

	warmupCtx, cancelWarmup := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelWarmup()
	_, err = client.Lookup(warmupCtx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("warmup")}))
	require.NoError(t, err)
	require.Equal(t, int64(1), proxy.Dials())

	proxy.EnableBlackholeOnFirstConn()

	evicted, evictElapsed := proxy.WaitFirstConnEvicted(7500 * time.Millisecond)
	followCtx, cancelFollow := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelFollow()
	_, followErr := client.Lookup(followCtx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("after-evict")}))

	require.True(t, evicted, "HTTP/2 PING must close the blackholed connection within 7.5s")
	require.GreaterOrEqual(t, evictElapsed, 4500*time.Millisecond, "HTTP/2 PING eviction must cover SendPingTimeout plus PingTimeout")
	require.Less(t, evictElapsed, 7500*time.Millisecond, "HTTP/2 PING eviction must complete near 5s")
	require.NoError(t, followErr, "subsequent RPC must redial and succeed after dead connection eviction")
	require.Equal(t, int64(2), proxy.Dials(), "client must redial once after PING evicts the dead connection")
}

func TestNewClient_GivenSharedTLSConfigAndConcurrentHTTPSRequests_ClonesTLSConfigAndClosesIdleConnections(t *testing.T) {
	var (
		connMu      sync.Mutex
		openConns   = make(map[net.Conn]struct{})
		openedTotal atomic.Int64
		allClosedCh = make(chan struct{})
		allClosed   atomic.Bool
		expectClose atomic.Bool
		h2Count     atomic.Int64
	)

	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		&tierTestStorageHandler{},
		connect.WithCodec(newProtoBinaryCodec()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)
	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 {
			h2Count.Add(1)
		}
		mux.ServeHTTP(w, r)
	})

	srv := httptest.NewUnstartedServer(rootHandler)
	srv.EnableHTTP2 = true
	srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
		connMu.Lock()
		defer connMu.Unlock()
		switch state {
		case http.StateNew:
			openConns[c] = struct{}{}
			openedTotal.Add(1)
		case http.StateClosed, http.StateHijacked:
			delete(openConns, c)
			if expectClose.Load() && len(openConns) == 0 && allClosed.CompareAndSwap(false, true) {
				close(allClosedCh)
			}
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	certPool := x509.NewCertPool()
	certPool.AddCert(srv.Certificate())
	callerTLS := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:       srv.URL,
		TLSConfig:      callerTLS,
		RetryConfig:    &RetryConfig{Disable: true},
		IsolateControl: true,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const concurrentReads = 8
	var wg sync.WaitGroup
	errCh := make(chan error, 2+concurrentReads)

	wg.Go(func() {
		_, callErr := client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{DiskId: "disk-1", MountId: "mount-1"}))
		errCh <- callErr
	})

	wg.Go(func() {
		_, callErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("tls-check")}))
		errCh <- callErr
	})

	for range concurrentReads {
		wg.Go(func() {
			_, callErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
			errCh <- callErr
		})
	}

	wg.Wait()
	close(errCh)
	for callErr := range errCh {
		require.NoError(t, callErr)
	}

	require.Empty(t, callerTLS.NextProtos, "caller TLSConfig must never be mutated by control-isolated transports")
	require.Equal(t, int64(2+concurrentReads), h2Count.Load())
	require.GreaterOrEqual(t, openedTotal.Load(), int64(2), "control-isolated HTTPS client must open at least 2 TCP connections")

	openedAfterWarmup := openedTotal.Load()
	_, err = client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{DiskId: "disk-1", MountId: "mount-1"}))
	require.NoError(t, err)
	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("tls-reuse")}))
	require.NoError(t, err)
	_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
	require.NoError(t, err)
	require.Equal(t, openedAfterWarmup, openedTotal.Load(), "steady-state HTTPS control and business RPCs must reuse established H2 connections")

	connMu.Lock()
	expectClose.Store(true)
	alreadyZero := len(openConns) == 0
	connMu.Unlock()
	if alreadyZero && allClosed.CompareAndSwap(false, true) {
		close(allClosedCh)
	}

	CloseIdleConnections(client)

	select {
	case <-allClosedCh:
	case <-time.After(2 * time.Second):
		connMu.Lock()
		remaining := len(openConns)
		connMu.Unlock()
		t.Fatalf("expected all owned HTTPS connections closed after CloseIdleConnections, %d still open", remaining)
	}

	CloseIdleConnections(nil)

	customRT := &recordingRoundTripper{}
	customClient, err := NewClientFromConfig(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: &http.Client{Transport: customRT},
	})
	require.NoError(t, err)
	CloseIdleConnections(customClient)
	require.Zero(t, customRT.idleClosed.Load(), "CloseIdleConnections must be a no-op for caller-owned HTTPClient")
}

func TestNewClient_GivenDefaultSingleTransportConfig_UsesSingleTransportWithControlBufferAndH2Coalesce(t *testing.T) {
	t.Run("HTTP_SingleConnectionControlBufferAndH2Coalesce", func(t *testing.T) {
		recorder := newRecordedProcedureAddrs()
		endpoint := startH2CStorageServer(t, &tierTestStorageHandler{}, func(r *http.Request) {
			recorder.Record(r.URL.Path, r.RemoteAddr)
		})

		client, err := NewClientFromConfig(ClientConfig{
			Endpoint:    endpoint,
			RetryConfig: &RetryConfig{Disable: true},
		})
		require.NoError(t, err)
		defer CloseIdleConnections(client)

		owned, ok := client.(*ownedTransportClient)
		require.True(t, ok, "default client must be wrapped in *ownedTransportClient")
		tr, ok := owned.closer.(*http.Transport)
		require.True(t, ok, "default single-transport client must use single *http.Transport, got %T", owned.closer)
		require.NotNil(t, tr.HTTP2)
		require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, tr.HTTP2.MaxReceiveBufferPerConnection)
		require.Equal(t, HTTP2SendPingTimeout, tr.HTTP2.SendPingTimeout)
		require.Equal(t, HTTP2PingTimeout, tr.HTTP2.PingTimeout)
		require.Equal(t, HTTP2IdleConnTimeout, tr.IdleConnTimeout)
		require.NotNil(t, tr.Protocols)
		require.True(t, tr.Protocols.UnencryptedHTTP2())
		requireDialWrapsH2CoalesceConn(t, "singleTransport", tr)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{DiskId: "disk-1", Branch: "main"}))
		require.NoError(t, err)
		_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("a")}))
		require.NoError(t, err)
		runRepeatedBulkCalls(t, ctx, client, 8)

		allAddrs := recorder.AddrSet(
			storagev1connect.StorageServiceMountBranchProcedure,
			storagev1connect.StorageServiceLookupProcedure,
			storagev1connect.StorageServiceReadProcedure,
			storagev1connect.StorageServiceWriteProcedure,
			storagev1connect.StorageServiceReadCommitNodeProcedure,
		)
		require.Len(t, allAddrs, 1, "default single-transport client must multiplex all RPCs over a single TCP connection")
	})

	t.Run("HTTPS_SingleConnectionClonedTLSAndCloseIdleConnections", func(t *testing.T) {
		var (
			connMu      sync.Mutex
			openConns   = make(map[net.Conn]struct{})
			openedTotal atomic.Int64
			allClosedCh = make(chan struct{})
			allClosed   atomic.Bool
			expectClose atomic.Bool
			h2Count     atomic.Int64
		)

		path, connectHandler := storagev1connect.NewStorageServiceHandler(
			&tierTestStorageHandler{},
			connect.WithCodec(newProtoBinaryCodec()),
		)
		mux := http.NewServeMux()
		mux.Handle(path, connectHandler)
		rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ProtoMajor == 2 {
				h2Count.Add(1)
			}
			mux.ServeHTTP(w, r)
		})

		srv := httptest.NewUnstartedServer(rootHandler)
		srv.EnableHTTP2 = true
		srv.Config.ConnState = func(c net.Conn, state http.ConnState) {
			connMu.Lock()
			defer connMu.Unlock()
			switch state {
			case http.StateNew:
				openConns[c] = struct{}{}
				openedTotal.Add(1)
			case http.StateClosed, http.StateHijacked:
				delete(openConns, c)
				if expectClose.Load() && len(openConns) == 0 && allClosed.CompareAndSwap(false, true) {
					close(allClosedCh)
				}
			}
		}
		srv.StartTLS()
		t.Cleanup(srv.Close)

		certPool := x509.NewCertPool()
		certPool.AddCert(srv.Certificate())
		callerTLS := &tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		}

		client, err := NewClientFromConfig(ClientConfig{
			Endpoint:    srv.URL,
			TLSConfig:   callerTLS,
			RetryConfig: &RetryConfig{Disable: true},
		})
		require.NoError(t, err)

		owned, ok := client.(*ownedTransportClient)
		require.True(t, ok)
		tr, ok := owned.closer.(*http.Transport)
		require.True(t, ok, "default single-transport HTTPS client must use single *http.Transport, got %T", owned.closer)
		require.True(t, tr.ForceAttemptHTTP2)
		require.Equal(t, HTTP2TLSHandshakeTimeout, tr.TLSHandshakeTimeout)
		require.Equal(t, HTTP2IdleConnTimeout, tr.IdleConnTimeout)
		require.NotNil(t, tr.HTTP2)
		require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, tr.HTTP2.MaxReceiveBufferPerConnection)
		require.NotNil(t, tr.TLSClientConfig)
		require.NotSame(t, callerTLS, tr.TLSClientConfig)
		require.Equal(t, uint16(tls.VersionTLS12), tr.TLSClientConfig.MinVersion)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err = client.MountBranch(ctx, connect.NewRequest(&storagev1.MountBranchRequest{DiskId: "disk-1", Branch: "main"}))
		require.NoError(t, err)
		_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("tls-single")}))
		require.NoError(t, err)
		_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))
		require.NoError(t, err)

		require.Empty(t, callerTLS.NextProtos, "caller TLSConfig must never be mutated")
		require.Equal(t, int64(3), h2Count.Load())
		require.Equal(t, int64(1), openedTotal.Load(), "default single-transport HTTPS client must open only one TCP connection")

		connMu.Lock()
		expectClose.Store(true)
		alreadyZero := len(openConns) == 0
		connMu.Unlock()
		if alreadyZero && allClosed.CompareAndSwap(false, true) {
			close(allClosedCh)
		}

		CloseIdleConnections(client)

		select {
		case <-allClosedCh:
		case <-time.After(2 * time.Second):
			connMu.Lock()
			remaining := len(openConns)
			connMu.Unlock()
			t.Fatalf("expected owned single HTTPS connection closed after CloseIdleConnections, %d still open", remaining)
		}
	})
}

func TestResolveEndpoint_GivenWhitespacePrefixAndMalformedURLs_NormalizesAndValidatesStrictly(t *testing.T) {
	t.Run("trims explicit whitespace and preserves reverse-proxy path prefix", func(t *testing.T) {
		got, err := ResolveEndpoint("  http://127.0.0.1:9000/  ")
		require.NoError(t, err)
		require.Equal(t, "http://127.0.0.1:9000", got)

		gotPrefix, err := ResolveEndpoint(" http://127.0.0.1:9000/prefix/ ")
		require.NoError(t, err)
		require.Equal(t, "http://127.0.0.1:9000/prefix", gotPrefix)

		gotHTTPS, err := ResolveEndpoint("\thttps://storage.bowl.local:9443/v1/api///\n")
		require.NoError(t, err)
		require.Equal(t, "https://storage.bowl.local:9443/v1/api", gotHTTPS)
	})

	t.Run("rejects empty hostname, userinfo, query, and fragment", func(t *testing.T) {
		invalid := []struct {
			name string
			raw  string
		}{
			{name: "missing host", raw: "http://"},
			{name: "port without hostname", raw: "http://:9000"},
			{name: "query string", raw: "http://127.0.0.1:9000?a=b"},
			{name: "trailing question mark", raw: "http://127.0.0.1:9000?"},
			{name: "fragment", raw: "http://127.0.0.1:9000#frag"},
			{name: "empty trailing fragment", raw: "http://127.0.0.1:9000#"},
			{name: "empty trailing fragment after path", raw: "http://127.0.0.1:9000/prefix#"},
			{name: "userinfo", raw: "http://user:pass@127.0.0.1:9000"},
		}
		for _, tc := range invalid {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ResolveEndpoint(tc.raw)
				require.Error(t, err, "ResolveEndpoint(%q) must return an error", tc.raw)
			})
		}
	})
}
