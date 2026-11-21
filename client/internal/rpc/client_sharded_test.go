package rpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
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

func buildTestShardedRoundTripper() (
	rt *shardedRoundTripper,
	controlRT *recordingRoundTripper,
	metaRT *recordingRoundTripper,
	dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper,
) {
	controlRT = &recordingRoundTripper{}
	metaRT = &recordingRoundTripper{}
	var shards [HTTP2DataConnectionShards]*recordingRoundTripper
	rt = &shardedRoundTripper{
		controlTransport: controlRT,
		metaTransport:    metaRT,
	}
	for i := range HTTP2DataConnectionShards {
		shards[i] = &recordingRoundTripper{}
		rt.dataTransports[i] = shards[i]
	}
	return rt, controlRT, metaRT, &shards
}

func dispatchRoundTrip(t *testing.T, rt http.RoundTripper, path string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:9000"+path, http.NoBody)
	require.NoError(t, err)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
}

func requireAllDataShardCalls(t *testing.T, dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper, want int64) {
	t.Helper()
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, want, dataRTs[i].calls.Load(), "data shard %d call count mismatch", i)
	}
}

func requireAllDataShardIdleClosed(t *testing.T, dataRTs *[HTTP2DataConnectionShards]*recordingRoundTripper, want int64) {
	t.Helper()
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, want, dataRTs[i].idleClosed.Load(), "data shard %d idle close count mismatch", i)
	}
}

func requireAllDataTransportsConfig(t *testing.T, rt *shardedRoundTripper, wantBuf int, wantHTTPS bool, wantTLS *tls.Config) {
	t.Helper()
	for i := range HTTP2DataConnectionShards {
		tr, ok := rt.dataTransports[i].(*http.Transport)
		require.True(t, ok, "data shard %d must be *http.Transport", i)
		require.NotNil(t, tr.HTTP2, "data shard %d must set HTTP2Config", i)
		require.Equal(t, wantBuf, tr.HTTP2.MaxReceiveBufferPerConnection, "data shard %d buffer mismatch", i)
		if wantHTTPS {
			require.True(t, tr.ForceAttemptHTTP2, "data shard %d must enable ForceAttemptHTTP2", i)
			require.Same(t, wantTLS, tr.TLSClientConfig, "data shard %d TLSConfig mismatch", i)
		}
	}
}

func TestShardedRoundTripper_GivenReadWriteAndReadSnapshotNodeTraffic_DistributesRoundRobinAcross8DataShards(t *testing.T) {

	rt, controlRT, metaRT, dataRTs := buildTestShardedRoundTripper()

	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadSnapshotNodeProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceReadSnapshotNodeProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadSnapshotNodeProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadSnapshotNodeProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceWriteProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadSnapshotNodeProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadProcedure)

	require.Zero(t, controlRT.calls.Load(), "bulk data traffic must not use controlTransport")
	require.Zero(t, metaRT.calls.Load(), "bulk data traffic must not use metaTransport")
	requireAllDataShardCalls(t, dataRTs, 2)
}

func TestShardedRoundTripper_GivenControlProcedures_RoutesToDedicatedControlTransport(t *testing.T) {

	rt, controlRT, metaRT, dataRTs := buildTestShardedRoundTripper()

	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceMountDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceRenewMountProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceUnmountDiskProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceMountDiskProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceRenewMountProcedure)
	dispatchRoundTrip(t, rt, "/prefix"+storagev1connect.StorageServiceUnmountDiskProcedure)

	require.Equal(t, int64(6), controlRT.calls.Load())
	require.Zero(t, metaRT.calls.Load())
	requireAllDataShardCalls(t, dataRTs, 0)
}

func TestShardedRoundTripper_GivenMetadataAndRemainingDiskTraffic_RoutesToDedicatedMetaTransport(t *testing.T) {

	rt, controlRT, metaRT, dataRTs := buildTestShardedRoundTripper()

	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceLookupProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceGetAttrProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceCreateProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceUnlinkProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceFsyncProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReadDirPlusProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceStatFSProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceCreateDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceGetDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceListDisksProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceUpdateDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceDeleteDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceForkDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceReclaimOrphanProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceDiffDiskProcedure)
	dispatchRoundTrip(t, rt, storagev1connect.StorageServiceMergeDiskProcedure)

	require.Zero(t, controlRT.calls.Load())
	require.Equal(t, int64(16), metaRT.calls.Load())
	requireAllDataShardCalls(t, dataRTs, 0)
}

func TestShardedRoundTripper_WhenCloseIdleConnectionsCalled_ForwardsToControlMetaAndAll8DataShards(t *testing.T) {

	rt, controlRT, metaRT, dataRTs := buildTestShardedRoundTripper()
	httpCli := &http.Client{Transport: rt}

	httpCli.CloseIdleConnections()

	require.Equal(t, int64(1), controlRT.idleClosed.Load())
	require.Equal(t, int64(1), metaRT.idleClosed.Load())
	requireAllDataShardIdleClosed(t, dataRTs, 1)
}

func TestNewClient_GivenEndpointAndTokenConfig_ResolvesAndInjectsHeaders(t *testing.T) {

	t.Setenv(EnvStorageEndpoint, "http://10.0.0.5:9001/")
	t.Setenv(EnvToken, "env-secret")
	resolvedEP, epErr := ResolveEndpoint("")
	require.NoError(t, epErr)
	require.Equal(t, "http://10.0.0.5:9001", resolvedEP)
	require.Equal(t, "env-secret", ResolveToken(""))
	require.Equal(t, "explicit-tok", ResolveToken("  explicit-tok  "))

	_, badSchemeErr := ResolveEndpoint("ftp://127.0.0.1:9000")
	require.Error(t, badSchemeErr)
	_, missingHostErr := ResolveEndpoint("http://")
	require.Error(t, missingHostErr)

	handler := &scriptedReadHandler{}
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000/",
		Token:      "tok-123",
		HTTPClient: newInMemoryHTTPClient(handler),
	})
	require.NoError(t, err)
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	require.NoError(t, err)
	require.Equal(t, "Bearer tok-123", handler.lastAuth)
	require.Equal(t, "identity", handler.lastAcceptEnc)
}

func TestNewShardedRoundTripper_GivenHTTPAndHTTPS_ConfiguresIsolatedH2Buffers(t *testing.T) {

	h2cRT, err := newShardedRoundTripper("http://127.0.0.1:9000", nil)
	require.NoError(t, err)
	defer h2cRT.CloseIdleConnections()

	controlH2C, ok := h2cRT.controlTransport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 8<<20, HTTP2ControlMaxReceiveBufferPerConnection)
	require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, controlH2C.HTTP2.MaxReceiveBufferPerConnection)

	metaH2C, ok := h2cRT.metaTransport.(*http.Transport)
	require.True(t, ok)
	require.Equal(t, 32<<20, HTTP2MetaMaxReceiveBufferPerConnection)
	require.Equal(t, HTTP2MetaMaxReceiveBufferPerConnection, metaH2C.HTTP2.MaxReceiveBufferPerConnection)
	requireAllDataTransportsConfig(t, h2cRT, HTTP2DataMaxReceiveBufferPerConnection, false, nil)

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	tlsRT, err := newShardedRoundTripper("https://storage.bowl.local:9443", tlsCfg)
	require.NoError(t, err)
	defer tlsRT.CloseIdleConnections()

	controlTLS, ok := tlsRT.controlTransport.(*http.Transport)
	require.True(t, ok)
	require.True(t, controlTLS.ForceAttemptHTTP2)
	require.Same(t, tlsCfg, controlTLS.TLSClientConfig)
	require.Equal(t, HTTP2ControlMaxReceiveBufferPerConnection, controlTLS.HTTP2.MaxReceiveBufferPerConnection)

	metaTLS, ok := tlsRT.metaTransport.(*http.Transport)
	require.True(t, ok)
	require.True(t, metaTLS.ForceAttemptHTTP2)
	require.Same(t, tlsCfg, metaTLS.TLSClientConfig)
	require.Equal(t, HTTP2MetaMaxReceiveBufferPerConnection, metaTLS.HTTP2.MaxReceiveBufferPerConnection)
	requireAllDataTransportsConfig(t, tlsRT, HTTP2DataMaxReceiveBufferPerConnection, true, tlsCfg)
}

type tierTestStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
}

func (h *tierTestStorageHandler) MountDisk(_ context.Context, _ *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	return connect.NewResponse(&storagev1.MountDiskResponse{MountId: "mount-1"}), nil
}

func (h *tierTestStorageHandler) RenewMount(_ context.Context, _ *connect.Request[storagev1.RenewMountRequest]) (*connect.Response[storagev1.RenewMountResponse], error) {
	return connect.NewResponse(&storagev1.RenewMountResponse{LeaseTtlSeconds: 30}), nil
}

func (h *tierTestStorageHandler) UnmountDisk(_ context.Context, _ *connect.Request[storagev1.UnmountDiskRequest]) (*connect.Response[storagev1.UnmountDiskResponse], error) {
	return connect.NewResponse(&storagev1.UnmountDiskResponse{}), nil
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

func (h *tierTestStorageHandler) ReadSnapshotNode(_ context.Context, _ *connect.Request[storagev1.ReadSnapshotNodeRequest]) (*connect.Response[storagev1.ReadSnapshotNodeResponse], error) {
	return connect.NewResponse(&storagev1.ReadSnapshotNodeResponse{Data: []byte("ok")}), nil
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
		_, snapErr := client.ReadSnapshotNode(ctx, connect.NewRequest(&storagev1.ReadSnapshotNodeRequest{Ino: 1, Size: 4}))
		require.NoError(t, snapErr)
	}
}

func TestNewClient_GivenControlMetaAndBulkTraffic_IsolatesThreeTransportTiers(t *testing.T) {

	recorder := newRecordedProcedureAddrs()
	endpoint := startH2CStorageServer(t, &tierTestStorageHandler{}, func(r *http.Request) {
		recorder.Record(r.URL.Path, r.RemoteAddr)
	})
	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:    endpoint,
		RetryConfig: &RetryConfig{Disable: true},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{DiskId: "disk-1"}))
	require.NoError(t, err)
	_, err = client.RenewMount(ctx, connect.NewRequest(&storagev1.RenewMountRequest{DiskId: "disk-1", MountId: "mount-1"}))
	require.NoError(t, err)
	_, err = client.UnmountDisk(ctx, connect.NewRequest(&storagev1.UnmountDiskRequest{DiskId: "disk-1", MountId: "mount-1"}))
	require.NoError(t, err)

	_, err = client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("a")}))
	require.NoError(t, err)
	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: 1}))
	require.NoError(t, err)
	_, err = client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "disk-2", MaxBytes: 1024}))
	require.NoError(t, err)
	_, err = client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: 1}))
	require.NoError(t, err)

	runRepeatedBulkCalls(t, ctx, client, HTTP2DataConnectionShards)

	controlAddrs := recorder.AddrSet(
		storagev1connect.StorageServiceMountDiskProcedure,
		storagev1connect.StorageServiceRenewMountProcedure,
		storagev1connect.StorageServiceUnmountDiskProcedure,
	)
	metaAddrs := recorder.AddrSet(
		storagev1connect.StorageServiceLookupProcedure,
		storagev1connect.StorageServiceGetAttrProcedure,
		storagev1connect.StorageServiceCreateDiskProcedure,
		storagev1connect.StorageServiceStatFSProcedure,
	)
	dataAddrs := recorder.AddrSet(
		storagev1connect.StorageServiceReadProcedure,
		storagev1connect.StorageServiceWriteProcedure,
		storagev1connect.StorageServiceReadSnapshotNodeProcedure,
	)
	snapNodeAddrs := recorder.AddrSet(storagev1connect.StorageServiceReadSnapshotNodeProcedure)

	require.Len(t, controlAddrs, 1, "control RPCs must share one dedicated TCP connection")
	require.Len(t, metaAddrs, 1, "metadata and disk RPCs must share one dedicated TCP connection")
	require.Len(t, dataAddrs, HTTP2DataConnectionShards, "bulk data RPCs must span 8 TCP connections")
	require.Len(t, snapNodeAddrs, HTTP2DataConnectionShards, "ReadSnapshotNode must span all 8 data TCP connections")
	requireDisjointAddrSets(t, "control", controlAddrs, "meta", metaAddrs)
	requireDisjointAddrSets(t, "control", controlAddrs, "data", dataAddrs)
	requireDisjointAddrSets(t, "meta", metaAddrs, "data", dataAddrs)
}

func requireTransportPingTimeouts(t *testing.T, name string, rt http.RoundTripper, wantSendPing, wantPingTimeout time.Duration) {
	t.Helper()
	tr, ok := rt.(*http.Transport)
	require.True(t, ok, "%s must be *http.Transport", name)
	require.NotNil(t, tr.HTTP2, "%s must set HTTP2Config", name)
	require.Equal(t, wantSendPing, tr.HTTP2.SendPingTimeout, "%s SendPingTimeout mismatch", name)
	require.Equal(t, wantPingTimeout, tr.HTTP2.PingTimeout, "%s PingTimeout mismatch", name)
}

func requireShardedRoundTripperPingTimeouts(t *testing.T, rt *shardedRoundTripper, wantSendPing, wantPingTimeout time.Duration) {
	t.Helper()
	requireTransportPingTimeouts(t, "controlTransport", rt.controlTransport, wantSendPing, wantPingTimeout)
	requireTransportPingTimeouts(t, "metaTransport", rt.metaTransport, wantSendPing, wantPingTimeout)
	for i := range HTTP2DataConnectionShards {
		requireTransportPingTimeouts(t, "dataTransport", rt.dataTransports[i], wantSendPing, wantPingTimeout)
	}
}

func TestNewShardedRoundTripper_GivenHTTPAndHTTPS_ConfiguresPingTimeoutsAcrossAllThreeTiers(t *testing.T) {

	h2cRT, err := newShardedRoundTripper("http://127.0.0.1:9000", nil)
	require.NoError(t, err)
	defer h2cRT.CloseIdleConnections()

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS13}
	tlsRT, err := newShardedRoundTripper("https://storage.bowl.local:9443", tlsCfg)
	require.NoError(t, err)
	defer tlsRT.CloseIdleConnections()

	require.Equal(t, 3*time.Second, HTTP2SendPingTimeout)
	require.Equal(t, 2*time.Second, HTTP2PingTimeout)
	requireShardedRoundTripperPingTimeouts(t, h2cRT, HTTP2SendPingTimeout, HTTP2PingTimeout)
	requireShardedRoundTripperPingTimeouts(t, tlsRT, HTTP2SendPingTimeout, HTTP2PingTimeout)
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
