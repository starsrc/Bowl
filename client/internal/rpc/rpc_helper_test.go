package rpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// fakeClock 为测试提供确定性虚拟时钟，消除真实 sleep 与系统时间抖动。
type fakeClock struct {
	mu          sync.Mutex
	now         time.Time
	autoAdvance bool
	sleeps      []time.Duration
	timers      []*fakeClockTimer
	deadlines   []*fakeClockDeadline
	timerArmed  chan time.Duration
}

type fakeClockTimer struct {
	clk      *fakeClock
	deadline time.Time
	ch       chan time.Time
	stopped  bool
	fired    bool
}

func (t *fakeClockTimer) Chan() <-chan time.Time {
	return t.ch
}

func (t *fakeClockTimer) Stop() bool {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	wasActive := !t.stopped && !t.fired
	t.stopped = true
	return wasActive
}

type fakeClockDeadline struct {
	deadline time.Time
	cancel   context.CancelCauseFunc
	done     bool
}

func newAutoAdvanceClock(start time.Time) *fakeClock {
	return &fakeClock{
		now:         start,
		autoAdvance: true,
	}
}

func newManualClock(start time.Time) *fakeClock {
	return &fakeClock{
		now:        start,
		timerArmed: make(chan time.Duration, 16),
	}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Since(t time.Time) time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now.Sub(t)
}

func (c *fakeClock) Sleeps() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]time.Duration, len(c.sleeps))
	copy(out, c.sleeps)
	return out
}

func (c *fakeClock) TimerArmed() <-chan time.Duration {
	return c.timerArmed
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.advanceLocked(d)
	c.mu.Unlock()
}

func (c *fakeClock) advanceLocked(d time.Duration) {
	c.now = c.now.Add(d)
	for _, dl := range c.deadlines {
		if !dl.done && !c.now.Before(dl.deadline) {
			dl.done = true
			dl.cancel(context.DeadlineExceeded)
		}
	}
	for _, tm := range c.timers {
		if !tm.stopped && !tm.fired && !c.now.Before(tm.deadline) {
			tm.fired = true
			select {
			case tm.ch <- c.now:
			default:
			}
		}
	}
}

func (c *fakeClock) NewTimer(d time.Duration) rpcTimer {
	c.mu.Lock()
	c.sleeps = append(c.sleeps, d)
	tm := &fakeClockTimer{
		clk:      c,
		deadline: c.now.Add(d),
		ch:       make(chan time.Time, 1),
	}
	if c.autoAdvance {
		c.timers = append(c.timers, tm)
		c.advanceLocked(d)
		c.mu.Unlock()
		return tm
	}
	if d <= 0 {
		tm.fired = true
		tm.ch <- c.now
	} else {
		c.timers = append(c.timers, tm)
	}
	ch := c.timerArmed
	c.mu.Unlock()
	if ch != nil {
		ch <- d
	}
	return tm
}

func (c *fakeClock) WithDeadline(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc) {
	ctx, cancelCause := context.WithCancelCause(parent)
	c.mu.Lock()
	dl := &fakeClockDeadline{
		deadline: deadline,
		cancel:   cancelCause,
	}
	if !c.now.Before(deadline) {
		dl.done = true
		cancelCause(context.DeadlineExceeded)
	} else {
		c.deadlines = append(c.deadlines, dl)
	}
	c.mu.Unlock()
	return ctx, func() {
		c.mu.Lock()
		dl.done = true
		c.mu.Unlock()
		cancelCause(nil)
	}
}

// inMemoryHandlerClient 将 ConnectRPC 处理适配器直接挂载在进程内 RoundTripper 接缝上，避免监听真实 TCP 端口。
func newInMemoryHTTPClient(h storagev1connect.StorageServiceHandler) *http.Client {
	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		h,
		connect.WithCodec(newProtoBinaryCodec()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)
	return &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			resp := rec.Result()
			resp.Request = req
			return resp, nil
		}),
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

type httpClientWithFirstErr struct {
	firstErr error
	next     http.RoundTripper
	attempts *int
}

func (c *httpClientWithFirstErr) HTTPClient() *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			*c.attempts++
			if *c.attempts == 1 {
				return nil, c.firstErr
			}
			return c.next.RoundTrip(req)
		}),
	}
}

type scriptedTransport struct {
	errs  []error
	next  http.RoundTripper
	calls int
}

func (s *scriptedTransport) HTTPClient() *http.Client {
	return &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			idx := s.calls
			s.calls++
			if idx < len(s.errs) {
				return nil, s.errs[idx]
			}
			return s.next.RoundTrip(req)
		}),
	}
}

// scriptedReadHandler 按预设响应序列应答 Read 请求并记录请求头，无分支猜测逻辑。
type scriptedReadHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu              sync.Mutex
	steps           []func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	calls           int
	lastAuth        string
	lastAcceptEnc   string
	lastRequestID   string
	lastTraceparent string
	onCall          func()
}

func (h *scriptedReadHandler) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	h.mu.Lock()
	idx := h.calls
	h.calls++
	h.lastAuth = req.Header().Get("Authorization")
	h.lastAcceptEnc = req.Header().Get("Accept-Encoding")
	h.lastRequestID = req.Header().Get(HeaderRequestID)
	h.lastTraceparent = req.Header().Get("Traceparent")
	var step func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	if idx < len(h.steps) {
		step = h.steps[idx]
	} else if len(h.steps) > 0 {
		step = h.steps[len(h.steps)-1]
	}
	cb := h.onCall
	h.mu.Unlock()

	if cb != nil {
		cb()
	}
	if step != nil {
		return step(ctx, req)
	}
	return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
}

func (h *scriptedReadHandler) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	h.mu.Lock()
	idx := h.calls
	h.calls++
	h.lastAuth = req.Header().Get("Authorization")
	h.lastAcceptEnc = req.Header().Get("Accept-Encoding")
	h.lastRequestID = req.Header().Get(HeaderRequestID)
	h.lastTraceparent = req.Header().Get("Traceparent")
	var step func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	if idx < len(h.steps) {
		step = h.steps[idx]
	} else if len(h.steps) > 0 {
		step = h.steps[len(h.steps)-1]
	}
	h.mu.Unlock()

	if step != nil {
		_, err := step(context.Background(), connect.NewRequest(&storagev1.ReadRequest{}))
		if err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
}

// recordingRoundTripper 记录路由次数与空闲连接关闭调用，用于验证条带传输分布。
type recordingRoundTripper struct {
	calls      atomic.Int64
	idleClosed atomic.Int64
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

func (r *recordingRoundTripper) CloseIdleConnections() {
	r.idleClosed.Add(1)
}

func requireMetricFamily(t *testing.T, mfs []*dto.MetricFamily, name string) *dto.MetricFamily {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == name {
			return mf
		}
	}
	t.Fatalf("未找到期望的 Prometheus 指标 %q", name)
	return nil
}

func requirePrometheusHistogramTotalCount(t *testing.T, mf *dto.MetricFamily, wantCount uint64, wantBuckets int, wantFirstBucket float64) {
	t.Helper()
	var total uint64
	for _, m := range mf.GetMetric() {
		h := m.GetHistogram()
		total += h.GetSampleCount()
		require.Len(t, h.GetBucket(), wantBuckets, "指标 %s 桶数量不匹配", mf.GetName())
		require.InDelta(t, wantFirstBucket, h.GetBucket()[0].GetUpperBound(), 1e-9, "指标 %s 首桶上限不匹配", mf.GetName())
	}
	require.Equal(t, wantCount, total, "指标 %s 样本数不匹配", mf.GetName())
}

func requireOTelMetricHistogram(t *testing.T, rm metricdata.ResourceMetrics, name string, wantCount uint64, wantFirstBoundary float64) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "指标 %s 应为 float64 Histogram", name)
			var total uint64
			for _, dp := range hist.DataPoints {
				total += dp.Count
				require.NotEmpty(t, dp.Bounds)
				require.InDelta(t, wantFirstBoundary, dp.Bounds[0], 1e-9)
			}
			require.Equal(t, wantCount, total, "OTel 指标 %s 样本数不匹配", name)
			return
		}
	}
	t.Fatalf("ManualReader 未采集到 OTel 指标 %q", name)
}

func requireSpanByName(t *testing.T, spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	t.Helper()
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	t.Fatalf("未采集到期望 Span %q", name)
	return nil
}

func requireSpanAttr(t *testing.T, sp *tracetest.SpanStub, key, want string) {
	t.Helper()
	for _, attr := range sp.Attributes {
		if string(attr.Key) == key {
			require.Equal(t, want, attr.Value.AsString(), "Span %s 属性 %s 不匹配", sp.Name, key)
			return
		}
	}
	t.Fatalf("Span %s 缺少属性 %q", sp.Name, key)
}

var _ sdkmetric.Reader = (*sdkmetric.ManualReader)(nil)

// recordingNetConn 为 h2_coalesce_test.go 提供纯内存 net.Conn 适配器。
type recordingNetConn struct {
	mu        sync.Mutex
	writes    [][]byte
	failAfter int
	failErr   error
	closed    bool
}

func (c *recordingNetConn) Read(_ []byte) (int, error) {
	return 0, io.EOF
}

func (c *recordingNetConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failErr != nil && len(c.writes) >= c.failAfter {
		return 0, c.failErr
	}
	cp := make([]byte, len(p))
	copy(cp, p)
	c.writes = append(c.writes, cp)
	return len(p), nil
}

func (c *recordingNetConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *recordingNetConn) LocalAddr() net.Addr                { return dummyAddr("local") }
func (c *recordingNetConn) RemoteAddr() net.Addr               { return dummyAddr("remote") }
func (c *recordingNetConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingNetConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingNetConn) SetWriteDeadline(_ time.Time) error { return nil }

func (c *recordingNetConn) Writes() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.writes))
	copy(out, c.writes)
	return out
}

func (c *recordingNetConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type dummyAddr string

func (a dummyAddr) Network() string { return "tcp" }
func (a dummyAddr) String() string  { return string(a) }

type manualCoalesceTimer struct {
	mu       sync.Mutex
	fn       func()
	stopped  bool
	resets   int
	lastWait time.Duration
}

func (t *manualCoalesceTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped
	t.stopped = true
	return wasActive
}

func (t *manualCoalesceTimer) Reset(d time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped
	t.stopped = false
	t.resets++
	t.lastWait = d
	return wasActive
}

func (t *manualCoalesceTimer) Fire() {
	t.mu.Lock()
	fn := t.fn
	t.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func startH2CHTTPServer(t *testing.T, handler http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:   handler,
		Protocols: &protocols,
	}
	serveErrCh := make(chan error, 1)
	go func() {
		serveErrCh <- srv.Serve(ln)
	}()
	t.Cleanup(func() {
		_ = srv.Close()
		serveErr := <-serveErrCh
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			t.Errorf("unexpected h2c server exit: %v", serveErr)
		}
	})
	return "http://" + ln.Addr().String()
}
