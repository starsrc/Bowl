package rpc

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// failOnceConn 在 armed 时让下一次 Write 失败，但不关闭连接（模拟写失败而连接看似存活）。
type failOnceConn struct {
	net.Conn
	armed *atomic.Bool
}

func (c *failOnceConn) Write(p []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		return 0, errors.New("injected write failure")
	}
	return c.Conn.Write(p)
}

// 真实 http.Transport 端到端验证定时冲刷失败后的行为（#50 第 4 点）：
// 撞上写失败的请求必须立刻失败，而不是等到下一次写或超时；之后 Transport 换新连接，后续请求不受影响。
func TestH2CoalesceConn_GivenTimerFlushFailsOnRealTransport_FailsFastAndRedials(t *testing.T) {
	var sp http.Protocols
	sp.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Config.Protocols = &sp
	srv.Start()
	defer srv.Close()

	var armed atomic.Bool
	var dials atomic.Int32
	var cp http.Protocols
	cp.SetUnencryptedHTTP2(true)
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			dials.Add(1)
			return newH2CoalesceConn(&failOnceConn{Conn: conn, armed: &armed}), nil
		},
		Protocols: &cp,
	}
	defer tr.CloseIdleConnections()
	cl := &http.Client{Transport: tr}

	do := func(body io.Reader) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, body)
		resp, err := cl.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		_, err = io.ReadAll(resp.Body)
		return err
	}

	if err := do(strings.NewReader("warm")); err != nil {
		t.Fatalf("warmup: %v", err)
	}

	// When: HEADERS 被暂存，请求体永不到达，定时器到期后冲刷失败
	pr, pw := io.Pipe()
	defer pw.Close()
	armed.Store(true)
	start := time.Now()
	poisonErr := do(pr)
	elapsed := time.Since(start)

	// Then: 该请求立刻失败（未关闭连接时要等满 3s 超时）
	if armed.Load() {
		t.Fatal("failure was never injected")
	}
	if poisonErr == nil {
		t.Fatal("request that hit the failed flush must fail")
	}
	if elapsed > time.Second {
		t.Fatalf("request hung %v after flush failure; underlying conn was not closed", elapsed)
	}
	for i := range 5 {
		if err := do(strings.NewReader("x")); err != nil {
			t.Fatalf("follow-up %d failed: %v", i, err)
		}
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("expected transport to redial exactly once, dials=%d", got)
	}
}
