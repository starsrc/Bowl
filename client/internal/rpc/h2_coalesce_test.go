package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func buildTestH2HeadersFrame(streamID uint32, flags byte, payload []byte) []byte {
	frame := make([]byte, h2FrameHeaderLen+len(payload))
	n := len(payload)
	frame[0] = byte(n >> 16)
	frame[1] = byte(n >> 8)
	frame[2] = byte(n)
	frame[3] = h2FrameTypeHeaders
	frame[4] = flags
	binary.BigEndian.PutUint32(frame[5:9], streamID&0x7fffffff)
	copy(frame[h2FrameHeaderLen:], payload)
	return frame
}

func TestH2CoalesceConn_GivenHoldableHeadersFollowedBySmallData_CoalescesIntoSingleWrite(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(1, h2FlagHeadersEndHeaders, []byte("hpack-hdr"))
	data := []byte("small-data-frame-body")

	// When
	n1, err1 := conn.Write(headers)
	require.NoError(t, err1)
	require.Equal(t, len(headers), n1)
	require.Empty(t, raw.Writes(), "HEADERS(END_STREAM=0) 应暂存等待紧随的 DATA 帧")

	n2, err2 := conn.Write(data)

	// Then
	require.NoError(t, err2)
	require.Equal(t, len(data), n2)
	require.True(t, createdTimer.stopped, "合并下发后必须停止兜底定时器")
	writes := raw.Writes()
	require.Len(t, writes, 1, "HEADERS 与小 DATA 帧必须合并为单次内部 Write")
	require.Equal(t, append(append([]byte(nil), headers...), data...), writes[0])
}

func TestH2CoalesceConn_GivenHoldableHeadersFollowedByLargeData_WritesCombinedPayloadViaWritev(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		return &manualCoalesceTimer{fn: f}
	})
	headers := buildTestH2HeadersFrame(3, h2FlagHeadersEndHeaders, []byte("hdr"))
	largeData := bytes.Repeat([]byte{0x42}, h2ScratchBufferBytes+512)

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	n2, err := conn.Write(largeData)

	// Then
	require.NoError(t, err)
	require.Equal(t, len(largeData), n2)
	combined := bytes.Join(raw.Writes(), nil)
	require.Equal(t, append(append([]byte(nil), headers...), largeData...), combined)
}

func TestH2CoalesceConn_GivenNonHoldableFrame_FlushesImmediatelyWithoutTimer(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var timerCreated bool
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		timerCreated = true
		return &manualCoalesceTimer{fn: f}
	})
	endStreamHeaders := buildTestH2HeadersFrame(5, h2FlagHeadersEndHeaders|h2FlagHeadersEndStream, []byte("no-body"))
	settingsFrame := []byte{0, 0, 0, 0x04, 0x00, 0, 0, 0, 0}

	// When
	_, err := conn.Write(endStreamHeaders)
	require.NoError(t, err)
	_, err = conn.Write(settingsFrame)
	require.NoError(t, err)

	// Then
	require.False(t, timerCreated, "非暂存帧不得启动合并定时器")
	writes := raw.Writes()
	require.Len(t, writes, 2)
	require.Equal(t, endStreamHeaders, writes[0])
	require.Equal(t, settingsFrame, writes[1])
}

func TestH2CoalesceConn_GivenTimerExpiresBeforeData_FlushesHeldHeadersAndPropagatesAsyncError(t *testing.T) {
	// Given: 虚拟定时器触发时连接写失败，后续 Write 应感知并传播异步错误
	boom := errors.New("broken pipe")
	raw := &recordingNetConn{failAfter: 0, failErr: boom}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(7, h2FlagHeadersEndHeaders, []byte("hdr"))

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	createdTimer.Fire()
	require.True(t, raw.IsClosed(), "timer-flush failure must close underlying conn immediately")
	_, nextErr := conn.Write([]byte("next"))

	// Then
	require.ErrorIs(t, nextErr, boom)
}

type closeHookNetConn struct {
	*recordingNetConn
	onClose func()
}

func (c *closeHookNetConn) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return c.recordingNetConn.Close()
}

func TestH2CoalesceConn_GivenTimerFlushFailure_UnlocksMutexBeforeClosingConnAndPreservesAsyncErr(t *testing.T) {
	// Given
	boom := errors.New("broken pipe")
	raw := &recordingNetConn{failAfter: 0, failErr: boom}
	var conn net.Conn
	var closeProbeErr error
	var closeProbeTimedOut bool
	var wg sync.WaitGroup

	hooked := &closeHookNetConn{
		recordingNetConn: raw,
		onClose: func() {
			done := make(chan error, 1)
			wg.Go(func() {
				_, writeErr := conn.Write([]byte("probe-during-close"))
				done <- writeErr
			})
			timer := time.NewTimer(500 * time.Millisecond)
			defer timer.Stop()
			select {
			case closeProbeErr = <-done:
			case <-timer.C:
				closeProbeTimedOut = true
			}
		},
	}

	var createdTimer *manualCoalesceTimer
	conn = newH2CoalesceConnWithAfterFunc(hooked, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(7, h2FlagHeadersEndHeaders, []byte("hdr"))

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	createdTimer.Fire()
	wg.Wait()
	closedByTimerFlush := raw.IsClosed()
	require.NoError(t, conn.Close())
	_, postCloseWriteErr := conn.Write([]byte("after-close"))

	// Then
	require.True(t, closedByTimerFlush, "flushOnTimer must close underlying conn on write error")
	require.False(t, closeProbeTimedOut, "flushOnTimer must release c.mu before calling c.Conn.Close")
	require.ErrorIs(t, closeProbeErr, boom)
	require.ErrorIs(t, postCloseWriteErr, boom)
}

type failHoldableHeadersConn struct {
	net.Conn
	failErr  error
	failures *atomic.Int64
}

func (c *failHoldableHeadersConn) Write(p []byte) (int, error) {
	if isHoldableH2HeadersFrame(p) {
		c.failures.Add(1)
		return 0, c.failErr
	}
	return c.Conn.Write(p)
}

func startEchoH2CServer(t *testing.T) string {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	return startH2CHTTPServer(t, handler)
}

func requireWarmupH2CGet(t *testing.T, client *http.Client, targetURL string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, http.NoBody)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, readErr := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, readErr)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func doHangingPipePost(ctx context.Context, client *http.Client, targetURL string) error {
	pr, pw := io.Pipe()
	defer func() {
		_ = pw.Close()
		_ = pr.Close()
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, pr)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	return err
}

func requireFollowUpH2CPostsSucceed(t *testing.T, client *http.Client, targetURL string, count int) {
	t.Helper()
	for i := range count {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, strings.NewReader("follow-up"))
		require.NoError(t, err, "follow-up request %d build", i)
		resp, err := client.Do(req)
		cancel()
		require.NoError(t, err, "follow-up request %d do", i)
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		require.NoError(t, readErr, "follow-up request %d read body", i)
		require.Equal(t, http.StatusOK, resp.StatusCode, "follow-up request %d status", i)
		require.Equal(t, "ok", string(body), "follow-up request %d body", i)
	}
}

func TestH2CoalesceConn_GivenTimerFlushFailureOnRealH2CTransport_FailsFastAndRedialsOnce(t *testing.T) {
	// Given
	serverURL := startEchoH2CServer(t)
	var dials atomic.Int64
	var heldHeaderFailures atomic.Int64
	boom := errors.New("injected timer-flush write failure")
	dialer := &net.Dialer{Timeout: 5 * time.Second}

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	tr := &http.Transport{
		Protocols: &protocols,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			rawConn, dialErr := dialer.DialContext(ctx, network, addr)
			if dialErr != nil {
				return nil, dialErr
			}
			if dials.Add(1) == 1 {
				return newH2CoalesceConn(&failHoldableHeadersConn{
					Conn:     rawConn,
					failErr:  boom,
					failures: &heldHeaderFailures,
				}), nil
			}
			return newH2CoalesceConn(rawConn), nil
		},
	}
	defer tr.CloseIdleConnections()
	client := &http.Client{Transport: tr}

	// Complete the HTTP/2 SETTINGS handshake before testing held POST HEADERS.
	requireWarmupH2CGet(t, client, serverURL)
	require.Equal(t, int64(1), dials.Load())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// When
	start := time.Now()
	firstErr := doHangingPipePost(ctx, client, serverURL)
	elapsed := time.Since(start)

	// Then
	require.Error(t, firstErr)
	require.Equal(t, int64(1), heldHeaderFailures.Load(), "timer flush must hit the injected held HEADERS error")
	require.Less(t, elapsed, time.Second, "stalled request must fail fast rather than wait for 3s timeout")
	requireFollowUpH2CPostsSucceed(t, client, serverURL, 5)
	require.Equal(t, int64(2), dials.Load(), "transport must redial once after timer-flush teardown")
}

func TestH2CoalesceConn_GivenTimerExpiresWithHealthyConn_FlushesHeldHeaders(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(9, h2FlagHeadersEndHeaders, []byte("hdr"))

	// When
	_, err := conn.Write(headers)
	require.NoError(t, err)
	createdTimer.Fire()

	// Then
	writes := raw.Writes()
	require.Len(t, writes, 1)
	require.Equal(t, headers, writes[0])
}

func TestH2CoalesceConn_WhenClosed_StopsTimerAndClosesUnderlyingConn(t *testing.T) {
	// Given
	raw := &recordingNetConn{}
	var createdTimer *manualCoalesceTimer
	conn := newH2CoalesceConnWithAfterFunc(raw, func(_ time.Duration, f func()) coalesceTimer {
		createdTimer = &manualCoalesceTimer{fn: f}
		return createdTimer
	})
	headers := buildTestH2HeadersFrame(11, h2FlagHeadersEndHeaders, []byte("hdr"))
	_, err := conn.Write(headers)
	require.NoError(t, err)

	// When
	require.NoError(t, conn.Close())

	// Then
	require.True(t, createdTimer.stopped)
	require.True(t, raw.IsClosed())
	require.Nil(t, newH2CoalesceConn(nil))
}
