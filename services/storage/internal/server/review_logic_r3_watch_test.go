package server_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// HTTP/2 零接收窗口使真实 stream.Send 阻塞，驱逐流必须同时终止其发送与处理器生命周期。
func TestReviewLogicR3_EvictionTerminatesBlockedWatchHandler(t *testing.T) {
	hub := meta.NewSessionWatchHub()
	const sid = uint64(30303)
	mgr := metamock.NewMockManager(t)
	mgr.EXPECT().ValidateWatchSession(sid, (*string)(nil)).Return(nil).Once()
	mgr.EXPECT().WatchHub().Return(hub).Once()
	srv, err := server.New(server.Params{Config: defaultTestConfig(), MetaManager: mgr})
	require.NoError(t, err)
	handlerDone := make(chan struct{})
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		srv.Handler().ServeHTTP(w, r)
	}))
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	ts.Config.Protocols = &protocols
	ts.Start()
	defer ts.Close()
	conn, err := net.DialTimeout("tcp", ts.Listener.Addr().String(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))
	framer := http2.NewFramer(conn, conn)
	_, err = conn.Write([]byte(http2.ClientPreface))
	require.NoError(t, err)
	require.NoError(t, framer.WriteSettings(http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}))
	var headers bytes.Buffer
	encoder := hpack.NewEncoder(&headers)
	for _, field := range []hpack.HeaderField{
		{Name: ":method", Value: "POST"},
		{Name: ":scheme", Value: "http"},
		{Name: ":authority", Value: ts.Listener.Addr().String()},
		{Name: ":path", Value: storagev1connect.StorageServiceWatchSessionProcedure},
		{Name: "content-type", Value: "application/grpc"},
		{Name: "te", Value: "trailers"},
	} {
		require.NoError(t, encoder.WriteField(field))
	}
	require.NoError(t, framer.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: headers.Bytes(), EndHeaders: true}))
	payload, err := proto.Marshal(&storagev1.WatchSessionRequest{SessionId: sid})
	require.NoError(t, err)
	envelope := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(envelope[1:5], uint32(len(payload)))
	copy(envelope[5:], payload)
	require.NoError(t, framer.WriteData(1, false, envelope))
	readerDone := make(chan struct{})
	var writeMu sync.Mutex
	go func() {
		defer close(readerDone)
		for {
			frame, readErr := framer.ReadFrame()
			if readErr != nil {
				return
			}
			if settings, ok := frame.(*http2.SettingsFrame); ok && !settings.IsAck() {
				writeMu.Lock()
				ackErr := framer.WriteSettingsAck()
				writeMu.Unlock()
				if ackErr != nil {
					return
				}
			}
		}
	}()
	defer func() {
		require.NoError(t, conn.Close())
		select {
		case <-readerDone:
		case <-time.After(time.Second):
			t.Error("HTTP/2 读协程未退出")
		}
		select {
		case <-handlerDone:
		case <-time.After(time.Second):
			t.Error("连接关闭后处理器未退出")
		}
	}()
	countBlockedSenders := func() int {
		var stacks bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
		count := 0
		for _, stack := range strings.Split(stacks.String(), "\n\n") {
			if strings.Contains(stack, "server.(*Server).WatchSession") && strings.Contains(stack, "connect.(*BidiStream[") {
				count++
			}
		}
		return count
	}
	require.Eventually(t, func() bool { return countBlockedSenders() == 1 }, time.Second, time.Millisecond,
		"首帧确认必须已进入真实 HTTP/2 阻塞发送")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.False(t, hub.Probe(ctx, sid, 10*time.Millisecond))
	hub.Evict(sid)
	require.False(t, hub.HasActiveStream(sid))
	select {
	case <-handlerDone:
	case <-time.After(200 * time.Millisecond):
		assert.Fail(t, "流驱逐后 WatchSession 处理器仍未退出", "真实 HTTP/2 阻塞发送协程数量=%d", countBlockedSenders())
	}
	assert.Zero(t, countBlockedSenders(), "流驱逐后必须释放真实 stream.Send 协程")
}
