package server_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	testifymock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

// 等待同会话提交门控的真实 HTTP/2 请求取消后，必须释放处理器及其发送协程。
func TestReviewLogicR5_CanceledWatchLeavesCommitGateWait(t *testing.T) {
	hub := meta.NewSessionWatchHub()
	const sid = uint64(50505)
	allowed, unlock := hub.AcquireAutoCloseGate(sid, 0)
	require.True(t, allowed)
	defer unlock()
	mgr := metamock.NewMockManager(t)
	mgr.EXPECT().ValidateWatchSession(sid, (*string)(nil)).Return(nil).Once()
	mgr.EXPECT().WatchHub().Return(hub).Once()
	srv, err := server.New(server.Params{Config: defaultTestConfig(), MetaManager: mgr})
	require.NoError(t, err)
	handlerDone := make(chan struct{})
	requestCanceled := make(chan struct{})
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(handlerDone)
		go func() { <-r.Context().Done(); close(requestCanceled) }()
		srv.Handler().ServeHTTP(w, r)
	}))
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer func() {
		unlock()
		ts.CloseClientConnections()
		ts.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL, connect.WithGRPC())
	stream := client.WatchSession(ctx)
	require.NoError(t, stream.Send(&storagev1.WatchSessionRequest{SessionId: sid}))
	require.Eventually(t, func() bool {
		var stacks bytes.Buffer
		require.NoError(t, pprof.Lookup("goroutine").WriteTo(&stacks, 2))
		return strings.Contains(stacks.String(), "meta.(*SessionWatchHub).Register")
	}, time.Second, time.Millisecond, "请求应已进入会话提交门控等待")
	cancel()
	select {
	case <-requestCanceled:
	case <-time.After(time.Second):
		t.Fatal("服务端未收到 HTTP/2 请求取消")
	}
	select {
	case <-handlerDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("HTTP/2 上下文已取消，但 WatchSession 处理器仍等待提交门控，无法释放请求及发送协程")
	}
	// 只在断言后释放门控，确保失败由取消不生效触发，并排空试验资源。
	unlock()
	select {
	case <-handlerDone:
	case <-time.After(time.Second):
		t.Error("释放门控后处理器仍未退出")
	}
	mgr.AssertNotCalled(t, "CloseSession", testifymock.Anything, sid)
	assert.Zero(t, hub.LatestStreamID(sid), "已在门控等待中取消的请求不得在门控释放后登记新流")

	var postStacks bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&postStacks, 2))
	leakedSenders := strings.Count(postStacks.String(), "created by github.com/starsrc/bowl/services/storage/internal/server.(*Server).WatchSession")
	assert.Zero(t, leakedSenders, "WatchSession 内部发送协程必须已完全退出无残留")
}
