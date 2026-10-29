package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 探活必须在发送端阻塞时独立响应探活预算到期与调用上下文取消，避免阻塞删盘请求。
func TestReviewLogicR1_ProbeCancellationDuringBlockedSend(t *testing.T) {
	t.Run("探活预算到期", func(t *testing.T) {
		hub := meta.NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(71, func(seq uint64) error {
			close(entered)
			<-release
			return nil
		})
		defer unregister()

		done := make(chan bool, 1)
		go func() { done <- hub.Probe(ctx, 71, 20*time.Millisecond) }()
		defer func() {
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("未进入发送回调")
		}

		select {
		case alive := <-done:
			require.False(t, alive)
			require.NoError(t, ctx.Err(), "探活预算到期时调用上下文仍应未取消")
			done <- alive
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Probe 在 20ms 探活预算到期后仍阻塞于发送回调")
		}
	})

	t.Run("上下文取消", func(t *testing.T) {
		hub := meta.NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(72, func(seq uint64) error {
			close(entered)
			<-release
			return nil
		})
		defer unregister()

		done := make(chan bool, 1)
		go func() { done <- hub.Probe(ctx, 72, 2*time.Second) }()
		defer func() {
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("未进入发送回调")
		}

		select {
		case alive := <-done:
			require.False(t, alive)
			done <- alive
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Probe 在 50ms 上下文取消后仍阻塞于发送回调")
		}
	})

	t.Run("排队等待发送令牌期间上下文取消", func(t *testing.T) {
		hub := meta.NewSessionWatchHub()
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer firstCancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(73, func(seq uint64) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil
		})
		defer unregister()

		firstDone := make(chan bool, 1)
		go func() { firstDone <- hub.Probe(firstCtx, 73, 2*time.Second) }()
		defer func() {
			firstCancel()
			close(release)
			select {
			case <-firstDone:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后首个探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-firstCtx.Done():
			t.Fatal("首个 Probe 未进入发送回调")
		}

		queuedCtx, queuedCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer queuedCancel()
		queuedDone := make(chan bool, 1)
		go func() { queuedDone <- hub.Probe(queuedCtx, 73, 2*time.Second) }()

		select {
		case alive := <-queuedDone:
			require.False(t, alive)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("排队等待 sendSem 的 Probe 在上下文取消后未在 200ms 内返回 false")
		}
	})

	t.Run("排队等待发送令牌期间流被驱逐", func(t *testing.T) {
		hub := meta.NewSessionWatchHub()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		entered := make(chan struct{})
		release := make(chan struct{})
		_, unregister := hub.Register(74, func(seq uint64) error {
			select {
			case <-entered:
			default:
				close(entered)
			}
			<-release
			return nil
		})
		defer unregister()

		firstDone := make(chan bool, 1)
		go func() { firstDone <- hub.Probe(ctx, 74, 2*time.Second) }()
		defer func() {
			close(release)
			select {
			case <-firstDone:
			case <-time.After(time.Second):
				t.Error("释放发送屏障后首个探活协程仍未退出")
			}
		}()

		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("首个 Probe 未进入发送回调")
		}

		queuedDone := make(chan bool, 1)
		go func() { queuedDone <- hub.Probe(ctx, 74, 2*time.Second) }()

		time.Sleep(20 * time.Millisecond)
		hub.Evict(74)

		select {
		case alive := <-queuedDone:
			require.False(t, alive)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("排队等待 sendSem 的 Probe 在流被驱逐后未在 200ms 内返回 false")
		}
	})
}
