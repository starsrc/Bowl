package fusefs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

type reclaimStubClient struct {
	storagev1connect.StorageServiceClient
	hook  func(context.Context, *storagev1.ReclaimOrphanRequest) error
	calls atomic.Int64
}

func (c *reclaimStubClient) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	c.calls.Add(1)
	if c.hook != nil {
		if err := c.hook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}

func (c *reclaimStubClient) FsyncDir(_ context.Context, _ *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

type threadSafeLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *threadSafeLogBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *threadSafeLogBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func countLogMsg(t *testing.T, raw, msg string) int {
	t.Helper()
	n := 0
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &rec))
		if rec["msg"] == msg {
			n++
		}
	}
	return n
}

func TestStep18_ShouldRetryReclaim_RejectsResourceExhaustedAndAborted(t *testing.T) {
	require.True(t, shouldRetryReclaim(connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))))
	require.True(t, shouldRetryReclaim(connect.NewError(connect.CodeDeadlineExceeded, errors.New("timeout"))))
	require.True(t, shouldRetryReclaim(connect.NewError(connect.CodeInternal, errors.New("internal"))))
	require.True(t, shouldRetryReclaim(context.DeadlineExceeded))

	require.False(t, shouldRetryReclaim(connect.NewError(connect.CodeResourceExhausted, errors.New("quota"))))
	require.False(t, shouldRetryReclaim(connect.NewError(connect.CodeAborted, errors.New("aborted"))))
	require.False(t, shouldRetryReclaim(connect.NewError(connect.CodeNotFound, errors.New("not found"))))
	require.False(t, shouldRetryReclaim(nil))
}

func TestStep18_ReleaseQueue_DeduplicatesCapsBackoffsAndDrainsOnUnmount(t *testing.T) {
	t.Run("DeduplicatesByServerInoAndCapsAtMaxSizeWithDroppedWarn", func(t *testing.T) {
		var logBuf threadSafeLogBuf
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		client := &reclaimStubClient{
			hook: func(context.Context, *storagev1.ReclaimOrphanRequest) error {
				return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
		}
		now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
		fs := newRawFS(client, Config{
			DiskID:  "disk-step18-cap",
			RootIno: 1,
			Mode:    MountModeSync,
			Logger:  logger,
		}, func() time.Time { return now })
		fs.releaseMu.Lock()
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		close(fs.releaseLoopDone)
		fs.releaseMu.Unlock()
		t.Cleanup(fs.OnUnmount)

		for ino := uint64(100); ino < 100+DefaultReleaseQueueMaxSize; ino++ {
			fs.markOrphanWithContext(context.Background(), ino)
		}
		require.Equal(t, DefaultReleaseQueueMaxSize, fs.releaseQueueDepth())

		fs.markOrphanWithContext(context.Background(), 100)
		require.Equal(t, DefaultReleaseQueueMaxSize, fs.releaseQueueDepth())

		fs.markOrphanWithContext(context.Background(), 99999)
		require.Equal(t, DefaultReleaseQueueMaxSize, fs.releaseQueueDepth())
		require.GreaterOrEqual(t, countLogMsg(t, logBuf.String(), "orphan_reclaim_dropped"), 1)
	})

	t.Run("ExponentialBackoffAndMaxAttemptsDropsItem", func(t *testing.T) {
		var logBuf threadSafeLogBuf
		logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		client := &reclaimStubClient{
			hook: func(context.Context, *storagev1.ReclaimOrphanRequest) error {
				return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
		}
		nowMu := sync.Mutex{}
		now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
		getNow := func() time.Time {
			nowMu.Lock()
			defer nowMu.Unlock()
			return now
		}
		advance := func(d time.Duration) {
			nowMu.Lock()
			now = now.Add(d)
			nowMu.Unlock()
		}

		fs := newRawFS(client, Config{
			DiskID:  "disk-step18-backoff",
			RootIno: 1,
			Mode:    MountModeSync,
			Logger:  logger,
		}, getNow)
		fs.releaseMu.Lock()
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		close(fs.releaseLoopDone)
		fs.releaseMu.Unlock()
		t.Cleanup(fs.OnUnmount)

		fs.markOrphanWithContext(context.Background(), 501)
		require.Equal(t, int64(1), client.calls.Load())
		require.Equal(t, 1, fs.releaseQueueDepth())

		advance(500 * time.Millisecond)
		fs.drainReleaseQueueOnce(context.Background(), false)
		require.Equal(t, int64(1), client.calls.Load())

		advance(500 * time.Millisecond)
		fs.drainReleaseQueueOnce(context.Background(), false)
		require.Equal(t, int64(2), client.calls.Load())

		advance(1500 * time.Millisecond)
		fs.drainReleaseQueueOnce(context.Background(), false)
		require.Equal(t, int64(2), client.calls.Load())

		advance(500 * time.Millisecond)
		fs.drainReleaseQueueOnce(context.Background(), false)
		require.Equal(t, int64(3), client.calls.Load())

		for i := 3; i < DefaultReleaseMaxAttempts; i++ {
			advance(30 * time.Second)
			fs.drainReleaseQueueOnce(context.Background(), false)
		}
		require.Equal(t, int64(DefaultReleaseMaxAttempts), client.calls.Load())
		require.Equal(t, 0, fs.releaseQueueDepth())
		require.GreaterOrEqual(t, countLogMsg(t, logBuf.String(), "orphan_reclaim_dropped"), 1)
	})

	t.Run("StopAndDrainReleaseQueueOnUnmountAttemptsBackedOffItemsImmediately", func(t *testing.T) {
		var allow atomic.Bool
		client := &reclaimStubClient{
			hook: func(context.Context, *storagev1.ReclaimOrphanRequest) error {
				if !allow.Load() {
					return connect.NewError(connect.CodeUnavailable, errors.New("transient"))
				}
				return nil
			},
		}
		now := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
		fs := newRawFS(client, Config{
			DiskID:  "disk-step18-unmount",
			RootIno: 1,
			Mode:    MountModeSync,
		}, func() time.Time { return now })
		fs.releaseMu.Lock()
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		close(fs.releaseLoopDone)
		fs.releaseMu.Unlock()

		fs.markOrphanWithContext(context.Background(), 601)
		require.Equal(t, int64(1), client.calls.Load())
		require.Equal(t, 1, fs.releaseQueueDepth())

		allow.Store(true)
		fs.OnUnmount()
		require.Equal(t, int64(2), client.calls.Load())
		require.Equal(t, 0, fs.releaseQueueDepth())
	})
}
