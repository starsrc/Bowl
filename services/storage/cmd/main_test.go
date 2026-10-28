package main

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	mockdb "github.com/starsrc/bowl/services/storage/internal/db/mock"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func TestRun_MissingDSN_Fails(t *testing.T) {
	t.Setenv("DB_DSN", "")
	t.Setenv("SERVER_PORT", "9000")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "load config")
}

func TestRun_InvalidServerPort_Fails(t *testing.T) {
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", "99999")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := run(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate server config")
}

func TestRun_Success_GracefulShutdown(t *testing.T) {
	port := getFreePort(t)
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", strconv.Itoa(port))
	t.Setenv("SERVER_STOP_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "error")

	mockStore := mockdb.NewMockStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	stopped := make(chan struct{})
	errCh := make(chan error, 1)

	opts := []fx.Option{
		fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error {
					close(started)
					return nil
				},
				OnStop: func(context.Context) error {
					close(stopped)
					return nil
				},
			})
		}),
	}

	go func() {
		errCh <- run(ctx, opts...)
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for application to start")
	}

	// 触发上下文取消以触发优雅停机
	cancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
		select {
		case <-stopped:
		default:
			t.Fatal("expected OnStop hook to have been executed")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for application to stop gracefully")
	}
}

func TestRun_GracefulShutdown_StopError(t *testing.T) {
	port := getFreePort(t)
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", strconv.Itoa(port))
	t.Setenv("SERVER_STOP_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "error")

	mockStore := mockdb.NewMockStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	errCh := make(chan error, 1)

	stopErr := errors.New("hook stop failed")
	opts := []fx.Option{
		fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))),
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(context.Context) error {
					close(started)
					return nil
				},
				OnStop: func(context.Context) error {
					return stopErr
				},
			})
		}),
	}

	go func() {
		errCh <- run(ctx, opts...)
	}()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for application to start")
	}

	// 触发上下文取消以触发优雅停机
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "server graceful shutdown error")
		assert.Contains(t, err.Error(), "hook stop failed")
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for application to stop")
	}
}

func TestRun_ShutdownSignal_PropagatesExitCode(t *testing.T) {
	port := getFreePort(t)
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", strconv.Itoa(port))
	t.Setenv("LOG_LEVEL", "error")

	mockStore := mockdb.NewMockStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	opts := []fx.Option{
		fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))),
		fx.Invoke(func(shutdowner fx.Shutdowner) {
			go func() {
				time.Sleep(50 * time.Millisecond)
				_ = shutdowner.Shutdown(fx.ExitCode(7))
			}()
		}),
	}

	err := run(ctx, opts...)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "service stopped with exit code 7")
}

func TestRun_StartFailure_PortConflict(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	tcpAddr := ln.Addr().(*net.TCPAddr)

	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_HOST", "127.0.0.1")
	t.Setenv("SERVER_PORT", strconv.Itoa(tcpAddr.Port))
	t.Setenv("LOG_LEVEL", "error")

	mockStore := mockdb.NewMockStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = run(ctx, fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "address already in use")
}

func TestRun_AutoGCWorker_TriggersOnIntervalAndDrainsGracefully(t *testing.T) {
	port := getFreePort(t)
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", strconv.Itoa(port))
	t.Setenv("SERVER_STOP_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("STORAGE_GC_INTERVAL", "20ms")

	mockStore := mockdb.NewMockStore(t)
	mockQuerier := mockdb.NewMockQuerier(t)

	gcCalled := make(chan struct{}, 10)
	var gcCount atomic.Int32

	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Maybe()

	mockQuerier.EXPECT().
		ListGarbageSliceRefs(mock.Anything, int32(100)).
		RunAndReturn(func(ctx context.Context, limit int32) ([]db.SliceRef, error) {
			gcCount.Add(1)
			select {
			case gcCalled <- struct{}{}:
			default:
			}
			return []db.SliceRef{}, nil
		}).
		Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := []fx.Option{
		fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, opts...)
	}()

	// 1. 等待短周期触发后台 CollectGarbage
	select {
	case <-gcCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for auto GC worker to trigger")
	}

	// 2. 取消上下文触发优雅停机
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for application to stop gracefully")
	}

	assert.GreaterOrEqual(t, gcCount.Load(), int32(1))
}

func TestRun_AutoGCWorker_WaitsForInFlightGCToCompleteOnStop(t *testing.T) {
	port := getFreePort(t)
	t.Setenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable")
	t.Setenv("SERVER_PORT", strconv.Itoa(port))
	t.Setenv("SERVER_STOP_TIMEOUT", "3s")
	t.Setenv("LOG_LEVEL", "error")
	t.Setenv("STORAGE_GC_INTERVAL", "20ms")

	mockStore := mockdb.NewMockStore(t)
	mockQuerier := mockdb.NewMockQuerier(t)

	inFlightStarted := make(chan struct{})
	inFlightDone := make(chan struct{})

	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			return fn(mockQuerier)
		}).
		Maybe()

	var once sync.Once
	mockQuerier.EXPECT().
		ListGarbageSliceRefs(mock.Anything, int32(100)).
		RunAndReturn(func(ctx context.Context, limit int32) ([]db.SliceRef, error) {
			once.Do(func() {
				close(inFlightStarted)
				time.Sleep(80 * time.Millisecond)
				close(inFlightDone)
			})
			return []db.SliceRef{}, nil
		}).
		Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := []fx.Option{
		fx.Replace(fx.Annotate(mockStore, fx.As(new(db.Store)))),
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, opts...)
	}()

	select {
	case <-inFlightStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for in-flight GC to start")
	}

	// 触发停机
	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err)
		// 验证停机完成时，在途的 GC 轮次必须已经完成排空
		select {
		case <-inFlightDone:
		default:
			t.Fatal("expected in-flight GC to be fully drained before application stop completes")
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for application shutdown")
	}
}
