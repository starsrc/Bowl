package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/observability"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

func reserveFreePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := addr.Port
	require.NoError(t, ln.Close())
	return port
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunManagedServers_GivenSecondPortConflict_WhenPreBinding_ThenClosesFirstListenerRunsReverseCleanupWithoutServing(t *testing.T) {
	occupiedAdmin, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupiedAdmin.Close() })

	pubPort := reserveFreePort(t)
	pubAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(pubPort))

	var served atomic.Int32
	pubSrv := &http.Server{
		Addr: pubAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			served.Add(1)
			w.WriteHeader(http.StatusOK)
		}),
		ReadHeaderTimeout: time.Second,
	}
	adminSrv := &http.Server{
		Addr:              occupiedAdmin.Addr().String(),
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: time.Second,
	}

	var (
		orderMu sync.Mutex
		order   []string
	)
	recordHook := func(name string) cleanupHook {
		return cleanupHook{
			name: name,
			fn: func(context.Context) error {
				orderMu.Lock()
				order = append(order, name)
				orderMu.Unlock()
				return nil
			},
		}
	}

	cleanups := []cleanupHook{
		recordHook("observability"),
		recordHook("meter provider"),
		recordHook("tracer provider"),
		recordHook("node store"),
	}

	runErr := runManagedServers(
		context.Background(),
		time.Second,
		2*time.Second,
		[]namedServer{
			{name: "public", server: pubSrv},
			{name: "admin", server: adminSrv},
		},
		cleanups,
		discardLogger(),
	)
	require.Error(t, runErr)
	require.Contains(t, runErr.Error(), "bind admin listener")
	require.Zero(t, served.Load(), "public server must never serve traffic when second listener fails to bind")

	rebound, err := net.Listen("tcp", pubAddr)
	require.NoError(t, err, "first listener must be closed immediately when second bind fails")
	require.NoError(t, rebound.Close())

	require.Equal(t, []string{
		"node store",
		"tracer provider",
		"meter provider",
		"observability",
	}, order)
}

func TestServeBoundServers_GivenUnexpectedMidRunListenerFailure_WhenServing_ThenShutsDownPeerAndReturnsError(t *testing.T) {
	pubLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	pubSrv := &http.Server{Handler: okHandler, ReadHeaderTimeout: time.Second}
	adminSrv := &http.Server{Handler: okHandler, ReadHeaderTimeout: time.Second}

	bound := []boundServer{
		{name: "public", server: pubSrv, listener: pubLn},
		{name: "admin", server: adminSrv, listener: adminLn},
	}

	errCh := make(chan error, 1)
	go func() {
		serveErr, shutdownErr := serveBoundServers(
			context.Background(),
			func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 2*time.Second)
			},
			time.Second,
			bound,
			discardLogger(),
		)
		errCh <- errors.Join(serveErr, shutdownErr)
	}()

	httpClient := &http.Client{Timeout: time.Second}
	for _, rawURL := range []string{"http://" + pubLn.Addr().String(), "http://" + adminLn.Addr().String()} {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, http.NoBody)
		require.NoError(t, reqErr)
		resp, doErr := httpClient.Do(req)
		require.NoError(t, doErr)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.NoError(t, resp.Body.Close())
	}

	require.NoError(t, adminLn.Close())

	select {
	case gotErr := <-errCh:
		require.Error(t, gotErr)
		require.Contains(t, gotErr.Error(), "admin serve")
	case <-time.After(5 * time.Second):
		t.Fatal("serveBoundServers did not exit promptly after unexpected mid-run listener failure")
	}

	reboundPub, err := net.Listen("tcp", pubLn.Addr().String())
	require.NoError(t, err, "peer public listener must be shut down after admin listener failure")
	require.NoError(t, reboundPub.Close())
}

func TestRunManagedServers_GivenContextCancel_WhenServingBothPorts_ThenGracefullyStopsInReverseOrder(t *testing.T) {
	pubPort := reserveFreePort(t)
	adminPort := reserveFreePort(t)
	pubAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(pubPort))
	adminAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(adminPort))

	okHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	pubSrv := &http.Server{Addr: pubAddr, Handler: okHandler, ReadHeaderTimeout: time.Second}
	adminSrv := &http.Server{Addr: adminAddr, Handler: okHandler, ReadHeaderTimeout: time.Second}

	var (
		orderMu sync.Mutex
		order   []string
	)
	recordHook := func(name string) cleanupHook {
		return cleanupHook{
			name: name,
			fn: func(context.Context) error {
				orderMu.Lock()
				order = append(order, name)
				orderMu.Unlock()
				return nil
			},
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runManagedServers(
			ctx,
			time.Second,
			2*time.Second,
			[]namedServer{
				{name: "public", server: pubSrv},
				{name: "admin", server: adminSrv},
			},
			[]cleanupHook{
				recordHook("observability"),
				recordHook("meter provider"),
				recordHook("tracer provider"),
				recordHook("node store"),
			},
			discardLogger(),
		)
	}()

	httpClient := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+pubAddr, http.NoBody)
		if reqErr != nil {
			return false
		}
		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			return false
		}
		_ = resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 2*time.Second, 10*time.Millisecond)

	cancel()

	select {
	case gotErr := <-errCh:
		require.NoError(t, gotErr)
	case <-time.After(5 * time.Second):
		t.Fatal("runManagedServers did not stop gracefully after context cancellation")
	}

	require.Equal(t, []string{
		"node store",
		"tracer provider",
		"meter provider",
		"observability",
	}, order)
}

func TestRunWithConfig_GivenUnwritableDataDir_WhenBootstrapping_ThenFailsFast(t *testing.T) {
	pubPort := reserveFreePort(t)
	adminPort := reserveFreePort(t)

	blockingFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(blockingFile, []byte("x"), 0o600))

	obsCfg := observability.DefaultConfig()
	obsCfg.AdminAddr = "off"
	obsCfg.MetricsEnabled = false
	obsCfg.OtelEnabled = false

	cfg := &config.Config{
		ServerConfig: config.ServerConfig{
			ServerHost:              "127.0.0.1",
			ServerPort:              pubPort,
			InternalServerHost:      "127.0.0.1",
			InternalServerPort:      adminPort,
			ServerReadHeaderTimeout: time.Second,
			ServerStopTimeout:       2 * time.Second,
			ServerReadMaxBytes:      config.DefaultServerReadMaxBytes,
		},
		EnvConfig: obsCfg,
		Meta: config.MetaConfig{
			AtimeModeVal:              config.DefaultAtimeMode,
			EntryTimeoutSecondsVal:    config.DefaultEntryTimeoutSeconds,
			AttrTimeoutSecondsVal:     config.DefaultAttrTimeoutSeconds,
			DirEntryTimeoutSecondsVal: config.DefaultDirEntryTimeoutSeconds,
			ActorIdleTTLVal:           config.DefaultActorIdleTTL,
			GCIntervalVal:             config.DefaultGCInterval,
			StorageDataDirVal:         blockingFile,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := runWithConfig(ctx, cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "open node store")
}

func TestRunWithConfig_GivenValidConfig_WhenRunning_ThenServesReadyzAndStopsCleanly(t *testing.T) {
	pubPort := reserveFreePort(t)
	adminPort := reserveFreePort(t)
	adminAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(adminPort))

	obsCfg := observability.DefaultConfig()
	obsCfg.AdminAddr = "off"
	obsCfg.MetricsEnabled = true
	obsCfg.OtelEnabled = false

	cfg := &config.Config{
		ServerConfig: config.ServerConfig{
			ServerHost:              "127.0.0.1",
			ServerPort:              pubPort,
			InternalServerHost:      "127.0.0.1",
			InternalServerPort:      adminPort,
			ServerReadHeaderTimeout: time.Second,
			ServerStopTimeout:       2 * time.Second,
			ServerReadMaxBytes:      config.DefaultServerReadMaxBytes,
		},
		EnvConfig: obsCfg,
		Meta: config.MetaConfig{
			AtimeModeVal:              config.DefaultAtimeMode,
			EntryTimeoutSecondsVal:    config.DefaultEntryTimeoutSeconds,
			AttrTimeoutSecondsVal:     config.DefaultAttrTimeoutSeconds,
			DirEntryTimeoutSecondsVal: config.DefaultDirEntryTimeoutSeconds,
			ActorIdleTTLVal:           config.DefaultActorIdleTTL,
			GCIntervalVal:             0,
			StorageDataDirVal:         t.TempDir(),
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- runWithConfig(ctx, cfg)
	}()

	httpClient := &http.Client{Timeout: time.Second}
	var readyBody string
	require.Eventually(t, func() bool {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+adminAddr+"/readyz", http.NoBody)
		if reqErr != nil {
			return false
		}
		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			return false
		}
		bodyBytes, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil || resp.StatusCode != http.StatusOK {
			return false
		}
		readyBody = string(bodyBytes)
		return true
	}, 3*time.Second, 15*time.Millisecond)
	require.Contains(t, readyBody, `"storage":"ok"`)

	cancel()

	select {
	case gotErr := <-errCh:
		require.NoError(t, gotErr)
	case <-time.After(5 * time.Second):
		t.Fatal("runWithConfig did not stop gracefully after context cancellation")
	}
}
