package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bowlv1connect "github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
)

func TestServer_StartAndStop(t *testing.T) {
	cfg := config.ServerConfig{
		Host:                 "127.0.0.1",
		Port:                 0, // Pick free port
		ReadHeaderTimeoutVal: 5 * time.Second,
		ShutdownTimeoutVal:   5 * time.Second,
	}

	mockHandler := &bowlv1connect.UnimplementedStorageServiceHandler{}
	srv := New(cfg, mockHandler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// Wait for server to accept or shut down
	time.Sleep(50 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	err := srv.Stop(shutdownCtx)
	assert.NoError(t, err)

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop in time")
	}
	_ = ctx
}

func TestServer_ConnectRPCRoutesMounted(t *testing.T) {
	cfg := config.ServerConfig{
		Host: "127.0.0.1",
		Port: 8089,
	}

	srv := New(cfg, &bowlv1connect.UnimplementedStorageServiceHandler{})
	require.NotNil(t, srv.httpServer.Handler)

	// Check that Mount procedure route returns Unimplemented (404 would mean not mounted)
	req := httptest.NewRequest(http.MethodPost, bowlv1connect.StorageServiceMountProcedure, nil)
	req.Header.Set("Content-Type", "application/proto")
	rec := httptest.NewRecorder()
	srv.httpServer.Handler.ServeHTTP(rec, req)

	// Connect-RPC should handle the procedure route
	assert.NotEqual(t, http.StatusNotFound, rec.Code)
}
