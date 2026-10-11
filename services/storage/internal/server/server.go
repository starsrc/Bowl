package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"

	"github.com/starsrc/bowl/gen/go/bowl/v1/bowlv1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
)

// Server wraps the net/http HTTP/2 server serving Connect-RPC for StorageService.
type Server struct {
	httpServer      *http.Server
	shutdownTimeout time.Duration
}

// New constructs a new Server instance.
func New(cfg config.ServerConfig, storageService bowlv1connect.StorageServiceHandler) *Server {
	connectServer := connect.NewServer()
	bowlv1connect.RegisterStorageServiceHandler(connectServer, storageService)

	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{
		Addr:              cfg.Addr(),
		Handler:           mux,
		Protocols:         &protocols,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout(),
	}

	return &Server{
		httpServer:      httpServer,
		shutdownTimeout: cfg.ShutdownTimeout(),
	}
}

// Start begins serving requests on the configured address.
func (s *Server) Start() error {
	err := s.httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("server listen and serve: %w", err)
	}
	return nil
}

// Stop gracefully stops the HTTP server within the configured shutdown timeout.
func (s *Server) Stop(ctx context.Context) error {
	shutdownCtx := ctx
	if s.shutdownTimeout > 0 {
		var cancel context.CancelFunc
		shutdownCtx, cancel = context.WithTimeout(ctx, s.shutdownTimeout)
		defer cancel()
	}
	return s.httpServer.Shutdown(shutdownCtx)
}
