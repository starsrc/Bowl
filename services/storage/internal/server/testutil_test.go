package server

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/engine"
	"github.com/starsrc/bowl/services/storage/internal/projection"
)

type testEnv struct {
	server *Server
	mgr    *projection.ControlManager
	store  *projection.MemStore
	client storagev1connect.StorageServiceClient
}

func newTestEnv(t *testing.T, metaCfg *config.MetaConfig) *testEnv {
	t.Helper()
	mem := projection.NewMemStore()
	mgr, err := projection.NewControlManager(projection.ManagerOptions{
		StorageRoot:             t.TempDir(),
		Store:                   mem,
		MountLeaseTTL:           30 * time.Second,
		DisableStartupReconcile: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close(context.Background()))
	})

	srv, err := New(Params{
		Config:  config.ServerConfig{},
		MetaCfg: metaCfg,
		Manager: mgr,
	})
	require.NoError(t, err)

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
	return &testEnv{
		server: srv,
		mgr:    mgr,
		store:  mem,
		client: client,
	}
}

func (e *testEnv) createDisk(t *testing.T, name string, maxBytes uint64) *storagev1.Disk {
	t.Helper()
	resp, err := e.client.CreateDisk(context.Background(), connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     name,
		MaxBytes: maxBytes,
	}))
	require.NoError(t, err)
	return resp.Msg.GetDisk()
}

func (e *testEnv) mountBranchRW(t *testing.T, diskID, branch string, force bool) *storagev1.MountBranchResponse {
	t.Helper()
	resp, err := e.client.MountBranch(context.Background(), connect.NewRequest(&storagev1.MountBranchRequest{
		DiskId: diskID,
		Branch: branch,
		Force:  force,
	}))
	require.NoError(t, err)
	return resp.Msg
}

func requireConnectErr(t *testing.T, err error, wantCode connect.Code, wantReason string) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, wantCode, connect.CodeOf(err), "unexpected connect code for err=%v", err)
	if wantReason != "" {
		info := ExtractErrorInfo(err)
		require.NotNil(t, info, "missing ErrorInfo detail for err=%v", err)
		require.Equal(t, wantReason, info.GetReason())
		require.Equal(t, ErrorDomain, info.GetDomain())
	}
}

func defaultBranch() string {
	return engine.DefaultBranchName
}
