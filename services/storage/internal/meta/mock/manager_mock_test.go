package mock_test

import (
	"context"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/caarlos0/env/v11"
	"github.com/exaring/otelpgx"
	"github.com/joho/godotenv"
	_ "github.com/starsrc/Aether/pkg/observability/slogx"
	_ "github.com/starsrc/Aether/pkg/observability/telemetry"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testStorageService 验证 ConnectRPC v1 一元签名与 meta.Manager 协同工作。
type testStorageService struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mgr meta.Manager
}

// 静态断言确保 testStorageService 实现了 ConnectRPC v1 StorageServiceHandler 接口。
var _ storagev1connect.StorageServiceHandler = (*testStorageService)(nil)

func (s *testStorageService) Lookup(
	ctx context.Context,
	req *connect.Request[storagev1.LookupRequest],
) (*connect.Response[storagev1.LookupResponse], error) {
	node, err := s.mgr.Lookup(ctx, req.Msg.GetParentIno(), req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode: &storagev1.Inode{
			Ino:        node.Ino,
			Generation: node.Generation,
		},
		EntryTimeoutSeconds: node.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  node.AttrTimeoutSeconds,
	}), nil
}

// TestMockManagerAndConnectRPCv1Signature 验证 meta.Manager Mock 的正确实例化及 ConnectRPC v1 一元接口签名调用。
func TestMockManagerAndConnectRPCv1Signature(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	require.NotNil(t, mockMgr)

	ctx := context.Background()
	expectedIno := uint64(10086)

	mockMgr.EXPECT().Lookup(mock.Anything, uint64(1), []byte("hello.txt")).
		Return(&meta.Inode{
			Ino:                 expectedIno,
			Generation:          1,
			EntryTimeoutSeconds: 1.0,
			AttrTimeoutSeconds:  1.0,
		}, nil).Once()

	svc := &testStorageService{mgr: mockMgr}
	req := connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: 1,
		Name:      []byte("hello.txt"),
	})

	resp, err := svc.Lookup(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, expectedIno, resp.Msg.GetInode().GetIno())
}

// TestStorageModuleDependencies 验证 storage 子模块引入的关键基础设施与协议依赖可正常初始化。
func TestStorageModuleDependencies(t *testing.T) {
	// 验证 connectrpc.com/otelconnect
	otelInterceptor, err := otelconnect.NewInterceptor()
	require.NoError(t, err)
	assert.NotNil(t, otelInterceptor)

	// 验证 github.com/exaring/otelpgx
	pgxTracer := otelpgx.NewTracer()
	assert.NotNil(t, pgxTracer)

	// 验证 github.com/caarlos0/env/v11
	type dummyConfig struct {
		Port int `env:"TEST_PORT" envDefault:"9000"`
	}
	var cfg dummyConfig
	require.NoError(t, env.Parse(&cfg))
	assert.Equal(t, 9000, cfg.Port)

	// 验证 github.com/joho/godotenv
	parsedEnv, err := godotenv.Unmarshal("KEY=VALUE")
	require.NoError(t, err)
	assert.Equal(t, "VALUE", parsedEnv["KEY"])

	// 验证 connectrpc.com/connect 构造与错误封装
	connErr := connect.NewError(connect.CodeNotFound, assert.AnError)
	assert.Equal(t, connect.CodeNotFound, connErr.Code())
	assert.True(t, strings.Contains(connErr.Error(), assert.AnError.Error()))
}

// TestMockManager_GetParentsContract 验证 MockManager 对 GetParents 的实质打桩与返回值契约。
func TestMockManager_GetParentsContract(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	require.NotNil(t, mockMgr)

	ctx := context.Background()
	targetIno := uint64(2001)
	expectedParents := map[uint64]uint32{
		1:    1,
		1001: 2,
	}

	// 1. 正向打桩与契约断言
	mockMgr.EXPECT().GetParents(mock.Anything, targetIno).
		Return(expectedParents, nil).Once()

	parents, err := mockMgr.GetParents(ctx, targetIno)
	require.NoError(t, err)
	assert.Equal(t, expectedParents, parents)

	// 2. 负向打桩与契约断言 (ErrNotFound)
	mockMgr.EXPECT().GetParents(mock.Anything, uint64(9999)).
		Return(nil, meta.ErrNotFound).Once()

	errParents, err := mockMgr.GetParents(ctx, uint64(9999))
	require.ErrorIs(t, err, meta.ErrNotFound)
	assert.Nil(t, errParents)
}
