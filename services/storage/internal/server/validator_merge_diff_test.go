package server

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	testValidationSourceDiskID = "11111111-1111-7111-8111-111111111111"
	testValidationTargetDiskID = "22222222-2222-7222-8222-222222222222"
	testValidationCommitID     = "33333333-3333-7333-8333-333333333333"
	testValidationNilUUID      = "00000000-0000-0000-0000-000000000000"
)

func runValidationIntercept(t *testing.T, msg any) (bool, error) {
	t.Helper()
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化 StorageValidator 失败")

	called := false
	chain := NewValidationInterceptor(v)(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		return connect.NewResponse(&storagev1.DiffDiskResponse{}), nil
	})

	var req connect.AnyRequest
	switch m := msg.(type) {
	case *storagev1.DiffDiskRequest:
		req = connect.NewRequest(m)
	case *storagev1.ReadCommitNodeRequest:
		req = connect.NewRequest(m)
	case *storagev1.MergeDiskRequest:
		req = connect.NewRequest(m)
	case *storagev1.MountDiskRequest:
		req = connect.NewRequest(m)
	case *storagev1.RenewMountRequest:
		req = connect.NewRequest(m)
	case *storagev1.UnmountDiskRequest:
		req = connect.NewRequest(m)
	default:
		t.Fatalf("不支持的校验测试消息类型: %T", msg)
	}

	_, callErr := chain(context.Background(), req)
	return called, callErr
}

func assertValidationRejected(t *testing.T, msg any) {
	t.Helper()
	called, err := runValidationIntercept(t, msg)
	require.False(t, called, "非法请求必须在校验拦截器短路返回")
	requireConnectErrorCodeAndReason(t, err, connect.CodeInvalidArgument, validator.ReasonInvalidArgument)
}

func assertValidationAccepted(t *testing.T, msg any) {
	t.Helper()
	called, err := runValidationIntercept(t, msg)
	require.NoError(t, err, "合法边界请求不应被校验拦截器拒绝")
	require.True(t, called, "合法边界请求必须放行至下游 Handler")
}

func TestValidation_DiffDiskRequest(t *testing.T) {
	sameDiskID := testValidationSourceDiskID
	otherDiskID := testValidationTargetDiskID
	badUUID := "not-a-uuid"
	nilUUID := testValidationNilUUID

	invalidCases := []struct {
		name string
		req  *storagev1.DiffDiskRequest
	}{
		{
			name: "非法_source_disk_id",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: badUUID},
		},
		{
			name: "全零_nil_source_disk_id",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: nilUUID},
		},
		{
			name: "非法_target_disk_id",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: &badUUID},
		},
		{
			name: "全零_nil_target_disk_id",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: &nilUUID},
		},
		{
			name: "source_disk_id_等于_target_disk_id",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: &sameDiskID},
		},
		{
			name: "path_prefix_包含_NUL_字节",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, PathPrefix: []byte("dir/\x00sub")},
		},
		{
			name: "path_prefix_超过_4096_字节",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, PathPrefix: bytes.Repeat([]byte("a"), 4097)},
		},
		{
			name: "page_size_超过_1000",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, PageSize: 1001},
		},
		{
			name: "page_token_包含_NUL_字节",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, PageToken: "tok\x00en"},
		},
		{
			name: "page_token_超过_4096_字符",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID, PageToken: strings.Repeat("t", 4097)},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.DiffDiskRequest
	}{
		{
			name: "省略_target_disk_id_最小合法请求",
			req:  &storagev1.DiffDiskRequest{SourceDiskId: testValidationSourceDiskID},
		},
		{
			name: "最大边界合法请求",
			req: &storagev1.DiffDiskRequest{
				SourceDiskId:  testValidationSourceDiskID,
				TargetDiskId:  &otherDiskID,
				PathPrefix:    bytes.Repeat([]byte("p"), 4096),
				ConflictsOnly: true,
				PageSize:      1000,
				PageToken:     strings.Repeat("k", 4096),
			},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}

func TestValidation_ReadCommitNodeRequest(t *testing.T) {
	invalidCases := []struct {
		name string
		req  *storagev1.ReadCommitNodeRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: "bad-uuid", CommitId: testValidationCommitID, Ino: 1},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationNilUUID, CommitId: testValidationCommitID, Ino: 1},
		},
		{
			name: "非法_commit_id",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: "bad-uuid", Ino: 1},
		},
		{
			name: "全零_nil_commit_id",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: testValidationNilUUID, Ino: 1},
		},
		{
			name: "ino_为零",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: testValidationCommitID, Ino: 0},
		},
		{
			name: "ino_溢出_MaxInt64",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: testValidationCommitID, Ino: uint64(math.MaxInt64) + 1},
		},
		{
			name: "offset_溢出_MaxInt64",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: testValidationCommitID, Ino: 1, Offset: uint64(math.MaxInt64) + 1},
		},
		{
			name: "size_超过_4MiB",
			req:  &storagev1.ReadCommitNodeRequest{DiskId: testValidationSourceDiskID, CommitId: testValidationCommitID, Ino: 1, Size: (4 << 20) + 1},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.ReadCommitNodeRequest
	}{
		{
			name: "最小合法边界",
			req: &storagev1.ReadCommitNodeRequest{
				DiskId:   testValidationSourceDiskID,
				CommitId: testValidationCommitID,
				Ino:      1,
				Offset:   0,
				Size:     0,
			},
		},
		{
			name: "最大合法边界",
			req: &storagev1.ReadCommitNodeRequest{
				DiskId:   testValidationSourceDiskID,
				CommitId: testValidationCommitID,
				Ino:      math.MaxInt64,
				Offset:   math.MaxInt64,
				Size:     4 << 20,
			},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}

func TestValidation_MergeDiskRequest(t *testing.T) {
	zeroMaxBytes := uint64(0)
	overflowMaxBytes := uint64(math.MaxInt64) + 1
	minMaxBytes := uint64(1)
	maxMaxBytes := uint64(math.MaxInt64)

	tooManyStrategies := make(map[string]storagev1.MergeStrategy, 4097)
	for i := range 4097 {
		tooManyStrategies[fmt.Sprintf("path/%d", i)] = storagev1.MergeStrategy_MERGE_STRATEGY_OURS
	}

	invalidCases := []struct {
		name string
		req  *storagev1.MergeDiskRequest
	}{
		{
			name: "非法_source_disk_id",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: "bad", TargetDiskId: testValidationTargetDiskID},
		},
		{
			name: "全零_nil_source_disk_id",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationNilUUID, TargetDiskId: testValidationTargetDiskID},
		},
		{
			name: "非法_target_disk_id",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: "bad"},
		},
		{
			name: "全零_nil_target_disk_id",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: testValidationNilUUID},
		},
		{
			name: "source_disk_id_等于_target_disk_id",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: testValidationSourceDiskID},
		},
		{
			name: "未定义_strategy_枚举",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: testValidationTargetDiskID, Strategy: storagev1.MergeStrategy(99)},
		},
		{
			name: "path_strategies_包含空键",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: map[string]storagev1.MergeStrategy{"": storagev1.MergeStrategy_MERGE_STRATEGY_OURS},
			},
		},
		{
			name: "path_strategies_键超过_4096_字符",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: map[string]storagev1.MergeStrategy{strings.Repeat("x", 4097): storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS},
			},
		},
		{
			name: "path_strategies_键包含_NUL_字节",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: map[string]storagev1.MergeStrategy{"a\x00b": storagev1.MergeStrategy_MERGE_STRATEGY_FAIL},
			},
		},
		{
			name: "path_strategies_值为_UNSPECIFIED",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: map[string]storagev1.MergeStrategy{"dir/file": storagev1.MergeStrategy_MERGE_STRATEGY_UNSPECIFIED},
			},
		},
		{
			name: "path_strategies_值为未定义枚举",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: map[string]storagev1.MergeStrategy{"dir/file": storagev1.MergeStrategy(42)},
			},
		},
		{
			name: "path_strategies_条目超过_4096",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId:   testValidationSourceDiskID,
				TargetDiskId:   testValidationTargetDiskID,
				PathStrategies: tooManyStrategies,
			},
		},
		{
			name: "max_bytes_为零",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: testValidationTargetDiskID, MaxBytes: &zeroMaxBytes},
		},
		{
			name: "max_bytes_溢出_MaxInt64",
			req:  &storagev1.MergeDiskRequest{SourceDiskId: testValidationSourceDiskID, TargetDiskId: testValidationTargetDiskID, MaxBytes: &overflowMaxBytes},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.MergeDiskRequest
	}{
		{
			name: "默认策略_UNSPECIFIED_且无可选字段",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId: testValidationSourceDiskID,
				TargetDiskId: testValidationTargetDiskID,
			},
		},
		{
			name: "显式策略_THEIRS_带边界_path_strategies_与最小_max_bytes",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId: testValidationSourceDiskID,
				TargetDiskId: testValidationTargetDiskID,
				Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_THEIRS,
				PathStrategies: map[string]storagev1.MergeStrategy{
					"a":                       storagev1.MergeStrategy_MERGE_STRATEGY_FAIL,
					strings.Repeat("k", 4096): storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
				},
				DryRun:   true,
				MaxBytes: &minMaxBytes,
			},
		},
		{
			name: "最大合法_max_bytes_与_OURS_策略",
			req: &storagev1.MergeDiskRequest{
				SourceDiskId: testValidationSourceDiskID,
				TargetDiskId: testValidationTargetDiskID,
				Strategy:     storagev1.MergeStrategy_MERGE_STRATEGY_OURS,
				MaxBytes:     &maxMaxBytes,
			},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}

func TestValidator_MountDiskRequest(t *testing.T) {
	t.Run("reject/read_only_与_force_同时为_true", func(t *testing.T) {
		assertValidationRejected(t, &storagev1.MountDiskRequest{
			DiskId:   testValidationTargetDiskID,
			ReadOnly: true,
			Force:    true,
		})
	})

	t.Run("accept/只读挂载_force_为_false", func(t *testing.T) {
		assertValidationAccepted(t, &storagev1.MountDiskRequest{
			DiskId:   testValidationTargetDiskID,
			ReadOnly: true,
			Force:    false,
		})
	})

	t.Run("accept/可写强制抢占挂载", func(t *testing.T) {
		assertValidationAccepted(t, &storagev1.MountDiskRequest{
			DiskId:   testValidationTargetDiskID,
			ReadOnly: false,
			Force:    true,
		})
	})
}

func TestValidator_RenewMountRequest(t *testing.T) {
	invalidCases := []struct {
		name string
		req  *storagev1.RenewMountRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.RenewMountRequest{DiskId: "bad-uuid", Epoch: 1},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationNilUUID, Epoch: 1},
		},
		{
			name: "epoch_为_0",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, Epoch: 0},
		},
		{
			name: "epoch_溢出_MaxInt64",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, Epoch: uint64(math.MaxInt64) + 1},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.RenewMountRequest
	}{
		{
			name: "最小合法_epoch",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, Epoch: 1},
		},
		{
			name: "最大合法_epoch",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, Epoch: math.MaxInt64},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}

func TestValidator_UnmountDiskRequest(t *testing.T) {
	invalidCases := []struct {
		name string
		req  *storagev1.UnmountDiskRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: "bad-uuid", Epoch: 1},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationNilUUID, Epoch: 1},
		},
		{
			name: "epoch_为_0",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, Epoch: 0},
		},
		{
			name: "epoch_溢出_MaxInt64",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, Epoch: uint64(math.MaxInt64) + 1},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.UnmountDiskRequest
	}{
		{
			name: "最小合法_epoch",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, Epoch: 1},
		},
		{
			name: "最大合法_epoch",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, Epoch: math.MaxInt64},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}
