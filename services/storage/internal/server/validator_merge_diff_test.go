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
	testValidationSnapshotID   = "33333333-3333-7333-8333-333333333333"
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
	case *storagev1.ReadSnapshotNodeRequest:
		req = connect.NewRequest(m)
	case *storagev1.MergeDiskRequest:
		req = connect.NewRequest(m)
	case *storagev1.RenewMountRequest:
		req = connect.NewRequest(m)
	case *storagev1.UnmountDiskRequest:
		req = connect.NewRequest(m)
	case *storagev1.CreateSnapshotRequest:
		req = connect.NewRequest(m)
	case *storagev1.ListSnapshotsRequest:
		req = connect.NewRequest(m)
	case *storagev1.DeleteSnapshotRequest:
		req = connect.NewRequest(m)
	case *storagev1.PruneSnapshotsRequest:
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

func TestValidation_ReadSnapshotNodeRequest(t *testing.T) {
	invalidCases := []struct {
		name string
		req  *storagev1.ReadSnapshotNodeRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: "bad-uuid", SnapshotId: testValidationSnapshotID, Ino: 1},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationNilUUID, SnapshotId: testValidationSnapshotID, Ino: 1},
		},
		{
			name: "非法_snapshot_id",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: "bad-uuid", Ino: 1},
		},
		{
			name: "全零_nil_snapshot_id",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationNilUUID, Ino: 1},
		},
		{
			name: "ino_为零",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationSnapshotID, Ino: 0},
		},
		{
			name: "ino_溢出_MaxInt64",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationSnapshotID, Ino: uint64(math.MaxInt64) + 1},
		},
		{
			name: "offset_溢出_MaxInt64",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationSnapshotID, Ino: 1, Offset: uint64(math.MaxInt64) + 1},
		},
		{
			name: "size_超过_4MiB",
			req:  &storagev1.ReadSnapshotNodeRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationSnapshotID, Ino: 1, Size: (4 << 20) + 1},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	validCases := []struct {
		name string
		req  *storagev1.ReadSnapshotNodeRequest
	}{
		{
			name: "最小合法边界",
			req: &storagev1.ReadSnapshotNodeRequest{
				DiskId:     testValidationSourceDiskID,
				SnapshotId: testValidationSnapshotID,
				Ino:        1,
				Offset:     0,
				Size:       0,
			},
		},
		{
			name: "最大合法边界",
			req: &storagev1.ReadSnapshotNodeRequest{
				DiskId:     testValidationSourceDiskID,
				SnapshotId: testValidationSnapshotID,
				Ino:        math.MaxInt64,
				Offset:     math.MaxInt64,
				Size:       4 << 20,
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

func TestValidator_RenewMountRequest(t *testing.T) {
	const testMountID = "44444444-4444-7444-8444-444444444444"

	invalidCases := []struct {
		name string
		req  *storagev1.RenewMountRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.RenewMountRequest{DiskId: "bad-uuid", MountId: testMountID, ReadOnly: false, Epoch: 1},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationNilUUID, MountId: testMountID, ReadOnly: false, Epoch: 1},
		},
		{
			name: "非法_mount_id",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: "bad-uuid", ReadOnly: false, Epoch: 1},
		},
		{
			name: "全零_nil_mount_id",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testValidationNilUUID, ReadOnly: false, Epoch: 1},
		},
		{
			name: "read_only_为_true_且_epoch_大于_0",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: true, Epoch: 1},
		},
		{
			name: "read_only_为_false_且_epoch_为_0",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: false, Epoch: 0},
		},
		{
			name: "epoch_溢出_MaxInt64",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: false, Epoch: uint64(math.MaxInt64) + 1},
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
			name: "只读挂载_read_only_为_true_且_epoch_为_0",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: true, Epoch: 0},
		},
		{
			name: "可写挂载_最小合法_epoch",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: false, Epoch: 1},
		},
		{
			name: "可写挂载_最大合法_epoch",
			req:  &storagev1.RenewMountRequest{DiskId: testValidationTargetDiskID, MountId: testMountID, ReadOnly: false, Epoch: math.MaxInt64},
		},
	}

	for _, tc := range validCases {
		t.Run("accept/"+tc.name, func(t *testing.T) {
			assertValidationAccepted(t, tc.req)
		})
	}
}

func TestValidator_UnmountDiskRequest(t *testing.T) {
	const testMountID = "44444444-4444-7444-8444-444444444444"

	invalidCases := []struct {
		name string
		req  *storagev1.UnmountDiskRequest
	}{
		{
			name: "非法_disk_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: "bad-uuid", MountId: testMountID},
		},
		{
			name: "全零_nil_disk_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationNilUUID, MountId: testMountID},
		},
		{
			name: "非法_mount_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, MountId: "bad-uuid"},
		},
		{
			name: "全零_nil_mount_id",
			req:  &storagev1.UnmountDiskRequest{DiskId: testValidationTargetDiskID, MountId: testValidationNilUUID},
		},
	}

	for _, tc := range invalidCases {
		t.Run("reject/"+tc.name, func(t *testing.T) {
			assertValidationRejected(t, tc.req)
		})
	}

	t.Run("accept/合法_disk_id_与_mount_id", func(t *testing.T) {
		assertValidationAccepted(t, &storagev1.UnmountDiskRequest{
			DiskId:  testValidationTargetDiskID,
			MountId: testMountID,
		})
	})
}

func TestValidator_SnapshotRequests(t *testing.T) {
	t.Run("CreateSnapshotRequest", func(t *testing.T) {
		invalidCases := []struct {
			name   string
			req    *storagev1.CreateSnapshotRequest
			reason string
		}{
			{
				name:   "invalid_disk_id",
				req:    &storagev1.CreateSnapshotRequest{DiskId: "bad-uuid"},
				reason: validator.ReasonInvalidArgument,
			},
			{
				name:   "nil_disk_id",
				req:    &storagev1.CreateSnapshotRequest{DiskId: testValidationNilUUID},
				reason: validator.ReasonInvalidArgument,
			},
			{
				name:   "name_exceeds_255_bytes",
				req:    &storagev1.CreateSnapshotRequest{DiskId: testValidationSourceDiskID, Name: strings.Repeat("a", 256)},
				reason: validator.ReasonNameTooLong,
			},
			{
				name:   "invalid_disk_id_overrides_overlong_name",
				req:    &storagev1.CreateSnapshotRequest{DiskId: "bad-uuid", Name: strings.Repeat("a", 256)},
				reason: validator.ReasonInvalidArgument,
			},
			{
				name:   "name_contains_nul",
				req:    &storagev1.CreateSnapshotRequest{DiskId: testValidationSourceDiskID, Name: "snap\x001"},
				reason: validator.ReasonInvalidArgument,
			},
		}
		for _, tc := range invalidCases {
			t.Run("reject/"+tc.name, func(t *testing.T) {
				called, err := runValidationIntercept(t, tc.req)
				require.False(t, called, "非法请求必须在校验拦截器短路返回")
				requireConnectErrorCodeAndReason(t, err, connect.CodeInvalidArgument, tc.reason)
			})
		}

		assertValidationAccepted(t, &storagev1.CreateSnapshotRequest{DiskId: testValidationSourceDiskID})
		assertValidationAccepted(t, &storagev1.CreateSnapshotRequest{DiskId: testValidationSourceDiskID, Name: strings.Repeat("b", 255)})
	})

	t.Run("ListSnapshotsRequest", func(t *testing.T) {
		invalidCases := []struct {
			name string
			req  *storagev1.ListSnapshotsRequest
		}{
			{name: "invalid_disk_id", req: &storagev1.ListSnapshotsRequest{DiskId: "bad-uuid"}},
			{name: "nil_disk_id", req: &storagev1.ListSnapshotsRequest{DiskId: testValidationNilUUID}},
			{name: "undefined_kind_enum", req: &storagev1.ListSnapshotsRequest{DiskId: testValidationSourceDiskID, Kind: storagev1.SnapshotKind(99)}},
			{name: "page_size_exceeds_1000", req: &storagev1.ListSnapshotsRequest{DiskId: testValidationSourceDiskID, PageSize: 1001}},
			{name: "page_token_contains_nul", req: &storagev1.ListSnapshotsRequest{DiskId: testValidationSourceDiskID, PageToken: "tok\x00en"}},
			{name: "page_token_exceeds_4096", req: &storagev1.ListSnapshotsRequest{DiskId: testValidationSourceDiskID, PageToken: strings.Repeat("t", 4097)}},
		}
		for _, tc := range invalidCases {
			t.Run("reject/"+tc.name, func(t *testing.T) {
				assertValidationRejected(t, tc.req)
			})
		}

		assertValidationAccepted(t, &storagev1.ListSnapshotsRequest{DiskId: testValidationSourceDiskID})
		assertValidationAccepted(t, &storagev1.ListSnapshotsRequest{
			DiskId:    testValidationSourceDiskID,
			Kind:      storagev1.SnapshotKind_SNAPSHOT_KIND_MANUAL,
			PageSize:  1000,
			PageToken: strings.Repeat("k", 4096),
		})
	})

	t.Run("DeleteSnapshotRequest", func(t *testing.T) {
		invalidCases := []struct {
			name string
			req  *storagev1.DeleteSnapshotRequest
		}{
			{name: "invalid_disk_id", req: &storagev1.DeleteSnapshotRequest{DiskId: "bad-uuid", SnapshotId: testValidationSnapshotID}},
			{name: "nil_disk_id", req: &storagev1.DeleteSnapshotRequest{DiskId: testValidationNilUUID, SnapshotId: testValidationSnapshotID}},
			{name: "invalid_snapshot_id", req: &storagev1.DeleteSnapshotRequest{DiskId: testValidationSourceDiskID, SnapshotId: "bad-uuid"}},
			{name: "nil_snapshot_id", req: &storagev1.DeleteSnapshotRequest{DiskId: testValidationSourceDiskID, SnapshotId: testValidationNilUUID}},
		}
		for _, tc := range invalidCases {
			t.Run("reject/"+tc.name, func(t *testing.T) {
				assertValidationRejected(t, tc.req)
			})
		}

		assertValidationAccepted(t, &storagev1.DeleteSnapshotRequest{
			DiskId:     testValidationSourceDiskID,
			SnapshotId: testValidationSnapshotID,
		})
	})

	t.Run("PruneSnapshotsRequest", func(t *testing.T) {
		invalidCases := []struct {
			name string
			req  *storagev1.PruneSnapshotsRequest
		}{
			{name: "invalid_disk_id", req: &storagev1.PruneSnapshotsRequest{DiskId: "bad-uuid"}},
			{name: "nil_disk_id", req: &storagev1.PruneSnapshotsRequest{DiskId: testValidationNilUUID}},
		}
		for _, tc := range invalidCases {
			t.Run("reject/"+tc.name, func(t *testing.T) {
				assertValidationRejected(t, tc.req)
			})
		}

		assertValidationAccepted(t, &storagev1.PruneSnapshotsRequest{DiskId: testValidationSourceDiskID, DryRun: true})
		assertValidationAccepted(t, &storagev1.PruneSnapshotsRequest{DiskId: testValidationSourceDiskID, DryRun: false})
	})
}
