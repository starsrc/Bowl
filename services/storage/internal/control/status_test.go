package control

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/pkg/rpcerr"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireMappedStatus(t *testing.T, err error, wantCode codes.Code, wantReason string) {
	t.Helper()
	st := ToStatus(err)
	require.NotNil(t, st, "错误=%v 时 ToStatus 不得返回 nil", err)
	require.Equal(t, wantCode, st.Code(), "错误=%v 的 gRPC 状态码不匹配: want=%v got=%v", err, wantCode, st.Code())
	require.Equal(t, err.Error(), st.Message(), "错误=%v 的状态消息不匹配: want=%q got=%q", err, err.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1, "错误=%v 应携带 1 个 ErrorInfo 详情: want=1 got=%d", err, len(details))
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "错误=%v 的详情类型应为 *errdetails.ErrorInfo: got=%T", err, details[0])
	require.Equal(t, rpcerr.Domain, info.GetDomain(), "错误=%v 的 ErrorInfo 领域不匹配: want=%q got=%q", err, rpcerr.Domain, info.GetDomain())
	require.Equal(t, wantReason, info.GetReason(), "错误=%v 的 ErrorInfo reason 不匹配: want=%q got=%q", err, wantReason, info.GetReason())
}

func TestToStatus_SentinelErrors_MapsToExpectedGRPCCodeAndErrorInfo(t *testing.T) {
	// Given: 控制模块与存储引擎模块定义的全部领域哨兵错误及其包装形式
	cases := []struct {
		name       string
		err        error
		wantCode   codes.Code
		wantReason string
	}{
		{name: "配额缩容低于已用空间", err: store.ErrQuotaShrinkBelowUsed, wantCode: codes.FailedPrecondition, wantReason: "EINVAL"},
		{name: "写入世代号陈旧", err: store.ErrStaleEpoch, wantCode: codes.FailedPrecondition, wantReason: "EBUSY"},
		{name: "非法参数", err: ErrInvalidArgument, wantCode: codes.InvalidArgument, wantReason: "EINVAL"},
		{name: "非法键", err: store.ErrInvalidKey, wantCode: codes.InvalidArgument, wantReason: "EINVAL"},
		{name: "非法值", err: store.ErrInvalidValue, wantCode: codes.InvalidArgument, wantReason: "EINVAL"},
		{name: "名称超长", err: ErrNameTooLong, wantCode: codes.InvalidArgument, wantReason: "ENAMETOOLONG"},
		{name: "数值越界", err: ErrRange, wantCode: codes.InvalidArgument, wantReason: "ERANGE"},
		{name: "目标不存在", err: ErrNotFound, wantCode: codes.NotFound, wantReason: "ENOENT"},
		{name: "扩展属性不存在", err: ErrNoData, wantCode: codes.NotFound, wantReason: "ENODATA"},
		{name: "目标已存在", err: ErrAlreadyExists, wantCode: codes.AlreadyExists, wantReason: "EEXIST"},
		{name: "权限拒绝", err: ErrPerm, wantCode: codes.PermissionDenied, wantReason: "EPERM"},
		{name: "只读保护", err: store.ErrReadOnly, wantCode: codes.FailedPrecondition, wantReason: rpcerr.ReasonReadOnly},
		{name: "目标非目录", err: ErrNotDir, wantCode: codes.FailedPrecondition, wantReason: "ENOTDIR"},
		{name: "目标为目录", err: ErrIsDir, wantCode: codes.FailedPrecondition, wantReason: "EISDIR"},
		{name: "目录非空", err: ErrNotEmpty, wantCode: codes.FailedPrecondition, wantReason: "ENOTEMPTY"},
		{name: "空间配额耗尽", err: ErrNoSpace, wantCode: codes.ResourceExhausted, wantReason: "ENOSPC"},
		{name: "Inode 号段耗尽", err: store.ErrInodeExhausted, wantCode: codes.ResourceExhausted, wantReason: "ENOSPC"},
		{name: "操作不支持", err: ErrNotSupported, wantCode: codes.Unimplemented, wantReason: "EOPNOTSUPP"},
		{name: "硬链接数超限", err: ErrTooManyLinks, wantCode: codes.ResourceExhausted, wantReason: "EMLINK"},
		{name: "文件过大", err: ErrFileTooLarge, wantCode: codes.InvalidArgument, wantReason: "EFBIG"},
		{name: "存储引擎输入输出故障", err: ErrIO, wantCode: codes.Internal, wantReason: "EIO"},
		{name: "实例已关闭", err: ErrClosed, wantCode: codes.Internal, wantReason: "EIO"},
		{name: "跨血缘操作拒绝", err: ErrCrossLineage, wantCode: codes.FailedPrecondition, wantReason: "EXDEV"},
		{name: "多公共祖先 Criss-Cross 拒绝", err: ErrAmbiguousLCA, wantCode: codes.FailedPrecondition, wantReason: "EINVAL"},
		{name: "磁盘无基准提交记录", err: ErrNoBaseCommit, wantCode: codes.FailedPrecondition, wantReason: "EINVAL"},
		{name: "并发合并写入中止", err: ErrConcurrentMerge, wantCode: codes.Aborted, wantReason: "EBUSY"},
		{name: "磁盘处于挂载租约中", err: ErrDiskMounted, wantCode: codes.Aborted, wantReason: "EBUSY"},
		{name: "数据库基准提交过期", err: db.ErrStaleBaseCommit, wantCode: codes.Aborted, wantReason: "EBUSY"},
		{
			name: "合并未决冲突",
			err: &MergeConflictError{
				Report: store.MergeConflictReport{TotalConflicts: 1},
				Err:    errors.New("merge conflicts"),
			},
			wantCode:   codes.Aborted,
			wantReason: "ECONFLICT",
		},
		{name: "包装后的缩容错误", err: fmt.Errorf("shrink rejected: %w", store.ErrQuotaShrinkBelowUsed), wantCode: codes.FailedPrecondition, wantReason: "EINVAL"},
	}

	// When / Then: 逐一映射为 gRPC Status 并校验状态码与 ErrorInfo
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireMappedStatus(t, tc.err, tc.wantCode, tc.wantReason)
		})
	}
}

func TestToStatus_JoinedSentinels_PrefersHigherPriorityMapping(t *testing.T) {
	// Given: 同时包含低优先级通用参数错误与高优先级缩容错误的聚合错误
	joined := errors.Join(store.ErrInvalid, store.ErrQuotaShrinkBelowUsed)

	// When / Then: 应优先命中 ErrQuotaShrinkBelowUsed 对应的 FailedPrecondition
	requireMappedStatus(t, joined, codes.FailedPrecondition, "EINVAL")
}

func TestToStatus_ExistingGRPCStatus_PassesThroughUnchanged(t *testing.T) {
	// Given: 已携带 ErrorInfo 详情的原生 gRPC 状态错误
	base, err := status.New(codes.InvalidArgument, "validator_erange").WithDetails(&errdetails.ErrorInfo{
		Reason: "ERANGE",
		Domain: rpcerr.Domain,
	})
	require.NoError(t, err, "构造基准 status 失败")

	// When: 经 ToStatus 转换
	got := ToStatus(base.Err())

	// Then: 状态码与 ErrorInfo 详情原样透传
	require.NotNil(t, got, "已有 gRPC status 透传不得返回 nil")
	require.Equal(t, codes.InvalidArgument, got.Code(), "透传状态码不匹配: want=%v got=%v", codes.InvalidArgument, got.Code())
	require.Len(t, got.Details(), 1, "透传详情数量不匹配: want=1 got=%d", len(got.Details()))
	info, ok := got.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "透传详情类型应为 *errdetails.ErrorInfo: got=%T", got.Details()[0])
	require.Equal(t, "ERANGE", info.GetReason(), "透传 reason 不匹配: want=ERANGE got=%q", info.GetReason())
}

func TestToStatus_ContextAndUnknownErrors_MapsWithoutErrorInfo(t *testing.T) {
	// Given: 上下文取消、上下文超时与未分类内部错误
	cases := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "上下文取消", err: fmt.Errorf("aborted: %w", context.Canceled), wantCode: codes.Canceled},
		{name: "上下文超时", err: fmt.Errorf("timed out: %w", context.DeadlineExceeded), wantCode: codes.DeadlineExceeded},
		{name: "未知内部错误", err: errors.New("unexpected internal failure"), wantCode: codes.Unknown},
	}

	// When / Then: 映射为对应状态码且不附加 ErrorInfo
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := ToStatus(tc.err)
			require.NotNil(t, st, "错误=%v 时 ToStatus 不得返回 nil", tc.err)
			require.Equal(t, tc.wantCode, st.Code(), "错误=%v 状态码不匹配: want=%v got=%v", tc.err, tc.wantCode, st.Code())
			require.Empty(t, st.Details(), "错误=%v 不应附加 ErrorInfo 详情: got=%v", tc.err, st.Details())
		})
	}
}

func TestToStatusAndToGRPCError_NilAndSentinelError_HandlesCorrectly(t *testing.T) {
	// Given / When / Then: nil 输入直接返回 nil，哨兵错误转换为带 ErrorInfo 的 gRPC error
	require.Nil(t, ToStatus(nil), "nil 错误经 ToStatus 应返回 nil")
	require.NoError(t, ToGRPCError(nil), "nil 错误经 ToGRPCError 应返回 nil")

	grpcErr := ToGRPCError(store.ErrStaleEpoch)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok, "ToGRPCError 输出应可被 status.FromError 解析: err=%v", grpcErr)
	require.Equal(t, codes.FailedPrecondition, st.Code(), "ErrStaleEpoch 状态码不匹配: want=%v got=%v", codes.FailedPrecondition, st.Code())
}
