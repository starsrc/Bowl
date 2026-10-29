package contract_test

import (
	"math"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

// TestRenewSessionRequestValidation 验证 RenewSessionRequest 的字段约束与边界逻辑。
func TestRenewSessionRequestValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化 StorageValidator 验证器失败")

	for _, tc := range []struct {
		name      string
		sessionID uint64
		diskID    *string
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: 最小正整数 session_id (1)",
			sessionID: 1,
			valid:     true,
		},
		{
			name:      "合法: 普通正整数 session_id (1001)",
			sessionID: 1001,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 session_id (math.MaxInt64)",
			sessionID: math.MaxInt64,
			valid:     true,
		},
		{
			name:      "合法: 携带合法非 Nil UUID disk_id",
			sessionID: 1001,
			diskID:    ptrString("11111111-1111-7111-8111-111111111111"),
			valid:     true,
		},

		// 边界与防御负向用例
		{
			name:      "非法: session_id 为 0",
			sessionID: 0,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			sessionID: uint64(math.MaxInt64) + 1,
			valid:     false,
		},
		{
			name:      "非法: session_id 为 uint64 最大值",
			sessionID: math.MaxUint64,
			valid:     false,
		},
		{
			name:      "非法: disk_id 为空字符串",
			sessionID: 1001,
			diskID:    ptrString(""),
			valid:     false,
		},
		{
			name:      "非法: disk_id 为非法 UUID 字符串",
			sessionID: 1001,
			diskID:    ptrString("not-a-valid-uuid"),
			valid:     false,
		},
		{
			name:      "非法: disk_id 为全零 Nil UUID",
			sessionID: 1001,
			diskID:    ptrString("00000000-0000-0000-0000-000000000000"),
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.RenewSessionRequest{
				SessionId: tc.sessionID,
				DiskId:    tc.diskID,
			}

			valErr := v.Validate(req)
			if tc.valid {
				assert.NoError(t, valErr, "预期校验通过")
			} else {
				assert.Error(t, valErr, "预期校验失败")
				assertErrorInfoReason(t, valErr, validator.ReasonInvalidArgument)
			}
		})
	}
}

func ptrString(s string) *string {
	return &s
}

// TestCloseSessionRequestValidation 验证 CloseSessionRequest 的字段约束与边界逻辑。
func TestCloseSessionRequestValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化 StorageValidator 验证器失败")

	for _, tc := range []struct {
		name      string
		sessionID uint64
		valid     bool
	}{
		{
			name:      "合法: 最小正整数 session_id (1)",
			sessionID: 1,
			valid:     true,
		},
		{
			name:      "合法: 普通正整数 session_id (1001)",
			sessionID: 1001,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 session_id (math.MaxInt64)",
			sessionID: math.MaxInt64,
			valid:     true,
		},
		{
			name:      "非法: session_id 为 0",
			sessionID: 0,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			sessionID: uint64(math.MaxInt64) + 1,
			valid:     false,
		},
		{
			name:      "非法: session_id 为 uint64 最大值",
			sessionID: math.MaxUint64,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.CloseSessionRequest{
				SessionId: tc.sessionID,
			}

			valErr := v.Validate(req)
			if tc.valid {
				assert.NoError(t, valErr, "预期校验通过")
			} else {
				assert.Error(t, valErr, "预期校验失败")
				assertErrorInfoReason(t, valErr, validator.ReasonInvalidArgument)
			}
		})
	}
}

// TestRenewSessionResponseValidation 验证 RenewSessionResponse 的字段约束。
func TestRenewSessionResponseValidation(t *testing.T) {
	rawValidator, err := protovalidate.New()
	require.NoError(t, err, "初始化 protovalidate 验证器失败")

	t.Run("合法: 携带必填的 expires_at 时间戳", func(t *testing.T) {
		resp := &storagev1.RenewSessionResponse{
			ExpiresAt: timestamppb.New(time.Now().Add(10 * time.Minute)),
		}
		require.NoError(t, rawValidator.Validate(resp), "有效响应预期校验通过")
	})

	t.Run("非法: expires_at 为 nil 触发必填校验失败", func(t *testing.T) {
		resp := &storagev1.RenewSessionResponse{
			ExpiresAt: nil,
		}
		require.Error(t, rawValidator.Validate(resp), "缺少必填 expires_at 应校验失败")
	})
}

// TestWatchSessionRequestValidation 验证 WatchSessionRequest 的字段约束与边界逻辑。
func TestWatchSessionRequestValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err, "初始化 StorageValidator 验证器失败")

	for _, tc := range []struct {
		name      string
		sessionID uint64
		diskID    *string
		pongSeq   uint64
		valid     bool
	}{
		{
			name:      "合法: 最小正整数 session_id (1)",
			sessionID: 1,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 session_id (math.MaxInt64) 且带 disk_id 与 pong_seq",
			sessionID: math.MaxInt64,
			diskID:    ptrString("11111111-1111-7111-8111-111111111111"),
			pongSeq:   42,
			valid:     true,
		},
		{
			name:      "非法: session_id 为 0",
			sessionID: 0,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			sessionID: uint64(math.MaxInt64) + 1,
			valid:     false,
		},
		{
			name:      "非法: session_id 为 uint64 最大值",
			sessionID: math.MaxUint64,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.WatchSessionRequest{
				SessionId: tc.sessionID,
				DiskId:    tc.diskID,
				PongSeq:   tc.pongSeq,
			}

			valErr := v.Validate(req)
			if tc.valid {
				assert.NoError(t, valErr, "预期校验通过")
			} else {
				assert.Error(t, valErr, "预期校验失败")
				assertErrorInfoReason(t, valErr, validator.ReasonInvalidArgument)
			}
		})
	}
}
