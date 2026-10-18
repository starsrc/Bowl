package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestFsyncDirRequestValidation 验证 FsyncDirRequest 的字段约束与边界逻辑。
func TestFsyncDirRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validSessionID := uint64(1001)
	maxSessionID := uint64(math.MaxInt64)
	zeroSessionID := uint64(0)
	overflowSessionID := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name      string
		ino       uint64
		fh        uint64
		datasync  bool
		sessionID *uint64
		valid     bool
	}{
		// 正向边界用例
		{
			name:     "合法: 全量目录同步 (datasync = false, 无 session_id)",
			ino:      1,
			fh:       1,
			datasync: false,
			valid:    true,
		},
		{
			name:      "合法: 仅数据同步 (datasync = true, 携带有效 session_id)",
			ino:       100,
			fh:        42,
			datasync:  true,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 Inode、句柄与 session_id",
			ino:       math.MaxInt64,
			fh:        math.MaxInt64,
			datasync:  true,
			sessionID: &maxSessionID,
			valid:     true,
		},

		// 边界与防御负向用例
		{
			name:     "非法: ino 为 0",
			ino:      0,
			fh:       1,
			datasync: false,
			valid:    false,
		},
		{
			name:     "非法: ino 超过 math.MaxInt64",
			ino:      uint64(math.MaxInt64) + 1,
			fh:       1,
			datasync: false,
			valid:    false,
		},
		{
			name:     "非法: ino 为 uint64 最大值",
			ino:      math.MaxUint64,
			fh:       1,
			datasync: false,
			valid:    false,
		},
		{
			name:     "非法: fh 为 0",
			ino:      1,
			fh:       0,
			datasync: false,
			valid:    false,
		},
		{
			name:     "非法: fh 超过 math.MaxInt64",
			ino:      1,
			fh:       uint64(math.MaxInt64) + 1,
			datasync: false,
			valid:    false,
		},
		{
			name:     "非法: fh 为 uint64 最大值",
			ino:      1,
			fh:       math.MaxUint64,
			datasync: false,
			valid:    false,
		},
		{
			name:      "非法: session_id 为 0",
			ino:       1,
			fh:        1,
			datasync:  false,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        1,
			datasync:  false,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.FsyncDirRequest{
				Ino:       tc.ino,
				Fh:        tc.fh,
				Datasync:  tc.datasync,
				SessionId: tc.sessionID,
			}

			valErr := validator.Validate(req)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestFsyncDirResponseValidation 验证 FsyncDirResponse 的字段约束。
func TestFsyncDirResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.FsyncDirResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("空 FsyncDirResponse 预期校验通过，实际失败: %v", err)
	}
}
