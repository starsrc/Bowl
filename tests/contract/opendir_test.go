package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestOpenDirRequestValidation 验证 OpenDirRequest 的字段约束与边界逻辑。
func TestOpenDirRequestValidation(t *testing.T) {
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
		flags     uint32
		sessionID *uint64
		valid     bool
	}{
		// 正向边界用例
		{
			name:  "合法: 根目录 Inode 打开 (只读)",
			ino:   1,
			flags: 0o0, // O_RDONLY
			valid: true,
		},
		{
			name:      "合法: 常规目录 Inode 打开 (携带有效 session_id)",
			ino:       100,
			flags:     0o0,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 Inode 与 session_id",
			ino:       math.MaxInt64,
			flags:     0o0,
			sessionID: &maxSessionID,
			valid:     true,
		},
		{
			name:  "合法: flags 为 0",
			ino:   42,
			flags: 0,
			valid: true,
		},

		// 边界与防御负向用例
		{
			name:  "非法: ino 为 0",
			ino:   0,
			flags: 0o0,
			valid: false,
		},
		{
			name:  "非法: ino 超过 math.MaxInt64 (int64 正数上限)",
			ino:   uint64(math.MaxInt64) + 1,
			flags: 0o0,
			valid: false,
		},
		{
			name:  "非法: ino 为 uint64 最大值",
			ino:   math.MaxUint64,
			flags: 0o0,
			valid: false,
		},
		{
			name:      "非法: session_id 为 0",
			ino:       1,
			flags:     0o0,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			ino:       1,
			flags:     0o0,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.OpenDirRequest{
				Ino:       tc.ino,
				Flags:     tc.flags,
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

// TestOpenDirResponseValidation 验证 OpenDirResponse 的字段约束与边界逻辑。
func TestOpenDirResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name      string
		fh        uint64
		openFlags uint32
		valid     bool
	}{
		{
			name:      "合法: fh > 0 且 open_flags 为 0",
			fh:        1,
			openFlags: 0,
			valid:     true,
		},
		{
			name:      "合法: fh 为较大值",
			fh:        1000000,
			openFlags: 0,
			valid:     true,
		},
		{
			name:      "合法: 最大合法 int64 边界 fh",
			fh:        math.MaxInt64,
			openFlags: 0,
			valid:     true,
		},
		{
			name:      "非法: fh 为 0",
			fh:        0,
			openFlags: 0,
			valid:     false,
		},
		{
			name:      "非法: fh 超过 math.MaxInt64",
			fh:        uint64(math.MaxInt64) + 1,
			openFlags: 0,
			valid:     false,
		},
		{
			name:      "非法: fh 为 uint64 最大值",
			fh:        math.MaxUint64,
			openFlags: 0,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.OpenDirResponse{
				Fh:        tc.fh,
				OpenFlags: tc.openFlags,
			}

			valErr := validator.Validate(resp)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}
