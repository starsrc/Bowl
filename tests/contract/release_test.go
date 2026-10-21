package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestReleaseRequestValidation 验证 ReleaseRequest 的字段约束与边界逻辑。
func TestReleaseRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	lockOwnerHighBit := uint64(math.MaxUint64)
	validSessionID := uint64(42)
	zeroSessionID := uint64(0)
	overflowSessionID := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name         string
		ino          uint64
		fh           uint64
		lockOwner    *uint64
		releaseFlags uint32
		sessionID    *uint64
		valid        bool
	}{
		// 正向边界用例
		{
			name:  "合法: 根目录 Inode 与有效句柄释放 (默认无锁释放标志)",
			ino:   1,
			fh:    1,
			valid: true,
		},
		{
			name:         "合法: 携带 FUSE_RELEASE_FLOCK_UNLOCK (0x2)、全范围 lock_owner 与 session_id",
			ino:          100,
			fh:           42,
			lockOwner:    &lockOwnerHighBit,
			releaseFlags: 0x2,
			sessionID:    &validSessionID,
			valid:        true,
		},
		{
			name:  "合法: 最大合法 int64 边界 Inode 与句柄",
			ino:   math.MaxInt64,
			fh:    math.MaxInt64,
			valid: true,
		},

		// 边界与防御负向用例
		{
			name:  "非法: ino 为 0",
			ino:   0,
			fh:    1,
			valid: false,
		},
		{
			name:  "非法: ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			fh:    1,
			valid: false,
		},
		{
			name:  "非法: ino 为 uint64 最大值",
			ino:   math.MaxUint64,
			fh:    1,
			valid: false,
		},
		{
			name:  "非法: fh 为 0",
			ino:   1,
			fh:    0,
			valid: false,
		},
		{
			name:  "非法: fh 超过 math.MaxInt64",
			ino:   1,
			fh:    uint64(math.MaxInt64) + 1,
			valid: false,
		},
		{
			name:  "非法: fh 为 uint64 最大值",
			ino:   1,
			fh:    math.MaxUint64,
			valid: false,
		},
		{
			name:  "非法: ino 与 fh 均为 0",
			ino:   0,
			fh:    0,
			valid: false,
		},
		{
			name:         "合法: release_flags 包含 FUSE_RELEASE_FLUSH (0x1)",
			ino:          1,
			fh:           1,
			releaseFlags: 0x1,
			valid:        true,
		},
		{
			name:         "合法: release_flags 包含组合位 0x3",
			ino:          1,
			fh:           1,
			releaseFlags: 0x3,
			valid:        true,
		},
		{
			name:         "非法: release_flags 超过 3 (如 0x4)",
			ino:          1,
			fh:           1,
			releaseFlags: 0x4,
			valid:        false,
		},
		{
			name:      "非法: session_id 为 0",
			ino:       1,
			fh:        1,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        1,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.ReleaseRequest{
				Ino:          tc.ino,
				Fh:           tc.fh,
				LockOwner:    tc.lockOwner,
				ReleaseFlags: tc.releaseFlags,
				SessionId:    tc.sessionID,
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

// TestReleaseResponseValidation 验证 ReleaseResponse 消息结构。
func TestReleaseResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.ReleaseResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("ReleaseResponse 默认校验失败: %v", err)
	}
}
