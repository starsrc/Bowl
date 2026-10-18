package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestFallocateRequestValidation 验证 FallocateRequest 的字段约束与边界逻辑。
func TestFallocateRequestValidation(t *testing.T) {
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
		offset    uint64
		length    uint64
		mode      uint32
		sessionID *uint64
		valid     bool
	}{
		// 正向边界用例
		{
			name:   "合法: 最小边界参数 (mode=0)",
			ino:    1,
			fh:     1,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  true,
		},
		{
			name:      "合法: 常规预分配 (offset > 0, mode=0x01, 携带有效 session_id)",
			ino:       100,
			fh:        42,
			offset:    4096,
			length:    8192,
			mode:      0x01,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:   "合法: 打洞保留大小 (mode=0x03)",
			ino:    100,
			fh:     42,
			offset: 1024,
			length: 2048,
			mode:   0x03,
			valid:  true,
		},
		{
			name:      "合法: 最大合法 int64 边界参数与 session_id",
			ino:       math.MaxInt64,
			fh:        math.MaxInt64,
			offset:    math.MaxInt64,
			length:    math.MaxInt64,
			mode:      0,
			sessionID: &maxSessionID,
			valid:     true,
		},

		// 边界与防御负向用例：ino
		{
			name:   "非法: ino 为 0",
			ino:    0,
			fh:     1,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: ino 超过 math.MaxInt64",
			ino:    uint64(math.MaxInt64) + 1,
			fh:     1,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: ino 为 uint64 最大值",
			ino:    math.MaxUint64,
			fh:     1,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},

		// 边界与防御负向用例：fh
		{
			name:   "非法: fh 为 0",
			ino:    1,
			fh:     0,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: fh 超过 math.MaxInt64",
			ino:    1,
			fh:     uint64(math.MaxInt64) + 1,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: fh 为 uint64 最大值",
			ino:    1,
			fh:     math.MaxUint64,
			offset: 0,
			length: 1,
			mode:   0,
			valid:  false,
		},

		// 边界与防御负向用例：offset
		{
			name:   "非法: offset 超过 math.MaxInt64",
			ino:    1,
			fh:     1,
			offset: uint64(math.MaxInt64) + 1,
			length: 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: offset 为 uint64 最大值",
			ino:    1,
			fh:     1,
			offset: math.MaxUint64,
			length: 1,
			mode:   0,
			valid:  false,
		},

		// 边界与防御负向用例：length
		{
			name:   "非法: length 为 0",
			ino:    1,
			fh:     1,
			offset: 0,
			length: 0,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: length 超过 math.MaxInt64",
			ino:    1,
			fh:     1,
			offset: 0,
			length: uint64(math.MaxInt64) + 1,
			mode:   0,
			valid:  false,
		},
		{
			name:   "非法: length 为 uint64 最大值",
			ino:    1,
			fh:     1,
			offset: 0,
			length: math.MaxUint64,
			mode:   0,
			valid:  false,
		},
		{
			name:      "非法: session_id 为 0",
			ino:       1,
			fh:        1,
			offset:    0,
			length:    1,
			mode:      0,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        1,
			offset:    0,
			length:    1,
			mode:      0,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.FallocateRequest{
				Ino:       tc.ino,
				Fh:        tc.fh,
				Offset:    tc.offset,
				Length:    tc.length,
				Mode:      tc.mode,
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

// TestFallocateResponseValidation 验证 FallocateResponse 结构。
func TestFallocateResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	resp := &storagev1.FallocateResponse{}
	if err := validator.Validate(resp); err != nil {
		t.Fatalf("空 FallocateResponse 校验失败: %v", err)
	}
}
