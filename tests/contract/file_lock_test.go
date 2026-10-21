package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestFileLockValidation 验证 FileLock 共享消息的字段约束与 CEL 闭区间规则。
func TestFileLockValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name  string
		lk    *storagev1.FileLock
		valid bool
	}{
		{
			name: "合法: 常规字节区间共享读锁 (F_RDLCK = 0)",
			lk: &storagev1.FileLock{
				Start: 0,
				End:   4095,
				Type:  0,
				Pid:   1234,
			},
			valid: true,
		},
		{
			name: "合法: 单字节锁 (start == end) 与排他写锁 (F_WRLCK = 1)",
			lk: &storagev1.FileLock{
				Start: 100,
				End:   100,
				Type:  1,
				Pid:   5678,
			},
			valid: true,
		},
		{
			name: "合法: 整文件区间至 OffsetMax (INT64_MAX) 与解锁 (F_UNLCK = 2)",
			lk: &storagev1.FileLock{
				Start: 0,
				End:   math.MaxInt64,
				Type:  2,
				Pid:   0,
			},
			valid: true,
		},
		{
			name: "合法: 起点与终点均为 OffsetMax",
			lk: &storagev1.FileLock{
				Start: math.MaxInt64,
				End:   math.MaxInt64,
				Type:  1,
				Pid:   1,
			},
			valid: true,
		},
		{
			name: "非法: start > end 违反闭区间 CEL 约束",
			lk: &storagev1.FileLock{
				Start: 200,
				End:   100,
				Type:  0,
				Pid:   1234,
			},
			valid: false,
		},
		{
			name: "非法: start 超过 OffsetMax (INT64_MAX)",
			lk: &storagev1.FileLock{
				Start: uint64(math.MaxInt64) + 1,
				End:   uint64(math.MaxInt64) + 1,
				Type:  0,
				Pid:   1234,
			},
			valid: false,
		},
		{
			name: "非法: end 超过 OffsetMax (INT64_MAX)",
			lk: &storagev1.FileLock{
				Start: 0,
				End:   uint64(math.MaxInt64) + 1,
				Type:  0,
				Pid:   1234,
			},
			valid: false,
		},
		{
			name: "非法: type 超过 2 (非 F_RDLCK/F_WRLCK/F_UNLCK)",
			lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  3,
				Pid:   1234,
			},
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valErr := v.Validate(tc.lk)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestGetLkRequestValidation 验证 GetLkRequest 的字段约束与 CEL 规则。
func TestGetLkRequestValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	highBitOwner := uint64(1)<<63 | 0xfeedfacecafebeef

	for _, tc := range []struct {
		name  string
		req   *storagev1.GetLkRequest
		valid bool
	}{
		{
			name: "合法: POSIX 字节区间读锁冲突探测 (支持最高位为 1 的 lock_owner)",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: highBitOwner,
				Lk: &storagev1.FileLock{
					Start: 1024,
					End:   2047,
					Type:  0, // F_RDLCK
					Pid:   99,
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: true,
		},
		{
			name: "合法: BSD flock 整文件写锁冲突探测 (lk_flags = 1, [0, OffsetMax])",
			req: &storagev1.GetLkRequest{
				Ino:       math.MaxInt64,
				Fh:        math.MaxInt64,
				LockOwner: math.MaxUint64,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   math.MaxInt64,
					Type:  1, // F_WRLCK
					Pid:   0,
				},
				LkFlags:   1,
				SessionId: math.MaxInt64,
			},
			valid: true,
		},
		{
			name: "合法: GetLk 允许传入 lk.type == 2 (F_UNLCK)",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  2, // F_UNLCK
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: true,
		},
		{
			name: "非法: GetLk 传入非法 lk.type == 3",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  3,
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: lk_flags == 1 (FUSE_LK_FLOCK) 但 start != 0",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 1,
					End:   math.MaxInt64,
					Type:  1,
				},
				LkFlags:   1,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: lk_flags == 1 (FUSE_LK_FLOCK) 但 end != OffsetMax",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   4095,
					Type:  1,
				},
				LkFlags:   1,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: lk_flags 超过 1",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   math.MaxInt64,
					Type:  1,
				},
				LkFlags:   2,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: 缺失必填的 lk 字段",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk:        nil,
				LkFlags:   0,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: session_id 为 0",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  0,
				},
				LkFlags:   0,
				SessionId: 0,
			},
			valid: false,
		},
		{
			name: "非法: session_id 超过 math.MaxInt64",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  0,
				},
				LkFlags:   0,
				SessionId: uint64(math.MaxInt64) + 1,
			},
			valid: false,
		},
		{
			name: "非法: ino 为 0",
			req: &storagev1.GetLkRequest{
				Ino:       0,
				Fh:        20,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  0,
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: fh 为 0",
			req: &storagev1.GetLkRequest{
				Ino:       10,
				Fh:        0,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  0,
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valErr := v.Validate(tc.req)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestGetLkResponseValidation 验证 GetLkResponse 的字段约束。
func TestGetLkResponseValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validResp := &storagev1.GetLkResponse{
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  2, // F_UNLCK
			Pid:   0,
		},
	}
	if err := v.Validate(validResp); err != nil {
		t.Fatalf("合法 GetLkResponse 预期校验通过，实际失败: %v", err)
	}

	invalidRespNilLk := &storagev1.GetLkResponse{Lk: nil}
	if err := v.Validate(invalidRespNilLk); err == nil {
		t.Fatalf("缺失 Lk 的 GetLkResponse 预期校验失败，实际通过")
	}
}

// TestSetLkRequestValidation 验证 SetLkRequest 的字段约束与 CEL 规则。
func TestSetLkRequestValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name  string
		req   *storagev1.SetLkRequest
		valid bool
	}{
		{
			name: "合法: POSIX 记录锁申请与解锁 (允许 type == 2 F_UNLCK 及全范围 lock_owner)",
			req: &storagev1.SetLkRequest{
				Ino:       1,
				Fh:        1,
				LockOwner: math.MaxUint64,
				Lk: &storagev1.FileLock{
					Start: 10,
					End:   20,
					Type:  2, // F_UNLCK
					Pid:   100,
				},
				LkFlags:   0,
				SessionId: 1,
			},
			valid: true,
		},
		{
			name: "合法: BSD flock 整文件加锁 (lk_flags = 1, [0, OffsetMax])",
			req: &storagev1.SetLkRequest{
				Ino:       1,
				Fh:        1,
				LockOwner: 123,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   math.MaxInt64,
					Type:  1,
				},
				LkFlags:   1,
				SessionId: 1,
			},
			valid: true,
		},
		{
			name: "非法: BSD flock 传入部分字节区间",
			req: &storagev1.SetLkRequest{
				Ino:       1,
				Fh:        1,
				LockOwner: 123,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   1024,
					Type:  1,
				},
				LkFlags:   1,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: lk_flags 超过 1",
			req: &storagev1.SetLkRequest{
				Ino:       1,
				Fh:        1,
				LockOwner: 123,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  1,
				},
				LkFlags:   3,
				SessionId: 1,
			},
			valid: false,
		},
		{
			name: "非法: session_id 为 0",
			req: &storagev1.SetLkRequest{
				Ino:       1,
				Fh:        1,
				LockOwner: 123,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  1,
				},
				LkFlags:   0,
				SessionId: 0,
			},
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valErr := v.Validate(tc.req)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestSetLkwRequestValidation 验证 SetLkwRequest 的字段约束（含 request_id）与 CEL 规则。
func TestSetLkwRequestValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	for _, tc := range []struct {
		name  string
		req   *storagev1.SetLkwRequest
		valid bool
	}{
		{
			name: "合法: POSIX 阻塞写锁请求 (含全范围 lock_owner 与合法 request_id)",
			req: &storagev1.SetLkwRequest{
				Ino:       100,
				Fh:        200,
				LockOwner: uint64(1)<<63 | 999,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   4095,
					Type:  1,
					Pid:   4321,
				},
				LkFlags:   0,
				SessionId: 10,
				RequestId: math.MaxInt64,
			},
			valid: true,
		},
		{
			name: "合法: BSD flock 阻塞整文件锁请求",
			req: &storagev1.SetLkwRequest{
				Ino:       100,
				Fh:        200,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   math.MaxInt64,
					Type:  0,
				},
				LkFlags:   1,
				SessionId: 10,
				RequestId: 1,
			},
			valid: true,
		},
		{
			name: "非法: BSD flock 传入非整文件区间",
			req: &storagev1.SetLkwRequest{
				Ino:       100,
				Fh:        200,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 10,
					End:   math.MaxInt64,
					Type:  0,
				},
				LkFlags:   1,
				SessionId: 10,
				RequestId: 1,
			},
			valid: false,
		},
		{
			name: "非法: request_id 为 0",
			req: &storagev1.SetLkwRequest{
				Ino:       100,
				Fh:        200,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  1,
				},
				LkFlags:   0,
				SessionId: 10,
				RequestId: 0,
			},
			valid: false,
		},
		{
			name: "非法: request_id 超过 math.MaxInt64",
			req: &storagev1.SetLkwRequest{
				Ino:       100,
				Fh:        200,
				LockOwner: 1,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   100,
					Type:  1,
				},
				LkFlags:   0,
				SessionId: 10,
				RequestId: uint64(math.MaxInt64) + 1,
			},
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valErr := v.Validate(tc.req)
			if tc.valid && valErr != nil {
				t.Fatalf("预期校验通过，实际失败: %v", valErr)
			}
			if !tc.valid && valErr == nil {
				t.Fatalf("预期校验失败，实际通过")
			}
		})
	}
}

// TestSetLkAndSetLkwResponseValidation 验证 SetLkResponse 与 SetLkwResponse 空响应结构。
func TestSetLkAndSetLkwResponseValidation(t *testing.T) {
	v, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	if err := v.Validate(&storagev1.SetLkResponse{}); err != nil {
		t.Fatalf("空 SetLkResponse 预期校验通过，实际失败: %v", err)
	}
	if err := v.Validate(&storagev1.SetLkwResponse{}); err != nil {
		t.Fatalf("空 SetLkwResponse 预期校验通过，实际失败: %v", err)
	}
}
