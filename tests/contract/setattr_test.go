package contract_test

import (
	"math"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func ptr[T any](v T) *T {
	return &v
}

// TestSetAttrRequestValidation 验证 SetAttrRequest 的字段约束与 CEL 表达式逻辑。
func TestSetAttrRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validTimestamp := timestamppb.New(time.Now())

	testCases := []struct {
		name    string
		req     *storagev1.SetAttrRequest
		isValid bool
	}{
		// 1. 基本合法场景（单一属性更新）
		{
			name: "合法: 仅更新 mode",
			req: &storagev1.SetAttrRequest{
				Ino:  1,
				Mode: ptr[uint32](0o644),
			},
			isValid: true,
		},
		{
			name: "合法: 仅更新 uid",
			req: &storagev1.SetAttrRequest{
				Ino: 1,
				Uid: ptr[uint32](1000),
			},
			isValid: true,
		},
		{
			name: "合法: 仅更新 gid",
			req: &storagev1.SetAttrRequest{
				Ino: 1,
				Gid: ptr[uint32](1000),
			},
			isValid: true,
		},
		{
			name: "合法: 仅更新 size",
			req: &storagev1.SetAttrRequest{
				Ino:  1,
				Size: ptr[uint64](1024),
			},
			isValid: true,
		},
		{
			name: "合法: 仅更新 atime 显式时间戳",
			req: &storagev1.SetAttrRequest{
				Ino:   1,
				Atime: validTimestamp,
			},
			isValid: true,
		},
		{
			name: "合法: 仅更新 mtime 显式时间戳",
			req: &storagev1.SetAttrRequest{
				Ino:   1,
				Mtime: validTimestamp,
			},
			isValid: true,
		},
		{
			name: "合法: 仅标记 atime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      1,
				AtimeNow: true,
			},
			isValid: true,
		},
		{
			name: "合法: 仅标记 mtime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      1,
				MtimeNow: true,
			},
			isValid: true,
		},
		{
			name: "合法: 组合更新 mode, uid, gid, size 与 mtime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      2,
				Mode:     ptr[uint32](0o755),
				Uid:      ptr[uint32](500),
				Gid:      ptr[uint32](500),
				Size:     ptr[uint64](4096),
				MtimeNow: true,
			},
			isValid: true,
		},
		{
			name: "合法: ino 与 size 处于 MaxInt64 临界值",
			req: &storagev1.SetAttrRequest{
				Ino:  math.MaxInt64,
				Size: ptr[uint64](math.MaxInt64),
			},
			isValid: true,
		},
		{
			name: "合法: mode 处于 65535 (0o177777) 临界值",
			req: &storagev1.SetAttrRequest{
				Ino:  1,
				Mode: ptr[uint32](65535),
			},
			isValid: true,
		},

		// 2. CEL 规则: setattr_has_update (空更新检查)
		{
			name: "非法: 未指定任何待更新属性",
			req: &storagev1.SetAttrRequest{
				Ino: 1,
			},
			isValid: false,
		},

		// 3. CEL 规则: setattr_atime_conflict (atime 与 atime_now 互斥)
		{
			name: "非法: 同时设置 atime 与 atime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      1,
				Atime:    validTimestamp,
				AtimeNow: true,
			},
			isValid: false,
		},

		// 4. CEL 规则: setattr_mtime_conflict (mtime 与 mtime_now 互斥)
		{
			name: "非法: 同时设置 mtime 与 mtime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      1,
				Mtime:    validTimestamp,
				MtimeNow: true,
			},
			isValid: false,
		},

		// 5. Ino 字段边界校验
		{
			name: "非法: ino 为 0",
			req: &storagev1.SetAttrRequest{
				Ino:  0,
				Mode: ptr[uint32](0o644),
			},
			isValid: false,
		},
		{
			name: "非法: ino 超过 MaxInt64 (防止转 PostgreSQL BIGINT 溢出)",
			req: &storagev1.SetAttrRequest{
				Ino:  math.MaxInt64 + 1,
				Mode: ptr[uint32](0o644),
			},
			isValid: false,
		},

		// 6. Mode 字段边界校验 (允许携带高位由领域层截断)
		{
			name: "合法: mode 携带高位 (如 65536 或 S_IFREG 0o100644)",
			req: &storagev1.SetAttrRequest{
				Ino:  1,
				Mode: ptr[uint32](65536),
			},
			isValid: true,
		},
		{
			name: "合法: 仅设置 ctime_now",
			req: &storagev1.SetAttrRequest{
				Ino:      1,
				CtimeNow: true,
			},
			isValid: true,
		},
		{
			name: "合法: 仅设置 flags",
			req: &storagev1.SetAttrRequest{
				Ino:   1,
				Flags: ptr[uint32](1),
			},
			isValid: true,
		},

		// 7. Size 字段边界校验
		{
			name: "非法: size 超过 MaxInt64",
			req: &storagev1.SetAttrRequest{
				Ino:  1,
				Size: ptr[uint64](math.MaxInt64 + 1),
			},
			isValid: false,
		},

		// 8. UID / GID 字段边界校验 (防止转 PostgreSQL INTEGER 符号位溢出)
		{
			name: "非法: uid 超过 MaxInt32",
			req: &storagev1.SetAttrRequest{
				Ino: 1,
				Uid: ptr[uint32](math.MaxInt32 + 1),
			},
			isValid: false,
		},
		{
			name: "非法: gid 超过 MaxInt32",
			req: &storagev1.SetAttrRequest{
				Ino: 1,
				Gid: ptr[uint32](math.MaxInt32 + 1),
			},
			isValid: false,
		},
		{
			name: "非法: flags 超过 MaxInt32",
			req: &storagev1.SetAttrRequest{
				Ino:   1,
				Flags: ptr[uint32](math.MaxInt32 + 1),
			},
			isValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.req)
			if (err == nil) != tc.isValid {
				t.Fatalf("用例 %q 判定不符合预期: 期望合法=%v, 实际错误=%v", tc.name, tc.isValid, err)
			}
		})
	}
}

// TestSetAttrResponseValidation 验证 SetAttrResponse 的约束规则。
func TestSetAttrResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	now := timestamppb.Now()
	validInode := &storagev1.Inode{
		Ino:        1,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o100644,
			Size:    1024,
			Nlink:   1,
			Blksize: 4096,
			Blocks:  2,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
		},
	}

	testCases := []struct {
		name    string
		resp    *storagev1.SetAttrResponse
		isValid bool
	}{
		{
			name: "合法: 完整 Inode 且超时时长大于等于 0",
			resp: &storagev1.SetAttrResponse{
				Inode:               validInode,
				AttrTimeoutSeconds: 1.0,
			},
			isValid: true,
		},
		{
			name: "合法: 超时时长为 0",
			resp: &storagev1.SetAttrResponse{
				Inode:               validInode,
				AttrTimeoutSeconds: 0.0,
			},
			isValid: true,
		},
		{
			name: "非法: 缺失 Inode 必填字段",
			resp: &storagev1.SetAttrResponse{
				Inode:               nil,
				AttrTimeoutSeconds: 1.0,
			},
			isValid: false,
		},
		{
			name: "非法: 属性超时时长为负数",
			resp: &storagev1.SetAttrResponse{
				Inode:               validInode,
				AttrTimeoutSeconds: -0.1,
			},
			isValid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.resp)
			if (err == nil) != tc.isValid {
				t.Fatalf("用例 %q 判定不符合预期: 期望合法=%v, 实际错误=%v", tc.name, tc.isValid, err)
			}
		})
	}
}
