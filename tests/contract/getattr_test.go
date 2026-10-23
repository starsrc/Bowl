package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestGetAttrRequestValidation 验证 GetAttrRequest 参数校验契约。
func TestGetAttrRequestValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name  string
		ino   uint64
		valid bool
	}{
		{name: "常规 ino", ino: 1, valid: true},
		{name: "任意有效 ino", ino: 42, valid: true},
		{name: "int64 最大边界值", ino: math.MaxInt64, valid: true},
		{name: "零值 ino 非法", ino: 0, valid: false},
		{name: "越过 int64 最大值上界非法", ino: math.MaxInt64 + 1, valid: false},
		{name: "uint64 最大值溢出非法", ino: math.MaxUint64, valid: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.GetAttrRequest{Ino: tc.ino}
			err := validator.Validate(req)
			if (err == nil) != tc.valid {
				t.Fatalf("用例 %q: valid = %v, 但 validation error = %v", tc.name, tc.valid, err)
			}
		})
	}
}

// TestGetAttrResponseValidation 验证 GetAttrResponse 结构校验契约。
func TestGetAttrResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}

	validInode := &storagev1.Inode{
		Ino: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  0o755,
			Nlink: 1,
			Atime: timestamppb.Now(),
			Mtime: timestamppb.Now(),
			Ctime: timestamppb.Now(),
		},
	}

	testCases := []struct {
		name        string
		inode       *storagev1.Inode
		attrTimeout float64
		valid       bool
	}{
		{name: "完整有效响应", inode: validInode, attrTimeout: 1.0, valid: true},
		{name: "超时时间为零有效", inode: validInode, attrTimeout: 0.0, valid: true},
		{name: "缺少 inode 非法", inode: nil, attrTimeout: 1.0, valid: false},
		{name: "超时时间为负非法", inode: validInode, attrTimeout: -0.1, valid: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.GetAttrResponse{
				Inode:              tc.inode,
				AttrTimeoutSeconds: tc.attrTimeout,
			}
			err := validator.Validate(resp)
			if (err == nil) != tc.valid {
				t.Fatalf("用例 %q: valid = %v, 但 validation error = %v", tc.name, tc.valid, err)
			}
		})
	}
}

// TestInodeAttributeValidation 验证 InodeAttribute 物理属性校验契约。
// 重点验证 nlink == 0 孤儿节点语义以及 atime/mtime/ctime 强必填约束。
func TestInodeAttributeValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}

	now := timestamppb.Now()

	testCases := []struct {
		name  string
		attr  *storagev1.InodeAttribute
		valid bool
	}{
		{
			name: "常规文件合法属性",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
				Flags: 1,
			},
			valid: true,
		},
		{
			name: "flags 超过 MaxInt32 校验失败",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
				Flags: math.MaxInt32 + 1,
			},
			valid: false,
		},
		{
			name: "目录合法属性",
			attr: &storagev1.InodeAttribute{
				Mode:  0o755,
				Nlink: 2,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
			valid: true,
		},
		{
			name: "已解链孤儿节点 nlink 为 0 合法",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 0,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
			valid: true,
		},
		{
			name: "缺少 atime 校验失败",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 1,
				Atime: nil,
				Mtime: now,
				Ctime: now,
			},
			valid: false,
		},
		{
			name: "缺少 mtime 校验失败",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 1,
				Atime: now,
				Mtime: nil,
				Ctime: now,
			},
			valid: false,
		},
		{
			name: "缺少 ctime 校验失败",
			attr: &storagev1.InodeAttribute{
				Mode:  0o644,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: nil,
			},
			valid: false,
		},
		{
			name: "mode 为 0 校验失败",
			attr: &storagev1.InodeAttribute{
				Mode:  0,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
			valid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validator.Validate(tc.attr)
			if (err == nil) != tc.valid {
				t.Fatalf("用例 %q: valid = %v, 但 validation error = %v", tc.name, tc.valid, err)
			}
		})
	}
}
