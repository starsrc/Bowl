package contract_test

import (
	"math"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestTmpFileRequestValidation 验证 TmpFileRequest 的字段约束与边界逻辑。
func TestTmpFileRequestValidation(t *testing.T) {
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
		parentIno uint64
		mode      uint32
		uid       uint32
		gid       uint32
		flags     uint32
		sessionID *uint64
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: S_IFREG 标准模式且 flags 为 O_RDWR",
			parentIno: 1,
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			flags:     0o2, // O_RDWR
			valid:     true,
		},
		{
			name:      "合法: 仅含权限位 mode (0o600) 且 flags 为 O_WRONLY",
			parentIno: 1,
			mode:      0o600,
			uid:       1000,
			gid:       1000,
			flags:     0o1, // O_WRONLY
			valid:     true,
		},
		{
			name:      "合法: mode 为 0 (零权限常规文件)",
			parentIno: 1,
			mode:      0,
			uid:       0,
			gid:       0,
			flags:     0o2,
			valid:     true,
		},
		{
			name:      "合法: mode 为 S_IFREG | 0o7777",
			parentIno: 1,
			mode:      0o107777,
			uid:       0,
			gid:       0,
			flags:     0o2,
			valid:     true,
		},
		{
			name:      "合法: flags 携带 O_TMPFILE | O_EXCL | O_RDWR",
			parentIno: 1,
			mode:      0o100600,
			uid:       1000,
			gid:       1000,
			flags:     0o20000000 | 0o200 | 0o2,
			valid:     true,
		},
		{
			name:      "合法: parent_ino 处于正整数上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			mode:      0o100644,
			flags:     0o2,
			valid:     true,
		},
		{
			name:      "合法: 携带有效 session_id",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o2,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:      "合法: session_id 处于正整数上限 math.MaxInt64",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o2,
			sessionID: &maxSessionID,
			valid:     true,
		},

		// 反向边界用例
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			mode:      0o100644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			mode:      0o100644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 超过 65535",
			parentIno: 1,
			mode:      65536,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFDIR (0o040000)",
			parentIno: 1,
			mode:      0o040755,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFLNK (0o120000)",
			parentIno: 1,
			mode:      0o120777,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFIFO (0o010000)",
			parentIno: 1,
			mode:      0o010644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFCHR (0o020000)",
			parentIno: 1,
			mode:      0o020644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFBLK (0o060000)",
			parentIno: 1,
			mode:      0o060644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: mode 文件类型为 S_IFSOCK (0o140000)",
			parentIno: 1,
			mode:      0o140644,
			flags:     0o2,
			valid:     false,
		},
		{
			name:      "非法: flags 访问模式为 O_RDONLY (0)",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o0,
			valid:     false,
		},
		{
			name:      "非法: flags 携带 O_TMPFILE 但访问模式为 O_RDONLY",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o20000000,
			valid:     false,
		},
		{
			name:      "非法: flags 访问模式为非法值 3 (O_ACCMODE)",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o3,
			valid:     false,
		},
		{
			name:      "非法: session_id 为 0",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o2,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			parentIno: 1,
			mode:      0o100644,
			flags:     0o2,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.TmpFileRequest{
				ParentIno: tc.parentIno,
				Mode:      tc.mode,
				Uid:       tc.uid,
				Gid:       tc.gid,
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

// TestTmpFileResponseValidation 验证 TmpFileResponse 的字段约束逻辑。
func TestTmpFileResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o100600,
			Size:    0,
			Nlink:   0,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 4096,
			Blocks:  0,
			Atime:   timestamppb.Now(),
			Mtime:   timestamppb.Now(),
			Ctime:   timestamppb.Now(),
		},
	}

	for _, tc := range []struct {
		name                string
		inode               *storagev1.Inode
		fh                  uint64
		openFlags           uint32
		entryTimeoutSeconds float64
		attrTimeoutSeconds  float64
		valid               bool
	}{
		{
			name:                "合法: 完整匿名 Inode (nlink = 0), fh > 0 且超时时长大于等于 0",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               true,
		},
		{
			name:                "合法: fh 处于正整数上限 math.MaxInt64 且超时时长为 0",
			inode:               validInode,
			fh:                  math.MaxInt64,
			openFlags:           0,
			entryTimeoutSeconds: 0.0,
			attrTimeoutSeconds:  0.0,
			valid:               true,
		},
		{
			name:                "非法: 缺失 Inode 必填字段",
			inode:               nil,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: fh 为 0",
			inode:               validInode,
			fh:                  0,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: fh 超过 math.MaxInt64",
			inode:               validInode,
			fh:                  uint64(math.MaxInt64) + 1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: 目录项超时时长为负数",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: -0.1,
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: 属性超时时长为负数",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  -0.1,
			valid:               false,
		},
		{
			name:                "非法: 目录项超时时长为 NaN",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: math.NaN(),
			attrTimeoutSeconds:  1.0,
			valid:               false,
		},
		{
			name:                "非法: 属性超时时长为 +Inf",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  math.Inf(1),
			valid:               false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.TmpFileResponse{
				Inode:               tc.inode,
				Fh:                  tc.fh,
				OpenFlags:           tc.openFlags,
				EntryTimeoutSeconds: tc.entryTimeoutSeconds,
				AttrTimeoutSeconds:  tc.attrTimeoutSeconds,
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
