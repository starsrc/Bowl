package contract_test

import (
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCreateRequestValidation 验证 CreateRequest 的字段约束与边界逻辑。
func TestCreateRequestValidation(t *testing.T) {
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
		entryName []byte
		mode      uint32
		uid       uint32
		gid       uint32
		flags     uint32
		sessionID *uint64
		valid     bool
	}{
		// 正向边界用例
		{
			name:      "合法: 常规文件标准模式",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			flags:     0o100 | 0o2, // O_CREAT | O_RDWR
			valid:     true,
		},
		{
			name:      "合法: 携带有效 session_id 及最大合法 int64 session_id",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			flags:     0o100 | 0o2,
			sessionID: &validSessionID,
			valid:     true,
		},
		{
			name:      "合法: session_id 处于正整数上限 math.MaxInt64",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			flags:     0o100 | 0o2,
			sessionID: &maxSessionID,
			valid:     true,
		},
		{
			name:      "合法: 非 UTF-8 字节序列文件名",
			parentIno: 1,
			entryName: []byte{0xff, 0xfe, 0x80, 0x81},
			mode:      0o100644,
			uid:       1000,
			gid:       1000,
			flags:     0o100 | 0o2,
			valid:     true,
		},
		{
			name:      "合法: 仅包含权限位模式",
			parentIno: 1,
			entryName: []byte("newfile"),
			mode:      0o644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: 文件名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("f", 255)),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: 多字节 UTF-8 文件名称恰好 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("字", 85)),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: 文件名称包含换行符",
			parentIno: 1,
			entryName: []byte("file\nname"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: 父节点 parent_ino 处于正整数上限 math.MaxInt64",
			parentIno: math.MaxInt64,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效下限 0 (零权限文件 0000)",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},
		{
			name:      "合法: mode 处于有效上限 0o177777 (65535)",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      65535,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     true,
		},

		// 反向边界用例
		{
			name:      "非法: parent_ino 为 0",
			parentIno: 0,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: parent_ino 超过 math.MaxInt64",
			parentIno: uint64(math.MaxInt64) + 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: name 为空字符串",
			parentIno: 1,
			entryName: []byte(""),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: name 超过 255 字节",
			parentIno: 1,
			entryName: []byte(strings.Repeat("a", 256)),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: name 包含路径分隔符 /",
			parentIno: 1,
			entryName: []byte("dir/file.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: name 包含空字节 \\x00",
			parentIno: 1,
			entryName: []byte("file\x00.txt"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: name 仅包含空字节 \\x00",
			parentIno: 1,
			entryName: []byte("\x00"),
			mode:      0o100644,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: mode 超过 65535",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      65536,
			uid:       0,
			gid:       0,
			flags:     0,
			valid:     false,
		},
		{
			name:      "非法: session_id 为 0",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			sessionID: &zeroSessionID,
			valid:     false,
		},
		{
			name:      "非法: session_id 超过 math.MaxInt64",
			parentIno: 1,
			entryName: []byte("file.txt"),
			mode:      0o100644,
			sessionID: &overflowSessionID,
			valid:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := &storagev1.CreateRequest{
				ParentIno: tc.parentIno,
				Name:      tc.entryName,
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

// TestCreateResponseValidation 验证 CreateResponse 的字段约束逻辑。
func TestCreateResponseValidation(t *testing.T) {
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("初始化 protovalidate 验证器失败: %v", err)
	}

	validInode := &storagev1.Inode{
		Ino:        100,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o100644,
			Size:    0,
			Nlink:   1,
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
			name:                "合法: 完整 Inode, fh > 0 且超时时长大于等于 0",
			inode:               validInode,
			fh:                  1,
			openFlags:           0,
			entryTimeoutSeconds: 1.0,
			attrTimeoutSeconds:  1.0,
			valid:               true,
		},
		{
			name:                "合法: 超时时长为 0",
			inode:               validInode,
			fh:                  42,
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &storagev1.CreateResponse{
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
