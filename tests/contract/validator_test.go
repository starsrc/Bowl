package contract_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func assertErrorInfoReason(t *testing.T, err error, expectedReason string) {
	t.Helper()
	require.Error(t, err)

	var pvErr *protovalidate.ValidationError
	require.True(t, errors.As(err, &pvErr), "应支持 errors.As 提取底层 *protovalidate.ValidationError")

	reason, ok := validator.ErrnoReason(err)
	require.True(t, ok, "ErrnoReason 应成功提取 POSIX errno")
	assert.Equal(t, expectedReason, reason)

	for _, st := range []*status.Status{
		validator.ToStatus(err),
		func() *status.Status {
			s, ok := status.FromError(err)
			require.True(t, ok, "status.FromError 应直接识别 ValidationError 的 GRPCStatus")
			return s
		}(),
	} {
		require.NotNil(t, st)
		assert.Equal(t, codes.InvalidArgument, st.Code())

		details := st.Details()
		require.Len(t, details, 1, "应携带且仅携带 1 个 ErrorInfo detail")
		info, ok := details[0].(*errdetails.ErrorInfo)
		require.True(t, ok, "detail 类型应为 *errdetails.ErrorInfo")
		assert.Equal(t, validator.ErrorDomain, info.GetDomain())
		assert.Equal(t, expectedReason, info.GetReason())
	}
}

func TestNewStorageValidator(t *testing.T) {
	v, err := validator.NewStorageValidator()
	if err != nil {
		t.Fatalf("NewStorageValidator() returned error: %v", err)
	}
	if v == nil {
		t.Fatal("NewStorageValidator() returned nil validator")
	}

	t.Run("有效请求校验通过", func(t *testing.T) {
		validLookup := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("test.txt"),
		}
		if err := v.Validate(validLookup); err != nil {
			t.Errorf("expected valid LookupRequest, got error: %v", err)
		}

		validLookupNonUTF8 := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte{0xff, 0xfe, 0x80, 0x81},
		}
		if err := v.Validate(validLookupNonUTF8); err != nil {
			t.Errorf("expected valid non-UTF8 LookupRequest, got error: %v", err)
		}

		validMkdir := &storagev1.MkdirRequest{
			ParentIno: 1,
			Name:      []byte("subdir"),
			Mode:      0o755,
			Uid:       1000,
			Gid:       1000,
		}
		if err := v.Validate(validMkdir); err != nil {
			t.Errorf("expected valid MkdirRequest, got error: %v", err)
		}
	})

	t.Run("非法请求校验失败并精确映射 ErrorInfo.reason", func(t *testing.T) {
		invalidLookupZeroParent := &storagev1.LookupRequest{
			ParentIno: 0,
			Name:      []byte("test.txt"),
		}
		assertErrorInfoReason(t, v.Validate(invalidLookupZeroParent), validator.ReasonInvalidArgument)

		invalidLookupNameTooLong := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte(strings.Repeat("a", 256)),
		}
		assertErrorInfoReason(t, v.Validate(invalidLookupNameTooLong), validator.ReasonNameTooLong)

		invalidLookupEmbeddedNul := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("foo\x00bar"),
		}
		assertErrorInfoReason(t, v.Validate(invalidLookupEmbeddedNul), validator.ReasonInvalidArgument)

		invalidLookupSlash := &storagev1.LookupRequest{
			ParentIno: 1,
			Name:      []byte("foo/bar"),
		}
		assertErrorInfoReason(t, v.Validate(invalidLookupSlash), validator.ReasonInvalidArgument)
	})

	t.Run("Errno 敏感边界校验违规映射 (ERANGE / E2BIG / ENAMETOOLONG / EINVAL)", func(t *testing.T) {
		testCases := []struct {
			name           string
			msg            proto.Message
			expectedReason string
		}{
			{
				name: "SetXattr 空名称映射为 ERANGE",
				msg: &storagev1.SetXattrRequest{
					Ino:   1,
					Name:  []byte{},
					Value: []byte("val"),
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "SetXattr 名称超过 255 字节映射为 ERANGE",
				msg: &storagev1.SetXattrRequest{
					Ino:   1,
					Name:  bytes.Repeat([]byte("a"), 256),
					Value: []byte("val"),
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "SetXattr 名称同时超长且含 NUL 优先映射为 ERANGE",
				msg: &storagev1.SetXattrRequest{
					Ino:   1,
					Name:  append(bytes.Repeat([]byte("a"), 255), 0),
					Value: []byte("val"),
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "SetXattr 名称含 NUL 映射为 EINVAL",
				msg: &storagev1.SetXattrRequest{
					Ino:   1,
					Name:  []byte("user.a\x00b"),
					Value: []byte("val"),
				},
				expectedReason: validator.ReasonInvalidArgument,
			},
			{
				name: "SetXattr 值超过 65536 字节映射为 E2BIG",
				msg: &storagev1.SetXattrRequest{
					Ino:   1,
					Name:  []byte("user.test"),
					Value: bytes.Repeat([]byte("v"), 65537),
				},
				expectedReason: validator.ReasonTooBig,
			},
			{
				name: "GetXattr 空名称映射为 ERANGE",
				msg: &storagev1.GetXattrRequest{
					Ino:  1,
					Name: nil,
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "RemoveXattr 名称超过 255 字节映射为 ERANGE",
				msg: &storagev1.RemoveXattrRequest{
					Ino:  1,
					Name: bytes.Repeat([]byte("x"), 256),
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "ListXattrResponse 包含空名称项映射为 ERANGE",
				msg: &storagev1.ListXattrResponse{
					Names: [][]byte{[]byte("user.ok"), {}},
				},
				expectedReason: validator.ReasonRange,
			},
			{
				name: "GetXattrResponse 值超过 65536 字节映射为 E2BIG",
				msg: &storagev1.GetXattrResponse{
					Value: bytes.Repeat([]byte("v"), 65537),
				},
				expectedReason: validator.ReasonTooBig,
			},
			{
				name: "Mkdir 目录名超过 255 字节映射为 ENAMETOOLONG",
				msg: &storagev1.MkdirRequest{
					ParentIno: 1,
					Name:      bytes.Repeat([]byte("d"), 256),
					Mode:      0o755,
				},
				expectedReason: validator.ReasonNameTooLong,
			},
			{
				name: "Mkdir 目录名同时超长且包含斜杠优先映射为 ENAMETOOLONG",
				msg: &storagev1.MkdirRequest{
					ParentIno: 1,
					Name:      bytes.Repeat([]byte("/"), 256),
					Mode:      0o755,
				},
				expectedReason: validator.ReasonNameTooLong,
			},
			{
				name: "Rename 新名称超过 255 字节映射为 ENAMETOOLONG",
				msg: &storagev1.RenameRequest{
					OldParentIno: 1,
					OldName:      []byte("old"),
					NewParentIno: 1,
					NewName:      bytes.Repeat([]byte("n"), 256),
				},
				expectedReason: validator.ReasonNameTooLong,
			},
			{
				name: "Symlink 目标路径超过 4095 字节映射为 ENAMETOOLONG",
				msg: &storagev1.SymlinkRequest{
					ParentIno: 1,
					Name:      []byte("link"),
					Target:    bytes.Repeat([]byte("t"), 4096),
				},
				expectedReason: validator.ReasonNameTooLong,
			},
			{
				name: "Symlink 目标路径为空映射为 EINVAL",
				msg: &storagev1.SymlinkRequest{
					ParentIno: 1,
					Name:      []byte("link"),
					Target:    []byte{},
				},
				expectedReason: validator.ReasonInvalidArgument,
			},
			{
				name: "Symlink 目标路径同时超长且含 NUL 优先映射为 EINVAL",
				msg: &storagev1.SymlinkRequest{
					ParentIno: 1,
					Name:      []byte("link"),
					Target:    append(bytes.Repeat([]byte("t"), 4095), 0),
				},
				expectedReason: validator.ReasonInvalidArgument,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := v.Validate(tc.msg)
				assertErrorInfoReason(t, err, tc.expectedReason)
			})
		}
	})

	t.Run("原始 protovalidate.ValidationError 也可通过 ToStatus 转换", func(t *testing.T) {
		rawValidator, err := protovalidate.New()
		require.NoError(t, err)

		rawErr := rawValidator.Validate(&storagev1.SetXattrRequest{
			Ino:   1,
			Name:  []byte{},
			Value: []byte("v"),
		})
		require.Error(t, rawErr)

		st := validator.ToStatus(rawErr)
		require.NotNil(t, st)
		assert.Equal(t, codes.InvalidArgument, st.Code())
		details := st.Details()
		require.Len(t, details, 1)
		info, ok := details[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, validator.ReasonRange, info.GetReason())
	})

	t.Run("文件锁请求与 Flush/Release 静态校验违规统一映射为 EINVAL", func(t *testing.T) {
		testCases := []struct {
			name string
			msg  proto.Message
		}{
			{
				name: "FileLock start > end 映射为 EINVAL",
				msg: &storagev1.FileLock{
					Start: 200,
					End:   100,
					Type:  0,
				},
			},
			{
				name: "GetLkRequest 传入 F_UNLCK (2) 映射为 EINVAL",
				msg: &storagev1.GetLkRequest{
					Ino:       1,
					Fh:        1,
					LockOwner: 1,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  2,
					},
					LkFlags:   0,
					SessionId: 1,
				},
			},
			{
				name: "GetLkRequest FUSE_LK_FLOCK 非整文件区间映射为 EINVAL",
				msg: &storagev1.GetLkRequest{
					Ino:       1,
					Fh:        1,
					LockOwner: 1,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  0,
					},
					LkFlags:   1,
					SessionId: 1,
				},
			},
			{
				name: "SetLkRequest 非法 lk_flags 映射为 EINVAL",
				msg: &storagev1.SetLkRequest{
					Ino:       1,
					Fh:        1,
					LockOwner: 1,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  1,
					},
					LkFlags:   2,
					SessionId: 1,
				},
			},
			{
				name: "SetLkwRequest request_id 为 0 映射为 EINVAL",
				msg: &storagev1.SetLkwRequest{
					Ino:       1,
					Fh:        1,
					LockOwner: 1,
					Lk: &storagev1.FileLock{
						Start: 0,
						End:   100,
						Type:  1,
					},
					LkFlags:   0,
					SessionId: 1,
					RequestId: 0,
				},
			},
			{
				name: "ReleaseRequest 非法 release_flags 映射为 EINVAL",
				msg: &storagev1.ReleaseRequest{
					Ino:          1,
					Fh:           1,
					ReleaseFlags: 0x1,
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := v.Validate(tc.msg)
				assertErrorInfoReason(t, err, validator.ReasonInvalidArgument)
			})
		}
	})
}
