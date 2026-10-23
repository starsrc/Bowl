package contract_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	validStorageDiskUUID = "018f78d9-2b63-71e1-87ab-18e0018f78d9"
	nilStorageDiskUUID   = "00000000-0000-0000-0000-000000000000"
)

func assertStorageErrorInfoReason(t *testing.T, err error, expectedReason string) {
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
		assert.Equal(t, validator.ErrorDomain, info.GetDomain(), "storage 消息校验失败 domain 必须为 bowl.storage")
		assert.Equal(t, expectedReason, info.GetReason())
	}
}

func validSampleStorageDisk() *storagev1.Disk {
	now := timestamppb.Now()
	return &storagev1.Disk{
		DiskId:     validStorageDiskUUID,
		Name:       "disk-storage-1",
		RootIno:    1,
		MaxBytes:   10 * 1024 * 1024 * 1024,
		UsedBytes:  1024,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func TestStorageDiskContract_ValidMessages(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	require.NotNil(t, v)

	t.Run("有效 CreateDiskRequest 校验通过", func(t *testing.T) {
		req := &storagev1.CreateDiskRequest{
			Name:     "primary-disk",
			MaxBytes: 1024 * 1024 * 1024,
		}
		assert.NoError(t, v.Validate(req))

		reqMaxBoundary := &storagev1.CreateDiskRequest{
			Name:     strings.Repeat("a", 255),
			MaxBytes: math.MaxInt64,
		}
		assert.NoError(t, v.Validate(reqMaxBoundary))

		reqMinBoundary := &storagev1.CreateDiskRequest{
			Name:     "",
			MaxBytes: 1,
		}
		assert.NoError(t, v.Validate(reqMinBoundary))
	})

	t.Run("有效 CreateDiskResponse 校验通过", func(t *testing.T) {
		resp := &storagev1.CreateDiskResponse{
			Disk: validSampleStorageDisk(),
		}
		assert.NoError(t, v.Validate(resp))
	})

	t.Run("有效 GetDiskRequest 校验通过", func(t *testing.T) {
		req := &storagev1.GetDiskRequest{
			DiskId: validStorageDiskUUID,
		}
		assert.NoError(t, v.Validate(req))
	})

	t.Run("有效 GetDiskResponse 校验通过", func(t *testing.T) {
		resp := &storagev1.GetDiskResponse{
			Disk: validSampleStorageDisk(),
		}
		assert.NoError(t, v.Validate(resp))
	})

	t.Run("有效 UpdateDiskRequest 校验通过", func(t *testing.T) {
		name := "updated-disk"
		maxBytes := uint64(20 * 1024 * 1024 * 1024)

		reqOnlyName := &storagev1.UpdateDiskRequest{
			DiskId: validStorageDiskUUID,
			Name:   &name,
		}
		assert.NoError(t, v.Validate(reqOnlyName))

		reqOnlyMaxBytes := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			MaxBytes: &maxBytes,
		}
		assert.NoError(t, v.Validate(reqOnlyMaxBytes))

		reqBoth := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			Name:     &name,
			MaxBytes: &maxBytes,
		}
		assert.NoError(t, v.Validate(reqBoth))

		maxName := strings.Repeat("b", 255)
		maxCap := uint64(math.MaxInt64)
		reqMaxBoundary := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			Name:     &maxName,
			MaxBytes: &maxCap,
		}
		assert.NoError(t, v.Validate(reqMaxBoundary))

		minCap := uint64(1)
		emptyName := ""
		reqMinBoundary := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			Name:     &emptyName,
			MaxBytes: &minCap,
		}
		assert.NoError(t, v.Validate(reqMinBoundary))
	})

	t.Run("有效 UpdateDiskResponse 校验通过", func(t *testing.T) {
		resp := &storagev1.UpdateDiskResponse{
			Disk: validSampleStorageDisk(),
		}
		assert.NoError(t, v.Validate(resp))
	})

	t.Run("有效 Disk 实体校验通过", func(t *testing.T) {
		disk := validSampleStorageDisk()
		assert.NoError(t, v.Validate(disk))

		diskBoundary := &storagev1.Disk{
			DiskId:     validStorageDiskUUID,
			Name:       strings.Repeat("z", 255),
			RootIno:    math.MaxInt64,
			MaxBytes:   math.MaxInt64,
			UsedBytes:  math.MaxInt64,
			UsedInodes: math.MaxInt64,
			CreatedAt:  timestamppb.Now(),
			UpdatedAt:  timestamppb.Now(),
		}
		assert.NoError(t, v.Validate(diskBoundary))

		diskZeroUsage := &storagev1.Disk{
			DiskId:     validStorageDiskUUID,
			Name:       "",
			RootIno:    1,
			MaxBytes:   1,
			UsedBytes:  0,
			UsedInodes: 0,
			CreatedAt:  timestamppb.Now(),
			UpdatedAt:  timestamppb.Now(),
		}
		assert.NoError(t, v.Validate(diskZeroUsage))
	})

	t.Run("有效 StatFSRequest 校验通过", func(t *testing.T) {
		req := &storagev1.StatFSRequest{
			Ino: 1,
		}
		assert.NoError(t, v.Validate(req))

		reqMaxBoundary := &storagev1.StatFSRequest{
			Ino: math.MaxInt64,
		}
		assert.NoError(t, v.Validate(reqMaxBoundary))

		reqArbitrary := &storagev1.StatFSRequest{
			Ino: 100,
		}
		assert.NoError(t, v.Validate(reqArbitrary))
	})

	t.Run("有效 StatFSResponse 校验通过", func(t *testing.T) {
		resp := &storagev1.StatFSResponse{
			TotalBytes: 10 * 1024 * 1024 * 1024,
			UsedBytes:  1024 * 1024,
			AvailBytes: 10*1024*1024*1024 - 1024*1024,
			Bsize:      4096,
			Frsize:     4096,
			Blocks:     (10 * 1024 * 1024 * 1024) / 4096,
			Bfree:      (10*1024*1024*1024 - 1024*1024) / 4096,
			Bavail:     (10*1024*1024*1024 - 1024*1024) / 4096,
			Files:      10,
			Ffree:      0,
			Namelen:    255,
		}
		assert.NoError(t, v.Validate(resp))
	})
}

func TestStorageDiskContract_CreateDiskRequest_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("name 长度超过 255 字节返回 ENAMETOOLONG", func(t *testing.T) {
		req := &storagev1.CreateDiskRequest{
			Name:     strings.Repeat("a", 256),
			MaxBytes: 4096,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonNameTooLong)
	})

	t.Run("name 包含空字节返回 EINVAL", func(t *testing.T) {
		req := &storagev1.CreateDiskRequest{
			Name:     "bad\x00name",
			MaxBytes: 4096,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 为 0 返回 EINVAL", func(t *testing.T) {
		req := &storagev1.CreateDiskRequest{
			Name:     "valid-name",
			MaxBytes: 0,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		req := &storagev1.CreateDiskRequest{
			Name:     "valid-name",
			MaxBytes: uint64(math.MaxInt64) + 1,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_CreateDiskResponse_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("缺少必填 disk 实体返回 EINVAL", func(t *testing.T) {
		resp := &storagev1.CreateDiskResponse{
			Disk: nil,
		}
		assertStorageErrorInfoReason(t, v.Validate(resp), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_GetDiskRequest_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("disk_id 为空字符串返回 EINVAL", func(t *testing.T) {
		req := &storagev1.GetDiskRequest{
			DiskId: "",
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 非 UUID 格式返回 EINVAL", func(t *testing.T) {
		req := &storagev1.GetDiskRequest{
			DiskId: "invalid-uuid-format",
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 全零 Nil UUID 返回 EINVAL", func(t *testing.T) {
		req := &storagev1.GetDiskRequest{
			DiskId: nilStorageDiskUUID,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_GetDiskResponse_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("缺少必填 disk 实体返回 EINVAL", func(t *testing.T) {
		resp := &storagev1.GetDiskResponse{
			Disk: nil,
		}
		assertStorageErrorInfoReason(t, v.Validate(resp), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_Disk_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("disk_id 为空字符串返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.DiskId = ""
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 非 UUID 格式返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.DiskId = "invalid-uuid"
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 全零 Nil UUID 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.DiskId = nilStorageDiskUUID
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("name 长度超过 255 字节返回 ENAMETOOLONG", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.Name = strings.Repeat("x", 256)
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonNameTooLong)
	})

	t.Run("name 包含空字节返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.Name = "invalid\x00name"
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("root_ino 为 0 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.RootIno = 0
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("root_ino 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.RootIno = uint64(math.MaxInt64) + 1
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 为 0 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.MaxBytes = 0
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.MaxBytes = uint64(math.MaxInt64) + 1
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("used_bytes 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.UsedBytes = uint64(math.MaxInt64) + 1
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("used_inodes 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.UsedInodes = uint64(math.MaxInt64) + 1
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("缺少必填 created_at 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.CreatedAt = nil
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})

	t.Run("缺少必填 updated_at 返回 EINVAL", func(t *testing.T) {
		d := validSampleStorageDisk()
		d.UpdatedAt = nil
		assertStorageErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_UpdateDiskRequest_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("disk_id 为空字符串返回 EINVAL", func(t *testing.T) {
		name := "valid-name"
		req := &storagev1.UpdateDiskRequest{
			DiskId: "",
			Name:   &name,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 非 UUID 格式返回 EINVAL", func(t *testing.T) {
		name := "valid-name"
		req := &storagev1.UpdateDiskRequest{
			DiskId: "invalid-uuid-format",
			Name:   &name,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("disk_id 全零 Nil UUID 返回 EINVAL", func(t *testing.T) {
		name := "valid-name"
		req := &storagev1.UpdateDiskRequest{
			DiskId: nilStorageDiskUUID,
			Name:   &name,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("未指定 name 与 max_bytes 违反消息级约束返回 EINVAL", func(t *testing.T) {
		req := &storagev1.UpdateDiskRequest{
			DiskId: validStorageDiskUUID,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("name 长度超过 255 字节返回 ENAMETOOLONG", func(t *testing.T) {
		tooLongName := strings.Repeat("a", 256)
		req := &storagev1.UpdateDiskRequest{
			DiskId: validStorageDiskUUID,
			Name:   &tooLongName,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonNameTooLong)
	})

	t.Run("name 包含空字节返回 EINVAL", func(t *testing.T) {
		badName := "bad\x00name"
		req := &storagev1.UpdateDiskRequest{
			DiskId: validStorageDiskUUID,
			Name:   &badName,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 为 0 返回 EINVAL", func(t *testing.T) {
		zero := uint64(0)
		req := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			MaxBytes: &zero,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("max_bytes 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		overflow := uint64(math.MaxInt64) + 1
		req := &storagev1.UpdateDiskRequest{
			DiskId:   validStorageDiskUUID,
			MaxBytes: &overflow,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_UpdateDiskResponse_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("缺少必填 disk 实体返回 EINVAL", func(t *testing.T) {
		resp := &storagev1.UpdateDiskResponse{
			Disk: nil,
		}
		assertStorageErrorInfoReason(t, v.Validate(resp), validator.ReasonInvalidArgument)
	})
}

func TestStorageDiskContract_StatFSRequest_Invalid(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("ino 为 0 返回 EINVAL", func(t *testing.T) {
		req := &storagev1.StatFSRequest{
			Ino: 0,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})

	t.Run("ino 超过 math.MaxInt64 返回 EINVAL", func(t *testing.T) {
		overflow := uint64(math.MaxInt64) + 1
		req := &storagev1.StatFSRequest{
			Ino: overflow,
		}
		assertStorageErrorInfoReason(t, v.Validate(req), validator.ReasonInvalidArgument)
	})
}


