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

	diskv1 "github.com/starsrc/bowl/gen/go/disk/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

const (
	validDiskUUID = "018f78d9-2b63-71e1-87ab-18e0018f78d9"
	nilDiskUUID   = "00000000-0000-0000-0000-000000000000"
)

func assertDiskErrorInfoReason(t *testing.T, err error, expectedReason string) {
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
		assert.Equal(t, validator.DiskErrorDomain, info.GetDomain())
		assert.Equal(t, expectedReason, info.GetReason())
	}
}

func validSampleDisk() *diskv1.Disk {
	now := timestamppb.Now()
	return &diskv1.Disk{
		DiskId:    validDiskUUID,
		Name:      "disk-1",
		RootIno:   1,
		MaxBytes:  10 * 1024 * 1024 * 1024,
		UsedBytes: 1024,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func TestDiskContract_ValidMessages(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)
	require.NotNil(t, v)

	t.Run("有效 CreateDiskRequest 校验通过", func(t *testing.T) {
		req := &diskv1.CreateDiskRequest{
			Name:     "primary-disk",
			RootIno:  1,
			MaxBytes: 1024 * 1024 * 1024,
		}
		assert.NoError(t, v.Validate(req))

		reqMaxBoundary := &diskv1.CreateDiskRequest{
			Name:     strings.Repeat("a", 255),
			RootIno:  math.MaxInt64,
			MaxBytes: math.MaxInt64,
		}
		assert.NoError(t, v.Validate(reqMaxBoundary))

		reqEmptyName := &diskv1.CreateDiskRequest{
			Name:     "",
			RootIno:  100,
			MaxBytes: 4096,
		}
		assert.NoError(t, v.Validate(reqEmptyName))
	})

	t.Run("有效 GetDiskRequest 校验通过", func(t *testing.T) {
		req := &diskv1.GetDiskRequest{
			DiskId: validDiskUUID,
		}
		assert.NoError(t, v.Validate(req))
	})

	t.Run("有效 UpdateDiskRequest 校验通过", func(t *testing.T) {
		nameVal := "renamed-disk"
		maxBytesVal := uint64(20 * 1024 * 1024 * 1024)

		reqOnlyName := &diskv1.UpdateDiskRequest{
			DiskId: validDiskUUID,
			Name:   &nameVal,
		}
		assert.NoError(t, v.Validate(reqOnlyName))

		reqOnlyMaxBytes := &diskv1.UpdateDiskRequest{
			DiskId:   validDiskUUID,
			MaxBytes: &maxBytesVal,
		}
		assert.NoError(t, v.Validate(reqOnlyMaxBytes))

		reqBoth := &diskv1.UpdateDiskRequest{
			DiskId:   validDiskUUID,
			Name:     &nameVal,
			MaxBytes: &maxBytesVal,
		}
		assert.NoError(t, v.Validate(reqBoth))
	})

	t.Run("有效 DeleteDiskRequest 校验通过", func(t *testing.T) {
		req := &diskv1.DeleteDiskRequest{
			DiskId: validDiskUUID,
		}
		assert.NoError(t, v.Validate(req))
	})

	t.Run("有效 StatFSRequest 与 StatFSResponse 校验通过", func(t *testing.T) {
		req := &diskv1.StatFSRequest{
			DiskId: validDiskUUID,
		}
		assert.NoError(t, v.Validate(req))

		resp := &diskv1.StatFSResponse{
			TotalBytes: 10 * 1024 * 1024 * 1024,
			UsedBytes:  2 * 1024 * 1024 * 1024,
			AvailBytes: 8 * 1024 * 1024 * 1024,
			Bsize:      4096,
			Frsize:     4096,
			Blocks:     2621440,
			Bfree:      2097152,
			Bavail:     2097152,
			Files:      0,
			Ffree:      0,
			Namelen:    255,
		}
		assert.NoError(t, v.Validate(resp))
	})

	t.Run("有效 UpdateDiskUsageRequest 校验通过", func(t *testing.T) {
		for _, delta := range []int64{1024, -1024, 0, math.MaxInt64, math.MinInt64} {
			req := &diskv1.UpdateDiskUsageRequest{
				DiskId:     validDiskUUID,
				DeltaBytes: delta,
			}
			assert.NoError(t, v.Validate(req))
		}
	})

	t.Run("有效 Disk 实体及响应消息校验通过", func(t *testing.T) {
		disk := validSampleDisk()
		assert.NoError(t, v.Validate(disk))

		assert.NoError(t, v.Validate(&diskv1.CreateDiskResponse{Disk: disk}))
		assert.NoError(t, v.Validate(&diskv1.GetDiskResponse{Disk: disk}))
		assert.NoError(t, v.Validate(&diskv1.UpdateDiskResponse{Disk: disk}))
		assert.NoError(t, v.Validate(&diskv1.DeleteDiskResponse{}))
		assert.NoError(t, v.Validate(&diskv1.UpdateDiskUsageResponse{Disk: disk}))
	})
}

func TestDiskContract_NameValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	tooLongName := strings.Repeat("a", 256)
	nulName := "disk\x00name"

	t.Run("CreateDiskRequest.name 超长映射为 ENAMETOOLONG，含空字节映射为 EINVAL", func(t *testing.T) {
		reqTooLong := &diskv1.CreateDiskRequest{
			Name:     tooLongName,
			RootIno:  1,
			MaxBytes: 1024,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqTooLong), validator.ReasonNameTooLong)

		reqNul := &diskv1.CreateDiskRequest{
			Name:     nulName,
			RootIno:  1,
			MaxBytes: 1024,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqNul), validator.ReasonInvalidArgument)
	})

	t.Run("UpdateDiskRequest.name 超长映射为 ENAMETOOLONG，含空字节映射为 EINVAL", func(t *testing.T) {
		reqTooLong := &diskv1.UpdateDiskRequest{
			DiskId: validDiskUUID,
			Name:   &tooLongName,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqTooLong), validator.ReasonNameTooLong)

		reqNul := &diskv1.UpdateDiskRequest{
			DiskId: validDiskUUID,
			Name:   &nulName,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqNul), validator.ReasonInvalidArgument)
	})

	t.Run("Disk.name 超长映射为 ENAMETOOLONG，含空字节映射为 EINVAL", func(t *testing.T) {
		diskTooLong := validSampleDisk()
		diskTooLong.Name = tooLongName
		assertDiskErrorInfoReason(t, v.Validate(diskTooLong), validator.ReasonNameTooLong)

		diskNul := validSampleDisk()
		diskNul.Name = nulName
		assertDiskErrorInfoReason(t, v.Validate(diskNul), validator.ReasonInvalidArgument)
	})
}

func TestDiskContract_DiskIDValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	invalidUUIDs := []string{
		"not-a-uuid",
		"12345678-1234-1234-1234",
		"zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz",
		nilDiskUUID,
	}

	for _, badID := range invalidUUIDs {
		badID := badID
		t.Run("非法或全零 disk_id 拦截: "+badID, func(t *testing.T) {
			assertDiskErrorInfoReason(t, v.Validate(&diskv1.GetDiskRequest{DiskId: badID}), validator.ReasonInvalidArgument)
			name := "foo"
			assertDiskErrorInfoReason(t, v.Validate(&diskv1.UpdateDiskRequest{DiskId: badID, Name: &name}), validator.ReasonInvalidArgument)
			assertDiskErrorInfoReason(t, v.Validate(&diskv1.DeleteDiskRequest{DiskId: badID}), validator.ReasonInvalidArgument)
			assertDiskErrorInfoReason(t, v.Validate(&diskv1.StatFSRequest{DiskId: badID}), validator.ReasonInvalidArgument)
			assertDiskErrorInfoReason(t, v.Validate(&diskv1.UpdateDiskUsageRequest{DiskId: badID, DeltaBytes: 10}), validator.ReasonInvalidArgument)

			d := validSampleDisk()
			d.DiskId = badID
			assertDiskErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
		})
	}
}

func TestDiskContract_NumericBoundaryValidation(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("CreateDiskRequest.root_ino 边界校验", func(t *testing.T) {
		reqZero := &diskv1.CreateDiskRequest{
			Name:     "d",
			RootIno:  0,
			MaxBytes: 1024,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqZero), validator.ReasonInvalidArgument)

		reqOverflow := &diskv1.CreateDiskRequest{
			Name:     "d",
			RootIno:  uint64(math.MaxInt64) + 1,
			MaxBytes: 1024,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqOverflow), validator.ReasonInvalidArgument)
	})

	t.Run("CreateDiskRequest.max_bytes 边界校验", func(t *testing.T) {
		reqZero := &diskv1.CreateDiskRequest{
			Name:     "d",
			RootIno:  1,
			MaxBytes: 0,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqZero), validator.ReasonInvalidArgument)

		reqOverflow := &diskv1.CreateDiskRequest{
			Name:     "d",
			RootIno:  1,
			MaxBytes: uint64(math.MaxInt64) + 1,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqOverflow), validator.ReasonInvalidArgument)
	})

	t.Run("UpdateDiskRequest.max_bytes 边界校验", func(t *testing.T) {
		zero := uint64(0)
		reqZero := &diskv1.UpdateDiskRequest{
			DiskId:   validDiskUUID,
			MaxBytes: &zero,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqZero), validator.ReasonInvalidArgument)

		overflow := uint64(math.MaxInt64) + 1
		reqOverflow := &diskv1.UpdateDiskRequest{
			DiskId:   validDiskUUID,
			MaxBytes: &overflow,
		}
		assertDiskErrorInfoReason(t, v.Validate(reqOverflow), validator.ReasonInvalidArgument)
	})

	t.Run("Disk.used_bytes 边界校验", func(t *testing.T) {
		d := validSampleDisk()
		d.UsedBytes = uint64(math.MaxInt64) + 1
		assertDiskErrorInfoReason(t, v.Validate(d), validator.ReasonInvalidArgument)
	})
}

func TestDiskContract_UpdateDiskRequest_NoFields(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	reqEmpty := &diskv1.UpdateDiskRequest{
		DiskId: validDiskUUID,
	}
	assertDiskErrorInfoReason(t, v.Validate(reqEmpty), validator.ReasonInvalidArgument)
}

func TestDiskContract_RequiredFields(t *testing.T) {
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	t.Run("Disk 实体缺少必填时间戳", func(t *testing.T) {
		dNoCreated := validSampleDisk()
		dNoCreated.CreatedAt = nil
		assertDiskErrorInfoReason(t, v.Validate(dNoCreated), validator.ReasonInvalidArgument)

		dNoUpdated := validSampleDisk()
		dNoUpdated.UpdatedAt = nil
		assertDiskErrorInfoReason(t, v.Validate(dNoUpdated), validator.ReasonInvalidArgument)
	})

	t.Run("响应消息缺少必填 Disk", func(t *testing.T) {
		assertDiskErrorInfoReason(t, v.Validate(&diskv1.CreateDiskResponse{Disk: nil}), validator.ReasonInvalidArgument)
		assertDiskErrorInfoReason(t, v.Validate(&diskv1.GetDiskResponse{Disk: nil}), validator.ReasonInvalidArgument)
		assertDiskErrorInfoReason(t, v.Validate(&diskv1.UpdateDiskResponse{Disk: nil}), validator.ReasonInvalidArgument)
		assertDiskErrorInfoReason(t, v.Validate(&diskv1.UpdateDiskUsageResponse{Disk: nil}), validator.ReasonInvalidArgument)
	})
}
