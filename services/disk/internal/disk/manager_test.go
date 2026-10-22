package disk_test

import (
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/disk/internal/db"
	dbmock "github.com/starsrc/bowl/services/disk/internal/db/mock"
	"github.com/starsrc/bowl/services/disk/internal/disk"
)

func TestNewAndModule(t *testing.T) {
	mockStore := dbmock.NewMockStore(t)
	mgr := disk.New(mockStore)
	require.NotNil(t, mgr)

	app := fxtest.New(t,
		fx.Provide(func() db.Store {
			return mockStore
		}),
		disk.Module,
		fx.Invoke(func(m disk.Manager) {
			assert.NotNil(t, m)
		}),
	)
	app.RequireStart()
	app.RequireStop()
}

func TestParseDiskIDAndToDomainDisk(t *testing.T) {
	// 正常 UUID 解析测试
	validUUIDStr := "0192a3b4-c5d6-7e8f-9012-3456789abcde"
	var expectedUUID pgtype.UUID
	err := expectedUUID.Scan(validUUIDStr)
	require.NoError(t, err)

	now := time.Now().Truncate(time.Microsecond)
	row := db.Disk{
		DiskID:    expectedUUID,
		Name:      "test-disk",
		RootIno:   1001,
		MaxBytes:  10737418240,
		UsedBytes: 1048576,
		CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
		UpdatedAt: pgtype.Timestamptz{Time: now, Valid: true},
	}

	d := disk.ExportToDomainDisk(row)
	require.NotNil(t, d)
	assert.Equal(t, validUUIDStr, d.DiskID)
	assert.Equal(t, "test-disk", d.Name)
	assert.Equal(t, uint64(1001), d.RootIno)
	assert.Equal(t, uint64(10737418240), d.MaxBytes)
	assert.Equal(t, uint64(1048576), d.UsedBytes)
	assert.Equal(t, now, d.CreatedAt)
	assert.Equal(t, now, d.UpdatedAt)

	// 无效 UUID 转换测试
	invalidRow := db.Disk{
		DiskID: pgtype.UUID{Valid: false},
	}
	invalidD := disk.ExportToDomainDisk(invalidRow)
	assert.Empty(t, invalidD.DiskID)

	// parseDiskID 校验
	parsedID, err := disk.ExportParseDiskID(validUUIDStr)
	require.NoError(t, err)
	assert.Equal(t, expectedUUID, parsedID)

	_, err = disk.ExportParseDiskID("invalid-uuid")
	require.Error(t, err)
	assert.ErrorIs(t, err, disk.ErrInvalidArgument)

	_, err = disk.ExportParseDiskID("")
	require.Error(t, err)
	assert.ErrorIs(t, err, disk.ErrInvalidArgument)
}
