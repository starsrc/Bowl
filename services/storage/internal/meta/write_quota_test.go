package meta_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func TestWrite_DiskQuota(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	rootIno := int64(10)
	fh := uint64(1)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       0,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		RootIno:    rootIno,
	}

	t.Run("首次写入扣减配额：文件大小从 0 扩容至 1024 字节，校验配额并通过 AddDiskQuotaByRootIno 原子扣减", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(1)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xAA
		}

		diskRow := db.GetDiskByRootInoForUpdateRow{
			RootIno:   rootIno,
			MaxBytes:  10000,
			UsedBytes: 0,
		}

		// 阶段 1：落盘物理块
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：事务提交
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{},
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, rootIno).
			Return(diskRow, nil).
			Once()

		expectedSliceBytes := meta.EncodeSlice(meta.Slice{
			Pos:  0,
			ID:   uint64(sliceID),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: expectedSliceBytes,
			}).
			Return(int64(24), nil).
			Once()

		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    1024,
				Refs:    1,
			}).
			Return(nil).
			Once()

		updatedFileRow := baseFileRow
		updatedFileRow.Size = 1024
		updatedFileRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedFileRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: 1024,
				Mode: baseFileRow.Mode,
			}).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, uint64(1024), resp.Inode.Attr.Size)
		mockQuerier.AssertExpectations(t)
	})

	t.Run("追加与跨洞扩展写：原 size 1024，在 offset 2048 写入 1024 字节，逻辑增量为 2048 字节原子核算", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(2)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xBB
		}

		fileRow := baseFileRow
		fileRow.Size = 1024 // 初始大小 1024

		diskRow := db.GetDiskByRootInoForUpdateRow{
			RootIno:   rootIno,
			MaxBytes:  10000,
			UsedBytes: 1024,
		}

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		// offset=2048, len=1024 -> newEnd=3072, deltaBytes = 3072 - 1024 = 2048
		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, rootIno).
			Return(diskRow, nil).
			Once()

		expectedSliceBytes := meta.EncodeSlice(meta.Slice{
			Pos:  2048,
			ID:   uint64(sliceID),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: expectedSliceBytes,
			}).
			Return(int64(24), nil).
			Once()

		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    1024,
				Refs:    1,
			}).
			Return(nil).
			Once()

		updatedFileRow := fileRow
		updatedFileRow.Size = 3072
		updatedFileRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedFileRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: 3072,
				Mode: fileRow.Mode,
			}).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 2048,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		assert.Equal(t, uint64(3072), resp.Inode.Attr.Size)
		mockQuerier.AssertExpectations(t)
	})

	t.Run("100% 满盘状态下内部覆写成功：newEnd <= inode.Size 时不核算追加配额，内部覆写依然成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(3)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xCC
		}

		fullFileRow := baseFileRow
		fullFileRow.Size = 4096 // 4096 字节

		// 阶段 1：落盘物理块
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：事务提交
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fullFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		// offset=512, len=1024 -> newEnd=1536 <= 4096
		// 不应调用 GetDiskByRootInoForUpdate 或 AddDiskQuotaByRootIno

		expectedSliceBytes := meta.EncodeSlice(meta.Slice{
			Pos:  512,
			ID:   uint64(sliceID),
			Size: 1024,
			Off:  0,
			Len:  1024,
		})

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, db.AppendChunkSliceParams{
				Ino:    int64(ino),
				Indx:   0,
				Slices: expectedSliceBytes,
			}).
			Return(int64(24), nil).
			Once()

		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, db.InsertSliceRefParams{
				SliceID: sliceID,
				Size:    1024,
				Refs:    1,
			}).
			Return(nil).
			Once()

		updatedFileRow := fullFileRow
		updatedFileRow.Mtime = pgtype.Timestamptz{Time: now, Valid: true}
		updatedFileRow.Ctime = pgtype.Timestamptz{Time: now, Valid: true}

		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, db.UpdateInodeSizeAndTouchForWriteParams{
				Ino:  int64(ino),
				Size: 4096,
				Mode: fullFileRow.Mode,
			}).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 512,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		assert.Equal(t, uint64(4096), resp.Inode.Attr.Size)
		mockQuerier.AssertExpectations(t)
	})

	t.Run("超配额写入拦截返回 ErrNoSpace 且临时物理块被彻底删除", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(4)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xDD
		}

		// 盘剩余配额仅 500 字节，而写入需 1024 字节
		diskRow := db.GetDiskByRootInoForUpdateRow{
			RootIno:   rootIno,
			MaxBytes:  1000,
			UsedBytes: 500,
		}

		// 阶段 1：落盘物理块成功
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：事务内检查配额超限
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, rootIno).
			Return(diskRow, nil).
			Once()

		// 关键断言：事务失败回滚后，阶段 1 写入的 Block 必须被 DeleteBlock 清理！
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(1024)).
			Return(nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrNoSpace)
		mockQuerier.AssertExpectations(t)
		mockBS.AssertExpectations(t)
	})

	t.Run("GetDiskByRootInoForUpdate 返回 pgx.ErrNoRows 时跳过配额检查与更新", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(5)
		payload := make([]byte, 512)

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, rootIno).
			Return(db.GetDiskByRootInoForUpdateRow{}, pgx.ErrNoRows).
			Once()

		mockQuerier.EXPECT().
			AppendChunkSlice(mock.Anything, mock.Anything).
			Return(int64(24), nil).
			Once()

		mockQuerier.EXPECT().
			InsertSliceRef(mock.Anything, mock.Anything).
			Return(nil).
			Once()

		updatedFileRow := baseFileRow
		updatedFileRow.Size = 512
		mockQuerier.EXPECT().
			UpdateInodeSizeAndTouchForWrite(mock.Anything, mock.Anything).
			Return(updatedFileRow, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(512), resp.Size)
		mockQuerier.AssertExpectations(t)
		mockBS.AssertExpectations(t)
	})

	t.Run("GetDiskByRootInoForUpdate 数据库错误：事务回滚并清理物理块", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(6)
		payload := make([]byte, 512)

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetDiskByRootInoForUpdate(mock.Anything, rootIno).
			Return(db.GetDiskByRootInoForUpdateRow{}, errors.New("connection failed")).
			Once()

		// 事务回滚，物理块被清理
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(512)).
			Return(nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.Error(t, err)
		mockQuerier.AssertExpectations(t)
		mockBS.AssertExpectations(t)
	})
}
