package meta_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func matchReadOnlyOpt() any {
	return mock.MatchedBy(func(opt tx.Option) bool {
		cfg := tx.ApplyOptions(opt)
		return cfg.AccessMode == tx.ReadOnly && cfg.IsoLevel == ""
	})
}

func matchRepeatableReadOpt() any {
	return mock.MatchedBy(func(opt tx.Option) bool {
		cfg := tx.ApplyOptions(opt)
		return cfg.AccessMode == "" && cfg.IsoLevel == tx.RepeatableRead
	})
}

func setupMockTx(mockStore *dbmock.MockStore, mockQuerier *dbmock.MockQuerier) {
	mockStore.EXPECT().
		ExecTxWithOptions(
			mock.Anything,
			mock.Anything,
			matchReadOnlyOpt(),
			matchRepeatableReadOpt(),
		).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			cfg := tx.ApplyOptions(opts...)
			if cfg.AccessMode != tx.ReadOnly || cfg.IsoLevel != tx.RepeatableRead {
				return fmt.Errorf("unexpected tx options: AccessMode=%v, IsoLevel=%v", cfg.AccessMode, cfg.IsoLevel)
			}
			return fn(mockQuerier)
		}).
		Once()
}

func TestLookup_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)

	t.Run("successfully lookup entry with correct attribute mappings", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(2)
		targetName := []byte("file.txt")
		targetIno := uint64(10)

		parentRow := db.Inode{
			Ino:        int64(parentIno),
			Generation: 1,
			Mode:       int32(meta.S_IFDIR | 0o755),
			Size:       4096,
			Nlink:      2,
			Uid:        1000,
			Gid:        1000,
			Rdev:       0,
			Atime:      pgtype.Timestamptz{Time: now, Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
		}

		targetRow := db.Inode{
			Ino:        int64(targetIno),
			Generation: 3,
			Mode:       int32(0o100644), // regular file
			Size:       1024,
			Nlink:      1,
			Uid:        1001,
			Gid:        1001,
			Rdev:       42,
			Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
			Mtime:      pgtype.Timestamptz{Time: now.Add(-30 * time.Minute), Valid: true},
			Ctime:      pgtype.Timestamptz{Time: now.Add(-10 * time.Minute), Valid: true},
		}

		// Configure mock expectations
		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
				ParentIno: int64(parentIno),
				Name:      targetName,
			}).
			Return(targetRow, nil).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, targetName)
		require.NoError(t, err)
		require.NotNil(t, inode)

		assert.Equal(t, targetIno, inode.Ino)
		assert.Equal(t, uint64(3), inode.Generation)
		assert.Equal(t, uint32(0o100644), inode.Attr.Mode)
		assert.Equal(t, uint64(1024), inode.Attr.Size)
		assert.Equal(t, uint32(1), inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), inode.Attr.UID)
		assert.Equal(t, uint32(1001), inode.Attr.GID)
		assert.Equal(t, uint32(0), inode.Attr.Rdev)
		assert.Equal(t, meta.DefaultBlockSize, inode.Attr.Blksize)
		assert.Equal(t, uint64(2), inode.Attr.Blocks) // (1024 + 511) / 512 = 2
		assert.Equal(t, targetRow.Atime.Time, inode.Attr.Atime)
		assert.Equal(t, targetRow.Mtime.Time, inode.Attr.Mtime)
		assert.Equal(t, targetRow.Ctime.Time, inode.Attr.Ctime)
	})

	t.Run("successfully lookup non-UTF8 raw bytes entry", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(2)
		rawTargetName := []byte{0xff, 0xfe, 0x80, 0x81, 'b', 'i', 'n'}
		targetIno := uint64(11)

		parentRow := db.Inode{
			Ino:   int64(parentIno),
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		targetRow := db.Inode{
			Ino:   int64(targetIno),
			Mode:  int32(0o100644),
			Size:  512,
			Nlink: 1,
		}

		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
				ParentIno: int64(parentIno),
				Name:      rawTargetName,
			}).
			Return(targetRow, nil).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, rawTargetName)
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, targetIno, inode.Ino)
	})
}

func TestLookup_DynamicBlockCalculations(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name           string
		size           int64
		expectedBlocks uint64
	}{
		{
			name:           "size 0 bytes yields 0 blocks",
			size:           0,
			expectedBlocks: 0,
		},
		{
			name:           "size 1 byte yields 1 block",
			size:           1,
			expectedBlocks: 1,
		},
		{
			name:           "size 512 bytes yields 1 block",
			size:           512,
			expectedBlocks: 1,
		},
		{
			name:           "size 513 bytes yields 2 blocks",
			size:           513,
			expectedBlocks: 2,
		},
		{
			name:           "size 1024 bytes yields 2 blocks",
			size:           1024,
			expectedBlocks: 2,
		},
		{
			name:           "size 4096 bytes yields 8 blocks",
			size:           4096,
			expectedBlocks: 8,
		},
		{
			name:           "size 4097 bytes yields 9 blocks",
			size:           4097,
			expectedBlocks: 9,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			parentIno := uint64(2)
			entryName := []byte("test_file")

			parentRow := db.Inode{
				Ino:   int64(parentIno),
				Mode:  int32(meta.S_IFDIR | 0o755),
				Nlink: 2,
			}

			targetRow := db.Inode{
				Ino:   100,
				Mode:  int32(0o100644),
				Size:  tc.size,
				Nlink: 1,
			}

			setupMockTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(parentIno)).
				Return(parentRow, nil).
				Once()

			mockQuerier.EXPECT().
				GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
					ParentIno: int64(parentIno),
					Name:      entryName,
				}).
				Return(targetRow, nil).
				Once()

			inode, err := mgr.Lookup(ctx, parentIno, entryName)
			require.NoError(t, err)
			require.NotNil(t, inode)
			assert.Equal(t, meta.DefaultBlockSize, inode.Attr.Blksize)
			assert.Equal(t, tc.expectedBlocks, inode.Attr.Blocks)
		})
	}
}

func TestLookup_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name      string
		parentIno uint64
		entryName []byte
		wantErr   error
		expectTx  bool
	}{
		{
			name:      "parentIno 0 invalid",
			parentIno: 0,
			entryName: []byte("file.txt"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with slash invalid",
			parentIno: 2,
			entryName: []byte("dir/file.txt"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with leading slash invalid",
			parentIno: 2,
			entryName: []byte("/file.txt"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with trailing slash invalid",
			parentIno: 2,
			entryName: []byte("file.txt/"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with embedded NUL byte invalid",
			parentIno: 2,
			entryName: []byte("a\x00b"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with single NUL byte invalid",
			parentIno: 2,
			entryName: []byte("\x00"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with leading NUL byte invalid",
			parentIno: 2,
			entryName: []byte("\x00file.txt"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with trailing NUL byte invalid",
			parentIno: 2,
			entryName: []byte("file.txt\x00"),
			wantErr:   meta.ErrInvalidArgument,
			expectTx:  false,
		},
		{
			name:      "name with 256 chars invalid",
			parentIno: 2,
			entryName: []byte(strings.Repeat("a", 256)),
			wantErr:   meta.ErrNameTooLong,
			expectTx:  false,
		},
		{
			name:      "name with exactly 255 chars valid",
			parentIno: 2,
			entryName: []byte(strings.Repeat("a", 255)),
			wantErr:   nil,
			expectTx:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			if !tc.expectTx {
				inode, err := mgr.Lookup(ctx, tc.parentIno, tc.entryName)
				assert.Nil(t, inode)
				assert.ErrorIs(t, err, tc.wantErr)
				mockStore.AssertNotCalled(t, "ExecTxWithOptions")
			} else {
				mockQuerier := dbmock.NewMockQuerier(t)
				setupMockTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInode(mock.Anything, int64(tc.parentIno)).
					Return(db.Inode{
						Ino:   int64(tc.parentIno),
						Mode:  int32(meta.S_IFDIR | 0o755),
						Nlink: 2,
					}, nil).
					Once()

				mockQuerier.EXPECT().
					GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
						ParentIno: int64(tc.parentIno),
						Name:      tc.entryName,
					}).
					Return(db.Inode{
						Ino:  100,
						Mode: int32(0o100644),
					}, nil).
					Once()

				inode, err := mgr.Lookup(ctx, tc.parentIno, tc.entryName)
				require.NoError(t, err)
				require.NotNil(t, inode)
				assert.Equal(t, uint64(100), inode.Ino)
			}
		})
	}
}

func TestLookup_NegativeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("parent inode not found returns ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(999)
		entryName := []byte("file.txt")

		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, entryName)
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
		assert.NotErrorIs(t, err, meta.ErrNotDir)
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("parent inode is not a directory returns ErrNotDir and does not call GetInodeByEntry", func(t *testing.T) {
		nonDirModes := []struct {
			name string
			mode uint32
		}{
			{name: "regular file 0o100644", mode: 0o100644},
			{name: "regular executable 0o100755", mode: 0o100755},
			{name: "symbolic link 0o120777", mode: 0o120777},
			{name: "fifo pipe 0o010666", mode: 0o010666},
			{name: "socket 0o140777", mode: 0o140777},
			{name: "block device 0o060660", mode: 0o060660},
			{name: "character device 0o020660", mode: 0o020660},
		}

		for _, tc := range nonDirModes {
			t.Run(tc.name, func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mockQuerier := dbmock.NewMockQuerier(t)
				mgr := meta.New(mockStore)

				parentIno := uint64(2)
				entryName := []byte("file.txt")

				parentRow := db.Inode{
					Ino:  int64(parentIno),
					Mode: int32(tc.mode),
				}

				setupMockTx(mockStore, mockQuerier)

				mockQuerier.EXPECT().
					GetInode(mock.Anything, int64(parentIno)).
					Return(parentRow, nil).
					Once()

				inode, err := mgr.Lookup(ctx, parentIno, entryName)
				assert.Nil(t, inode)
				assert.ErrorIs(t, err, meta.ErrNotDir)
				assert.NotErrorIs(t, err, meta.ErrNotFound)
				assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
				mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
			})
		}
	})

	t.Run("parent DB error returns wrapped error and is not ErrNotFound or ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(2)
		entryName := []byte("file.txt")
		dbErr := errors.New("db query connection failure")

		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, entryName)
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
		assert.NotErrorIs(t, err, meta.ErrNotFound)
		assert.NotErrorIs(t, err, meta.ErrNotDir)
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("child entry not found returns ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(2)
		entryName := []byte("nonexistent.txt")

		parentRow := db.Inode{
			Ino:   int64(parentIno),
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
				ParentIno: int64(parentIno),
				Name:      entryName,
			}).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, entryName)
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
		assert.NotErrorIs(t, err, meta.ErrNotDir)
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("child entry DB error returns wrapped error and is not ErrNotFound or ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		parentIno := uint64(2)
		entryName := []byte("file.txt")
		dbErr := errors.New("db query entry read timeout")

		parentRow := db.Inode{
			Ino:   int64(parentIno),
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(parentIno)).
			Return(parentRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{
				ParentIno: int64(parentIno),
				Name:      entryName,
			}).
			Return(db.Inode{}, dbErr).
			Once()

		inode, err := mgr.Lookup(ctx, parentIno, entryName)
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, dbErr)
		assert.NotErrorIs(t, err, meta.ErrNotFound)
		assert.NotErrorIs(t, err, meta.ErrNotDir)
		assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("transaction execution failure wraps underlying error", func(t *testing.T) {
		t.Run("tx begin or pool failure", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			parentIno := uint64(2)
			entryName := []byte("file.txt")
			txErr := errors.New("connection pool closed")

			mockStore.EXPECT().
				ExecTxWithOptions(mock.Anything, mock.Anything, matchReadOnlyOpt(), matchRepeatableReadOpt()).
				Return(txErr).
				Once()

			inode, err := mgr.Lookup(ctx, parentIno, entryName)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, txErr)
			assert.NotErrorIs(t, err, meta.ErrNotFound)
			assert.NotErrorIs(t, err, meta.ErrNotDir)
			assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
		})

		t.Run("tx commit failure", func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			parentIno := uint64(2)
			entryName := []byte("file.txt")
			commitErr := errors.New("tx commit failed")

			mockStore.EXPECT().
				ExecTxWithOptions(mock.Anything, mock.Anything, matchReadOnlyOpt(), matchRepeatableReadOpt()).
				Return(commitErr).
				Once()

			inode, err := mgr.Lookup(ctx, parentIno, entryName)
			assert.Nil(t, inode)
			assert.ErrorIs(t, err, commitErr)
			assert.NotErrorIs(t, err, meta.ErrNotFound)
			assert.NotErrorIs(t, err, meta.ErrNotDir)
			assert.NotErrorIs(t, err, meta.ErrInvalidArgument)
		})
	})

	t.Run("transaction propagates wrapped domain errors as expected", func(t *testing.T) {
		testCases := []struct {
			name        string
			txErr       error
			expectedErr error
		}{
			{
				name:        "wrapped ErrNotFound converges to ErrNotFound",
				txErr:       fmt.Errorf("tx wrapped: %w", meta.ErrNotFound),
				expectedErr: meta.ErrNotFound,
			},
			{
				name:        "wrapped ErrNotDir converges to ErrNotDir",
				txErr:       fmt.Errorf("tx wrapped: %w", meta.ErrNotDir),
				expectedErr: meta.ErrNotDir,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mgr := meta.New(mockStore)

				parentIno := uint64(2)
				entryName := []byte("file.txt")

				mockStore.EXPECT().
					ExecTxWithOptions(mock.Anything, mock.Anything, matchReadOnlyOpt(), matchRepeatableReadOpt()).
					Return(tc.txErr).
					Once()

				inode, err := mgr.Lookup(ctx, parentIno, entryName)
				assert.Nil(t, inode)
				assert.ErrorIs(t, err, tc.expectedErr)
			})
		}
	})
}

func TestLookup_TransactionOptionsContract(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mgr := meta.New(mockStore)

	var capturedOpts []tx.Option
	mockStore.EXPECT().
		ExecTxWithOptions(mock.Anything, mock.Anything, matchReadOnlyOpt(), matchRepeatableReadOpt()).
		RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			capturedOpts = opts
			return fn(mockQuerier)
		}).
		Once()

	mockQuerier.EXPECT().
		GetInode(mock.Anything, int64(1)).
		Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}, nil).
		Once()

	mockQuerier.EXPECT().
		GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{ParentIno: 1, Name: []byte("child")}).
		Return(db.Inode{Ino: 2, Mode: int32(0o100644)}, nil).
		Once()

	inode, err := mgr.Lookup(ctx, 1, []byte("child"))
	require.NoError(t, err)
	require.NotNil(t, inode)

	require.Len(t, capturedOpts, 2)
	cfg := tx.ApplyOptions(capturedOpts...)
	assert.Equal(t, tx.ReadOnly, cfg.AccessMode)
	assert.Equal(t, tx.RepeatableRead, cfg.IsoLevel)
}

func TestLookup_SpecialEntries_DotAndDotDot(t *testing.T) {
	ctx := context.Background()

	t.Run("根目录下查找 '.' 返回根目录自身", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootRow := db.Inode{
			Ino:       1,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 1,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(rootRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 1, []byte("."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("根目录下查找 '..' 返回根目录自身 (自指)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootRow := db.Inode{
			Ino:       1,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 1,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(rootRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 1, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("子目录下查找 '.' 返回子目录自身", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		subDirRow := db.Inode{
			Ino:       2,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 1,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(2)).Return(subDirRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 2, []byte("."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(2), inode.Ino)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("正常子目录下查找 '..' 通过 parent.ParentIno 返回父目录 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		subDirRow := db.Inode{
			Ino:       2,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 1,
		}
		grandParentRow := db.Inode{
			Ino:   1,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(2)).Return(subDirRow, nil).Once()
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(grandParentRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 2, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("已解链孤儿目录 (rmdir-whilst-open, nlink==0) 查找 '..' 仍能解析原父目录 (STARSRC-342)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDirRow := db.Inode{
			Ino:       3,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     0, // 已被 rmdir 解链
			ParentIno: 1, // 但仍保留原父节点 1
		}
		grandParentRow := db.Inode{
			Ino:   1,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(3)).Return(orphanDirRow, nil).Once()
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(grandParentRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 3, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
	})

	t.Run("parent.ParentIno 查询不存在时回退 GetParentInode 查询", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		subDirRow := db.Inode{
			Ino:       4,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 10,
		}
		fallbackGPRow := db.Inode{
			Ino:   1,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(4)).Return(subDirRow, nil).Once()
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(10)).Return(db.Inode{}, pgx.ErrNoRows).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, int64(4)).Return(fallbackGPRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 4, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
	})

	t.Run("ParentIno <= 0 老数据回退 GetParentInode 成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		subDirRow := db.Inode{
			Ino:       5,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 0,
		}
		fallbackGPRow := db.Inode{
			Ino:   1,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(5)).Return(subDirRow, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, int64(5)).Return(fallbackGPRow, nil).Once()

		inode, err := mgr.Lookup(ctx, 5, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(1), inode.Ino)
	})

	t.Run("ParentIno <= 0 且 GetParentInode 未找到但 nlink > 0 回退为 parent 自身", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		subDirRow := db.Inode{
			Ino:       6,
			Mode:      int32(meta.S_IFDIR | 0o755),
			Nlink:     2,
			ParentIno: 0,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(6)).Return(subDirRow, nil).Once()
		mockQuerier.EXPECT().GetParentInode(mock.Anything, int64(6)).Return(db.Inode{}, pgx.ErrNoRows).Once()

		inode, err := mgr.Lookup(ctx, 6, []byte(".."))
		require.NoError(t, err)
		require.NotNil(t, inode)
		assert.Equal(t, uint64(6), inode.Ino)
	})
}

func TestLookup_OrphanParentIntercept(t *testing.T) {
	ctx := context.Background()

	t.Run("parent.Nlink == 0 时查找常规文件返回 ErrNotFound 且不查 entries (STARSRC-323)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanParent := db.Inode{
			Ino:   10,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 0,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(10)).Return(orphanParent, nil).Once()

		inode, err := mgr.Lookup(ctx, 10, []byte("file.txt"))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})

	t.Run("parent.Nlink == 0 时查找 '.' 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanParent := db.Inode{
			Ino:   10,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 0,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(10)).Return(orphanParent, nil).Once()

		inode, err := mgr.Lookup(ctx, 10, []byte("."))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("parent.Nlink == 0 时查找 '' 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanParent := db.Inode{
			Ino:   10,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 0,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(10)).Return(orphanParent, nil).Once()

		inode, err := mgr.Lookup(ctx, 10, []byte(""))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})
}

func TestLookup_EmptyNameSemantics(t *testing.T) {
	ctx := context.Background()

	t.Run("空名称在父目录不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(999)).Return(db.Inode{}, pgx.ErrNoRows).Once()

		inode, err := mgr.Lookup(ctx, 999, []byte(""))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("空名称在父节点非目录时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		fileParent := db.Inode{
			Ino:   2,
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(2)).Return(fileParent, nil).Once()

		inode, err := mgr.Lookup(ctx, 2, []byte(""))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("空名称在父目录合法时进入事务校验后返回 ErrNotFound (ENOENT, STARSRC-335)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		dirParent := db.Inode{
			Ino:   2,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}

		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(2)).Return(dirParent, nil).Once()

		inode, err := mgr.Lookup(ctx, 2, []byte(""))
		assert.Nil(t, inode)
		assert.ErrorIs(t, err, meta.ErrNotFound)
		mockQuerier.AssertNotCalled(t, "GetInodeByEntry", mock.Anything, mock.Anything)
	})
}

func TestLookup_CacheTimeouts(t *testing.T) {
	ctx := context.Background()

	t.Run("配置 WithDirEntryTimeout/WithEntryTimeout/WithAttrTimeout 独立生效", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore,
			meta.WithEntryTimeout(3.0),
			meta.WithDirEntryTimeout(6.0),
			meta.WithAttrTimeout(4.5),
		)

		parentRow := db.Inode{
			Ino:   1,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		dirChildRow := db.Inode{
			Ino:   2,
			Mode:  int32(meta.S_IFDIR | 0o755),
			Nlink: 2,
		}
		fileChildRow := db.Inode{
			Ino:   3,
			Mode:  0o100644,
			Nlink: 1,
		}

		// 查找目录子项
		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{ParentIno: 1, Name: []byte("sub")}).Return(dirChildRow, nil).Once()

		dirInode, err := mgr.Lookup(ctx, 1, []byte("sub"))
		require.NoError(t, err)
		require.NotNil(t, dirInode)
		assert.Equal(t, 6.0, dirInode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, dirInode.AttrTimeoutSeconds)

		// 查找普通文件子项
		setupMockTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().GetInode(mock.Anything, int64(1)).Return(parentRow, nil).Once()
		mockQuerier.EXPECT().GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{ParentIno: 1, Name: []byte("file")}).Return(fileChildRow, nil).Once()

		fileInode, err := mgr.Lookup(ctx, 1, []byte("file"))
		require.NoError(t, err)
		require.NotNil(t, fileInode)
		assert.Equal(t, 3.0, fileInode.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, fileInode.AttrTimeoutSeconds)
	})
}
