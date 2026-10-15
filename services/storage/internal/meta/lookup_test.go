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
		targetName := "file.txt"
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
		assert.Equal(t, uint32(42), inode.Attr.Rdev)
		assert.Equal(t, uint32(4096), inode.Attr.Blksize)
		assert.Equal(t, uint64(2), inode.Attr.Blocks) // (1024 + 511) / 512 = 2
		assert.Equal(t, targetRow.Atime.Time, inode.Attr.Atime)
		assert.Equal(t, targetRow.Mtime.Time, inode.Attr.Mtime)
		assert.Equal(t, targetRow.Ctime.Time, inode.Attr.Ctime)
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
			entryName := "test_file"

			parentRow := db.Inode{
				Ino:  int64(parentIno),
				Mode: int32(meta.S_IFDIR | 0o755),
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
			assert.Equal(t, uint32(4096), inode.Attr.Blksize)
			assert.Equal(t, tc.expectedBlocks, inode.Attr.Blocks)
		})
	}
}

func TestLookup_InputValidation(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		name       string
		parentIno  uint64
		entryName  string
		wantErr    error
		expectTx   bool
	}{
		{
			name:       "parentIno 0 invalid",
			parentIno:  0,
			entryName:  "file.txt",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "empty name invalid",
			parentIno:  2,
			entryName:  "",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with slash invalid",
			parentIno:  2,
			entryName:  "dir/file.txt",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with leading slash invalid",
			parentIno:  2,
			entryName:  "/file.txt",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with trailing slash invalid",
			parentIno:  2,
			entryName:  "file.txt/",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with embedded NUL byte invalid",
			parentIno:  2,
			entryName:  "a\x00b",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with single NUL byte invalid",
			parentIno:  2,
			entryName:  "\x00",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with leading NUL byte invalid",
			parentIno:  2,
			entryName:  "\x00file.txt",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with trailing NUL byte invalid",
			parentIno:  2,
			entryName:  "file.txt\x00",
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with 256 chars invalid",
			parentIno:  2,
			entryName:  strings.Repeat("a", 256),
			wantErr:    meta.ErrInvalidArgument,
			expectTx:   false,
		},
		{
			name:       "name with exactly 255 chars valid",
			parentIno:  2,
			entryName:  strings.Repeat("a", 255),
			wantErr:    nil,
			expectTx:   true,
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
						Ino:  int64(tc.parentIno),
						Mode: int32(meta.S_IFDIR | 0o755),
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
		entryName := "file.txt"

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
				entryName := "file.txt"

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
		entryName := "file.txt"
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
		entryName := "nonexistent.txt"

		parentRow := db.Inode{
			Ino:  int64(parentIno),
			Mode: int32(meta.S_IFDIR | 0o755),
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
		entryName := "file.txt"
		dbErr := errors.New("db query entry read timeout")

		parentRow := db.Inode{
			Ino:  int64(parentIno),
			Mode: int32(meta.S_IFDIR | 0o755),
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
			entryName := "file.txt"
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
			entryName := "file.txt"
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
				entryName := "file.txt"

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
		Return(db.Inode{Ino: 1, Mode: int32(meta.S_IFDIR | 0o755)}, nil).
		Once()

	mockQuerier.EXPECT().
		GetInodeByEntry(mock.Anything, db.GetInodeByEntryParams{ParentIno: 1, Name: "child"}).
		Return(db.Inode{Ino: 2, Mode: int32(0o100644)}, nil).
		Once()

	inode, err := mgr.Lookup(ctx, 1, "child")
	require.NoError(t, err)
	require.NotNil(t, inode)

	require.Len(t, capturedOpts, 2)
	cfg := tx.ApplyOptions(capturedOpts...)
	assert.Equal(t, tx.ReadOnly, cfg.AccessMode)
	assert.Equal(t, tx.RepeatableRead, cfg.IsoLevel)
}

func TestModule(t *testing.T) {
	assert.NotNil(t, meta.Module)
}
