package projection_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/projection"
)

func runStoreContractSuite(t *testing.T, ctx context.Context, store projection.Store) {
	t.Helper()

	base := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)

	idA := uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	idB := uuid.MustParse("00000000-0000-0000-0000-00000000000b")
	idC := uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	missingID := uuid.MustParse("00000000-0000-0000-0000-0000000000ff")

	_, err := store.GetDisk(ctx, missingID)
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListBranches(ctx, missingID)
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: missingID})
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListMounts(ctx, missingID, "", base)
	require.ErrorIs(t, err, projection.ErrNotFound)

	diskA := projection.DiskRecord{
		DiskID:        idA,
		Name:          "alpha",
		DefaultBranch: "main",
		MaxBytes:      1024 * 1024,
		UsedBytes:     4096,
		CreatedAt:     base,
		UpdatedAt:     base,
	}
	branchAMain := projection.BranchRecord{
		DiskID:       idA,
		Name:         "main",
		HeadCommitID: 1,
		WriterEpoch:  0,
		CreatedAt:    base,
		UpdatedAt:    base,
	}
	commitA1 := projection.CommitRecord{
		DiskID:    idA,
		CommitID:  1,
		ParentID:  0,
		Kind:      projection.CommitKindInit,
		Message:   "init",
		CreatedAt: base,
	}
	require.NoError(t, store.UpsertDisk(ctx, diskA, &branchAMain, &commitA1))

	diskB := projection.DiskRecord{
		DiskID:        idB,
		Name:          "beta",
		DefaultBranch: "main",
		MaxBytes:      2048 * 1024,
		UsedBytes:     0,
		CreatedAt:     base.Add(10 * time.Second),
		UpdatedAt:     base.Add(10 * time.Second),
	}
	require.NoError(t, store.UpsertDisk(ctx, diskB, nil, nil))

	diskC := projection.DiskRecord{
		DiskID:        idC,
		Name:          "gamma",
		DefaultBranch: "main",
		MaxBytes:      4096 * 1024,
		UsedBytes:     128,
		CreatedAt:     base.Add(10 * time.Second),
		UpdatedAt:     base.Add(10 * time.Second),
	}
	require.NoError(t, store.UpsertDisk(ctx, diskC, nil, nil))

	gotA, err := store.GetDisk(ctx, idA)
	require.NoError(t, err)
	require.Equal(t, diskA, gotA)

	page1, err := store.ListDisks(ctx, projection.ListDisksQuery{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page1.Disks, 2)
	require.Equal(t, idC, page1.Disks[0].DiskID)
	require.Equal(t, idB, page1.Disks[1].DiskID)
	require.NotEmpty(t, page1.NextPageToken)

	page2, err := store.ListDisks(ctx, projection.ListDisksQuery{PageSize: 2, PageToken: page1.NextPageToken})
	require.NoError(t, err)
	require.Len(t, page2.Disks, 1)
	require.Equal(t, idA, page2.Disks[0].DiskID)
	require.Empty(t, page2.NextPageToken)

	diskA.Name = "alpha-renamed"
	diskA.MaxBytes = 8 * 1024 * 1024
	diskA.UpdatedAt = base.Add(time.Minute)
	require.NoError(t, store.UpdateDisk(ctx, diskA))
	gotA, err = store.GetDisk(ctx, idA)
	require.NoError(t, err)
	require.Equal(t, "alpha-renamed", gotA.Name)
	require.Equal(t, uint64(8*1024*1024), gotA.MaxBytes)

	err = store.UpdateDisk(ctx, projection.DiskRecord{
		DiskID:        missingID,
		Name:          "ghost",
		DefaultBranch: "main",
		MaxBytes:      1024,
		UpdatedAt:     base,
	})
	require.ErrorIs(t, err, projection.ErrNotFound)

	branchADev := projection.BranchRecord{
		DiskID:       idA,
		Name:         "dev",
		HeadCommitID: 2,
		WriterEpoch:  1,
		CreatedAt:    base.Add(2 * time.Second),
		UpdatedAt:    base.Add(2 * time.Second),
	}
	branchAMain.HeadCommitID = 4
	branchAMain.UpdatedAt = base.Add(4 * time.Second)
	commits := []projection.CommitRecord{
		{DiskID: idA, CommitID: 2, ParentID: 1, Kind: projection.CommitKindFork, Message: "fork dev", CreatedAt: base.Add(2 * time.Second)},
		{DiskID: idA, CommitID: 3, ParentID: 2, Kind: projection.CommitKindManual, Message: "wip", CreatedAt: base.Add(3 * time.Second)},
		{DiskID: idA, CommitID: 4, ParentID: 1, MergeParentID: 3, Kind: projection.CommitKindMerge, Message: "merge dev", CreatedAt: base.Add(4 * time.Second)},
	}
	require.NoError(t, store.UpsertBranchAndCommit(ctx, []projection.BranchRecord{branchADev, branchAMain}, commits, nil))

	branches, err := store.ListBranches(ctx, idA)
	require.NoError(t, err)
	require.Len(t, branches, 2)
	require.Equal(t, "dev", branches[0].Name)
	require.Equal(t, "main", branches[1].Name)
	require.Equal(t, uint64(4), branches[1].HeadCommitID)

	cPage1, err := store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, cPage1.Commits, 2)
	require.Equal(t, uint64(4), cPage1.Commits[0].CommitID)
	require.Equal(t, uint64(3), cPage1.Commits[1].CommitID)
	require.NotEmpty(t, cPage1.NextPageToken)

	cPage2, err := store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA, PageSize: 2, PageToken: cPage1.NextPageToken})
	require.NoError(t, err)
	require.Len(t, cPage2.Commits, 2)
	require.Equal(t, uint64(2), cPage2.Commits[0].CommitID)
	require.Equal(t, uint64(1), cPage2.Commits[1].CommitID)
	require.Empty(t, cPage2.NextPageToken)

	cManual, err := store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA, Kind: projection.CommitKindManual})
	require.NoError(t, err)
	require.Len(t, cManual.Commits, 1)
	require.Equal(t, uint64(3), cManual.Commits[0].CommitID)

	mount1 := uuid.MustParse("10000000-0000-0000-0000-000000000001")
	mount2 := uuid.MustParse("10000000-0000-0000-0000-000000000002")
	mount3 := uuid.MustParse("10000000-0000-0000-0000-000000000003")

	branchAMain.WriterEpoch = 1
	require.NoError(t, store.UpsertMount(ctx, projection.MountRecord{
		DiskID:      idA,
		MountID:     mount1,
		BranchName:  "main",
		ReadOnly:    false,
		WriterEpoch: 1,
		MountedAt:   base,
		ExpiresAt:   base.Add(30 * time.Second),
	}, &branchAMain, nil))
	require.NoError(t, store.UpsertMount(ctx, projection.MountRecord{
		DiskID:      idA,
		MountID:     mount2,
		BranchName:  "dev",
		ReadOnly:    true,
		WriterEpoch: 0,
		MountedAt:   base,
		ExpiresAt:   base.Add(5 * time.Second),
	}, nil, nil))
	require.NoError(t, store.UpsertMount(ctx, projection.MountRecord{
		DiskID:      idA,
		MountID:     mount3,
		BranchName:  "main",
		ReadOnly:    true,
		WriterEpoch: 0,
		MountedAt:   base,
		ExpiresAt:   base.Add(60 * time.Second),
	}, nil, nil))

	activeAll, err := store.ListMounts(ctx, idA, "", base.Add(10*time.Second))
	require.NoError(t, err)
	require.Len(t, activeAll, 2)
	require.Equal(t, mount1, activeAll[0].MountID)
	require.Equal(t, mount3, activeAll[1].MountID)

	activeDev, err := store.ListMounts(ctx, idA, "dev", base.Add(time.Second))
	require.NoError(t, err)
	require.Len(t, activeDev, 1)
	require.Equal(t, mount2, activeDev[0].MountID)

	require.NoError(t, store.BatchUpdateDiskUsage(ctx, []projection.DiskUsageUpdate{
		{DiskID: idA, UsedBytes: 99999},
		{DiskID: idB, UsedBytes: 12345},
		{DiskID: idA, UsedBytes: 111111},
	}))
	gotA, err = store.GetDisk(ctx, idA)
	require.NoError(t, err)
	require.Equal(t, uint64(111111), gotA.UsedBytes)
	gotB, err := store.GetDisk(ctx, idB)
	require.NoError(t, err)
	require.Equal(t, uint64(12345), gotB.UsedBytes)

	require.NoError(t, store.BatchRenewMounts(ctx, []projection.MountRenewUpdate{
		{DiskID: idA, MountID: mount2, ExpiresAt: base.Add(120 * time.Second)},
	}))
	activeDev, err = store.ListMounts(ctx, idA, "dev", base.Add(10*time.Second))
	require.NoError(t, err)
	require.Len(t, activeDev, 1)
	require.Equal(t, base.Add(120*time.Second), activeDev[0].ExpiresAt)

	pruned, err := store.PruneExpiredMounts(ctx, base.Add(40*time.Second))
	require.NoError(t, err)
	require.Equal(t, int64(1), pruned)

	branchAMain.WriterEpoch = 2
	require.NoError(t, store.DeleteMounts(ctx, idA, []uuid.UUID{mount3}, []projection.BranchRecord{branchAMain}))
	activeMain, err := store.ListMounts(ctx, idA, "main", base)
	require.NoError(t, err)
	require.Empty(t, activeMain)

	require.NoError(t, store.DeleteBranch(ctx, idA, "dev"))
	branches, err = store.ListBranches(ctx, idA)
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.Equal(t, "main", branches[0].Name)
	require.Equal(t, uint64(2), branches[0].WriterEpoch)

	require.NoError(t, store.DeleteCommits(ctx, idA, []uint64{2, 3}))
	remainingCommits, err := store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA})
	require.NoError(t, err)
	require.Len(t, remainingCommits.Commits, 2)
	require.Equal(t, uint64(4), remainingCommits.Commits[0].CommitID)
	require.Equal(t, uint64(1), remainingCommits.Commits[1].CommitID)

	allIDs, err := store.ListAllDiskIDs(ctx)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{idA, idB, idC}, allIDs)

	require.NoError(t, store.ReplaceDiskSnapshot(ctx, projection.DiskSnapshot{
		Disk: projection.DiskRecord{
			DiskID:        idA,
			Name:          "alpha-reconciled",
			DefaultBranch: "main",
			MaxBytes:      16 * 1024 * 1024,
			UsedBytes:     8192,
			CreatedAt:     base,
			UpdatedAt:     base.Add(5 * time.Minute),
		},
		Branches: []projection.BranchRecord{
			{DiskID: idA, Name: "main", HeadCommitID: 10, WriterEpoch: 5, CreatedAt: base, UpdatedAt: base.Add(5 * time.Minute)},
		},
		Commits: []projection.CommitRecord{
			{DiskID: idA, CommitID: 10, ParentID: 0, Kind: projection.CommitKindInit, Message: "reconciled", CreatedAt: base.Add(5 * time.Minute)},
		},
		Mounts: []projection.MountRecord{
			{DiskID: idA, MountID: mount1, BranchName: "main", ReadOnly: false, WriterEpoch: 5, MountedAt: base.Add(5 * time.Minute), ExpiresAt: base.Add(10 * time.Minute)},
		},
	}))

	gotA, err = store.GetDisk(ctx, idA)
	require.NoError(t, err)
	require.Equal(t, "alpha-reconciled", gotA.Name)
	require.Equal(t, uint64(8192), gotA.UsedBytes)
	branches, err = store.ListBranches(ctx, idA)
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.Equal(t, uint64(10), branches[0].HeadCommitID)
	remainingCommits, err = store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA})
	require.NoError(t, err)
	require.Len(t, remainingCommits.Commits, 1)
	require.Equal(t, uint64(10), remainingCommits.Commits[0].CommitID)
	activeAll, err = store.ListMounts(ctx, idA, "", base.Add(6*time.Minute))
	require.NoError(t, err)
	require.Len(t, activeAll, 1)
	require.Equal(t, mount1, activeAll[0].MountID)

	require.NoError(t, store.DeleteDisk(ctx, idA))
	require.NoError(t, store.DeleteDisk(ctx, idA))
	_, err = store.GetDisk(ctx, idA)
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListBranches(ctx, idA)
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListCommits(ctx, projection.ListCommitsQuery{DiskID: idA})
	require.ErrorIs(t, err, projection.ErrNotFound)
	_, err = store.ListMounts(ctx, idA, "", base)
	require.ErrorIs(t, err, projection.ErrNotFound)

	allIDs, err = store.ListAllDiskIDs(ctx)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{idB, idC}, allIDs)
}

func TestStep15_ProjectionStore_FourTablesCRUDPaginationAndCascadeDelete(t *testing.T) {
	store := projection.NewMemStore()
	runStoreContractSuite(t, context.Background(), store)
}

func TestStep15_PGStore_IsolatedSchemaFourTablesAndReconcile(t *testing.T) {
	pool, ctx := setupProjectionTestDB(t)
	store := projection.NewPGStore(pool)
	runStoreContractSuite(t, ctx, store)
}

func TestProjectionStore_CursorAndUnavailableClassification(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 34, 56, 789123000, time.UTC)
	diskID := uuid.New()

	tok := projection.EncodeDiskPageCursor(now, diskID)
	gotTime, gotID, err := projection.DecodeDiskPageCursor(tok)
	require.NoError(t, err)
	require.Equal(t, now.Truncate(time.Microsecond), gotTime)
	require.Equal(t, diskID, gotID)

	_, _, err = projection.DecodeDiskPageCursor("not-valid-base64!!!")
	require.ErrorIs(t, err, projection.ErrInvalidArgument)

	cTok := projection.EncodeCommitPageCursor(42)
	gotCID, err := projection.DecodeCommitPageCursor(cTok)
	require.NoError(t, err)
	require.Equal(t, uint64(42), gotCID)

	_, err = projection.DecodeCommitPageCursor("e30")
	require.ErrorIs(t, err, projection.ErrInvalidArgument)

	require.False(t, projection.IsUnavailable(nil))
	require.False(t, projection.IsUnavailable(projection.ErrNotFound))
	require.False(t, projection.IsUnavailable(projection.ErrInvalidArgument))
	require.True(t, projection.IsUnavailable(projection.ErrUnavailable))
	require.True(t, projection.IsUnavailable(fmt.Errorf("wrap: %w", projection.ErrUnavailable)))
	require.True(t, projection.IsUnavailable(context.DeadlineExceeded))
	require.True(t, projection.IsUnavailable(context.Canceled))
	require.True(t, projection.IsUnavailable(io.EOF))
	require.True(t, projection.IsUnavailable(syscall.ECONNREFUSED))
	require.True(t, projection.IsUnavailable(&net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}))
	require.True(t, projection.IsUnavailable(&pgconn.PgError{Code: "57P01"}))
	require.True(t, projection.IsUnavailable(&pgconn.PgError{Code: "08006"}))
	require.False(t, projection.IsUnavailable(&pgconn.PgError{Code: "23505"}))

	ctx := context.Background()
	require.ErrorIs(t, projection.EnsureSchema(ctx, nil), projection.ErrUnavailable)
	nilPG := projection.NewPGStore(nil)
	_, err = nilPG.GetDisk(ctx, diskID)
	require.ErrorIs(t, err, projection.ErrUnavailable)
	_, err = nilPG.ListDisks(ctx, projection.ListDisksQuery{})
	require.ErrorIs(t, err, projection.ErrUnavailable)

	mem := projection.NewMemStore()
	_, err = mem.GetDisk(ctx, uuid.Nil)
	require.ErrorIs(t, err, projection.ErrInvalidArgument)
	_, err = mem.ListDisks(ctx, projection.ListDisksQuery{PageSize: projection.MaxPageSize + 1})
	require.ErrorIs(t, err, projection.ErrInvalidArgument)
	_, err = mem.ListCommits(ctx, projection.ListCommitsQuery{DiskID: diskID, Kind: "bogus"})
	require.ErrorIs(t, err, projection.ErrInvalidArgument)
	require.ErrorIs(t, mem.UpsertDisk(ctx, projection.DiskRecord{DiskID: diskID, DefaultBranch: "main", MaxBytes: 0}, nil, nil), projection.ErrInvalidArgument)

	require.NoError(t, mem.UpsertDisk(ctx, projection.DiskRecord{
		DiskID:        diskID,
		Name:          "atomic-mount-del",
		DefaultBranch: "main",
		MaxBytes:      1024,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil, nil))
	m1 := uuid.New()
	m2 := uuid.New()
	require.NoError(t, mem.UpsertMount(ctx, projection.MountRecord{
		DiskID:     diskID,
		MountID:    m1,
		BranchName: "main",
		MountedAt:  now,
		ExpiresAt:  now.Add(time.Hour),
	}, nil, nil))
	require.NoError(t, mem.UpsertMount(ctx, projection.MountRecord{
		DiskID:     diskID,
		MountID:    m2,
		BranchName: "main",
		MountedAt:  now,
		ExpiresAt:  now.Add(time.Hour),
	}, nil, []uuid.UUID{m1}))
	mounts, err := mem.ListMounts(ctx, diskID, "main", now)
	require.NoError(t, err)
	require.Len(t, mounts, 1)
	require.Equal(t, m2, mounts[0].MountID)

	require.NoError(t, mem.UpsertBranchAndCommit(ctx,
		[]projection.BranchRecord{{DiskID: diskID, Name: "main", HeadCommitID: 1, CreatedAt: now, UpdatedAt: now}},
		nil,
		[]uuid.UUID{m2},
	))
	mounts, err = mem.ListMounts(ctx, diskID, "main", now)
	require.NoError(t, err)
	require.Empty(t, mounts)
}

func TestPGTypeCodecs(t *testing.T) {
	m := pgtype.NewMap()
	id := uuid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)

	uuidEnc := m.PlanEncode(pgtype.UUIDOID, pgtype.BinaryFormatCode, id)
	require.NotNil(t, uuidEnc)
	rawUUID, err := uuidEnc.Encode(id, nil)
	require.NoError(t, err)

	var decodedID uuid.UUID
	uuidScan := m.PlanScan(pgtype.UUIDOID, pgtype.BinaryFormatCode, &decodedID)
	require.NotNil(t, uuidScan)
	require.NoError(t, uuidScan.Scan(rawUUID, &decodedID))
	require.Equal(t, id, decodedID)

	pgUUIDs := []pgtype.UUID{{Bytes: id, Valid: true}}
	require.NotNil(t, m.PlanEncode(pgtype.UUIDArrayOID, pgtype.BinaryFormatCode, pgUUIDs))
	require.NotNil(t, m.PlanEncode(pgtype.Int8ArrayOID, pgtype.BinaryFormatCode, []int64{1, 2}))
	require.NotNil(t, m.PlanEncode(pgtype.TimestamptzArrayOID, pgtype.BinaryFormatCode, []time.Time{now}))
	require.NotNil(t, m.PlanEncode(pgtype.TimestamptzOID, pgtype.BinaryFormatCode, now))
}
