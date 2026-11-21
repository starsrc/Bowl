package db_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

const testDefaultMaxBytes uint64 = 16 << 20

type seedManualSnapshotFn func(
	t *testing.T,
	ctx context.Context,
	rec db.SnapshotRecord,
	setBaseOnDiskID uuid.UUID,
)

type storeAdapter struct {
	name     string
	newStore func(t *testing.T) (db.Store, context.Context, seedManualSnapshotFn)
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func storeAdapters() []storeAdapter {
	return []storeAdapter{
		{
			name: "MemStore",
			newStore: func(t *testing.T) (db.Store, context.Context, seedManualSnapshotFn) {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(cancel)
				mem := db.NewMemStore()
				seeder := func(t *testing.T, _ context.Context, rec db.SnapshotRecord, setBaseOnDiskID uuid.UUID) {
					t.Helper()
					require.NoError(t, mem.SeedSnapshot(rec, setBaseOnDiskID))
				}
				return mem, ctx, seeder
			},
		},
		{
			name: "SQLStore",
			newStore: func(t *testing.T) (db.Store, context.Context, seedManualSnapshotFn) {
				t.Helper()
				pool, ctx := setupMetaTestDB(t)
				q := db.New(pool)
				seeder := func(t *testing.T, c context.Context, rec db.SnapshotRecord, setBaseOnDiskID uuid.UUID) {
					t.Helper()
					_, err := q.CreateDiskSnapshot(c, db.CreateDiskSnapshotParams{
						SnapshotID:            pgUUID(rec.SnapshotID),
						LineageID:             pgUUID(rec.LineageID),
						SourceDiskID:          pgUUID(rec.SourceDiskID),
						ParentSnapshotID:      pgUUID(rec.ParentSnapshotID),
						MergeParentSnapshotID: pgUUID(rec.MergeParentSnapshotID),
						Kind:                  string(rec.Kind),
						Name:                  rec.Name,
					})
					require.NoError(t, err)
					if setBaseOnDiskID != uuid.Nil {
						require.NoError(t, q.SetDiskBaseSnapshot(c, db.SetDiskBaseSnapshotParams{
							DiskID:         pgUUID(setBaseOnDiskID),
							BaseSnapshotID: pgUUID(rec.SnapshotID),
						}))
					}
				}
				return db.NewStore(pool), ctx, seeder
			},
		},
	}
}

func runStoreAdapters(t *testing.T, fn func(t *testing.T, s db.Store, ctx context.Context)) {
	t.Helper()
	for _, adapter := range storeAdapters() {
		t.Run(adapter.name, func(t *testing.T) {
			s, ctx, _ := adapter.newStore(t)
			fn(t, s, ctx)
		})
	}
}

func runStoreAdaptersWithSeeder(t *testing.T, fn func(t *testing.T, s db.Store, ctx context.Context, seed seedManualSnapshotFn)) {
	t.Helper()
	for _, adapter := range storeAdapters() {
		t.Run(adapter.name, func(t *testing.T) {
			s, ctx, seeder := adapter.newStore(t)
			fn(t, s, ctx, seeder)
		})
	}
}

func runSubtests[T any](t *testing.T, cases []T, nameFn func(T) string, fn func(*testing.T, T)) {
	t.Helper()
	for _, tc := range cases {
		t.Run(nameFn(tc), func(t *testing.T) {
			fn(t, tc)
		})
	}
}

func mustNewUUIDv7(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err, "generate UUIDv7 failed")
	return id
}

func createTestDisks(t *testing.T, ctx context.Context, s db.Store, names ...string) []db.DiskRecord {
	t.Helper()
	created := make([]db.DiskRecord, 0, len(names))
	for _, name := range names {
		rec, err := s.CreateDisk(ctx, mustNewUUIDv7(t), name, testDefaultMaxBytes)
		require.NoError(t, err, "create disk %s failed", name)
		created = append(created, rec)
	}
	return created
}

func requireOrderedDesc(t *testing.T, disks []db.DiskRecord) {
	t.Helper()
	for i := 1; i < len(disks); i++ {
		prev, curr := disks[i-1], disks[i]
		if prev.CreatedAt.Equal(curr.CreatedAt) {
			require.Positive(t, bytes.Compare(prev.DiskID[:], curr.DiskID[:]),
				"same timestamp must sort by DiskID desc: prev=%s curr=%s", prev.DiskID, curr.DiskID)
			continue
		}
		require.True(t, prev.CreatedAt.After(curr.CreatedAt),
			"disks must sort by CreatedAt desc: prev=%s curr=%s", prev.CreatedAt, curr.CreatedAt)
	}
}

func collectDiskIDs(disks []db.DiskRecord) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(disks))
	for i := range disks {
		ids = append(ids, disks[i].DiskID)
	}
	return ids
}

func collectSnapshotIDs(snaps []db.SnapshotRecord) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(snaps))
	for i := range snaps {
		ids = append(ids, snaps[i].SnapshotID)
	}
	return ids
}

func runConcurrentSegmentReserves(t *testing.T, ctx context.Context, s db.Store, kind db.SegmentKind, lineageID uuid.UUID, workers int, count int64) []db.SegmentRange {
	t.Helper()
	start := make(chan struct{})
	results := make([]db.SegmentRange, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = s.ReserveSegment(ctx, kind, lineageID, 2, count)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "concurrent reserve segment worker=%d failed", i)
	}
	return results
}

func requireDisjointSortedSegments(t *testing.T, segs []db.SegmentRange, wantCount int64) {
	t.Helper()
	ordered := slices.Clone(segs)
	slices.SortFunc(ordered, func(a, b db.SegmentRange) int {
		return int(a.First - b.First)
	})
	for i, seg := range ordered {
		require.Equal(t, wantCount, seg.Last-seg.First+1, "segment length want=%d, got=%d", wantCount, seg.Last-seg.First+1)
		if i > 0 {
			require.Greater(t, seg.First, ordered[i-1].Last, "concurrent segments overlapped: prev=%+v curr=%+v", ordered[i-1], seg)
		}
	}
}

func runConcurrentForkAndDelete(t *testing.T, ctx context.Context, s db.Store, sourceID uuid.UUID) (forkErr, delErr error) {
	t.Helper()
	childID := mustNewUUIDv7(t)
	snapID := mustNewUUIDv7(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, forkErr = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: sourceID,
			ChildDiskID:  childID,
			SnapshotID:   snapID,
			Name:         "race-child",
		})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, delErr = s.DeleteDisk(ctx, sourceID, false)
	}()
	close(start)
	wg.Wait()
	return forkErr, delErr
}

func runConcurrentForkDeleteRounds(t *testing.T, ctx context.Context, s db.Store, rounds int) {
	t.Helper()
	for i := range rounds {
		rootID := mustNewUUIDv7(t)
		_, err := s.CreateDisk(ctx, rootID, fmt.Sprintf("race-root-%d", i), testDefaultMaxBytes)
		require.NoError(t, err)

		forkErr, delErr := runConcurrentForkAndDelete(t, ctx, s, rootID)
		require.NoError(t, delErr, "DeleteDisk under concurrent ForkDisk failed (round=%d)", i)
		require.Condition(t, func() bool {
			return forkErr == nil || errors.Is(forkErr, db.ErrNotFound)
		}, "concurrent ForkDisk must succeed or return ErrNotFound (round=%d), got=%v", i, forkErr)
	}
}

func TestStoreContract_GivenValidDisk_WhenCreatedUpdatedRead_ThenPersistsEndState(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		diskID := mustNewUUIDv7(t)

		created, err := s.CreateDisk(ctx, diskID, "alpha", 8<<20)
		require.NoError(t, err)
		require.Equal(t, diskID, created.DiskID)
		require.Equal(t, uuid.Nil, created.ParentDiskID)
		require.Equal(t, diskID, created.LineageID)
		require.Equal(t, uuid.Nil, created.BaseSnapshotID)
		require.Equal(t, "alpha", created.Name)
		require.Equal(t, uint64(8<<20), created.MaxBytes)
		require.Equal(t, uint64(0), created.UsedBytes)
		require.Equal(t, uint64(1), created.UsedInodes)
		require.False(t, created.CreatedAt.IsZero())
		require.Equal(t, created.CreatedAt.Truncate(time.Microsecond), created.CreatedAt)

		got, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, created, got)

		renamedName := "alpha-renamed"
		updated, err := s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: diskID, Name: &renamedName})
		require.NoError(t, err)
		require.Equal(t, "alpha-renamed", updated.Name)
		require.Equal(t, uint64(8<<20), updated.MaxBytes)
		require.Equal(t, created.CreatedAt, updated.CreatedAt)
		require.False(t, updated.UpdatedAt.Before(created.UpdatedAt))

		newQuota := uint64(32 << 20)
		resized, err := s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: diskID, MaxBytes: &newQuota})
		require.NoError(t, err)
		require.Equal(t, "alpha-renamed", resized.Name)
		require.Equal(t, newQuota, resized.MaxBytes)

		require.NoError(t, s.UpdateDiskUsage(ctx, diskID, 4096, 5))
		reloaded, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, uint64(4096), reloaded.UsedBytes)
		require.Equal(t, uint64(5), reloaded.UsedInodes)
		require.Equal(t, resized.UpdatedAt, reloaded.UpdatedAt)

		_, dupErr := s.CreateDisk(ctx, diskID, "alpha-dup", 8<<20)
		require.Error(t, dupErr)
		afterDup, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, "alpha-renamed", afterDup.Name)
	})
}

func TestStoreContract_GivenInvalidInputsOrMissingTargets_WhenCalled_ThenFailsFastWithSentinelErrors(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		validID := mustNewUUIDv7(t)
		badName := "bad"
		zeroQuota := uint64(0)
		overflowQuota := uint64(math.MaxInt64) + 1

		_, err := s.CreateDisk(ctx, uuid.Nil, "bad", testDefaultMaxBytes)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateDisk(ctx, validID, "bad", 0)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateDisk(ctx, validID, "bad", overflowQuota)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.GetDisk(ctx, uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ListLineageDisks(ctx, uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: uuid.Nil, Name: &badName})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: validID})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: validID, MaxBytes: &zeroQuota})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: validID, MaxBytes: &overflowQuota})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		err = s.UpdateDiskUsage(ctx, uuid.Nil, 0, 1)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		err = s.UpdateDiskUsage(ctx, validID, 0, 0)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		err = s.UpdateDiskUsage(ctx, validID, overflowQuota, 1)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		err = s.UpdateDiskUsage(ctx, validID, 0, overflowQuota)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.DeleteDisk(ctx, uuid.Nil, false)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.DeleteDisk(ctx, uuid.Nil, true)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: validID, ChildDiskID: validID, SnapshotID: mustNewUUIDv7(t), Name: "self"})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, uuid.Nil, 2, 10)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, validID, 2, 0)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, validID, 2, -5)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKind(99), validID, 2, 10)
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		missingID := mustNewUUIDv7(t)
		missingName := "missing"
		_, err = s.GetDisk(ctx, missingID)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: missingID, Name: &missingName})
		require.ErrorIs(t, err, db.ErrNotFound)
		err = s.UpdateDiskUsage(ctx, missingID, 128, 2)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.DeleteDisk(ctx, missingID, false)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.DeleteDisk(ctx, missingID, true)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: missingID, ChildDiskID: mustNewUUIDv7(t), SnapshotID: mustNewUUIDv7(t), Name: "child"})
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, missingID, 2, 10)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindPack, missingID, 1, 10)
		require.ErrorIs(t, err, db.ErrNotFound)
	})
}

func TestStoreContract_GivenMalformedPageTokens_WhenListDisksCalled_ThenReturnsErrInvalidArgument(t *testing.T) {
	type malformedTokenCase struct {
		name  string
		token string
	}
	badTokens := []malformedTokenCase{
		{name: "invalid_base64", token: "not-valid-base64!!!"},
		{name: "empty_json_object", token: base64.RawURLEncoding.EncodeToString([]byte(`{}`))},
		{name: "unknown_json_field", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"11111111-1111-7111-8111-111111111111","extra":1}`))},
		{name: "nil_uuid_in_token", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"00000000-0000-0000-0000-000000000000"}`))},
		{name: "invalid_uuid_in_token", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"not-a-uuid"}`))},
		{name: "trailing_json_bytes", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"11111111-1111-7111-8111-111111111111"}{}`))},
	}

	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		runSubtests(t, badTokens, func(tc malformedTokenCase) string { return tc.name }, func(t *testing.T, tc malformedTokenCase) {
			_, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 10, PageToken: tc.token})
			require.ErrorIs(t, err, db.ErrInvalidArgument, "malformed token %s must fail", tc.name)
		})
	})
}

func TestStoreContract_GivenFiveDisks_WhenListedWithCursorPagination_ThenReturnsStableDescPagesWithoutGaps(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		emptyPage, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 0})
		require.NoError(t, err)
		require.Empty(t, emptyPage.Disks)
		require.Empty(t, emptyPage.NextPageToken)

		created := createTestDisks(t, ctx, s, "d0", "d1", "d2", "d3", "d4")

		fullPage, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 0})
		require.NoError(t, err)
		require.Len(t, fullPage.Disks, 5)
		require.Empty(t, fullPage.NextPageToken)
		requireOrderedDesc(t, fullPage.Disks)

		page1, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, page1.Disks, 2)
		require.NotEmpty(t, page1.NextPageToken)

		page2, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2, PageToken: page1.NextPageToken})
		require.NoError(t, err)
		require.Len(t, page2.Disks, 2)
		require.NotEmpty(t, page2.NextPageToken)

		page3, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2, PageToken: page2.NextPageToken})
		require.NoError(t, err)
		require.Len(t, page3.Disks, 1)
		require.Empty(t, page3.NextPageToken)

		allPaged := slices.Concat(page1.Disks, page2.Disks, page3.Disks)
		requireOrderedDesc(t, allPaged)
		require.ElementsMatch(t, collectDiskIDs(created), collectDiskIDs(allPaged))
	})
}

func TestStoreContract_GivenForkedLineage_WhenDeletedInTriStateOrder_ThenReclaimsSnapshotsAndLineage(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)

		c1ID, s1ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c1, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c1ID,
			SnapshotID:   s1ID,
			Name:         "child-1",
		})
		require.NoError(t, err)
		require.Equal(t, rootID, c1.ParentDiskID)
		require.Equal(t, root.LineageID, c1.LineageID)

		c2ID, s2ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c2, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c2ID,
			SnapshotID:   s2ID,
			Name:         "child-2",
		})
		require.NoError(t, err)
		require.Equal(t, root.LineageID, c2.LineageID)

		_, dupChildErr := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c1ID,
			SnapshotID:   mustNewUUIDv7(t),
			Name:         "dup-child",
		})
		require.Error(t, dupChildErr)
		_, dupSnapErr := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  mustNewUUIDv7(t),
			SnapshotID:   s1ID,
			Name:         "dup-snap",
		})
		require.Error(t, dupSnapErr)

		delRoot, err := s.DeleteDisk(ctx, rootID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{rootID}, delRoot.DeletedDiskIDs)
		require.Empty(t, delRoot.ReclaimedSnapshotIDs, "all snapshots remain reachable when remaining==2")
		_, err = s.GetDisk(ctx, rootID)
		require.ErrorIs(t, err, db.ErrNotFound)

		delC2, err := s.DeleteDisk(ctx, c2ID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{c2ID}, delC2.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{s1ID, s2ID}, delC2.ReclaimedSnapshotIDs)

		c3ID, s3ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c3, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: c1ID,
			ChildDiskID:  c3ID,
			SnapshotID:   s3ID,
			Name:         "child-3",
		})
		require.NoError(t, err)
		require.Equal(t, c1ID, c3.ParentDiskID)
		require.Equal(t, root.LineageID, c3.LineageID)

		delC3, err := s.DeleteDisk(ctx, c3ID, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{s3ID}, delC3.ReclaimedSnapshotIDs)

		delC1, err := s.DeleteDisk(ctx, c1ID, false)
		require.NoError(t, err)
		require.Empty(t, delC1.ReclaimedSnapshotIDs)
		_, err = s.GetDisk(ctx, c1ID)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 2, 10)
		require.ErrorIs(t, err, db.ErrNotFound)
	})
}

func TestStoreContract_GivenLineageTree_WhenDeleteDiskRecursiveAndIndependent_ThenReclaimsUnreachableAndDeletesSubtree(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)

		bID, sB := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  bID,
			SnapshotID:   sB,
			Name:         "branch-b",
		})
		require.NoError(t, err)

		cID, sC := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  cID,
			SnapshotID:   sC,
			Name:         "branch-c",
		})
		require.NoError(t, err)

		b1ID, sB1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: bID,
			ChildDiskID:  b1ID,
			SnapshotID:   sB1,
			Name:         "branch-b1",
		})
		require.NoError(t, err)

		b2ID, sB2 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: b1ID,
			ChildDiskID:  b2ID,
			SnapshotID:   sB2,
			Name:         "branch-b2",
		})
		require.NoError(t, err)

		listed, err := s.ListLineageDisks(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{rootID, bID, cID, b1ID, b2ID}, collectDiskIDs(listed))

		recRes, err := s.DeleteDisk(ctx, bID, true)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{bID, b1ID, b2ID}, recRes.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sB1, sB2}, recRes.ReclaimedSnapshotIDs,
			"remaining==2 (root,c) must reclaim unreachable fork snapshots sB1,sB2 while preserving reachable sB,sC")

		survivingDisks, err := s.ListLineageDisks(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{rootID, cID}, collectDiskIDs(survivingDisks))

		survivingSnaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sB, sC}, collectSnapshotIDs(survivingSnaps))

		delRoot, err := s.DeleteDisk(ctx, rootID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{rootID}, delRoot.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sB, sC}, delRoot.ReclaimedSnapshotIDs)

		gotC, err := s.GetDisk(ctx, cID)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, gotC.BaseSnapshotID)
	})
}

func TestStoreContract_GivenManualSnapshotsAcrossAllTiers_WhenDeleteDiskCalled_ThenStrictlyPreservesManualAndGuardsLineage(t *testing.T) {
	runStoreAdaptersWithSeeder(t, func(t *testing.T, s db.Store, ctx context.Context, seed seedManualSnapshotFn) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)

		d1ID, sFork1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  d1ID,
			SnapshotID:   sFork1,
			Name:         "d1",
		})
		require.NoError(t, err)

		d2ID, sFork2 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: d1ID,
			ChildDiskID:  d2ID,
			SnapshotID:   sFork2,
			Name:         "d2",
		})
		require.NoError(t, err)

		d3ID, sFork3 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  d3ID,
			SnapshotID:   sFork3,
			Name:         "d3",
		})
		require.NoError(t, err)

		sManualOnDeadBranch := mustNewUUIDv7(t)
		seed(t, ctx, db.SnapshotRecord{
			LineageID:        root.LineageID,
			SnapshotID:       sManualOnDeadBranch,
			SourceDiskID:     d2ID,
			ParentSnapshotID: sFork2,
			Kind:             db.SnapshotKindManual,
		}, uuid.Nil)

		sManualOnRoot := mustNewUUIDv7(t)
		seed(t, ctx, db.SnapshotRecord{
			LineageID:        root.LineageID,
			SnapshotID:       sManualOnRoot,
			SourceDiskID:     rootID,
			ParentSnapshotID: sFork3,
			Kind:             db.SnapshotKindManual,
		}, rootID)

		resTier2, err := s.DeleteDisk(ctx, d1ID, true)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{d1ID, d2ID}, resTier2.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sFork2, sManualOnDeadBranch}, resTier2.ReclaimedSnapshotIDs,
			"tier remaining>=2 must reclaim unreachable sFork2 and ownerless sManualOnDeadBranch while keeping root's manual snapshot")

		snapsAfterTier2, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		snapByID := make(map[uuid.UUID]db.SnapshotRecord, len(snapsAfterTier2))
		for _, snap := range snapsAfterTier2 {
			snapByID[snap.SnapshotID] = snap
		}
		require.NotContains(t, snapByID, sManualOnDeadBranch)
		require.Contains(t, snapByID, sManualOnRoot)
		require.Contains(t, snapByID, sFork1)
		require.Contains(t, snapByID, sFork3)

		resTier1, err := s.DeleteDisk(ctx, d3ID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{d3ID}, resTier1.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sFork1, sFork3}, resTier1.ReclaimedSnapshotIDs,
			"tier remaining==1 must reclaim all fork/merge snapshots and preserve manual snapshots owned by surviving disks")

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, sManualOnRoot, gotRoot.BaseSnapshotID,
			"sole surviving disk whose base_snapshot_id is manual must keep it intact")

		snapsAfterTier1, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sManualOnRoot}, collectSnapshotIDs(snapsAfterTier1))
		require.Equal(t, db.SnapshotKindManual, snapsAfterTier1[0].Kind)
		require.Equal(t, uuid.Nil, snapsAfterTier1[0].ParentSnapshotID)
		require.Equal(t, uuid.Nil, snapsAfterTier1[0].MergeParentSnapshotID)

		resTier0, err := s.DeleteDisk(ctx, rootID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{rootID}, resTier0.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sManualOnRoot}, resTier0.ReclaimedSnapshotIDs)

		snapsAfterTier0, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Empty(t, snapsAfterTier0, "tier remaining==0 must reclaim root's manual snapshot with no ownerless leftovers")

		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 2, 1)
		require.ErrorIs(t, err, db.ErrNotFound, "lineage row must be deleted when remaining_disks == 0")
	})
}

func TestStoreContract_GivenFastForwardMerge_WhenExecuted_ThenPersistsSingleOrZeroMergeSnapshotsAndNeverAutoDeletes(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "main", testDefaultMaxBytes)
		require.NoError(t, err)

		branchID, sFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  branchID,
			SnapshotID:   sFork,
			Name:         "feat",
		})
		require.NoError(t, err)

		sFF1 := mustNewUUIDv7(t)
		updated1, err := s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 branchID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sFork,
			ExpectedTargetBaseSnapshotID: sFork,
			NewSourceSnapshotID:          sFF1,
			NewMergeSnapshotID:           sFF1,
			SkipSourceSnapshot:           false,
			FastForward:                  true,
			TargetMaxBytes:               32 << 20,
			TargetUsedBytes:              4096,
			TargetUsedInodes:             3,
		})
		require.NoError(t, err)
		require.Equal(t, sFF1, updated1.BaseSnapshotID)
		require.Equal(t, uint64(32<<20), updated1.MaxBytes)
		require.Equal(t, uint64(4096), updated1.UsedBytes)
		require.Equal(t, uint64(3), updated1.UsedInodes)

		gotBranch, err := s.GetDisk(ctx, branchID)
		require.NoError(t, err)
		require.Equal(t, sFF1, gotBranch.BaseSnapshotID)

		snaps1, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snaps1, 2)
		require.Equal(t, db.SnapshotKindFork, snaps1[0].Kind)
		require.Equal(t, sFF1, snaps1[1].SnapshotID)
		require.Equal(t, db.SnapshotKindMerge, snaps1[1].Kind)
		require.Equal(t, sFork, snaps1[1].ParentSnapshotID)
		require.Equal(t, uuid.Nil, snaps1[1].MergeParentSnapshotID)
		require.Equal(t, branchID, snaps1[1].SourceDiskID)

		branch2ID, sFork2 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: branchID,
			ChildDiskID:  branch2ID,
			SnapshotID:   sFork2,
			Name:         "feat-sub",
		})
		require.NoError(t, err)

		updated2, err := s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 branchID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sFork2,
			ExpectedTargetBaseSnapshotID: sFF1,
			NewSourceSnapshotID:          uuid.Nil,
			NewMergeSnapshotID:           sFork2,
			SkipSourceSnapshot:           true,
			FastForward:                  true,
			TargetMaxBytes:               32 << 20,
			TargetUsedBytes:              8192,
			TargetUsedInodes:             4,
		})
		require.NoError(t, err)
		require.Equal(t, sFork2, updated2.BaseSnapshotID)

		snaps2, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snaps2, 3, "fast-forward reuse must create 0 new snapshots and never auto-delete prior snapshots")
	})
}

func TestStoreContract_GivenSharedAndIsolatedLineages_WhenReserveSegmentCalled_ThenMonotonicAndDisjoint(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "seg-root", testDefaultMaxBytes)
		require.NoError(t, err)

		otherID := mustNewUUIDv7(t)
		other, err := s.CreateDisk(ctx, otherID, "seg-other", testDefaultMaxBytes)
		require.NoError(t, err)

		r1, err := s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 1, 100)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 2, Last: 101}, r1)

		r2, err := s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 500, 50)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 500, Last: 549}, r2)

		p1, err := s.ReserveSegment(ctx, db.SegmentKindPack, root.LineageID, 0, 64)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 1, Last: 64}, p1)

		p2, err := s.ReserveSegment(ctx, db.SegmentKindPack, root.LineageID, 1, 64)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 65, Last: 128}, p2)

		child, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  mustNewUUIDv7(t),
			SnapshotID:   mustNewUUIDv7(t),
			Name:         "seg-child",
		})
		require.NoError(t, err)

		r3, err := s.ReserveSegment(ctx, db.SegmentKindInode, child.LineageID, 2, 10)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 550, Last: 559}, r3)

		otherIno, err := s.ReserveSegment(ctx, db.SegmentKindInode, other.LineageID, 2, 5)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 2, Last: 6}, otherIno)

		concurrentSegs := runConcurrentSegmentReserves(t, ctx, s, db.SegmentKindInode, other.LineageID, 8, 10)
		requireDisjointSortedSegments(t, concurrentSegs, 10)
	})
}

func TestStoreContract_GivenConcurrentForkDelete_WhenExecuted_ThenConsistentWithoutDeadlock(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		runConcurrentForkDeleteRounds(t, ctx, s, 5)
	})
}

func TestStoreContract_GivenForkedDisks_WhenInspectedAndListed_ThenPopulatesBaseSnapshotIDAndLineageSnapshots(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		_, err := s.ListLineageSnapshots(ctx, uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, root.BaseSnapshotID)

		emptySnaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Empty(t, emptySnaps)

		otherID := mustNewUUIDv7(t)
		other, err := s.CreateDisk(ctx, otherID, "other", testDefaultMaxBytes)
		require.NoError(t, err)
		otherChildID, otherSnapID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: otherID,
			ChildDiskID:  otherChildID,
			SnapshotID:   otherSnapID,
			Name:         "other-child",
		})
		require.NoError(t, err)

		c1ID, s1ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c1, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c1ID,
			SnapshotID:   s1ID,
			Name:         "child-1",
		})
		require.NoError(t, err)
		require.Equal(t, s1ID, c1.BaseSnapshotID)

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, s1ID, gotRoot.BaseSnapshotID)

		gotC1, err := s.GetDisk(ctx, c1ID)
		require.NoError(t, err)
		require.Equal(t, s1ID, gotC1.BaseSnapshotID)

		child1Renamed := "child-1-renamed"
		renamedC1, err := s.UpdateDisk(ctx, db.UpdateDiskInput{DiskID: c1ID, Name: &child1Renamed})
		require.NoError(t, err)
		require.Equal(t, s1ID, renamedC1.BaseSnapshotID)

		c2ID, s2ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c2ID,
			SnapshotID:   s2ID,
			Name:         "child-2",
		})
		require.NoError(t, err)

		page, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 10})
		require.NoError(t, err)
		byID := make(map[uuid.UUID]db.DiskRecord, len(page.Disks))
		for i := range page.Disks {
			byID[page.Disks[i].DiskID] = page.Disks[i]
		}
		require.Equal(t, s2ID, byID[rootID].BaseSnapshotID)
		require.Equal(t, s1ID, byID[c1ID].BaseSnapshotID)
		require.Equal(t, s2ID, byID[c2ID].BaseSnapshotID)

		snaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snaps, 2)
		require.Equal(t, s1ID, snaps[0].SnapshotID)
		require.Equal(t, root.LineageID, snaps[0].LineageID)
		require.Equal(t, uuid.Nil, snaps[0].ParentSnapshotID)
		require.Equal(t, uuid.Nil, snaps[0].MergeParentSnapshotID)
		require.Equal(t, rootID, snaps[0].SourceDiskID)
		require.Equal(t, db.SnapshotKindFork, snaps[0].Kind)
		require.False(t, snaps[0].CreatedAt.IsZero())

		require.Equal(t, s2ID, snaps[1].SnapshotID)
		require.Equal(t, s1ID, snaps[1].ParentSnapshotID)
		require.Equal(t, uuid.Nil, snaps[1].MergeParentSnapshotID)
		require.Equal(t, rootID, snaps[1].SourceDiskID)
		require.Equal(t, db.SnapshotKindFork, snaps[1].Kind)

		otherSnaps, err := s.ListLineageSnapshots(ctx, other.LineageID)
		require.NoError(t, err)
		require.Len(t, otherSnaps, 1)
		require.Equal(t, otherSnapID, otherSnaps[0].SnapshotID)
	})
}

func TestStoreContract_GivenSameLineageDisks_WhenMergeDiskCalled_ThenUpdatesDAGAndAdvancesBaseSnapshots(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "main", testDefaultMaxBytes)
		require.NoError(t, err)

		branchID, snapFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  branchID,
			SnapshotID:   snapFork,
			Name:         "feature",
		})
		require.NoError(t, err)

		snapSrc1, snapMerge1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		merged1, err := s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 branchID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          snapSrc1,
			NewMergeSnapshotID:           snapMerge1,
			SkipSourceSnapshot:           false,
		})
		require.NoError(t, err)
		require.Equal(t, rootID, merged1.DiskID)
		require.Equal(t, snapMerge1, merged1.BaseSnapshotID)
		require.False(t, merged1.UpdatedAt.Before(root.UpdatedAt))

		gotBranch, err := s.GetDisk(ctx, branchID)
		require.NoError(t, err)
		require.Equal(t, snapSrc1, gotBranch.BaseSnapshotID)

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, snapMerge1, gotRoot.BaseSnapshotID)

		snaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snaps, 3)
		require.Equal(t, snapFork, snaps[0].SnapshotID)
		require.Equal(t, db.SnapshotKindFork, snaps[0].Kind)

		require.Equal(t, snapSrc1, snaps[1].SnapshotID)
		require.Equal(t, snapFork, snaps[1].ParentSnapshotID)
		require.Equal(t, uuid.Nil, snaps[1].MergeParentSnapshotID)
		require.Equal(t, branchID, snaps[1].SourceDiskID)
		require.Equal(t, db.SnapshotKindMerge, snaps[1].Kind)

		require.Equal(t, snapMerge1, snaps[2].SnapshotID)
		require.Equal(t, snapFork, snaps[2].ParentSnapshotID)
		require.Equal(t, snapSrc1, snaps[2].MergeParentSnapshotID)
		require.Equal(t, rootID, snaps[2].SourceDiskID)
		require.Equal(t, db.SnapshotKindMerge, snaps[2].Kind)

		snapMerge2 := mustNewUUIDv7(t)
		merged2, err := s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 branchID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapSrc1,
			ExpectedTargetBaseSnapshotID: snapMerge1,
			NewMergeSnapshotID:           snapMerge2,
			SkipSourceSnapshot:           true,
		})
		require.NoError(t, err)
		require.Equal(t, snapMerge2, merged2.BaseSnapshotID)

		gotBranchAfterSkip, err := s.GetDisk(ctx, branchID)
		require.NoError(t, err)
		require.Equal(t, snapSrc1, gotBranchAfterSkip.BaseSnapshotID)

		snapsAfterSkip, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snapsAfterSkip, 4)
		lastSnap := snapsAfterSkip[3]
		require.Equal(t, snapMerge2, lastSnap.SnapshotID)
		require.Equal(t, snapMerge1, lastSnap.ParentSnapshotID)
		require.Equal(t, snapSrc1, lastSnap.MergeParentSnapshotID)
		require.Equal(t, rootID, lastSnap.SourceDiskID)
		require.Equal(t, db.SnapshotKindMerge, lastSnap.Kind)

		snapMerge3 := uuid.MustParse("01900000-0000-7000-8000-000000000001")
		snapSrc3 := uuid.MustParse("01900000-0000-7000-8000-000000000002")
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 branchID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapSrc1,
			ExpectedTargetBaseSnapshotID: snapMerge2,
			NewSourceSnapshotID:          snapSrc3,
			NewMergeSnapshotID:           snapMerge3,
			SkipSourceSnapshot:           false,
		})
		require.NoError(t, err)

		snapsAfterNonMono, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snapsAfterNonMono, 6)
		require.Equal(t, snapSrc3, snapsAfterNonMono[4].SnapshotID)
		require.Equal(t, snapMerge3, snapsAfterNonMono[5].SnapshotID)
	})
}

func TestStoreContract_GivenInvalidOrConflictingMergeInputs_WhenMergeDiskCalled_ThenFailsAtomicallyWithoutSideEffects(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)

		childID, snapFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  childID,
			SnapshotID:   snapFork,
			Name:         "child",
		})
		require.NoError(t, err)

		otherID := mustNewUUIDv7(t)
		_, err = s.CreateDisk(ctx, otherID, "other", testDefaultMaxBytes)
		require.NoError(t, err)
		otherChildID, otherForkSnap := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: otherID,
			ChildDiskID:  otherChildID,
			SnapshotID:   otherForkSnap,
			Name:         "other-child",
		})
		require.NoError(t, err)

		_, err = s.MergeDisk(ctx, nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 rootID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: uuid.Nil,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewMergeSnapshotID:           mustNewUUIDv7(t),
			SkipSourceSnapshot:           true,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: uuid.Nil,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
			SkipSourceSnapshot:           false,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewMergeSnapshotID:           mustNewUUIDv7(t),
			SkipSourceSnapshot:           false,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		sameSnap := mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          sameSnap,
			NewMergeSnapshotID:           sameSnap,
			SkipSourceSnapshot:           false,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
			FastForward:                  true,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          uuid.Nil,
			NewMergeSnapshotID:           mustNewUUIDv7(t),
			SkipSourceSnapshot:           true,
			FastForward:                  true,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 otherID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: otherForkSnap,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrCrossLineage)

		missingID := mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 missingID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrDiskNotFound)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 missingID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrDiskNotFound)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    missingID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrDiskNotFound)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: mustNewUUIDv7(t),
			ExpectedTargetBaseSnapshotID: snapFork,
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrStaleBaseSnapshot)

		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: mustNewUUIDv7(t),
			NewSourceSnapshotID:          mustNewUUIDv7(t),
			NewMergeSnapshotID:           mustNewUUIDv7(t),
		})
		require.ErrorIs(t, err, db.ErrStaleBaseSnapshot)

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, snapFork, gotRoot.BaseSnapshotID)
		gotChild, err := s.GetDisk(ctx, childID)
		require.NoError(t, err)
		require.Equal(t, snapFork, gotChild.BaseSnapshotID)
		snaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		require.Equal(t, snapFork, snaps[0].SnapshotID)
	})
}

func TestStoreContract_GivenForkAndMergeWithQuotaAndUsage_WhenExecuted_ThenPersistsQuotaAndUsage(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", 16<<20)
		require.NoError(t, err)
		require.NoError(t, s.UpdateDiskUsage(ctx, rootID, 8192, 4))

		childID, snapFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		child, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  childID,
			SnapshotID:   snapFork,
			Name:         "child-explicit",
			MaxBytes:     32 << 20,
			UsedBytes:    4096,
			UsedInodes:   3,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(32<<20), child.MaxBytes)
		require.Equal(t, uint64(4096), child.UsedBytes)
		require.Equal(t, uint64(3), child.UsedInodes)

		inheritedID, snapFork2 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		inherited, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  inheritedID,
			SnapshotID:   snapFork2,
			Name:         "child-inherited",
		})
		require.NoError(t, err)
		require.Equal(t, uint64(16<<20), inherited.MaxBytes)
		require.Equal(t, uint64(8192), inherited.UsedBytes)
		require.Equal(t, uint64(4), inherited.UsedInodes)

		snapSrc, snapMerge := mustNewUUIDv7(t), mustNewUUIDv7(t)
		merged, err := s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 childID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: snapFork,
			ExpectedTargetBaseSnapshotID: snapFork2,
			NewSourceSnapshotID:          snapSrc,
			NewMergeSnapshotID:           snapMerge,
			TargetMaxBytes:               64 << 20,
			TargetUsedBytes:              12288,
			TargetUsedInodes:             6,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(64<<20), merged.MaxBytes)
		require.Equal(t, uint64(12288), merged.UsedBytes)
		require.Equal(t, uint64(6), merged.UsedInodes)

		reloaded, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, merged, reloaded)
	})
}

func TestStoreContract_GivenMountLeases_WhenCreatedRenewedListedDeletedPrunedAndCascaded_ThenEnforcesStrictContract(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		t0 := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
		diskID := mustNewUUIDv7(t)
		_, err := s.CreateDisk(ctx, diskID, "mount-disk", 16<<20)
		require.NoError(t, err)

		missingDiskID := mustNewUUIDv7(t)
		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    missingDiskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0,
			ExpiresAt: t0.Add(30 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrNotFound)

		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     1,
			Now:       t0,
			ExpiresAt: t0.Add(30 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  false,
			Epoch:     0,
			Now:       t0,
			ExpiresAt: t0.Add(30 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0,
			ExpiresAt: t0,
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		ro1ID := uuid.MustParse("01900000-0000-7000-8000-000000000020")
		rw1ID := uuid.MustParse("01900000-0000-7000-8000-000000000010")
		ro1, err := s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   ro1ID,
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0,
			ExpiresAt: t0.Add(30 * time.Second),
		})
		require.NoError(t, err)
		require.Equal(t, db.MountLeaseRecord{
			DiskID:    diskID,
			MountID:   ro1ID,
			ReadOnly:  true,
			Epoch:     0,
			ExpiresAt: t0.Add(30 * time.Second),
		}, ro1)

		rw1, err := s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw1ID,
			ReadOnly:  false,
			Epoch:     1,
			Now:       t0,
			ExpiresAt: t0.Add(30 * time.Second),
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), rw1.Epoch)

		active, err := s.ListActiveMountLeases(ctx, diskID, t0)
		require.NoError(t, err)
		require.Equal(t, []db.MountLeaseRecord{rw1, ro1}, active)

		_, err = s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw1ID,
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0.Add(5 * time.Second),
			ExpiresAt: t0.Add(35 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		_, err = s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw1ID,
			ReadOnly:  false,
			Epoch:     2,
			Now:       t0.Add(5 * time.Second),
			ExpiresAt: t0.Add(35 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrStaleEpoch)

		renewedRW1, err := s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw1ID,
			ReadOnly:  false,
			Epoch:     1,
			Now:       t0.Add(10 * time.Second),
			ExpiresAt: t0.Add(40 * time.Second),
		})
		require.NoError(t, err)
		require.Equal(t, t0.Add(40*time.Second), renewedRW1.ExpiresAt)

		rw2ID := uuid.MustParse("01900000-0000-7000-8000-000000000030")
		rw2, err := s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw2ID,
			ReadOnly:  false,
			Epoch:     2,
			Now:       t0.Add(12 * time.Second),
			ExpiresAt: t0.Add(42 * time.Second),
		})
		require.NoError(t, err)
		activeAfterPreempt, err := s.ListActiveMountLeases(ctx, diskID, t0.Add(12*time.Second))
		require.NoError(t, err)
		require.Equal(t, []db.MountLeaseRecord{ro1, rw2}, activeAfterPreempt)

		_, err = s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw1ID,
			ReadOnly:  false,
			Epoch:     1,
			Now:       t0.Add(13 * time.Second),
			ExpiresAt: t0.Add(43 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrNotFound)

		_, err = s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   ro1ID,
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0.Add(30 * time.Second),
			ExpiresAt: t0.Add(60 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrNotFound)

		prunedExpiredRO, err := s.PruneExpiredMountLeases(ctx, t0.Add(30*time.Second))
		require.NoError(t, err)
		require.Equal(t, int64(1), prunedExpiredRO)

		activeAt30, err := s.ListActiveMountLeases(ctx, diskID, t0.Add(30*time.Second))
		require.NoError(t, err)
		require.Equal(t, []db.MountLeaseRecord{rw2}, activeAt30)

		deleted, err := s.DeleteMountLease(ctx, diskID, rw2ID)
		require.NoError(t, err)
		require.True(t, deleted)
		deletedAgain, err := s.DeleteMountLease(ctx, diskID, rw2ID)
		require.NoError(t, err)
		require.False(t, deletedAgain)

		_, err = s.RenewMountLease(ctx, db.RenewMountLeaseInput{
			DiskID:    diskID,
			MountID:   rw2ID,
			ReadOnly:  false,
			Epoch:     2,
			Now:       t0.Add(31 * time.Second),
			ExpiresAt: t0.Add(61 * time.Second),
		})
		require.ErrorIs(t, err, db.ErrNotFound)

		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0.Add(35 * time.Second),
			ExpiresAt: t0.Add(40 * time.Second),
		})
		require.NoError(t, err)
		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  false,
			Epoch:     3,
			Now:       t0.Add(35 * time.Second),
			ExpiresAt: t0.Add(65 * time.Second),
		})
		require.NoError(t, err)

		evicted, err := s.DeleteDiskMountLeases(ctx, diskID, t0.Add(45*time.Second))
		require.NoError(t, err)
		require.Equal(t, uint32(1), evicted)

		_, err = s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0.Add(50 * time.Second),
			ExpiresAt: t0.Add(55 * time.Second),
		})
		require.NoError(t, err)
		liveKeep, err := s.CreateMountLease(ctx, db.CreateMountLeaseInput{
			DiskID:    diskID,
			MountID:   mustNewUUIDv7(t),
			ReadOnly:  true,
			Epoch:     0,
			Now:       t0.Add(50 * time.Second),
			ExpiresAt: t0.Add(80 * time.Second),
		})
		require.NoError(t, err)

		pruned, err := s.PruneExpiredMountLeases(ctx, t0.Add(60*time.Second))
		require.NoError(t, err)
		require.Equal(t, int64(1), pruned)
		remaining, err := s.ListActiveMountLeases(ctx, diskID, t0.Add(60*time.Second))
		require.NoError(t, err)
		require.Equal(t, []db.MountLeaseRecord{liveKeep}, remaining)

		_, err = s.DeleteDisk(ctx, diskID, false)
		require.NoError(t, err)
		afterDelete, err := s.ListActiveMountLeases(ctx, diskID, t0.Add(60*time.Second))
		require.NoError(t, err)
		require.Empty(t, afterDelete)
	})
}

func requireSnapshotsOrderedDesc(t *testing.T, snaps []db.SnapshotRecord) {
	t.Helper()
	for i := 1; i < len(snaps); i++ {
		prev, curr := snaps[i-1], snaps[i]
		if prev.CreatedAt.Equal(curr.CreatedAt) {
			require.Positive(t, bytes.Compare(prev.SnapshotID[:], curr.SnapshotID[:]),
				"same timestamp must sort by SnapshotID desc: prev=%s curr=%s", prev.SnapshotID, curr.SnapshotID)
			continue
		}
		require.True(t, prev.CreatedAt.After(curr.CreatedAt),
			"snapshots must sort by CreatedAt desc: prev=%s curr=%s", prev.CreatedAt, curr.CreatedAt)
	}
}

func collectHullKeys(hull map[uuid.UUID]struct{}) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(hull))
	for id := range hull {
		out = append(out, id)
	}
	return out
}

func TestComputeActiveLCAHull_GivenGraphTopologies_ThenProtectsExactMinimalClosure(t *testing.T) {
	s0 := mustNewUUIDv7(t)
	s1 := mustNewUUIDv7(t)
	s2 := mustNewUUIDv7(t)
	s3 := mustNewUUIDv7(t)
	s4 := mustNewUUIDv7(t)
	s5 := mustNewUUIDv7(t)
	s6 := mustNewUUIDv7(t)
	s7 := mustNewUUIDv7(t)
	s8 := mustNewUUIDv7(t)
	s9 := mustNewUUIDv7(t)
	s10 := mustNewUUIDv7(t)
	m1 := mustNewUUIDv7(t)
	m2 := mustNewUUIDv7(t)
	b1 := mustNewUUIDv7(t)
	b2 := mustNewUUIDv7(t)
	sManual := mustNewUUIDv7(t)

	t.Run("zero_bases_returns_empty", func(t *testing.T) {
		hull := db.ComputeActiveLCAHull(
			[]db.DiskRecord{{DiskID: mustNewUUIDv7(t)}},
			[]db.SnapshotRecord{{SnapshotID: s0, Kind: db.SnapshotKindFork}},
		)
		require.Empty(t, hull)
	})

	t.Run("single_base_or_shared_base_after_fast_forward", func(t *testing.T) {
		snaps := []db.SnapshotRecord{
			{SnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s1, ParentSnapshotID: s0, Kind: db.SnapshotKindMerge},
			{SnapshotID: s2, ParentSnapshotID: s1, Kind: db.SnapshotKindMerge},
		}
		hullSingle := db.ComputeActiveLCAHull(
			[]db.DiskRecord{{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s2}},
			snaps,
		)
		require.ElementsMatch(t, []uuid.UUID{s2}, collectHullKeys(hullSingle))

		hullShared := db.ComputeActiveLCAHull(
			[]db.DiskRecord{
				{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s2},
				{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s2},
			},
			snaps,
		)
		require.ElementsMatch(t, []uuid.UUID{s2}, collectHullKeys(hullShared))
	})

	t.Run("long_merge_chain_unprotects_old_ancestors", func(t *testing.T) {
		snaps := []db.SnapshotRecord{
			{SnapshotID: s6, Kind: db.SnapshotKindFork},
			{SnapshotID: s7, ParentSnapshotID: s6, Kind: db.SnapshotKindMerge},
			{SnapshotID: s8, ParentSnapshotID: s7, Kind: db.SnapshotKindMerge},
			{SnapshotID: s9, ParentSnapshotID: s7, Kind: db.SnapshotKindMerge},
			{SnapshotID: s10, ParentSnapshotID: s8, MergeParentSnapshotID: s9, Kind: db.SnapshotKindMerge},
		}
		disks := []db.DiskRecord{
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s10},
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s9},
		}
		hull := db.ComputeActiveLCAHull(disks, snaps)
		require.ElementsMatch(t, []uuid.UUID{s9, s10}, collectHullKeys(hull))
	})

	t.Run("multi_branch_fork_tree_excludes_below_lca_and_manual", func(t *testing.T) {
		snaps := []db.SnapshotRecord{
			{SnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s1, ParentSnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s2, ParentSnapshotID: s1, Kind: db.SnapshotKindFork},
			{SnapshotID: s3, ParentSnapshotID: s1, Kind: db.SnapshotKindFork},
			{SnapshotID: s5, ParentSnapshotID: s2, Kind: db.SnapshotKindFork},
			{SnapshotID: s6, ParentSnapshotID: s3, Kind: db.SnapshotKindFork},
			{SnapshotID: sManual, ParentSnapshotID: s1, Kind: db.SnapshotKindManual},
		}
		disks := []db.DiskRecord{
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s5},
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s6},
		}
		hull := db.ComputeActiveLCAHull(disks, snaps)
		require.ElementsMatch(t, []uuid.UUID{s1, s2, s3, s5, s6}, collectHullKeys(hull))
	})

	t.Run("three_branch_tree_pulls_deeper_lca_into_hull", func(t *testing.T) {
		snaps := []db.SnapshotRecord{
			{SnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s1, ParentSnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s2, ParentSnapshotID: s1, Kind: db.SnapshotKindFork},
			{SnapshotID: s3, ParentSnapshotID: s1, Kind: db.SnapshotKindFork},
			{SnapshotID: s4, ParentSnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: s5, ParentSnapshotID: s2, Kind: db.SnapshotKindFork},
			{SnapshotID: s6, ParentSnapshotID: s3, Kind: db.SnapshotKindFork},
			{SnapshotID: s7, ParentSnapshotID: s4, Kind: db.SnapshotKindFork},
		}
		disks := []db.DiskRecord{
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s5},
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s6},
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s7},
		}
		hull := db.ComputeActiveLCAHull(disks, snaps)
		require.ElementsMatch(t, []uuid.UUID{s0, s1, s2, s3, s4, s5, s6, s7}, collectHullKeys(hull))
	})

	t.Run("criss_cross_multi_mca_protects_all_mcas_and_bridges_not_below", func(t *testing.T) {
		snaps := []db.SnapshotRecord{
			{SnapshotID: s0, Kind: db.SnapshotKindFork},
			{SnapshotID: m1, ParentSnapshotID: s0, Kind: db.SnapshotKindMerge},
			{SnapshotID: m2, ParentSnapshotID: s0, Kind: db.SnapshotKindMerge},
			{SnapshotID: b1, ParentSnapshotID: m1, MergeParentSnapshotID: m2, Kind: db.SnapshotKindMerge},
			{SnapshotID: b2, ParentSnapshotID: m2, MergeParentSnapshotID: m1, Kind: db.SnapshotKindMerge},
			{SnapshotID: s5, ParentSnapshotID: b1, Kind: db.SnapshotKindFork},
			{SnapshotID: s6, ParentSnapshotID: b2, Kind: db.SnapshotKindFork},
		}
		disks := []db.DiskRecord{
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s5},
			{DiskID: mustNewUUIDv7(t), BaseSnapshotID: s6},
		}
		hull := db.ComputeActiveLCAHull(disks, snaps)
		require.ElementsMatch(t, []uuid.UUID{m1, m2, b1, b2, s5, s6}, collectHullKeys(hull))
	})
}

func TestStoreContract_GivenDisk_WhenCreateSnapshotCalled_ThenPersistsManualWithoutAdvancingBase(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		_, err := s.CreateSnapshot(ctx, db.CreateSnapshotInput{DiskID: uuid.Nil, SnapshotID: mustNewUUIDv7(t), Name: "bad"})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{DiskID: mustNewUUIDv7(t), SnapshotID: uuid.Nil, Name: "bad"})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{DiskID: mustNewUUIDv7(t), SnapshotID: mustNewUUIDv7(t), Name: "missing"})
		require.ErrorIs(t, err, db.ErrNotFound)

		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root", testDefaultMaxBytes)
		require.NoError(t, err)

		m1ID := mustNewUUIDv7(t)
		m1, err := s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: m1ID,
			Name:       "before-fork",
		})
		require.NoError(t, err)
		require.Equal(t, root.LineageID, m1.LineageID)
		require.Equal(t, m1ID, m1.SnapshotID)
		require.Equal(t, rootID, m1.SourceDiskID)
		require.Equal(t, uuid.Nil, m1.ParentSnapshotID)
		require.Equal(t, uuid.Nil, m1.MergeParentSnapshotID)
		require.Equal(t, db.SnapshotKindManual, m1.Kind)
		require.Equal(t, "before-fork", m1.Name)
		require.False(t, m1.Protected)
		require.False(t, m1.CreatedAt.IsZero())

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, uuid.Nil, gotRoot.BaseSnapshotID, "CreateSnapshot must not modify disk base_snapshot_id")

		_, dupErr := s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: m1ID,
			Name:       "dup",
		})
		require.Error(t, dupErr)

		childID, sFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  childID,
			SnapshotID:   sFork,
			Name:         "child",
		})
		require.NoError(t, err)

		m2ID := mustNewUUIDv7(t)
		m2, err := s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: m2ID,
			Name:       "",
		})
		require.NoError(t, err)
		require.Equal(t, sFork, m2.ParentSnapshotID)
		require.Equal(t, uuid.Nil, m2.MergeParentSnapshotID)
		require.Equal(t, db.SnapshotKindManual, m2.Kind)
		require.Empty(t, m2.Name)
		require.False(t, m2.Protected)

		gotRootAfter, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, sFork, gotRootAfter.BaseSnapshotID, "CreateSnapshot must leave non-nil base_snapshot_id unchanged")

		snaps, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		byID := make(map[uuid.UUID]db.SnapshotRecord, len(snaps))
		for _, snap := range snaps {
			byID[snap.SnapshotID] = snap
		}
		require.Equal(t, "before-fork", byID[m1ID].Name)
		require.Empty(t, byID[sFork].Name)
		require.Empty(t, byID[m2ID].Name)
	})
}

func TestStoreContract_GivenLineageSnapshots_WhenListSnapshotsCalled_ThenComputesProtectionFiltersAndPaginates(t *testing.T) {
	type malformedTokenCase struct {
		name  string
		token string
	}
	badTokens := []malformedTokenCase{
		{name: "invalid_base64", token: "!!!bad!!!"},
		{name: "empty_json_object", token: base64.RawURLEncoding.EncodeToString([]byte(`{}`))},
		{name: "disk_cursor_field_rejected", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"11111111-1111-7111-8111-111111111111"}`))},
		{name: "nil_uuid", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"sid":"00000000-0000-0000-0000-000000000000"}`))},
		{name: "trailing_bytes", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"sid":"11111111-1111-7111-8111-111111111111"}{}`))},
	}

	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		_, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: uuid.Nil})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: mustNewUUIDv7(t), Kind: db.SnapshotKind("unknown")})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: mustNewUUIDv7(t), PageSize: 1001})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: mustNewUUIDv7(t)})
		require.ErrorIs(t, err, db.ErrNotFound)

		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "main", testDefaultMaxBytes)
		require.NoError(t, err)

		runSubtests(t, badTokens, func(tc malformedTokenCase) string { return tc.name }, func(t *testing.T, tc malformedTokenCase) {
			_, tokErr := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, PageSize: 10, PageToken: tc.token})
			require.ErrorIs(t, tokErr, db.ErrInvalidArgument)
		})

		emptyPage, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID})
		require.NoError(t, err)
		require.Empty(t, emptyPage.Snapshots)
		require.Empty(t, emptyPage.NextPageToken)

		otherID, otherChildID, otherSnapID := mustNewUUIDv7(t), mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.CreateDisk(ctx, otherID, "other", testDefaultMaxBytes)
		require.NoError(t, err)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: otherID,
			ChildDiskID:  otherChildID,
			SnapshotID:   otherSnapID,
			Name:         "other-child",
		})
		require.NoError(t, err)

		devID, sFork1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  devID,
			SnapshotID:   sFork1,
			Name:         "dev",
		})
		require.NoError(t, err)

		sSrc1, sMerge1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 devID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sFork1,
			ExpectedTargetBaseSnapshotID: sFork1,
			NewSourceSnapshotID:          sSrc1,
			NewMergeSnapshotID:           sMerge1,
		})
		require.NoError(t, err)

		sManual1 := mustNewUUIDv7(t)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: sManual1,
			Name:       "release-v1",
		})
		require.NoError(t, err)

		sManual2 := mustNewUUIDv7(t)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     devID,
			SnapshotID: sManual2,
			Name:       "dev-checkpoint",
		})
		require.NoError(t, err)

		fullPage, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: devID, PageSize: 0})
		require.NoError(t, err)
		require.Len(t, fullPage.Snapshots, 5)
		require.Empty(t, fullPage.NextPageToken)
		requireSnapshotsOrderedDesc(t, fullPage.Snapshots)

		byID := make(map[uuid.UUID]db.SnapshotRecord, len(fullPage.Snapshots))
		for _, snap := range fullPage.Snapshots {
			byID[snap.SnapshotID] = snap
		}
		require.NotContains(t, byID, otherSnapID)
		require.False(t, byID[sFork1].Protected, "sFork1 is strictly below MCA(sMerge1, sSrc1) = sSrc1")
		require.True(t, byID[sSrc1].Protected)
		require.True(t, byID[sMerge1].Protected)
		require.False(t, byID[sManual1].Protected)
		require.Equal(t, "release-v1", byID[sManual1].Name)
		require.False(t, byID[sManual2].Protected)
		require.Equal(t, "dev-checkpoint", byID[sManual2].Name)

		manualOnly, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, Kind: db.SnapshotKindManual})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sManual1, sManual2}, collectSnapshotIDs(manualOnly.Snapshots))

		forkOnly, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, Kind: db.SnapshotKindFork})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sFork1}, collectSnapshotIDs(forkOnly.Snapshots))

		mergeOnly, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, Kind: db.SnapshotKindMerge})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sSrc1, sMerge1}, collectSnapshotIDs(mergeOnly.Snapshots))

		p1, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, PageSize: 2})
		require.NoError(t, err)
		require.Len(t, p1.Snapshots, 2)
		require.NotEmpty(t, p1.NextPageToken)

		p2, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, PageSize: 2, PageToken: p1.NextPageToken})
		require.NoError(t, err)
		require.Len(t, p2.Snapshots, 2)
		require.NotEmpty(t, p2.NextPageToken)

		p3, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID, PageSize: 2, PageToken: p2.NextPageToken})
		require.NoError(t, err)
		require.Len(t, p3.Snapshots, 1)
		require.Empty(t, p3.NextPageToken)

		paged := slices.Concat(p1.Snapshots, p2.Snapshots, p3.Snapshots)
		requireSnapshotsOrderedDesc(t, paged)
		require.Equal(t, collectSnapshotIDs(fullPage.Snapshots), collectSnapshotIDs(paged))
	})
}

func TestStoreContract_GivenLineageSnapshots_WhenDeleteSnapshotCalled_ThenEnforcesLineageAndHullProtection(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		_, err := s.DeleteSnapshot(ctx, uuid.Nil, mustNewUUIDv7(t))
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.DeleteSnapshot(ctx, mustNewUUIDv7(t), uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.DeleteSnapshot(ctx, mustNewUUIDv7(t), mustNewUUIDv7(t))
		require.ErrorIs(t, err, db.ErrNotFound)

		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "main", testDefaultMaxBytes)
		require.NoError(t, err)

		_, err = s.DeleteSnapshot(ctx, rootID, mustNewUUIDv7(t))
		require.ErrorIs(t, err, db.ErrNotFound)

		otherID, otherSnapID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.CreateDisk(ctx, otherID, "other", testDefaultMaxBytes)
		require.NoError(t, err)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     otherID,
			SnapshotID: otherSnapID,
			Name:       "other-manual",
		})
		require.NoError(t, err)

		_, err = s.DeleteSnapshot(ctx, rootID, otherSnapID)
		require.ErrorIs(t, err, db.ErrCrossLineage)

		devID, sFork1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  devID,
			SnapshotID:   sFork1,
			Name:         "dev",
		})
		require.NoError(t, err)

		sSrc1, sMerge1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 devID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sFork1,
			ExpectedTargetBaseSnapshotID: sFork1,
			NewSourceSnapshotID:          sSrc1,
			NewMergeSnapshotID:           sMerge1,
		})
		require.NoError(t, err)

		_, err = s.DeleteSnapshot(ctx, rootID, sSrc1)
		require.ErrorIs(t, err, db.ErrSnapshotInUse)
		_, err = s.DeleteSnapshot(ctx, rootID, sMerge1)
		require.ErrorIs(t, err, db.ErrSnapshotInUse)

		sManual := mustNewUUIDv7(t)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: sManual,
			Name:       "manual-snap",
		})
		require.NoError(t, err)

		deletedManual, err := s.DeleteSnapshot(ctx, devID, sManual)
		require.NoError(t, err)
		require.Equal(t, sManual, deletedManual.SnapshotID)
		require.Equal(t, "manual-snap", deletedManual.Name)
		require.Equal(t, db.SnapshotKindManual, deletedManual.Kind)
		require.False(t, deletedManual.Protected)

		_, err = s.DeleteSnapshot(ctx, rootID, sManual)
		require.ErrorIs(t, err, db.ErrNotFound)

		deletedFork, err := s.DeleteSnapshot(ctx, rootID, sFork1)
		require.NoError(t, err)
		require.Equal(t, sFork1, deletedFork.SnapshotID)

		remaining, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sSrc1, sMerge1}, collectSnapshotIDs(remaining.Snapshots))
		for _, snap := range remaining.Snapshots {
			require.Equal(t, uuid.Nil, snap.ParentSnapshotID, "cleared parent FK to deleted sFork1")
		}

		subID, sSubFork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: devID,
			ChildDiskID:  subID,
			SnapshotID:   sSubFork,
			Name:         "dev-sub",
		})
		require.NoError(t, err)

		sub2ID, sSub2Fork := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: subID,
			ChildDiskID:  sub2ID,
			SnapshotID:   sSub2Fork,
			Name:         "dev-sub2",
		})
		require.NoError(t, err)

		_, err = s.DeleteDisk(ctx, devID, false)
		require.NoError(t, err)

		_, err = s.DeleteSnapshot(ctx, rootID, sSrc1)
		require.ErrorIs(t, err, db.ErrSnapshotInUse, "non-base LCA node sSrc1 must remain protected")
		_, err = s.DeleteSnapshot(ctx, rootID, sSubFork)
		require.ErrorIs(t, err, db.ErrSnapshotInUse, "non-base bridge node sSubFork between sSub2Fork and LCA sSrc1 must remain protected")
	})
}

func TestStoreContract_GivenLineageSnapshots_WhenPruneSnapshotsCalled_ThenSupportsDryRunProtectsHullAndManualAndIsIdempotent(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		_, err := s.PruneSnapshots(ctx, uuid.Nil, false)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.PruneSnapshots(ctx, mustNewUUIDv7(t), false)
		require.ErrorIs(t, err, db.ErrNotFound)

		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "main", testDefaultMaxBytes)
		require.NoError(t, err)

		devID, sFork1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  devID,
			SnapshotID:   sFork1,
			Name:         "dev",
		})
		require.NoError(t, err)

		sSrc1, sMerge1 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 devID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sFork1,
			ExpectedTargetBaseSnapshotID: sFork1,
			NewSourceSnapshotID:          sSrc1,
			NewMergeSnapshotID:           sMerge1,
		})
		require.NoError(t, err)

		sSrc2, sMerge2 := mustNewUUIDv7(t), mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 devID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sSrc1,
			ExpectedTargetBaseSnapshotID: sMerge1,
			NewSourceSnapshotID:          sSrc2,
			NewMergeSnapshotID:           sMerge2,
		})
		require.NoError(t, err)

		sManual := mustNewUUIDv7(t)
		_, err = s.CreateSnapshot(ctx, db.CreateSnapshotInput{
			DiskID:     rootID,
			SnapshotID: sManual,
			Name:       "keep-me",
		})
		require.NoError(t, err)

		dryRunCandidates, err := s.PruneSnapshots(ctx, rootID, true)
		require.NoError(t, err)
		requireSnapshotsOrderedDesc(t, dryRunCandidates)
		require.ElementsMatch(t, []uuid.UUID{sFork1, sSrc1, sMerge1}, collectSnapshotIDs(dryRunCandidates))
		for _, snap := range dryRunCandidates {
			require.False(t, snap.Protected)
			require.NotEqual(t, db.SnapshotKindManual, snap.Kind)
		}

		afterDryRun, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID})
		require.NoError(t, err)
		require.Len(t, afterDryRun.Snapshots, 6, "dry_run must not mutate persisted snapshots")

		pruned, err := s.PruneSnapshots(ctx, devID, false)
		require.NoError(t, err)
		require.Equal(t, collectSnapshotIDs(dryRunCandidates), collectSnapshotIDs(pruned))

		afterPrune, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sSrc2, sMerge2, sManual}, collectSnapshotIDs(afterPrune.Snapshots))

		byID := make(map[uuid.UUID]db.SnapshotRecord, len(afterPrune.Snapshots))
		for _, snap := range afterPrune.Snapshots {
			byID[snap.SnapshotID] = snap
		}
		require.Equal(t, uuid.Nil, byID[sMerge2].ParentSnapshotID, "pruning sMerge1 clears sMerge2.ParentSnapshotID")
		require.Equal(t, sSrc2, byID[sMerge2].MergeParentSnapshotID, "sMerge2.MergeParentSnapshotID to protected sSrc2 stays intact")
		require.Equal(t, uuid.Nil, byID[sSrc2].ParentSnapshotID)
		require.Equal(t, sMerge2, byID[sManual].ParentSnapshotID)

		secondPrune, err := s.PruneSnapshots(ctx, rootID, false)
		require.NoError(t, err)
		require.Empty(t, secondPrune, "second prune must be idempotent")

		sFF := mustNewUUIDv7(t)
		_, err = s.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                    root.LineageID,
			SourceDiskID:                 devID,
			TargetDiskID:                 rootID,
			ExpectedSourceBaseSnapshotID: sSrc2,
			ExpectedTargetBaseSnapshotID: sMerge2,
			NewSourceSnapshotID:          sFF,
			NewMergeSnapshotID:           sFF,
			FastForward:                  true,
		})
		require.NoError(t, err)

		prunedAfterFF, err := s.PruneSnapshots(ctx, rootID, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sSrc2, sMerge2}, collectSnapshotIDs(prunedAfterFF),
			"shared base after fast-forward must prune older ancestors while keeping sFF and manual")

		afterFFPrune, err := s.ListSnapshots(ctx, db.ListSnapshotsQuery{DiskID: rootID})
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sFF, sManual}, collectSnapshotIDs(afterFFPrune.Snapshots))
	})
}
