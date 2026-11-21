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
		require.ElementsMatch(t, []uuid.UUID{sFork2}, resTier2.ReclaimedSnapshotIDs,
			"tier remaining>=2 must reclaim unreachable sFork2 while preserving sManualOnDeadBranch")

		snapsAfterTier2, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		snapByID := make(map[uuid.UUID]db.SnapshotRecord, len(snapsAfterTier2))
		for _, snap := range snapsAfterTier2 {
			snapByID[snap.SnapshotID] = snap
		}
		require.Contains(t, snapByID, sManualOnDeadBranch)
		require.Equal(t, uuid.Nil, snapByID[sManualOnDeadBranch].ParentSnapshotID, "surviving manual snapshot FK to deleted fork must be cleared")
		require.Contains(t, snapByID, sManualOnRoot)
		require.Contains(t, snapByID, sFork1)
		require.Contains(t, snapByID, sFork3)

		resTier1, err := s.DeleteDisk(ctx, d3ID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{d3ID}, resTier1.DeletedDiskIDs)
		require.ElementsMatch(t, []uuid.UUID{sFork1, sFork3}, resTier1.ReclaimedSnapshotIDs,
			"tier remaining==1 must reclaim all fork/merge snapshots and preserve all manual snapshots")

		gotRoot, err := s.GetDisk(ctx, rootID)
		require.NoError(t, err)
		require.Equal(t, sManualOnRoot, gotRoot.BaseSnapshotID,
			"sole surviving disk whose base_snapshot_id is manual must keep it intact")

		snapsAfterTier1, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sManualOnDeadBranch, sManualOnRoot}, collectSnapshotIDs(snapsAfterTier1))
		for _, snap := range snapsAfterTier1 {
			require.Equal(t, db.SnapshotKindManual, snap.Kind)
			require.Equal(t, uuid.Nil, snap.ParentSnapshotID)
			require.Equal(t, uuid.Nil, snap.MergeParentSnapshotID)
		}

		resTier0, err := s.DeleteDisk(ctx, rootID, false)
		require.NoError(t, err)
		require.Equal(t, []uuid.UUID{rootID}, resTier0.DeletedDiskIDs)
		require.Empty(t, resTier0.ReclaimedSnapshotIDs)

		snapsAfterTier0, err := s.ListLineageSnapshots(ctx, root.LineageID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{sManualOnDeadBranch, sManualOnRoot}, collectSnapshotIDs(snapsAfterTier0),
			"tier remaining==0 must preserve manual snapshots and not cascade delete lineage")

		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 2, 1)
		require.NoError(t, err, "lineage row must remain alive when remaining_snapshots > 0")
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
