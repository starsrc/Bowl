package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/tx"
)

type retryTestCtxKey struct{}

type sqlInvocation struct {
	op        string
	queryName string
	ctx       context.Context
	args      []any
}

type stubQueryResult struct {
	tag  pgconn.CommandTag
	row  []any
	rows [][]any
}

type stubTxAttempt struct {
	attempt             int
	beginCtx            context.Context
	opts                pgx.TxOptions
	calls               []sqlInvocation
	committed           bool
	commitCtx           context.Context
	rolledBack          bool
	rollbackCtx         context.Context
	rollbackCtxErr      error
	rollbackHasDeadline bool
}

type queryHandler func(attempt int, inv sqlInvocation) (stubQueryResult, error)

type stubBeginner struct {
	attempts    []*stubTxAttempt
	beginErrFn  func(attempt int, ctx context.Context, opts pgx.TxOptions) error
	queryFn     queryHandler
	commitErrFn func(attempt int, ctx context.Context) error
	rollbackFn  func(attempt int, ctx context.Context) error
}

//nolint:gocritic // 实现 tx.Beginner 接口所需签名，pgx.TxOptions 无法改传指针。
func (b *stubBeginner) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	idx := len(b.attempts)
	if b.beginErrFn != nil {
		if err := b.beginErrFn(idx, ctx, txOptions); err != nil {
			return nil, err
		}
	}
	rec := &stubTxAttempt{
		attempt:  idx,
		beginCtx: ctx,
		opts:     txOptions,
	}
	b.attempts = append(b.attempts, rec)
	return &stubPGXTx{
		beginner: b,
		attempt:  rec,
	}, nil
}

type stubPGXTx struct {
	beginner *stubBeginner
	attempt  *stubTxAttempt
}

func (t *stubPGXTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("unimplemented")
}

func (t *stubPGXTx) Commit(ctx context.Context) error {
	t.attempt.committed = true
	t.attempt.commitCtx = ctx
	if t.beginner.commitErrFn != nil {
		return t.beginner.commitErrFn(t.attempt.attempt, ctx)
	}
	return nil
}

func (t *stubPGXTx) Rollback(ctx context.Context) error {
	t.attempt.rolledBack = true
	t.attempt.rollbackCtx = ctx
	t.attempt.rollbackCtxErr = ctx.Err()
	_, t.attempt.rollbackHasDeadline = ctx.Deadline()
	if t.beginner.rollbackFn != nil {
		return t.beginner.rollbackFn(t.attempt.attempt, ctx)
	}
	return nil
}

func (t *stubPGXTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("unimplemented")
}

func (t *stubPGXTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

func (t *stubPGXTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (t *stubPGXTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unimplemented")
}

func dispatchStubExec(ctx context.Context, sql string, args []any, invoke func(sqlInvocation) (stubQueryResult, error)) (pgconn.CommandTag, error) {
	res, err := invoke(sqlInvocation{op: "Exec", queryName: extractQueryName(sql), ctx: ctx, args: args})
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	return res.tag, nil
}

func dispatchStubQuery(ctx context.Context, sql string, args []any, invoke func(sqlInvocation) (stubQueryResult, error)) (pgx.Rows, error) {
	res, err := invoke(sqlInvocation{op: "Query", queryName: extractQueryName(sql), ctx: ctx, args: args})
	if err != nil {
		return nil, err
	}
	return &stubRows{rows: res.rows}, nil
}

func dispatchStubQueryRow(ctx context.Context, sql string, args []any, invoke func(sqlInvocation) (stubQueryResult, error)) pgx.Row {
	res, err := invoke(sqlInvocation{op: "QueryRow", queryName: extractQueryName(sql), ctx: ctx, args: args})
	if err != nil {
		return stubRow{err: err}
	}
	return stubRow{values: res.row}
}

func (t *stubPGXTx) recordAndInvoke(inv sqlInvocation) (stubQueryResult, error) {
	t.attempt.calls = append(t.attempt.calls, inv)
	if t.beginner.queryFn == nil {
		return stubQueryResult{}, fmt.Errorf("unexpected tx query %s", inv.queryName)
	}
	return t.beginner.queryFn(t.attempt.attempt, inv)
}

func (t *stubPGXTx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return dispatchStubExec(ctx, sql, arguments, t.recordAndInvoke)
}

func (t *stubPGXTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return dispatchStubQuery(ctx, sql, args, t.recordAndInvoke)
}

func (t *stubPGXTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return dispatchStubQueryRow(ctx, sql, args, t.recordAndInvoke)
}

func (t *stubPGXTx) Conn() *pgx.Conn {
	return nil
}

type stubDBTX struct {
	calls   []sqlInvocation
	queryFn queryHandler
}

func (d *stubDBTX) recordAndInvoke(inv sqlInvocation) (stubQueryResult, error) {
	attempt := len(d.calls)
	d.calls = append(d.calls, inv)
	if d.queryFn == nil {
		return stubQueryResult{}, fmt.Errorf("unexpected non-tx query %s", inv.queryName)
	}
	return d.queryFn(attempt, inv)
}

func (d *stubDBTX) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return dispatchStubExec(ctx, sql, arguments, d.recordAndInvoke)
}

func (d *stubDBTX) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return dispatchStubQuery(ctx, sql, args, d.recordAndInvoke)
}

func (d *stubDBTX) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return dispatchStubQueryRow(ctx, sql, args, d.recordAndInvoke)
}

type stubRow struct {
	values []any
	err    error
}

func (r stubRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanStubValues(r.values, dest...)
}

type stubRows struct {
	rows [][]any
	idx  int
	err  error
}

func (r *stubRows) Close()                                       {}
func (r *stubRows) Err() error                                   { return r.err }
func (r *stubRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *stubRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *stubRows) Values() ([]any, error)                       { return nil, nil }
func (r *stubRows) RawValues() [][]byte                          { return nil }
func (r *stubRows) Conn() *pgx.Conn                              { return nil }
func (r *stubRows) TypeMap() *pgtype.Map                         { return nil }

func (r *stubRows) Next() bool {
	if r.idx >= len(r.rows) {
		return false
	}
	r.idx++
	return true
}

func (r *stubRows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("scan called without active row")
	}
	return scanStubValues(r.rows[r.idx-1], dest...)
}

func scanStubValues(values []any, dest ...any) error {
	if len(values) != len(dest) {
		return fmt.Errorf("stub column mismatch: have %d values, want %d dest", len(values), len(dest))
	}
	for i := range dest {
		switch target := dest[i].(type) {
		case *pgtype.UUID:
			v, ok := values[i].(pgtype.UUID)
			if !ok {
				return fmt.Errorf("col %d: expected pgtype.UUID, got %T", i, values[i])
			}
			*target = v
		case *string:
			v, ok := values[i].(string)
			if !ok {
				return fmt.Errorf("col %d: expected string, got %T", i, values[i])
			}
			*target = v
		case *pgtype.Timestamptz:
			v, ok := values[i].(pgtype.Timestamptz)
			if !ok {
				return fmt.Errorf("col %d: expected pgtype.Timestamptz, got %T", i, values[i])
			}
			*target = v
		case *int64:
			v, ok := values[i].(int64)
			if !ok {
				return fmt.Errorf("col %d: expected int64, got %T", i, values[i])
			}
			*target = v
		default:
			return fmt.Errorf("unsupported stub scan target %T", dest[i])
		}
	}
	return nil
}

func extractQueryName(sql string) string {
	const prefix = "-- name: "
	rest, ok := strings.CutPrefix(sql, prefix)
	if !ok {
		return sql
	}
	name, _, _ := strings.Cut(rest, " ")
	return name
}

func stubDiskRow(diskID, lineageID, parentID, baseCommitID uuid.UUID, name string, ts time.Time) []any {
	pgTS := pgtype.Timestamptz{Time: ts.UTC(), Valid: true}
	return []any{
		toPGUUID(diskID),
		name,
		pgTS,
		pgTS,
		toPGUUID(lineageID),
		toPGUUID(parentID),
		toPGUUID(baseCommitID),
	}
}

func stubLineageRow(lineageID uuid.UUID, ts time.Time) []any {
	return []any{
		toPGUUID(lineageID),
		int64(2),
		int64(1),
		pgtype.Timestamptz{Time: ts.UTC(), Valid: true},
	}
}

func stubCommitRow(commitID, lineageID, sourceDiskID, parentCommitID, mergeParentCommitID uuid.UUID, ts time.Time) []any {
	return []any{
		toPGUUID(commitID),
		toPGUUID(lineageID),
		toPGUUID(sourceDiskID),
		toPGUUID(parentCommitID),
		toPGUUID(mergeParentCommitID),
		pgtype.Timestamptz{Time: ts.UTC(), Valid: true},
	}
}

func newRetryTestStore(dbtx DBTX, beginner tx.Beginner) *SQLStore {
	return newSQLStore(dbtx, beginner, tx.WithRetry(4, 0, 0))
}

func assertTxAttemptCtxConsistency(t *testing.T, attempts []*stubTxAttempt, wantVal string) {
	t.Helper()
	for i, att := range attempts {
		require.NotNil(t, att.beginCtx, "attempt %d beginCtx should not be nil", i)
		assert.Equal(t, wantVal, att.beginCtx.Value(retryTestCtxKey{}))
		require.NotEmpty(t, att.calls, "attempt %d should execute SQL calls", i)
		for j, call := range att.calls {
			assert.Equal(t, att.beginCtx, call.ctx, "attempt %d call %d (%s) ctx must match BeginTx ctx", i, j, call.queryName)
			assert.Equal(t, wantVal, call.ctx.Value(retryTestCtxKey{}))
		}
		if att.committed {
			assert.Equal(t, att.beginCtx, att.commitCtx, "attempt %d commitCtx must match BeginTx ctx", i)
		}
	}
}

func TestSQLStore_TxMethodsRetryAndAttemptCtx(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000000, 0).UTC()
	diskA := uuid.MustParse("01900000-0000-7000-8000-000000000001")
	diskB := uuid.MustParse("01900000-0000-7000-8000-000000000002")
	lineageID := uuid.MustParse("01900000-0000-7000-8000-000000000010")
	commitSrcBase := uuid.MustParse("01900000-0000-7000-8000-000000000021")
	commitDstBase := uuid.MustParse("01900000-0000-7000-8000-000000000022")
	commitNewSrc := uuid.MustParse("01900000-0000-7000-8000-000000000023")
	commitNewMerge := uuid.MustParse("01900000-0000-7000-8000-000000000024")

	defaultTxHandler := func(_ int, inv sqlInvocation) (stubQueryResult, error) {
		switch inv.queryName {
		case "CreateDiskLineage", "LockDiskLineage":
			return stubQueryResult{row: stubLineageRow(lineageID, now)}, nil
		case "CreateDisk":
			return stubQueryResult{row: stubDiskRow(diskB, lineageID, diskA, commitNewSrc, "child", now)}, nil
		case "GetDisk":
			return stubQueryResult{row: stubDiskRow(diskA, lineageID, uuid.Nil, commitSrcBase, "root", now)}, nil
		case "GetDiskForUpdate":
			if len(inv.args) > 0 {
				if id, ok := inv.args[0].(pgtype.UUID); ok && fromPGUUID(id) == diskB {
					return stubQueryResult{row: stubDiskRow(diskB, lineageID, diskA, commitDstBase, "target", now)}, nil
				}
			}
			return stubQueryResult{row: stubDiskRow(diskA, lineageID, uuid.Nil, commitSrcBase, "source", now)}, nil
		case "CreateDiskCommit":
			return stubQueryResult{row: stubCommitRow(commitNewMerge, lineageID, diskA, commitSrcBase, uuid.Nil, now)}, nil
		case "SetDiskBaseCommit", "ClearLineageBaseCommits", "DeleteDiskLineage":
			return stubQueryResult{tag: pgconn.NewCommandTag("UPDATE 1")}, nil
		case "SetDiskBaseCommitAndTouch":
			return stubQueryResult{row: stubDiskRow(diskB, lineageID, diskA, commitNewMerge, "target", now)}, nil
		case "DeleteDisk":
			return stubQueryResult{tag: pgconn.NewCommandTag("DELETE 1")}, nil
		case "CountDisksByLineage":
			return stubQueryResult{row: []any{int64(0)}}, nil
		case "DeleteLineageCommits":
			return stubQueryResult{rows: [][]any{{toPGUUID(commitSrcBase)}}}, nil
		default:
			return stubQueryResult{}, fmt.Errorf("unexpected query %s", inv.queryName)
		}
	}

	cases := []struct {
		name      string
		failStage string
		injectErr error
		invoke    func(ctx context.Context, s Store) error
	}{
		{
			name:      "CreateDisk_SQL阶段40001回滚并重试",
			failStage: "CreateDisk",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.CreateDisk(ctx, diskA, "root")
				return err
			},
		},
		{
			name:      "CreateDisk_提交阶段40001重试",
			failStage: "commit",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.CreateDisk(ctx, diskA, "root")
				return err
			},
		},
		{
			name:      "ForkDisk_SQL阶段40P01回滚并重试",
			failStage: "CreateDiskCommit",
			injectErr: &pgconn.PgError{Code: tx.SQLStateDeadlockDetected},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.ForkDisk(ctx, ForkDiskInput{
					SourceDiskID: diskA,
					ChildDiskID:  diskB,
					CommitID:     commitNewSrc,
					Name:         "child",
				})
				return err
			},
		},
		{
			name:      "ForkDisk_提交阶段40001重试",
			failStage: "commit",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.ForkDisk(ctx, ForkDiskInput{
					SourceDiskID: diskA,
					ChildDiskID:  diskB,
					CommitID:     commitNewSrc,
					Name:         "child",
				})
				return err
			},
		},
		{
			name:      "MergeDisk_SQL阶段40001回滚并重试",
			failStage: "SetDiskBaseCommitAndTouch",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.MergeDisk(ctx, &MergeDiskInput{
					LineageID:                  lineageID,
					SourceDiskID:               diskA,
					TargetDiskID:               diskB,
					ExpectedSourceBaseCommitID: commitSrcBase,
					ExpectedTargetBaseCommitID: commitDstBase,
					NewSourceCommitID:          commitNewSrc,
					NewMergeCommitID:           commitNewMerge,
				})
				return err
			},
		},
		{
			name:      "MergeDisk_提交阶段40001重试",
			failStage: "commit",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.MergeDisk(ctx, &MergeDiskInput{
					LineageID:                  lineageID,
					SourceDiskID:               diskA,
					TargetDiskID:               diskB,
					ExpectedSourceBaseCommitID: commitSrcBase,
					ExpectedTargetBaseCommitID: commitDstBase,
					NewSourceCommitID:          commitNewSrc,
					NewMergeCommitID:           commitNewMerge,
				})
				return err
			},
		},
		{
			name:      "DeleteDisk_SQL阶段40P01回滚并重试",
			failStage: "DeleteDisk",
			injectErr: &pgconn.PgError{Code: tx.SQLStateDeadlockDetected},
			invoke: func(ctx context.Context, s Store) error {
				commits, err := s.DeleteDisk(ctx, diskA)
				if err != nil {
					return err
				}
				require.Equal(t, []uuid.UUID{commitSrcBase}, commits)
				return nil
			},
		},
		{
			name:      "DeleteDisk_提交阶段40001重试",
			failStage: "commit",
			injectErr: &pgconn.PgError{Code: tx.SQLStateSerializationFailure},
			invoke: func(ctx context.Context, s Store) error {
				_, err := s.DeleteDisk(ctx, diskA)
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dbStub := &stubDBTX{
				queryFn: func(int, sqlInvocation) (stubQueryResult, error) {
					return stubQueryResult{}, errors.New("unexpected non-tx query")
				},
			}
			beginner := &stubBeginner{
				queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
					if attempt == 0 && inv.queryName == tc.failStage {
						return stubQueryResult{}, tc.injectErr
					}
					return defaultTxHandler(attempt, inv)
				},
				commitErrFn: func(attempt int, _ context.Context) error {
					if attempt == 0 && tc.failStage == "commit" {
						return tc.injectErr
					}
					return nil
				},
			}

			s := newRetryTestStore(dbStub, beginner)
			ctx := context.WithValue(context.Background(), retryTestCtxKey{}, "trace-tx")

			err := tc.invoke(ctx, s)
			require.NoError(t, err)
			assert.Empty(t, dbStub.calls)
			require.Len(t, beginner.attempts, 2)
			if tc.failStage == "commit" {
				assert.True(t, beginner.attempts[0].committed)
			} else {
				assert.True(t, beginner.attempts[0].rolledBack)
				assert.False(t, beginner.attempts[0].committed)
				require.NoError(t, beginner.attempts[0].rollbackCtxErr)
				assert.True(t, beginner.attempts[0].rollbackHasDeadline)
			}
			assert.True(t, beginner.attempts[1].committed)
			assert.False(t, beginner.attempts[1].rolledBack)
			assertTxAttemptCtxConsistency(t, beginner.attempts, "trace-tx")
		})
	}
}

func TestSQLStore_SingleQueryWriteAndReadRetry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000100, 0).UTC()
	diskID := uuid.MustParse("01900000-0000-7000-8000-000000000031")
	lineageID := uuid.MustParse("01900000-0000-7000-8000-000000000032")
	commitID := uuid.MustParse("01900000-0000-7000-8000-000000000033")

	t.Run("UpdateDiskName不开启事务且在40001下重试成功", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{
			queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
				require.Equal(t, "UpdateDisk", inv.queryName)
				if attempt == 0 {
					return stubQueryResult{}, &pgconn.PgError{Code: tx.SQLStateSerializationFailure}
				}
				return stubQueryResult{row: stubDiskRow(diskID, lineageID, uuid.Nil, uuid.Nil, "renamed", now)}, nil
			},
		}

		s := newRetryTestStore(dbStub, beginner)
		ctx := context.WithValue(context.Background(), retryTestCtxKey{}, "trace-update")

		rec, err := s.UpdateDiskName(ctx, diskID, "renamed")
		require.NoError(t, err)
		assert.Equal(t, "renamed", rec.Name)
		assert.Empty(t, beginner.attempts, "UpdateDiskName 不应开启数据库事务")
		require.Len(t, dbStub.calls, 2)
		assert.Equal(t, "trace-update", dbStub.calls[0].ctx.Value(retryTestCtxKey{}))
		assert.Equal(t, "trace-update", dbStub.calls[1].ctx.Value(retryTestCtxKey{}))
	})

	t.Run("GetDisk在40001下重试成功且不开启事务", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{
			queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
				require.Equal(t, "GetDisk", inv.queryName)
				if attempt == 0 {
					return stubQueryResult{}, &pgconn.PgError{Code: tx.SQLStateSerializationFailure}
				}
				return stubQueryResult{row: stubDiskRow(diskID, lineageID, uuid.Nil, uuid.Nil, "alpha", now)}, nil
			},
		}

		s := newRetryTestStore(dbStub, beginner)
		rec, err := s.GetDisk(context.Background(), diskID)
		require.NoError(t, err)
		assert.Equal(t, diskID, rec.DiskID)
		assert.Empty(t, beginner.attempts)
		require.Len(t, dbStub.calls, 2)
	})

	t.Run("ListDisks在40P01下重试成功且映射ErrNoRows", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{
			queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
				require.Equal(t, "ListDisks", inv.queryName)
				if attempt == 0 {
					return stubQueryResult{}, &pgconn.PgError{Code: tx.SQLStateDeadlockDetected}
				}
				return stubQueryResult{rows: [][]any{stubDiskRow(diskID, lineageID, uuid.Nil, uuid.Nil, "alpha", now)}}, nil
			},
		}

		s := newRetryTestStore(dbStub, beginner)
		page, err := s.ListDisks(context.Background(), ListDisksQuery{PageSize: 10})
		require.NoError(t, err)
		require.Len(t, page.Disks, 1)
		assert.Equal(t, diskID, page.Disks[0].DiskID)
		assert.Empty(t, beginner.attempts)
		require.Len(t, dbStub.calls, 2)

		noRowsDB := &stubDBTX{
			queryFn: func(int, sqlInvocation) (stubQueryResult, error) {
				return stubQueryResult{}, pgx.ErrNoRows
			},
		}
		noRowsStore := newRetryTestStore(noRowsDB, &stubBeginner{})
		_, err = noRowsStore.ListDisks(context.Background(), ListDisksQuery{PageSize: 10})
		require.ErrorIs(t, err, ErrNotFound)
		require.Len(t, noRowsDB.calls, 1)
	})

	t.Run("ListLineageCommits在40001下重试成功且不开启事务", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{
			queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
				require.Equal(t, "ListLineageCommits", inv.queryName)
				if attempt == 0 {
					return stubQueryResult{}, &pgconn.PgError{Code: tx.SQLStateSerializationFailure}
				}
				return stubQueryResult{rows: [][]any{stubCommitRow(commitID, lineageID, diskID, uuid.Nil, uuid.Nil, now)}}, nil
			},
		}

		s := newRetryTestStore(dbStub, beginner)
		commits, err := s.ListLineageCommits(context.Background(), lineageID)
		require.NoError(t, err)
		require.Len(t, commits, 1)
		assert.Equal(t, commitID, commits[0].CommitID)
		assert.Empty(t, beginner.attempts)
		require.Len(t, dbStub.calls, 2)
	})

	t.Run("ReserveSegment在40001下重试成功且不开启事务", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{
			queryFn: func(attempt int, inv sqlInvocation) (stubQueryResult, error) {
				require.Equal(t, "ReserveInodeSegment", inv.queryName)
				if attempt == 0 {
					return stubQueryResult{}, &pgconn.PgError{Code: tx.SQLStateSerializationFailure}
				}
				return stubQueryResult{row: []any{int64(2), int64(11)}}, nil
			},
		}

		s := newRetryTestStore(dbStub, beginner)
		seg, err := s.ReserveSegment(context.Background(), SegmentKindInode, lineageID, 2, 10)
		require.NoError(t, err)
		assert.Equal(t, SegmentRange{First: 2, Last: 11}, seg)
		assert.Empty(t, beginner.attempts)
		require.Len(t, dbStub.calls, 2)
	})
}

func TestSQLStore_NonTransientErrorsZeroRetry(t *testing.T) {
	t.Parallel()

	now := time.Unix(1700000200, 0).UTC()
	diskA := uuid.MustParse("01900000-0000-7000-8000-000000000041")
	diskB := uuid.MustParse("01900000-0000-7000-8000-000000000042")
	lineageID := uuid.MustParse("01900000-0000-7000-8000-000000000043")
	otherLineage := uuid.MustParse("01900000-0000-7000-8000-000000000044")
	commitSrc := uuid.MustParse("01900000-0000-7000-8000-000000000045")
	commitDst := uuid.MustParse("01900000-0000-7000-8000-000000000046")
	commitStale := uuid.MustParse("01900000-0000-7000-8000-000000000047")
	commitNewSrc := uuid.MustParse("01900000-0000-7000-8000-000000000048")
	commitNewMerge := uuid.MustParse("01900000-0000-7000-8000-000000000049")

	t.Run("pgx.ErrNoRows零重试并映射为ErrNotFound", func(t *testing.T) {
		t.Parallel()

		dbStub := &stubDBTX{
			queryFn: func(int, sqlInvocation) (stubQueryResult, error) {
				return stubQueryResult{}, pgx.ErrNoRows
			},
		}
		beginner := &stubBeginner{
			queryFn: func(int, sqlInvocation) (stubQueryResult, error) {
				return stubQueryResult{}, pgx.ErrNoRows
			},
		}
		s := newRetryTestStore(dbStub, beginner)

		_, err := s.GetDisk(context.Background(), diskA)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = s.UpdateDiskName(context.Background(), diskA, "x")
		require.ErrorIs(t, err, ErrNotFound)
		_, err = s.ListDisks(context.Background(), ListDisksQuery{PageSize: 5})
		require.ErrorIs(t, err, ErrNotFound)
		require.Len(t, dbStub.calls, 3)

		_, err = s.ListLineageCommits(context.Background(), lineageID)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = s.ReserveSegment(context.Background(), SegmentKindPack, lineageID, 2, 4)
		require.ErrorIs(t, err, ErrNotFound)
		require.Len(t, dbStub.calls, 5)

		_, err = s.DeleteDisk(context.Background(), diskA)
		require.ErrorIs(t, err, ErrNotFound)
		_, err = s.ForkDisk(context.Background(), ForkDiskInput{
			SourceDiskID: diskA,
			ChildDiskID:  diskB,
			CommitID:     commitNewSrc,
			Name:         "child",
		})
		require.ErrorIs(t, err, ErrNotFound)
		require.Len(t, beginner.attempts, 2)
		assert.True(t, beginner.attempts[0].rolledBack)
		assert.True(t, beginner.attempts[1].rolledBack)
	})

	runMergeConflictCase := func(t *testing.T, targetLineage, targetCommit uuid.UUID, wantErr error) {
		t.Helper()
		beginner := &stubBeginner{
			queryFn: func(_ int, inv sqlInvocation) (stubQueryResult, error) {
				switch inv.queryName {
				case "LockDiskLineage":
					return stubQueryResult{row: stubLineageRow(lineageID, now)}, nil
				case "GetDiskForUpdate":
					pgID, ok := inv.args[0].(pgtype.UUID)
					if !ok {
						return stubQueryResult{}, fmt.Errorf("unexpected arg type %T", inv.args[0])
					}
					if fromPGUUID(pgID) == diskB {
						return stubQueryResult{row: stubDiskRow(diskB, targetLineage, diskA, targetCommit, "dst", now)}, nil
					}
					return stubQueryResult{row: stubDiskRow(diskA, lineageID, uuid.Nil, commitSrc, "src", now)}, nil
				default:
					return stubQueryResult{}, fmt.Errorf("unexpected query %s", inv.queryName)
				}
			},
		}
		s := newRetryTestStore(&stubDBTX{}, beginner)
		_, err := s.MergeDisk(context.Background(), &MergeDiskInput{
			LineageID:                  lineageID,
			SourceDiskID:               diskA,
			TargetDiskID:               diskB,
			ExpectedSourceBaseCommitID: commitSrc,
			ExpectedTargetBaseCommitID: commitDst,
			NewSourceCommitID:          commitNewSrc,
			NewMergeCommitID:           commitNewMerge,
		})
		require.ErrorIs(t, err, wantErr)
		require.Len(t, beginner.attempts, 1)
		assert.True(t, beginner.attempts[0].rolledBack)
	}

	t.Run("MergeDisk返回ErrCrossLineage零重试", func(t *testing.T) {
		t.Parallel()
		runMergeConflictCase(t, otherLineage, commitDst, ErrCrossLineage)
	})

	t.Run("MergeDisk返回ErrStaleBaseCommit零重试", func(t *testing.T) {
		t.Parallel()
		runMergeConflictCase(t, lineageID, commitStale, ErrStaleBaseCommit)
	})

	t.Run("context.Canceled零重试且事务安全回滚", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())

		beginner := &stubBeginner{
			queryFn: func(_ int, _ sqlInvocation) (stubQueryResult, error) {
				cancel()
				return stubQueryResult{}, ctx.Err()
			},
		}
		s := newRetryTestStore(&stubDBTX{}, beginner)
		_, err := s.CreateDisk(ctx, diskA, "canceled")
		require.ErrorIs(t, err, context.Canceled)
		require.Len(t, beginner.attempts, 1)
		assert.True(t, beginner.attempts[0].rolledBack)
		require.NoError(t, beginner.attempts[0].rollbackCtxErr, "回滚必须使用 context.WithoutCancel")
		assert.True(t, beginner.attempts[0].rollbackHasDeadline, "回滚必须带 5s 超时")

		dbStub := &stubDBTX{
			queryFn: func(int, sqlInvocation) (stubQueryResult, error) {
				return stubQueryResult{}, errors.New("should not run after cancel")
			},
		}
		sQuery := newRetryTestStore(dbStub, &stubBeginner{})
		_, err = sQuery.GetDisk(ctx, diskA)
		require.ErrorIs(t, err, context.Canceled)
		assert.Empty(t, dbStub.calls)
	})
}

func TestSQLStore_ExecTxAndExecQuery_Contract(t *testing.T) {
	t.Parallel()

	t.Run("NewStore默认装配WithDefaultRetry", func(t *testing.T) {
		t.Parallel()
		s, ok := NewStore(nil).(*SQLStore)
		require.True(t, ok)
		opts := tx.ApplyOptions(s.retryOpts...)
		assert.Equal(t, tx.DefaultMaxAttempts, opts.MaxAttempts)
		assert.Equal(t, tx.DefaultInitialBackoff, opts.InitialBackoff)
		assert.Equal(t, tx.DefaultMaxBackoff, opts.MaxBackoff)
		assert.Equal(t, pgx.TxOptions{}, s.pgxTxOpts)
	})

	t.Run("execSingleTx透传attemptCtx并在panic时安全回滚", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		s := newSQLStore(&stubDBTX{}, beginner, tx.WithIsoLevel(tx.Serializable), tx.WithRetry(2, 0, 0))

		attemptCtx, cancel := context.WithCancel(context.WithValue(context.Background(), retryTestCtxKey{}, "attempt-1"))
		panicVal := "boom"
		require.PanicsWithValue(t, panicVal, func() {
			_ = s.execSingleTx(attemptCtx, func(gotCtx context.Context, _ *Queries) error {
				assert.Equal(t, attemptCtx, gotCtx)
				cancel()
				panic(panicVal)
			})
		})

		require.Len(t, beginner.attempts, 1)
		assert.Equal(t, attemptCtx, beginner.attempts[0].beginCtx)
		assert.Equal(t, pgx.Serializable, beginner.attempts[0].opts.IsoLevel)
		assert.True(t, beginner.attempts[0].rolledBack)
		assert.False(t, beginner.attempts[0].committed)
		require.NoError(t, beginner.attempts[0].rollbackCtxErr)
		assert.True(t, beginner.attempts[0].rollbackHasDeadline)
	})

	t.Run("execSingleTx聚合非ErrTxClosed回滚错误并忽略ErrTxClosed", func(t *testing.T) {
		t.Parallel()
		bizErr := errors.New("business failed")
		rbErr := errors.New("rollback failed")

		beginner := &stubBeginner{
			rollbackFn: func(int, context.Context) error {
				return rbErr
			},
		}
		s := newRetryTestStore(&stubDBTX{}, beginner)
		err := s.execSingleTx(context.Background(), func(context.Context, *Queries) error {
			return bizErr
		})
		require.ErrorIs(t, err, bizErr)
		require.ErrorIs(t, err, rbErr)

		closedBeginner := &stubBeginner{
			rollbackFn: func(int, context.Context) error {
				return pgx.ErrTxClosed
			},
		}
		closedStore := newRetryTestStore(&stubDBTX{}, closedBeginner)
		err = closedStore.execSingleTx(context.Background(), func(context.Context, *Queries) error {
			return bizErr
		})
		require.ErrorIs(t, err, bizErr)
		assert.NotErrorIs(t, err, pgx.ErrTxClosed)
	})

	t.Run("execQuery不开启事务并透传queries与attemptCtx", func(t *testing.T) {
		t.Parallel()
		beginner := &stubBeginner{}
		dbStub := &stubDBTX{}
		s := newRetryTestStore(dbStub, beginner)
		ctx := context.WithValue(context.Background(), retryTestCtxKey{}, "query-ctx")

		err := s.execQuery(ctx, func(attemptCtx context.Context, q *Queries) error {
			assert.Equal(t, ctx, attemptCtx)
			assert.Equal(t, s.queries, q)
			return nil
		})
		require.NoError(t, err)
		assert.Empty(t, beginner.attempts)
	})
}
