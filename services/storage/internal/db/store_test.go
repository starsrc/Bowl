package db

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestMapPGXErr(t *testing.T) {
	otherErr := errors.New("unexpected driver error")
	serializationErr := &pgconn.PgError{Code: "40001", Message: "could not serialize access"}
	deadlockErr := &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}

	cases := []struct {
		name            string
		input           error
		wantNil         bool
		wantSentinel    error
		wantRetainPG    bool
		wantRetainCode  string
		wantPassthrough error
	}{
		{
			name:    "nil 输入返回 nil",
			input:   nil,
			wantNil: true,
		},
		{
			name:         "pgx.ErrNoRows 映射为 ErrNotFound 并吞并原错",
			input:        pgx.ErrNoRows,
			wantSentinel: ErrNotFound,
		},
		{
			name:         "包装的 pgx.ErrNoRows 映射为 ErrNotFound 并吞并原错",
			input:        fmt.Errorf("get disk: %w", pgx.ErrNoRows),
			wantSentinel: ErrNotFound,
		},
		{
			name:         "23505 唯一约束冲突映射为 ErrAlreadyExists 并吞并 PgError",
			input:        &pgconn.PgError{Code: "23505", ConstraintName: "disks_pkey", Message: "duplicate key"},
			wantSentinel: ErrAlreadyExists,
		},
		{
			name:         "23503 外键约束冲突映射为 ErrNotFound 并吞并 PgError",
			input:        &pgconn.PgError{Code: "23503", ConstraintName: "fk_disks_lineage", Message: "foreign key violation"},
			wantSentinel: ErrNotFound,
		},
		{
			name:         "23514 检查约束冲突映射为 ErrInvalidArgument 并吞并 PgError",
			input:        &pgconn.PgError{Code: "23514", ConstraintName: "chk_disk_commits_not_self_parent", Message: "check violation"},
			wantSentinel: ErrInvalidArgument,
		},
		{
			name:         "包装且无约束名的 23505 映射为 ErrAlreadyExists 并吞并 PgError",
			input:        fmt.Errorf("exec tx: %w", &pgconn.PgError{Code: "23505", Message: "duplicate key value"}),
			wantSentinel: ErrAlreadyExists,
		},
		{
			name:           "40001 串行化冲突原样保留供事务重试层识别",
			input:          serializationErr,
			wantRetainPG:   true,
			wantRetainCode: "40001",
		},
		{
			name:           "40P01 死锁检测原样保留供事务重试层识别",
			input:          deadlockErr,
			wantRetainPG:   true,
			wantRetainCode: "40P01",
		},
		{
			name:            "未分类错误原样透传",
			input:           otherErr,
			wantPassthrough: otherErr,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mapPGXErr(tc.input)
			if tc.wantNil {
				require.NoError(t, got)
				return
			}
			if tc.wantSentinel != nil {
				require.ErrorIs(t, got, tc.wantSentinel)
				require.NotErrorIs(t, got, pgx.ErrNoRows)
				_, hasPG := errors.AsType[*pgconn.PgError](got)
				require.False(t, hasPG, "领域错误不得保留 *pgconn.PgError: got=%v", got)
				return
			}
			if tc.wantRetainPG {
				pgErr, hasPG := errors.AsType[*pgconn.PgError](got)
				require.True(t, hasPG, "可重试事务错误必须保留 *pgconn.PgError: got=%v", got)
				require.Equal(t, tc.wantRetainCode, pgErr.Code)
				return
			}
			require.ErrorIs(t, got, tc.wantPassthrough)
		})
	}
}
