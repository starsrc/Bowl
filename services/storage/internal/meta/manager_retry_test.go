package meta

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
)

func TestIsRetryableTxError(t *testing.T) {
	t.Run("nil error is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableTxError(nil))
	})

	t.Run("errRetryTx is retryable", func(t *testing.T) {
		assert.True(t, isRetryableTxError(errRetryTx))
	})

	t.Run("wrapped errRetryTx is retryable", func(t *testing.T) {
		wrapped := errors.Join(errors.New("outer"), errRetryTx)
		assert.True(t, isRetryableTxError(wrapped))
	})

	t.Run("PostgreSQL 40001 serialization_failure is retryable", func(t *testing.T) {
		err := &pgconn.PgError{Code: "40001"}
		assert.True(t, isRetryableTxError(err))
	})

	t.Run("PostgreSQL 40P01 deadlock_detected is retryable", func(t *testing.T) {
		err := &pgconn.PgError{Code: "40P01"}
		assert.True(t, isRetryableTxError(err))
	})

	t.Run("other PostgreSQL error codes are not retryable", func(t *testing.T) {
		err := &pgconn.PgError{Code: "23505"} // unique_violation
		assert.False(t, isRetryableTxError(err))
	})

	t.Run("arbitrary error is not retryable", func(t *testing.T) {
		assert.False(t, isRetryableTxError(errors.New("some business error")))
	})
}

func TestExecWriteTxWithRetry_Behavior(t *testing.T) {
	ctx := context.Background()

	t.Run("non-retryable error returns immediately without retry", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		m := &manager{store: mockStore}

		expectedErr := errors.New("fatal database error")
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			Return(expectedErr).
			Once()

		err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
			return nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, expectedErr)
	})

	t.Run("retryable error retries and succeeds", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		m := &manager{store: mockStore}

		attempts := 0
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				attempts++
				if attempts < 3 {
					return &pgconn.PgError{Code: "40001"}
				}
				return nil
			}).
			Times(3)

		err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("exceeded max retries returns formatted error", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		m := &manager{store: mockStore}

		attempts := 0
		retryErr := &pgconn.PgError{Code: "40P01"}
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				attempts++
				return retryErr
			}).
			Times(16)

		err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
			return nil
		})
		require.Error(t, err)
		assert.Equal(t, 16, attempts)
		assert.Contains(t, err.Error(), "exceeded max transaction retries (16)")
		assert.ErrorIs(t, err, retryErr)
	})

	t.Run("context cancelled aborts retry immediately", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		m := &manager{store: mockStore}

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := m.execWriteTxWithRetry(cancelCtx, func(q db.Querier) error {
			return nil
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})
}
