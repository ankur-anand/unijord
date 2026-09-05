package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// commitTransaction preserves the distinction between a transaction that
// definitely did not commit and one whose durable outcome cannot be known.
func commitTransaction(ctx context.Context, tx pgx.Tx, operation string) error {
	// pgx does not send COMMIT when the context is already canceled. Classify
	// that case before calling Commit so callers do not reconcile a rollback.
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		wrapped := fmt.Errorf("metastore/postgres: commit %s: %w", operation, err)
		// PostgreSQL explicitly rejected the commit and rolled the transaction
		// back, so this is also a definite failure.
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return wrapped
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && (pgErr.Code == "40001" || pgErr.Code == "40P01") {
			// Serialization failure and deadlock detection explicitly abort
			// the transaction. Preserve the SQLSTATE so callers can retry the
			// complete transaction without reconciling an unknown outcome.
			return wrapped
		}
		// Other errors remain conservative, including SQLSTATE 40003
		// (statement_completion_unknown) and a reply lost after commit.
		return errors.Join(metastore.ErrOutcomeUnknown, wrapped)
	}
	return nil
}
