package postgres

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCommitTransactionTreatsPreCommitCancellationAsDefinite(t *testing.T) {
	store := newPostgresTestStore(t, true)
	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	namespace := metastore.CopyNamespace([]byte("tenant/canceled-commit"))
	hash := namespace.Hash()
	if _, err := tx.Exec(context.Background(), `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		VALUES ($1, $2)`, hash[:], namespace.Bytes()); err != nil {
		t.Fatalf("insert transaction fixture: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = commitTransaction(ctx, tx, "test transaction")
	if !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("commitTransaction() error = %v", err)
	}

	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback canceled transaction: %v", err)
	}
	var count int
	if err := store.pool.QueryRow(context.Background(), `
		SELECT count(*) FROM unijord_metastore.namespaces WHERE namespace_hash = $1`,
		hash[:]).Scan(&count); err != nil {
		t.Fatalf("query canceled transaction: %v", err)
	}
	if count != 0 {
		t.Fatalf("canceled transaction committed %d rows", count)
	}
}

func TestCommitTransactionPreservesKnownAndUnknownOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cause   error
		unknown bool
	}{
		{name: "deadlock", cause: &pgconn.PgError{Code: "40P01"}},
		{name: "statement-completion-unknown", cause: &pgconn.PgError{Code: "40003"}, unknown: true},
		{name: "lost-reply", cause: io.EOF, unknown: true},
		{name: "cancel-during-commit", cause: context.Canceled, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := commitTransaction(context.Background(), commitReplyTx{reply: tc.cause}, "test commit")
			if !errors.Is(err, tc.cause) || errors.Is(err, metastore.ErrOutcomeUnknown) != tc.unknown {
				t.Fatalf("commit error=%v, want cause=%v unknown=%v", err, tc.cause, tc.unknown)
			}
		})
	}
}

type commitReplyTx struct {
	pgx.Tx
	reply error
}

func (tx commitReplyTx) Commit(context.Context) error { return tx.reply }

func TestCommitTransactionSerializationFailureIsDefinite(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	if _, err := store.pool.Exec(ctx, `
		CREATE TABLE unijord_metastore.test_commit_serialization(id int PRIMARY KEY, value int);
		INSERT INTO unijord_metastore.test_commit_serialization VALUES(1, 0), (2, 0)`); err != nil {
		t.Fatal(err)
	}
	first, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = first.Rollback(context.Background()) }()
	second, err := store.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Rollback(context.Background()) }()
	// Both transactions read the same predicate, then update distinct rows.
	// The second commit must abort to prevent a serialization anomaly.
	for _, tx := range []pgx.Tx{first, second} {
		var value int
		if err := tx.QueryRow(ctx, "SELECT sum(value) FROM unijord_metastore.test_commit_serialization").Scan(&value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := first.Exec(ctx, "UPDATE unijord_metastore.test_commit_serialization SET value=1 WHERE id=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Exec(ctx, "UPDATE unijord_metastore.test_commit_serialization SET value=1 WHERE id=2"); err != nil {
		t.Fatal(err)
	}
	if err := commitTransaction(ctx, first, "first serializable transaction"); err != nil {
		t.Fatal(err)
	}
	err = commitTransaction(ctx, second, "second serializable transaction")
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "40001" || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("serialization commit error = %v, want definite 40001 rollback", err)
	}
	var value int
	if err := store.pool.QueryRow(ctx, "SELECT value FROM unijord_metastore.test_commit_serialization WHERE id=2").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 0 {
		t.Fatalf("aborted transaction persisted value=%d", value)
	}
}

func TestCommitTransactionTreatsServerRollbackAsDefinite(t *testing.T) {
	store := newPostgresTestStore(t, true)
	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(context.Background(), "SELECT 1 / 0"); err == nil {
		t.Fatal("division by zero unexpectedly succeeded")
	}
	err = commitTransaction(context.Background(), tx, "aborted transaction")
	if !errors.Is(err, pgx.ErrTxCommitRollback) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("commitTransaction() error = %v", err)
	}
}
