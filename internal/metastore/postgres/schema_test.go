package postgres

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestFoundationSchemaIsEmbedded(t *testing.T) {
	if SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", SchemaVersion)
	}
	for _, fragment := range []string{
		"CREATE SCHEMA IF NOT EXISTS unijord_metastore",
		"unijord_metastore.namespaces",
		"unijord_metastore.shards",
		"unijord_metastore.shard_writers",
		"unijord_metastore.shard_materializers",
		"VALUES (1)",
	} {
		if !strings.Contains(foundationSQL, fragment) {
			t.Fatalf("foundation migration is missing %q", fragment)
		}
	}
}

func TestFoundationSchemaConstraints(t *testing.T) {
	dsn := os.Getenv("UNIJORD_METASTORE_POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("UNIJORD_POSTGRES_TEST_DSN")
	}
	if dsn == "" {
		t.Skip("set UNIJORD_METASTORE_POSTGRES_TEST_DSN to run PostgreSQL schema tests")
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer tx.Rollback() // Restore any schema that existed before this test.

	if _, err := tx.ExecContext(ctx, "DROP SCHEMA IF EXISTS unijord_metastore CASCADE"); err != nil {
		t.Fatalf("drop test schema: %v", err)
	}
	if _, err := tx.ExecContext(ctx, foundationSQL); err != nil {
		t.Fatalf("apply foundation migration: %v", err)
	}
	if _, err := tx.ExecContext(ctx, foundationSQL); err != nil {
		t.Fatalf("reapply foundation migration: %v", err)
	}

	var versions int
	if err := tx.QueryRowContext(ctx,
		"SELECT count(*) FROM unijord_metastore.schema_migrations WHERE version = 1").Scan(&versions); err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if versions != 1 {
		t.Fatalf("schema version rows = %d, want 1", versions)
	}

	namespaceHash := bytesOf(32, 0x11)
	namespaceKey := []byte("tenant/acme")
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		VALUES ($1, $2)`, namespaceHash, namespaceKey); err != nil {
		t.Fatalf("insert namespace: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO unijord_metastore.shards(namespace_hash, shard)
		VALUES ($1, 7)`, namespaceHash); err != nil {
		t.Fatalf("insert shard: %v", err)
	}

	maxUint64 := bytesOf(8, 0xff)
	epochOne := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO unijord_metastore.shard_writers(
			namespace_hash, shard, writer_epoch, writer_owner, next_chunk_sequence)
		VALUES ($1, 7, $2, $3, $4)`, namespaceHash, epochOne, []byte("writer-a"), maxUint64); err != nil {
		t.Fatalf("insert full-range writer state: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO unijord_metastore.shard_materializers(
			namespace_hash, shard, materializer_epoch, materializer_owner, materialized_before)
		VALUES ($1, 7, $2, NULL, $2)`, namespaceHash, make([]byte, 8)); err != nil {
		t.Fatalf("insert unclaimed materializer state: %v", err)
	}

	assertSQLRejected(t, ctx, tx, `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		VALUES ($1, $2)`, bytesOf(31, 0x22), []byte("bad-hash"))
	assertSQLRejected(t, ctx, tx, `
		INSERT INTO unijord_metastore.shards(namespace_hash, shard)
		VALUES ($1, 8)`, bytesOf(32, 0x33))

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO unijord_metastore.shards(namespace_hash, shard)
		VALUES ($1, 8), ($1, 9)`, namespaceHash); err != nil {
		t.Fatalf("insert constraint-test shards: %v", err)
	}
	assertSQLRejected(t, ctx, tx, `
		INSERT INTO unijord_metastore.shard_writers(
			namespace_hash, shard, writer_epoch, writer_owner, next_chunk_sequence)
		VALUES ($1, 8, $2, $3, $2)`, namespaceHash, make([]byte, 8), []byte("writer-b"))
	assertSQLRejected(t, ctx, tx, `
		INSERT INTO unijord_metastore.shard_materializers(
			namespace_hash, shard, materializer_epoch, materializer_owner, materialized_before)
		VALUES ($1, 9, $2, $3, $2)`, namespaceHash, make([]byte, 8), []byte("owner-with-zero-epoch"))
}

func assertSQLRejected(t *testing.T, ctx context.Context, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, "SAVEPOINT constraint_case"); err != nil {
		t.Fatalf("create savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, query, args...); err == nil {
		t.Fatal("invalid row was accepted")
	}
	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT constraint_case"); err != nil {
		t.Fatalf("rollback savepoint: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "RELEASE SAVEPOINT constraint_case"); err != nil {
		t.Fatalf("release savepoint: %v", err)
	}
}

func bytesOf(size int, value byte) []byte {
	result := make([]byte, size)
	for i := range result {
		result[i] = value
	}
	return result
}
