package postgres

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

const (
	migrationLockKey int64 = 0x554a4d4554410001
	bootstrapSQL           = `
		CREATE SCHEMA IF NOT EXISTS unijord_metastore;
		CREATE TABLE IF NOT EXISTS unijord_metastore.schema_migrations (
			version integer PRIMARY KEY CHECK (version > 0),
			applied_at timestamptz NOT NULL DEFAULT transaction_timestamp()
		)`
)

type migration struct {
	version int
	sql     string
}

var migrations = [...]migration{
	{version: 1, sql: foundationSQL},
	{version: 2, sql: publicationSQL},
	{version: 3, sql: materializerOwnerSQL},
	{version: 4, sql: producersSQL},
}

// Migrate applies every pending metastore migration under one transaction-level
// advisory lock. DDL and its version marker commit together.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.checkContext(ctx); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("metastore/postgres: begin migration: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", migrationLockKey); err != nil {
		return fmt.Errorf("metastore/postgres: lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, bootstrapSQL); err != nil {
		return fmt.Errorf("metastore/postgres: bootstrap migrations: %w", err)
	}

	current, err := readSchemaVersion(ctx, tx)
	if err != nil {
		return err
	}
	for _, item := range migrations {
		if item.version <= current {
			continue
		}
		if item.version != current+1 {
			return fmt.Errorf("%w: migration gap current=%d next=%d", metastore.ErrCorrupt,
				current, item.version)
		}
		if _, err := tx.Exec(ctx, item.sql); err != nil {
			return fmt.Errorf("metastore/postgres: apply migration %d: %w", item.version, err)
		}
		current = item.version
	}
	if current != SchemaVersion {
		return fmt.Errorf("%w: schema version=%d supported=%d", metastore.ErrCorrupt,
			current, SchemaVersion)
	}
	if err := commitTransaction(ctx, tx, "migrations"); err != nil {
		return err
	}
	return nil
}

func readSchemaVersion(ctx context.Context, tx pgx.Tx) (int, error) {
	rows, err := tx.Query(ctx, `
		SELECT version
		FROM unijord_metastore.schema_migrations
		ORDER BY version`)
	if err != nil {
		return 0, fmt.Errorf("metastore/postgres: read migration versions: %w", err)
	}
	defer rows.Close()

	current := 0
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return 0, fmt.Errorf("metastore/postgres: scan migration version: %w", err)
		}
		if version != current+1 {
			return 0, fmt.Errorf("%w: migration versions are not contiguous at %d", metastore.ErrCorrupt,
				version)
		}
		if version > SchemaVersion {
			return 0, fmt.Errorf("%w: database schema=%d newer than supported=%d",
				metastore.ErrCorrupt, version, SchemaVersion)
		}
		current = version
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("metastore/postgres: iterate migration versions: %w", err)
	}
	return current, nil
}
