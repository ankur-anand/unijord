package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMaterializerOwnerConstraint(t *testing.T) {
	for _, upgrade := range []bool{false, true} {
		name := "fresh"
		if upgrade {
			name = "upgrade-from-version-2"
		}
		t.Run(name, func(t *testing.T) {
			store := newPostgresTestStore(t, false)
			ctx := context.Background()
			if upgrade {
				for _, sql := range []string{foundationSQL, publicationSQL} {
					if _, err := store.pool.Exec(ctx, sql); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := store.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if err := store.Migrate(ctx); err != nil {
				t.Fatalf("repeat migration: %v", err)
			}
			namespace := metastore.CopyNamespace([]byte("tenant/materializer-owner"))
			shard := metastore.ShardKey{Namespace: namespace, Shard: 1}
			claimTestShard(t, store, shard, "writer")
			hash := namespace.Hash()
			cases := []struct {
				name  string
				epoch uint64
				owner []byte
				valid bool
			}{
				{name: "unclaimed", valid: true},
				{name: "claimed", epoch: 1, owner: []byte("materializer"), valid: true},
				{name: "claimed-without-owner", epoch: 1},
				{name: "claimed-empty-owner", epoch: 1, owner: []byte{}},
				{name: "owner-without-epoch", owner: []byte("materializer")},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					_, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.shard_materializers
						SET materializer_epoch=$2, materializer_owner=$3
						WHERE namespace_hash=$1 AND shard=1`, hash[:], encodeUint64(tc.epoch), tc.owner)
					if tc.valid {
						if err != nil {
							t.Fatalf("valid materializer owner rejected: %v", err)
						}
						return
					}
					var pgErr *pgconn.PgError
					if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
						t.Fatalf("invalid materializer owner error = %v, want check violation", err)
					}
				})
			}
		})
	}
}

func TestMaterializerOwnerUpgradeRejectsInvalidExistingRow(t *testing.T) {
	store := newPostgresTestStore(t, false)
	ctx := context.Background()
	for _, sql := range []string{foundationSQL, publicationSQL} {
		if _, err := store.pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	namespace := metastore.CopyNamespace([]byte("tenant/invalid-materializer-upgrade"))
	claimTestShard(t, store, metastore.ShardKey{Namespace: namespace, Shard: 1}, "writer")
	hash := namespace.Hash()
	if _, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.shard_materializers
		SET materializer_epoch=$2 WHERE namespace_hash=$1`, hash[:], encodeUint64(1)); err != nil {
		t.Fatalf("create invalid version-2 fixture: %v", err)
	}
	err := store.Migrate(ctx)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("upgrade with invalid owner error = %v, want definite check violation", err)
	}
	var version int
	if err := store.pool.QueryRow(ctx, "SELECT max(version) FROM unijord_metastore.schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 2 {
		t.Fatalf("failed migration advanced schema to %d", version)
	}
	var epoch, owner []byte
	if err := store.pool.QueryRow(ctx, `SELECT materializer_epoch, materializer_owner
		FROM unijord_metastore.shard_materializers WHERE namespace_hash=$1`, hash[:]).Scan(&epoch, &owner); err != nil {
		t.Fatal(err)
	}
	value, err := decodeUint64(epoch)
	if err != nil || value != 1 || owner != nil {
		t.Fatalf("migration changed invalid row: epoch=%x owner=%x error=%v", epoch, owner, err)
	}
	if _, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.shard_materializers
		SET materializer_owner=$2 WHERE namespace_hash=$1`, hash[:], []byte("repaired-owner")); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("upgrade after repairing owner: %v", err)
	}
}
