package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestProducerMigrationUpgradesVersionThree(t *testing.T) {
	store := newPostgresTestStore(t, false)
	ctx := context.Background()
	for _, sql := range []string{foundationSQL, publicationSQL, materializerOwnerSQL} {
		if _, err := store.pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	request := producerOpenFixture("tenant/producer-upgrade")
	shard := metastore.ShardKey{Namespace: request.Key.Namespace, Shard: 7}
	claimTestShard(t, store, shard, "writer")
	before, err := store.Shard(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := store.OpenProducer(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := store.Shard(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if !metastore.SameWriterFence(before.Fence, after.Fence) || before.NextChunkSequence != after.NextChunkSequence || before.MaterializedBefore != after.MaterializedBefore {
		t.Fatal("producer migration changed existing shard authority")
	}
	var version int
	if err := store.pool.QueryRow(ctx, "SELECT max(version) FROM unijord_metastore.schema_migrations").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != SchemaVersion {
		t.Fatalf("version=%d want=%d", version, SchemaVersion)
	}
}

func TestProducerSchemaRejectsInvalidState(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer-constraints")
	if _, err := store.OpenProducer(ctx, request); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		sql  string
		arg  any
		code string
	}{
		{"null-incarnation", "incarnation_id", nil, "23502"},
		{"zero-incarnation", "incarnation_id", make([]byte, 16), "23514"},
		{"short-incarnation", "incarnation_id", []byte{1}, "23514"},
		{"zero-producer", "producer_id", make([]byte, 16), "23514"},
		{"short-producer", "producer_id", []byte{1}, "23514"},
		{"zero-epoch", "epoch", encodeUint64(0), "23514"},
		{"null-epoch", "epoch", nil, "23502"},
		{"short-sequence", "next_sequence", []byte{1}, "23514"},
		{"null-sequence", "next_sequence", nil, "23502"},
		{"invalid-status", "state", int16(3), "23514"},
		{"null-status", "state", nil, "23502"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.pool.Exec(ctx, "UPDATE unijord_metastore.producers SET "+tc.sql+"=$1", tc.arg)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != tc.code {
				t.Fatalf("constraint error=%v want=%s", err, tc.code)
			}
		})
	}
}
