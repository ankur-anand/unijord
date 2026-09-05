package postgres

import (
	"context"
	"errors"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrateIsIdempotentAndSerialized(t *testing.T) {
	store := newPostgresTestStore(t, false)

	start := make(chan struct{})
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			<-start
			errs <- store.Migrate(context.Background())
		}()
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("idempotent Migrate() error = %v", err)
	}
}

func TestClaimDirectShardsCreatesReplaysAndTakesOver(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespaceA := metastore.CopyNamespace([]byte("tenant/a"))
	namespaceB := metastore.CopyNamespace([]byte("tenant/b"))
	shards := []metastore.ShardKey{
		{Namespace: namespaceA, Shard: 9},
		{Namespace: namespaceB, Shard: 2},
		{Namespace: namespaceA, Shard: 1},
	}
	firstRequest := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer-a"), Shards: shards,
	}
	first, err := store.ClaimDirectShards(context.Background(), firstRequest)
	if err != nil {
		t.Fatalf("first ClaimDirectShards() error = %v", err)
	}
	assertDirectLeases(t, firstRequest, first, 1)

	replayed, err := store.ClaimDirectShards(context.Background(), firstRequest)
	if err != nil {
		t.Fatalf("replayed ClaimDirectShards() error = %v", err)
	}
	assertDirectLeases(t, firstRequest, replayed, 1)

	replacementRequest := metastore.DirectShardClaimRequest{
		Owner:  metastore.OwnerIDFromString("writer-b"),
		Shards: []metastore.ShardKey{shards[2], shards[0], shards[1]},
	}
	replacement, err := store.ClaimDirectShards(context.Background(), replacementRequest)
	if err != nil {
		t.Fatalf("replacement ClaimDirectShards() error = %v", err)
	}
	assertDirectLeases(t, replacementRequest, replacement, 2)
	for _, shard := range shards {
		state, err := store.Shard(context.Background(), shard)
		if err != nil {
			t.Fatalf("Shard(%d) error = %v", shard.Shard, err)
		}
		if state.Fence.Epoch != 2 || !state.Fence.Owner.Equal(replacementRequest.Owner) ||
			state.NextChunkSequence != 0 || state.MaterializedBefore != 0 {
			t.Fatalf("Shard(%d) = %+v", shard.Shard, state)
		}
	}
}

func TestClaimDirectShardsMixesNewAndExistingShards(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/mixed-claim"))
	existing := metastore.ShardKey{Namespace: namespace, Shard: 2}
	first := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer-a"), Shards: []metastore.ShardKey{existing},
	}
	if _, err := store.ClaimDirectShards(context.Background(), first); err != nil {
		t.Fatalf("seed ClaimDirectShards() error = %v", err)
	}

	request := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer-b"),
		Shards: []metastore.ShardKey{
			{Namespace: namespace, Shard: 9},
			existing,
			{Namespace: namespace, Shard: 1},
		},
	}
	leases, err := store.ClaimDirectShards(context.Background(), request)
	if err != nil {
		t.Fatalf("mixed ClaimDirectShards() error = %v", err)
	}
	if err := metastore.ValidateDirectShardClaimResult(request, leases); err != nil {
		t.Fatalf("mixed leases invalid: %v", err)
	}
	for i, lease := range leases {
		wantEpoch := uint64(1)
		if request.Shards[i].Shard == existing.Shard {
			wantEpoch = 2
		}
		if lease.State.Fence.Epoch != wantEpoch ||
			!lease.State.Fence.Owner.Equal(request.Owner) ||
			lease.State.NextChunkSequence != 0 || lease.State.MaterializedBefore != 0 {
			t.Fatalf("lease[%d] = %+v, want epoch=%d", i, lease, wantEpoch)
		}
	}
}

func TestClaimDirectShardsOppositeOrdersDoNotDeadlock(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/concurrent"))
	first := metastore.ShardKey{Namespace: namespace, Shard: 1}
	second := metastore.ShardKey{Namespace: namespace, Shard: 2}
	seed := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("seed"), Shards: []metastore.ShardKey{first, second},
	}
	if _, err := store.ClaimDirectShards(context.Background(), seed); err != nil {
		t.Fatalf("seed ClaimDirectShards() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	requests := []metastore.DirectShardClaimRequest{
		{Owner: metastore.OwnerIDFromString("writer-a"), Shards: []metastore.ShardKey{first, second}},
		{Owner: metastore.OwnerIDFromString("writer-b"), Shards: []metastore.ShardKey{second, first}},
	}
	type result struct {
		request metastore.DirectShardClaimRequest
		leases  []metastore.DirectShardLease
		err     error
	}
	results := make(chan result, len(requests))
	var ready sync.WaitGroup
	ready.Add(len(requests))
	for _, request := range requests {
		go func(request metastore.DirectShardClaimRequest) {
			ready.Done()
			<-start
			leases, err := store.ClaimDirectShards(ctx, request)
			results <- result{request: request, leases: leases, err: err}
		}(request)
	}
	ready.Wait()
	close(start)
	for range requests {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent ClaimDirectShards() error = %v", result.err)
		}
		if err := metastore.ValidateDirectShardClaimResult(result.request, result.leases); err != nil {
			t.Fatalf("concurrent leases invalid: %v", err)
		}
	}
	for _, shard := range []metastore.ShardKey{first, second} {
		state, err := store.Shard(context.Background(), shard)
		if err != nil {
			t.Fatalf("Shard(%d) error = %v", shard.Shard, err)
		}
		if state.Fence.Epoch != 3 {
			t.Fatalf("Shard(%d) epoch = %d, want 3", shard.Shard, state.Fence.Epoch)
		}
	}
}

func TestClaimDirectShardsRollsBackOnEpochExhaustion(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/exhaustion"))
	shards := []metastore.ShardKey{
		{Namespace: namespace, Shard: 1},
		{Namespace: namespace, Shard: 2},
	}
	ownerA := metastore.OwnerIDFromString("writer-a")
	if _, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner: ownerA, Shards: shards,
	}); err != nil {
		t.Fatalf("seed ClaimDirectShards() error = %v", err)
	}
	hash := namespace.Hash()
	if _, err := store.pool.Exec(context.Background(), `
		UPDATE unijord_metastore.shard_writers
		SET writer_epoch = $3
		WHERE namespace_hash = $1 AND shard = $2`, hash[:], int64(shards[1].Shard), encodeUint64(math.MaxUint64)); err != nil {
		t.Fatalf("set exhausted epoch: %v", err)
	}

	_, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer-b"), Shards: shards,
	})
	if !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("exhausted ClaimDirectShards() error = %v", err)
	}
	state, err := store.Shard(context.Background(), shards[0])
	if err != nil {
		t.Fatalf("Shard() after rollback error = %v", err)
	}
	if state.Fence.Epoch != 1 || !state.Fence.Owner.Equal(ownerA) {
		t.Fatalf("non-exhausted shard changed despite rollback: %+v", state)
	}
}

func TestShardAndClaimRejectMissingOrInvalidState(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/corrupt"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 8}
	if _, err := store.Shard(context.Background(), shard); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("missing Shard() error = %v", err)
	}

	hash := namespace.Hash()
	if _, err := store.pool.Exec(context.Background(), `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		VALUES ($1, $2)`, hash[:], namespace.Bytes()); err != nil {
		t.Fatalf("insert incomplete namespace: %v", err)
	}
	if _, err := store.pool.Exec(context.Background(), `
		INSERT INTO unijord_metastore.shards(namespace_hash, shard)
		VALUES ($1, $2)`, hash[:], int64(shard.Shard)); err != nil {
		t.Fatalf("insert incomplete shard: %v", err)
	}
	if _, err := store.Shard(context.Background(), shard); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("incomplete Shard() error = %v", err)
	}
	if _, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer"), Shards: []metastore.ShardKey{shard},
	}); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("claim incomplete shard error = %v", err)
	}
}

func TestClaimDirectShardsRejectsNamespaceDigestCollision(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/collision"))
	hash := namespace.Hash()
	if _, err := store.pool.Exec(context.Background(), `
		INSERT INTO unijord_metastore.namespaces(namespace_hash, namespace_key)
		VALUES ($1, $2)`, hash[:], []byte("different-exact-bytes")); err != nil {
		t.Fatalf("insert collision fixture: %v", err)
	}
	_, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner:  metastore.OwnerIDFromString("writer"),
		Shards: []metastore.ShardKey{{Namespace: namespace, Shard: 1}},
	})
	if !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("namespace collision error = %v", err)
	}
}

func TestClaimDirectShardsHonorsCanceledContext(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.ClaimDirectShards(ctx, metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer"),
		Shards: []metastore.ShardKey{{
			Namespace: metastore.CopyNamespace([]byte("tenant/canceled")), Shard: 1,
		}},
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("canceled claim error = %v", err)
	}
}

func assertDirectLeases(t *testing.T, request metastore.DirectShardClaimRequest, leases []metastore.DirectShardLease, epoch uint64) {
	t.Helper()
	if err := metastore.ValidateDirectShardClaimResult(request, leases); err != nil {
		t.Fatalf("ValidateDirectShardClaimResult() error = %v", err)
	}
	for i, lease := range leases {
		if lease.State.Fence.Epoch != epoch || lease.State.NextChunkSequence != 0 ||
			lease.State.MaterializedBefore != 0 {
			t.Fatalf("lease[%d] = %+v", i, lease)
		}
	}
}

func newPostgresTestStore(t *testing.T, migrate bool) *Store {
	t.Helper()
	dsn := os.Getenv("UNIJORD_METASTORE_POSTGRES_TEST_DSN")
	if dsn == "" {
		dsn = os.Getenv("UNIJORD_POSTGRES_TEST_DSN")
	}
	if dsn == "" {
		t.Skip("set UNIJORD_METASTORE_POSTGRES_TEST_DSN to run PostgreSQL tests")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if _, err := pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS unijord_metastore CASCADE"); err != nil {
		t.Fatalf("reset metastore schema: %v", err)
	}
	store, err := New(pool)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if migrate {
		if err := store.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate() error = %v", err)
		}
	}
	return store
}
