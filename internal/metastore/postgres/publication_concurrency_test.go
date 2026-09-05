package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPublicationOppositeNewHeadOrdersDoNotDeadlock(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	namespace := metastore.CopyNamespace([]byte("tenant/new-head-race"))
	shards := []metastore.ShardKey{{Namespace: namespace, Shard: 1}, {Namespace: namespace, Shard: 2}}
	leases, err := store.ClaimDirectShards(ctx, metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer"), Shards: shards,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Hold each transaction before its second insert. Once both
	// sessions are waiting, release the gate: opposite insertion orders form
	// a lock cycle, whereas canonical orders allow one complete publication.
	if _, err := store.pool.Exec(ctx, `
		CREATE FUNCTION unijord_metastore.test_head_gate() RETURNS trigger AS $$
		BEGIN
			IF current_setting('unijord.test_first_head', true) IS DISTINCT FROM 'inserted' THEN
				PERFORM set_config('unijord.test_first_head', 'inserted', true);
			ELSE
				PERFORM pg_advisory_xact_lock(9614596);
			END IF;
			RETURN NEW;
		END $$ LANGUAGE plpgsql;
		CREATE TRIGGER test_head_gate BEFORE INSERT ON unijord_metastore.timeline_heads
		FOR EACH ROW EXECUTE FUNCTION unijord_metastore.test_head_gate()`); err != nil {
		t.Fatal(err)
	}
	gate, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gate.Rollback(context.Background()) }()
	if _, err := gate.Exec(ctx, "SELECT pg_advisory_xact_lock(9614596)"); err != nil {
		t.Fatal(err)
	}
	keys := []metastore.TimelineKey{
		metastore.CopyTimelineKey(namespace, []byte("a")),
		metastore.CopyTimelineKey(namespace, []byte("b")),
	}
	mutations := []metastore.TimelineMutation{
		{Key: keys[0], LastLSN: 0, FirstTimestampMS: 1, LastTimestampMS: 1},
		{Key: keys[1], LastLSN: 0, FirstTimestampMS: 1, LastTimestampMS: 1},
	}
	publications := []metastore.ChunkPublication{
		testChunkPublication(leases[0].State.Fence, 0, mutations),
		testChunkPublication(leases[1].State.Fence, 0, []metastore.TimelineMutation{mutations[1], mutations[0]}),
	}
	arrived, release := make(chan struct{}, 2), make(chan struct{})
	type outcome struct {
		index  int
		result metastore.ChunkPublicationResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	pids := make([]int32, 2)
	for i := range publications {
		tx, err := store.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pids[i] = int32(tx.Conn().PgConn().PID())
		go func(i int, tx pgx.Tx) {
			result, err := applyNewChunkPublication(ctx, headInsertBarrierTx{
				Tx: tx, arrived: arrived, release: release,
			}, publications[i])
			if err == nil {
				err = commitTransaction(ctx, tx, "concurrent publication")
			}
			_ = tx.Rollback(context.Background())
			outcomes <- outcome{index: i, result: result, err: err}
		}(i, tx)
	}
	for range publications {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	close(release) // Both queries have already observed the heads as absent.
	waitForPublicationLocks(t, ctx, store, pids)
	if err := gate.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	results := []outcome{<-outcomes, <-outcomes}
	winner := -1
	for _, result := range results {
		if result.err != nil {
			if !errors.Is(result.err, metastore.ErrConflict) {
				t.Fatalf("losing publication error = %v, want ErrConflict", result.err)
			}
			continue
		}
		if winner != -1 {
			t.Fatal("both shards created the same timelines")
		}
		winner = result.index
		if err := metastore.ValidateChunkPublicationResult(publications[winner], result.result); err != nil {
			t.Fatalf("acknowledgement changed input order: %v", err)
		}
	}
	if winner == -1 {
		t.Fatal("neither publication succeeded")
	}
	for _, key := range keys {
		head, err := store.Head(ctx, key)
		if err != nil || head.Shard != shards[winner].Shard || head.NextLSN != 1 {
			t.Fatalf("published head = %+v, error=%v", head, err)
		}
	}
	for i, shard := range shards {
		wantNext := uint64(0)
		if i == winner {
			wantNext = 1
		}
		state, err := store.Shard(ctx, shard)
		if err != nil || state.NextChunkSequence != wantNext {
			t.Fatalf("shard state = %+v, error=%v, want next=%d", state, err, wantNext)
		}
	}
	if _, err := queryChunk(ctx, store.pool, namespace.Hash(), shards[1-winner].Shard, 0); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("losing publication left a chunk: %v", err)
	}
}

// Synchronize immediately before SQL insertion without changing the SQL or
// its lock behavior. The context also releases a peer if setup fails.
type headInsertBarrierTx struct {
	pgx.Tx
	arrived chan<- struct{}
	release <-chan struct{}
}

func (tx headInsertBarrierTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.Contains(sql, "INSERT INTO unijord_metastore.timeline_heads(") {
		tx.arrived <- struct{}{}
		select {
		case <-tx.release:
		case <-ctx.Done():
			return pgconn.CommandTag{}, ctx.Err()
		}
	}
	return tx.Tx.Exec(ctx, sql, args...)
}

func waitForPublicationLocks(t *testing.T, ctx context.Context, store *Store, pids []int32) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting int
		if err := store.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE pid = ANY($1::int[]) AND wait_event_type = 'Lock'`, pids).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting == len(pids) {
			return
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("waiting for publication lock interleaving: %v", ctx.Err())
		}
	}
}
