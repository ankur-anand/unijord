package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/ankur-anand/unijord/internal/chunkref"
	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/ankur-anand/unijord/internal/ujtc"
)

func TestApplyNewChunkPublicationPersistsChunkAndHeads(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/publication-kernel"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 9}
	lease := claimTestShard(t, store, shard, "writer-a")
	keyA := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
	keyB := metastore.CopyTimelineKey(namespace, []byte("timeline-b"))
	first := testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{
		{Key: keyB, ExpectedNextLSN: 0, LastLSN: 0, FirstTimestampMS: 12, LastTimestampMS: 12, SealAfterAppend: true},
		{Key: keyA, ExpectedNextLSN: 0, LastLSN: 1, FirstTimestampMS: 10, LastTimestampMS: 11},
	})

	result := applyAndCommitPublication(t, store, first)
	if err := metastore.ValidateChunkPublicationResult(first, result); err != nil {
		t.Fatalf("publication result invalid: %v", err)
	}
	if result.Replayed || !result.WriterFenceActive ||
		result.Heads[0].State != metastore.TimelineSealed || result.Heads[1].NextLSN != 2 {
		t.Fatalf("publication result = %+v", result)
	}

	headA, err := store.Head(context.Background(), keyA)
	if err != nil {
		t.Fatalf("Head(A) error = %v", err)
	}
	if headA.NextLSN != 2 || headA.Revision != 1 || headA.State != metastore.TimelineOpen {
		t.Fatalf("Head(A) = %+v", headA)
	}
	lookups, err := store.LookupHeads(context.Background(), []metastore.TimelineKey{
		keyB, metastore.CopyTimelineKey(namespace, []byte("missing")), keyA,
	})
	if err != nil {
		t.Fatalf("LookupHeads() error = %v", err)
	}
	if !lookups[0].Found || lookups[1].Found || !lookups[2].Found ||
		lookups[0].Head.State != metastore.TimelineSealed {
		t.Fatalf("LookupHeads() = %+v", lookups)
	}

	stored, err := queryChunk(context.Background(), store.pool, namespace.Hash(), shard.Shard, 0)
	if err != nil {
		t.Fatalf("queryChunk() error = %v", err)
	}
	if !chunkref.Same(stored.ref, first.Chunk) ||
		stored.publicationHash != metastore.HashChunkPublication(first) {
		t.Fatalf("stored chunk = %+v hash=%x", stored.ref, stored.publicationHash)
	}
	shardState, err := store.Shard(context.Background(), shard)
	if err != nil {
		t.Fatalf("Shard() error = %v", err)
	}
	if shardState.NextChunkSequence != 1 || shardState.MaterializedBefore != 0 {
		t.Fatalf("Shard() = %+v", shardState)
	}

	second := testChunkPublication(lease.State.Fence, 1, []metastore.TimelineMutation{{
		Key: keyA, ExpectedNextLSN: 2, LastLSN: 2,
		FirstTimestampMS: 13, LastTimestampMS: 13, SealAfterAppend: true,
	}})
	secondResult := applyAndCommitPublication(t, store, second)
	if secondResult.Heads[0].NextLSN != 3 || secondResult.Heads[0].Revision != 2 ||
		secondResult.Heads[0].State != metastore.TimelineSealed {
		t.Fatalf("second result = %+v", secondResult)
	}
}

func TestApplyNewChunkPublicationFailureChangesNothing(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/publication-rollback"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 3}
	lease := claimTestShard(t, store, shard, "writer-a")
	sealed := metastore.CopyTimelineKey(namespace, []byte("sealed"))
	first := testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{{
		Key: sealed, ExpectedNextLSN: 0, LastLSN: 0,
		FirstTimestampMS: 1, LastTimestampMS: 1, SealAfterAppend: true,
	}})
	applyAndCommitPublication(t, store, first)

	newKey := metastore.CopyTimelineKey(namespace, []byte("must-not-appear"))
	failing := testChunkPublication(lease.State.Fence, 1, []metastore.TimelineMutation{
		{Key: newKey, ExpectedNextLSN: 0, LastLSN: 0, FirstTimestampMS: 2, LastTimestampMS: 2},
		{Key: sealed, ExpectedNextLSN: 1, LastLSN: 1, FirstTimestampMS: 2, LastTimestampMS: 2},
	})
	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin publication: %v", err)
	}
	_, err = applyNewChunkPublication(context.Background(), tx, failing)
	if !errors.Is(err, metastore.ErrSealed) {
		t.Fatalf("failing publication error = %v", err)
	}
	if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
		t.Fatalf("rollback publication: %v", rollbackErr)
	}
	if _, err := store.Head(context.Background(), newKey); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("failed publication created a head: %v", err)
	}
	if _, err := queryChunk(context.Background(), store.pool, namespace.Hash(), shard.Shard, 1); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("failed publication created a chunk: %v", err)
	}
	state, err := store.Shard(context.Background(), shard)
	if err != nil || state.NextChunkSequence != 1 {
		t.Fatalf("failed publication advanced shard: state=%+v error=%v", state, err)
	}

	_ = claimTestShard(t, store, shard, "writer-b")
	stale := testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{{
		Key: newKey, ExpectedNextLSN: 0, LastLSN: 0,
		FirstTimestampMS: 2, LastTimestampMS: 2,
	}})
	tx, err = store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin stale publication: %v", err)
	}
	_, err = applyNewChunkPublication(context.Background(), tx, stale)
	if !errors.Is(err, metastore.ErrStaleWriter) {
		t.Fatalf("stale publication error = %v", err)
	}
	_ = tx.Rollback(context.Background())
}

func TestApplyNewChunkPublicationSerializesOneShardSequence(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/publication-race"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 5}
	lease := claimTestShard(t, store, shard, "writer-a")
	keys := []metastore.TimelineKey{
		metastore.CopyTimelineKey(namespace, []byte("timeline-a")),
		metastore.CopyTimelineKey(namespace, []byte("timeline-b")),
	}
	publications := []metastore.ChunkPublication{
		testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{{
			Key: keys[0], ExpectedNextLSN: 0, LastLSN: 0,
			FirstTimestampMS: 1, LastTimestampMS: 1,
		}}),
		testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{{
			Key: keys[1], ExpectedNextLSN: 0, LastLSN: 0,
			FirstTimestampMS: 1, LastTimestampMS: 1,
		}}),
	}
	publications[0].Chunk.Key = "chunks/race-a.ujtc"
	publications[1].Chunk.Key = "chunks/race-b.ujtc"
	publications[1].Chunk.SHA256[0] = 0x72

	type outcome struct {
		index int
		err   error
	}
	start := make(chan struct{})
	outcomes := make(chan outcome, len(publications))
	for i := range publications {
		go func(i int) {
			ctx := context.Background()
			tx, err := store.pool.Begin(ctx)
			if err == nil {
				<-start
				_, err = applyNewChunkPublication(ctx, tx, publications[i])
				if err == nil {
					err = tx.Commit(ctx)
				} else {
					_ = tx.Rollback(ctx)
				}
			}
			outcomes <- outcome{index: i, err: err}
		}(i)
	}
	close(start)
	winner := -1
	for range publications {
		result := <-outcomes
		if result.err == nil {
			if winner != -1 {
				t.Fatal("both publications committed at one shard sequence")
			}
			winner = result.index
			continue
		}
		if !errors.Is(result.err, metastore.ErrConflict) {
			t.Fatalf("losing publication error = %v", result.err)
		}
	}
	if winner == -1 {
		t.Fatal("neither publication committed")
	}
	lookups, err := store.LookupHeads(context.Background(), keys)
	if err != nil {
		t.Fatalf("LookupHeads() error = %v", err)
	}
	if !lookups[winner].Found || lookups[1-winner].Found {
		t.Fatalf("heads after race = %+v winner=%d", lookups, winner)
	}
	stored, err := queryChunk(context.Background(), store.pool, namespace.Hash(), shard.Shard, 0)
	if err != nil {
		t.Fatalf("queryChunk() error = %v", err)
	}
	if !chunkref.Same(stored.ref, publications[winner].Chunk) {
		t.Fatalf("stored chunk belongs to loser: got=%+v winner=%+v", stored.ref, publications[winner].Chunk)
	}
}

func TestApplyNewChunkPublicationRejectsTimelineOwnedByAnotherShard(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/cross-shard"))
	shards := []metastore.ShardKey{
		{Namespace: namespace, Shard: 1},
		{Namespace: namespace, Shard: 2},
	}
	request := metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString("writer-a"), Shards: shards,
	}
	leases, err := store.ClaimDirectShards(context.Background(), request)
	if err != nil {
		t.Fatalf("ClaimDirectShards() error = %v", err)
	}
	key := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
	first := testChunkPublication(leases[0].State.Fence, 0, []metastore.TimelineMutation{{
		Key: key, ExpectedNextLSN: 0, LastLSN: 0,
		FirstTimestampMS: 1, LastTimestampMS: 1,
	}})
	applyAndCommitPublication(t, store, first)

	crossShard := testChunkPublication(leases[1].State.Fence, 0, []metastore.TimelineMutation{{
		Key: key, ExpectedNextLSN: 1, LastLSN: 1,
		FirstTimestampMS: 2, LastTimestampMS: 2,
	}})
	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin cross-shard publication: %v", err)
	}
	_, err = applyNewChunkPublication(context.Background(), tx, crossShard)
	if !errors.Is(err, metastore.ErrConflict) {
		t.Fatalf("cross-shard publication error = %v", err)
	}
	if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
		t.Fatalf("rollback cross-shard publication: %v", rollbackErr)
	}
	if _, err := queryChunk(context.Background(), store.pool, namespace.Hash(), shards[1].Shard, 0); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("cross-shard publication created a chunk: %v", err)
	}
	head, err := store.Head(context.Background(), key)
	if err != nil {
		t.Fatalf("Head() error = %v", err)
	}
	if head.Shard != shards[0].Shard || head.NextLSN != 1 {
		t.Fatalf("Head() = %+v", head)
	}
}

func TestApplyNewChunkPublicationTreatsOccupiedExpectedSequenceAsCorrupt(t *testing.T) {
	store := newPostgresTestStore(t, true)
	namespace := metastore.CopyNamespace([]byte("tenant/chunk-collision"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 4}
	lease := claimTestShard(t, store, shard, "writer-a")
	key := metastore.CopyTimelineKey(namespace, []byte("timeline-a"))
	publication := testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{{
		Key: key, ExpectedNextLSN: 0, LastLSN: 0,
		FirstTimestampMS: 1, LastTimestampMS: 1,
	}})

	seed, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin corrupt fixture: %v", err)
	}
	if err := insertChunk(context.Background(), seed, publication.Chunk,
		metastore.HashChunkPublication(publication)); err != nil {
		_ = seed.Rollback(context.Background())
		t.Fatalf("insert corrupt fixture: %v", err)
	}
	if err := seed.Commit(context.Background()); err != nil {
		t.Fatalf("commit corrupt fixture: %v", err)
	}

	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin publication: %v", err)
	}
	_, err = applyNewChunkPublication(context.Background(), tx, publication)
	if !errors.Is(err, metastore.ErrCorrupt) || errors.Is(err, metastore.ErrConflict) {
		t.Fatalf("occupied expected sequence error = %v", err)
	}
	if rollbackErr := tx.Rollback(context.Background()); rollbackErr != nil {
		t.Fatalf("rollback publication: %v", rollbackErr)
	}
	if _, err := store.Head(context.Background(), key); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("corrupt publication created a head: %v", err)
	}
}

func applyAndCommitPublication(t *testing.T, store *Store,
	publication metastore.ChunkPublication,
) metastore.ChunkPublicationResult {
	t.Helper()
	ctx := context.Background()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin publication: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := applyNewChunkPublication(ctx, tx, publication)
	if err != nil {
		t.Fatalf("applyNewChunkPublication() error = %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit publication: %v", err)
	}
	return result
}

func claimTestShard(t *testing.T, store *Store, shard metastore.ShardKey,
	owner string,
) metastore.DirectShardLease {
	t.Helper()
	leases, err := store.ClaimDirectShards(context.Background(), metastore.DirectShardClaimRequest{
		Owner: metastore.OwnerIDFromString(owner), Shards: []metastore.ShardKey{shard},
	})
	if err != nil {
		t.Fatalf("ClaimDirectShards() error = %v", err)
	}
	return leases[0]
}

func testChunkPublication(fence metastore.WriterFence, sequence uint64,
	mutations []metastore.TimelineMutation,
) metastore.ChunkPublication {
	var recordCount uint32
	minTimestamp := mutations[0].FirstTimestampMS
	maxTimestamp := mutations[0].LastTimestampMS
	for _, mutation := range mutations {
		recordCount += uint32(mutation.LastLSN - mutation.ExpectedNextLSN + 1)
		minTimestamp = min(minTimestamp, mutation.FirstTimestampMS)
		maxTimestamp = max(maxTimestamp, mutation.LastTimestampMS)
	}
	hash := [32]byte{0x71}
	namespaceHash := fence.Shard.Namespace.Hash()
	return metastore.ChunkPublication{
		Fence: fence,
		Chunk: chunkref.Ref{
			Key: "chunks/test.ujtc", FormatVersion: ujtc.Version,
			NamespaceHash: namespaceHash, Shard: fence.Shard.Shard,
			WriterEpoch: fence.Epoch, Sequence: sequence,
			RecordCount: recordCount, TimelineCount: uint32(len(mutations)),
			SizeBytes:      ujtc.HeaderSize + ujtc.RecordHeaderSize + 1,
			MinTimestampMS: minTimestamp, MaxTimestampMS: maxTimestamp, SHA256: hash,
		},
		Mutations: mutations,
	}
}
