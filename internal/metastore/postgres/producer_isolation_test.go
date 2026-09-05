package postgres

import (
	"context"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func TestProducerLifecycleIsIndependentOfShardAndTimeline(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer-independent")
	producer, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	shard := metastore.ShardKey{Namespace: request.Key.Namespace, Shard: 7}
	lease := claimTestShard(t, store, shard, "writer-a")
	key := metastore.CopyTimelineKey(request.Key.Namespace, []byte("open-timeline"))
	publication := testChunkPublication(lease.State.Fence, 0, []metastore.TimelineMutation{
		{Key: key, LastLSN: 0, FirstTimestampMS: 1, LastTimestampMS: 1},
	})
	result := applyAndCommitPublication(t, store, publication)
	replacement := claimTestShard(t, store, shard, "writer-b")
	afterTakeover, err := store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, afterTakeover, producer)
	if _, err := store.CloseProducer(ctx, producer.Fence); err != nil {
		t.Fatal(err)
	}
	current, err := store.Head(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if !metastore.SameTimelineHead(current, result.Heads[0]) || current.State != metastore.TimelineOpen {
		t.Fatalf("producer close changed timeline: %+v", current)
	}
	state, err := store.Shard(ctx, shard)
	if err != nil {
		t.Fatal(err)
	}
	if !metastore.SameWriterFence(state.Fence, replacement.State.Fence) || state.NextChunkSequence != 1 {
		t.Fatalf("producer close changed shard: %+v", state)
	}
}
