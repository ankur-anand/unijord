package postgres

import (
	"errors"
	"math"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func TestPlanNewPublicationCreatesAdvancesAndSealsHeads(t *testing.T) {
	namespace := metastore.CopyNamespace([]byte("tenant/plan"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 7}
	fence := metastore.WriterFence{Shard: shard, Epoch: 3, Owner: metastore.OwnerIDFromString("writer-a")}
	keyA := metastore.CopyTimelineKey(namespace, []byte("a"))
	keyB := metastore.CopyTimelineKey(namespace, []byte("b"))
	publication := testChunkPublication(fence, 4, []metastore.TimelineMutation{
		{Key: keyA, ExpectedNextLSN: 2, LastLSN: 3, FirstTimestampMS: 12, LastTimestampMS: 13, SealAfterAppend: true},
		{Key: keyB, ExpectedNextLSN: 0, LastLSN: 0, FirstTimestampMS: 14, LastTimestampMS: 14},
	})
	stored := map[[32]byte]metastore.TimelineHead{
		keyA.Hash(): {Key: keyA, Shard: 7, NextLSN: 2, LastTimestampMS: 11, State: metastore.TimelineOpen, Revision: 8},
	}
	state := metastore.ShardState{Fence: fence, NextChunkSequence: 4, MaterializedBefore: 1}

	plan, err := planNewPublication(state, publication, stored)
	if err != nil {
		t.Fatalf("planNewPublication() error = %v", err)
	}
	if plan.nextChunkSequence != 5 || len(plan.heads) != 2 {
		t.Fatalf("plan = %+v", plan)
	}
	if plan.heads[0].insert || plan.heads[0].head.NextLSN != 4 ||
		plan.heads[0].head.Revision != 9 || plan.heads[0].head.State != metastore.TimelineSealed {
		t.Fatalf("updated head = %+v", plan.heads[0])
	}
	if !plan.heads[1].insert || plan.heads[1].head.NextLSN != 1 ||
		plan.heads[1].head.Revision != 1 || plan.heads[1].head.State != metastore.TimelineOpen {
		t.Fatalf("inserted head = %+v", plan.heads[1])
	}
	if stored[keyA.Hash()].NextLSN != 2 {
		t.Fatal("planner mutated its stored-head input")
	}
}

func TestPlanNewPublicationRejectsInvalidDurableTransitions(t *testing.T) {
	namespace := metastore.CopyNamespace([]byte("tenant/plan-errors"))
	shard := metastore.ShardKey{Namespace: namespace, Shard: 3}
	fence := metastore.WriterFence{Shard: shard, Epoch: 1, Owner: metastore.OwnerIDFromString("writer")}
	key := metastore.CopyTimelineKey(namespace, []byte("timeline"))
	mutation := metastore.TimelineMutation{
		Key: key, ExpectedNextLSN: 1, LastLSN: 1, FirstTimestampMS: 11, LastTimestampMS: 11,
	}
	publication := testChunkPublication(fence, 0, []metastore.TimelineMutation{mutation})
	base := metastore.TimelineHead{
		Key: key, Shard: 3, NextLSN: 1, LastTimestampMS: 10,
		State: metastore.TimelineOpen, Revision: 1,
	}
	state := metastore.ShardState{Fence: fence}
	if _, err := planNewPublication(state, publication,
		map[[32]byte]metastore.TimelineHead{}); !errors.Is(err, metastore.ErrConflict) {
		t.Fatalf("missing head with non-zero expected LSN error = %v", err)
	}

	cases := []struct {
		name string
		head metastore.TimelineHead
		want error
	}{
		{name: "sealed", head: withHead(base, func(h *metastore.TimelineHead) { h.State = metastore.TimelineSealed }), want: metastore.ErrSealed},
		{name: "LSN", head: withHead(base, func(h *metastore.TimelineHead) { h.NextLSN = 2 }), want: metastore.ErrConflict},
		{name: "timestamp", head: withHead(base, func(h *metastore.TimelineHead) { h.LastTimestampMS = 12 }), want: metastore.ErrConflict},
		{name: "revision", head: withHead(base, func(h *metastore.TimelineHead) { h.Revision = math.MaxUint64 }), want: metastore.ErrCorrupt},
		{name: "shard", head: withHead(base, func(h *metastore.TimelineHead) { h.Shard = 4 }), want: metastore.ErrConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := planNewPublication(state, publication,
				map[[32]byte]metastore.TimelineHead{key.Hash(): tc.head})
			if !errors.Is(err, tc.want) {
				t.Fatalf("planNewPublication() error = %v, want %v", err, tc.want)
			}
		})
	}

	stale := state
	stale.Fence.Owner = metastore.OwnerIDFromString("replacement")
	stale.NextChunkSequence = 9
	if _, err := planNewPublication(stale, publication,
		map[[32]byte]metastore.TimelineHead{key.Hash(): base}); !errors.Is(err, metastore.ErrStaleWriter) {
		t.Fatalf("stale fence precedence error = %v", err)
	}

	full := state
	full.NextChunkSequence = metastore.MaxActiveTailChunks
	if _, err := planNewPublication(full, testChunkPublication(fence,
		metastore.MaxActiveTailChunks, []metastore.TimelineMutation{mutation}),
		map[[32]byte]metastore.TimelineHead{key.Hash(): base}); !errors.Is(err, metastore.ErrTailFull) {
		t.Fatalf("tail bound error = %v", err)
	}
}

func withHead(head metastore.TimelineHead, mutate func(*metastore.TimelineHead)) metastore.TimelineHead {
	mutate(&head)
	return head
}
