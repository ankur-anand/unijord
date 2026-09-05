package postgres

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func producerOpenFixture(namespace string) metastore.OpenProducerRequest {
	return metastore.OpenProducerRequest{
		Key:           metastore.ProducerKey{Namespace: metastore.CopyNamespace([]byte(namespace)), ID: metastore.ProducerID{1}},
		IncarnationID: metastore.ProducerIncarnationID{2},
	}
}

func requireProducerState(t *testing.T, got metastore.ProducerState, want metastore.ProducerState) {
	t.Helper()
	if !metastore.SameProducerFence(got.Fence, want.Fence) || got.Status != want.Status || got.NextSequence != want.NextSequence {
		t.Fatalf("producer=%+v, want=%+v", got, want)
	}
}

func TestProducerLifecyclePersistsAcrossStores(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer")
	state, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	want := metastore.ProducerState{
		Fence:  metastore.ProducerFence{Key: request.Key, IncarnationID: request.IncarnationID, Epoch: 1},
		Status: metastore.ProducerOpen,
	}
	requireProducerState(t, state, want)
	// A separate pool/process needs no in-memory producer cache to recover.
	other, err := Open(ctx, store.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	state, err = other.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want)
	conflict := request
	conflict.IncarnationID = metastore.ProducerIncarnationID{3}
	if _, err := other.OpenProducer(ctx, conflict); !errors.Is(err, metastore.ErrConflict) {
		t.Fatalf("implicit takeover error=%v", err)
	}
	// Lifecycle must preserve counters without assuming signed int64 bounds.
	hash := request.Key.Namespace.Hash()
	if _, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.producers
		SET next_sequence=$3 WHERE namespace_hash=$1 AND producer_id=$2`,
		hash[:], request.Key.ID[:], encodeUint64(math.MaxUint64)); err != nil {
		t.Fatal(err)
	}
	want.NextSequence = math.MaxUint64
	state, err = other.CloseProducer(ctx, want.Fence)
	if err != nil {
		t.Fatal(err)
	}
	want.Status = metastore.ProducerClosed
	requireProducerState(t, state, want)
	state, err = other.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want) // Delayed open cannot undo close.
	resume := metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: conflict.IncarnationID}
	state, err = other.ResumeProducer(ctx, resume)
	if err != nil {
		t.Fatal(err)
	}
	oldFence := want.Fence
	want.Fence.Epoch = 2
	want.Fence.IncarnationID = resume.IncarnationID
	want.Status = metastore.ProducerOpen
	requireProducerState(t, state, want)
	if _, err := store.CloseProducer(ctx, oldFence); !errors.Is(err, metastore.ErrStaleProducer) {
		t.Fatalf("zombie close error=%v", err)
	}
	state, err = store.ResumeProducer(ctx, resume)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want)
	state, err = other.CloseProducer(ctx, want.Fence)
	if err != nil {
		t.Fatal(err)
	}
	want.Status = metastore.ProducerClosed
	requireProducerState(t, state, want)
	state, err = store.ResumeProducer(ctx, resume)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want) // Delayed resume cannot reopen its epoch.
	state, err = store.CloseProducer(ctx, want.Fence)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want)
	state, err = store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, state, want)
	var shards, heads, chunks int
	if err := store.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM unijord_metastore.shards),
		(SELECT count(*) FROM unijord_metastore.timeline_heads),
		(SELECT count(*) FROM unijord_metastore.chunks)`).Scan(&shards, &heads, &chunks); err != nil {
		t.Fatal(err)
	}
	if shards != 0 || heads != 0 || chunks != 0 {
		t.Fatalf("lifecycle created unrelated state: shards=%d heads=%d chunks=%d", shards, heads, chunks)
	}
}

func TestProducerResumeRejectsInvalidTransitions(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer-invalid")
	state, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		req  metastore.ResumeProducerRequest
		want error
	}{
		{"same-incarnation", metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: request.IncarnationID}, metastore.ErrConflict},
		{"future-epoch", metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 2, IncarnationID: metastore.ProducerIncarnationID{3}}, metastore.ErrStaleProducer},
		{"zero-epoch", metastore.ResumeProducerRequest{Key: request.Key, IncarnationID: metastore.ProducerIncarnationID{3}}, metastore.ErrInvalidRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := store.ResumeProducer(ctx, tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("resume error=%v, want=%v", err, tc.want)
			}
		})
	}
	hash := request.Key.Namespace.Hash()
	if _, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.producers SET epoch=$3
		WHERE namespace_hash=$1 AND producer_id=$2`, hash[:], request.Key.ID[:], encodeUint64(math.MaxUint64)); err != nil {
		t.Fatal(err)
	}
	resume := metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: math.MaxUint64, IncarnationID: metastore.ProducerIncarnationID{3}}
	if _, err := store.ResumeProducer(ctx, resume); !errors.Is(err, metastore.ErrConflict) {
		t.Fatalf("exhausted epoch error=%v", err)
	}
	current, err := store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	state.Fence.Epoch = math.MaxUint64
	requireProducerState(t, current, state)
}

func TestProducerNamespaceIsolationAndMissingState(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	a := producerOpenFixture("tenant/a")
	b := producerOpenFixture("tenant/b")
	b.IncarnationID = metastore.ProducerIncarnationID{3}
	if _, err := store.OpenProducer(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetProducerState(ctx, b.Key); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("cross-namespace lookup error=%v", err)
	}
	if _, err := store.ResumeProducer(ctx, metastore.ResumeProducerRequest{Key: b.Key, ExpectedEpoch: 1, IncarnationID: b.IncarnationID}); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("resume missing producer error=%v", err)
	}
	if _, err := store.CloseProducer(ctx, metastore.ProducerFence{Key: b.Key, Epoch: 1, IncarnationID: b.IncarnationID}); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("close missing producer error=%v", err)
	}
	if _, err := store.OpenProducer(ctx, b); err != nil {
		t.Fatal(err)
	}
	// Exact namespace bytes must still be checked after a digest match.
	hash := a.Key.Namespace.Hash()
	if _, err := store.pool.Exec(ctx, `UPDATE unijord_metastore.namespaces SET namespace_key=$2
		WHERE namespace_hash=$1`, hash[:], []byte("corrupted identity")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetProducerState(ctx, a.Key); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("namespace collision read error=%v", err)
	}
	if _, err := store.OpenProducer(ctx, a); !errors.Is(err, metastore.ErrCorrupt) {
		t.Fatalf("namespace collision open error=%v", err)
	}
}
