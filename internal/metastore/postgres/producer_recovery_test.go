package postgres

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Lose the reply only after PostgreSQL has committed. No production failure
// hook is needed: the lifecycle kernel already accepts the pgx transaction.
type lostProducerReplyTx struct{ pgx.Tx }

func (tx lostProducerReplyTx) Commit(ctx context.Context) error {
	if err := tx.Tx.Commit(ctx); err != nil {
		return err
	}
	return io.ErrUnexpectedEOF
}

type cancelProducerUpdateTx struct {
	pgx.Tx
	cancel context.CancelFunc
}

func (tx cancelProducerUpdateTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := tx.Tx.Exec(ctx, sql, args...)
	if err == nil && strings.Contains(sql, "UPDATE unijord_metastore.producers") {
		tx.cancel()
	}
	return tag, err
}

func TestProducerLifecycleRecoversLostCommitReplies(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer-lost-reply")
	resume := metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: metastore.ProducerIncarnationID{3}}
	fence := metastore.ProducerFence{Key: request.Key, Epoch: 2, IncarnationID: resume.IncarnationID}
	for _, tc := range []struct {
		name       string
		transition producerTransition
		retry      func() (metastore.ProducerState, error)
	}{
		{"open", func(s metastore.ProducerState) (metastore.ProducerState, error) { return planProducerOpen(s, request) },
			func() (metastore.ProducerState, error) { return store.OpenProducer(ctx, request) }},
		{"resume", func(s metastore.ProducerState) (metastore.ProducerState, error) { return planProducerResume(s, resume) },
			func() (metastore.ProducerState, error) { return store.ResumeProducer(ctx, resume) }},
		{"close", func(s metastore.ProducerState) (metastore.ProducerState, error) { return planProducerClose(s, fence) },
			func() (metastore.ProducerState, error) { return store.CloseProducer(ctx, fence) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := store.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.Background()) }()
			if tc.name == "open" {
				if err := insertProducer(ctx, tx, request); err != nil {
					t.Fatal(err)
				}
			}
			_, err = finishProducerTransition(ctx, lostProducerReplyTx{tx}, request.Key, tc.name, tc.transition)
			if !errors.Is(err, metastore.ErrOutcomeUnknown) || !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("lost reply error=%v", err)
			}
			committed, err := store.GetProducerState(ctx, request.Key)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := tc.retry()
			if err != nil {
				t.Fatal(err)
			}
			requireProducerState(t, replayed, committed)
		})
	}
}

func TestProducerLifecycleCanceledCallsDoNotMutate(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx := context.Background()
	request := producerOpenFixture("tenant/producer-cancel")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.OpenProducer(canceled, request); !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("canceled open: %v", err)
	}
	if _, err := store.GetProducerState(ctx, request.Key); !errors.Is(err, metastore.ErrNotFound) {
		t.Fatalf("canceled open created a producer: %v", err)
	}
	state, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ResumeProducer(canceled, metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: metastore.ProducerIncarnationID{4}}); !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("canceled resume: %v", err)
	}
	if _, err := store.CloseProducer(canceled, state.Fence); !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("canceled close: %v", err)
	}
	if _, err := store.GetProducerState(canceled, request.Key); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read: %v", err)
	}
	current, err := store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, current, state)
	// Cancel after the SQL transition, before COMMIT, and prove rollback.
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	beforeCommit, cancelCommit := context.WithCancel(ctx)
	defer cancelCommit()
	_, err = finishProducerTransition(beforeCommit, cancelProducerUpdateTx{tx, cancelCommit}, request.Key, "canceled transition",
		func(s metastore.ProducerState) (metastore.ProducerState, error) {
			return planProducerClose(s, state.Fence)
		})
	if !errors.Is(err, context.Canceled) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("canceled before commit: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	current, err = store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	requireProducerState(t, current, state)
}
