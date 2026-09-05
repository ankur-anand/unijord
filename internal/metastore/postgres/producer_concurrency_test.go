package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/internal/metastore"
)

func TestProducerConcurrentOpenAndResume(t *testing.T) {
	for _, identical := range []bool{false, true} {
		name := "different-incarnations"
		if identical {
			name = "same-incarnation"
		}
		t.Run(name, func(t *testing.T) {
			store := newPostgresTestStore(t, true)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := producerOpenFixture("tenant/producer-concurrent")
			type result struct {
				state metastore.ProducerState
				err   error
			}
			for _, resume := range []bool{false, true} {
				start := make(chan struct{})
				results := make(chan result, 2)
				for i := range 2 {
					incarnation := metastore.ProducerIncarnationID{byte(5 + i)}
					if identical {
						incarnation = metastore.ProducerIncarnationID{5}
					}
					if resume {
						incarnation[0] += 10
					}
					go func() {
						<-start
						var state metastore.ProducerState
						var err error
						if resume {
							state, err = store.ResumeProducer(ctx, metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: incarnation})
						} else {
							state, err = store.OpenProducer(ctx, metastore.OpenProducerRequest{Key: request.Key, IncarnationID: incarnation})
						}
						results <- result{state, err}
					}()
				}
				close(start)
				var winner metastore.ProducerState
				successes := 0
				for range 2 {
					got := <-results
					if got.err == nil {
						if successes != 0 {
							requireProducerState(t, got.state, winner)
						}
						winner = got.state
						successes++
					} else {
						want := metastore.ErrConflict
						if resume {
							want = metastore.ErrStaleProducer
						}
						if identical || !errors.Is(got.err, want) {
							t.Fatalf("resume=%t concurrent error=%v", resume, got.err)
						}
					}
				}
				wantSuccesses, wantEpoch := 1, uint64(1)
				if identical {
					wantSuccesses = 2
				}
				if resume {
					wantEpoch = 2
				}
				if successes != wantSuccesses || winner.Fence.Epoch != wantEpoch {
					t.Fatalf("successes=%d epoch=%d, want %d/%d", successes, winner.Fence.Epoch, wantSuccesses, wantEpoch)
				}
			}
		})
	}
}

func TestProducerCloseCannotFenceConcurrentReplacement(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request := producerOpenFixture("tenant/producer-close-race")
	state, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	resume := metastore.ResumeProducerRequest{Key: request.Key, ExpectedEpoch: 1, IncarnationID: metastore.ProducerIncarnationID{9}}
	start, closeResult, resumeResult := make(chan struct{}), make(chan error, 1), make(chan error, 1)
	go func() {
		<-start
		_, err := store.CloseProducer(ctx, state.Fence)
		closeResult <- err
	}()
	go func() {
		<-start
		_, err := store.ResumeProducer(ctx, resume)
		resumeResult <- err
	}()
	close(start)
	closeErr, resumeErr := <-closeResult, <-resumeResult
	if resumeErr != nil || (closeErr != nil && !errors.Is(closeErr, metastore.ErrStaleProducer)) {
		t.Fatalf("close=%v resume=%v", closeErr, resumeErr)
	}
	current, err := store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != metastore.ProducerOpen || current.Fence.Epoch != 2 || current.Fence.IncarnationID != resume.IncarnationID {
		t.Fatalf("old close affected replacement: %+v", current)
	}
}

func TestProducerLockedRowDoesNotBlockReadsOrOtherProducers(t *testing.T) {
	store := newPostgresTestStore(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request := producerOpenFixture("tenant/producer-lock")
	state, err := store.OpenProducer(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := queryProducer(ctx, tx, request.Key, true); err != nil {
		t.Fatal(err)
	}
	current, err := store.GetProducerState(ctx, request.Key)
	if err != nil {
		t.Fatalf("non-locking read: %v", err)
	}
	requireProducerState(t, current, state)
	other := request
	other.Key.ID = metastore.ProducerID{8}
	if _, err := store.OpenProducer(ctx, other); err != nil {
		t.Fatalf("unrelated producer: %v", err)
	}
	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer waitCancel()
	if _, err := store.CloseProducer(waitCtx, state.Fence); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, metastore.ErrOutcomeUnknown) {
		t.Fatalf("cancel before acquiring row lock error=%v", err)
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
