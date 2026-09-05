package postgres

import (
	"context"
	"fmt"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

var _ metastore.ProducerLifecycle = (*Store)(nil)

func (s *Store) OpenProducer(ctx context.Context, request metastore.OpenProducerRequest) (metastore.ProducerState, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.ProducerState{}, err
	}
	if err := metastore.ValidateOpenProducer(request); err != nil {
		return metastore.ProducerState{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return metastore.ProducerState{}, fmt.Errorf("metastore/postgres: begin producer open: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	// Unique-key insertion serializes absent producers. Existing producer
	// authority is locked and rechecked below; a different incarnation never
	// gets an implicit takeover from OpenProducer.
	if err := insertProducer(ctx, tx, request); err != nil {
		return metastore.ProducerState{}, err
	}
	return finishProducerTransition(ctx, tx, request.Key, "producer open", func(current metastore.ProducerState) (metastore.ProducerState, error) {
		return planProducerOpen(current, request)
	})
}

func (s *Store) ResumeProducer(ctx context.Context, request metastore.ResumeProducerRequest) (metastore.ProducerState, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.ProducerState{}, err
	}
	if err := metastore.ValidateResumeProducer(request); err != nil {
		return metastore.ProducerState{}, err
	}
	return s.changeProducer(ctx, request.Key, "producer resume", func(current metastore.ProducerState) (metastore.ProducerState, error) {
		return planProducerResume(current, request)
	})
}

func (s *Store) CloseProducer(ctx context.Context, fence metastore.ProducerFence) (metastore.ProducerState, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.ProducerState{}, err
	}
	if err := metastore.ValidateProducerFence(fence); err != nil {
		return metastore.ProducerState{}, err
	}
	return s.changeProducer(ctx, fence.Key, "producer close", func(current metastore.ProducerState) (metastore.ProducerState, error) {
		return planProducerClose(current, fence)
	})
}

func (s *Store) GetProducerState(ctx context.Context, key metastore.ProducerKey) (metastore.ProducerState, error) {
	if err := s.checkContext(ctx); err != nil {
		return metastore.ProducerState{}, err
	}
	if err := metastore.ValidateProducerKey(key); err != nil {
		return metastore.ProducerState{}, err
	}
	return queryProducer(ctx, s.pool, key, false)
}

func (s *Store) changeProducer(ctx context.Context, key metastore.ProducerKey,
	operation string, transition producerTransition,
) (metastore.ProducerState, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return metastore.ProducerState{}, fmt.Errorf("metastore/postgres: begin %s: %w", operation, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	return finishProducerTransition(ctx, tx, key, operation, transition)
}

// finishProducerTransition commits one locked authority transition. The caller
// owns rollback/connection cleanup. No shard or timeline row is locked here.
func finishProducerTransition(ctx context.Context, tx pgx.Tx, key metastore.ProducerKey,
	operation string, transition producerTransition,
) (metastore.ProducerState, error) {
	before, err := queryProducer(ctx, tx, key, true)
	if err != nil {
		return metastore.ProducerState{}, err
	}
	after, err := transition(before)
	if err != nil {
		return metastore.ProducerState{}, err
	}
	if err := metastore.ValidateProducerState(after); err != nil {
		return metastore.ProducerState{}, err
	}
	if !metastore.SameProducerFence(before.Fence, after.Fence) || before.Status != after.Status {
		if err := updateProducer(ctx, tx, after); err != nil {
			return metastore.ProducerState{}, err
		}
	}
	if err := commitTransaction(ctx, tx, operation); err != nil {
		return metastore.ProducerState{}, err
	}
	return after, nil
}
