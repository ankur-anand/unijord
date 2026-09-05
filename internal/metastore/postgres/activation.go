package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5"
)

// ClaimDirectShards atomically creates or takes over a bounded shard set.
// Lock order is canonical and independent of request order; returned leases
// remain aligned with request.Shards.
func (s *Store) ClaimDirectShards(ctx context.Context, request metastore.DirectShardClaimRequest) ([]metastore.DirectShardLease, error) {
	if err := s.checkContext(ctx); err != nil {
		return nil, err
	}
	if err := metastore.ValidateDirectShardClaim(request); err != nil {
		return nil, err
	}
	items, namespaces, err := prepareClaim(request.Shards)
	if err != nil {
		return nil, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("metastore/postgres: begin direct shard claim: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := lockShardIdentities(ctx, tx, items); err != nil {
		return nil, err
	}
	if err := ensureNamespaces(ctx, tx, namespaces); err != nil {
		return nil, err
	}
	created, err := ensureShards(ctx, tx, items)
	if err != nil {
		return nil, err
	}
	if err := initializeShardControls(ctx, tx, created, request.Owner); err != nil {
		return nil, err
	}

	states, err := lockShardWriters(ctx, tx, items)
	if err != nil {
		return nil, err
	}
	changed := make([]metastore.ShardState, 0, len(states))
	for identity, state := range states {
		if state.Fence.Owner.Equal(request.Owner) {
			continue
		}
		if state.Fence.Epoch == math.MaxUint64 {
			return nil, fmt.Errorf("%w: writer epoch exhausted for shard=%d",
				metastore.ErrCorrupt, identity.shard)
		}
		state.Fence.Epoch++
		state.Fence.Owner = request.Owner
		states[identity] = state
		changed = append(changed, state)
	}
	if err := updateWriterFences(ctx, tx, changed); err != nil {
		return nil, err
	}

	leases := make([]metastore.DirectShardLease, len(request.Shards))
	for _, item := range items {
		state, exists := states[item.identity]
		if !exists {
			return nil, fmt.Errorf("%w: claimed shard row is missing", metastore.ErrCorrupt)
		}
		leases[item.input] = metastore.DirectShardLease{State: state}
	}
	if err := metastore.ValidateDirectShardClaimResult(request, leases); err != nil {
		return nil, err
	}
	if err := commitTransaction(ctx, tx, "direct shard claim"); err != nil {
		return nil, err
	}
	return leases, nil
}
