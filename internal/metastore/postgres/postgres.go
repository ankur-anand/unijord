// Package postgres implements the PostgreSQL metastore backend.
//
// Object bytes remain in object storage. This package owns the transactional
// metadata that makes those objects visible and assigns logical timeline
// positions. Each ingress protocol is implemented as a separate transaction
// family over shared shard and timeline primitives.
package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/internal/metastore"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store owns or borrows a native pgx connection pool. A Store created by Open
// closes its pool; a Store created by New leaves the caller's pool untouched.
type Store struct {
	pool      *pgxpool.Pool
	owned     bool
	closeOnce sync.Once
}

var _ metastore.DirectActivator = (*Store)(nil)

func Open(ctx context.Context, dsn string) (*Store, error) {
	if ctx == nil || dsn == "" {
		return nil, fmt.Errorf("%w: nil context or empty DSN", metastore.ErrInvalidRequest)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("metastore/postgres: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("metastore/postgres: ping: %w", err)
	}
	return &Store{pool: pool, owned: true}, nil
}

func New(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, fmt.Errorf("%w: nil PostgreSQL pool", metastore.ErrInvalidRequest)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() error {
	if s == nil || s.pool == nil || !s.owned {
		return nil
	}
	s.closeOnce.Do(s.pool.Close)
	return nil
}

func (s *Store) checkContext(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: nil PostgreSQL store", metastore.ErrInvalidRequest)
	}
	if ctx == nil {
		return fmt.Errorf("%w: nil context", metastore.ErrInvalidRequest)
	}
	return ctx.Err()
}
