package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// querier is the common read surface implemented by both pgxpool.Pool and
// pgx.Tx. Transaction ownership remains with the operation orchestrator.
type querier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}
