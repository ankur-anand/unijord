package blobcatalog

import (
	"context"
	"errors"
	"time"
)

var (
	ErrObjectNotFound    = errors.New("blobcatalog: object not found")
	ErrImmutableConflict = errors.New("blobcatalog: immutable object conflict")
	ErrIndexFull         = errors.New("blobcatalog: index full")
	ErrCorruptCatalog    = errors.New("blobcatalog: corrupt catalog")
)

const (
	DefaultObjectListLimit = 1024
	MaxObjectListLimit     = 4096
	ObjectContentType      = "application/json"
)

// Backend is the minimal blob/object-store protocol required by the catalog.
//
// Put is for immutable page candidates. Implementations should make identical
// replays idempotent and reject the same key with different bytes.
//
// CompareAndSwap is for the mutable partition head. expectedToken == "" means
// create-if-absent.
//
// Delete is used by bounded catalog-page GC. It should be idempotent for
// missing objects.
type Backend interface {
	Get(ctx context.Context, key string) (Object, error)
	Put(ctx context.Context, key string, body []byte) (Object, error)
	CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (Object, bool, error)
	List(ctx context.Context, opts ListOptions) (ObjectPage, error)
	Delete(ctx context.Context, key string) error
}

type Object struct {
	Key       string
	Body      []byte
	Token     string
	CreatedAt time.Time
}

type ObjectInfo struct {
	Key       string
	Token     string
	SizeBytes int
	CreatedAt time.Time
}

type ListOptions struct {
	// Prefix restricts results to object keys with this prefix.
	Prefix string

	// Cursor is an opaque value previously returned as ObjectPage.NextCursor.
	// Callers must not construct it themselves.
	Cursor string

	Limit int
}

func (o ListOptions) NormalizedLimit() int {
	return o.normalizedLimit()
}

func (o ListOptions) normalizedLimit() int {
	switch {
	case o.Limit <= 0:
		return DefaultObjectListLimit
	case o.Limit > MaxObjectListLimit:
		return MaxObjectListLimit
	default:
		return o.Limit
	}
}

type ObjectPage struct {
	Objects    []ObjectInfo
	NextCursor string
	HasMore    bool
}
