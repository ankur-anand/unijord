// Package blobstore defines the conditional object operations shared by
// Unijord metadata services.
package blobstore

import (
	"context"
	"errors"
	"time"
)

var (
	ErrObjectNotFound    = errors.New("blobstore: object not found")
	ErrImmutableConflict = errors.New("blobstore: immutable object conflict")
	ErrInvalidRequest    = errors.New("blobstore: invalid request")
)

const (
	DefaultListLimit = 1024
	MaxListLimit     = 4096
	JSONContentType  = "application/json"
)

// Store is the conditional object protocol used by metadata layers.
//
// Put creates an immutable object. Repeating Put with identical bytes is
// idempotent; writing different bytes to the same key returns
// ErrImmutableConflict.
//
// CompareAndSwap updates a mutable object. An empty expectedToken means
// create-if-absent. On a failed comparison, implementations return the current
// object when it still exists so callers can reconcile without another Get.
type Store interface {
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
	Prefix string

	// Cursor is an opaque value previously returned as ObjectPage.NextCursor.
	Cursor string

	Limit int
}

func (o ListOptions) NormalizedLimit() int {
	switch {
	case o.Limit <= 0:
		return DefaultListLimit
	case o.Limit > MaxListLimit:
		return MaxListLimit
	default:
		return o.Limit
	}
}

type ObjectPage struct {
	Objects    []ObjectInfo
	NextCursor string
	HasMore    bool
}
