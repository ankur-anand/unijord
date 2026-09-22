// Package blobstore defines the two conditional object contracts used by
// IsleDB: small CAS-mutable metadata objects and large immutable run objects.
// It imports no provider SDK and owns no key layout, admission, retry, or
// metric policy. DESIGN.md in this directory is the frozen contract.
package blobstore

import (
	"context"
	"errors"
	"io"
	"strings"
	"unicode/utf8"
)

var (
	ErrNotFound          = errors.New("blobstore: object not found")
	ErrInvalidRequest    = errors.New("blobstore: invalid request")
	ErrTooLarge          = errors.New("blobstore: object exceeds read bound")
	ErrImmutableConflict = errors.New("blobstore: immutable object conflict")
	ErrAlreadyExists     = errors.New("blobstore: run object already exists")
	ErrRunChanged        = errors.New("blobstore: run object identity changed")
	ErrInvalidIdentity   = errors.New("blobstore: invalid run identity")
	// ErrIndeterminate means the operation may have taken effect. Reconcile the
	// key; never infer absence or delete it in response to this error.
	ErrIndeterminate = errors.New("blobstore: provider outcome indeterminate")
	ErrCleanup       = errors.New("blobstore: resource cleanup failed")
)

const (
	MaxKeyBytes = 1024
	// MaxMetadataBytes is the largest bound BoundedGet accepts. It is only a
	// sanity ceiling above the largest metadata format (a 512 MiB run
	// checkpoint plus its envelope); the caller's maxBytes is the real bound
	// and no backend allocates from it, only from the provider-reported
	// length capped by it.
	MaxMetadataBytes = int64(1 << 30)
	// Object-store list APIs do not share one maximum page size. The common
	// contract uses S3's 1,000-key ceiling on every backend.
	DefaultListLimit = 1000
	MaxListLimit     = 1000
)

// ValidKey is storage hygiene only. It has no knowledge of key layout.
func ValidKey(key string) bool {
	if key == "" || len(key) > MaxKeyBytes || !utf8.ValidString(key) {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] == 0x7f {
			return false
		}
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

// MetadataStore stores small, fully buffered objects.
//
// Put creates an immutable object; replaying identical bytes is idempotent and
// different bytes return ErrImmutableConflict. CompareAndSwap has exactly three
// outcomes; see CASOutcome. An empty expectedToken means create-if-absent.
type MetadataStore interface {
	BoundedGet(ctx context.Context, key string, maxBytes int64) (Object, error)
	Put(ctx context.Context, key string, body []byte) (Object, error)
	CompareAndSwap(ctx context.Context, key, expectedToken string, body []byte) (CASResult, error)
	List(ctx context.Context, opts ListOptions) (ObjectPage, error)
	Delete(ctx context.Context, key string) error
}

type Object struct {
	Key   string
	Body  []byte
	Token string
}

type ObjectInfo struct {
	Key  string
	Size int64
}

type CASOutcome uint8

const (
	// CASUnknown is the zero value: the write is neither proven applied nor
	// proven rejected. It always accompanies a non-nil error.
	CASUnknown CASOutcome = iota
	CASApplied
	// CASConflict is returned only for a definite conditional-write rejection.
	CASConflict
)

func (o CASOutcome) String() string {
	switch o {
	case CASApplied:
		return "applied"
	case CASConflict:
		return "conflict"
	default:
		return "unknown"
	}
}

type CASResult struct {
	Outcome CASOutcome
	// Object carries the new token after CASApplied. Body is not echoed.
	Object Object
	// Current describes the winning object after CASConflict when it still
	// exists and could be observed. The body is never fetched here because
	// reads must be bounded by the caller.
	Current      ObjectInfo
	CurrentToken string
	CurrentKnown bool
}

type ListOptions struct {
	Prefix string
	// AfterKey is an exclusive lexicographic lower bound. It need not name an
	// existing object and is safe to persist across restarts and deletions.
	AfterKey string
	Limit    int
}

func (o ListOptions) NormalizedLimit() int {
	if o.Limit <= 0 || o.Limit > MaxListLimit {
		return DefaultListLimit
	}
	return o.Limit
}

type ObjectPage struct {
	Objects      []ObjectInfo
	NextAfterKey string
	HasMore      bool
}

// RunStore stores large immutable objects that are streamed exactly once.
type RunStore interface {
	// Create streams exactSize bytes to a key that must not exist. EOF from
	// body is commit authorization. It is never retried internally.
	Create(ctx context.Context, key string, body io.Reader, exactSize int64) (CreateResult, error)
	Stat(ctx context.Context, key string) (RunIdentity, error)
	// OpenRange reads exactly [offset, offset+length) pinned to identity.
	OpenRange(ctx context.Context, key string, identity RunIdentity, offset, length int64) (io.ReadCloser, error)
	// DeleteIfIdentity never deletes a replacement object.
	DeleteIfIdentity(ctx context.Context, key string, identity RunIdentity) error
}

// RunIdentity is safe to persist as JSON. Token is provider-owned and opaque;
// callers compare identities with Equal and never parse Token.
type RunIdentity struct {
	Key   string `json:"key"`
	Size  int64  `json:"size"`
	Token string `json:"token"`
}

func (i RunIdentity) Valid() bool { return ValidKey(i.Key) && i.Size > 0 && i.Token != "" }

func (i RunIdentity) Equal(other RunIdentity) bool { return i == other }

type CreateOutcome uint8

const (
	// CreateIndeterminate is the zero value: the object may exist.
	CreateIndeterminate CreateOutcome = iota
	Created
	// AlreadyExists is only a definite create-only rejection.
	AlreadyExists
	// DefinitelyAbsent is returned only when the backend can prove that this
	// attempt did not create the permanent object.
	DefinitelyAbsent
)

func (o CreateOutcome) String() string {
	switch o {
	case Created:
		return "created"
	case AlreadyExists:
		return "already-exists"
	case DefinitelyAbsent:
		return "definitely-absent"
	default:
		return "indeterminate"
	}
}

type CreateResult struct {
	Outcome  CreateOutcome
	Identity RunIdentity
}

// CheckRange validates a pinned range without overflow before any I/O.
func CheckRange(identity RunIdentity, key string, offset, length int64) error {
	if !ValidKey(key) || offset < 0 || length <= 0 {
		return ErrInvalidRequest
	}
	if !identity.Valid() || identity.Key != key {
		return ErrInvalidIdentity
	}
	if offset > identity.Size || length > identity.Size-offset {
		return ErrInvalidRequest
	}
	return nil
}

// CountingBody adapts a forward-only producer to a provider transport and
// makes producer EOF the commit authorization on every backend. It withholds
// the final byte until the producer has returned EOF at exactly exactSize, so a
// short body, a long body, or a producer error can never hand a transport a
// complete exactSize-byte payload. Consumed reports the bytes released.
type CountingBody struct {
	r        io.Reader
	want     int64
	source   int64 // bytes taken from the producer
	released int64 // bytes handed to the transport
	err      error
	ended    bool
}

const countingBodyMaxEmptyReads = 128

func NewCountingBody(r io.Reader, exactSize int64) *CountingBody {
	return &CountingBody{r: r, want: exactSize}
}

func (b *CountingBody) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.ended {
		return 0, io.EOF
	}
	if len(p) == 0 {
		return 0, nil
	}
	if remaining := b.want - b.source; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := b.r.Read(p)
	if n < 0 || n > len(p) {
		b.err = io.ErrNoProgress
		return 0, b.err
	}
	b.source += int64(n)
	if b.source < b.want {
		switch {
		case err == io.EOF:
			b.err = io.ErrUnexpectedEOF
		case err != nil:
			b.err = err
		}
		b.released += int64(n)
		return n, b.err
	}
	// p holds the final byte. Release it only after the producer's EOF.
	if err != nil && err != io.EOF {
		b.err = err
		b.released += int64(n - 1)
		return n - 1, b.err
	}
	for empty := 0; err != io.EOF; empty++ {
		var probe [1]byte
		var extra int
		extra, err = b.r.Read(probe[:])
		switch {
		case extra > 0:
			b.err = errors.Join(ErrInvalidRequest, errors.New("blobstore: producer body longer than exact size"))
		case err != nil && err != io.EOF:
			b.err = err
		case err == nil && empty >= countingBodyMaxEmptyReads:
			b.err = io.ErrNoProgress
		}
		if b.err != nil {
			b.released += int64(n - 1)
			return n - 1, b.err
		}
	}
	b.ended = true
	b.released += int64(n)
	return n, nil
}

// Consumed reports the producer bytes released to the transport.
func (b *CountingBody) Consumed() int64 { return b.released }

// Complete reports whether exactly exactSize bytes and then EOF were observed.
func (b *CountingBody) Complete() bool { return b.ended && b.released == b.want && b.err == nil }

// Err returns the first producer or length error.
func (b *CountingBody) Err() error { return b.err }
