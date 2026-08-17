// Package lifecycle reclaims unreachable partitionlog objects from object
// storage. It is explicitly scheduled; writers and readers never start it.
package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var (
	ErrInvalidOptions = errors.New("lifecycle: invalid options")
	ErrLeaseHeld      = errors.New("lifecycle: lease held")
	ErrLeaseLost      = errors.New("lifecycle: lease lost")
	ErrCorruptState   = errors.New("lifecycle: corrupt state")
)

const (
	DefaultDeleteDelay       = 24 * time.Hour
	DefaultLeaseDuration     = time.Minute
	DefaultListPageSize      = 1000
	DefaultMaxObjectsPerRun  = 10_000
	DefaultMaxDeletesPerRun  = 1_000
	DefaultMaxDeleteBytes    = uint64(1 << 30)
	DefaultDeleteBatchSize   = 1000
	DefaultDeleteConcurrency = 16
	DefaultMaxQuarantine     = 256
	DefaultCASAttempts       = 8
	maxQuarantineEntries     = 1000
)

type Object = blobstore.Object
type ObjectInfo = blobstore.ObjectInfo
type ListOptions = blobstore.ListOptions
type ObjectPage = blobstore.ObjectPage

// Backend is the bounded conditional object protocol used by lifecycle work.
type Backend interface {
	Get(ctx context.Context, key string) (Object, error)
	CompareAndSwap(ctx context.Context, key string, expectedToken string, body []byte) (Object, bool, error)
	List(ctx context.Context, opts ListOptions) (ObjectPage, error)
	Delete(ctx context.Context, key string) error
}

// Catalog supplies one validated, bounded head snapshot per observation.
type Catalog interface {
	LoadMaintenanceSnapshot(ctx context.Context, partition uint32) (catalogblob.MaintenanceSnapshot, error)
	ListMaintenanceSegments(ctx context.Context, req catalog.ListSegmentsRequest) (catalogblob.MaintenanceSnapshot, pmeta.SegmentPage, error)
	ListMaintenancePages(ctx context.Context, req catalogblob.MaintenancePageRequest) (catalogblob.MaintenanceSnapshot, catalogblob.MaintenancePage, error)
}

// DeleteRateLimiter coordinates physical delete throughput across reclaimers.
// The objects count is the number of keys in the provider request. Implementations
// must be safe for concurrent use.
type DeleteRateLimiter interface {
	Wait(ctx context.Context, objects int) error
}

type Options struct {
	// StreamID identifies the stream scoped by this reclaimer.
	StreamID string
	// CatalogPrefix is the object-catalog root used to locate GC state.
	CatalogPrefix string
	// OwnerID identifies this maintenance worker. Zero generates a process-local
	// random identity at construction.
	OwnerID [16]byte

	DeleteDelay      time.Duration
	LeaseDuration    time.Duration
	ListPageSize     int
	MaxObjectsPerRun int
	MaxDeletesPerRun int
	MaxDeleteBytes   uint64
	// DeleteBatchSize bounds one provider batch or parallel delete wave.
	DeleteBatchSize int
	// DeleteConcurrency bounds individual Delete calls when the backend does
	// not expose native batch deletion.
	DeleteConcurrency int
	// DeleteRateLimiter optionally coordinates object delete throughput across
	// multiple reclaimers. Share one instance at the service level.
	DeleteRateLimiter DeleteRateLimiter
	MaxQuarantine     int
	CASAttempts       int
	DryRun            bool
}

type Result struct {
	SafeFloorLSN        uint64
	ReclaimedThroughLSN uint64
	ScannedObjects      int
	CandidateObjects    int
	DeletedObjects      int
	DeletedBytes        uint64
	InvalidObjects      int
	QuarantinedObjects  int
	PendingQuarantine   int
	HasMore             bool
}

type Reclaimer struct {
	backend Backend
	catalog Catalog
	layout  segmentsink.Layout
	opts    Options
	now     func() time.Time
}

func New(backend Backend, catalog Catalog, layout segmentsink.Layout, opts Options) (*Reclaimer, error) {
	return newReclaimer(backend, catalog, layout, opts, time.Now)
}

func newReclaimer(backend Backend, catalog Catalog, layout segmentsink.Layout, opts Options, now func() time.Time) (*Reclaimer, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: nil backend", ErrInvalidOptions)
	}
	if catalog == nil {
		return nil, fmt.Errorf("%w: nil catalog", ErrInvalidOptions)
	}
	streamID, err := keylayout.CanonicalStreamID(opts.StreamID)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	opts.StreamID = streamID
	if opts.DeleteDelay < 0 {
		return nil, fmt.Errorf("%w: negative delete delay", ErrInvalidOptions)
	}
	if opts.DeleteDelay == 0 {
		opts.DeleteDelay = DefaultDeleteDelay
	}
	if opts.LeaseDuration < 0 {
		return nil, fmt.Errorf("%w: negative lease duration", ErrInvalidOptions)
	}
	if opts.LeaseDuration == 0 {
		opts.LeaseDuration = DefaultLeaseDuration
	}
	if opts.ListPageSize < 0 {
		return nil, fmt.Errorf("%w: negative list page size", ErrInvalidOptions)
	}
	if opts.ListPageSize == 0 {
		opts.ListPageSize = DefaultListPageSize
	}
	if opts.ListPageSize > blobstore.MaxListLimit {
		return nil, fmt.Errorf("%w: list page size=%d max=%d", ErrInvalidOptions, opts.ListPageSize, blobstore.MaxListLimit)
	}
	if opts.MaxObjectsPerRun < 0 || opts.MaxDeletesPerRun < 0 {
		return nil, fmt.Errorf("%w: negative run budget", ErrInvalidOptions)
	}
	if opts.MaxObjectsPerRun == 0 {
		opts.MaxObjectsPerRun = DefaultMaxObjectsPerRun
	}
	if opts.MaxDeletesPerRun == 0 {
		opts.MaxDeletesPerRun = DefaultMaxDeletesPerRun
	}
	if opts.MaxDeleteBytes == 0 {
		opts.MaxDeleteBytes = DefaultMaxDeleteBytes
	}
	if opts.DeleteBatchSize < 0 || opts.DeleteBatchSize > blobstore.MaxListLimit {
		return nil, fmt.Errorf("%w: delete batch size=%d range=0..%d", ErrInvalidOptions, opts.DeleteBatchSize, blobstore.MaxListLimit)
	}
	if opts.DeleteBatchSize == 0 {
		opts.DeleteBatchSize = DefaultDeleteBatchSize
	}
	if opts.DeleteConcurrency < 0 {
		return nil, fmt.Errorf("%w: negative delete concurrency", ErrInvalidOptions)
	}
	if opts.DeleteConcurrency == 0 {
		opts.DeleteConcurrency = DefaultDeleteConcurrency
	}
	if opts.MaxQuarantine < 0 || opts.MaxQuarantine > maxQuarantineEntries {
		return nil, fmt.Errorf("%w: max quarantine=%d range=0..%d", ErrInvalidOptions, opts.MaxQuarantine, maxQuarantineEntries)
	}
	if opts.MaxQuarantine == 0 {
		opts.MaxQuarantine = DefaultMaxQuarantine
	}
	if opts.CASAttempts < 0 {
		return nil, fmt.Errorf("%w: negative CAS attempts", ErrInvalidOptions)
	}
	if opts.CASAttempts == 0 {
		opts.CASAttempts = DefaultCASAttempts
	}
	if now == nil {
		return nil, fmt.Errorf("%w: nil clock", ErrInvalidOptions)
	}
	if opts.OwnerID == ([16]byte{}) {
		owner, err := randomOwnerID()
		if err != nil {
			return nil, fmt.Errorf("lifecycle: generate owner id: %w", err)
		}
		opts.OwnerID = owner
	}
	return &Reclaimer{backend: backend, catalog: catalog, layout: layout, opts: opts, now: now}, nil
}
