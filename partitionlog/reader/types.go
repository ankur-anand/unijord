package reader

import (
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

const (
	DefaultMaxRecordsPerBatch = 1024
)

type SegmentStore = segreader.SegmentStore

type Options struct {
	MaxRecordsPerBatch int
	SegmentOptions     segreader.Options
	SegmentCache       *SegmentReaderCache
	Refresh            RefreshPolicy
	Observer           Observer
}

type Reader struct {
	catalog catalog.Reader
	store   SegmentStore
	opts    Options
	refresh *refreshCoordinator
}

type Record struct {
	Partition   uint32
	LSN         uint64
	TimestampMS int64
	Headers     []segformat.Header
	Value       []byte
}

type MetricName string

const (
	MetricHead           MetricName = "reader.head"
	MetricRead           MetricName = "reader.read"
	MetricFetch          MetricName = "reader.fetch"
	MetricTimestampRead  MetricName = "reader.timestamp_read"
	MetricTailNext       MetricName = "reader.tail_next"
	MetricCatalogRefresh MetricName = "reader.catalog_refresh"
	MetricSegmentRead    MetricName = "reader.segment_read"
)

type MetricEvent struct {
	Name      MetricName
	Partition uint32

	StartLSN uint64
	NextLSN  uint64
	Limit    int
	Records  int

	SegmentURI string
	Duration   time.Duration
	Err        error
}

type Observer interface {
	Observe(MetricEvent)
}

func (r Record) Clone() Record {
	out := r
	out.Headers = segformat.CloneHeaders(r.Headers)
	if len(r.Value) > 0 {
		out.Value = append([]byte(nil), r.Value...)
	}
	return out
}

type ConsumeRequest struct {
	Partition uint32
	StartLSN  uint64
	Limit     int
}

type ConsumeAfterRequest struct {
	Partition     uint32
	StartAfterLSN uint64
	Limit         int
}

type ConsumeFromTimestampRequest struct {
	Partition   uint32
	TimestampMS int64
	Limit       int
}

type FetchRequest struct {
	Partition uint32
	LSN       uint64
}

type FetchResult struct {
	Record Record
	Found  bool
	Head   pmeta.PartitionHead
}

type ConsumeResult struct {
	Records []Record
	NextLSN uint64
	Head    pmeta.PartitionHead
}

// Freshness controls how a passive read consults catalog state.
type Freshness int

const (
	// FreshnessDefault uses FreshnessOnTail.
	FreshnessDefault Freshness = iota

	// FreshnessCached uses a cached partition head when one exists. If the
	// partition has not been seen before, the reader loads the head once.
	FreshnessCached

	// FreshnessOnTail refreshes the partition head only when the requested LSN
	// is at or beyond the cached tail. This is the default for Read and Cursor.
	FreshnessOnTail

	// FreshnessLatest refreshes the partition head before every read.
	FreshnessLatest
)

// RefreshPolicy controls explicit Watch background refresh. Passive reads do
// not start background polling.
type RefreshPolicy struct {
	// PollInterval is how often the shared reader refresh loop checks watched
	// partitions.
	PollInterval time.Duration

	// MaxConcurrentRefreshes limits concurrent catalog head refreshes. Actual
	// concurrency is capped by the number of watched partitions.
	MaxConcurrentRefreshes int

	// RefreshTimeout bounds one catalog head refresh. Zero means no extra
	// timeout beyond the Watch context.
	RefreshTimeout time.Duration
}

// ReadRequest reads committed records from one partition reader.
type ReadRequest struct {
	StartLSN  uint64
	Limit     int
	Freshness Freshness
}

// ReadResult is returned by one-shot reads, cursors, and tailers.
type ReadResult = ConsumeResult

// CursorOptions configures a passive replay cursor.
type CursorOptions struct {
	StartLSN uint64
	Limit    int
}

// WatchOptions explicitly starts background catalog refresh for partitions.
type WatchOptions struct {
	Partitions []uint32
}

// TailOptions configures a tail cursor attached to a Watch.
type TailOptions struct {
	Partition uint32
	StartLSN  uint64
	Limit     int
}
