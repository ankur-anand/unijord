package partitionlog

import plreader "github.com/ankur-anand/unijord/partitionlog/reader"

// Reader reads committed records from a partition log.
type Reader = plreader.Reader

// PartitionReader is a per-partition read view over a Reader.
type PartitionReader = plreader.PartitionReader

// Cursor is a passive replay cursor.
type Cursor = plreader.Cursor

// Watch owns explicit background refresh for tailing.
type Watch = plreader.Watch

// Tailer is a blocking cursor attached to a Watch.
type Tailer = plreader.Tailer

// LSNExpiredError reports reads below the catalog oldest LSN.
type LSNExpiredError = plreader.LSNExpiredError

// ReadRecord is returned by partition readers.
type ReadRecord = plreader.Record

// SegmentStore reads byte ranges from committed segment objects.
type SegmentStore = plreader.SegmentStore

// ReadResult is returned by one-shot reads, cursors, and tailers.
type ReadResult = plreader.ReadResult

// ReadRequest reads committed records from one partition reader.
type ReadRequest = plreader.ReadRequest

// CursorOptions configures a passive replay cursor.
type CursorOptions = plreader.CursorOptions

// CursorResumeOptions configures a cursor restored from a durable checkpoint.
type CursorResumeOptions = plreader.CursorResumeOptions

// CursorCheckpoint is a durable next-read position bound to one stream
// partition.
type CursorCheckpoint = plreader.CursorCheckpoint

// WatchOptions explicitly starts background catalog refresh for partitions.
type WatchOptions = plreader.WatchOptions

// TailOptions configures a tail cursor attached to a Watch.
type TailOptions = plreader.TailOptions

// RefreshPolicy controls explicit Watch background refresh.
type RefreshPolicy = plreader.RefreshPolicy

// Freshness controls how a passive read consults catalog state.
type Freshness = plreader.Freshness

const (
	CursorCheckpointVersion        = plreader.CursorCheckpointVersion
	DefaultMaxCachedPartitionHeads = plreader.DefaultMaxCachedPartitionHeads

	FreshnessDefault = plreader.FreshnessDefault
	FreshnessCached  = plreader.FreshnessCached
	FreshnessOnTail  = plreader.FreshnessOnTail
	FreshnessLatest  = plreader.FreshnessLatest
)

var (
	ErrReaderClosed        = plreader.ErrClosed
	ErrWatchClosed         = plreader.ErrWatchClosed
	ErrPartitionNotWatched = plreader.ErrPartitionNotWatched
	ErrCheckpointInvalid   = plreader.ErrCheckpointInvalid
	ErrCheckpointMismatch  = plreader.ErrCheckpointMismatch
	ErrCheckpointAhead     = plreader.ErrCheckpointAhead
)
