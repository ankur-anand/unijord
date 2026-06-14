package partitionlog

import (
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

// Header is one record header key/value pair.
type Header = segformat.Header

// PartitionHead is the bounded current state for one partition.
type PartitionHead = pmeta.PartitionHead

// SegmentRef is the durable catalog reference for one committed segment.
type SegmentRef = pmeta.SegmentRef

// Record is appended to one partition writer.
type Record struct {
	TimestampMS int64
	Headers     []Header
	Value       []byte
}

type AppendResult struct {
	LSN uint64
}

type WriterIdentity struct {
	Epoch uint64
	Tag   [16]byte
}

type Snapshot struct {
	Head     PartitionHead
	Identity WriterIdentity
}

type WriterState struct {
	Snapshot          Snapshot
	OptimisticNextLSN uint64
	InflightSegments  int
	InflightBytes     uint64
}

// BatchPolicy controls when the writer closes the current active batch and
// starts publishing it in the background. If more than one limit is set, the
// first limit reached wins. Zero values keep the defaults.
type BatchPolicy struct {
	// MaxDelay cuts a non-empty active batch after this duration since the
	// first record in that batch. The timer resets after every cut.
	MaxDelay time.Duration

	// MaxBytes cuts the active batch once its raw record bytes reach this
	// threshold. This is measured before compression.
	MaxBytes uint64

	// MaxRecords cuts the active batch once this many records are accepted.
	MaxRecords uint32
}

// BackpressurePolicy bounds cut batches waiting for background finalize,
// upload, and catalog publish. When a limit is reached, future cuts block until
// pending work drains. Zero values keep the defaults.
type BackpressurePolicy struct {
	// MaxPendingBatches limits how many cut batches may wait in the background.
	MaxPendingBatches int

	// MaxPendingBytes limits estimated bytes across pending cut batches.
	MaxPendingBytes uint64
}

// WriterPipelineOptions tunes the internal segment build/upload pipeline. Most
// users should leave this empty.
type WriterPipelineOptions struct {
	// BlockBytes is the target raw bytes per encoded block before compression.
	BlockBytes int

	// PartBytes is the target multipart upload part size.
	PartBytes int

	// SealParallelism is the number of workers compressing and hashing blocks.
	SealParallelism int

	// BlockBuffers is the number of reusable raw block buffers in the seal
	// pipeline. It must be large enough to cover seal workers plus the active
	// append buffer.
	BlockBuffers int

	// UploadParallelism is the number of concurrent multipart part uploads.
	UploadParallelism int

	// UploadQueueSize is the number of sealed parts that may wait for upload
	// workers before backpressuring the segment emitter.
	UploadQueueSize int

	// UploadLimiter optionally coordinates upload concurrency across writers.
	UploadLimiter segwriter.UploadLimiter
}
