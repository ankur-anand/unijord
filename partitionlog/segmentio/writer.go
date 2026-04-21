package segmentio

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

/*
Writer implementation notes

This file intentionally treats multipart and staged commit as first-class writer
concerns.

The primary abstraction is no longer "serialize a segment to an io.Writer".
That is too weak for the real lifecycle we need:

- accumulate records into blocks
- seal blocks into immutable payloads
- validate and assemble the final segment
- begin a staged sink transaction
- upload parts
- complete or abort

The segment wire format remains version 1:

- fixed header
- stored block bytes
- fixed block index
- fixed footer

Only the writer architecture changes.

The design is built around five roles:

1. BlockBuilder
2. BlockSealer
3. SegmentAssembler
4. SegmentSink
5. SegmentEmitter

Performance implications

- The final segment is emitted as multipart-sized upload chunks instead of
  requiring a second whole-segment buffer for transport.
- The assembler owns final validation, so corruption is caught before sink
  commit.
- zstd encoders are pooled because constructing one per block is pure writer
  overhead.

Intentional remaining costs

- CRC32C remains block-local. That is part of the format contract.
- Sealed block payloads spill to temp files. Finish and Abort remove those
  files. A crash can still leave orphan spill files in the temp directory.
*/

const DefaultUploadPartSize = 8 << 20

var (
	ErrInvalidWriterOptions = errors.New("segmentio: invalid writer options")
	ErrWriterClosed         = errors.New("segmentio: writer already finished")
	ErrWriterAborted        = errors.New("segmentio: writer aborted")
)

// WriterOptions control how a transactional segment writer seals and uploads a
// segment.
type WriterOptions struct {
	Codec           Codec
	TargetBlockSize int
	MaxRecordSize   int
	UploadPartSize  int
	TempDir         string
}

func (o WriterOptions) withDefaults() WriterOptions {
	if o.TargetBlockSize <= 0 {
		o.TargetBlockSize = DefaultTargetBlockSize
	}
	if o.MaxRecordSize <= 0 {
		o.MaxRecordSize = DefaultMaxRecordSize
	}
	if o.UploadPartSize <= 0 {
		o.UploadPartSize = DefaultUploadPartSize
	}
	return o
}

func (o WriterOptions) validate() error {
	if err := o.Codec.validate(); err != nil {
		return err
	}
	if o.MaxRecordSize <= 0 {
		return fmt.Errorf("%w: max_record_size must be positive", ErrInvalidWriterOptions)
	}
	if o.UploadPartSize <= 0 {
		return fmt.Errorf("%w: upload_part_size must be positive", ErrInvalidWriterOptions)
	}
	return nil
}

// CommitResult is returned after a sink transaction has completed successfully.
type CommitResult struct {
	Metadata Metadata
	Object   CommittedObject
}

// SegmentWriter incrementally builds one immutable segment and commits it
// transactionally into a sink.
type SegmentWriter struct {
	opts WriterOptions
	sink SegmentSink

	initialized  bool
	partition    uint32
	prevLSN      uint64
	prevTS       int64
	nextSequence uint32

	current     rawBlockBuilder
	descriptors []blockDescriptor

	finished bool
	aborted  bool
}

// NewSegmentWriter constructs a writer that emits one immutable segment into
// the provided sink.
func NewSegmentWriter(opts WriterOptions, sink SegmentSink) (*SegmentWriter, error) {
	if sink == nil {
		return nil, fmt.Errorf("%w: sink is nil", ErrInvalidWriterOptions)
	}

	opts = opts.withDefaults()
	if err := opts.validate(); err != nil {
		return nil, err
	}

	return &SegmentWriter{
		opts: opts,
		sink: sink,
	}, nil
}

// Encode is a convenience helper that builds a segment fully in memory using
// the transactional writer path.
func Encode(records []Record, opts WriterOptions) ([]byte, Metadata, error) {
	sink := newBufferSegmentSink()
	writer, err := NewSegmentWriter(opts, sink)
	if err != nil {
		return nil, Metadata{}, err
	}

	for _, record := range records {
		if err := writer.Append(record); err != nil {
			_ = writer.Abort(context.Background())
			return nil, Metadata{}, err
		}
	}

	result, err := writer.Finish(context.Background())
	if err != nil {
		return nil, Metadata{}, err
	}

	return sink.Bytes(), result.Metadata, nil
}

// Append appends one logical record to the open segment.
func (w *SegmentWriter) Append(record Record) error {
	if w.finished {
		return ErrWriterClosed
	}
	if w.aborted {
		return ErrWriterAborted
	}

	if !w.initialized {
		w.partition = record.Partition
		w.initialized = true
	} else {
		if record.Partition != w.partition {
			return ErrMixedPartition
		}
		if record.LSN != w.prevLSN+1 {
			return fmt.Errorf("%w: lsn=%d after=%d", ErrNonContiguousLSN, record.LSN, w.prevLSN)
		}
		if record.TimestampMS < w.prevTS {
			return fmt.Errorf("%w: timestamp=%d after=%d", ErrTimestampOrder, record.TimestampMS, w.prevTS)
		}
	}

	if len(record.Value) > MaxRecordValueLen {
		return fmt.Errorf("%w: record value length=%d max=%d", ErrRecordTooLarge, len(record.Value), MaxRecordValueLen)
	}

	recordSize := encodeRecordSize(len(record.Value))
	if recordSize > w.opts.MaxRecordSize {
		return fmt.Errorf("%w: record size=%d max=%d", ErrRecordTooLarge, recordSize, w.opts.MaxRecordSize)
	}
	if recordSize > MaxBlockSize {
		return fmt.Errorf("%w: record size=%d exceeds max block size=%d", ErrRecordTooLarge, recordSize, MaxBlockSize)
	}

	blockTarget := w.opts.TargetBlockSize
	if blockTarget > MaxBlockSize {
		blockTarget = MaxBlockSize
	}

	if w.current.hasRecords() && w.current.len()+recordSize > blockTarget {
		if err := w.flushCurrentBlock(); err != nil {
			return err
		}
	}

	w.current.append(record)
	w.prevLSN = record.LSN
	w.prevTS = record.TimestampMS
	return nil
}

// Finish seals the current segment, uploads all parts through the sink, and
// returns the committed object metadata.
func (w *SegmentWriter) Finish(ctx context.Context) (CommitResult, error) {
	if w.finished {
		return CommitResult{}, ErrWriterClosed
	}
	if w.aborted {
		return CommitResult{}, ErrWriterAborted
	}
	if !w.initialized {
		return CommitResult{}, ErrEmptySegment
	}

	completed := false
	defer func() {
		if !completed {
			_ = w.Abort(ctx)
		}
	}()

	if err := w.flushCurrentBlock(); err != nil {
		return CommitResult{}, err
	}

	descriptors := w.descriptors
	w.descriptors = nil
	defer destroyDescriptors(descriptors)

	assembled, err := assembleSegment(w.partition, w.opts.Codec, descriptors)
	if err != nil {
		return CommitResult{}, err
	}

	txn, err := w.sink.Begin(ctx, assembled.Plan)
	if err != nil {
		return CommitResult{}, err
	}

	committed := false
	defer func() {
		if !committed {
			_ = txn.Abort(ctx)
		}
	}()

	emitter := newSegmentEmitter(ctx, txn, w.opts.UploadPartSize)
	if err := emitAssembledSegment(emitter, assembled); err != nil {
		return CommitResult{}, err
	}

	object, err := emitter.Complete()
	if err != nil {
		return CommitResult{}, err
	}

	w.current.reset()
	w.descriptors = nil
	w.finished = true
	committed = true
	completed = true

	return CommitResult{
		Metadata: assembled.Plan.Metadata,
		Object:   object,
	}, nil
}

// Abort discards any staged writer state and destroys owned payloads.
func (w *SegmentWriter) Abort(_ context.Context) error {
	if w.aborted || w.finished {
		return nil
	}

	destroyDescriptors(w.descriptors)
	w.descriptors = nil
	w.current.reset()
	w.aborted = true
	return nil
}

func (w *SegmentWriter) flushCurrentBlock() error {
	if !w.current.hasRecords() {
		return nil
	}

	raw := w.current.block(w.nextSequence)
	descriptor, err := sealRawBlock(raw, w.opts.Codec, w.opts.TempDir)
	if err != nil {
		return err
	}

	w.nextSequence++
	w.current.reset()
	w.descriptors = append(w.descriptors, descriptor)
	return nil
}

type rawBlock struct {
	Sequence       uint32
	BaseLSN        uint64
	LastLSN        uint64
	RecordCount    uint32
	MinTimestampMS int64
	MaxTimestampMS int64
	RawBytes       []byte
}

type rawBlockBuilder struct {
	raw          []byte
	baseLSN      uint64
	lastLSN      uint64
	recordCount  uint32
	minTimestamp int64
	maxTimestamp int64
}

func (b *rawBlockBuilder) hasRecords() bool {
	return b.recordCount > 0
}

func (b *rawBlockBuilder) len() int {
	return len(b.raw)
}

func (b *rawBlockBuilder) append(record Record) {
	if b.recordCount == 0 {
		b.baseLSN = record.LSN
		b.minTimestamp = record.TimestampMS
	}
	b.lastLSN = record.LSN
	b.maxTimestamp = record.TimestampMS
	b.recordCount++
	b.raw = appendRawRecord(b.raw, record.TimestampMS, record.Value)
}

func (b *rawBlockBuilder) block(sequence uint32) rawBlock {
	return rawBlock{
		Sequence:       sequence,
		BaseLSN:        b.baseLSN,
		LastLSN:        b.lastLSN,
		RecordCount:    b.recordCount,
		MinTimestampMS: b.minTimestamp,
		MaxTimestampMS: b.maxTimestamp,
		RawBytes:       b.raw,
	}
}

func (b *rawBlockBuilder) reset() {
	b.raw = b.raw[:0]
	b.baseLSN = 0
	b.lastLSN = 0
	b.recordCount = 0
	b.minTimestamp = 0
	b.maxTimestamp = 0
}

type payloadRef interface {
	Size() uint32
	Open() (io.ReadCloser, error)
	Destroy() error
}

type blockDescriptor struct {
	Sequence       uint32
	BaseLSN        uint64
	LastLSN        uint64
	RecordCount    uint32
	MinTimestampMS int64
	MaxTimestampMS int64
	RawSize        uint32
	StoredSize     uint32
	CRC32C         uint32
	Codec          Codec
	Payload        payloadRef
}

func (d blockDescriptor) validate() error {
	if d.RecordCount == 0 {
		return fmt.Errorf("%w: block record_count must be positive", ErrInvalidSegment)
	}
	if d.LastLSN < d.BaseLSN {
		return fmt.Errorf("%w: block last_lsn < base_lsn", ErrInvalidSegment)
	}
	if d.LastLSN != d.BaseLSN+uint64(d.RecordCount)-1 {
		return fmt.Errorf("%w: block lsn span does not match record_count", ErrInvalidSegment)
	}
	if d.RawSize == 0 {
		return fmt.Errorf("%w: block raw_size must be positive", ErrInvalidSegment)
	}
	if d.StoredSize == 0 {
		return fmt.Errorf("%w: block stored_size must be positive", ErrInvalidSegment)
	}
	if d.RawSize > MaxBlockSize {
		return fmt.Errorf("%w: block raw_size=%d exceeds max=%d", ErrInvalidSegment, d.RawSize, MaxBlockSize)
	}
	if d.StoredSize > MaxBlockSize {
		return fmt.Errorf("%w: block stored_size=%d exceeds max=%d", ErrInvalidSegment, d.StoredSize, MaxBlockSize)
	}
	if d.MaxTimestampMS < d.MinTimestampMS {
		return fmt.Errorf("%w: block max_timestamp < min_timestamp", ErrInvalidSegment)
	}
	if err := d.Codec.validate(); err != nil {
		return err
	}
	if d.Payload == nil {
		return fmt.Errorf("%w: block payload is nil", ErrInvalidSegment)
	}
	if d.Payload.Size() != d.StoredSize {
		return fmt.Errorf(
			"%w: payload size=%d does not match stored_size=%d",
			ErrInvalidSegment,
			d.Payload.Size(),
			d.StoredSize,
		)
	}
	return nil
}

func sealRawBlock(raw rawBlock, codec Codec, tempDir string) (blockDescriptor, error) {
	if len(raw.RawBytes) == 0 {
		return blockDescriptor{}, fmt.Errorf("%w: empty raw block", ErrInvalidSegment)
	}
	if len(raw.RawBytes) > MaxBlockSize {
		return blockDescriptor{}, fmt.Errorf("%w: raw block too large=%d max=%d", ErrInvalidSegment, len(raw.RawBytes), MaxBlockSize)
	}

	payloadBuilder, err := beginTempFilePayload(tempDir)
	if err != nil {
		return blockDescriptor{}, err
	}
	finished := false
	defer func() {
		if !finished {
			_ = payloadBuilder.Abort()
		}
	}()

	if err := encodeBlockTo(payloadBuilder, raw.RawBytes, codec); err != nil {
		return blockDescriptor{}, err
	}
	if payloadBuilder.Size() > MaxBlockSize {
		return blockDescriptor{}, fmt.Errorf("%w: stored block too large=%d max=%d", ErrInvalidSegment, payloadBuilder.Size(), MaxBlockSize)
	}

	payload, err := payloadBuilder.Finish()
	if err != nil {
		return blockDescriptor{}, err
	}
	finished = true

	descriptor := blockDescriptor{
		Sequence:       raw.Sequence,
		BaseLSN:        raw.BaseLSN,
		LastLSN:        raw.LastLSN,
		RecordCount:    raw.RecordCount,
		MinTimestampMS: raw.MinTimestampMS,
		MaxTimestampMS: raw.MaxTimestampMS,
		RawSize:        uint32(len(raw.RawBytes)),
		StoredSize:     payload.Size(),
		CRC32C:         crc32Checksum(raw.RawBytes),
		Codec:          codec,
		Payload:        payload,
	}
	if err := descriptor.validate(); err != nil {
		_ = descriptor.Payload.Destroy()
		return blockDescriptor{}, err
	}

	return descriptor, nil
}

func destroyDescriptors(descriptors []blockDescriptor) {
	for i := range descriptors {
		if descriptors[i].Payload != nil {
			_ = descriptors[i].Payload.Destroy()
		}
	}
}

func appendRawRecord(dst []byte, timestampMS int64, value []byte) []byte {
	recordSize := encodeRecordSize(len(value))
	start := len(dst)
	end := start + recordSize

	if cap(dst) < end {
		grown := make([]byte, start, growRawRecordCap(cap(dst), end))
		copy(grown, dst)
		dst = grown
	}

	dst = dst[:end]
	binary.BigEndian.PutUint64(dst[start:start+8], uint64(timestampMS))
	binary.BigEndian.PutUint32(dst[start+8:start+12], uint32(len(value)))
	copy(dst[start+12:end], value)
	return dst
}

func growRawRecordCap(currentCap int, need int) int {
	if currentCap >= need {
		return currentCap
	}
	if currentCap == 0 {
		currentCap = need
	}

	newCap := currentCap
	for newCap < need {
		if newCap < 1024 {
			newCap *= 2
		} else {
			newCap += newCap / 2
		}
	}
	return newCap
}

var zstdEncoderPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic(fmt.Sprintf("segmentio: create zstd encoder: %v", err))
		}
		return enc
	},
}

func encodeBlockTo(dst io.Writer, raw []byte, codec Codec) error {
	switch codec {
	case CodecNone:
		_, err := dst.Write(raw)
		return err
	case CodecZstd:
		enc := zstdEncoderPool.Get().(*zstd.Encoder)
		defer zstdEncoderPool.Put(enc)

		enc.Reset(dst)
		if _, err := enc.Write(raw); err != nil {
			enc.Reset(io.Discard)
			return fmt.Errorf("encode zstd block: %w", err)
		}
		if err := enc.Close(); err != nil {
			enc.Reset(io.Discard)
			return fmt.Errorf("close zstd encoder: %w", err)
		}
		enc.Reset(io.Discard)
		return nil
	default:
		return fmt.Errorf("%w: %d", ErrUnsupportedCodec, uint16(codec))
	}
}

func crc32Checksum(data []byte) uint32 {
	return crc32.Checksum(data, crc32cTable)
}
