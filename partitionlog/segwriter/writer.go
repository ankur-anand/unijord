package segwriter

import (
	"context"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/ankur-anand/unijord/partitionlog/segblock"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const (
	DefaultTargetBlockSize   = 1 << 20
	DefaultPartSize          = 8 << 20
	DefaultUploadParallelism = 2
)

type Options struct {
	Partition uint32

	Codec    segformat.Codec
	HashAlgo segformat.HashAlgo

	TargetBlockSize   int
	PartSize          int
	UploadParallelism int
	UploadQueueSize   int
	UploadLimiter     UploadLimiter

	SegmentUUID   [16]byte
	WriterTag     [16]byte
	CreatedUnixMS int64
}

type Record struct {
	LSN         uint64
	TimestampMS int64
	Value       []byte
}

type Metadata struct {
	Partition        uint32
	BaseLSN          uint64
	LastLSN          uint64
	MinTimestampMS   int64
	MaxTimestampMS   int64
	RecordCount      uint32
	BlockCount       uint32
	SizeBytes        uint64
	BlockIndexOffset uint64
	BlockIndexLength uint32
	SegmentUUID      [16]byte
	Codec            segformat.Codec
	HashAlgo         segformat.HashAlgo
	SegmentHash      uint64
	TrailerHash      uint64
}

type Result struct {
	Metadata Metadata
	Object   CommittedObject
	Trailer  segformat.Trailer
}

type Writer struct {
	opts Options
	sink Sink

	txn    Txn
	packer *packer

	builder rawBlockBuilder
	index   []segformat.BlockIndexEntry

	hasRecords bool
	closed     bool
	aborted    bool

	baseLSN        uint64
	nextLSN        uint64
	recordCount    uint32
	minTimestampMS int64
	maxTimestampMS int64
}

func DefaultOptions(partition uint32) Options {
	return Options{
		Partition:         partition,
		Codec:             segformat.CodecZstd,
		HashAlgo:          segformat.HashXXH64,
		TargetBlockSize:   DefaultTargetBlockSize,
		PartSize:          DefaultPartSize,
		UploadParallelism: DefaultUploadParallelism,
		UploadQueueSize:   DefaultUploadParallelism,
		SegmentUUID:       [16]byte{},
		WriterTag:         [16]byte{},
		CreatedUnixMS:     0,
	}
}

func New(opts Options, sink Sink) (*Writer, error) {
	if sink == nil {
		return nil, fmt.Errorf("%w: sink is nil", ErrInvalidOptions)
	}
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Writer{
		opts: normalized,
		sink: sink,
	}, nil
}

func (w *Writer) Append(ctx context.Context, r Record) error {
	if w.closed {
		return ErrWriterClosed
	}
	if w.aborted {
		return ErrWriterAborted
	}
	if err := w.validateRecord(r); err != nil {
		return w.fail(ctx, err)
	}

	recordSize := segformat.RecordHeaderSize + len(r.Value)
	if w.builder.Len() > 0 && !w.builder.CanAppend(recordSize, w.opts.TargetBlockSize) {
		if err := w.flushBlock(ctx); err != nil {
			return w.fail(ctx, err)
		}
	}
	if err := w.builder.Append(r, w.opts.TargetBlockSize); err != nil {
		return w.fail(ctx, err)
	}

	if !w.hasRecords {
		w.hasRecords = true
		w.baseLSN = r.LSN
		w.minTimestampMS = r.TimestampMS
	} else if r.TimestampMS < w.minTimestampMS {
		w.minTimestampMS = r.TimestampMS
	}
	w.maxTimestampMS = r.TimestampMS
	w.nextLSN = r.LSN + 1
	w.recordCount++
	return nil
}

func (w *Writer) Close(ctx context.Context) (Result, error) {
	if w.closed {
		return Result{}, ErrWriterClosed
	}
	if w.aborted {
		return Result{}, ErrWriterAborted
	}
	if !w.hasRecords {
		return Result{}, ErrEmptySegment
	}
	if err := w.flushBlock(ctx); err != nil {
		return Result{}, w.fail(ctx, err)
	}

	indexOffset := w.packer.Offset()
	trailer := w.trailer(indexOffset, 0)
	indexBytes, _, err := segformat.MarshalBlockIndex(w.index, trailer)
	if err != nil {
		return Result{}, w.fail(ctx, err)
	}
	if err := w.packer.WriteBody(ctx, indexBytes); err != nil {
		return Result{}, w.fail(ctx, err)
	}
	trailer.SegmentHash = w.packer.BodyHash()

	trailerBytes, err := trailer.MarshalBinary()
	if err != nil {
		return Result{}, w.fail(ctx, err)
	}
	trailer.TrailerHash = binary.BigEndian.Uint64(trailerBytes[136:144])
	if err := w.packer.WriteFinal(ctx, trailerBytes); err != nil {
		return Result{}, w.fail(ctx, err)
	}
	object, err := w.packer.Complete(ctx)
	if err != nil {
		return Result{}, w.fail(ctx, err)
	}

	w.closed = true
	return Result{
		Metadata: metadataFromTrailer(trailer),
		Object:   object,
		Trailer:  trailer,
	}, nil
}

func (w *Writer) Abort(ctx context.Context) error {
	if w.closed || w.aborted {
		return nil
	}
	w.aborted = true
	if w.packer != nil {
		return w.packer.Abort(ctx)
	}
	return nil
}

func (w *Writer) validateRecord(r Record) error {
	if len(r.Value) > segformat.MaxRecordValueLen {
		return fmt.Errorf("%w: value_len=%d max=%d", segformat.ErrRecordTooLarge, len(r.Value), segformat.MaxRecordValueLen)
	}
	if !w.hasRecords {
		return nil
	}
	if w.nextLSN == 0 {
		return fmt.Errorf("%w: lsn range exhausted", ErrNonContiguousLSN)
	}
	if w.recordCount == segformat.MaxRecordCount {
		return fmt.Errorf("%w: record_count=%d max=%d", segformat.ErrInvalidSegment, w.recordCount, segformat.MaxRecordCount)
	}
	if r.LSN != w.nextLSN {
		return fmt.Errorf("%w: got=%d want=%d", ErrNonContiguousLSN, r.LSN, w.nextLSN)
	}
	if r.TimestampMS < w.maxTimestampMS {
		return fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, r.TimestampMS, w.maxTimestampMS)
	}
	return nil
}

func (w *Writer) flushBlock(ctx context.Context) error {
	if w.builder.Len() == 0 {
		return nil
	}
	raw, meta := w.builder.Freeze()
	sealed, err := segblock.SealOwned(w.opts.Codec, w.opts.HashAlgo, raw, meta)
	if err != nil {
		return err
	}
	if err := w.ensurePacker(ctx); err != nil {
		return err
	}
	blockOffset := w.packer.Offset()
	preambleBytes, err := sealed.Preamble.MarshalBinary()
	if err != nil {
		return err
	}
	if err := w.packer.WriteBody(ctx, preambleBytes); err != nil {
		return err
	}
	if err := w.packer.WriteBody(ctx, sealed.Stored); err != nil {
		return err
	}
	w.index = append(w.index, segformat.BlockIndexEntry{
		BlockOffset:    blockOffset,
		StoredSize:     sealed.Preamble.StoredSize,
		RawSize:        sealed.Preamble.RawSize,
		RecordCount:    sealed.Preamble.RecordCount,
		BaseLSN:        sealed.Preamble.BaseLSN,
		MinTimestampMS: sealed.Preamble.MinTimestampMS,
		MaxTimestampMS: sealed.Preamble.MaxTimestampMS,
		BlockHash:      sealed.Preamble.BlockHash,
	})
	return nil
}

func (w *Writer) ensurePacker(ctx context.Context) error {
	if w.packer != nil {
		return nil
	}
	txn, err := w.sink.Begin(ctx, Plan{
		Partition: w.opts.Partition,
		Codec:     w.opts.Codec,
		HashAlgo:  w.opts.HashAlgo,
		PartSize:  w.opts.PartSize,
	})
	if err != nil {
		return err
	}
	p, err := newPacker(ctx, txn, packerOptions{
		PartSize:          w.opts.PartSize,
		UploadParallelism: w.opts.UploadParallelism,
		UploadQueueSize:   w.opts.UploadQueueSize,
		HashAlgo:          w.opts.HashAlgo,
		UploadLimiter:     w.opts.UploadLimiter,
	})
	if err != nil {
		_ = txn.Abort(ctx)
		return err
	}
	preamble, err := (segformat.FilePreamble{
		Partition:    w.opts.Partition,
		Codec:        w.opts.Codec,
		HashAlgo:     w.opts.HashAlgo,
		RecordFormat: segformat.DefaultRecordFormat,
		BaseLSN:      w.baseLSN,
		SegmentUUID:  w.opts.SegmentUUID,
		WriterTag:    w.opts.WriterTag,
	}).MarshalBinary()
	if err != nil {
		_ = p.Abort(ctx)
		return err
	}
	if err := p.WriteBody(ctx, preamble); err != nil {
		_ = p.Abort(ctx)
		return err
	}
	w.txn = txn
	w.packer = p
	return nil
}

func (w *Writer) trailer(indexOffset uint64, segmentHash uint64) segformat.Trailer {
	blockCount := uint32(len(w.index))
	indexLength := uint32(segformat.IndexPreambleSize) + blockCount*uint32(segformat.BlockIndexEntrySize)
	return segformat.Trailer{
		Partition:        w.opts.Partition,
		Codec:            w.opts.Codec,
		HashAlgo:         w.opts.HashAlgo,
		RecordFormat:     segformat.DefaultRecordFormat,
		BaseLSN:          w.baseLSN,
		LastLSN:          w.nextLSN - 1,
		MinTimestampMS:   w.minTimestampMS,
		MaxTimestampMS:   w.maxTimestampMS,
		RecordCount:      w.recordCount,
		BlockCount:       blockCount,
		BlockIndexOffset: indexOffset,
		BlockIndexLength: indexLength,
		TotalSize:        indexOffset + uint64(indexLength) + uint64(segformat.TrailerSize),
		CreatedUnixMS:    w.opts.CreatedUnixMS,
		SegmentUUID:      w.opts.SegmentUUID,
		WriterTag:        w.opts.WriterTag,
		SegmentHash:      segmentHash,
	}
}

func (w *Writer) fail(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	w.aborted = true
	if w.packer != nil {
		_ = w.packer.Abort(ctx)
	}
	return err
}

func normalizeOptions(opts Options) (Options, error) {
	if err := opts.Codec.Validate(); err != nil {
		return Options{}, err
	}
	if err := opts.HashAlgo.Validate(); err != nil {
		return Options{}, err
	}
	if opts.TargetBlockSize <= 0 {
		opts.TargetBlockSize = DefaultTargetBlockSize
	}
	if opts.TargetBlockSize > segformat.MaxRawBlockSize {
		opts.TargetBlockSize = segformat.MaxRawBlockSize
	}
	if opts.PartSize <= 0 {
		opts.PartSize = DefaultPartSize
	}
	if opts.UploadParallelism <= 0 {
		opts.UploadParallelism = DefaultUploadParallelism
	}
	if opts.UploadQueueSize <= 0 {
		opts.UploadQueueSize = opts.UploadParallelism
	}
	return opts, nil
}

func metadataFromTrailer(t segformat.Trailer) Metadata {
	return Metadata{
		Partition:        t.Partition,
		BaseLSN:          t.BaseLSN,
		LastLSN:          t.LastLSN,
		MinTimestampMS:   t.MinTimestampMS,
		MaxTimestampMS:   t.MaxTimestampMS,
		RecordCount:      t.RecordCount,
		BlockCount:       t.BlockCount,
		SizeBytes:        t.TotalSize,
		BlockIndexOffset: t.BlockIndexOffset,
		BlockIndexLength: t.BlockIndexLength,
		SegmentUUID:      t.SegmentUUID,
		Codec:            t.Codec,
		HashAlgo:         t.HashAlgo,
		SegmentHash:      t.SegmentHash,
		TrailerHash:      t.TrailerHash,
	}
}

type rawBlockBuilder struct {
	buf            []byte
	baseLSN        uint64
	recordCount    uint32
	minTimestampMS int64
	maxTimestampMS int64
}

func (b *rawBlockBuilder) Len() int {
	return len(b.buf)
}

func (b *rawBlockBuilder) CanAppend(recordSize int, targetBlockSize int) bool {
	if recordSize <= 0 || recordSize > segformat.MaxRawBlockSize {
		return false
	}
	nextSize := len(b.buf) + recordSize
	if nextSize > segformat.MaxRawBlockSize {
		return false
	}
	if len(b.buf) == 0 {
		return true
	}
	return nextSize <= targetBlockSize
}

func (b *rawBlockBuilder) Append(r Record, targetBlockSize int) error {
	recordSize := segformat.RecordHeaderSize + len(r.Value)
	if !b.CanAppend(recordSize, segformat.MaxRawBlockSize) {
		return fmt.Errorf("%w: raw block size=%d max=%d", segformat.ErrBlockTooLarge, len(b.buf)+recordSize, segformat.MaxRawBlockSize)
	}
	if b.buf == nil {
		b.buf = make([]byte, 0, initialRawBlockCapacity(recordSize, targetBlockSize))
	}
	if b.recordCount == 0 {
		b.baseLSN = r.LSN
		b.minTimestampMS = r.TimestampMS
	} else if r.TimestampMS < b.maxTimestampMS {
		return fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, r.TimestampMS, b.maxTimestampMS)
	}
	b.maxTimestampMS = r.TimestampMS

	b.buf = slices.Grow(b.buf, recordSize)
	off := len(b.buf)
	b.buf = b.buf[:off+recordSize]
	binary.BigEndian.PutUint64(b.buf[off:off+8], uint64(r.TimestampMS))
	binary.BigEndian.PutUint32(b.buf[off+8:off+12], uint32(len(r.Value)))
	copy(b.buf[off+segformat.RecordHeaderSize:], r.Value)
	b.recordCount++
	return nil
}

func (b *rawBlockBuilder) Freeze() ([]byte, segblock.Meta) {
	raw := b.buf
	meta := segblock.Meta{
		BaseLSN:        b.baseLSN,
		RecordCount:    b.recordCount,
		MinTimestampMS: b.minTimestampMS,
		MaxTimestampMS: b.maxTimestampMS,
	}
	*b = rawBlockBuilder{}
	return raw, meta
}

func initialRawBlockCapacity(recordSize int, targetBlockSize int) int {
	if targetBlockSize <= 0 || targetBlockSize > segformat.MaxRawBlockSize {
		targetBlockSize = segformat.MaxRawBlockSize
	}
	capacity := 64 << 10
	if recordSize > capacity {
		capacity = recordSize
	}
	if capacity > targetBlockSize {
		capacity = targetBlockSize
	}
	return capacity
}
