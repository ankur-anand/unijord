package segwriter

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"time"

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
	SealParallelism   int
	BlockBufferCount  int
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
	// Metadata is the stable commit summary intended for manifest/catalog
	// publication. Trailer is the raw on-disk trailer, kept for debugging and
	// tests that want to compare exact format metadata.
	Metadata Metadata
	Object   CommittedObject
	Trailer  segformat.Trailer
}

// Writer owns one segment and is not safe for concurrent use. The partition
// writer should call Append, Close, and Abort from one goroutine.
type Writer struct {
	opts Options
	sink Sink

	ctx    context.Context
	cancel context.CancelFunc

	freeBuffers chan *blockBuffer
	sealJobs    chan *blockBuffer
	sealedOut   chan sealedBlockResult
	emitted     chan emitResult

	sealWG        sync.WaitGroup
	sealJobsOnce  sync.Once
	sealedOutOnce sync.Once

	packerMu sync.Mutex
	packer   *packer

	active *blockBuffer

	hasRecords bool
	closed     bool
	aborted    bool
	errMu      sync.Mutex
	firstErr   error

	baseLSN        uint64
	nextLSN        uint64
	recordCount    uint32
	minTimestampMS int64
	maxTimestampMS int64
	nextSeq        uint64
}

type blockBuffer struct {
	Seq            uint64
	Raw            []byte
	BaseLSN        uint64
	RecordCount    uint32
	MinTimestampMS int64
	MaxTimestampMS int64
}

type sealedBlockResult struct {
	Buf    *blockBuffer
	Seq    uint64
	Sealed segblock.Sealed
	Err    error
}

type emitResult struct {
	Index  []segformat.BlockIndexEntry
	Packer *packer
	Err    error
}

func DefaultOptions(partition uint32) Options {
	sealParallelism := defaultSealParallelism()
	return Options{
		Partition:         partition,
		Codec:             segformat.CodecZstd,
		HashAlgo:          segformat.HashXXH64,
		TargetBlockSize:   DefaultTargetBlockSize,
		PartSize:          DefaultPartSize,
		SealParallelism:   sealParallelism,
		BlockBufferCount:  2*sealParallelism + 1,
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
	ctx, cancel := context.WithCancel(context.Background())
	w := &Writer{
		opts:        normalized,
		sink:        sink,
		ctx:         ctx,
		cancel:      cancel,
		freeBuffers: make(chan *blockBuffer, normalized.BlockBufferCount),
		sealJobs:    make(chan *blockBuffer, normalized.BlockBufferCount),
		sealedOut:   make(chan sealedBlockResult, normalized.BlockBufferCount),
		emitted:     make(chan emitResult, 1),
	}
	for i := 0; i < normalized.BlockBufferCount; i++ {
		w.freeBuffers <- &blockBuffer{}
	}
	if err := w.takeFreeBuffer(ctx); err != nil {
		cancel()
		return nil, err
	}
	for i := 0; i < normalized.SealParallelism; i++ {
		w.sealWG.Add(1)
		go w.sealWorker()
	}
	go w.emitter()
	return w, nil
}

func (w *Writer) Append(ctx context.Context, r Record) error {
	if w.closed {
		return ErrWriterClosed
	}
	if w.aborted {
		return ErrWriterAborted
	}
	if err := w.getFirstErr(); err != nil {
		return w.fail(ctx, err)
	}
	if err := w.validateRecord(r); err != nil {
		return w.fail(ctx, err)
	}

	recordSize := segformat.RecordHeaderSize + len(r.Value)
	if w.active.Len() > 0 && !w.active.CanAppend(recordSize, w.opts.TargetBlockSize) {
		if err := w.enqueueActive(ctx); err != nil {
			return w.fail(ctx, err)
		}
		if err := w.takeFreeBuffer(ctx); err != nil {
			return w.fail(ctx, err)
		}
	}
	if err := w.active.Append(r, w.opts.TargetBlockSize); err != nil {
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
	if err := w.getFirstErr(); err != nil {
		return Result{}, err
	}
	if !w.hasRecords {
		w.setFirstErr(ErrEmptySegment)
		_ = w.finishPipeline()
		w.aborted = true
		return Result{}, ErrEmptySegment
	}
	if w.active != nil && w.active.Len() > 0 {
		if err := w.enqueueActive(ctx); err != nil {
			return Result{}, w.fail(ctx, err)
		}
		w.active = nil
	}
	emitted := w.finishPipeline()
	if emitted.Err != nil {
		return Result{}, w.failCompletedPipeline(ctx, emitted.Err, emitted.Packer)
	}
	if err := w.getFirstErr(); err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, emitted.Packer)
	}

	p := emitted.Packer
	if p == nil {
		return Result{}, w.failCompletedPipeline(ctx, ErrEmptySegment, nil)
	}
	indexOffset := p.Offset()
	trailer := w.trailer(indexOffset, uint32(len(emitted.Index)), 0)
	indexBytes, _, err := segformat.MarshalBlockIndex(emitted.Index, trailer)
	if err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, p)
	}
	if err := p.WriteBody(ctx, indexBytes); err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, p)
	}
	trailer.SegmentHash = p.BodyHash()
	trailerBytes, sealedTrailer, err := segformat.MarshalTrailer(trailer)
	if err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, p)
	}
	trailer = sealedTrailer
	if err := p.WriteFinal(ctx, trailerBytes); err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, p)
	}
	object, err := p.Complete(ctx)
	if err != nil {
		return Result{}, w.failCompletedPipeline(ctx, err, p)
	}

	w.closed = true
	w.cancel()
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
	w.setFirstErr(ErrWriterAborted)
	w.finishPipeline()
	if p := w.getPacker(); p != nil {
		return p.Abort(ctx)
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

func (w *Writer) takeFreeBuffer(ctx context.Context) error {
	select {
	case buf := <-w.freeBuffers:
		buf.Reset()
		w.active = buf
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		if err := w.getFirstErr(); err != nil {
			return err
		}
		return ErrWriterAborted
	}
}

func (w *Writer) enqueueActive(ctx context.Context) error {
	if w.active == nil || w.active.Len() == 0 {
		return nil
	}
	w.active.Seq = w.nextSeq
	w.nextSeq++
	buf := w.active
	w.active = nil
	select {
	case w.sealJobs <- buf:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.ctx.Done():
		if err := w.getFirstErr(); err != nil {
			return err
		}
		return ErrWriterAborted
	}
}

func (w *Writer) sealWorker() {
	defer w.sealWG.Done()
	for buf := range w.sealJobs {
		sealed, err := segblock.SealOwned(w.opts.Codec, w.opts.HashAlgo, buf.Raw, buf.Meta())
		result := sealedBlockResult{
			Buf:    buf,
			Seq:    buf.Seq,
			Sealed: sealed,
			Err:    err,
		}
		select {
		case w.sealedOut <- result:
		case <-w.ctx.Done():
			return
		}
		if err != nil {
			w.setFirstErr(err)
			return
		}
	}
}

func (w *Writer) emitter() {
	var index []segformat.BlockIndexEntry
	pending := make(map[uint64]sealedBlockResult)
	nextEmit := uint64(0)
	for result := range w.sealedOut {
		if result.Err != nil {
			w.setFirstErr(result.Err)
			w.returnBuffer(result.Buf)
			continue
		}
		pending[result.Seq] = result
		for {
			next, ok := pending[nextEmit]
			if !ok {
				break
			}
			entry, err := w.emitSealed(next.Sealed)
			w.returnBuffer(next.Buf)
			delete(pending, nextEmit)
			if err != nil {
				w.setFirstErr(err)
				w.emitted <- emitResult{Err: err, Packer: w.getPacker()}
				return
			}
			index = append(index, entry)
			nextEmit++
		}
	}
	if err := w.getFirstErr(); err != nil {
		w.emitted <- emitResult{Err: err, Packer: w.getPacker()}
		return
	}
	w.emitted <- emitResult{Index: index, Packer: w.getPacker()}
}

func (w *Writer) emitSealed(sealed segblock.Sealed) (segformat.BlockIndexEntry, error) {
	p, err := w.ensurePacker(w.ctx)
	if err != nil {
		return segformat.BlockIndexEntry{}, err
	}
	blockOffset := p.Offset()
	preambleBytes, err := sealed.Preamble.MarshalBinary()
	if err != nil {
		return segformat.BlockIndexEntry{}, err
	}
	if err := p.WriteBody(w.ctx, preambleBytes); err != nil {
		return segformat.BlockIndexEntry{}, err
	}
	if err := p.WriteBody(w.ctx, sealed.Stored); err != nil {
		return segformat.BlockIndexEntry{}, err
	}
	return segformat.BlockIndexEntry{
		BlockOffset:    blockOffset,
		StoredSize:     sealed.Preamble.StoredSize,
		RawSize:        sealed.Preamble.RawSize,
		RecordCount:    sealed.Preamble.RecordCount,
		BaseLSN:        sealed.Preamble.BaseLSN,
		MinTimestampMS: sealed.Preamble.MinTimestampMS,
		MaxTimestampMS: sealed.Preamble.MaxTimestampMS,
		BlockHash:      sealed.Preamble.BlockHash,
	}, nil
}

func (w *Writer) ensurePacker(ctx context.Context) (*packer, error) {
	w.packerMu.Lock()
	defer w.packerMu.Unlock()
	if w.packer != nil {
		return w.packer, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	txn, err := w.sink.Begin(ctx, Plan{
		Partition: w.opts.Partition,
		Codec:     w.opts.Codec,
		HashAlgo:  w.opts.HashAlgo,
		PartSize:  w.opts.PartSize,
	})
	if err != nil {
		return nil, err
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
		return nil, err
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
		return nil, err
	}
	if err := p.WriteBody(ctx, preamble); err != nil {
		_ = p.Abort(ctx)
		return nil, err
	}
	w.packer = p
	return p, nil
}

func (w *Writer) getPacker() *packer {
	w.packerMu.Lock()
	defer w.packerMu.Unlock()
	return w.packer
}

func (w *Writer) trailer(indexOffset uint64, blockCount uint32, segmentHash uint64) segformat.Trailer {
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
	w.setFirstErr(err)
	w.aborted = true
	w.finishPipeline()
	if p := w.getPacker(); p != nil {
		_ = p.Abort(ctx)
	}
	return err
}

func (w *Writer) failCompletedPipeline(ctx context.Context, err error, p *packer) error {
	if err == nil {
		return nil
	}
	w.setFirstErr(err)
	w.aborted = true
	if p != nil {
		_ = p.Abort(ctx)
	}
	return err
}

func (w *Writer) setFirstErr(err error) {
	if err == nil {
		return
	}
	w.errMu.Lock()
	if w.firstErr == nil {
		w.firstErr = err
	}
	w.errMu.Unlock()
	w.cancel()
}

func (w *Writer) getFirstErr() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.firstErr
}

func (w *Writer) finishPipeline() emitResult {
	w.closeSealJobs()
	w.sealWG.Wait()
	w.closeSealedOut()
	return <-w.emitted
}

func (w *Writer) closeSealJobs() {
	w.sealJobsOnce.Do(func() {
		close(w.sealJobs)
	})
}

func (w *Writer) closeSealedOut() {
	w.sealedOutOnce.Do(func() {
		close(w.sealedOut)
	})
}

func (w *Writer) returnBuffer(buf *blockBuffer) {
	if buf == nil {
		return
	}
	buf.Reset()
	select {
	case w.freeBuffers <- buf:
	case <-w.ctx.Done():
	}
}

func normalizeOptions(opts Options) (Options, error) {
	if err := opts.Codec.Validate(); err != nil {
		return Options{}, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if err := opts.HashAlgo.Validate(); err != nil {
		return Options{}, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
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
	if opts.SealParallelism <= 0 {
		opts.SealParallelism = defaultSealParallelism()
	}
	if opts.BlockBufferCount <= 0 {
		opts.BlockBufferCount = 2*opts.SealParallelism + 1
	}
	if opts.BlockBufferCount < opts.SealParallelism+1 {
		opts.BlockBufferCount = opts.SealParallelism + 1
	}
	if opts.UploadParallelism <= 0 {
		opts.UploadParallelism = DefaultUploadParallelism
	}
	if opts.UploadQueueSize <= 0 {
		opts.UploadQueueSize = opts.UploadParallelism
	}
	if isZero16(opts.SegmentUUID) {
		if _, err := rand.Read(opts.SegmentUUID[:]); err != nil {
			return Options{}, fmt.Errorf("%w: segment uuid: %w", ErrInvalidOptions, err)
		}
	}
	if opts.CreatedUnixMS == 0 {
		opts.CreatedUnixMS = time.Now().UTC().UnixMilli()
	}
	return opts, nil
}

func isZero16(v [16]byte) bool {
	for _, b := range v {
		if b != 0 {
			return false
		}
	}
	return true
}

func defaultSealParallelism() int {
	n := runtime.NumCPU()
	if n > 4 {
		n = 4
	}
	if n < 1 {
		n = 1
	}
	return n
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

func (b *blockBuffer) Reset() {
	b.Seq = 0
	b.Raw = b.Raw[:0]
	b.BaseLSN = 0
	b.RecordCount = 0
	b.MinTimestampMS = 0
	b.MaxTimestampMS = 0
}

func (b *blockBuffer) Len() int {
	return len(b.Raw)
}

func (b *blockBuffer) CanAppend(recordSize int, targetBlockSize int) bool {
	if recordSize <= 0 || recordSize > segformat.MaxRawBlockSize {
		return false
	}
	nextSize := len(b.Raw) + recordSize
	if nextSize > segformat.MaxRawBlockSize {
		return false
	}
	if len(b.Raw) == 0 {
		return true
	}
	return nextSize <= targetBlockSize
}

func (b *blockBuffer) Append(r Record, targetBlockSize int) error {
	recordSize := segformat.RecordHeaderSize + len(r.Value)
	if !b.CanAppend(recordSize, segformat.MaxRawBlockSize) {
		return fmt.Errorf("%w: raw block size=%d max=%d", segformat.ErrBlockTooLarge, len(b.Raw)+recordSize, segformat.MaxRawBlockSize)
	}
	if b.Raw == nil {
		b.Raw = make([]byte, 0, initialRawBlockCapacity(recordSize, targetBlockSize))
	}
	if b.RecordCount == 0 {
		b.BaseLSN = r.LSN
		b.MinTimestampMS = r.TimestampMS
	} else if r.TimestampMS < b.MaxTimestampMS {
		return fmt.Errorf("%w: got=%d previous=%d", ErrTimestampOrder, r.TimestampMS, b.MaxTimestampMS)
	}
	b.MaxTimestampMS = r.TimestampMS

	b.Raw = slices.Grow(b.Raw, recordSize)
	off := len(b.Raw)
	b.Raw = b.Raw[:off+recordSize]
	binary.BigEndian.PutUint64(b.Raw[off:off+8], uint64(r.TimestampMS))
	binary.BigEndian.PutUint32(b.Raw[off+8:off+12], uint32(len(r.Value)))
	copy(b.Raw[off+segformat.RecordHeaderSize:], r.Value)
	b.RecordCount++
	return nil
}

func (b *blockBuffer) Meta() segblock.Meta {
	return segblock.Meta{
		BaseLSN:        b.BaseLSN,
		RecordCount:    b.RecordCount,
		MinTimestampMS: b.MinTimestampMS,
		MaxTimestampMS: b.MaxTimestampMS,
	}
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
