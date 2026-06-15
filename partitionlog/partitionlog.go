package partitionlog

import (
	"context"
	"fmt"
	"time"

	blobcache "github.com/ankur-anand/unijord/partitionlog/blob/cache"
	"github.com/ankur-anand/unijord/partitionlog/catalog/writeradapter"
	"github.com/ankur-anand/unijord/partitionlog/reader"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
	lowwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

type Options struct {
	Store   Store
	Reader  ReaderOptions
	Metrics Metrics
}

// ReaderOptions configures the default reader created by Open.
type ReaderOptions struct {
	MaxRecordsPerBatch int

	// RangeCacheBytes is the memory budget for cached byte ranges read from
	// segment objects.
	RangeCacheBytes uint64

	// OpenSegmentReaders is the number of parsed segment readers to keep in
	// memory. It avoids repeatedly opening and parsing hot segment metadata.
	OpenSegmentReaders int

	Refresh RefreshPolicy
}

// WriterOptions configures one per-partition writer opened from a Log.
type WriterOptions struct {
	Partition uint32
	WriterID  [16]byte

	Batch        BatchPolicy
	Backpressure BackpressurePolicy
	Pipeline     WriterPipelineOptions
}

// Log is one partitionlog client over one configured store.
type Log struct {
	store   Store
	metrics Metrics
	reader  *Reader
}

// Open validates a complete Store and prepares the default reader runtime.
func Open(opts Options) (*Log, error) {
	if opts.Store == nil {
		return nil, fmt.Errorf("partitionlog: nil store")
	}
	r, err := newReader(opts.Store, opts.Reader, opts.Metrics)
	if err != nil {
		return nil, err
	}
	return &Log{store: opts.Store, metrics: opts.Metrics, reader: r}, nil
}

// Reader returns the default reader runtime for this log.
func (l *Log) Reader() *Reader {
	return l.reader
}

// NewReader creates an additional reader runtime over the same store.
func (l *Log) NewReader(opts ReaderOptions) (*Reader, error) {
	if l == nil || l.store == nil {
		return nil, fmt.Errorf("partitionlog: nil log")
	}
	return newReader(l.store, opts, l.metrics)
}

// OpenWriter opens one fenced writer for one partition.
func (l *Log) OpenWriter(ctx context.Context, opts WriterOptions) (*Writer, error) {
	if l == nil || l.store == nil {
		return nil, fmt.Errorf("partitionlog: nil log")
	}
	if err := validateWriterOptions(opts); err != nil {
		return nil, err
	}

	catalogWriterManager := l.store.WriterManager()
	if catalogWriterManager == nil {
		return nil, fmt.Errorf("partitionlog: nil writer catalog")
	}
	sinkFactory := l.store.SinkFactory()
	if sinkFactory == nil {
		return nil, fmt.Errorf("partitionlog: nil sink factory")
	}
	wopts := lowwriter.DefaultOptions(sinkFactory)
	if opts.Batch.MaxRecords > 0 {
		wopts.Roll.MaxSegmentRecords = opts.Batch.MaxRecords
	}
	if opts.Batch.MaxBytes > 0 {
		wopts.Roll.MaxSegmentRawBytes = opts.Batch.MaxBytes
	}
	if opts.Batch.MaxDelay != 0 {
		wopts.Roll.MaxSegmentAge = opts.Batch.MaxDelay
	}
	if opts.Backpressure.MaxPendingBatches != 0 {
		wopts.Queue.MaxInflightSegments = opts.Backpressure.MaxPendingBatches
	}
	if opts.Backpressure.MaxPendingBytes > 0 {
		wopts.Queue.MaxInflightBytes = opts.Backpressure.MaxPendingBytes
	}
	if err := applyWriterPipelineOptions(&wopts, opts.Partition, opts.Pipeline); err != nil {
		return nil, err
	}
	if l.metrics != nil {
		wopts.Observer = writerMetricsAdapter{metrics: l.metrics}
	}

	catalogSession, err := catalogWriterManager.OpenWriter(ctx, opts.Partition, opts.WriterID)
	if err != nil {
		return nil, err
	}
	session, err := writeradapter.New(catalogSession)
	if err != nil {
		return nil, err
	}
	wopts.Session = session
	inner, err := lowwriter.New(wopts)
	if err != nil {
		return nil, err
	}
	return &Writer{inner: inner, partition: opts.Partition, metrics: l.metrics}, nil
}

// Writer appends records to one fenced partition.
type Writer struct {
	inner     *lowwriter.Writer
	partition uint32
	metrics   Metrics
}

// Append assigns the next LSN and appends record to this writer's partition.
func (w *Writer) Append(ctx context.Context, record Record) (result AppendResult, err error) {
	start := time.Now()
	recordSize, _ := segformat.RecordSize(record.Headers, record.Value)
	recordBytes := uint64(0)
	if recordSize > 0 {
		recordBytes = uint64(recordSize)
	}
	defer func() {
		w.observe(Metric{
			Name:      MetricWriterAppend,
			Partition: w.partition,
			LSN:       result.LSN,
			Records:   1,
			Bytes:     recordBytes,
			Duration:  time.Since(start),
			Err:       err,
		})
	}()
	innerResult, err := w.inner.Append(ctx, lowwriter.Record{
		TimestampMS: record.TimestampMS,
		Headers:     record.Headers,
		Value:       record.Value,
	})
	if err != nil {
		return AppendResult{}, err
	}
	result = AppendResult{LSN: innerResult.LSN}
	return result, nil
}

// Cut rotates the current active segment if it contains records.
func (w *Writer) Cut(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		w.observeWriterOperation(MetricWriterCut, time.Since(start), err)
	}()
	return w.inner.Cut(ctx)
}

// Flush publishes all records accepted before Flush returns.
func (w *Writer) Flush(ctx context.Context) (result Snapshot, err error) {
	start := time.Now()
	defer func() {
		w.observeWriterSnapshotOperation(MetricWriterFlush, result, time.Since(start), err)
	}()
	snapshot, err := w.inner.Flush(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotFromWriter(snapshot), nil
}

// Close flushes and closes the writer.
func (w *Writer) Close(ctx context.Context) (result Snapshot, err error) {
	start := time.Now()
	defer func() {
		w.observeWriterSnapshotOperation(MetricWriterClose, result, time.Since(start), err)
	}()
	snapshot, err := w.inner.Close(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	return snapshotFromWriter(snapshot), nil
}

func (w *Writer) Abort(ctx context.Context) (err error) {
	start := time.Now()
	defer func() {
		w.observeWriterOperation(MetricWriterAbort, time.Since(start), err)
	}()
	return w.inner.Abort(ctx)
}

func (w *Writer) State() WriterState {
	return stateFromWriter(w.inner.State())
}

func (w *Writer) Err() error {
	return w.inner.Err()
}

func (w *Writer) observeWriterOperation(name MetricName, duration time.Duration, err error) {
	w.observeWriterSnapshotOperation(name, Snapshot{}, duration, err)
}

func (w *Writer) observeWriterSnapshotOperation(name MetricName, snapshot Snapshot, duration time.Duration, err error) {
	metric := Metric{
		Name:      name,
		Partition: w.partition,
		Duration:  duration,
		Err:       err,
	}
	if snapshot.Head.Partition != 0 || snapshot.Head.NextLSN != 0 || snapshot.Head.SegmentCount != 0 {
		metric.NextLSN = snapshot.Head.NextLSN
		metric.SegmentCount = snapshot.Head.SegmentCount
	}
	w.observe(metric)
}

func (w *Writer) observe(metric Metric) {
	if w.metrics == nil {
		return
	}
	state := w.inner.State()
	metric.InflightSegments = state.InflightSegments
	metric.InflightBytes = state.InflightBytes
	w.metrics.Observe(metric)
}

func newReader(store Store, opts ReaderOptions, metrics Metrics) (*Reader, error) {
	cat := store.ReaderCatalog()
	if cat == nil {
		return nil, fmt.Errorf("partitionlog: nil reader catalog")
	}
	segmentStore := store.SegmentStore()
	if segmentStore == nil {
		return nil, fmt.Errorf("partitionlog: nil segment store")
	}

	ropts := reader.Options{
		MaxRecordsPerBatch: opts.MaxRecordsPerBatch,
		Refresh:            opts.Refresh,
	}
	if metrics != nil {
		ropts.Observer = readerMetricsAdapter{metrics: metrics}
	}
	if opts.RangeCacheBytes > 0 {
		cachedStore, err := blobcache.NewStore(segmentStore, blobcache.NewLRU(opts.RangeCacheBytes))
		if err != nil {
			return nil, err
		}
		segmentStore = cachedStore
	}
	if opts.OpenSegmentReaders > 0 {
		segmentCache, err := reader.NewSegmentReaderCache(opts.OpenSegmentReaders)
		if err != nil {
			return nil, err
		}
		ropts.SegmentCache = segmentCache
	}
	return reader.New(cat, segmentStore, ropts)
}

func validateWriterOptions(opts WriterOptions) error {
	switch {
	case opts.Batch.MaxDelay < 0:
		return fmt.Errorf("partitionlog: negative batch max delay %s", opts.Batch.MaxDelay)
	case opts.Backpressure.MaxPendingBatches < 0:
		return fmt.Errorf("partitionlog: negative max pending batches %d", opts.Backpressure.MaxPendingBatches)
	default:
		return validateWriterPipelineOptions(opts.Pipeline)
	}
}

func applyWriterPipelineOptions(wopts *lowwriter.Options, partition uint32, opts WriterPipelineOptions) error {
	if !hasWriterPipelineOptions(opts) {
		return nil
	}
	segment := segwriter.DefaultOptions(partition)
	if opts.BlockBytes > 0 {
		segment.TargetBlockSize = opts.BlockBytes
	}
	if opts.PartBytes > 0 {
		segment.PartSize = opts.PartBytes
	}
	if opts.SealParallelism > 0 {
		segment.SealParallelism = opts.SealParallelism
	}
	if opts.BlockBuffers > 0 {
		segment.BlockBufferCount = opts.BlockBuffers
	}
	if opts.UploadParallelism > 0 {
		segment.UploadParallelism = opts.UploadParallelism
	}
	if opts.UploadQueueSize > 0 {
		segment.UploadQueueSize = opts.UploadQueueSize
	}
	if opts.UploadLimiter != nil {
		segment.UploadLimiter = opts.UploadLimiter
	}
	wopts.SegmentOptions = segment
	return nil
}

func hasWriterPipelineOptions(opts WriterPipelineOptions) bool {
	return opts.BlockBytes != 0 ||
		opts.PartBytes != 0 ||
		opts.SealParallelism != 0 ||
		opts.BlockBuffers != 0 ||
		opts.UploadParallelism != 0 ||
		opts.UploadQueueSize != 0 ||
		opts.UploadLimiter != nil
}

func validateWriterPipelineOptions(opts WriterPipelineOptions) error {
	switch {
	case opts.BlockBytes < 0:
		return fmt.Errorf("partitionlog: negative block bytes %d", opts.BlockBytes)
	case opts.PartBytes < 0:
		return fmt.Errorf("partitionlog: negative part bytes %d", opts.PartBytes)
	case opts.SealParallelism < 0:
		return fmt.Errorf("partitionlog: negative seal parallelism %d", opts.SealParallelism)
	case opts.BlockBuffers < 0:
		return fmt.Errorf("partitionlog: negative block buffers %d", opts.BlockBuffers)
	case opts.UploadParallelism < 0:
		return fmt.Errorf("partitionlog: negative upload parallelism %d", opts.UploadParallelism)
	case opts.UploadQueueSize < 0:
		return fmt.Errorf("partitionlog: negative upload queue size %d", opts.UploadQueueSize)
	default:
		return nil
	}
}

func snapshotFromWriter(snapshot lowwriter.Snapshot) Snapshot {
	return Snapshot{
		Head: snapshot.Head,
		Identity: WriterIdentity{
			Epoch: snapshot.Identity.Epoch,
			Tag:   snapshot.Identity.Tag,
		},
	}
}

func stateFromWriter(state lowwriter.State) WriterState {
	return WriterState{
		Snapshot:          snapshotFromWriter(state.Snapshot),
		OptimisticNextLSN: state.OptimisticNextLSN,
		InflightSegments:  state.InflightSegments,
		InflightBytes:     state.InflightBytes,
	}
}
