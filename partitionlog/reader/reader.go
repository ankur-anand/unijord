package reader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segreader"
)

func New(cat catalog.Reader, store SegmentStore, opts Options) (*Reader, error) {
	if cat == nil {
		return nil, fmt.Errorf("%w: catalog is nil", ErrInvalidOptions)
	}
	if store == nil {
		return nil, fmt.Errorf("%w: segment store is nil", ErrInvalidOptions)
	}
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Reader{
		catalog: cat,
		store:   store,
		opts:    normalized,
		refresh: newRefreshCoordinator(cat, normalized.Refresh, normalized.Observer),
	}, nil
}

func (r *Reader) Head(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	return r.Partition(partition).Head(ctx)
}

func (r *Reader) ConsumeAfter(ctx context.Context, req ConsumeAfterRequest) (ConsumeResult, error) {
	if req.StartAfterLSN == math.MaxUint64 {
		return ConsumeResult{}, fmt.Errorf("%w: start_after_lsn=%d", ErrLSNExhausted, req.StartAfterLSN)
	}
	return r.Consume(ctx, ConsumeRequest{
		Partition: req.Partition,
		StartLSN:  req.StartAfterLSN + 1,
		Limit:     req.Limit,
	})
}

func (r *Reader) Fetch(ctx context.Context, req FetchRequest) (result FetchResult, err error) {
	start := time.Now()
	defer func() {
		records := 0
		if result.Found {
			records = 1
		}
		r.observe(MetricEvent{
			Name:      MetricFetch,
			Partition: req.Partition,
			StartLSN:  req.LSN,
			NextLSN:   result.Head.NextLSN,
			Limit:     1,
			Records:   records,
			Duration:  time.Since(start),
			Err:       err,
		})
	}()
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
		return FetchResult{}, err
	}
	head, err := r.catalog.LoadPartition(ctx, req.Partition)
	if err != nil {
		return FetchResult{}, err
	}
	result = FetchResult{Head: head}
	if req.LSN < head.OldestLSN {
		return FetchResult{}, LSNExpiredError{
			Requested: req.LSN,
			Oldest:    head.OldestLSN,
			HeadNext:  head.NextLSN,
		}
	}
	if req.LSN >= head.NextLSN {
		return result, nil
	}
	if !head.HasLastSegment {
		return FetchResult{}, fmt.Errorf("%w: no segment for partition=%d lsn=%d head_next=%d", ErrCorruptData, req.Partition, req.LSN, head.NextLSN)
	}

	segment, found, err := r.catalog.FindSegment(ctx, req.Partition, req.LSN)
	if err != nil {
		return FetchResult{}, err
	}
	if !found {
		return FetchResult{}, fmt.Errorf("%w: no segment for partition=%d lsn=%d head_next=%d", ErrCorruptData, req.Partition, req.LSN, head.NextLSN)
	}
	if segment.Partition != req.Partition {
		return FetchResult{}, fmt.Errorf("%w: segment partition=%d request partition=%d", ErrCorruptData, segment.Partition, req.Partition)
	}
	if req.LSN < segment.BaseLSN || req.LSN > segment.LastLSN {
		return FetchResult{}, fmt.Errorf("%w: segment base_lsn=%d last_lsn=%d does not contain lsn=%d", ErrCorruptData, segment.BaseLSN, segment.LastLSN, req.LSN)
	}

	records, err := r.readSegment(ctx, segment, req.LSN, 1, head.NextLSN)
	if err != nil {
		return FetchResult{}, err
	}
	if len(records) == 0 {
		return FetchResult{}, fmt.Errorf("%w: no record in segment uri=%s lsn=%d", ErrCorruptData, segment.URI, req.LSN)
	}
	if records[0].LSN != req.LSN {
		return FetchResult{}, fmt.Errorf("%w: segment uri=%s returned lsn=%d for requested lsn=%d", ErrCorruptData, segment.URI, records[0].LSN, req.LSN)
	}
	result.Record = records[0]
	result.Found = true
	return result, nil
}

func (r *Reader) Consume(ctx context.Context, req ConsumeRequest) (ConsumeResult, error) {
	start := time.Now()
	var result ConsumeResult
	var err error
	defer func() {
		r.observe(MetricEvent{
			Name:      MetricRead,
			Partition: req.Partition,
			StartLSN:  req.StartLSN,
			NextLSN:   result.NextLSN,
			Limit:     req.Limit,
			Records:   len(result.Records),
			Duration:  time.Since(start),
			Err:       err,
		})
	}()
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
		return ConsumeResult{}, err
	}
	if req.Limit < 0 {
		err = fmt.Errorf("%w: limit=%d", ErrInvalidRequest, req.Limit)
		return ConsumeResult{}, err
	}
	limit := r.normalizedLimit(req.Limit)
	head, err := r.refresh.headForRead(ctx, req.Partition, req.StartLSN, FreshnessOnTail)
	if err != nil {
		return ConsumeResult{}, err
	}
	result, err = r.consumeFromHead(ctx, head, req.Partition, req.StartLSN, limit)
	return result, err
}

func (r *Reader) consumeWithHead(ctx context.Context, head pmeta.PartitionHead, req ConsumeRequest) (ConsumeResult, error) {
	if err := ctx.Err(); err != nil {
		return ConsumeResult{}, err
	}
	if req.Limit < 0 {
		return ConsumeResult{}, fmt.Errorf("%w: limit=%d", ErrInvalidRequest, req.Limit)
	}
	if head.Partition != req.Partition {
		return ConsumeResult{}, fmt.Errorf("%w: head partition=%d request partition=%d", ErrInvalidRequest, head.Partition, req.Partition)
	}
	limit := r.normalizedLimit(req.Limit)
	return r.consumeFromHead(ctx, head, req.Partition, req.StartLSN, limit)
}

func (r *Reader) ConsumeFromTimestamp(ctx context.Context, req ConsumeFromTimestampRequest) (result ConsumeResult, err error) {
	start := time.Now()
	defer func() {
		r.observe(MetricEvent{
			Name:      MetricTimestampRead,
			Partition: req.Partition,
			NextLSN:   result.NextLSN,
			Limit:     req.Limit,
			Records:   len(result.Records),
			Duration:  time.Since(start),
			Err:       err,
		})
	}()
	if ctxErr := ctx.Err(); ctxErr != nil {
		err = ctxErr
		return ConsumeResult{}, err
	}
	if req.Limit < 0 {
		return ConsumeResult{}, fmt.Errorf("%w: limit=%d", ErrInvalidRequest, req.Limit)
	}
	limit := r.normalizedLimit(req.Limit)
	head, err := r.catalog.LoadPartition(ctx, req.Partition)
	if err != nil {
		return ConsumeResult{}, err
	}
	result = ConsumeResult{
		Head:    head,
		NextLSN: head.NextLSN,
	}
	if !head.HasLastSegment {
		return result, nil
	}

	from := head.OldestLSN
	for from < head.NextLSN {
		page, err := r.catalog.ListSegments(ctx, catalog.ListSegmentsRequest{
			Partition: req.Partition,
			FromLSN:   from,
			Limit:     catalog.MaxSegmentPageLimit,
		})
		if err != nil {
			return ConsumeResult{}, err
		}
		if len(page.Segments) == 0 {
			return ConsumeResult{}, fmt.Errorf("%w: no segment for partition=%d lsn=%d head_next=%d", ErrCorruptData, req.Partition, from, head.NextLSN)
		}

		advanced := false
		for _, segment := range page.Segments {
			if from >= head.NextLSN {
				break
			}
			if segment.Partition != req.Partition {
				return ConsumeResult{}, fmt.Errorf("%w: segment partition=%d request partition=%d", ErrCorruptData, segment.Partition, req.Partition)
			}
			if segment.LastLSN < from {
				continue
			}
			if segment.BaseLSN > from {
				return ConsumeResult{}, fmt.Errorf("%w: gap before segment base_lsn=%d next_lsn=%d", ErrCorruptData, segment.BaseLSN, from)
			}
			if segment.MaxTimestampMS < req.TimestampMS {
				from = segment.NextLSN()
				advanced = true
				continue
			}

			startLSN, ok, err := r.findTimestampStart(ctx, segment, req.TimestampMS)
			if err != nil {
				return ConsumeResult{}, err
			}
			if !ok {
				return ConsumeResult{}, fmt.Errorf("%w: segment uri=%s max_timestamp_ms=%d but no record >= %d", ErrCorruptData, segment.URI, segment.MaxTimestampMS, req.TimestampMS)
			}
			if startLSN >= head.NextLSN {
				return result, nil
			}
			return r.consumeFromHead(ctx, head, req.Partition, startLSN, limit)
		}
		if page.HasMore {
			if page.NextLSN <= from {
				return ConsumeResult{}, fmt.Errorf("%w: non-advancing page next_lsn=%d from_lsn=%d", ErrCorruptData, page.NextLSN, from)
			}
			from = page.NextLSN
			advanced = true
		}
		if !advanced {
			break
		}
	}
	return result, nil
}

func (r *Reader) consumeFromHead(ctx context.Context, head pmeta.PartitionHead, partition uint32, startLSN uint64, limit int) (ConsumeResult, error) {
	result := ConsumeResult{
		Head:    head,
		NextLSN: startLSN,
	}
	if startLSN < head.OldestLSN {
		return ConsumeResult{}, LSNExpiredError{
			Requested: startLSN,
			Oldest:    head.OldestLSN,
			HeadNext:  head.NextLSN,
		}
	}
	if !head.HasLastSegment {
		if startLSN < head.NextLSN {
			result.NextLSN = head.NextLSN
		}
		return result, nil
	}
	if startLSN >= head.NextLSN {
		return result, nil
	}

	next := startLSN
	for next < head.NextLSN && len(result.Records) < limit {
		page, err := r.catalog.ListSegments(ctx, catalog.ListSegmentsRequest{
			Partition: partition,
			FromLSN:   next,
			Limit:     catalog.MaxSegmentPageLimit,
		})
		if err != nil {
			return ConsumeResult{}, err
		}
		if len(page.Segments) == 0 {
			return ConsumeResult{}, fmt.Errorf("%w: no segment for partition=%d lsn=%d head_next=%d", ErrCorruptData, partition, next, head.NextLSN)
		}

		advanced := false
		for _, segment := range page.Segments {
			if len(result.Records) >= limit || next >= head.NextLSN {
				break
			}
			if segment.Partition != partition {
				return ConsumeResult{}, fmt.Errorf("%w: segment partition=%d request partition=%d", ErrCorruptData, segment.Partition, partition)
			}
			if segment.LastLSN < next {
				continue
			}
			if segment.BaseLSN > next {
				return ConsumeResult{}, fmt.Errorf("%w: gap before segment base_lsn=%d next_lsn=%d", ErrCorruptData, segment.BaseLSN, next)
			}

			remaining := limit - len(result.Records)
			records, err := r.readSegment(ctx, segment, next, remaining, head.NextLSN)
			if err != nil {
				return ConsumeResult{}, err
			}
			if len(records) == 0 {
				return ConsumeResult{}, fmt.Errorf("%w: no records in segment uri=%s from_lsn=%d", ErrCorruptData, segment.URI, next)
			}
			result.Records = append(result.Records, records...)
			next = result.Records[len(result.Records)-1].LSN + 1
			result.NextLSN = next
			advanced = true
		}
		if !advanced {
			return ConsumeResult{}, fmt.Errorf("%w: reader made no progress at lsn=%d", ErrCorruptData, next)
		}
	}
	return result, nil
}

func (r *Reader) findTimestampStart(ctx context.Context, segment pmeta.SegmentRef, timestampMS int64) (uint64, bool, error) {
	sr, err := r.openSegment(ctx, segment)
	if err != nil {
		return 0, false, mapSegmentError(err)
	}
	startLSN, ok, err := sr.FindLSNByTimestamp(ctx, timestampMS)
	if err != nil {
		return 0, false, mapSegmentError(err)
	}
	return startLSN, ok, nil
}

func (r *Reader) readSegment(ctx context.Context, segment pmeta.SegmentRef, fromLSN uint64, limit int, headNextLSN uint64) ([]Record, error) {
	start := time.Now()
	var out []Record
	var err error
	defer func() {
		r.observe(MetricEvent{
			Name:       MetricSegmentRead,
			Partition:  segment.Partition,
			StartLSN:   fromLSN,
			Limit:      limit,
			Records:    len(out),
			SegmentURI: segment.URI,
			Duration:   time.Since(start),
			Err:        err,
		})
	}()
	sr, err := r.openSegment(ctx, segment)
	if err != nil {
		err = mapSegmentError(err)
		return nil, err
	}
	segmentRecords, err := sr.Read(ctx, fromLSN, limit)
	if err != nil {
		err = mapSegmentError(err)
		return nil, err
	}
	out = make([]Record, 0, len(segmentRecords))
	for _, record := range segmentRecords {
		if record.LSN >= headNextLSN {
			break
		}
		out = append(out, Record{
			Partition:   record.Partition,
			LSN:         record.LSN,
			TimestampMS: record.TimestampMS,
			Headers:     segformat.CloneHeaders(record.Headers),
			Value:       append([]byte(nil), record.Value...),
		})
	}
	return out, nil
}

func (r *Reader) openSegment(ctx context.Context, segment pmeta.SegmentRef) (*segreader.Reader, error) {
	if r.opts.SegmentCache != nil {
		return r.opts.SegmentCache.Open(ctx, r.store, segment, r.opts.SegmentOptions)
	}
	return segreader.Open(ctx, r.store, segment, r.opts.SegmentOptions)
}

func normalizeOptions(opts Options) (Options, error) {
	if opts.MaxRecordsPerBatch == 0 {
		opts.MaxRecordsPerBatch = DefaultMaxRecordsPerBatch
	}
	if opts.MaxRecordsPerBatch < 0 {
		return Options{}, fmt.Errorf("%w: max_records_per_batch=%d", ErrInvalidOptions, opts.MaxRecordsPerBatch)
	}
	if opts.SegmentOptions == (segreader.Options{}) {
		opts.SegmentOptions = segreader.DefaultOptions()
	}
	return opts, nil
}

func (r *Reader) normalizedLimit(limit int) int {
	switch {
	case limit <= 0:
		return r.opts.MaxRecordsPerBatch
	case limit > r.opts.MaxRecordsPerBatch:
		return r.opts.MaxRecordsPerBatch
	default:
		return limit
	}
}

func (r *Reader) observe(event MetricEvent) {
	if r.opts.Observer == nil {
		return
	}
	r.opts.Observer.Observe(event)
}

func mapSegmentError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, segreader.ErrStoreRead) {
		return fmt.Errorf("%w: %w", ErrStoreRead, err)
	}
	if errors.Is(err, segreader.ErrCorruptData) || errors.Is(err, segreader.ErrInvalidSegment) {
		return fmt.Errorf("%w: %w", ErrCorruptData, err)
	}
	if errors.Is(err, segreader.ErrInvalidOptions) {
		return fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	return err
}
