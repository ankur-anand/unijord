package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.WriterManager = (*Catalog)(nil)
var _ csession.WriterSession = (*writerSession)(nil)

func (c *Catalog) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (csession.WriterSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if writerID == ([16]byte{}) {
		return nil, fmt.Errorf("%w: empty writer_id", csession.ErrInvalidRequest)
	}

	path := HeadPath(c.opts.Prefix, c.opts.StreamID, partition)
	backoff := c.opts.WriterAcquireInitialBackoff
	for attempt := 0; attempt < c.opts.WriterAcquireMaxAttempts; attempt++ {
		head, token, err := c.loadHead(ctx, partition)
		if err != nil {
			return nil, err
		}

		next := head
		next.Version = pageVersion
		next.StreamID = c.opts.StreamID
		next.Partition = partition
		next.WriterEpoch++
		next.WriterID = writerID
		next.Generation++
		body, err := marshalHead(next, c.opts.StreamID, partition)
		if err != nil {
			return nil, err
		}

		obj, swapped, err := c.backend.CompareAndSwap(ctx, path, token, body)
		if err != nil {
			return nil, err
		}
		if swapped {
			return &writerSession{
				cat:   c,
				head:  next,
				token: obj.Token,
			}, nil
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			return nil, err
		}
		if backoff < c.opts.WriterAcquireMaxBackoff {
			backoff *= 2
			if backoff > c.opts.WriterAcquireMaxBackoff {
				backoff = c.opts.WriterAcquireMaxBackoff
			}
		}
	}
	return nil, fmt.Errorf("%w: open writer contention partition=%d", csession.ErrConflict, partition)
}

func (s *writerSession) Head() pmeta.PartitionHead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return stateFromHead(s.head)
}

func (s *writerSession) Epoch() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head.WriterEpoch
}

func (s *writerSession) WriterID() [16]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.head.WriterID
}

func (s *writerSession) AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.appendSegmentLocked(ctx, segment)
}

func (s *writerSession) appendSegmentLocked(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	head := s.head
	if _, ok := idempotentHeadRetry(head, segment); ok {
		current, token, err := s.cat.loadHead(ctx, head.Partition)
		if err != nil {
			return pmeta.PartitionHead{}, err
		}
		if retry, ok := idempotentHeadRetry(current, segment); ok {
			s.head = current
			s.token = token
			return retry, nil
		}
		if current.WriterEpoch != head.WriterEpoch || current.WriterID != head.WriterID {
			return pmeta.PartitionHead{}, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, head.Partition)
		}
		return pmeta.PartitionHead{}, fmt.Errorf("%w: idempotent retry no longer matches head partition=%d", csession.ErrConflict, head.Partition)
	}
	if err := validateAppend(head, segment); err != nil {
		return pmeta.PartitionHead{}, err
	}

	generation := head.Generation + 1
	pages, err := s.cat.buildNextPageSet(ctx, head, segment, generation)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}

	next := head
	next.NextLSN = segment.NextLSN()
	if !next.HasLastSegment {
		next.OldestLSN = segment.BaseLSN
	}
	next.LastSegment = segment
	next.HasLastSegment = true
	next.SegmentCount++
	next.IndexFrontier = pages.IndexFrontier
	next.LeafFrontier = pages.LeafFrontier
	next.ActiveSegments = pages.ActiveSegments
	next.Generation = generation

	body, err := marshalHead(next, s.cat.opts.StreamID, head.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	obj, swapped, err := s.cat.backend.CompareAndSwap(ctx, HeadPath(s.cat.opts.Prefix, s.cat.opts.StreamID, head.Partition), s.token, body)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	if swapped {
		s.head = next
		s.token = obj.Token
		return stateFromHead(next), nil
	}

	current, decodeErr := decodeHead(obj.Body, s.cat.opts.StreamID, head.Partition)
	if decodeErr != nil {
		return pmeta.PartitionHead{}, decodeErr
	}
	if retry, ok := idempotentHeadRetry(current, segment); ok {
		s.head = current
		s.token = obj.Token
		return retry, nil
	}
	if current.WriterEpoch != head.WriterEpoch || current.WriterID != head.WriterID {
		return pmeta.PartitionHead{}, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, head.Partition)
	}
	return pmeta.PartitionHead{}, fmt.Errorf("%w: head changed partition=%d", csession.ErrConflict, head.Partition)
}

func (c *Catalog) loadHead(ctx context.Context, partition uint32) (headFile, string, error) {
	obj, err := c.backend.Get(ctx, HeadPath(c.opts.Prefix, c.opts.StreamID, partition))
	if errors.Is(err, ErrObjectNotFound) {
		return headFile{Version: pageVersion, StreamID: c.opts.StreamID, Partition: partition}, "", nil
	}
	if err != nil {
		return headFile{}, "", err
	}
	head, err := decodeHead(obj.Body, c.opts.StreamID, partition)
	if err != nil {
		return headFile{}, "", err
	}
	return head, obj.Token, nil
}

func decodeHead(body []byte, streamID string, partition uint32) (headFile, error) {
	var head headFile
	if err := json.Unmarshal(body, &head); err != nil {
		return headFile{}, fmt.Errorf("%w: decode head partition=%d: %v", ErrCorruptCatalog, partition, err)
	}
	if err := validateHeadFile(head, streamID, partition); err != nil {
		return headFile{}, err
	}
	return head, nil
}

func marshalHead(head headFile, streamID string, partition uint32) ([]byte, error) {
	if err := validateHeadFile(head, streamID, partition); err != nil {
		return nil, err
	}
	body, err := json.Marshal(head)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func validateAppend(head headFile, segment pmeta.SegmentRef) error {
	if head.WriterEpoch == 0 || head.WriterID == ([16]byte{}) {
		return fmt.Errorf("%w: writer fence not acquired", csession.ErrStaleWriter)
	}
	if segment.Partition != head.Partition {
		return fmt.Errorf("%w: head partition=%d segment partition=%d", csession.ErrInvalidRequest, head.Partition, segment.Partition)
	}
	if segment.StreamID != head.StreamID {
		return fmt.Errorf("%w: head stream_id=%q segment stream_id=%q", csession.ErrInvalidRequest, head.StreamID, segment.StreamID)
	}
	if segment.WriterEpoch != head.WriterEpoch {
		return fmt.Errorf("%w: head writer_epoch=%d segment writer_epoch=%d", csession.ErrStaleWriter, head.WriterEpoch, segment.WriterEpoch)
	}
	if segment.BaseLSN != head.NextLSN {
		return fmt.Errorf("%w: expected_next_lsn=%d segment base_lsn=%d", csession.ErrConflict, head.NextLSN, segment.BaseLSN)
	}
	if err := segment.Validate(); err != nil {
		return fmt.Errorf("%w: %w", csession.ErrInvalidSegment, err)
	}
	if last, ok := stateFromHead(head).Last(); ok && segment.MinTimestampMS < last.MaxTimestampMS {
		return fmt.Errorf("%w: segment min_ts=%d previous max_ts=%d", csession.ErrTimestampOrder, segment.MinTimestampMS, last.MaxTimestampMS)
	}
	return nil
}

func idempotentHeadRetry(head headFile, segment pmeta.SegmentRef) (pmeta.PartitionHead, bool) {
	if !head.HasLastSegment {
		return pmeta.PartitionHead{}, false
	}
	if head.LastSegment.BaseLSN != segment.BaseLSN {
		return pmeta.PartitionHead{}, false
	}
	if head.LastSegment != segment {
		return pmeta.PartitionHead{}, false
	}
	return stateFromHead(head), true
}

func stateFromHead(head headFile) pmeta.PartitionHead {
	return pmeta.PartitionHead{
		StreamID:       head.StreamID,
		Partition:      head.Partition,
		NextLSN:        head.NextLSN,
		OldestLSN:      head.OldestLSN,
		WriterEpoch:    head.WriterEpoch,
		SegmentCount:   head.SegmentCount,
		LastSegment:    head.LastSegment,
		HasLastSegment: head.HasLastSegment,
	}
}

func sleepBackoff(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
