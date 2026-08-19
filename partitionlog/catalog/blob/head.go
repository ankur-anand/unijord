package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.WriterManager = (*Catalog)(nil)
var _ csession.WriterSession = (*writerSession)(nil)

func (c *Catalog) InitializePartition(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if nextLSN == math.MaxUint64 {
		return pmeta.PartitionHead{}, false, fmt.Errorf("%w: next_lsn exhausted", csession.ErrInvalidRequest)
	}

	head := headFile{
		Version:   pageVersion,
		StreamID:  c.opts.StreamID,
		Partition: partition,
		NextLSN:   nextLSN,
		OldestLSN: nextLSN,
	}
	body, err := marshalHead(head, c.opts.StreamID, partition)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}

	obj, swapped, err := c.backend.CompareAndSwap(ctx, HeadPath(c.opts.Prefix, c.opts.StreamID, partition), "", body)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if swapped {
		return stateFromHead(head), true, nil
	}

	current, err := decodeHead(obj.Body, c.opts.StreamID, partition)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	return stateFromHead(current), false, nil
}

func (c *Catalog) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (csession.WriterSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if writerID == ([16]byte{}) {
		return nil, fmt.Errorf("%w: empty writer_id", csession.ErrInvalidRequest)
	}

	path := HeadPath(c.opts.Prefix, c.opts.StreamID, partition)
	head, token, err := c.loadHead(ctx, partition)
	if err != nil {
		return nil, err
	}
	backoff := c.opts.WriterAcquireInitialBackoff
	var candidateBase headFile
	var candidate headFile
	var candidateBody []byte
	var candidateReady bool
	var lastCASErr error

	for attempt := 0; attempt < c.opts.WriterAcquireMaxAttempts; attempt++ {
		if !candidateReady {
			candidateBase = head
			candidate, err = nextWriterHead(head, c.opts.StreamID, partition, writerID)
			if err != nil {
				return nil, err
			}
			candidateBody, err = marshalHead(candidate, c.opts.StreamID, partition)
			if err != nil {
				return nil, err
			}
			candidateReady = true
			lastCASErr = nil
		}

		obj, swapped, err := c.backend.CompareAndSwap(ctx, path, token, candidateBody)
		if err != nil {
			lastCASErr = err
		} else if swapped {
			return c.newWriterSession(candidate, obj.Token), nil
		} else {
			current, err := decodeHead(obj.Body, c.opts.StreamID, partition)
			if err != nil {
				return nil, err
			}
			if sameHeadState(current, candidate) {
				return c.newWriterSession(candidate, obj.Token), nil
			}
			token = obj.Token
			if !sameHeadState(current, candidateBase) {
				head = current
				candidateReady = false
				lastCASErr = nil
			}
			if attempt+1 == c.opts.WriterAcquireMaxAttempts {
				if lastCASErr != nil {
					return nil, fmt.Errorf("acquire writer fence partition=%d: %w", partition, lastCASErr)
				}
				return nil, fmt.Errorf("%w: open writer contention partition=%d", csession.ErrConflict, partition)
			}
		}
		if attempt+1 == c.opts.WriterAcquireMaxAttempts {
			break
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			if lastCASErr != nil {
				return nil, indeterminateFence(partition, errors.Join(lastCASErr, err))
			}
			return nil, err
		}
		backoff = growBackoff(backoff, c.opts.WriterAcquireMaxBackoff)
	}

	current, currentToken, err := c.loadHead(ctx, partition)
	if err != nil {
		return nil, indeterminateFence(partition, errors.Join(lastCASErr, err))
	}
	if candidateReady && sameHeadState(current, candidate) {
		return c.newWriterSession(candidate, currentToken), nil
	}
	if candidateReady && sameHeadState(current, candidateBase) && lastCASErr != nil {
		return nil, fmt.Errorf("acquire writer fence partition=%d: %w", partition, lastCASErr)
	}
	return nil, fmt.Errorf("%w: open writer contention partition=%d", csession.ErrConflict, partition)
}

func nextWriterHead(head headFile, streamID string, partition uint32, writerID [16]byte) (headFile, error) {
	if head.WriterEpoch == math.MaxUint64 {
		return headFile{}, fmt.Errorf("%w: writer_epoch partition=%d", csession.ErrFenceExhausted, partition)
	}
	generation, err := nextGeneration(head.Generation, partition)
	if err != nil {
		return headFile{}, err
	}
	next := head
	next.Version = pageVersion
	next.StreamID = streamID
	next.Partition = partition
	next.WriterEpoch++
	next.WriterID = writerID
	next.Generation = generation
	return next, nil
}

func nextGeneration(current uint64, partition uint32) (uint64, error) {
	if current == math.MaxUint64 {
		return 0, fmt.Errorf("%w: partition=%d", csession.ErrGenerationExhausted, partition)
	}
	return current + 1, nil
}

func (c *Catalog) newWriterSession(head headFile, token string) *writerSession {
	return &writerSession{
		cat:         c,
		writerEpoch: head.WriterEpoch,
		writerID:    head.WriterID,
		head:        head,
		token:       token,
	}
}

func indeterminateFence(partition uint32, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: partition=%d", csession.ErrFenceIndeterminate, partition)
	}
	return fmt.Errorf("%w: partition=%d: %w", csession.ErrFenceIndeterminate, partition, cause)
}

func (s *writerSession) Head() pmeta.PartitionHead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return stateFromHead(s.head)
}

func (s *writerSession) Epoch() uint64 {
	return s.writerEpoch
}

func (s *writerSession) WriterID() [16]byte {
	return s.writerID
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
	if segment.WriterTag != s.writerID {
		return pmeta.PartitionHead{}, fmt.Errorf("%w: segment writer_tag does not match writer_id", csession.ErrInvalidRequest)
	}
	if _, ok := idempotentHeadRetry(head, segment); ok {
		current, token, err := s.cat.loadHead(ctx, head.Partition)
		if err != nil {
			return pmeta.PartitionHead{}, err
		}
		observation, err := s.observeSegmentCommit(ctx, head, current, segment)
		if err != nil {
			return pmeta.PartitionHead{}, err
		}
		if observation == commitApplied {
			return s.acceptObservedCommit(head, current, token), nil
		}
		return pmeta.PartitionHead{}, fmt.Errorf("%w: idempotent retry no longer matches head partition=%d", csession.ErrConflict, head.Partition)
	}
	if err := validateAppend(head, segment); err != nil {
		return pmeta.PartitionHead{}, err
	}

	generation, err := nextGeneration(head.Generation, head.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
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
	if level := highestIndexLevel(pages.IndexFrontier); level > next.MaxIndexLevel {
		next.MaxIndexLevel = level
	}
	next.Generation = generation

	body, err := marshalHead(next, s.cat.opts.StreamID, head.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	return s.commitSegmentHead(ctx, head, next, segment, body)
}

type commitObservation uint8

const (
	commitNeedsRetry commitObservation = iota
	commitApplied
)

func (s *writerSession) commitSegmentHead(ctx context.Context, previous, next headFile, segment pmeta.SegmentRef, body []byte) (pmeta.PartitionHead, error) {
	path := HeadPath(s.cat.opts.Prefix, s.cat.opts.StreamID, previous.Partition)
	expectedToken := s.token
	backoff := s.cat.opts.WriterCommitInitialBackoff
	var lastCASErr error

	for attempt := 0; attempt < s.cat.opts.WriterCommitMaxAttempts; attempt++ {
		obj, swapped, err := s.cat.backend.CompareAndSwap(ctx, path, expectedToken, body)
		if err != nil {
			lastCASErr = err
		} else if swapped {
			s.head = next
			s.token = obj.Token
			return stateFromHead(next), nil
		} else {
			current, err := decodeHead(obj.Body, s.cat.opts.StreamID, previous.Partition)
			if err != nil {
				return pmeta.PartitionHead{}, err
			}
			observation, err := s.observeSegmentCommit(ctx, previous, current, segment)
			if err != nil {
				return pmeta.PartitionHead{}, err
			}
			if observation == commitApplied {
				return s.acceptObservedCommit(next, current, obj.Token), nil
			}
			expectedToken = obj.Token
			if attempt+1 == s.cat.opts.WriterCommitMaxAttempts {
				if lastCASErr != nil {
					return pmeta.PartitionHead{}, fmt.Errorf("commit head partition=%d: %w", previous.Partition, lastCASErr)
				}
				return pmeta.PartitionHead{}, fmt.Errorf("%w: head CAS did not apply partition=%d", csession.ErrConflict, previous.Partition)
			}
		}

		if attempt+1 == s.cat.opts.WriterCommitMaxAttempts {
			break
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			if lastCASErr != nil {
				return pmeta.PartitionHead{}, indeterminateCommit(previous.Partition, errors.Join(lastCASErr, err))
			}
			return pmeta.PartitionHead{}, err
		}
		backoff = growBackoff(backoff, s.cat.opts.WriterCommitMaxBackoff)
	}

	current, token, err := s.cat.loadHead(ctx, previous.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, indeterminateCommit(previous.Partition, errors.Join(lastCASErr, err))
	}
	observation, err := s.observeSegmentCommit(ctx, previous, current, segment)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	if observation == commitApplied {
		return s.acceptObservedCommit(next, current, token), nil
	}
	if lastCASErr != nil {
		return pmeta.PartitionHead{}, fmt.Errorf("commit head partition=%d: %w", previous.Partition, lastCASErr)
	}
	return pmeta.PartitionHead{}, fmt.Errorf("%w: head CAS did not apply partition=%d", csession.ErrConflict, previous.Partition)
}

func (s *writerSession) observeSegmentCommit(ctx context.Context, previous, current headFile, segment pmeta.SegmentRef) (commitObservation, error) {
	if current.HasLastSegment && current.LastSegment == segment {
		return commitApplied, nil
	}
	if sameHeadState(current, previous) {
		return commitNeedsRetry, nil
	}
	if current.NextLSN >= segment.NextLSN() {
		if segment.BaseLSN < current.OldestLSN {
			return commitNeedsRetry, indeterminateCommit(previous.Partition, fmt.Errorf("segment base_lsn=%d is below retained oldest_lsn=%d", segment.BaseLSN, current.OldestLSN))
		}
		committed, ok, err := s.cat.findSegmentInHead(ctx, current, segment.BaseLSN)
		if err != nil {
			return commitNeedsRetry, indeterminateCommit(previous.Partition, err)
		}
		if !ok {
			return commitNeedsRetry, fmt.Errorf("%w: committed range does not contain base_lsn=%d", ErrCorruptCatalog, segment.BaseLSN)
		}
		if committed != segment {
			if current.WriterEpoch != previous.WriterEpoch || current.WriterID != previous.WriterID {
				return commitNeedsRetry, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, previous.Partition)
			}
			return commitNeedsRetry, fmt.Errorf("%w: base_lsn=%d belongs to a different segment", csession.ErrConflict, segment.BaseLSN)
		}
		return commitApplied, nil
	}
	if current.WriterEpoch != previous.WriterEpoch || current.WriterID != previous.WriterID {
		return commitNeedsRetry, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, previous.Partition)
	}
	return commitNeedsRetry, fmt.Errorf("%w: head changed partition=%d", csession.ErrConflict, previous.Partition)
}

func (s *writerSession) acceptObservedCommit(expected, observed headFile, token string) pmeta.PartitionHead {
	s.head = expected
	if sameHeadState(observed, expected) {
		s.token = token
	}
	return stateFromHead(expected)
}

func sameHeadState(a, b headFile) bool {
	if a.Version != b.Version ||
		a.StreamID != b.StreamID ||
		a.Partition != b.Partition ||
		a.NextLSN != b.NextLSN ||
		a.OldestLSN != b.OldestLSN ||
		a.AppliedRetentionLSN != b.AppliedRetentionLSN ||
		a.AppliedRetentionVersion != b.AppliedRetentionVersion ||
		a.MaxIndexLevel != b.MaxIndexLevel ||
		a.WriterEpoch != b.WriterEpoch ||
		a.WriterID != b.WriterID ||
		a.SegmentCount != b.SegmentCount ||
		a.LastSegment != b.LastSegment ||
		a.HasLastSegment != b.HasLastSegment ||
		a.Generation != b.Generation ||
		len(a.IndexFrontier) != len(b.IndexFrontier) ||
		len(a.ActiveSegments) != len(b.ActiveSegments) {
		return false
	}
	if (a.LeafFrontier == nil) != (b.LeafFrontier == nil) {
		return false
	}
	if a.LeafFrontier != nil && *a.LeafFrontier != *b.LeafFrontier {
		return false
	}
	for i := range a.IndexFrontier {
		if a.IndexFrontier[i] != b.IndexFrontier[i] {
			return false
		}
	}
	for i := range a.ActiveSegments {
		if a.ActiveSegments[i] != b.ActiveSegments[i] {
			return false
		}
	}
	return true
}

func indeterminateCommit(partition uint32, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: partition=%d", csession.ErrCommitIndeterminate, partition)
	}
	return fmt.Errorf("%w: partition=%d: %w", csession.ErrCommitIndeterminate, partition, cause)
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum {
		return maximum
	}
	if current > maximum-current {
		return maximum
	}
	next := current * 2
	if next > maximum {
		return maximum
	}
	return next
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
	if segment.WriterTag != head.WriterID {
		return fmt.Errorf("%w: segment writer_tag does not match writer_id", csession.ErrInvalidRequest)
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
		StreamID:                head.StreamID,
		Partition:               head.Partition,
		NextLSN:                 head.NextLSN,
		OldestLSN:               head.OldestLSN,
		AppliedRetentionLSN:     head.AppliedRetentionLSN,
		AppliedRetentionVersion: head.AppliedRetentionVersion,
		WriterEpoch:             head.WriterEpoch,
		SegmentCount:            head.SegmentCount,
		LastSegment:             head.LastSegment,
		HasLastSegment:          head.HasLastSegment,
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
