package blob

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

var _ csession.RetentionManager = (*Catalog)(nil)
var _ csession.RetentionWriterSession = (*writerSession)(nil)

type retentionFile struct {
	Version       uint16 `json:"version"`
	StreamID      string `json:"stream_id,omitempty"`
	Partition     uint32 `json:"partition"`
	PolicyVersion uint64 `json:"policy_version"`
	BeforeLSN     uint64 `json:"before_lsn"`
	CreatedUnixMS int64  `json:"created_unix_ms"`
}

func (c *Catalog) RequestRetention(ctx context.Context, partition uint32, request csession.RetentionRequest) (csession.RetentionRequest, error) {
	if err := ctx.Err(); err != nil {
		return csession.RetentionRequest{}, err
	}
	if err := request.Validate(); err != nil {
		return csession.RetentionRequest{}, err
	}

	path := RetentionRequestPath(c.opts.Prefix, c.opts.StreamID, partition)
	current, token, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil {
		return csession.RetentionRequest{}, err
	}
	if found {
		if existing, done, err := compareRetentionRequest(current.request(), request); done || err != nil {
			return existing, err
		}
	}

	candidate := retentionFile{
		Version:       request.Version,
		StreamID:      c.opts.StreamID,
		Partition:     partition,
		PolicyVersion: request.PolicyVersion,
		BeforeLSN:     request.BeforeLSN,
		CreatedUnixMS: request.CreatedUnixMS,
	}
	body, err := json.Marshal(candidate)
	if err != nil {
		return csession.RetentionRequest{}, err
	}

	backoff := c.opts.WriterCommitInitialBackoff
	var lastCASErr error
	for attempt := 0; attempt < c.opts.WriterCommitMaxAttempts; attempt++ {
		obj, swapped, casErr := c.backend.CompareAndSwap(ctx, path, token, body)
		if casErr != nil {
			lastCASErr = casErr
		} else if swapped {
			return request, nil
		} else {
			observed, err := decodeRetentionFile(obj.Body, c.opts.StreamID, partition)
			if err != nil {
				return csession.RetentionRequest{}, err
			}
			if existing, done, err := compareRetentionRequest(observed.request(), request); done || err != nil {
				return existing, err
			}
			token = obj.Token
			lastCASErr = nil
		}

		if attempt+1 == c.opts.WriterCommitMaxAttempts {
			break
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			return csession.RetentionRequest{}, errors.Join(lastCASErr, err)
		}
		backoff = growBackoff(backoff, c.opts.WriterCommitMaxBackoff)
	}

	observed, _, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil {
		return csession.RetentionRequest{}, errors.Join(lastCASErr, err)
	}
	if found {
		if existing, done, err := compareRetentionRequest(observed.request(), request); done || err != nil {
			return existing, err
		}
	}
	if lastCASErr != nil {
		return csession.RetentionRequest{}, fmt.Errorf("request retention partition=%d: %w", partition, lastCASErr)
	}
	return csession.RetentionRequest{}, fmt.Errorf("%w: retention request CAS did not apply partition=%d", csession.ErrConflict, partition)
}

func (c *Catalog) LoadRetentionRequest(ctx context.Context, partition uint32) (csession.RetentionRequest, bool, error) {
	if err := ctx.Err(); err != nil {
		return csession.RetentionRequest{}, false, err
	}
	file, _, found, err := c.loadRetentionFile(ctx, partition)
	if err != nil || !found {
		return csession.RetentionRequest{}, found, err
	}
	return file.request(), true, nil
}

func (c *Catalog) loadRetentionFile(ctx context.Context, partition uint32) (retentionFile, string, bool, error) {
	obj, err := c.backend.Get(ctx, RetentionRequestPath(c.opts.Prefix, c.opts.StreamID, partition))
	if errors.Is(err, ErrObjectNotFound) {
		return retentionFile{}, "", false, nil
	}
	if err != nil {
		return retentionFile{}, "", false, err
	}
	file, err := decodeRetentionFile(obj.Body, c.opts.StreamID, partition)
	if err != nil {
		return retentionFile{}, "", false, err
	}
	return file, obj.Token, true, nil
}

func decodeRetentionFile(body []byte, streamID string, partition uint32) (retentionFile, error) {
	var file retentionFile
	if err := json.Unmarshal(body, &file); err != nil {
		return retentionFile{}, fmt.Errorf("%w: decode retention partition=%d: %v", ErrCorruptCatalog, partition, err)
	}
	if file.StreamID != streamID {
		return retentionFile{}, fmt.Errorf("%w: retention stream_id=%q want=%q", ErrCorruptCatalog, file.StreamID, streamID)
	}
	if file.Partition != partition {
		return retentionFile{}, fmt.Errorf("%w: retention partition=%d want=%d", ErrCorruptCatalog, file.Partition, partition)
	}
	if err := file.request().Validate(); err != nil {
		return retentionFile{}, fmt.Errorf("%w: %v", ErrCorruptCatalog, err)
	}
	return file, nil
}

func (f retentionFile) request() csession.RetentionRequest {
	return csession.RetentionRequest{
		Version:       f.Version,
		PolicyVersion: f.PolicyVersion,
		BeforeLSN:     f.BeforeLSN,
		CreatedUnixMS: f.CreatedUnixMS,
	}
}

func compareRetentionRequest(current, requested csession.RetentionRequest) (csession.RetentionRequest, bool, error) {
	switch {
	case current.PolicyVersion == requested.PolicyVersion && current.BeforeLSN == requested.BeforeLSN:
		return current, true, nil
	case requested.PolicyVersion <= current.PolicyVersion:
		return csession.RetentionRequest{}, true, fmt.Errorf("%w: policy_version=%d current=%d", csession.ErrRetentionRegression, requested.PolicyVersion, current.PolicyVersion)
	case requested.BeforeLSN < current.BeforeLSN:
		return csession.RetentionRequest{}, true, fmt.Errorf("%w: before_lsn=%d current=%d", csession.ErrRetentionRegression, requested.BeforeLSN, current.BeforeLSN)
	default:
		return csession.RetentionRequest{}, false, nil
	}
}

func (s *writerSession) ApplyPendingRetention(ctx context.Context) (csession.RetentionApplyResult, error) {
	if err := ctx.Err(); err != nil {
		return csession.RetentionApplyResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, token, err := s.cat.loadHead(ctx, s.head.Partition)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	if current.WriterEpoch != s.writerEpoch || current.WriterID != s.writerID {
		return csession.RetentionApplyResult{}, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, s.head.Partition)
	}
	s.head = current
	s.token = token

	request, found, err := s.cat.LoadRetentionRequest(ctx, s.head.Partition)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	if !found || request.PolicyVersion <= s.head.AppliedRetentionVersion {
		return csession.RetentionApplyResult{Head: stateFromHead(s.head), Request: request}, nil
	}
	if request.BeforeLSN < s.head.AppliedRetentionLSN {
		return csession.RetentionApplyResult{}, fmt.Errorf("%w: before_lsn=%d applied=%d", csession.ErrRetentionRegression, request.BeforeLSN, s.head.AppliedRetentionLSN)
	}
	generation, err := nextGeneration(s.head.Generation, s.head.Partition)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	target := request.BeforeLSN
	if target > s.head.NextLSN {
		target = s.head.NextLSN
	}
	pages, effectiveOldest, err := s.cat.buildRetentionPageSet(ctx, s.head, target, generation)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}

	previous := s.head
	next := previous
	next.OldestLSN = effectiveOldest
	next.AppliedRetentionLSN = target
	next.AppliedRetentionVersion = request.PolicyVersion
	next.IndexFrontier = pages.IndexFrontier
	next.LeafFrontier = pages.LeafFrontier
	next.ActiveSegments = pages.ActiveSegments
	next.Generation = generation
	body, err := marshalHead(next, s.cat.opts.StreamID, next.Partition)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}

	state, err := s.commitRetentionHead(ctx, previous, next, request, body)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	return csession.RetentionApplyResult{Head: state, Request: request, Applied: true}, nil
}

func (s *writerSession) commitRetentionHead(ctx context.Context, previous, next headFile, request csession.RetentionRequest, body []byte) (pmeta.PartitionHead, error) {
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
			if retentionApplied(current, request, next.AppliedRetentionLSN) {
				return s.acceptObservedRetention(next, current, obj.Token), nil
			}
			if current.WriterEpoch != previous.WriterEpoch || current.WriterID != previous.WriterID {
				return pmeta.PartitionHead{}, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, previous.Partition)
			}
			if !sameHeadState(current, previous) {
				return pmeta.PartitionHead{}, fmt.Errorf("%w: head changed while applying retention partition=%d", csession.ErrConflict, previous.Partition)
			}
			expectedToken = obj.Token
			lastCASErr = nil
		}

		if attempt+1 == s.cat.opts.WriterCommitMaxAttempts {
			break
		}
		if err := sleepBackoff(ctx, backoff); err != nil {
			return pmeta.PartitionHead{}, errors.Join(lastCASErr, err)
		}
		backoff = growBackoff(backoff, s.cat.opts.WriterCommitMaxBackoff)
	}

	current, token, err := s.cat.loadHead(ctx, previous.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, errors.Join(lastCASErr, err)
	}
	if retentionApplied(current, request, next.AppliedRetentionLSN) {
		return s.acceptObservedRetention(next, current, token), nil
	}
	if current.WriterEpoch != previous.WriterEpoch || current.WriterID != previous.WriterID {
		return pmeta.PartitionHead{}, fmt.Errorf("%w: writer fence moved partition=%d", csession.ErrStaleWriter, previous.Partition)
	}
	if lastCASErr != nil {
		return pmeta.PartitionHead{}, fmt.Errorf("apply retention partition=%d: %w", previous.Partition, lastCASErr)
	}
	return pmeta.PartitionHead{}, fmt.Errorf("%w: retention head CAS did not apply partition=%d", csession.ErrConflict, previous.Partition)
}

func retentionApplied(head headFile, request csession.RetentionRequest, target uint64) bool {
	return head.AppliedRetentionVersion == request.PolicyVersion && head.AppliedRetentionLSN == target
}

func (s *writerSession) acceptObservedRetention(expected, observed headFile, token string) pmeta.PartitionHead {
	s.head = expected
	if sameHeadState(observed, expected) {
		s.token = token
	}
	return stateFromHead(expected)
}

func (c *Catalog) buildRetentionPageSet(ctx context.Context, head headFile, target, generation uint64) (nextPageSet, uint64, error) {
	next := nextPageSet{
		IndexFrontier:  cloneRefs(head.IndexFrontier),
		LeafFrontier:   clonePageRefPtr(head.LeafFrontier),
		ActiveSegments: slices.Clone(head.ActiveSegments),
	}
	if target <= head.OldestLSN {
		return next, head.OldestLSN, nil
	}
	if target >= head.NextLSN {
		return nextPageSet{}, head.NextLSN, nil
	}

	segment, found, err := c.findSegmentInHead(ctx, head, target)
	if err != nil {
		return nextPageSet{}, 0, err
	}
	if !found {
		return nextPageSet{}, 0, fmt.Errorf("%w: retention target=%d is not in committed history", ErrCorruptCatalog, target)
	}
	effective := segment.BaseLSN
	if effective <= head.OldestLSN {
		return next, head.OldestLSN, nil
	}

	for i, ref := range next.IndexFrontier {
		switch {
		case ref.Path == "":
		case ref.SeqHi < effective:
			next.IndexFrontier[i] = pageRef{}
		case ref.SeqLo < effective:
			trimmed, err := c.trimPageRef(ctx, ref, head.StreamID, head.Partition, effective, generation)
			if err != nil {
				return nextPageSet{}, 0, err
			}
			next.IndexFrontier[i] = trimmed
		}
	}
	next.IndexFrontier = trimFrontier(next.IndexFrontier)

	if next.LeafFrontier != nil {
		switch {
		case next.LeafFrontier.SeqHi < effective:
			next.LeafFrontier = nil
		case next.LeafFrontier.SeqLo < effective:
			trimmed, err := c.trimPageRef(ctx, *next.LeafFrontier, head.StreamID, head.Partition, effective, generation)
			if err != nil {
				return nextPageSet{}, 0, err
			}
			next.LeafFrontier = &trimmed
		}
	}

	start := firstSegmentAtOrAfter(next.ActiveSegments, effective)
	if start == len(next.ActiveSegments) {
		next.ActiveSegments = nil
	} else {
		next.ActiveSegments = slices.Clone(next.ActiveSegments[start:])
	}
	if err := validateRetentionBoundary(next, effective); err != nil {
		return nextPageSet{}, 0, err
	}
	return next, effective, nil
}

func (c *Catalog) trimPageRef(ctx context.Context, ref pageRef, streamID string, partition uint32, effective, generation uint64) (pageRef, error) {
	if effective <= ref.SeqLo {
		return ref, nil
	}
	if effective > ref.SeqHi {
		return pageRef{}, fmt.Errorf("%w: retention boundary=%d beyond page=%d-%d", ErrCorruptCatalog, effective, ref.SeqLo, ref.SeqHi)
	}
	if ref.Level == 0 {
		page, err := c.loadLeaf(ctx, ref, streamID, partition)
		if err != nil {
			return pageRef{}, err
		}
		start := firstSegmentAtOrAfter(page.Segments, effective)
		if start == len(page.Segments) || page.Segments[start].BaseLSN != effective {
			return pageRef{}, fmt.Errorf("%w: leaf does not start retained history at lsn=%d", ErrCorruptCatalog, effective)
		}
		page.Generation = generation
		page.Segments = slices.Clone(page.Segments[start:])
		trimmed, _, err := c.writeLeaf(ctx, page)
		if err != nil {
			return pageRef{}, err
		}
		return *trimmed, nil
	}

	page, err := c.loadIndex(ctx, ref, streamID, partition)
	if err != nil {
		return pageRef{}, err
	}
	start := firstPageRefAtOrAfter(page.Refs, effective)
	if start == len(page.Refs) {
		return pageRef{}, fmt.Errorf("%w: index does not contain retained lsn=%d", ErrCorruptCatalog, effective)
	}
	refs := slices.Clone(page.Refs[start:])
	if refs[0].SeqLo < effective {
		refs[0], err = c.trimPageRef(ctx, refs[0], streamID, partition, effective, generation)
		if err != nil {
			return pageRef{}, err
		}
	}
	if refs[0].SeqLo != effective {
		return pageRef{}, fmt.Errorf("%w: index retained seq_lo=%d want=%d", ErrCorruptCatalog, refs[0].SeqLo, effective)
	}
	page.Generation = generation
	page.Refs = refs
	trimmed, err := c.writeIndex(ctx, page)
	if err != nil {
		return pageRef{}, err
	}
	return *trimmed, nil
}

func validateRetentionBoundary(pages nextPageSet, effective uint64) error {
	probe := headFile{
		IndexFrontier: pages.IndexFrontier,
		LeafFrontier:  pages.LeafFrontier,
	}
	roots := reachableRoots(probe)
	if len(roots) > 0 {
		if roots[0].SeqLo != effective {
			return fmt.Errorf("%w: retained root starts at %d want=%d", ErrCorruptCatalog, roots[0].SeqLo, effective)
		}
		return nil
	}
	if len(pages.ActiveSegments) == 0 || pages.ActiveSegments[0].BaseLSN != effective {
		return fmt.Errorf("%w: retained active history does not start at lsn=%d", ErrCorruptCatalog, effective)
	}
	return nil
}
