package writeradapter

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

// Session adapts a fenced catalog.WriterSession into writer.Session.
type Session struct {
	mu       sync.Mutex
	inner    catalog.WriterSession
	snapshot writer.Snapshot
}

var _ writer.Session = (*Session)(nil)
var _ writer.RetentionSession = (*Session)(nil)

func New(inner catalog.WriterSession) (*Session, error) {
	if inner == nil {
		return nil, fmt.Errorf("catalog/writeradapter: nil writer session")
	}
	snapshot := writer.Snapshot{
		Head: inner.Head(),
		Identity: writer.WriterIdentity{
			Epoch: inner.Epoch(),
			Tag:   inner.WriterID(),
		},
	}
	return &Session{inner: inner, snapshot: snapshot}, nil
}

func (s *Session) Snapshot() writer.Snapshot {
	if s == nil {
		return writer.Snapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *Session) PublishSegment(ctx context.Context, req writer.PublishRequest) (writer.Snapshot, error) {
	if s == nil || s.inner == nil {
		return writer.Snapshot{}, fmt.Errorf("%w: nil catalog session", writer.ErrInvalidSession)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.ExpectedNextLSN != s.snapshot.Head.NextLSN {
		return writer.Snapshot{}, fmt.Errorf("%w: expected_next_lsn=%d current=%d", writer.ErrPublishFailed, req.ExpectedNextLSN, s.snapshot.Head.NextLSN)
	}
	if req.Segment.BaseLSN != req.ExpectedNextLSN {
		return writer.Snapshot{}, fmt.Errorf("%w: segment base_lsn=%d expected_next_lsn=%d", writer.ErrPublishFailed, req.Segment.BaseLSN, req.ExpectedNextLSN)
	}

	nextHead, err := s.inner.AppendSegment(ctx, req.Segment)
	if err != nil {
		return writer.Snapshot{}, mapCatalogError(err)
	}

	s.snapshot = writer.Snapshot{
		Head: nextHead,
		Identity: writer.WriterIdentity{
			Epoch: s.snapshot.Identity.Epoch,
			Tag:   s.snapshot.Identity.Tag,
		},
	}
	next := s.snapshot
	return next, nil
}

func (s *Session) ApplyPendingRetention(ctx context.Context) (writer.RetentionResult, error) {
	if s == nil || s.inner == nil {
		return writer.RetentionResult{}, fmt.Errorf("%w: nil catalog session", writer.ErrInvalidSession)
	}
	inner, ok := s.inner.(catalog.RetentionWriterSession)
	if !ok {
		return writer.RetentionResult{}, writer.ErrRetentionUnsupported
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	result, err := inner.ApplyPendingRetention(ctx)
	if err != nil {
		return writer.RetentionResult{}, mapRetentionError(err)
	}
	s.snapshot = writer.Snapshot{
		Head: result.Head,
		Identity: writer.WriterIdentity{
			Epoch: s.snapshot.Identity.Epoch,
			Tag:   s.snapshot.Identity.Tag,
		},
	}
	return writer.RetentionResult{
		Snapshot:      s.snapshot,
		PolicyVersion: result.Request.PolicyVersion,
		RequestedLSN:  result.Head.AppliedRetentionLSN,
		Applied:       result.Applied,
	}, nil
}

func mapCatalogError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, catalog.ErrStaleWriter) {
		return fmt.Errorf("%w: %w", writer.ErrStaleWriter, err)
	}
	if errors.Is(err, catalog.ErrCommitIndeterminate) {
		return fmt.Errorf("%w: %w", writer.ErrPublishIndeterminate, err)
	}
	return fmt.Errorf("%w: %w", writer.ErrPublishFailed, err)
}

func mapRetentionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, catalog.ErrStaleWriter) {
		return fmt.Errorf("%w: %w", writer.ErrStaleWriter, err)
	}
	if errors.Is(err, catalog.ErrRetentionUnsupported) {
		return fmt.Errorf("%w: %w", writer.ErrRetentionUnsupported, err)
	}
	return fmt.Errorf("%w: %w", writer.ErrRetentionFailed, err)
}
