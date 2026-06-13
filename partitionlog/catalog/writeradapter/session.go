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

func New(inner catalog.WriterSession) (*Session, error) {
	if inner == nil {
		return nil, fmt.Errorf("catalogsession/writeradapter: nil writer session")
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

	nextHead, err := s.inner.AppendSegment(ctx, req.Segment)
	if err != nil {
		return writer.Snapshot{}, mapCatalogError(err)
	}

	s.mu.Lock()
	s.snapshot = writer.Snapshot{
		Head: nextHead,
		Identity: writer.WriterIdentity{
			Epoch: s.snapshot.Identity.Epoch,
			Tag:   s.snapshot.Identity.Tag,
		},
	}
	next := s.snapshot
	s.mu.Unlock()
	return next, nil
}

func mapCatalogError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, catalog.ErrStaleWriter) {
		return fmt.Errorf("%w: %w", writer.ErrStaleWriter, err)
	}
	return fmt.Errorf("%w: %w", writer.ErrPublishFailed, err)
}
