package blobsink

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	pcatalog "github.com/ankur-anand/unijord/partitionlog/catalog"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

type testWriterSession struct {
	mu       sync.Mutex
	snapshot plwriter.Snapshot
	publish  func(ctx context.Context, req plwriter.PublishRequest, current plwriter.Snapshot) (plwriter.Snapshot, error)
}

func (s *testWriterSession) Snapshot() plwriter.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *testWriterSession) PublishSegment(ctx context.Context, req plwriter.PublishRequest) (plwriter.Snapshot, error) {
	s.mu.Lock()
	current := s.snapshot
	publish := s.publish
	s.mu.Unlock()
	if publish == nil {
		return plwriter.Snapshot{}, fmt.Errorf("%w: publish not configured", plwriter.ErrPublishFailed)
	}
	next, err := publish(ctx, req, current)
	if err != nil {
		return plwriter.Snapshot{}, err
	}
	s.mu.Lock()
	s.snapshot = next
	s.mu.Unlock()
	return next, nil
}

func newCatalogWriterSession(t testing.TB, cat interface {
	pcatalog.Reader
	pcatalog.WriterManager
}, partition uint32, writerTag [16]byte) plwriter.Session {
	t.Helper()

	ws, err := cat.OpenWriter(context.Background(), partition, writerTag)
	if err != nil {
		t.Fatalf("OpenWriter() error = %v", err)
	}
	return &testWriterSession{
		snapshot: plwriter.Snapshot{
			Head: ws.Head(),
			Identity: plwriter.WriterIdentity{
				Epoch: ws.Epoch(),
				Tag:   writerTag,
			},
		},
		publish: func(ctx context.Context, req plwriter.PublishRequest, current plwriter.Snapshot) (plwriter.Snapshot, error) {
			state, err := ws.AppendSegment(ctx, req.Segment)
			if err != nil {
				if errors.Is(err, pcatalog.ErrStaleWriter) {
					return plwriter.Snapshot{}, fmt.Errorf("%w: %w", plwriter.ErrStaleWriter, err)
				}
				return plwriter.Snapshot{}, fmt.Errorf("%w: %w", plwriter.ErrPublishFailed, err)
			}
			return plwriter.Snapshot{
				Head: state,
				Identity: plwriter.WriterIdentity{
					Epoch: current.Identity.Epoch,
					Tag:   current.Identity.Tag,
				},
			}, nil
		},
	}
}
