package blob

import (
	"context"
	"fmt"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catengine"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

type writerSession struct {
	cat     *Catalog
	session *catengine.Session
}

var _ csession.WriterSession = (*writerSession)(nil)
var _ csession.RetentionWriterSession = (*writerSession)(nil)

func (s *writerSession) Head() pmeta.PartitionHead { return s.session.Head() }
func (s *writerSession) Epoch() uint64             { return s.session.Epoch() }
func (s *writerSession) WriterID() [16]byte        { return s.session.WriterID() }

func (s *writerSession) AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	return s.session.AppendSegment(ctx, segment)
}

func (s *writerSession) ApplyPendingRetention(ctx context.Context) (csession.RetentionApplyResult, error) {
	if s.session.IsStale() {
		return csession.RetentionApplyResult{}, fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.session.Partition())
	}
	request, found, err := s.cat.LoadRetentionRequest(ctx, s.session.Partition())
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	head, applied, err := s.session.ApplyPendingRetention(ctx, request, found)
	if err != nil {
		return csession.RetentionApplyResult{}, err
	}
	return csession.RetentionApplyResult{Head: head, Request: request, Applied: applied}, nil
}
