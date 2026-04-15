package reader

import (
	"context"
	"errors"

	readerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/reader/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the ReaderService gRPC contract.
type Service struct {
	readerv1.UnimplementedReaderServiceServer
	backend Backend
}

func NewService(backend Backend) (*Service, error) {
	if backend == nil {
		return nil, errors.New("reader backend is required")
	}
	return &Service{backend: backend}, nil
}

func (s *Service) GetPartitionHead(ctx context.Context, req *readerv1.GetPartitionHeadRequest) (*readerv1.GetPartitionHeadResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetPartition() < 0 {
		return nil, status.Error(codes.InvalidArgument, "partition must be non-negative")
	}

	result, err := s.backend.GetPartitionHead(ctx, req.GetPartition())
	if err != nil {
		return nil, status.Errorf(mapReadError(err), "get partition head failed: %v", err)
	}

	return &readerv1.GetPartitionHeadResponse{
		Partition:        result.Partition,
		HighWatermarkLsn: result.HighWatermarkLSN,
	}, nil
}

func (s *Service) ConsumePartition(ctx context.Context, req *readerv1.ConsumePartitionRequest) (*readerv1.ConsumePartitionResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetPartition() < 0 {
		return nil, status.Error(codes.InvalidArgument, "partition must be non-negative")
	}

	result, err := s.backend.ConsumePartition(ctx, req.GetPartition(), req.GetStartAfterLsn(), req.GetLimit())
	if err != nil {
		return nil, status.Errorf(mapReadError(err), "consume partition failed: %v", err)
	}

	resp := &readerv1.ConsumePartitionResponse{
		NextStartAfterLsn: result.NextStartAfterLSN,
		HighWatermarkLsn:  result.HighWatermarkLSN,
	}
	for _, event := range result.Events {
		resp.Events = append(resp.Events, eventToProto(event))
	}
	return resp, nil
}

func (s *Service) TailPartition(req *readerv1.TailPartitionRequest, stream readerv1.ReaderService_TailPartitionServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetPartition() < 0 {
		return status.Error(codes.InvalidArgument, "partition must be non-negative")
	}
	if req.GetFromNow() && req.GetStartAfterLsn() != 0 {
		return status.Error(codes.InvalidArgument, "from_now cannot be combined with start_after_lsn")
	}

	err := s.backend.TailPartition(stream.Context(), req.GetPartition(), req.GetStartAfterLsn(), req.GetFromNow(), func(event Event) error {
		return stream.Send(&readerv1.TailPartitionResponse{
			Event: eventToProto(event),
		})
	})
	if err != nil {
		return status.Errorf(mapReadError(err), "tail partition failed: %v", err)
	}
	return nil
}

func eventToProto(event Event) *readerv1.PartitionEvent {
	return &readerv1.PartitionEvent{
		Partition: event.Partition,
		Lsn:       event.LSN,
		Value:     event.Value,
	}
}

func mapReadError(err error) codes.Code {
	switch {
	case errors.Is(err, context.Canceled):
		return codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return codes.DeadlineExceeded
	case errors.Is(err, ErrPartitionNotFound):
		return codes.NotFound
	default:
		return codes.Internal
	}
}
