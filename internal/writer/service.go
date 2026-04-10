package writer

import (
	"context"
	"errors"
	"io"

	"github.com/ankur-anand/isledb"
	"github.com/ankur-anand/isledb/manifest"
	writerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/writer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Service implements the internal WriterService gRPC contract.
type Service struct {
	writerv1.UnimplementedWriterServiceServer
	appender Appender
}

func NewService(appender Appender) (*Service, error) {
	if appender == nil {
		return nil, errors.New("writer appender is required")
	}
	return &Service{appender: appender}, nil
}

func (s *Service) Append(ctx context.Context, req *writerv1.AppendRequest) (*writerv1.AppendResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}

	record, err := s.appender.Append(ctx, req.GetValue())
	if err != nil {
		return nil, status.Errorf(mapAppendError(err), "append failed: %v", err)
	}
	return &writerv1.AppendResponse{Lsn: record.LSN}, nil
}

func (s *Service) AppendStream(stream writerv1.WriterService_AppendStreamServer) error {
	for {
		req, err := stream.Recv()
		switch {
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return err
		case req == nil:
			return status.Error(codes.InvalidArgument, "request is required")
		}

		record, err := s.appender.Append(stream.Context(), req.GetValue())
		if err != nil {
			return status.Errorf(mapAppendError(err), "append stream failed: %v", err)
		}
		if err := stream.Send(&writerv1.AppendStreamResponse{Lsn: record.LSN}); err != nil {
			return err
		}
	}
}

func mapAppendError(err error) codes.Code {
	switch {
	case errors.Is(err, isledb.ErrBackpressure):
		return codes.ResourceExhausted
	case errors.Is(err, manifest.ErrFenced):
		return codes.FailedPrecondition
	default:
		return codes.Internal
	}
}
