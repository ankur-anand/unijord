package reader

import (
	"context"
	"io"
	"testing"

	readerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/reader/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestNewServiceRequiresBackend(t *testing.T) {
	t.Parallel()

	if _, err := NewService(nil); err == nil {
		t.Fatal("NewService(nil) error = nil, want error")
	}
}

func TestConsumePartition(t *testing.T) {
	t.Parallel()

	backend := &backendStub{
		consumeResult: ConsumeResult{
			Events: []Event{
				{Partition: 2, LSN: 11, Value: []byte("hello")},
				{Partition: 2, LSN: 12, Value: []byte("world")},
			},
			NextStartAfterLSN: 12,
			HighWatermarkLSN:  15,
		},
	}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	resp, err := service.ConsumePartition(context.Background(), &readerv1.ConsumePartitionRequest{
		Partition:     2,
		StartAfterLsn: 10,
		Limit:         2,
	})
	if err != nil {
		t.Fatalf("ConsumePartition() error = %v", err)
	}

	if len(resp.GetEvents()) != 2 {
		t.Fatalf("len(events) = %d, want 2", len(resp.GetEvents()))
	}
	if resp.GetEvents()[0].GetLsn() != 11 {
		t.Fatalf("events[0].lsn = %d, want 11", resp.GetEvents()[0].GetLsn())
	}
	if resp.GetNextStartAfterLsn() != 12 {
		t.Fatalf("next_start_after_lsn = %d, want 12", resp.GetNextStartAfterLsn())
	}
	if resp.GetHighWatermarkLsn() != 15 {
		t.Fatalf("high_watermark_lsn = %d, want 15", resp.GetHighWatermarkLsn())
	}
	if backend.consumePartition != 2 || backend.consumeStartAfter != 10 || backend.consumeLimit != 2 {
		t.Fatalf("backend consume args = (%d,%d,%d), want (2,10,2)", backend.consumePartition, backend.consumeStartAfter, backend.consumeLimit)
	}
}

func TestTailPartition(t *testing.T) {
	t.Parallel()

	backend := &backendStub{
		tailEvents: []Event{
			{Partition: 1, LSN: 7, Value: []byte("a")},
			{Partition: 1, LSN: 8, Value: []byte("b")},
		},
	}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	stream := &tailPartitionStreamStub{ctx: context.Background()}
	err = service.TailPartition(&readerv1.TailPartitionRequest{
		Partition:     1,
		StartAfterLsn: 6,
	}, stream)
	if err != nil {
		t.Fatalf("TailPartition() error = %v", err)
	}

	if len(stream.responses) != 2 {
		t.Fatalf("len(responses) = %d, want 2", len(stream.responses))
	}
	if stream.responses[1].GetEvent().GetLsn() != 8 {
		t.Fatalf("responses[1].event.lsn = %d, want 8", stream.responses[1].GetEvent().GetLsn())
	}
	if backend.tailPartition != 1 || backend.tailStartAfter != 6 || backend.tailFromNow {
		t.Fatalf("backend tail args = (%d,%d,%t), want (1,6,false)", backend.tailPartition, backend.tailStartAfter, backend.tailFromNow)
	}
}

func TestTailPartitionFromNow(t *testing.T) {
	t.Parallel()

	backend := &backendStub{
		tailEvents: []Event{
			{Partition: 1, LSN: 8, Value: []byte("b")},
		},
	}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	stream := &tailPartitionStreamStub{ctx: context.Background()}
	err = service.TailPartition(&readerv1.TailPartitionRequest{
		Partition: 1,
		FromNow:   true,
	}, stream)
	if err != nil {
		t.Fatalf("TailPartition() error = %v", err)
	}

	if !backend.tailFromNow {
		t.Fatal("backend tail did not receive from_now=true")
	}
	if backend.tailStartAfter != 0 {
		t.Fatalf("backend tail start_after = %d, want 0", backend.tailStartAfter)
	}
}

func TestTailPartitionRejectsMixedStartModes(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	err = service.TailPartition(&readerv1.TailPartitionRequest{
		Partition:     1,
		StartAfterLsn: 6,
		FromNow:       true,
	}, &tailPartitionStreamStub{ctx: context.Background()})
	if err == nil {
		t.Fatal("TailPartition() error = nil, want invalid argument")
	}
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("TailPartition() code = %s, want %s", got, codes.InvalidArgument)
	}
}

func TestConsumePartitionMapsPartitionNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{consumeErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	_, err = service.ConsumePartition(context.Background(), &readerv1.ConsumePartitionRequest{Partition: 99})
	if err == nil {
		t.Fatal("ConsumePartition() error = nil, want not found")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("ConsumePartition() code = %s, want %s", got, codes.NotFound)
	}
}

type backendStub struct {
	consumePartition  int32
	consumeStartAfter uint64
	consumeLimit      uint32
	consumeResult     ConsumeResult
	consumeErr        error

	tailPartition  int32
	tailStartAfter uint64
	tailFromNow    bool
	tailEvents     []Event
	tailErr        error
}

func (b *backendStub) ConsumePartition(_ context.Context, partition int32, startAfterLSN uint64, limit uint32) (ConsumeResult, error) {
	b.consumePartition = partition
	b.consumeStartAfter = startAfterLSN
	b.consumeLimit = limit
	if b.consumeErr != nil {
		return ConsumeResult{}, b.consumeErr
	}
	return b.consumeResult, nil
}

func (b *backendStub) TailPartition(_ context.Context, partition int32, startAfterLSN uint64, fromNow bool, handler func(Event) error) error {
	b.tailPartition = partition
	b.tailStartAfter = startAfterLSN
	b.tailFromNow = fromNow
	if b.tailErr != nil {
		return b.tailErr
	}
	for _, event := range b.tailEvents {
		if err := handler(event); err != nil {
			return err
		}
	}
	return nil
}

func (b *backendStub) Close() error {
	return nil
}

type tailPartitionStreamStub struct {
	readerv1.ReaderService_TailPartitionServer
	ctx       context.Context
	responses []*readerv1.TailPartitionResponse
}

func (s *tailPartitionStreamStub) Context() context.Context {
	return s.ctx
}

func (s *tailPartitionStreamStub) Send(resp *readerv1.TailPartitionResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *tailPartitionStreamStub) SetHeader(metadata.MD) error {
	return nil
}

func (s *tailPartitionStreamStub) SendHeader(metadata.MD) error {
	return nil
}

func (s *tailPartitionStreamStub) SetTrailer(metadata.MD) {}

func (s *tailPartitionStreamStub) SendMsg(any) error {
	return nil
}

func (s *tailPartitionStreamStub) RecvMsg(any) error {
	return io.EOF
}
