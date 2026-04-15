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

func TestListPartitionHeads(t *testing.T) {
	t.Parallel()

	backend := &backendStub{
		listHeadsResult: []PartitionHeadResult{
			{Partition: 0, HeadLSN: 12, OldestAvailableLSN: 4},
			{Partition: 1, HeadLSN: 15, OldestAvailableLSN: 7},
		},
	}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	resp, err := service.ListPartitionHeads(context.Background(), &readerv1.ListPartitionHeadsRequest{})
	if err != nil {
		t.Fatalf("ListPartitionHeads() error = %v", err)
	}

	if len(resp.GetHeads()) != 2 {
		t.Fatalf("len(heads) = %d, want 2", len(resp.GetHeads()))
	}
	if resp.GetHeads()[0].GetPartition() != 0 || resp.GetHeads()[0].GetHeadLsn() != 12 || resp.GetHeads()[0].GetOldestAvailableLsn() != 4 {
		t.Fatalf(
			"heads[0] = (%d,%d,%d), want (0,12,4)",
			resp.GetHeads()[0].GetPartition(),
			resp.GetHeads()[0].GetHeadLsn(),
			resp.GetHeads()[0].GetOldestAvailableLsn(),
		)
	}
	if resp.GetHeads()[1].GetPartition() != 1 || resp.GetHeads()[1].GetHeadLsn() != 15 || resp.GetHeads()[1].GetOldestAvailableLsn() != 7 {
		t.Fatalf(
			"heads[1] = (%d,%d,%d), want (1,15,7)",
			resp.GetHeads()[1].GetPartition(),
			resp.GetHeads()[1].GetHeadLsn(),
			resp.GetHeads()[1].GetOldestAvailableLsn(),
		)
	}
	if backend.listHeadsCalls != 1 {
		t.Fatalf("listHeadsCalls = %d, want 1", backend.listHeadsCalls)
	}
}

func TestGetPartitionHead(t *testing.T) {
	t.Parallel()

	backend := &backendStub{
		headResult: PartitionHeadResult{
			Partition:          2,
			HeadLSN:            15,
			OldestAvailableLSN: 7,
		},
	}
	service, err := NewService(backend)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	resp, err := service.GetPartitionHead(context.Background(), &readerv1.GetPartitionHeadRequest{
		Partition: 2,
	})
	if err != nil {
		t.Fatalf("GetPartitionHead() error = %v", err)
	}

	if resp.GetPartition() != 2 {
		t.Fatalf("partition = %d, want 2", resp.GetPartition())
	}
	if resp.GetHeadLsn() != 15 {
		t.Fatalf("head_lsn = %d, want 15", resp.GetHeadLsn())
	}
	if resp.GetOldestAvailableLsn() != 7 {
		t.Fatalf("oldest_available_lsn = %d, want 7", resp.GetOldestAvailableLsn())
	}
	if backend.headPartition != 2 {
		t.Fatalf("backend head partition = %d, want 2", backend.headPartition)
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
			NextStartAfterLSN:  12,
			HeadLSN:            15,
			OldestAvailableLSN: 7,
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
	if resp.GetHeadLsn() != 15 {
		t.Fatalf("head_lsn = %d, want 15", resp.GetHeadLsn())
	}
	if resp.GetOldestAvailableLsn() != 7 {
		t.Fatalf("oldest_available_lsn = %d, want 7", resp.GetOldestAvailableLsn())
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
	startAfterLSN := uint64(6)
	err = service.TailPartition(&readerv1.TailPartitionRequest{
		Partition:     1,
		StartAfterLsn: &startAfterLSN,
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
	if backend.tailPartition != 1 || backend.tailStartAfter != 6 || !backend.tailHasStartAfter {
		t.Fatalf("backend tail args = (%d,%d,%t), want (1,6,true)", backend.tailPartition, backend.tailStartAfter, backend.tailHasStartAfter)
	}
}

func TestTailPartitionWithoutStartAfterBeginsLive(t *testing.T) {
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
	}, stream)
	if err != nil {
		t.Fatalf("TailPartition() error = %v", err)
	}

	if backend.tailHasStartAfter {
		t.Fatal("backend tail unexpectedly received start_after_lsn")
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

func TestGetPartitionHeadMapsPartitionNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{headErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	_, err = service.GetPartitionHead(context.Background(), &readerv1.GetPartitionHeadRequest{Partition: 99})
	if err == nil {
		t.Fatal("GetPartitionHead() error = nil, want not found")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("GetPartitionHead() code = %s, want %s", got, codes.NotFound)
	}
}

func TestListPartitionHeadsMapsPartitionNotFound(t *testing.T) {
	t.Parallel()

	service, err := NewService(&backendStub{listHeadsErr: ErrPartitionNotFound})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	_, err = service.ListPartitionHeads(context.Background(), &readerv1.ListPartitionHeadsRequest{})
	if err == nil {
		t.Fatal("ListPartitionHeads() error = nil, want not found")
	}
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("ListPartitionHeads() code = %s, want %s", got, codes.NotFound)
	}
}

type backendStub struct {
	listHeadsCalls  int
	listHeadsResult []PartitionHeadResult
	listHeadsErr    error

	headPartition int32
	headResult    PartitionHeadResult
	headErr       error

	consumePartition  int32
	consumeStartAfter uint64
	consumeLimit      uint32
	consumeResult     ConsumeResult
	consumeErr        error

	tailPartition  int32
	tailStartAfter uint64
	tailHasStartAfter bool
	tailEvents     []Event
	tailErr        error
}

func (b *backendStub) ListPartitionHeads(_ context.Context) ([]PartitionHeadResult, error) {
	b.listHeadsCalls++
	if b.listHeadsErr != nil {
		return nil, b.listHeadsErr
	}
	return b.listHeadsResult, nil
}

func (b *backendStub) GetPartitionHead(_ context.Context, partition int32) (PartitionHeadResult, error) {
	b.headPartition = partition
	if b.headErr != nil {
		return PartitionHeadResult{}, b.headErr
	}
	return b.headResult, nil
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

func (b *backendStub) TailPartition(_ context.Context, partition int32, startAfterLSN *uint64, handler func(Event) error) error {
	b.tailPartition = partition
	if startAfterLSN != nil {
		b.tailStartAfter = *startAfterLSN
		b.tailHasStartAfter = true
	} else {
		b.tailStartAfter = 0
		b.tailHasStartAfter = false
	}
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
