package writer

import (
	"context"
	"io"
	"sync"
	"testing"

	writerv1 "github.com/ankur-anand/unijord/gen/go/eventlake/writer/v1"
	"google.golang.org/grpc/metadata"
)

func TestNewServiceRequiresAppender(t *testing.T) {
	t.Parallel()

	if _, err := NewService(nil); err == nil {
		t.Fatal("NewService(nil) error = nil, want error")
	}
}

func TestAppend(t *testing.T) {
	t.Parallel()

	appender := &recordingAppender{}
	service, err := NewService(appender)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	resp, err := service.Append(context.Background(), &writerv1.AppendRequest{
		Value: []byte("hello"),
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if resp.GetLsn() != 1 {
		t.Fatalf("Append() lsn = %d, want 1", resp.GetLsn())
	}

	records := appender.snapshot()
	if len(records) != 1 {
		t.Fatalf("Snapshot() len = %d, want 1", len(records))
	}
	if records[0].LSN != 1 {
		t.Fatalf("Snapshot()[0].LSN = %d, want 1", records[0].LSN)
	}
	if got := string(records[0].Value); got != "hello" {
		t.Fatalf("Snapshot()[0].Value = %q, want %q", got, "hello")
	}
}

func TestAppendStream(t *testing.T) {
	t.Parallel()

	appender := &recordingAppender{}
	service, err := NewService(appender)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	stream := &appendStreamStub{
		ctx: context.Background(),
		requests: []*writerv1.AppendStreamRequest{
			{Value: []byte("a")},
			{Value: []byte("b")},
			{Value: []byte("c")},
		},
	}

	if err := service.AppendStream(stream); err != nil {
		t.Fatalf("AppendStream() error = %v", err)
	}

	if len(stream.responses) != 3 {
		t.Fatalf("AppendStream() len(responses) = %d, want 3", len(stream.responses))
	}
	for i, resp := range stream.responses {
		want := uint64(i + 1)
		if resp.GetLsn() != want {
			t.Fatalf("AppendStream() responses[%d].lsn = %d, want %d", i, resp.GetLsn(), want)
		}
	}

	records := appender.snapshot()
	if len(records) != 3 {
		t.Fatalf("Snapshot() len = %d, want 3", len(records))
	}
	for i, record := range records {
		want := uint64(i + 1)
		if record.LSN != want {
			t.Fatalf("Snapshot()[%d].LSN = %d, want %d", i, record.LSN, want)
		}
	}
}

type appendStreamStub struct {
	writerv1.WriterService_AppendStreamServer
	ctx       context.Context
	requests  []*writerv1.AppendStreamRequest
	responses []*writerv1.AppendStreamResponse
	index     int
}

func (s *appendStreamStub) Context() context.Context {
	return s.ctx
}

func (s *appendStreamStub) Recv() (*writerv1.AppendStreamRequest, error) {
	if s.index >= len(s.requests) {
		return nil, io.EOF
	}
	req := s.requests[s.index]
	s.index++
	return req, nil
}

func (s *appendStreamStub) Send(resp *writerv1.AppendStreamResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

func (s *appendStreamStub) SetHeader(metadata.MD) error {
	return nil
}

func (s *appendStreamStub) SendHeader(metadata.MD) error {
	return nil
}

func (s *appendStreamStub) SetTrailer(metadata.MD) {}

func (s *appendStreamStub) SendMsg(any) error {
	return nil
}

func (s *appendStreamStub) RecvMsg(any) error {
	return nil
}

type recordingAppender struct {
	mu      sync.Mutex
	nextLSN uint64
	records []Record
}

func (a *recordingAppender) Append(_ context.Context, value []byte) (Record, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.nextLSN++
	record := Record{
		LSN:   a.nextLSN,
		Value: cloneBytes(value),
	}
	a.records = append(a.records, record)
	return record, nil
}

func (a *recordingAppender) Close() error {
	return nil
}

func (a *recordingAppender) snapshot() []Record {
	a.mu.Lock()
	defer a.mu.Unlock()

	records := make([]Record, 0, len(a.records))
	for _, record := range a.records {
		records = append(records, Record{
			LSN:   record.LSN,
			Value: cloneBytes(record.Value),
		})
	}
	return records
}
