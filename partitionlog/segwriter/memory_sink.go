package segwriter

import (
	"context"
	"sync"
)

type MemorySink struct {
	mu         sync.Mutex
	uri        string
	object     []byte
	token      string
	beginCount int
}

func NewMemorySink(uri string) *MemorySink {
	if uri == "" {
		uri = "memory://segment"
	}
	return &MemorySink{uri: uri}
}

func (s *MemorySink) Begin(_ context.Context, _ Plan) (Txn, error) {
	s.mu.Lock()
	s.beginCount++
	s.mu.Unlock()
	return &memoryTxn{
		sink: s,
	}, nil
}

func (s *MemorySink) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.object...)
}

func (s *MemorySink) BeginCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginCount
}

type memoryTxn struct {
	mu        sync.Mutex
	sink      *MemorySink
	bytes     []byte
	aborted   bool
	completed bool
}

func (t *memoryTxn) Write(ctx context.Context, bytes []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.aborted {
		return ErrTxnAborted
	}
	if t.completed {
		return ErrTxnCompleted
	}
	t.bytes = append(t.bytes, bytes...)
	return nil
}

func (t *memoryTxn) Commit(ctx context.Context) (CommittedObject, error) {
	if err := ctx.Err(); err != nil {
		return CommittedObject{}, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.aborted {
		return CommittedObject{}, ErrTxnAborted
	}
	if t.completed {
		return CommittedObject{}, ErrTxnCompleted
	}

	object := append([]byte(nil), t.bytes...)
	t.completed = true

	t.sink.mu.Lock()
	t.sink.object = append(t.sink.object[:0], object...)
	t.sink.token = "memory-complete"
	uri := t.sink.uri
	token := t.sink.token
	t.sink.mu.Unlock()

	return CommittedObject{
		URI:       uri,
		SizeBytes: uint64(len(object)),
		Token:     token,
	}, nil
}

func (t *memoryTxn) Abort(_ context.Context) error {
	t.mu.Lock()
	if t.completed {
		t.mu.Unlock()
		return nil
	}
	t.aborted = true
	t.bytes = nil
	t.mu.Unlock()
	return nil
}
