package segwriter

import (
	"bytes"
	"context"
	"fmt"
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
		sink:  s,
		parts: make(map[int][]byte),
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
	parts     map[int][]byte
	aborted   bool
	completed bool
}

func (t *memoryTxn) UploadPart(_ context.Context, part Part) (PartReceipt, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.aborted {
		return PartReceipt{}, ErrTxnAborted
	}
	if t.completed {
		return PartReceipt{}, ErrTxnCompleted
	}
	if part.Number <= 0 {
		return PartReceipt{}, fmt.Errorf("%w: invalid part number %d", ErrInvalidOptions, part.Number)
	}
	t.parts[part.Number] = append([]byte(nil), part.Bytes...)
	return PartReceipt{
		Number: part.Number,
		Token:  fmt.Sprintf("memory-part-%d", part.Number),
	}, nil
}

func (t *memoryTxn) Complete(_ context.Context, receipts []PartReceipt) (CommittedObject, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.aborted {
		return CommittedObject{}, ErrTxnAborted
	}
	if t.completed {
		return CommittedObject{}, ErrTxnCompleted
	}

	var out bytes.Buffer
	for i, receipt := range receipts {
		want := i + 1
		if receipt.Number != want {
			return CommittedObject{}, fmt.Errorf("%w: receipt number=%d want=%d", ErrInvalidOptions, receipt.Number, want)
		}
		part, ok := t.parts[receipt.Number]
		if !ok {
			return CommittedObject{}, fmt.Errorf("%w: missing part %d", ErrInvalidOptions, receipt.Number)
		}
		out.Write(part)
	}
	object := out.Bytes()
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
	t.parts = nil
	t.mu.Unlock()
	return nil
}
