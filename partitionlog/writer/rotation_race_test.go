package writer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

func TestWriterAgeAndPolicyCutDetachActiveOnce(t *testing.T) {
	testWriterActiveTransitionRace(t, false)
}

func TestWriterAgeCutAndFlushDetachActiveOnce(t *testing.T) {
	testWriterActiveTransitionRace(t, true)
}

func TestWriterAppendWaitsForActiveTransition(t *testing.T) {
	session := newRotationRaceSession(1)
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	opts.Roll.MaxSegmentAge = 0
	opts.Queue.MaxInflightSegments = 1

	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = w.Abort(context.Background())
	})

	if _, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("first")}); err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}

	w.mu.Lock()
	w.inflightSegments = 1
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ageCtx := newTransitionWaitContext(ctx)
	ageDone := runLockedWriterTransition(w, ageCtx, false)
	waitForTransitionWait(t, ageCtx.waiting, "age cut")

	appendCtx := newTransitionWaitContext(ctx)
	appendDone := make(chan appendCallResult, 1)
	go func() {
		result, err := w.Append(appendCtx, Record{TimestampMS: 2, Value: []byte("second")})
		appendDone <- appendCallResult{result: result, err: err}
	}()
	waitForTransitionWait(t, appendCtx.waiting, "append")
	select {
	case result := <-appendDone:
		t.Fatalf("Append returned before active transition completed: result=%+v err=%v", result.result, result.err)
	default:
	}

	w.mu.Lock()
	w.inflightSegments = 0
	w.signalStateLocked()
	w.mu.Unlock()

	waitForTransitionResult(t, ageDone, "age cut")
	select {
	case result := <-appendDone:
		if result.err != nil {
			t.Fatalf("Append(second) error = %v", result.err)
		}
		if result.result.LSN != 1 {
			t.Fatalf("Append(second) LSN = %d, want 1", result.result.LSN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Append after active transition")
	}

	session.waitForPublish(t)
	session.releaseOne()
	for i := 0; i < 4; i++ {
		session.releaseOne()
	}
	if _, err := w.Close(ctx); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

type appendCallResult struct {
	result AppendResult
	err    error
}

func testWriterActiveTransitionRace(t *testing.T, detachForeground bool) {
	t.Helper()

	session := newRotationRaceSession(1)
	opts := testSessionOptions(session, newMemorySegmentFactory())
	opts.Roll.MaxSegmentRecords = 0
	opts.Roll.MaxSegmentRawBytes = 0
	opts.Roll.MaxSegmentAge = 0
	opts.Queue.MaxInflightSegments = 1

	w, err := New(opts)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		_ = w.Abort(context.Background())
	})

	first, err := w.Append(context.Background(), Record{TimestampMS: 1, Value: []byte("first")})
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	if first.LSN != 0 {
		t.Fatalf("Append(first) LSN = %d, want 0", first.LSN)
	}

	// Occupy the only in-flight slot so both transitions must drop w.mu and
	// wait after capturing the same active segment.
	w.mu.Lock()
	w.inflightSegments = 1
	w.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ageCtx := newTransitionWaitContext(ctx)
	foregroundCtx := newTransitionWaitContext(ctx)

	ageDone := runLockedWriterTransition(w, ageCtx, false)
	waitForTransitionWait(t, ageCtx.waiting, "age cut")
	foregroundDone := runLockedWriterTransition(w, foregroundCtx, detachForeground)
	waitForTransitionWait(t, foregroundCtx.waiting, "foreground transition")

	// Release the synthetic slot. Exactly one transition should rotate the
	// active segment; the other must observe that the transition was handled.
	w.mu.Lock()
	w.inflightSegments = 0
	w.signalStateLocked()
	w.mu.Unlock()

	session.waitForPublish(t)
	second, err := w.Append(ctx, Record{TimestampMS: 2, Value: []byte("second")})
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}
	if second.LSN != 1 {
		t.Fatalf("Append(second) LSN = %d, want 1", second.LSN)
	}

	// Publishing the original segment frees capacity. A stale transition must
	// not detach it again or overwrite the replacement containing LSN 1.
	session.releaseOne()
	waitForTransitionResult(t, ageDone, "age cut")
	waitForTransitionResult(t, foregroundDone, "foreground transition")

	// Allow the replacement segment to publish during Close.
	for i := 0; i < 4; i++ {
		session.releaseOne()
	}
	snapshot, err := w.Close(ctx)
	if err != nil {
		t.Fatalf("Close() error = %v; writer error = %v", err, w.Err())
	}
	if err := w.Err(); err != nil {
		t.Fatalf("writer became terminal after overlapping transitions: %v", err)
	}
	if snapshot.Head.NextLSN != 2 || snapshot.Head.SegmentCount != 2 {
		t.Fatalf("Close() snapshot = %+v, want next_lsn=2 segment_count=2", snapshot)
	}

	published := session.publishedSegments()
	if len(published) != 2 {
		t.Fatalf("published segments = %d, want 2: %+v", len(published), published)
	}
	if published[0].BaseLSN != 0 || published[0].LastLSN != 0 ||
		published[1].BaseLSN != 1 || published[1].LastLSN != 1 {
		t.Fatalf("published ranges = [%d,%d] [%d,%d], want [0,0] [1,1]",
			published[0].BaseLSN, published[0].LastLSN,
			published[1].BaseLSN, published[1].LastLSN)
	}
}

type transitionWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func newTransitionWaitContext(ctx context.Context) *transitionWaitContext {
	return &transitionWaitContext{Context: ctx, waiting: make(chan struct{})}
}

func (c *transitionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func runLockedWriterTransition(w *Writer, ctx context.Context, detach bool) <-chan error {
	done := make(chan error, 1)
	go func() {
		w.mu.Lock()
		var err error
		if detach {
			err = w.detachActiveLocked(ctx)
		} else {
			err = w.cutLocked(ctx)
		}
		w.mu.Unlock()
		done <- err
	}()
	return done
}

func waitForTransitionWait(t *testing.T, waiting <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-waiting:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s to enter backpressure", name)
	}
}

func waitForTransitionResult(t *testing.T, done <-chan error, name string) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("%s error = %v", name, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type rotationRaceSession struct {
	mu        sync.Mutex
	snapshot  Snapshot
	started   chan PublishRequest
	release   chan struct{}
	published []pmeta.SegmentRef
}

func newRotationRaceSession(partition uint32) *rotationRaceSession {
	return &rotationRaceSession{
		snapshot: Snapshot{
			Head: pmeta.PartitionHead{
				Partition:   partition,
				WriterEpoch: 1,
			},
			Identity: WriterIdentity{
				Epoch: 1,
				Tag:   [16]byte{9, 8, 7},
			},
		},
		started: make(chan PublishRequest, 8),
		release: make(chan struct{}, 8),
	}
}

func (s *rotationRaceSession) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

func (s *rotationRaceSession) PublishSegment(ctx context.Context, req PublishRequest) (Snapshot, error) {
	select {
	case s.started <- req:
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return Snapshot{}, ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if req.ExpectedNextLSN != s.snapshot.Head.NextLSN || req.Segment.BaseLSN != s.snapshot.Head.NextLSN {
		return Snapshot{}, fmt.Errorf("publish base_lsn=%d expected=%d current=%d",
			req.Segment.BaseLSN, req.ExpectedNextLSN, s.snapshot.Head.NextLSN)
	}
	next := s.snapshot
	next.Head.NextLSN = req.Segment.NextLSN()
	if !next.Head.HasLastSegment {
		next.Head.OldestLSN = req.Segment.BaseLSN
	}
	next.Head.LastSegment = req.Segment
	next.Head.HasLastSegment = true
	next.Head.SegmentCount++
	s.snapshot = next
	s.published = append(s.published, req.Segment)
	return next, nil
}

func (s *rotationRaceSession) waitForPublish(t *testing.T) PublishRequest {
	t.Helper()
	select {
	case req := <-s.started:
		return req
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for segment publication")
		return PublishRequest{}
	}
}

func (s *rotationRaceSession) releaseOne() {
	s.release <- struct{}{}
}

func (s *rotationRaceSession) publishedSegments() []pmeta.SegmentRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]pmeta.SegmentRef(nil), s.published...)
}
