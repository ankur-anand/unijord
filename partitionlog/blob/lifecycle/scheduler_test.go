package lifecycle

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSchedulerBoundsConcurrencyAndFairlyRequeuesContinuation(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{delay: time.Millisecond}
	runner.reclaim = func(partition uint32, call int) (Result, error) {
		return Result{HasMore: partition == 1 && call == 1}, nil
	}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{
		MaxConcurrentPartitions: 1,
		ContinuationDelay:       time.Millisecond,
	})

	summary, err := scheduler.Run(context.Background(), []Task{
		{Partition: 1, Operation: OperationReclaim},
		{Partition: 2, Operation: OperationReclaim},
		{Partition: 3, Operation: OperationReclaim},
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Tasks != 3 || summary.Completed != 3 || summary.Passes != 4 || summary.Continuations != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	runner.mu.Lock()
	order := append([]uint32(nil), runner.order...)
	maxActive := runner.maxActive
	runner.mu.Unlock()
	if want := []uint32{1, 2, 3, 1}; !equalUint32s(order, want) {
		t.Fatalf("run order = %v, want %v", order, want)
	}
	if maxActive != 1 {
		t.Fatalf("max active = %d, want 1", maxActive)
	}
}

func TestSchedulerUsesConfiguredPartitionConcurrency(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{delay: 5 * time.Millisecond}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{MaxConcurrentPartitions: 2})
	tasks := make([]Task, 6)
	for i := range tasks {
		tasks[i] = Task{Partition: uint32(i + 1)}
	}
	if _, err := scheduler.Run(context.Background(), tasks); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	runner.mu.Lock()
	maxActive := runner.maxActive
	runner.mu.Unlock()
	if maxActive != 2 {
		t.Fatalf("max active = %d, want 2", maxActive)
	}
}

func TestSchedulerDefersAnUnboundedContinuationAtPassLimit(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{}
	runner.reclaim = func(uint32, int) (Result, error) {
		return Result{HasMore: true}, nil
	}
	var final SchedulerEvent
	scheduler := newTestScheduler(t, runner, SchedulerOptions{
		MaxPassesPerTask:  3,
		ContinuationDelay: time.Millisecond,
		Observer: SchedulerObserverFunc(func(event SchedulerEvent) {
			if event.Final {
				final = event
			}
		}),
	})
	summary, err := scheduler.Run(context.Background(), []Task{{Partition: 12}})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Deferred != 1 || summary.Completed != 0 || summary.Passes != 3 || summary.Continuations != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	if !final.Final || !final.Deferred || !final.Result.HasMore {
		t.Fatalf("final event = %+v", final)
	}
}

func TestSchedulerRetriesWithBackoffAndObservesEveryPass(t *testing.T) {
	t.Parallel()

	retryErr := errors.New("provider throttled")
	runner := &recordingRunner{}
	runner.reclaim = func(_ uint32, call int) (Result, error) {
		if call < 3 {
			return Result{}, retryErr
		}
		return Result{DeletedObjects: 7}, nil
	}
	var events []SchedulerEvent
	scheduler := newTestScheduler(t, runner, SchedulerOptions{
		MaxAttempts:         3,
		RetryInitialBackoff: time.Millisecond,
		RetryMaxBackoff:     2 * time.Millisecond,
		Observer: SchedulerObserverFunc(func(event SchedulerEvent) {
			events = append(events, event)
		}),
	})

	summary, err := scheduler.Run(context.Background(), []Task{{Partition: 9}})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if summary.Completed != 1 || summary.Passes != 3 || summary.Retries != 2 || summary.Failed != 0 {
		t.Fatalf("summary = %+v", summary)
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	if events[0].Attempt != 1 || events[0].NextDelay != time.Millisecond || events[0].Final || !errors.Is(events[0].Err, retryErr) {
		t.Fatalf("event[0] = %+v", events[0])
	}
	if events[1].Attempt != 2 || events[1].NextDelay != 2*time.Millisecond || events[1].Final {
		t.Fatalf("event[1] = %+v", events[1])
	}
	if events[2].Attempt != 3 || !events[2].Final || events[2].Result.DeletedObjects != 7 || events[2].Err != nil {
		t.Fatalf("event[2] = %+v", events[2])
	}
}

func TestSchedulerRunTimeoutExhaustsRetryBudget(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{waitForContext: true}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{
		PartitionRunTimeout: time.Millisecond,
		MaxAttempts:         2,
		RetryInitialBackoff: time.Millisecond,
		RetryMaxBackoff:     time.Millisecond,
	})

	summary, err := scheduler.Run(context.Background(), []Task{{Partition: 4}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run() error = %v, want %v", err, context.DeadlineExceeded)
	}
	if summary.Failed != 1 || summary.Passes != 2 || summary.Retries != 1 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestSchedulerContinuesOtherPartitionsAfterTerminalFailure(t *testing.T) {
	t.Parallel()

	partitionErr := errors.New("partition failed")
	runner := &recordingRunner{}
	runner.reclaim = func(partition uint32, _ int) (Result, error) {
		if partition == 1 {
			return Result{}, partitionErr
		}
		return Result{}, nil
	}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{MaxAttempts: 1})
	summary, err := scheduler.Run(context.Background(), []Task{{Partition: 1}, {Partition: 2}})
	if !errors.Is(err, partitionErr) {
		t.Fatalf("Run() error = %v, want %v", err, partitionErr)
	}
	if summary.Completed != 1 || summary.Failed != 1 || summary.Passes != 2 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestSchedulerCancellationWaitsForStartedPasses(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	finished := make(chan struct{})
	runner := runnerFuncs{
		reclaim: func(ctx context.Context, _ uint32) (Result, error) {
			close(started)
			defer close(finished)
			<-ctx.Done()
			return Result{}, ctx.Err()
		},
	}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := scheduler.Run(ctx, []Task{{Partition: 7}})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after cancellation")
	}
	select {
	case <-finished:
	default:
		t.Fatal("Run() returned before the active pass exited")
	}
}

func TestSchedulerDispatchesScrubAndRejectsDuplicatePartition(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{}
	scheduler := newTestScheduler(t, runner, SchedulerOptions{})
	if _, err := scheduler.Run(context.Background(), []Task{{Partition: 5, Operation: OperationScrub}}); err != nil {
		t.Fatalf("Run(scrub) error = %v", err)
	}
	runner.mu.Lock()
	scrubCalls := runner.scrubCalls[5]
	runner.mu.Unlock()
	if scrubCalls != 1 {
		t.Fatalf("scrub calls = %d, want 1", scrubCalls)
	}

	_, err := scheduler.Run(context.Background(), []Task{
		{Partition: 5, Operation: OperationReclaim},
		{Partition: 5, Operation: OperationScrub},
	})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Run(duplicate) error = %v, want %v", err, ErrInvalidOptions)
	}
	if _, err := scheduler.Run(context.Background(), []Task{{Partition: 6, Operation: Operation(99)}}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Run(invalid operation) error = %v, want %v", err, ErrInvalidOptions)
	}
}

func TestNewSchedulerRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	runner := &recordingRunner{}
	invalid := []SchedulerOptions{
		{MaxConcurrentPartitions: -1},
		{PartitionRunTimeout: -1},
		{MaxPassesPerTask: -1},
		{MaxAttempts: -1},
		{RetryInitialBackoff: -1},
		{RetryMaxBackoff: -1},
		{RetryInitialBackoff: time.Second, RetryMaxBackoff: time.Millisecond},
		{RetryJitterFraction: -0.1},
		{RetryJitterFraction: 1.1},
		{ContinuationDelay: -1},
	}
	for _, opts := range invalid {
		if _, err := NewScheduler(runner, opts); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("NewScheduler(%+v) error = %v, want %v", opts, err, ErrInvalidOptions)
		}
	}
	if _, err := NewScheduler(nil, SchedulerOptions{}); !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("NewScheduler(nil) error = %v, want %v", err, ErrInvalidOptions)
	}
}

func newTestScheduler(t *testing.T, runner Runner, opts SchedulerOptions) *Scheduler {
	t.Helper()
	scheduler, err := newScheduler(runner, opts, time.Now, func() float64 { return 0.5 })
	if err != nil {
		t.Fatalf("newScheduler() error = %v", err)
	}
	return scheduler
}

type recordingRunner struct {
	mu             sync.Mutex
	reclaim        func(partition uint32, call int) (Result, error)
	delay          time.Duration
	waitForContext bool
	active         int
	maxActive      int
	order          []uint32
	reclaimCalls   map[uint32]int
	scrubCalls     map[uint32]int
}

func (r *recordingRunner) RunPartition(ctx context.Context, partition uint32) (Result, error) {
	call := r.begin(partition, false)
	defer r.end()
	if r.waitForContext {
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	if r.reclaim != nil {
		return r.reclaim(partition, call)
	}
	return Result{}, nil
}

func (r *recordingRunner) ScrubPartition(ctx context.Context, partition uint32) (Result, error) {
	r.begin(partition, true)
	defer r.end()
	return Result{}, ctx.Err()
}

func (r *recordingRunner) begin(partition uint32, scrub bool) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reclaimCalls == nil {
		r.reclaimCalls = make(map[uint32]int)
		r.scrubCalls = make(map[uint32]int)
	}
	if scrub {
		r.scrubCalls[partition]++
	} else {
		r.reclaimCalls[partition]++
	}
	r.active++
	r.maxActive = max(r.maxActive, r.active)
	r.order = append(r.order, partition)
	if scrub {
		return r.scrubCalls[partition]
	}
	return r.reclaimCalls[partition]
}

func (r *recordingRunner) end() {
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
}

type runnerFuncs struct {
	reclaim func(context.Context, uint32) (Result, error)
	scrub   func(context.Context, uint32) (Result, error)
}

func (r runnerFuncs) RunPartition(ctx context.Context, partition uint32) (Result, error) {
	return r.reclaim(ctx, partition)
}

func (r runnerFuncs) ScrubPartition(ctx context.Context, partition uint32) (Result, error) {
	if r.scrub == nil {
		return Result{}, nil
	}
	return r.scrub(ctx, partition)
}

func equalUint32s(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
