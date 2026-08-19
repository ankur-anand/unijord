package lifecycle

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

const (
	DefaultMaxConcurrentPartitions = 8
	DefaultPartitionRunTimeout     = 30 * time.Second
	DefaultMaxPassesPerTask        = 64
	DefaultSchedulerMaxAttempts    = 5
	DefaultRetryInitialBackoff     = 250 * time.Millisecond
	DefaultRetryMaxBackoff         = 30 * time.Second
	DefaultRetryJitterFraction     = 0.2
	DefaultContinuationDelay       = 10 * time.Millisecond
)

// Operation identifies one bounded lifecycle pass.
type Operation uint8

const (
	// OperationReclaim applies ordered retention and stale staging cleanup.
	OperationReclaim Operation = iota
	// OperationScrub discovers segment and catalog-page orphans.
	OperationScrub
)

func (o Operation) String() string {
	switch o {
	case OperationReclaim:
		return "reclaim"
	case OperationScrub:
		return "scrub"
	default:
		return fmt.Sprintf("operation(%d)", o)
	}
}

func (o Operation) valid() bool {
	return o == OperationReclaim || o == OperationScrub
}

// Task requests bounded lifecycle work for one known partition. A Scheduler
// run accepts at most one task per partition so reclaim and scrub never race
// for the same lease within the process.
type Task struct {
	Partition uint32
	Operation Operation
}

// Runner is implemented by Reclaimer and can be replaced by a test or service
// adapter.
type Runner interface {
	RunPartition(ctx context.Context, partition uint32) (Result, error)
	ScrubPartition(ctx context.Context, partition uint32) (Result, error)
}

type SchedulerOptions struct {
	// MaxConcurrentPartitions bounds lifecycle passes active at once.
	MaxConcurrentPartitions int
	// PartitionRunTimeout bounds any Runner call. Reclaimer also enforces its
	// own MaxPassDuration; this scheduler timeout may impose a tighter bound.
	PartitionRunTimeout time.Duration
	// MaxPassesPerTask bounds all attempts and continuations for one task in a
	// Run call. A successful HasMore result at the limit is deferred.
	MaxPassesPerTask int
	// MaxAttempts bounds consecutive failed attempts. A successful continuation
	// resets the attempt count.
	MaxAttempts int
	// RetryInitialBackoff is the delay after the first failed attempt.
	RetryInitialBackoff time.Duration
	// RetryMaxBackoff caps exponential retry delay.
	RetryMaxBackoff time.Duration
	// RetryJitterFraction randomizes retry delay by plus or minus this fraction.
	RetryJitterFraction float64
	// ContinuationDelay makes a HasMore task yield to other ready partitions.
	ContinuationDelay time.Duration
	// Observer receives one synchronous event after every pass.
	Observer SchedulerObserver
}

// SchedulerEvent describes one completed pass and the scheduler decision that
// follows it. Observers must return promptly.
type SchedulerEvent struct {
	Task      Task
	Attempt   int
	Duration  time.Duration
	Result    Result
	Err       error
	NextDelay time.Duration
	Final     bool
	Deferred  bool
}

type SchedulerObserver interface {
	Observe(SchedulerEvent)
}

// SchedulerObserverFunc adapts a function into a SchedulerObserver.
type SchedulerObserverFunc func(SchedulerEvent)

func (f SchedulerObserverFunc) Observe(event SchedulerEvent) {
	f(event)
}

// ScheduleResult summarizes one finite Scheduler.Run call.
type ScheduleResult struct {
	// Tasks is the number of accepted input tasks.
	Tasks int
	// Completed is the number of tasks that reported no remaining work.
	Completed int
	// Failed is the number of tasks that exhausted an attempt or pass limit
	// while returning an error.
	Failed int
	// Deferred is the number of tasks that still had work at the pass limit.
	Deferred int
	// Passes is the total number of runner calls that returned.
	Passes int
	// Retries is the number of error retries placed back on the queue.
	Retries int
	// Continuations is the number of HasMore passes placed back on the queue.
	Continuations int
}

// Scheduler runs bounded lifecycle passes over caller-supplied partitions. It
// does not discover partitions, persist a queue, or start work outside Run.
type Scheduler struct {
	runner Runner
	opts   SchedulerOptions
	now    func() time.Time
	random func() float64
}

// NewScheduler creates an explicit scheduler over runner.
func NewScheduler(runner Runner, opts SchedulerOptions) (*Scheduler, error) {
	return newScheduler(runner, opts, time.Now, rand.Float64)
}

func newScheduler(runner Runner, opts SchedulerOptions, now func() time.Time, random func() float64) (*Scheduler, error) {
	if runner == nil {
		return nil, fmt.Errorf("%w: nil lifecycle runner", ErrInvalidOptions)
	}
	if opts.MaxConcurrentPartitions < 0 {
		return nil, fmt.Errorf("%w: negative max concurrent partitions", ErrInvalidOptions)
	}
	if opts.MaxConcurrentPartitions == 0 {
		opts.MaxConcurrentPartitions = DefaultMaxConcurrentPartitions
	}
	if opts.PartitionRunTimeout < 0 {
		return nil, fmt.Errorf("%w: negative partition run timeout", ErrInvalidOptions)
	}
	if opts.PartitionRunTimeout == 0 {
		opts.PartitionRunTimeout = DefaultPartitionRunTimeout
	}
	if opts.MaxPassesPerTask < 0 {
		return nil, fmt.Errorf("%w: negative max passes per task", ErrInvalidOptions)
	}
	if opts.MaxPassesPerTask == 0 {
		opts.MaxPassesPerTask = DefaultMaxPassesPerTask
	}
	if opts.MaxAttempts < 0 {
		return nil, fmt.Errorf("%w: negative max attempts", ErrInvalidOptions)
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = DefaultSchedulerMaxAttempts
	}
	if opts.RetryInitialBackoff < 0 || opts.RetryMaxBackoff < 0 {
		return nil, fmt.Errorf("%w: negative retry backoff", ErrInvalidOptions)
	}
	if opts.RetryInitialBackoff == 0 {
		opts.RetryInitialBackoff = DefaultRetryInitialBackoff
	}
	if opts.RetryMaxBackoff == 0 {
		opts.RetryMaxBackoff = DefaultRetryMaxBackoff
	}
	if opts.RetryMaxBackoff < opts.RetryInitialBackoff {
		return nil, fmt.Errorf("%w: max retry backoff below initial backoff", ErrInvalidOptions)
	}
	if opts.RetryJitterFraction < 0 || opts.RetryJitterFraction > 1 {
		return nil, fmt.Errorf("%w: retry jitter fraction must be in [0,1]", ErrInvalidOptions)
	}
	if opts.RetryJitterFraction == 0 {
		opts.RetryJitterFraction = DefaultRetryJitterFraction
	}
	if opts.ContinuationDelay < 0 {
		return nil, fmt.Errorf("%w: negative continuation delay", ErrInvalidOptions)
	}
	if opts.ContinuationDelay == 0 {
		opts.ContinuationDelay = DefaultContinuationDelay
	}
	if now == nil || random == nil {
		return nil, fmt.Errorf("%w: nil scheduler clock or random source", ErrInvalidOptions)
	}
	return &Scheduler{runner: runner, opts: opts, now: now, random: random}, nil
}

// Run processes tasks until each reaches a no-more-work result, reaches its
// pass limit, exhausts its retry budget, or ctx is canceled. It waits for every
// started pass to return before exiting.
func (s *Scheduler) Run(ctx context.Context, tasks []Task) (ScheduleResult, error) {
	summary := ScheduleResult{Tasks: len(tasks)}
	queue, err := s.initialQueue(tasks)
	if err != nil {
		return summary, err
	}
	if len(tasks) == 0 {
		return summary, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan passResult, s.opts.MaxConcurrentPartitions)
	active := 0
	order := uint64(len(tasks))
	var failures []error

	for queue.Len() > 0 || active > 0 {
		if err := ctx.Err(); err != nil {
			cancel()
			s.drainCanceled(completed, active, &summary)
			return summary, err
		}

		now := s.now()
		for active < s.opts.MaxConcurrentPartitions && queue.Len() > 0 && !queue.peek().readyAt.After(now) {
			item := heap.Pop(queue).(*scheduledTask)
			active++
			go s.runPass(runCtx, item, completed)
		}

		var timer *time.Timer
		var timerC <-chan time.Time
		if active < s.opts.MaxConcurrentPartitions && queue.Len() > 0 {
			wait := queue.peek().readyAt.Sub(s.now())
			if wait < 0 {
				wait = 0
			}
			timer = time.NewTimer(wait)
			timerC = timer.C
		}

		select {
		case pass := <-completed:
			stopSchedulerTimer(timer)
			active--
			summary.Passes++
			pass.task.passes++
			event := SchedulerEvent{
				Task: pass.task.Task, Attempt: pass.task.attempt,
				Duration: pass.duration, Result: pass.result, Err: pass.err,
			}
			switch {
			case pass.err == nil && !pass.result.HasMore:
				summary.Completed++
				event.Final = true
			case pass.err != nil && (pass.task.attempt >= s.opts.MaxAttempts || pass.task.passes >= s.opts.MaxPassesPerTask):
				summary.Failed++
				event.Final = true
				failures = append(failures, fmt.Errorf(
					"lifecycle scheduler: %s partition=%d attempts=%d passes=%d: %w",
					pass.task.Operation, pass.task.Partition, pass.task.attempt, pass.task.passes, pass.err,
				))
			case pass.err == nil && pass.task.passes >= s.opts.MaxPassesPerTask:
				summary.Deferred++
				event.Final = true
				event.Deferred = true
			case pass.err == nil:
				summary.Continuations++
				event.NextDelay = s.opts.ContinuationDelay
				pass.task.attempt = 1
				pass.task.readyAt = s.now().Add(event.NextDelay)
				pass.task.order = order
				order++
				heap.Push(queue, pass.task)
			default:
				summary.Retries++
				event.NextDelay = s.retryDelay(pass.task.attempt)
				pass.task.attempt++
				pass.task.readyAt = s.now().Add(event.NextDelay)
				pass.task.order = order
				order++
				heap.Push(queue, pass.task)
			}
			s.observe(event)
		case <-timerC:
		case <-ctx.Done():
			stopSchedulerTimer(timer)
			cancel()
			s.drainCanceled(completed, active, &summary)
			return summary, ctx.Err()
		}
	}
	return summary, errors.Join(failures...)
}

func (s *Scheduler) initialQueue(tasks []Task) (*taskQueue, error) {
	queue := make(taskQueue, 0, len(tasks))
	seen := make(map[uint32]struct{}, len(tasks))
	now := s.now()
	for i, task := range tasks {
		if !task.Operation.valid() {
			return nil, fmt.Errorf("%w: invalid lifecycle operation=%d", ErrInvalidOptions, task.Operation)
		}
		if _, exists := seen[task.Partition]; exists {
			return nil, fmt.Errorf("%w: duplicate partition=%d", ErrInvalidOptions, task.Partition)
		}
		seen[task.Partition] = struct{}{}
		queue = append(queue, &scheduledTask{Task: task, attempt: 1, readyAt: now, order: uint64(i)})
	}
	heap.Init(&queue)
	return &queue, nil
}

func (s *Scheduler) runPass(ctx context.Context, task *scheduledTask, completed chan<- passResult) {
	passCtx, cancel := context.WithTimeout(ctx, s.opts.PartitionRunTimeout)
	defer cancel()
	started := s.now()
	var result Result
	var err error
	switch task.Operation {
	case OperationReclaim:
		result, err = s.runner.RunPartition(passCtx, task.Partition)
	case OperationScrub:
		result, err = s.runner.ScrubPartition(passCtx, task.Partition)
	}
	completed <- passResult{task: task, result: result, err: err, duration: s.now().Sub(started)}
}

func (s *Scheduler) retryDelay(failedAttempt int) time.Duration {
	delay := s.opts.RetryInitialBackoff
	for i := 1; i < failedAttempt && delay < s.opts.RetryMaxBackoff; i++ {
		if delay > s.opts.RetryMaxBackoff/2 {
			delay = s.opts.RetryMaxBackoff
			break
		}
		delay *= 2
	}
	if delay > s.opts.RetryMaxBackoff {
		delay = s.opts.RetryMaxBackoff
	}
	factor := 1 + (2*s.random()-1)*s.opts.RetryJitterFraction
	jittered := time.Duration(float64(delay) * factor)
	if jittered < 0 {
		return 0
	}
	return jittered
}

func (s *Scheduler) drainCanceled(completed <-chan passResult, active int, summary *ScheduleResult) {
	for range active {
		pass := <-completed
		summary.Passes++
		s.observe(SchedulerEvent{
			Task: pass.task.Task, Attempt: pass.task.attempt,
			Duration: pass.duration, Result: pass.result, Err: pass.err, Final: true,
		})
	}
}

func (s *Scheduler) observe(event SchedulerEvent) {
	if s.opts.Observer != nil {
		s.opts.Observer.Observe(event)
	}
}

func stopSchedulerTimer(timer *time.Timer) {
	if timer == nil {
		return
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

type scheduledTask struct {
	Task
	attempt int
	passes  int
	readyAt time.Time
	order   uint64
	index   int
}

type taskQueue []*scheduledTask

func (q taskQueue) Len() int { return len(q) }

func (q taskQueue) Less(i, j int) bool {
	if q[i].readyAt.Equal(q[j].readyAt) {
		return q[i].order < q[j].order
	}
	return q[i].readyAt.Before(q[j].readyAt)
}

func (q taskQueue) Swap(i, j int) {
	q[i], q[j] = q[j], q[i]
	q[i].index = i
	q[j].index = j
}

func (q *taskQueue) Push(value any) {
	item := value.(*scheduledTask)
	item.index = len(*q)
	*q = append(*q, item)
}

func (q *taskQueue) Pop() any {
	old := *q
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*q = old[:last]
	return item
}

func (q taskQueue) peek() *scheduledTask {
	return q[0]
}

type passResult struct {
	task     *scheduledTask
	result   Result
	err      error
	duration time.Duration
}
