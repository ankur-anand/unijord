package reader

import (
	"context"
	"errors"
	"sync"
)

var errNoLoadWaiters = errors.New("partitionlog/reader: shared load has no waiters")

// loadGroup coalesces unfinished work by key. Each caller owns only its wait;
// the current waiter set owns the shared call, and the group owns shutdown.
// Values returned by load are shared between waiters and must be immutable.
type loadGroup struct {
	mu       sync.Mutex
	calls    map[string]*loadCall
	active   sync.WaitGroup
	closed   bool
	closeErr error
}

type loadCall struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	waiters  int
	finished bool
	value    any
	err      error
}

func (g *loadGroup) Do(ctx context.Context, key string, load func(context.Context) (any, error)) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	g.mu.Lock()
	if g.closed {
		err := g.closeErr
		g.mu.Unlock()
		return nil, err
	}
	if call := g.calls[key]; call != nil {
		call.waiters++
		g.mu.Unlock()
		return g.wait(ctx, key, call)
	}
	if g.calls == nil {
		g.calls = make(map[string]*loadCall)
	}

	callCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	call := &loadCall{
		ctx:     callCtx,
		cancel:  cancel,
		done:    make(chan struct{}),
		waiters: 1,
	}
	g.calls[key] = call
	g.active.Add(1)
	g.mu.Unlock()

	go g.run(key, call, load)
	return g.wait(ctx, key, call)
}

func (g *loadGroup) wait(ctx context.Context, key string, call *loadCall) (any, error) {
	select {
	case <-ctx.Done():
		g.releaseWaiter(key, call)
		return nil, ctx.Err()
	case <-call.done:
		g.releaseWaiter(key, call)
		return call.value, call.err
	}
}

func (g *loadGroup) releaseWaiter(key string, call *loadCall) {
	var cancel bool
	g.mu.Lock()
	call.waiters--
	if call.waiters == 0 && !call.finished {
		if g.calls[key] == call {
			delete(g.calls, key)
		}
		cancel = true
	}
	g.mu.Unlock()
	if cancel {
		call.cancel(errNoLoadWaiters)
	}
}

func (g *loadGroup) run(key string, call *loadCall, load func(context.Context) (any, error)) {
	value, err := load(call.ctx)
	if cause := context.Cause(call.ctx); cause != nil {
		value = nil
		err = cause
	}

	g.mu.Lock()
	call.value = value
	call.err = err
	call.finished = true
	if g.calls[key] == call {
		delete(g.calls, key)
	}
	close(call.done)
	g.mu.Unlock()

	call.cancel(context.Canceled)
	g.active.Done()
}

// Close rejects new calls, cancels every active load with cause, and waits for
// all workers to return. Loads must honor their shared context.
func (g *loadGroup) Close(cause error) {
	if cause == nil {
		cause = ErrClosed
	}

	var calls []*loadCall
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		g.closeErr = cause
		calls = make([]*loadCall, 0, len(g.calls))
		for _, call := range g.calls {
			calls = append(calls, call)
		}
	}
	g.mu.Unlock()

	for _, call := range calls {
		call.cancel(cause)
	}
	g.active.Wait()
}
