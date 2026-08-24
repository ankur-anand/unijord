package reader

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoadGroupCoalescesOneKey(t *testing.T) {
	const callers = 16
	var group loadGroup
	var loads atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	start := make(chan struct{})
	results := make(chan error, callers)

	for range callers {
		go func() {
			<-start
			value, err := group.Do(context.Background(), "same", func(context.Context) (any, error) {
				if loads.Add(1) == 1 {
					close(entered)
				}
				<-release
				return "value", nil
			})
			if err == nil && value != "value" {
				err = errors.New("unexpected value")
			}
			results <- err
		}()
	}
	close(start)
	waitForLoadSignal(t, entered, "shared load")
	waitForLoadWaiters(t, &group, "same", callers)
	close(release)
	for range callers {
		if err := receiveLoadError(t, results); err != nil {
			t.Fatalf("Do() error = %v", err)
		}
	}
	if got := loads.Load(); got != 1 {
		t.Fatalf("load calls = %d, want 1", got)
	}
}

func TestLoadGroupRunsDifferentKeysConcurrently(t *testing.T) {
	var group loadGroup
	enteredA := make(chan struct{})
	enteredB := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 2)

	go func() {
		_, err := group.Do(context.Background(), "a", func(context.Context) (any, error) {
			close(enteredA)
			<-release
			return "a", nil
		})
		done <- err
	}()
	go func() {
		_, err := group.Do(context.Background(), "b", func(context.Context) (any, error) {
			close(enteredB)
			<-release
			return "b", nil
		})
		done <- err
	}()
	waitForLoadSignal(t, enteredA, "key a load")
	waitForLoadSignal(t, enteredB, "key b load")
	close(release)
	if err := receiveLoadError(t, done); err != nil {
		t.Fatalf("first Do() error = %v", err)
	}
	if err := receiveLoadError(t, done); err != nil {
		t.Fatalf("second Do() error = %v", err)
	}
}

func TestLoadGroupLeaderCanCancelWhileFollowerRemains(t *testing.T) {
	var group loadGroup
	entered := make(chan struct{})
	release := make(chan struct{})
	loadCanceled := make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	followerDone := make(chan error, 1)
	load := func(ctx context.Context) (any, error) {
		close(entered)
		select {
		case <-release:
			return "value", nil
		case <-ctx.Done():
			close(loadCanceled)
			return nil, context.Cause(ctx)
		}
	}

	go func() {
		_, err := group.Do(leaderCtx, "key", load)
		leaderDone <- err
	}()
	waitForLoadSignal(t, entered, "leader load")
	go func() {
		_, err := group.Do(context.Background(), "key", load)
		followerDone <- err
	}()
	waitForLoadWaiters(t, &group, "key", 2)
	cancelLeader()
	if err := receiveLoadError(t, leaderDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context.Canceled", err)
	}
	select {
	case <-loadCanceled:
		t.Fatal("leader cancellation stopped a load with a live follower")
	default:
	}
	close(release)
	if err := receiveLoadError(t, followerDone); err != nil {
		t.Fatalf("follower error = %v", err)
	}
}

func TestLoadGroupFollowerCanCancelWithoutStoppingSharedLoad(t *testing.T) {
	var group loadGroup
	entered := make(chan struct{})
	release := make(chan struct{})
	loadCanceled := make(chan struct{})
	leaderDone := make(chan error, 1)
	followerCtx, cancelFollower := context.WithCancel(context.Background())
	followerDone := make(chan error, 1)
	load := func(ctx context.Context) (any, error) {
		close(entered)
		select {
		case <-release:
			return "value", nil
		case <-ctx.Done():
			close(loadCanceled)
			return nil, context.Cause(ctx)
		}
	}

	go func() {
		_, err := group.Do(context.Background(), "key", load)
		leaderDone <- err
	}()
	waitForLoadSignal(t, entered, "leader load")
	go func() {
		_, err := group.Do(followerCtx, "key", load)
		followerDone <- err
	}()
	waitForLoadWaiters(t, &group, "key", 2)
	cancelFollower()
	if err := receiveLoadError(t, followerDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("follower error = %v, want context.Canceled", err)
	}
	select {
	case <-loadCanceled:
		t.Fatal("follower cancellation stopped a load with a live leader")
	default:
	}
	close(release)
	if err := receiveLoadError(t, leaderDone); err != nil {
		t.Fatalf("leader error = %v", err)
	}
}

func TestLoadGroupCancelsLoadWhenFinalWaiterLeaves(t *testing.T) {
	var group loadGroup
	entered := make(chan struct{})
	loadCanceled := make(chan error, 1)
	load := func(ctx context.Context) (any, error) {
		close(entered)
		<-ctx.Done()
		cause := context.Cause(ctx)
		loadCanceled <- cause
		return nil, cause
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { _, err := group.Do(ctxA, "key", load); done <- err }()
	waitForLoadSignal(t, entered, "shared load")
	go func() { _, err := group.Do(ctxB, "key", load); done <- err }()
	waitForLoadWaiters(t, &group, "key", 2)

	cancelA()
	cancelB()
	for range 2 {
		if err := receiveLoadError(t, done); !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	}
	if err := receiveLoadError(t, loadCanceled); !errors.Is(err, errNoLoadWaiters) {
		t.Fatalf("load cancellation cause = %v, want errNoLoadWaiters", err)
	}
}

func TestLoadGroupCloseRejectsCancelsAndDrains(t *testing.T) {
	var group loadGroup
	closeErr := errors.New("reader shutting down")
	entered := make(chan struct{})
	canceled := make(chan struct{})
	releaseWorker := make(chan struct{})
	callerDone := make(chan error, 1)
	go func() {
		_, err := group.Do(context.Background(), "key", func(ctx context.Context) (any, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-releaseWorker
			return nil, context.Cause(ctx)
		})
		callerDone <- err
	}()
	waitForLoadSignal(t, entered, "load worker")

	closeDone := make(chan struct{})
	go func() {
		group.Close(closeErr)
		close(closeDone)
	}()
	waitForLoadSignal(t, canceled, "load cancellation")
	if _, err := group.Do(context.Background(), "new", func(context.Context) (any, error) {
		return nil, nil
	}); !errors.Is(err, closeErr) {
		t.Fatalf("Do(after Close) error = %v, want close cause", err)
	}
	select {
	case <-closeDone:
		t.Fatal("Close returned before its worker drained")
	default:
	}
	close(releaseWorker)
	waitForLoadSignal(t, closeDone, "group Close")
	if err := receiveLoadError(t, callerDone); !errors.Is(err, closeErr) {
		t.Fatalf("active caller error = %v, want close cause", err)
	}
}

func TestLoadGroupOldWorkerCannotDeleteReplacement(t *testing.T) {
	var group loadGroup
	oldEntered := make(chan struct{})
	oldCanceled := make(chan struct{})
	releaseOld := make(chan struct{})
	oldCtx, cancelOld := context.WithCancel(context.Background())
	oldDone := make(chan error, 1)
	go func() {
		_, err := group.Do(oldCtx, "key", func(ctx context.Context) (any, error) {
			close(oldEntered)
			<-ctx.Done()
			close(oldCanceled)
			<-releaseOld
			return nil, context.Cause(ctx)
		})
		oldDone <- err
	}()
	waitForLoadSignal(t, oldEntered, "old load")
	oldCall := currentLoadCall(t, &group, "key")
	cancelOld()
	if err := receiveLoadError(t, oldDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("old caller error = %v, want context.Canceled", err)
	}
	waitForLoadSignal(t, oldCanceled, "old worker cancellation")

	newEntered := make(chan struct{})
	releaseNew := make(chan struct{})
	newDone := make(chan error, 1)
	var newLoads atomic.Int32
	newLoad := func(context.Context) (any, error) {
		newLoads.Add(1)
		close(newEntered)
		<-releaseNew
		return "new", nil
	}
	go func() { _, err := group.Do(context.Background(), "key", newLoad); newDone <- err }()
	waitForLoadSignal(t, newEntered, "replacement load")
	newCall := currentLoadCall(t, &group, "key")
	if newCall == oldCall {
		t.Fatal("replacement reused old call identity")
	}

	close(releaseOld)
	waitForLoadFinished(t, &group, oldCall)
	if got := currentLoadCall(t, &group, "key"); got != newCall {
		t.Fatal("old worker removed the replacement call")
	}
	followerDone := make(chan error, 1)
	go func() { _, err := group.Do(context.Background(), "key", newLoad); followerDone <- err }()
	waitForLoadWaiters(t, &group, "key", 2)
	close(releaseNew)
	if err := receiveLoadError(t, newDone); err != nil {
		t.Fatalf("replacement caller error = %v", err)
	}
	if err := receiveLoadError(t, followerDone); err != nil {
		t.Fatalf("replacement follower error = %v", err)
	}
	if got := newLoads.Load(); got != 1 {
		t.Fatalf("replacement load calls = %d, want 1", got)
	}
}

func waitForLoadSignal(t *testing.T, signal <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", operation)
	}
}

func receiveLoadError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for load result")
		return nil
	}
}

func waitForLoadWaiters(t *testing.T, group *loadGroup, key string, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		group.mu.Lock()
		call := group.calls[key]
		got := 0
		if call != nil {
			got = call.waiters
		}
		group.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiters for %q = %d, want %d", key, got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func currentLoadCall(t *testing.T, group *loadGroup, key string) *loadCall {
	t.Helper()
	group.mu.Lock()
	defer group.mu.Unlock()
	call := group.calls[key]
	if call == nil {
		t.Fatalf("no active call for %q", key)
	}
	return call
}

func waitForLoadFinished(t *testing.T, group *loadGroup, call *loadCall) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		group.mu.Lock()
		finished := call.finished
		group.mu.Unlock()
		if finished {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for old load worker to finish")
		}
		time.Sleep(time.Millisecond)
	}
}
