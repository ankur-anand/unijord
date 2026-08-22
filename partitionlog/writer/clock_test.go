package writer

import (
	"sync"
	"time"
)

type manualClock struct {
	mu           sync.Mutex
	now          time.Time
	timers       map[*manualTimer]time.Time
	timerCreated chan struct{}
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{
		now:          now,
		timers:       make(map[*manualTimer]time.Time),
		timerCreated: make(chan struct{}, 16),
	}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(d time.Duration) Timer {
	c.mu.Lock()
	defer c.mu.Unlock()

	timer := &manualTimer{clock: c, ch: make(chan time.Time, 1)}
	if d <= 0 {
		timer.fired = true
		timer.ch <- c.now
	} else {
		c.timers[timer] = c.now.Add(d)
	}
	select {
	case c.timerCreated <- struct{}{}:
	default:
	}
	return timer
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
	for timer, deadline := range c.timers {
		if deadline.After(c.now) {
			continue
		}
		delete(c.timers, timer)
		timer.fired = true
		timer.ch <- c.now
	}
}

type manualTimer struct {
	clock   *manualClock
	ch      chan time.Time
	stopped bool
	fired   bool
}

func (t *manualTimer) C() <-chan time.Time {
	return t.ch
}

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.stopped || t.fired {
		return false
	}
	t.stopped = true
	delete(t.clock.timers, t)
	return true
}
