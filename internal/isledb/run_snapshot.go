package isledb

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

var ErrRunSnapshotExpired = errors.New("run snapshot expired")

func minTime(left, right time.Time) time.Time {
	if left.Before(right) {
		return left
	}
	return right
}

// RunSnapshot pins one immutable run-manifest view. It does not refresh and a
// later manifest replacement cannot change its candidate set.
type RunSnapshot struct {
	reader    *RunReader
	view      *runReadView
	expiresAt time.Time
	pinID     uint64
	closed    atomic.Bool
}

func (r *RunReader) Snapshot(ctx context.Context) (*RunSnapshot, error) {
	if ctx == nil {
		return nil, ErrRunReaderConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view, err := r.currentView()
	if err != nil {
		return nil, err
	}
	expiresAt := minTime(view.expiresAt, r.config.Now().Add(r.config.MaxSnapshotAge))
	pinID := registerPinnedRunView(view.manifest, expiresAt)
	return &RunSnapshot{reader: r, view: view, expiresAt: expiresAt, pinID: pinID}, nil
}

func (s *RunSnapshot) Revision() uint64 {
	if s == nil || s.view == nil {
		return 0
	}
	return s.view.revision
}

func (s *RunSnapshot) Head(ctx context.Context, timeline []byte) (RunHead, bool, error) {
	if err := s.ensureRunOpen(); err != nil {
		return RunHead{}, false, err
	}
	return s.reader.readHead(ctx, s.view, timeline)
}

func (s *RunSnapshot) Events(ctx context.Context, timeline []byte) ([]RunEvent, error) {
	if err := s.ensureRunOpen(); err != nil {
		return nil, err
	}
	return s.reader.readEvents(ctx, s.view, timeline)
}

func (s *RunSnapshot) Close() error {
	if s == nil {
		return nil
	}
	if s.closed.CompareAndSwap(false, true) {
		unregisterPinnedRunView(s.pinID)
	}
	return nil
}

func (s *RunSnapshot) ensureRunOpen() error {
	if s == nil || s.reader == nil || s.view == nil || s.closed.Load() || s.reader.closed.Load() {
		return ErrRunReaderClosed
	}
	if !s.reader.config.Now().Before(s.expiresAt) {
		_ = s.Close()
		return ErrRunSnapshotExpired
	}
	return nil
}
