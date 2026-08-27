package catengine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ankur-anand/unijord/internal/blobstore"
	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type EngineOptions struct {
	CatalogPrefix string
	DataRoot      string
	StreamID      string
	LeafLimit     uint32
	IndexLimit    uint32
	HashAlgo      segformat.HashAlgo

	// CASAttempts and its backoffs are shorthand used when the operation-
	// specific values below are zero.
	CASAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration

	AcquireAttempts       int
	AcquireInitialBackoff time.Duration
	AcquireMaxBackoff     time.Duration
	CommitAttempts        int
	CommitInitialBackoff  time.Duration
	CommitMaxBackoff      time.Duration
}

type Engine struct {
	backend blobstore.Store
	opts    EngineOptions
}

func NewEngine(backend blobstore.Store, opts EngineOptions) (*Engine, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: nil backend", csession.ErrInvalidRequest)
	}
	if opts.LeafLimit == 0 {
		opts.LeafLimit = pmeta.DefaultSegmentPageLimit
	}
	if opts.IndexLimit == 0 {
		opts.IndexLimit = pmeta.DefaultSegmentPageLimit
	}
	if opts.HashAlgo != segformat.HashCRC32C && opts.HashAlgo != segformat.HashXXH64 {
		opts.HashAlgo = segformat.HashXXH64
	}
	if opts.StreamID != "" {
		streamID, err := keylayout.CanonicalStreamID(opts.StreamID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", csession.ErrInvalidRequest, err)
		}
		opts.StreamID = streamID
	}
	if opts.CASAttempts <= 0 {
		opts.CASAttempts = 3
	}
	if opts.InitialBackoff < 0 || opts.MaxBackoff < 0 {
		return nil, fmt.Errorf("%w: negative CAS backoff", csession.ErrInvalidRequest)
	}
	if opts.MaxBackoff > 0 && opts.MaxBackoff < opts.InitialBackoff {
		return nil, fmt.Errorf("%w: max CAS backoff below initial backoff", csession.ErrInvalidRequest)
	}
	if opts.AcquireAttempts <= 0 {
		opts.AcquireAttempts = opts.CASAttempts
	}
	if opts.CommitAttempts <= 0 {
		opts.CommitAttempts = opts.CASAttempts
	}
	if opts.AcquireInitialBackoff == 0 {
		opts.AcquireInitialBackoff = opts.InitialBackoff
	}
	if opts.AcquireMaxBackoff == 0 {
		opts.AcquireMaxBackoff = opts.MaxBackoff
	}
	if opts.CommitInitialBackoff == 0 {
		opts.CommitInitialBackoff = opts.InitialBackoff
	}
	if opts.CommitMaxBackoff == 0 {
		opts.CommitMaxBackoff = opts.MaxBackoff
	}
	if opts.AcquireInitialBackoff < 0 || opts.AcquireMaxBackoff < 0 || opts.CommitInitialBackoff < 0 || opts.CommitMaxBackoff < 0 {
		return nil, fmt.Errorf("%w: negative operation CAS backoff", csession.ErrInvalidRequest)
	}
	if opts.AcquireMaxBackoff > 0 && opts.AcquireMaxBackoff < opts.AcquireInitialBackoff || opts.CommitMaxBackoff > 0 && opts.CommitMaxBackoff < opts.CommitInitialBackoff {
		return nil, fmt.Errorf("%w: operation max CAS backoff below initial backoff", csession.ErrInvalidRequest)
	}
	if _, err := NewConfig(opts.CatalogPrefix, opts.DataRoot, opts.StreamID, 0, opts.LeafLimit, opts.IndexLimit, opts.HashAlgo); err != nil {
		return nil, fmt.Errorf("%w: %w", csession.ErrInvalidRequest, err)
	}
	return &Engine{backend: backend, opts: opts}, nil
}

func (e *Engine) Initialize(ctx context.Context, partition uint32, nextLSN uint64) (pmeta.PartitionHead, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	config, err := e.config(partition)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	head, body, err := NewHead(config, nextLSN)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	object, swapped, err := e.backend.CompareAndSwap(ctx, config.HeadPath(), "", body)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if !swapped {
		head, err = config.DecodeHead(object.Body)
		if err != nil {
			return pmeta.PartitionHead{}, false, err
		}
	}
	state, err := config.PartitionHead(head)
	return state, swapped, err
}

func (e *Engine) Load(ctx context.Context, partition uint32) (catformat.Head, string, error) {
	config, err := e.config(partition)
	if err != nil {
		return catformat.Head{}, "", err
	}
	object, err := e.backend.Get(ctx, config.HeadPath())
	if errors.Is(err, blobstore.ErrObjectNotFound) {
		head, _, newErr := NewHead(config, 0)
		return head, "", newErr
	}
	if err != nil {
		return catformat.Head{}, "", err
	}
	head, err := config.DecodeHead(object.Body)
	return head, object.Token, err
}

func (e *Engine) LoadPartition(ctx context.Context, partition uint32) (pmeta.PartitionHead, error) {
	head, _, err := e.Load(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	config, err := e.config(partition)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	return config.PartitionHead(head)
}

func (e *Engine) OpenWriter(ctx context.Context, partition uint32, writerID [16]byte) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, err := e.config(partition)
	if err != nil {
		return nil, err
	}
	head, token, err := e.Load(ctx, partition)
	if err != nil {
		return nil, err
	}
	backoff := e.opts.AcquireInitialBackoff
	var lastErr error
	var attemptedEpoch uint64
	for attempt := 0; attempt < e.opts.AcquireAttempts; attempt++ {
		mutation, err := Takeover(config, head, writerID)
		if err != nil {
			return nil, err
		}
		attemptedEpoch = mutation.Head.Header.WriterEpoch
		object, swapped, casErr := e.backend.CompareAndSwap(ctx, config.HeadPath(), token, mutation.HeadBody)
		if casErr == nil {
			if swapped || bytes.Equal(object.Body, mutation.HeadBody) {
				return newSession(e, config, mutation.Head, object.Token), nil
			}
			observed, err := config.DecodeHead(object.Body)
			if err != nil {
				return nil, err
			}
			head, token = observed, object.Token
			lastErr = nil
			if attempt+1 == e.opts.AcquireAttempts {
				return nil, fmt.Errorf("%w: acquire writer partition=%d", csession.ErrConflict, partition)
			}
		} else {
			lastErr = casErr
		}
		if attempt+1 < e.opts.AcquireAttempts {
			if err := waitBackoff(ctx, backoff); err != nil {
				return nil, fmt.Errorf("%w: acquire writer partition=%d: %w", csession.ErrFenceIndeterminate, partition, errors.Join(lastErr, err))
			}
			backoff = growBackoff(backoff, e.opts.AcquireMaxBackoff)
		}
	}
	observed, observedToken, err := e.Load(ctx, partition)
	if err != nil {
		return nil, fmt.Errorf("%w: acquire writer partition=%d: %w", csession.ErrFenceIndeterminate, partition, errors.Join(lastErr, err))
	}
	if observed.Header.WriterID == writerID && observed.Header.WriterEpoch == attemptedEpoch {
		return newSession(e, config, observed, observedToken), nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("acquire writer partition=%d: %w", partition, lastErr)
	}
	return nil, fmt.Errorf("%w: acquire writer partition=%d", csession.ErrConflict, partition)
}

func (e *Engine) FindSegment(ctx context.Context, partition uint32, lsn uint64) (pmeta.SegmentRef, bool, error) {
	_, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	return reader.FindSegment(ctx, head, lsn)
}

func (e *Engine) LookupTimestamp(ctx context.Context, partition uint32, timestampMS int64) (pmeta.SegmentRef, bool, error) {
	_, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.SegmentRef{}, false, err
	}
	return reader.LookupTimestamp(ctx, head, timestampMS)
}

func (e *Engine) LookupTimestampSnapshot(ctx context.Context, partition uint32, timestampMS int64) (pmeta.PartitionHead, pmeta.SegmentRef, bool, error) {
	config, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, pmeta.SegmentRef{}, false, err
	}
	state, err := config.PartitionHead(head)
	if err != nil {
		return pmeta.PartitionHead{}, pmeta.SegmentRef{}, false, err
	}
	segment, found, err := reader.LookupTimestamp(ctx, head, timestampMS)
	return state, segment, found, err
}

func (e *Engine) ListSegments(ctx context.Context, partition uint32, fromLSN uint64, limit int) (pmeta.SegmentPage, error) {
	_, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.SegmentPage{}, err
	}
	return reader.ListSegments(ctx, head, fromLSN, limit)
}

// MaintenanceSnapshot returns the authoritative public head plus the physical
// catalog generation and highest reachable immutable page level.
func (e *Engine) MaintenanceSnapshot(ctx context.Context, partition uint32) (pmeta.PartitionHead, uint64, uint8, error) {
	config, head, _, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, err
	}
	state, err := config.PartitionHead(head)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, err
	}
	return state, head.Header.Generation, MaxPageLevel(head), nil
}

// ListSegmentsSnapshot lists segments from the same decoded head returned to
// lifecycle code, preventing a retention decision from mixing generations.
func (e *Engine) ListSegmentsSnapshot(ctx context.Context, partition uint32, fromLSN uint64, limit int) (pmeta.PartitionHead, uint64, uint8, pmeta.SegmentPage, error) {
	config, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, pmeta.SegmentPage{}, err
	}
	state, err := config.PartitionHead(head)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, pmeta.SegmentPage{}, err
	}
	page, err := reader.ListSegments(ctx, head, fromLSN, limit)
	return state, head.Header.Generation, MaxPageLevel(head), page, err
}

// ListPagePathsSnapshot lists reachable immutable pages from exactly the head
// snapshot returned alongside the result.
func (e *Engine) ListPagePathsSnapshot(ctx context.Context, partition uint32, level uint8, fromSeqLo uint64, limit int) (pmeta.PartitionHead, uint64, uint8, PagePathPage, error) {
	config, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, PagePathPage{}, err
	}
	state, err := config.PartitionHead(head)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, PagePathPage{}, err
	}
	page, err := reader.ListPagePaths(ctx, head, level, fromSeqLo, limit)
	return state, head.Header.Generation, MaxPageLevel(head), page, err
}

// IsPageReachableSnapshot checks target against one authoritative head and
// returns that same snapshot to the caller.
func (e *Engine) IsPageReachableSnapshot(ctx context.Context, partition uint32, target PageTarget) (pmeta.PartitionHead, uint64, uint8, bool, error) {
	config, head, reader, err := e.reader(ctx, partition)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, false, err
	}
	state, err := config.PartitionHead(head)
	if err != nil {
		return pmeta.PartitionHead{}, 0, 0, false, err
	}
	reachable, err := reader.IsPageReachable(ctx, head, target)
	return state, head.Header.Generation, MaxPageLevel(head), reachable, err
}

func (e *Engine) reader(ctx context.Context, partition uint32) (Config, catformat.Head, *Reader, error) {
	config, err := e.config(partition)
	if err != nil {
		return Config{}, catformat.Head{}, nil, err
	}
	head, _, err := e.Load(ctx, partition)
	if err != nil {
		return Config{}, catformat.Head{}, nil, err
	}
	reader, err := NewReader(config, backendPageSource{backend: e.backend})
	return config, head, reader, err
}

func (e *Engine) config(partition uint32) (Config, error) {
	return NewConfig(e.opts.CatalogPrefix, e.opts.DataRoot, e.opts.StreamID, partition, e.opts.LeafLimit, e.opts.IndexLimit, e.opts.HashAlgo)
}

type Session struct {
	engine *Engine
	config Config

	writerEpoch uint64
	writerID    [16]byte

	mu    sync.Mutex
	head  catformat.Head
	token string
	stale bool

	lastAppend      pmeta.SegmentRef
	lastAppendState pmeta.PartitionHead
	hasLastAppend   bool
}

func newSession(engine *Engine, config Config, head catformat.Head, token string) *Session {
	return &Session{
		engine: engine, config: config,
		writerEpoch: head.Header.WriterEpoch, writerID: head.Header.WriterID,
		head: head, token: token,
	}
}

func (s *Session) Epoch() uint64      { return s.writerEpoch }
func (s *Session) WriterID() [16]byte { return s.writerID }
func (s *Session) Partition() uint32  { return s.config.Partition }
func (s *Session) IsStale() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stale
}

func (s *Session) Head() pmeta.PartitionHead {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, _ := s.config.PartitionHead(s.head)
	return state
}

func (s *Session) AppendSegment(ctx context.Context, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stale {
		if s.hasLastAppend && s.lastAppend == segment {
			if segment.BaseLSN < s.head.Header.OldestLSN {
				return pmeta.PartitionHead{}, fmt.Errorf("%w: segment base_lsn=%d is below oldest_lsn=%d", csession.ErrCommitIndeterminate, segment.BaseLSN, s.head.Header.OldestLSN)
			}
			return s.lastAppendState, nil
		}
		return pmeta.PartitionHead{}, fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.config.Partition)
	}
	if s.head.HasLastSegment() && s.config.segmentRef(s.head.LastSegment) == segment {
		// A retry acknowledges this session's previously returned commit state. A
		// refresh may observe a successor fence without a successor segment, in
		// which case the durable head is newer even though this exact commit is
		// still the tip. Cache that authoritative head, but do not rebase the old
		// writer's acknowledgement onto the successor epoch.
		acknowledged := s.lastAppendState
		if !s.hasLastAppend || s.lastAppend != segment {
			var err error
			acknowledged, err = s.config.PartitionHead(s.head)
			if err != nil {
				return pmeta.PartitionHead{}, err
			}
		}
		if err := s.refreshLocked(ctx); err != nil {
			return pmeta.PartitionHead{}, err
		}
		if segment.BaseLSN < s.head.Header.OldestLSN {
			return pmeta.PartitionHead{}, fmt.Errorf("%w: segment base_lsn=%d is below oldest_lsn=%d", csession.ErrCommitIndeterminate, segment.BaseLSN, s.head.Header.OldestLSN)
		}
		reader, err := NewReader(s.config, backendPageSource{backend: s.engine.backend})
		if err != nil {
			return pmeta.PartitionHead{}, err
		}
		observed, found, err := reader.FindSegment(ctx, s.head, segment.BaseLSN)
		if err != nil {
			return pmeta.PartitionHead{}, err
		}
		if !found || observed != segment {
			return pmeta.PartitionHead{}, fmt.Errorf("%w: idempotent segment cannot be reconciled", csession.ErrCommitIndeterminate)
		}
		return acknowledged, nil
	}
	mutation, err := Append(s.config, s.head, segment)
	if err != nil {
		return pmeta.PartitionHead{}, err
	}
	if err := s.putPages(ctx, mutation.Pages); err != nil {
		return pmeta.PartitionHead{}, err
	}
	state, err := s.commitAppendLocked(ctx, s.head, mutation, segment)
	if err == nil {
		s.lastAppend = segment
		s.lastAppendState = state
		s.hasLastAppend = true
	}
	return state, err
}

func (s *Session) ApplyPendingRetention(ctx context.Context, request csession.RetentionRequest, found bool) (pmeta.PartitionHead, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshActiveLocked(ctx); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if !found || request.PolicyVersion <= s.head.Header.AppliedRetentionVersion {
		state, err := s.config.PartitionHead(s.head)
		return state, false, err
	}
	return s.applyRetentionLocked(ctx, request.BeforeLSN, request.PolicyVersion)
}

func (s *Session) ApplyRetention(ctx context.Context, beforeLSN, policyVersion uint64) (pmeta.PartitionHead, bool, error) {
	if err := ctx.Err(); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshActiveLocked(ctx); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	return s.applyRetentionLocked(ctx, beforeLSN, policyVersion)
}

func (s *Session) refreshActiveLocked(ctx context.Context) error {
	if s.stale {
		return fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.config.Partition)
	}
	if err := s.refreshLocked(ctx); err != nil {
		return err
	}
	if s.stale {
		return fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.config.Partition)
	}
	return nil
}

func (s *Session) applyRetentionLocked(ctx context.Context, beforeLSN, policyVersion uint64) (pmeta.PartitionHead, bool, error) {
	previous := s.head
	mutation, err := ApplyRetention(ctx, s.config, backendPageSource{backend: s.engine.backend}, previous, beforeLSN, policyVersion)
	if err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	if mutation.Head.Header.Generation == previous.Header.Generation {
		state, err := s.config.PartitionHead(previous)
		return state, false, err
	}
	if err := s.putPages(ctx, mutation.Pages); err != nil {
		return pmeta.PartitionHead{}, false, err
	}
	state, err := s.commitRetentionLocked(ctx, previous, mutation)
	return state, err == nil, err
}

func (s *Session) refreshLocked(ctx context.Context) error {
	head, token, err := s.engine.Load(ctx, s.config.Partition)
	if err != nil {
		return err
	}
	s.observeFence(head)
	s.head, s.token = head, token
	return nil
}

func (s *Session) putPages(ctx context.Context, pages []PageObject) error {
	for _, page := range pages {
		if _, err := s.engine.backend.Put(ctx, page.Key, page.Body); err != nil {
			return fmt.Errorf("catengine: put immutable page %q: %w", page.Key, err)
		}
	}
	return nil
}

func (s *Session) commitAppendLocked(ctx context.Context, previous catformat.Head, mutation Mutation, segment pmeta.SegmentRef) (pmeta.PartitionHead, error) {
	return s.commitLocked(ctx, previous, mutation, func(observed catformat.Head) (bool, error) {
		reader, err := NewReader(s.config, backendPageSource{backend: s.engine.backend})
		if err != nil {
			return false, err
		}
		found, ok, err := reader.FindSegment(ctx, observed, segment.BaseLSN)
		return ok && found == segment, err
	})
}

func (s *Session) commitRetentionLocked(ctx context.Context, previous catformat.Head, mutation Mutation) (pmeta.PartitionHead, error) {
	return s.commitLocked(ctx, previous, mutation, func(observed catformat.Head) (bool, error) {
		return observed.Header.AppliedRetentionVersion >= mutation.Head.Header.AppliedRetentionVersion &&
			observed.Header.OldestLSN >= mutation.Head.Header.OldestLSN, nil
	})
}

func (s *Session) commitLocked(ctx context.Context, previous catformat.Head, mutation Mutation, applied func(catformat.Head) (bool, error)) (pmeta.PartitionHead, error) {
	expectedToken := s.token
	backoff := s.engine.opts.CommitInitialBackoff
	var lastErr error
	for attempt := 0; attempt < s.engine.opts.CommitAttempts; attempt++ {
		object, swapped, casErr := s.engine.backend.CompareAndSwap(ctx, s.config.HeadPath(), expectedToken, mutation.HeadBody)
		if casErr == nil {
			if swapped {
				s.head, s.token = mutation.Head, object.Token
				return s.config.PartitionHead(mutation.Head)
			}
			observed, err := s.config.DecodeHead(object.Body)
			if err != nil {
				return pmeta.PartitionHead{}, err
			}
			s.observeFence(observed)
			ok, err := applied(observed)
			if err != nil {
				return pmeta.PartitionHead{}, err
			}
			if bytes.Equal(object.Body, mutation.HeadBody) {
				s.head, s.token = observed, object.Token
				return s.config.PartitionHead(observed)
			}
			if ok {
				s.head, s.token = observed, object.Token
				return s.config.PartitionHead(mutation.Head)
			}
			if s.stale {
				return pmeta.PartitionHead{}, fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.config.Partition)
			}
			if sameHead(previous, observed) {
				expectedToken = object.Token
				lastErr = nil
				if attempt+1 == s.engine.opts.CommitAttempts {
					return pmeta.PartitionHead{}, fmt.Errorf("%w: head CAS conflict partition=%d", csession.ErrConflict, s.config.Partition)
				}
			} else {
				return pmeta.PartitionHead{}, fmt.Errorf("%w: head changed partition=%d", csession.ErrConflict, s.config.Partition)
			}
		} else {
			lastErr = casErr
		}
		if attempt+1 < s.engine.opts.CommitAttempts {
			if err := waitBackoff(ctx, backoff); err != nil {
				return pmeta.PartitionHead{}, errors.Join(lastErr, err)
			}
			backoff = growBackoff(backoff, s.engine.opts.CommitMaxBackoff)
		}
	}

	observed, token, err := s.engine.Load(ctx, s.config.Partition)
	if err != nil {
		return pmeta.PartitionHead{}, fmt.Errorf("%w: partition=%d: %w", csession.ErrCommitIndeterminate, s.config.Partition, errors.Join(lastErr, err))
	}
	s.observeFence(observed)
	ok, observeErr := applied(observed)
	if observeErr != nil {
		return pmeta.PartitionHead{}, observeErr
	}
	if ok {
		s.head, s.token = observed, token
		return s.config.PartitionHead(mutation.Head)
	}
	if s.stale {
		return pmeta.PartitionHead{}, fmt.Errorf("%w: partition=%d", csession.ErrStaleWriter, s.config.Partition)
	}
	if sameHead(previous, observed) && lastErr != nil {
		return pmeta.PartitionHead{}, lastErr
	}
	return pmeta.PartitionHead{}, fmt.Errorf("%w: partition=%d", csession.ErrConflict, s.config.Partition)
}

func (s *Session) observeFence(head catformat.Head) {
	if head.Header.WriterEpoch != s.writerEpoch || head.Header.WriterID != s.writerID {
		s.stale = true
	}
}

func sameHead(left, right catformat.Head) bool {
	leftBody, leftErr := catformat.MarshalHead(left)
	rightBody, rightErr := catformat.MarshalHead(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftBody, rightBody)
}

type backendPageSource struct{ backend blobstore.Store }

func (s backendPageSource) GetPage(ctx context.Context, key string) ([]byte, error) {
	object, err := s.backend.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return object.Body, nil
}

func waitBackoff(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func growBackoff(current, maximum time.Duration) time.Duration {
	if current <= 0 || maximum <= 0 {
		return current
	}
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}
