package runfile

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2/objstorage"
)

// RegionRef is the manifest-facing name for a region descriptor.
type RegionRef = RegionDescriptor

// RegionBlockCache is the cache surface used for immutable region ranges. Its
// shape intentionally matches a Ristretto cache keyed by string.
type RegionBlockCache interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte, cost int64) bool
}

// RegionLoadCoalescer shares concurrent loads for one exact region cache key.
type RegionLoadCoalescer interface {
	Do(ctx context.Context, key string, load func(context.Context) ([]byte, error)) ([]byte, error)
}

// rangeSourceError preserves the provenance of an object-store transport
// failure through Pebble's error wrapping. Callers can still match the
// original error through errors.Is/errors.As.
type rangeSourceError struct {
	cause error
}

func (e *rangeSourceError) Error() string { return e.cause.Error() }
func (e *rangeSourceError) Unwrap() error { return e.cause }

// RegionReadOptions configures cache and in-flight load sharing. RunID is
// mandatory whenever Cache or Coalescer is configured, because it is part of
// the isolation key alongside region kind and logical range.
type RegionReadOptions struct {
	RunID     [RunIDBytes]byte
	Cache     RegionBlockCache
	Coalescer RegionLoadCoalescer
}

type regionReadable struct {
	source     RangeSource
	objectKey  string
	objectSize int64
	region     RegionRef
	runID      [RunIDBytes]byte
	cache      RegionBlockCache
	coalescer  RegionLoadCoalescer
}

var _ objstorage.Readable = (*regionReadable)(nil)

// NewRegionReadable exposes one physical object region to Pebble as a bounded,
// zero-based logical file.
func NewRegionReadable(source RangeSource, objectKey string, objectSize int64, region RegionRef, configured ...RegionReadOptions) (objstorage.Readable, error) {
	if source == nil {
		return nil, invalidRunf("nil range source")
	}
	if len(configured) > 1 {
		return nil, invalidRunf("multiple region read options")
	}
	var options RegionReadOptions
	if len(configured) == 1 {
		options = configured[0]
	}
	if (options.Cache != nil || options.Coalescer != nil) && allZero(options.RunID[:]) {
		return nil, invalidRunf("region cache or coalescer requires a run ID")
	}
	if objectSize < 0 {
		return nil, invalidRunf("negative object size %d", objectSize)
	}
	if uint64(objectSize) > MaxRunObjectBytes {
		return nil, runTooLargef("object size %d exceeds %d", objectSize, MaxRunObjectBytes)
	}
	if region.Length == 0 {
		return nil, invalidRunf("region kind %d has zero length", region.Kind)
	}
	if region.Offset > uint64(objectSize) || region.Length > uint64(objectSize)-region.Offset {
		return nil, invalidRunf("region kind %d range exceeds object size", region.Kind)
	}
	if region.Offset > uint64(^uint64(0)>>1) || region.Length > uint64(^uint64(0)>>1) {
		return nil, runTooLargef("region kind %d does not fit signed range-reader offsets", region.Kind)
	}
	return &regionReadable{
		source: source, objectKey: objectKey, objectSize: objectSize, region: cloneRegion(region),
		runID: options.RunID, cache: options.Cache, coalescer: options.Coalescer,
	}, nil
}

func (r *regionReadable) ReadAt(ctx context.Context, dst []byte, logicalOffset int64) error {
	if r == nil || r.source == nil {
		return io.ErrClosedPipe
	}
	data, err := r.read(ctx, logicalOffset, len(dst))
	if err != nil {
		return err
	}
	copy(dst, data)
	return nil
}

func (r *regionReadable) read(ctx context.Context, logicalOffset int64, length int) ([]byte, error) {
	if logicalOffset < 0 || length < 0 || logicalOffset > int64(r.region.Length) || int64(length) > int64(r.region.Length)-logicalOffset {
		return nil, io.ErrUnexpectedEOF
	}
	if length == 0 {
		return nil, nil
	}
	key := regionCacheKey(r.runID, r.region.Kind, logicalOffset, int64(length))
	if r.cache != nil {
		if data, ok := r.cache.Get(key); ok && len(data) == length {
			return data, nil
		}
	}
	load := func(loadCtx context.Context) ([]byte, error) {
		// A preceding load may fill the cache between the initial miss and this
		// caller joining the in-flight group.
		if r.cache != nil {
			if data, ok := r.cache.Get(key); ok && len(data) == length {
				return data, nil
			}
		}
		data, err := r.readRange(loadCtx, logicalOffset, length)
		if err != nil {
			return nil, err
		}
		// The source owns its returned slice. Cache and retain a detached,
		// immutable copy so neither provider reuse nor caller buffers can alter it.
		data = bytes.Clone(data)
		if r.cache != nil {
			r.cache.Set(key, data, int64(len(data)))
		}
		return data, nil
	}
	if r.coalescer != nil {
		return r.coalescer.Do(ctx, key, load)
	}
	return load(ctx)
}

func (r *regionReadable) readRange(ctx context.Context, logicalOffset int64, length int) ([]byte, error) {
	physicalOffset := int64(r.region.Offset) + logicalOffset
	if physicalOffset < 0 || physicalOffset > r.objectSize || int64(length) > r.objectSize-physicalOffset {
		return nil, io.ErrUnexpectedEOF
	}
	data, err := r.source.ReadRange(ctx, r.objectKey, physicalOffset, int64(length))
	if err != nil {
		return nil, &rangeSourceError{cause: err}
	}
	if len(data) != length {
		return nil, fmt.Errorf(
			"%w: successful object range [%d,%d) returned %d bytes, want %d: %w",
			ErrCorruptRun,
			physicalOffset,
			physicalOffset+int64(length),
			len(data),
			length,
			io.ErrUnexpectedEOF,
		)
	}
	return data, nil
}

func (*regionReadable) Close() error { return nil }

func (r *regionReadable) Size() int64 {
	if r == nil {
		return 0
	}
	return int64(r.region.Length)
}

func (r *regionReadable) NewReadHandle(requested objstorage.ReadBeforeSize) objstorage.ReadHandle {
	return &regionReadHandle{readable: r, readBeforeSize: regionReadBeforeSize(r.Size(), requested)}
}

// regionReadBeforeSize preserves the existing IsleDB remote-SST policy while
// applying it to the logical region size rather than the enclosing run object.
func regionReadBeforeSize(regionSize int64, requested objstorage.ReadBeforeSize) int64 {
	if regionSize <= 0 || requested <= 0 {
		return 0
	}
	var window int64
	switch {
	case regionSize <= 4<<20:
		window = 32 << 10
	case regionSize <= 8<<20:
		window = 64 << 10
	case regionSize <= 16<<20:
		window = 128 << 10
	case regionSize <= 32<<20:
		window = 256 << 10
	default:
		window = 512 << 10
	}
	return min(min(window, regionSize), int64(requested))
}

func regionCacheKey(runID [RunIDBytes]byte, kind RegionKind, logicalOffset, length int64) string {
	var builder strings.Builder
	builder.Grow(RunIDBytes*2 + 1 + 5 + 1 + 20 + 1 + 20)
	builder.WriteString(hex.EncodeToString(runID[:]))
	builder.WriteByte(':')
	builder.WriteString(strconv.FormatUint(uint64(kind), 10))
	builder.WriteByte(':')
	builder.WriteString(strconv.FormatInt(logicalOffset, 10))
	builder.WriteByte(':')
	builder.WriteString(strconv.FormatInt(length, 10))
	return builder.String()
}

type regionReadHandle struct {
	readable       *regionReadable
	readBeforeSize int64
	buffer         []byte
	bufferOffset   int64
}

var _ objstorage.ReadHandle = (*regionReadHandle)(nil)

func (h *regionReadHandle) ReadAt(ctx context.Context, dst []byte, offset int64) error {
	if h.readable == nil {
		return io.ErrClosedPipe
	}
	if h.bufferContains(offset, len(dst)) {
		copy(dst, h.buffer[offset-h.bufferOffset:])
		return nil
	}
	readBeforeSize := h.readBeforeSize
	h.readBeforeSize = 0
	if readBeforeSize > int64(len(dst)) && offset >= 0 {
		extra := min(readBeforeSize-int64(len(dst)), offset)
		if extra > 0 {
			h.bufferOffset = offset - extra
			var err error
			h.buffer, err = h.readable.read(ctx, h.bufferOffset, len(dst)+int(extra))
			if err != nil {
				h.buffer = nil
				return err
			}
			copy(dst, h.buffer[extra:])
			return nil
		}
	}
	return h.readable.ReadAt(ctx, dst, offset)
}

func (h *regionReadHandle) bufferContains(offset int64, length int) bool {
	if len(h.buffer) == 0 || offset < h.bufferOffset || length < 0 {
		return false
	}
	relative := offset - h.bufferOffset
	return relative <= int64(len(h.buffer)) && int64(length) <= int64(len(h.buffer))-relative
}

func (h *regionReadHandle) Close() error {
	h.readable = nil
	h.buffer = nil
	return nil
}

func (*regionReadHandle) SetupForCompaction() {}

func (h *regionReadHandle) RecordCacheHit(context.Context, int64, int64) {
	h.readBeforeSize = 0
}

// RegionLoadGroup is a cancellation-safe in-flight load coalescer. The zero
// value is ready for use. Cancellation of one waiter does not cancel a shared
// load while another waiter remains.
type RegionLoadGroup struct {
	mu       sync.Mutex
	calls    map[string]*regionLoadCall
	active   sync.WaitGroup
	closed   bool
	closeErr error
}

type regionLoadCall struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	waiters  int
	finished bool
	value    []byte
	err      error
}

var _ RegionLoadCoalescer = (*RegionLoadGroup)(nil)

func (g *RegionLoadGroup) Do(ctx context.Context, key string, load func(context.Context) ([]byte, error)) ([]byte, error) {
	if ctx == nil {
		return nil, invalidRunf("nil coalesced-load context")
	}
	if load == nil {
		return nil, invalidRunf("nil coalesced-load function")
	}
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
		g.calls = make(map[string]*regionLoadCall)
	}
	loadCtx, cancel := context.WithCancelCause(context.WithoutCancel(ctx))
	call := &regionLoadCall{ctx: loadCtx, cancel: cancel, done: make(chan struct{}), waiters: 1}
	g.calls[key] = call
	g.active.Add(1)
	g.mu.Unlock()
	go g.run(key, call, load)
	return g.wait(ctx, key, call)
}

func (g *RegionLoadGroup) run(key string, call *regionLoadCall, load func(context.Context) ([]byte, error)) {
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

func (g *RegionLoadGroup) wait(ctx context.Context, key string, call *regionLoadCall) ([]byte, error) {
	select {
	case <-ctx.Done():
		g.releaseWaiter(key, call)
		return nil, ctx.Err()
	case <-call.done:
		g.mu.Lock()
		value, err := call.value, call.err
		call.waiters--
		g.mu.Unlock()
		return value, err
	}
}

func (g *RegionLoadGroup) releaseWaiter(key string, call *regionLoadCall) {
	g.mu.Lock()
	call.waiters--
	if call.waiters == 0 && !call.finished {
		if g.calls[key] == call {
			delete(g.calls, key)
		}
		call.cancel(context.Canceled)
	}
	g.mu.Unlock()
}

// Close rejects new loads, cancels active loads, and waits for their load
// functions to stop. A nil error uses context.Canceled.
func (g *RegionLoadGroup) Close(err error) {
	if err == nil {
		err = context.Canceled
	}
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		g.closeErr = err
		for _, call := range g.calls {
			call.cancel(err)
		}
	}
	g.mu.Unlock()
	g.active.Wait()
}
