package stream

import (
	"context"
	"fmt"
	"sync"
)

// BufferPool bounds the memory retained by one or more streaming uploads.
// Buffers are allocated lazily and reused. A pool never allocates more than
// MaxBuffers buffers of BufferSize bytes each.
type BufferPool struct {
	bufferSize int
	maxBuffers int
	slots      chan struct{}

	mu     sync.Mutex
	idle   [][]byte
	inUse  int
	peakIn int
}

// NewBufferPool creates a fixed-size buffer pool. Sharing a pool between
// uploads applies one aggregate memory bound to all of them.
func NewBufferPool(bufferSize, maxBuffers int) (*BufferPool, error) {
	if bufferSize <= 0 {
		return nil, fmt.Errorf("%w: buffer_size must be positive", ErrInvalidOptions)
	}
	if maxBuffers <= 0 {
		return nil, fmt.Errorf("%w: max_buffers must be positive", ErrInvalidOptions)
	}
	return &BufferPool{
		bufferSize: bufferSize,
		maxBuffers: maxBuffers,
		slots:      make(chan struct{}, maxBuffers),
	}, nil
}

// BufferSize returns the size of every buffer in the pool.
func (p *BufferPool) BufferSize() int {
	if p == nil {
		return 0
	}
	return p.bufferSize
}

// MaxBuffers returns the maximum number of simultaneously leased buffers.
func (p *BufferPool) MaxBuffers() int {
	if p == nil {
		return 0
	}
	return p.maxBuffers
}

// CapacityBytes returns the pool's hard memory capacity.
func (p *BufferPool) CapacityBytes() uint64 {
	if p == nil {
		return 0
	}
	return uint64(p.bufferSize) * uint64(p.maxBuffers)
}

// InUseBytes returns the number of bytes currently leased to uploads.
func (p *BufferPool) InUseBytes() uint64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return uint64(p.inUse) * uint64(p.bufferSize)
}

// PeakInUseBytes returns the largest number of simultaneously leased bytes.
func (p *BufferPool) PeakInUseBytes() uint64 {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return uint64(p.peakIn) * uint64(p.bufferSize)
}

func (p *BufferPool) acquire(ctx, uploadCtx context.Context) ([]byte, error) {
	select {
	case p.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-uploadCtx.Done():
		return nil, context.Cause(uploadCtx)
	}
	// A slot and cancellation may become ready together. Do not lease a new
	// buffer after the upload has already become terminal.
	if cause := context.Cause(uploadCtx); cause != nil {
		<-p.slots
		return nil, cause
	}
	if err := ctx.Err(); err != nil {
		<-p.slots
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var buffer []byte
	last := len(p.idle) - 1
	if last >= 0 {
		buffer = p.idle[last]
		p.idle[last] = nil
		p.idle = p.idle[:last]
	} else {
		buffer = make([]byte, p.bufferSize)
	}
	p.inUse++
	if p.inUse > p.peakIn {
		p.peakIn = p.inUse
	}
	return buffer[:p.bufferSize], nil
}

func (p *BufferPool) release(buffer []byte) {
	if buffer == nil {
		return
	}
	if cap(buffer) < p.bufferSize {
		panic("blob/sink/stream: released buffer is smaller than pool buffer size")
	}

	p.mu.Lock()
	p.idle = append(p.idle, buffer[:p.bufferSize])
	p.inUse--
	p.mu.Unlock()
	<-p.slots
}
