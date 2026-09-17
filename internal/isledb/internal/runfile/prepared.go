package runfile

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// PreparedRun owns immutable framing and three scratch regions. WriteTo always
// starts at byte zero, including after a failed or canceled write. The caller
// must use a fresh/reset destination on retry and call Close when finished.
// Ref is available after preparation; publication requires a successful write.
//
// Methods are safe concurrently and serialized, bounding streaming memory to
// one 128 KiB buffer per prepared run. Close waits for an active write; it does
// not cancel that write. A destination must not call back into this object.
// After Close, WriteTo returns os.ErrClosed and Ref returns the zero value.
// Repeated Close calls return the original cleanup result.
type PreparedRun interface {
	Ref() Ref
	WriteTo(context.Context, io.Writer) error
	Close() error
}

type preparedRegion struct {
	file           *os.File
	path           string
	offset, length uint64
}

type preparedRun struct {
	mu                           sync.Mutex
	closed                       bool
	closeErr                     error
	ref                          Ref
	preamble, directory, trailer []byte
	directoryOffset              uint64
	regions                      [3]preparedRegion
}

func (p *preparedRun) Ref() Ref {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return Ref{}
	}
	r := p.ref
	r.MinTimeline, r.MaxTimeline = bytes.Clone(r.MinTimeline), bytes.Clone(r.MaxTimeline)
	r.Events, r.Heads = cloneRegion(r.Events), cloneRegion(r.Heads)
	filter := cloneFilterRef(*r.TimelineFilter)
	r.TimelineFilter = &filter
	return r
}

func (p *preparedRun) WriteTo(ctx context.Context, dst io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	if ctx == nil || dst == nil {
		return invalidRunf("nil write context or destination")
	}
	if err := p.writePayload(ctx, dst); err != nil {
		return err
	}
	if err := writeContext(ctx, dst, p.trailer); err != nil {
		return fmt.Errorf("runfile: write trailer: %w", err)
	}
	return ctx.Err()
}

func (p *preparedRun) writePayload(ctx context.Context, dst io.Writer) error {
	payload := newPayloadWriter(dst)
	if err := payload.write(ctx, p.preamble); err != nil {
		return err
	}
	buffer := make([]byte, 128<<10)
	for i, region := range p.regions {
		if err := streamRegion(ctx, payload, region, buffer); err != nil {
			return fmt.Errorf("runfile: write region %d: %w", i+1, err)
		}
	}
	if err := writePaddingTo(ctx, payload, p.directoryOffset); err != nil {
		return err
	}
	if err := payload.write(ctx, p.directory); err != nil {
		return fmt.Errorf("runfile: write directory: %w", err)
	}
	if payload.size != p.directoryOffset+uint64(len(p.directory)) {
		return invalidRunf("prepared payload size mismatch")
	}
	return ctx.Err()
}

func (p *preparedRun) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		for _, region := range p.regions {
			p.closeErr = errors.Join(p.closeErr, closeScratch(region.file, region.path))
		}
		p.regions = [3]preparedRegion{}
		p.ref = Ref{}
		p.preamble, p.directory, p.trailer = nil, nil, nil
	}
	return p.closeErr
}

func closeScratch(file *os.File, path string) error {
	var err error
	if file != nil {
		err = file.Close()
	}
	if path != "" {
		removeErr := os.Remove(path)
		if !errors.Is(removeErr, os.ErrNotExist) {
			err = errors.Join(err, removeErr)
		}
	}
	return err
}
