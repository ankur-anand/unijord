package segmentio

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var (
	errPayloadDestroyed = errors.New("segmentio: payload destroyed")
	errPayloadClosed    = errors.New("segmentio: payload builder closed")
)

type tempFilePayloadBuilder struct {
	file   *os.File
	path   string
	size   uint32
	closed bool
}

func beginTempFilePayload(dir string) (*tempFilePayloadBuilder, error) {
	file, err := os.CreateTemp(dir, "segmentio-block-*.payload")
	if err != nil {
		return nil, fmt.Errorf("create temp payload: %w", err)
	}
	return &tempFilePayloadBuilder{
		file: file,
		path: file.Name(),
	}, nil
}

func (b *tempFilePayloadBuilder) Write(p []byte) (int, error) {
	if b.closed {
		return 0, errPayloadClosed
	}
	n, err := b.file.Write(p)
	b.size += uint32(n)
	return n, err
}

func (b *tempFilePayloadBuilder) Size() uint32 {
	return b.size
}

func (b *tempFilePayloadBuilder) Finish() (*tempFilePayload, error) {
	if b.closed {
		return nil, errPayloadClosed
	}
	b.closed = true
	if err := b.file.Close(); err != nil {
		return nil, fmt.Errorf("close temp payload: %w", err)
	}
	return &tempFilePayload{
		path: b.path,
		size: b.size,
	}, nil
}

func (b *tempFilePayloadBuilder) Abort() error {
	if b.closed {
		return nil
	}
	b.closed = true
	errClose := b.file.Close()
	errRemove := os.Remove(b.path)
	if errClose != nil {
		return errClose
	}
	if errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
		return errRemove
	}
	return nil
}

type tempFilePayload struct {
	mu        sync.Mutex
	path      string
	size      uint32
	destroyed bool
}

func (p *tempFilePayload) Size() uint32 {
	return p.size
}

func (p *tempFilePayload) Open() (io.ReadCloser, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.destroyed {
		return nil, errPayloadDestroyed
	}
	file, err := os.Open(p.path)
	if err != nil {
		return nil, fmt.Errorf("open payload: %w", err)
	}
	return file, nil
}

func (p *tempFilePayload) Destroy() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.destroyed {
		return nil
	}
	p.destroyed = true
	if err := os.Remove(p.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("destroy payload: %w", err)
	}
	return nil
}
