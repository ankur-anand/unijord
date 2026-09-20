package isledb

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/cockroachdb/pebble/v2/objstorage"
)

type stagedCompactionSST struct {
	file *os.File
	path string
	size int64
	once sync.Once
	err  error
}

func (s *stagedCompactionSST) ReadAt(ctx context.Context, p []byte, off int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s == nil || s.file == nil || off < 0 || off > s.size || int64(len(p)) > s.size-off {
		return io.ErrUnexpectedEOF
	}
	n, err := s.file.ReadAt(p, off)
	if err != nil {
		return err
	}
	if n != len(p) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (s *stagedCompactionSST) Size() int64 {
	if s == nil {
		return 0
	}
	return s.size
}

func (s *stagedCompactionSST) NewReadHandle(_ objstorage.ReadBeforeSize) objstorage.ReadHandle {
	h := objstorage.MakeNoopReadHandle(s)
	return &h
}

func (s *stagedCompactionSST) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		if s.file != nil {
			s.err = s.file.Close()
			s.file = nil
		}
		if s.path != "" {
			err := os.Remove(s.path)
			if errors.Is(err, os.ErrNotExist) {
				err = nil
			}
			s.err = errors.Join(s.err, err)
			s.path = ""
		}
	})
	return s.err
}
