package reader

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidOptions = errors.New("partitionlog/reader: invalid options")
	ErrInvalidRequest = errors.New("partitionlog/reader: invalid request")
	ErrLSNExpired     = errors.New("partitionlog/reader: lsn expired")
	ErrLSNExhausted   = errors.New("partitionlog/reader: lsn exhausted")
	ErrCorruptData    = errors.New("partitionlog/reader: corrupt data")
	ErrStoreRead      = errors.New("partitionlog/reader: store read failed")
)

type LSNExpiredError struct {
	Requested uint64
	Oldest    uint64
	HeadNext  uint64
}

func (e LSNExpiredError) Error() string {
	return fmt.Sprintf("%v: requested=%d oldest=%d head_next=%d", ErrLSNExpired, e.Requested, e.Oldest, e.HeadNext)
}

func (e LSNExpiredError) Unwrap() error {
	return ErrLSNExpired
}
