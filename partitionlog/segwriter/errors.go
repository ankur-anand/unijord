package segwriter

import "errors"

var (
	ErrInvalidOptions   = errors.New("segwriter: invalid options")
	ErrWriterClosed     = errors.New("segwriter: writer closed")
	ErrWriterAborted    = errors.New("segwriter: writer aborted")
	ErrEmptySegment     = errors.New("segwriter: empty segment")
	ErrNonContiguousLSN = errors.New("segwriter: non-contiguous lsn")
	ErrTimestampOrder   = errors.New("segwriter: timestamp regression")
	ErrPackerClosed     = errors.New("segwriter: packer closed")
	ErrPackerAborted    = errors.New("segwriter: packer aborted")
	ErrBodySealed       = errors.New("segwriter: body already sealed")
	ErrBodyNotSealed    = errors.New("segwriter: body not sealed")
	ErrEmptyObject      = errors.New("segwriter: empty object")
)
