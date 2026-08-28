package segwriter

import "errors"

var (
	ErrInvalidOptions         = errors.New("segwriter: invalid options")
	ErrWriterClosed           = errors.New("segwriter: writer closed")
	ErrWriterAborted          = errors.New("segwriter: writer aborted")
	ErrAppendNotAccepted      = errors.New("segwriter: append canceled before record acceptance")
	ErrEmptySegment           = errors.New("segwriter: empty segment")
	ErrNonContiguousLSN       = errors.New("segwriter: non-contiguous lsn")
	ErrLSNExhausted           = errors.New("segwriter: lsn range exhausted")
	ErrTimestampOrder         = errors.New("segwriter: timestamp regression")
	ErrPackerClosed           = errors.New("segwriter: packer closed")
	ErrPackerAborted          = errors.New("segwriter: packer aborted")
	ErrBodySealed             = errors.New("segwriter: body already sealed")
	ErrBodyNotSealed          = errors.New("segwriter: body not sealed")
	ErrEmptyObject            = errors.New("segwriter: empty object")
	ErrSinkContract           = errors.New("segwriter: sink contract violation")
	ErrTxnAborted             = errors.New("segwriter: transaction aborted")
	ErrTxnCompleted           = errors.New("segwriter: transaction completed")
	ErrTxnCommitIndeterminate = errors.New("segwriter: transaction commit outcome unknown")
)
