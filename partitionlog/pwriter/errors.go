package pwriter

import "errors"

var (
	ErrInvalidOptions       = errors.New("pwriter: invalid options")
	ErrInvalidSession       = errors.New("pwriter: invalid session")
	ErrClosed               = errors.New("pwriter: writer closed")
	ErrAborted              = errors.New("pwriter: writer aborted")
	ErrTimestampOrder       = errors.New("pwriter: timestamp regression")
	ErrLSNExhausted         = errors.New("pwriter: lsn exhausted")
	ErrSegmentWriteFailed   = errors.New("pwriter: segment write failed")
	ErrStaleWriter          = errors.New("pwriter: stale writer")
	ErrPublishFailed        = errors.New("pwriter: publish failed")
	ErrPublishIndeterminate = errors.New("pwriter: publish outcome unknown")
	ErrInvalidPublishResult = errors.New("pwriter: invalid publish result")
)
