package writer

import "errors"

var (
	ErrInvalidOptions       = errors.New("writer: invalid options")
	ErrInvalidSession       = errors.New("writer: invalid session")
	ErrClosed               = errors.New("writer: writer closed")
	ErrAborted              = errors.New("writer: writer aborted")
	ErrTimestampOrder       = errors.New("writer: timestamp regression")
	ErrLSNExhausted         = errors.New("writer: lsn exhausted")
	ErrSegmentStartFailed   = errors.New("writer: segment start failed")
	ErrSegmentWriteFailed   = errors.New("writer: segment write failed")
	ErrStaleWriter          = errors.New("writer: stale writer")
	ErrPublishFailed        = errors.New("writer: publish failed")
	ErrPublishIndeterminate = errors.New("writer: publish outcome unknown")
	ErrInvalidPublishResult = errors.New("writer: invalid publish result")
)
