package catalog

import "errors"

var (
	ErrInvalidRequest = errors.New("catalog: invalid request")
	ErrInvalidSegment = errors.New("catalog: invalid segment")
	ErrConflict       = errors.New("catalog: conflict")
	ErrStaleWriter    = errors.New("catalog: stale writer")
	ErrTimestampOrder = errors.New("catalog: timestamp regression")
	ErrLSNExhausted   = errors.New("catalog: lsn exhausted")
)
