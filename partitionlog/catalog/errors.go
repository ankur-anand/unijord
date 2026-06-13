package catalog

import "errors"

var (
	ErrInvalidRequest = errors.New("catalogsession: invalid request")
	ErrInvalidSegment = errors.New("catalogsession: invalid segment")
	ErrConflict       = errors.New("catalogsession: conflict")
	ErrStaleWriter    = errors.New("catalogsession: stale writer")
	ErrTimestampOrder = errors.New("catalogsession: timestamp regression")
	ErrLSNExhausted   = errors.New("catalogsession: lsn exhausted")
)
