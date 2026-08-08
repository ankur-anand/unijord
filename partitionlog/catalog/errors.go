package catalog

import "errors"

var (
	ErrInvalidRequest       = errors.New("catalog: invalid request")
	ErrInvalidSegment       = errors.New("catalog: invalid segment")
	ErrConflict             = errors.New("catalog: conflict")
	ErrStaleWriter          = errors.New("catalog: stale writer")
	ErrFenceExhausted       = errors.New("catalog: writer fence exhausted")
	ErrFenceIndeterminate   = errors.New("catalog: writer fence outcome unknown")
	ErrGenerationExhausted  = errors.New("catalog: generation exhausted")
	ErrCommitIndeterminate  = errors.New("catalog: commit outcome unknown")
	ErrTimestampOrder       = errors.New("catalog: timestamp regression")
	ErrLSNExhausted         = errors.New("catalog: lsn exhausted")
	ErrRetentionRegression  = errors.New("catalog: retention regression")
	ErrRetentionUnsupported = errors.New("catalog: retention unsupported")
)
