package segformat

import "errors"

var (
	ErrInvalidSegment      = errors.New("segformat: invalid segment")
	ErrUnsupportedVersion  = errors.New("segformat: unsupported version")
	ErrUnsupportedCodec    = errors.New("segformat: unsupported codec")
	ErrUnsupportedHashAlgo = errors.New("segformat: unsupported hash algorithm")
	ErrUnsupportedRecord   = errors.New("segformat: unsupported record format")
	ErrRecordTooLarge      = errors.New("segformat: record too large")
	ErrBlockTooLarge       = errors.New("segformat: block too large")
	ErrIntegrityMismatch   = errors.New("segformat: integrity mismatch")
	ErrBlockIndexMismatch  = errors.New("segformat: block index mismatch")
)
