package metastore

import "errors"

var (
	ErrInvalidRequest        = errors.New("metastore: invalid request")
	ErrNotFound              = errors.New("metastore: not found")
	ErrConflict              = errors.New("metastore: conditional conflict")
	ErrStaleWriter           = errors.New("metastore: stale shard writer")
	ErrSealed                = errors.New("metastore: timeline sealed")
	ErrTailFull              = errors.New("metastore: active tail requires materialization")
	ErrCorrupt               = errors.New("metastore: corrupt state")
	ErrOutcomeUnknown        = errors.New("metastore: commit outcome unknown")
	ErrStaleProducer         = errors.New("metastore: stale producer")
	ErrProducerSequenceGap   = errors.New("metastore: producer sequence gap")
	ErrProducerReplayExpired = errors.New("metastore: producer replay expired")
	ErrStaleSource           = errors.New("metastore: stale source owner")
	ErrSourcePosition        = errors.New("metastore: source position conflict")
	ErrSourceDataLost        = errors.New("metastore: required source data is unavailable")
)
