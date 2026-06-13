package segreader

import "errors"

var (
	ErrInvalidOptions = errors.New("partitionlog/segreader: invalid options")
	ErrInvalidSegment = errors.New("partitionlog/segreader: invalid segment")
	ErrStoreRead      = errors.New("partitionlog/segreader: store read failed")
	ErrCorruptData    = errors.New("partitionlog/segreader: corrupt data")
)
