package pwriter

import "errors"

var (
	ErrInvalidOptions = errors.New("pwriter: invalid options")
	ErrClosed         = errors.New("pwriter: writer closed")
	ErrAborted        = errors.New("pwriter: writer aborted")
	ErrTimestampOrder = errors.New("pwriter: timestamp regression")
)
