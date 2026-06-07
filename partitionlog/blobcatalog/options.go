package blobcatalog

import "time"

const (
	DefaultIndexRefLimit               = 1024
	DefaultWriterAcquireMaxAttempts    = 32
	DefaultWriterAcquireInitialBackoff = 2 * time.Millisecond
	DefaultWriterAcquireMaxBackoff     = 100 * time.Millisecond
)

type Options struct {
	Prefix string

	// LeafSegmentLimit bounds SegmentRef entries per leaf page.
	LeafSegmentLimit int

	// IndexRefLimit bounds refs per index page at every index level.
	IndexRefLimit int

	// WriterAcquireMaxAttempts bounds OpenWriter CAS retries before returning
	// catalog.ErrConflict.
	WriterAcquireMaxAttempts int

	// WriterAcquireInitialBackoff is the first sleep after an OpenWriter CAS
	// conflict. Later conflicts use exponential backoff capped by
	// WriterAcquireMaxBackoff.
	WriterAcquireInitialBackoff time.Duration

	// WriterAcquireMaxBackoff caps OpenWriter CAS retry sleep.
	WriterAcquireMaxBackoff time.Duration
}
