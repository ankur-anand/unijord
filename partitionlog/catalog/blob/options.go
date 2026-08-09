package blob

import (
	"fmt"
	"time"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
)

const (
	DefaultIndexRefLimit               = 1024
	DefaultWriterAcquireMaxAttempts    = 32
	DefaultWriterAcquireInitialBackoff = 2 * time.Millisecond
	DefaultWriterAcquireMaxBackoff     = 100 * time.Millisecond
	DefaultWriterCommitMaxAttempts     = 3
	DefaultWriterCommitInitialBackoff  = 5 * time.Millisecond
	DefaultWriterCommitMaxBackoff      = 50 * time.Millisecond
)

type Options struct {
	Prefix string
	// StreamID scopes this catalog instance to one stream. Non-empty values are
	// included in object keys and in committed metadata.
	StreamID string

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

	// WriterCommitMaxAttempts bounds retries of one head CAS. Every attempt
	// reuses the same immutable pages, head bytes, and expected segment.
	WriterCommitMaxAttempts int

	// WriterCommitInitialBackoff is the first sleep after an ambiguous head
	// CAS failure. Later attempts use exponential backoff.
	WriterCommitInitialBackoff time.Duration

	// WriterCommitMaxBackoff caps head commit retry sleep.
	WriterCommitMaxBackoff time.Duration
}

func normalizeOptions(opts Options) (Options, error) {
	opts.Prefix = normalizePrefix(opts.Prefix)
	if opts.StreamID != "" {
		streamID, err := keylayout.CanonicalStreamID(opts.StreamID)
		if err != nil {
			return Options{}, fmt.Errorf("%w: %w", csession.ErrInvalidRequest, err)
		}
		opts.StreamID = streamID
	}
	switch {
	case opts.LeafSegmentLimit <= 0:
		opts.LeafSegmentLimit = csession.DefaultSegmentPageLimit
	case opts.LeafSegmentLimit > csession.MaxSegmentPageLimit:
		return Options{}, fmt.Errorf("%w: leaf segment limit=%d max=%d", csession.ErrInvalidRequest, opts.LeafSegmentLimit, csession.MaxSegmentPageLimit)
	}
	switch {
	case opts.IndexRefLimit <= 0:
		opts.IndexRefLimit = DefaultIndexRefLimit
	case opts.IndexRefLimit > csession.MaxSegmentPageLimit:
		return Options{}, fmt.Errorf("%w: index ref limit=%d max=%d", csession.ErrInvalidRequest, opts.IndexRefLimit, csession.MaxSegmentPageLimit)
	}
	if opts.WriterAcquireMaxAttempts <= 0 {
		opts.WriterAcquireMaxAttempts = DefaultWriterAcquireMaxAttempts
	}
	if opts.WriterAcquireInitialBackoff < 0 {
		return Options{}, fmt.Errorf("%w: negative writer acquire initial backoff", csession.ErrInvalidRequest)
	}
	if opts.WriterAcquireInitialBackoff == 0 {
		opts.WriterAcquireInitialBackoff = DefaultWriterAcquireInitialBackoff
	}
	if opts.WriterAcquireMaxBackoff < 0 {
		return Options{}, fmt.Errorf("%w: negative writer acquire max backoff", csession.ErrInvalidRequest)
	}
	if opts.WriterAcquireMaxBackoff == 0 {
		opts.WriterAcquireMaxBackoff = DefaultWriterAcquireMaxBackoff
	}
	if opts.WriterAcquireMaxBackoff < opts.WriterAcquireInitialBackoff {
		return Options{}, fmt.Errorf("%w: writer acquire max backoff %s below initial backoff %s", csession.ErrInvalidRequest, opts.WriterAcquireMaxBackoff, opts.WriterAcquireInitialBackoff)
	}
	if opts.WriterCommitMaxAttempts <= 0 {
		opts.WriterCommitMaxAttempts = DefaultWriterCommitMaxAttempts
	}
	if opts.WriterCommitInitialBackoff < 0 {
		return Options{}, fmt.Errorf("%w: negative writer commit initial backoff", csession.ErrInvalidRequest)
	}
	if opts.WriterCommitInitialBackoff == 0 {
		opts.WriterCommitInitialBackoff = DefaultWriterCommitInitialBackoff
	}
	if opts.WriterCommitMaxBackoff < 0 {
		return Options{}, fmt.Errorf("%w: negative writer commit max backoff", csession.ErrInvalidRequest)
	}
	if opts.WriterCommitMaxBackoff == 0 {
		opts.WriterCommitMaxBackoff = DefaultWriterCommitMaxBackoff
	}
	if opts.WriterCommitMaxBackoff < opts.WriterCommitInitialBackoff {
		return Options{}, fmt.Errorf("%w: writer commit max backoff %s below initial backoff %s", csession.ErrInvalidRequest, opts.WriterCommitMaxBackoff, opts.WriterCommitInitialBackoff)
	}
	return opts, nil
}
