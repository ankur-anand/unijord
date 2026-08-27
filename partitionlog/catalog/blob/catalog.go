package blob

import (
	"fmt"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catengine"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type Catalog struct {
	backend Backend
	opts    Options
	engine  *catengine.Engine
}

func New(backend Backend, opts Options) (*Catalog, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: nil backend", csession.ErrInvalidRequest)
	}
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	engine, err := catengine.NewEngine(backend, catengine.EngineOptions{
		CatalogPrefix:         normalized.Prefix,
		DataRoot:              normalized.SegmentRootPrefix,
		StreamID:              normalized.StreamID,
		LeafLimit:             uint32(normalized.LeafSegmentLimit),
		IndexLimit:            uint32(normalized.IndexRefLimit),
		HashAlgo:              segformat.HashXXH64,
		AcquireAttempts:       normalized.WriterAcquireMaxAttempts,
		AcquireInitialBackoff: normalized.WriterAcquireInitialBackoff,
		AcquireMaxBackoff:     normalized.WriterAcquireMaxBackoff,
		CommitAttempts:        normalized.WriterCommitMaxAttempts,
		CommitInitialBackoff:  normalized.WriterCommitInitialBackoff,
		CommitMaxBackoff:      normalized.WriterCommitMaxBackoff,
	})
	if err != nil {
		return nil, err
	}
	return &Catalog{backend: backend, opts: normalized, engine: engine}, nil
}

func NewMemory(opts Options) (*Catalog, error) {
	return New(NewMemoryBackend(), opts)
}
