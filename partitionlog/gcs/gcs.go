// Package gcs builds a complete partitionlog store on Google Cloud Storage.
package gcs

import (
	"fmt"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/ankur-anand/unijord/partitionlog/blob/lifecycle"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	gcssink "github.com/ankur-anand/unijord/partitionlog/blob/sink/gcs"
	gcssource "github.com/ankur-anand/unijord/partitionlog/blob/source/gcs"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	gcscatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob/gcs"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/reader"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

// Options configures a complete GCS-backed partitionlog store.
type Options struct {
	Client *storage.Client
	Bucket string

	// Prefix is the common object key root. Catalog metadata defaults to
	// <prefix>/catalog and segment objects default to <prefix>/segments.
	Prefix string

	// StreamID scopes catalog metadata and segment object keys to one stream.
	StreamID string

	// CatalogPrefix overrides the catalog metadata prefix.
	CatalogPrefix string

	// SegmentRootPrefix overrides the segment sink root. Final segment objects
	// are placed under <segment-root>/segments.
	SegmentRootPrefix string

	// SegmentContentType is attached to committed segment objects.
	SegmentContentType string

	// CatalogLeafSegmentLimit and CatalogIndexRefLimit override the catalog
	// page fan-out. Zero keeps the catalog defaults. They are writer-side
	// settings; readers follow page references regardless of the values.
	CatalogLeafSegmentLimit int
	CatalogIndexRefLimit    int
}

// Store wires GCS catalog metadata, segment writes, and segment reads.
type Store struct {
	catalog       *catalogblob.Catalog
	sink          *segmentsink.Factory
	source        *gcssource.Store
	admin         *gcscatalog.Backend
	streamID      string
	catalogPrefix string
}

// New builds a complete GCS-backed partitionlog store.
func New(opts Options) (*Store, error) {
	root := rootPrefix(opts.Prefix)
	streamID, err := normalizeStreamID(opts.StreamID)
	if err != nil {
		return nil, err
	}

	admin, err := gcscatalog.NewBackend(opts.Client, opts.Bucket)
	if err != nil {
		return nil, err
	}
	catPrefix := catalogPrefix(root, opts.CatalogPrefix)
	segmentPrefix := segmentRootPrefix(root, opts.SegmentRootPrefix)
	cat, err := catalogblob.New(admin, catalogblob.Options{
		Prefix:            catPrefix,
		SegmentRootPrefix: segmentPrefix,
		StreamID:          streamID,
		LeafSegmentLimit:  opts.CatalogLeafSegmentLimit,
		IndexRefLimit:     opts.CatalogIndexRefLimit,
	})
	if err != nil {
		return nil, err
	}

	multipartStore, err := gcssink.NewStore(opts.Client, opts.Bucket)
	if err != nil {
		return nil, err
	}
	sinkFactory, err := segmentsink.New(multipartStore, segmentsink.Options{
		Prefix:      segmentPrefix,
		ContentType: opts.SegmentContentType,
	})
	if err != nil {
		return nil, err
	}

	source, err := gcssource.NewStore(opts.Client, opts.Bucket)
	if err != nil {
		return nil, err
	}

	return &Store{catalog: cat, sink: sinkFactory, source: source, admin: admin, streamID: streamID, catalogPrefix: catPrefix}, nil
}

func normalizeStreamID(streamID string) (string, error) {
	streamID, err := keylayout.CanonicalStreamID(streamID)
	if err != nil {
		return "", fmt.Errorf("partitionlog/gcs: %w", err)
	}
	return streamID, nil
}

func (s *Store) WriterManager() catalog.WriterManager {
	return s.catalog
}

func (s *Store) RetentionManager() catalog.RetentionManager {
	return s.catalog
}

func (s *Store) ReaderCatalog() catalog.Reader {
	return s.catalog
}

func (s *Store) SinkFactory() writer.SinkFactory {
	return s.sink
}

func (s *Store) SegmentStore() reader.SegmentStore {
	return s.source
}

// NewReclaimer creates an explicitly scheduled object lifecycle worker.
func (s *Store) NewReclaimer(opts lifecycle.Options) (*lifecycle.Reclaimer, error) {
	opts.StreamID = s.streamID
	opts.CatalogPrefix = s.catalogPrefix
	return lifecycle.New(s.admin, s.catalog, s.sink.Layout(), opts)
}

func rootPrefix(prefix string) string {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return segmentsink.DefaultPrefix
	}
	return prefix
}

func catalogPrefix(root string, override string) string {
	override = strings.Trim(override, "/")
	if override != "" {
		return override
	}
	return path.Join(root, "catalog")
}

func segmentRootPrefix(root string, override string) string {
	override = strings.Trim(override, "/")
	if override != "" {
		return override
	}
	return root
}
