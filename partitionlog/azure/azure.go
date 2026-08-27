// Package azure builds a complete partitionlog store on Azure Blob Storage.
package azure

import (
	"fmt"
	"path"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/ankur-anand/unijord/partitionlog/blob/lifecycle"
	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	azuresink "github.com/ankur-anand/unijord/partitionlog/blob/sink/azure"
	azuresource "github.com/ankur-anand/unijord/partitionlog/blob/source/azure"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	azurecatalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob/azure"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/reader"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

// Options configures a complete Azure-backed partitionlog store.
type Options struct {
	Container *container.Client

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
}

// Store wires Azure catalog metadata, segment writes, and segment reads.
type Store struct {
	catalog       *catalogblob.Catalog
	sink          *segmentsink.Factory
	source        *azuresource.Store
	admin         *azurecatalog.Backend
	streamID      string
	catalogPrefix string
}

// New builds a complete Azure-backed partitionlog store.
func New(opts Options) (*Store, error) {
	root := rootPrefix(opts.Prefix)
	streamID, err := normalizeStreamID(opts.StreamID)
	if err != nil {
		return nil, err
	}

	admin, err := azurecatalog.NewBackend(opts.Container)
	if err != nil {
		return nil, err
	}
	catPrefix := catalogPrefix(root, opts.CatalogPrefix)
	segmentPrefix := segmentRootPrefix(root, opts.SegmentRootPrefix)
	cat, err := catalogblob.New(admin, catalogblob.Options{
		Prefix:            catPrefix,
		SegmentRootPrefix: segmentPrefix,
		StreamID:          streamID,
	})
	if err != nil {
		return nil, err
	}

	multipartStore, err := azuresink.NewStore(opts.Container)
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

	source, err := azuresource.NewStore(opts.Container)
	if err != nil {
		return nil, err
	}

	return &Store{catalog: cat, sink: sinkFactory, source: source, admin: admin, streamID: streamID, catalogPrefix: catPrefix}, nil
}

func normalizeStreamID(streamID string) (string, error) {
	streamID, err := keylayout.CanonicalStreamID(streamID)
	if err != nil {
		return "", fmt.Errorf("partitionlog/azure: %w", err)
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
