// Package s3 builds a complete partitionlog store on S3-compatible object
// storage.
package s3

import (
	"fmt"
	"path"
	"strings"

	segmentsink "github.com/ankur-anand/unijord/partitionlog/blob/sink"
	s3sink "github.com/ankur-anand/unijord/partitionlog/blob/sink/s3"
	s3source "github.com/ankur-anand/unijord/partitionlog/blob/source/s3"
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	catalogblob "github.com/ankur-anand/unijord/partitionlog/catalog/blob"
	s3catalog "github.com/ankur-anand/unijord/partitionlog/catalog/blob/s3"
	"github.com/ankur-anand/unijord/partitionlog/reader"
	"github.com/ankur-anand/unijord/partitionlog/writer"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// Options configures a complete S3-backed partitionlog store.
type Options struct {
	Client *awss3.Client
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
}

// Store wires S3 catalog metadata, segment writes, and segment reads.
type Store struct {
	catalog *catalogblob.Catalog
	sink    *segmentsink.Factory
	source  *s3source.Store
}

// New builds a complete S3-backed partitionlog store.
func New(opts Options) (*Store, error) {
	root := rootPrefix(opts.Prefix)
	streamID, err := normalizeStreamID(opts.StreamID)
	if err != nil {
		return nil, err
	}

	cat, err := s3catalog.New(opts.Client, opts.Bucket, s3catalog.Options{
		Prefix:   catalogPrefix(root, opts.CatalogPrefix),
		StreamID: streamID,
	})
	if err != nil {
		return nil, err
	}

	multipartStore, err := s3sink.NewStore(opts.Client, opts.Bucket)
	if err != nil {
		return nil, err
	}
	sinkFactory, err := segmentsink.New(multipartStore, segmentsink.Options{
		Prefix:      segmentRootPrefix(root, opts.SegmentRootPrefix),
		ContentType: opts.SegmentContentType,
	})
	if err != nil {
		return nil, err
	}

	source, err := s3source.NewStore(opts.Client, opts.Bucket)
	if err != nil {
		return nil, err
	}

	return &Store{catalog: cat, sink: sinkFactory, source: source}, nil
}

func normalizeStreamID(streamID string) (string, error) {
	streamID = strings.Trim(streamID, "/")
	if streamID == "" {
		return "", fmt.Errorf("partitionlog/s3: empty stream_id")
	}
	return streamID, nil
}

func (s *Store) WriterManager() catalog.WriterManager {
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
