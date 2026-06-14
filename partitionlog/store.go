package partitionlog

import (
	"github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/reader"
	"github.com/ankur-anand/unijord/partitionlog/writer"
)

// Store is a complete partitionlog backend.
//
// Provider packages such as partitionlog/s3, partitionlog/gcs, and
// partitionlog/azure implement this interface by wiring together a catalog,
// segment sink, and segment source.
type Store interface {
	WriterManager() catalog.WriterManager
	ReaderCatalog() catalog.Reader
	SinkFactory() writer.SinkFactory
	SegmentStore() reader.SegmentStore
}
