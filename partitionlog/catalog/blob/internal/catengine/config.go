package catengine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

const (
	defaultCatalogPrefix = "catalog"
	defaultDataRoot      = "partitionlog"
)

type Config struct {
	CatalogPrefix string
	DataRoot      string
	StreamID      string
	Partition     uint32
	LeafLimit     uint32
	IndexLimit    uint32
	HashAlgo      segformat.HashAlgo

	StreamKey   [32]byte
	DataRootKey [32]byte
}

func NewConfig(catalogPrefix, dataRoot, streamID string, partition uint32, leafLimit, indexLimit uint32, hashAlgo segformat.HashAlgo) (Config, error) {
	catalogPrefix = strings.Trim(catalogPrefix, "/")
	if catalogPrefix == "" {
		catalogPrefix = defaultCatalogPrefix
	}
	if streamID != "" {
		canonical, err := keylayout.CanonicalStreamID(streamID)
		if err != nil {
			return Config{}, err
		}
		streamID = canonical
	}
	if leafLimit == 0 || leafLimit > catformat.MaxLeafEntries {
		return Config{}, fmt.Errorf("catengine: leaf limit=%d outside [1,%d]", leafLimit, catformat.MaxLeafEntries)
	}
	if indexLimit < 2 || indexLimit > catformat.MaxIndexEntries {
		return Config{}, fmt.Errorf("catengine: index limit=%d outside [2,%d]", indexLimit, catformat.MaxIndexEntries)
	}
	if err := hashAlgo.Validate(); err != nil {
		return Config{}, fmt.Errorf("catengine: %w", err)
	}
	dataRoot = strings.Trim(dataRoot, "/")
	if dataRoot == "" {
		dataRoot = defaultDataRoot
	}
	return Config{
		CatalogPrefix: catalogPrefix,
		DataRoot:      dataRoot,
		StreamID:      streamID,
		Partition:     partition,
		LeafLimit:     leafLimit,
		IndexLimit:    indexLimit,
		HashAlgo:      hashAlgo,
		StreamKey:     sha256.Sum256([]byte(streamID)),
		DataRootKey:   sha256.Sum256([]byte(dataRoot)),
	}, nil
}

func (c Config) HeadPath() string {
	return c.partitionPrefix() + "/head.plc"
}

func (c Config) LeafPagePath(ref catformat.IndexEntry) string {
	return fmt.Sprintf(
		"%s/pages/l00/leaf-%020d-%020d-%020d-%s.plc",
		c.partitionPrefix(), ref.SeqHi, ref.SeqLo, ref.Generation, hex.EncodeToString(ref.PageID[:]),
	)
}

func (c Config) IndexPagePath(ref catformat.IndexEntry) string {
	return fmt.Sprintf(
		"%s/pages/l%02d/index-l%02d-%020d-%020d-%020d-%s.plc",
		c.partitionPrefix(), ref.Level, ref.Level, ref.SeqHi, ref.SeqLo, ref.Generation, hex.EncodeToString(ref.PageID[:]),
	)
}

func (c Config) PagePath(ref catformat.IndexEntry) string {
	if ref.Level == 0 {
		return c.LeafPagePath(ref)
	}
	return c.IndexPagePath(ref)
}

func (c Config) SegmentURI(entry catformat.LeafEntry) string {
	return keylayout.SegmentObjectKey(c.DataRoot, c.StreamID, c.Partition, entry.BaseLSN, entry.WriterEpoch, entry.SegmentUUID)
}

func (c Config) DecodeHead(body []byte) (catformat.Head, error) {
	head, err := catformat.ParseHead(body)
	if err != nil {
		return catformat.Head{}, fmt.Errorf("%w: decode head: %w", ErrCorruptCatalog, err)
	}
	if err := validateConfigHead(c, head); err != nil {
		return catformat.Head{}, fmt.Errorf("%w: decode head: %w", ErrCorruptCatalog, err)
	}
	return head, nil
}

func (c Config) PartitionHead(head catformat.Head) (pmeta.PartitionHead, error) {
	if err := validateConfigHead(c, head); err != nil {
		return pmeta.PartitionHead{}, err
	}
	state := pmeta.PartitionHead{
		StreamID:                c.StreamID,
		Partition:               c.Partition,
		NextLSN:                 head.Header.NextLSN,
		OldestLSN:               head.Header.OldestLSN,
		AppliedRetentionLSN:     head.Header.AppliedRetentionLSN,
		AppliedRetentionVersion: head.Header.AppliedRetentionVersion,
		WriterEpoch:             head.Header.WriterEpoch,
		SegmentCount:            head.Header.SegmentCount,
		ReachableSegmentCount:   head.Header.ReachableSegmentCount,
		HasLastSegment:          head.HasLastSegment(),
	}
	if state.HasLastSegment {
		state.LastSegment = c.segmentRef(head.LastSegment)
	}
	return state, nil
}

func (c Config) leafEntry(segment pmeta.SegmentRef) (catformat.LeafEntry, error) {
	if err := segment.Validate(); err != nil {
		return catformat.LeafEntry{}, fmt.Errorf("catengine: %w", err)
	}
	if segment.StreamID != c.StreamID || segment.Partition != c.Partition {
		return catformat.LeafEntry{}, fmt.Errorf("catengine: segment identity stream=%q partition=%d, want stream=%q partition=%d", segment.StreamID, segment.Partition, c.StreamID, c.Partition)
	}
	if segment.WriterTag == ([16]byte{}) {
		return catformat.LeafEntry{}, fmt.Errorf("catengine: segment has empty writer tag")
	}
	entry := catformat.LeafEntry{
		BaseLSN:          segment.BaseLSN,
		LastLSN:          segment.LastLSN,
		MinTimestampMS:   segment.MinTimestampMS,
		MaxTimestampMS:   segment.MaxTimestampMS,
		RecordCount:      segment.RecordCount,
		BlockCount:       segment.BlockCount,
		SizeBytes:        segment.SizeBytes,
		BlockIndexOffset: segment.BlockIndexOffset,
		BlockIndexLength: segment.BlockIndexLength,
		Codec:            segment.Codec,
		HashAlgo:         segment.HashAlgo,
		SegmentHash:      segment.SegmentHash,
		TrailerHash:      segment.TrailerHash,
		WriterEpoch:      segment.WriterEpoch,
		SegmentUUID:      segment.SegmentUUID,
		WriterTag:        segment.WriterTag,
	}
	if want := c.SegmentURI(entry); segment.URI != want {
		return catformat.LeafEntry{}, fmt.Errorf("catengine: segment URI=%q, want derived URI=%q", segment.URI, want)
	}
	return entry, nil
}

func (c Config) segmentRef(entry catformat.LeafEntry) pmeta.SegmentRef {
	return pmeta.SegmentRef{
		URI:              c.SegmentURI(entry),
		StreamID:         c.StreamID,
		Partition:        c.Partition,
		WriterEpoch:      entry.WriterEpoch,
		SegmentUUID:      entry.SegmentUUID,
		WriterTag:        entry.WriterTag,
		BaseLSN:          entry.BaseLSN,
		LastLSN:          entry.LastLSN,
		MinTimestampMS:   entry.MinTimestampMS,
		MaxTimestampMS:   entry.MaxTimestampMS,
		RecordCount:      entry.RecordCount,
		BlockCount:       entry.BlockCount,
		SizeBytes:        entry.SizeBytes,
		BlockIndexOffset: entry.BlockIndexOffset,
		BlockIndexLength: entry.BlockIndexLength,
		Codec:            entry.Codec,
		HashAlgo:         entry.HashAlgo,
		SegmentHash:      entry.SegmentHash,
		TrailerHash:      entry.TrailerHash,
	}
}

func (c Config) partitionPrefix() string {
	parts := []string{c.CatalogPrefix, keylayout.Bucket(c.StreamID, c.Partition)}
	if c.StreamID != "" {
		parts = append(parts, "streams", keylayout.StreamKey(c.StreamID))
	}
	parts = append(parts, fmt.Sprintf("p%08d", c.Partition))
	return strings.Join(parts, "/")
}
