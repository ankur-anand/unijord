package blob

import (
	"fmt"
	"sync"

	csession "github.com/ankur-anand/unijord/partitionlog/catalog"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
)

const pageVersion = 1

type Catalog struct {
	backend Backend
	opts    Options
}

func New(backend Backend, opts Options) (*Catalog, error) {
	if backend == nil {
		return nil, fmt.Errorf("%w: nil backend", csession.ErrInvalidRequest)
	}
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	return &Catalog{backend: backend, opts: normalized}, nil
}

func NewMemory(opts Options) (*Catalog, error) {
	return New(NewMemoryBackend(), opts)
}

type writerSession struct {
	cat *Catalog

	writerEpoch uint64
	writerID    [16]byte

	mu    sync.Mutex
	head  headFile
	token string
}

type headFile struct {
	Version                 int                `json:"version"`
	StreamID                string             `json:"stream_id,omitempty"`
	Partition               uint32             `json:"partition"`
	NextLSN                 uint64             `json:"next_lsn"`
	OldestLSN               uint64             `json:"oldest_lsn"`
	AppliedRetentionLSN     uint64             `json:"applied_retention_lsn,omitempty"`
	AppliedRetentionVersion uint64             `json:"applied_retention_version,omitempty"`
	WriterEpoch             uint64             `json:"writer_epoch"`
	WriterID                [16]byte           `json:"writer_id,omitempty"`
	SegmentCount            uint64             `json:"segment_count"`
	LastSegment             pmeta.SegmentRef   `json:"last_segment,omitempty"`
	HasLastSegment          bool               `json:"has_last_segment,omitempty"`
	IndexFrontier           []pageRef          `json:"index_frontier,omitempty"`
	LeafFrontier            *pageRef           `json:"leaf_frontier,omitempty"`
	ActiveSegments          []pmeta.SegmentRef `json:"active_segments,omitempty"`
	MaxIndexLevel           uint8              `json:"max_index_level,omitempty"`
	Generation              uint64             `json:"generation"`
}

type pageRef struct {
	Level             uint8  `json:"level,omitempty"`
	SeqLo             uint64 `json:"seq_lo"`
	SeqHi             uint64 `json:"seq_hi"`
	MinTimestampMS    int64  `json:"min_timestamp_ms"`
	MaxTimestampMS    int64  `json:"max_timestamp_ms"`
	HasTimestampRange bool   `json:"has_timestamp_range"`
	Generation        uint64 `json:"generation"`
	PageID            string `json:"page_id"`
	Path              string `json:"path"`
	Count             int    `json:"count"`
}

type leafPage struct {
	Version           int                `json:"version"`
	Type              string             `json:"type"`
	StreamID          string             `json:"stream_id,omitempty"`
	Partition         uint32             `json:"partition"`
	SeqLo             uint64             `json:"seq_lo"`
	SeqHi             uint64             `json:"seq_hi"`
	MinTimestampMS    int64              `json:"min_timestamp_ms"`
	MaxTimestampMS    int64              `json:"max_timestamp_ms"`
	HasTimestampRange bool               `json:"has_timestamp_range"`
	Generation        uint64             `json:"generation"`
	PageID            string             `json:"page_id,omitempty"`
	Segments          []pmeta.SegmentRef `json:"segments"`
}

type indexPage struct {
	Version           int       `json:"version"`
	Type              string    `json:"type"`
	Level             uint8     `json:"level"`
	StreamID          string    `json:"stream_id,omitempty"`
	Partition         uint32    `json:"partition"`
	SeqLo             uint64    `json:"seq_lo"`
	SeqHi             uint64    `json:"seq_hi"`
	MinTimestampMS    int64     `json:"min_timestamp_ms"`
	MaxTimestampMS    int64     `json:"max_timestamp_ms"`
	HasTimestampRange bool      `json:"has_timestamp_range"`
	Generation        uint64    `json:"generation"`
	PageID            string    `json:"page_id,omitempty"`
	Refs              []pageRef `json:"refs"`
}
