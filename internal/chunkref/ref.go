// Package chunkref defines the authenticated publication reference for one
// immutable UJTC object.
package chunkref

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/ankur-anand/unijord/internal/chunkfile"
	"github.com/ankur-anand/unijord/internal/record"
)

var (
	ErrInvalidRef = errors.New("chunkref: invalid reference")
	ErrMismatch   = errors.New("chunkref: object mismatch")
)

// Ref is the complete catalog-independent identity of one UJTC object. It is
// comparable and safe to use as an idempotency value.
//
// Per-timeline LSN spans are deliberately absent. They describe how a
// timeline uses the object and belong to the publishing index, not the chunk.
type Ref struct {
	Key            string   `json:"key"`
	FormatVersion  uint16   `json:"format_version"`
	NamespaceHash  [32]byte `json:"namespace_hash"`
	Shard          uint32   `json:"shard"`
	WriterEpoch    uint64   `json:"writer_epoch"`
	Sequence       uint64   `json:"sequence"`
	RecordCount    uint32   `json:"record_count"`
	TimelineCount  uint32   `json:"timeline_count"`
	SizeBytes      uint64   `json:"size_bytes"`
	MinTimestampMS int64    `json:"min_timestamp_ms"`
	MaxTimestampMS int64    `json:"max_timestamp_ms"`
	SHA256         [32]byte `json:"object_hash"`
}

// FromMetadata constructs the only valid reference for key and metadata.
func FromMetadata(key string, metadata chunkfile.Metadata) (Ref, error) {
	ref := Ref{
		Key:            key,
		FormatVersion:  chunkfile.Version,
		NamespaceHash:  metadata.NamespaceHash,
		Shard:          metadata.Shard,
		WriterEpoch:    metadata.WriterEpoch,
		Sequence:       metadata.Sequence,
		RecordCount:    metadata.RecordCount,
		TimelineCount:  metadata.TimelineCount,
		SizeBytes:      chunkfile.HeaderSize + metadata.BodyBytes,
		MinTimestampMS: metadata.MinTimestamp,
		MaxTimestampMS: metadata.MaxTimestamp,
		SHA256:         metadata.ObjectHash,
	}
	if err := Validate(ref); err != nil {
		return Ref{}, err
	}
	return ref, nil
}

// Validate checks the reference without reading its object.
func Validate(ref Ref) error {
	if ref.Key == "" {
		return fmt.Errorf("%w: empty object key", ErrInvalidRef)
	}
	if ref.FormatVersion != chunkfile.Version {
		return fmt.Errorf("%w: format version=%d", ErrInvalidRef, ref.FormatVersion)
	}
	if ref.NamespaceHash == ([32]byte{}) {
		return fmt.Errorf("%w: zero namespace hash", ErrInvalidRef)
	}
	if ref.WriterEpoch == 0 {
		return fmt.Errorf("%w: zero writer epoch", ErrInvalidRef)
	}
	if ref.RecordCount == 0 || ref.RecordCount > chunkfile.MaxRecords {
		return fmt.Errorf("%w: record count=%d", ErrInvalidRef, ref.RecordCount)
	}
	if ref.TimelineCount == 0 || ref.TimelineCount > ref.RecordCount {
		return fmt.Errorf("%w: timeline count=%d records=%d", ErrInvalidRef, ref.TimelineCount, ref.RecordCount)
	}
	if ref.SizeBytes < chunkfile.HeaderSize+chunkfile.RecordHeaderSize+1 || ref.SizeBytes > chunkfile.MaxObjectBytes {
		return fmt.Errorf("%w: object bytes=%d", ErrInvalidRef, ref.SizeBytes)
	}
	if ref.MinTimestampMS > ref.MaxTimestampMS {
		return fmt.Errorf("%w: timestamp range=[%d,%d]", ErrInvalidRef, ref.MinTimestampMS, ref.MaxTimestampMS)
	}
	if ref.SHA256 == ([32]byte{}) {
		return fmt.Errorf("%w: zero SHA-256", ErrInvalidRef)
	}
	return nil
}

// MatchesMetadata reports whether decoded UJTC metadata is exactly the object
// described by ref. The caller still must authenticate the complete bytes.
func MatchesMetadata(ref Ref, metadata chunkfile.Metadata) bool {
	return ref.FormatVersion == chunkfile.Version &&
		ref.NamespaceHash == metadata.NamespaceHash &&
		ref.Shard == metadata.Shard &&
		ref.WriterEpoch == metadata.WriterEpoch &&
		ref.Sequence == metadata.Sequence &&
		ref.RecordCount == metadata.RecordCount &&
		ref.TimelineCount == metadata.TimelineCount &&
		ref.SizeBytes == chunkfile.HeaderSize+metadata.BodyBytes &&
		ref.MinTimestampMS == metadata.MinTimestamp &&
		ref.MaxTimestampMS == metadata.MaxTimestamp &&
		ref.SHA256 == metadata.ObjectHash
}

// Decode authenticates and decodes the complete object described by ref.
func Decode(ref Ref, object []byte) (chunkfile.Metadata, []record.Record, error) {
	if err := Validate(ref); err != nil {
		return chunkfile.Metadata{}, nil, err
	}
	if uint64(len(object)) != ref.SizeBytes || sha256.Sum256(object) != ref.SHA256 {
		return chunkfile.Metadata{}, nil, fmt.Errorf("%w: size or SHA-256", ErrMismatch)
	}
	metadata, records, err := chunkfile.Unmarshal(object)
	if err != nil {
		return chunkfile.Metadata{}, nil, fmt.Errorf("%w: %w", ErrMismatch, err)
	}
	if !MatchesMetadata(ref, metadata) {
		return chunkfile.Metadata{}, nil, fmt.Errorf("%w: decoded metadata", ErrMismatch)
	}
	return metadata, records, nil
}

func Same(a, b Ref) bool { return a == b }
