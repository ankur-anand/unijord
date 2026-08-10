package sink

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	plwriter "github.com/ankur-anand/unijord/partitionlog/writer"
)

// Layout owns object key naming for partitionlog segment objects and staging
// prefixes. It is value-safe and can be copied.
type Layout struct {
	prefix string
}

// SegmentObjectKey is the validated identity encoded in a final segment key.
type SegmentObjectKey struct {
	Key         string
	BaseLSN     uint64
	WriterEpoch uint64
	SegmentUUID [16]byte
}

// StagingObjectKey is the validated segment identity encoded in a staging
// object key. RelativeKey identifies the provider-owned object under it.
type StagingObjectKey struct {
	Key         string
	BaseLSN     uint64
	WriterEpoch uint64
	SegmentUUID [16]byte
	RelativeKey string
}

func NewLayout(prefix string) Layout {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		prefix = DefaultPrefix
	}
	return Layout{prefix: prefix}
}

func (l Layout) Prefix() string {
	return l.root()
}

func (l Layout) SegmentKey(info plwriter.SegmentInfo) string {
	return l.SegmentPrefix(info.StreamID, info.Partition) + segmentName(info.BaseLSN, info.WriterEpoch, info.SegmentUUID, segmentFileSuffix)
}

// SegmentPrefix returns the final-segment prefix for one stream partition.
func (l Layout) SegmentPrefix(streamID string, partition uint32) string {
	return l.partitionObjectPrefix("segments", streamID, partition)
}

// SegmentLowerBound returns a synthetic key that sorts immediately before all
// final segment keys with baseLSN. It need not exist in object storage.
func (l Layout) SegmentLowerBound(streamID string, partition uint32, baseLSN uint64) string {
	return l.SegmentPrefix(streamID, partition) + fmt.Sprintf("seg-%020d-", baseLSN)
}

// ParseSegmentKey validates a final key in this layout and extracts its
// retention and fencing fields.
func (l Layout) ParseSegmentKey(streamID string, partition uint32, key string) (SegmentObjectKey, error) {
	prefix := l.SegmentPrefix(streamID, partition)
	name, ok := strings.CutPrefix(key, prefix)
	if !ok || strings.Contains(name, "/") {
		return SegmentObjectKey{}, fmt.Errorf("sink: segment key %q is outside prefix %q", key, prefix)
	}
	baseLSN, epoch, uuid, err := parseSegmentName(name, segmentFileSuffix)
	if err != nil {
		return SegmentObjectKey{}, err
	}
	return SegmentObjectKey{Key: key, BaseLSN: baseLSN, WriterEpoch: epoch, SegmentUUID: uuid}, nil
}

func (l Layout) StagingPrefix(info plwriter.SegmentInfo) string {
	return l.PartitionStagingPrefix(info.StreamID, info.Partition) + segmentName(info.BaseLSN, info.WriterEpoch, info.SegmentUUID, "")
}

// PartitionStagingPrefix returns the provider staging prefix for one stream
// partition.
func (l Layout) PartitionStagingPrefix(streamID string, partition uint32) string {
	return l.partitionObjectPrefix("staging", streamID, partition)
}

// ParseStagingKey validates a provider staging object key and extracts the
// segment identity that owns it.
func (l Layout) ParseStagingKey(streamID string, partition uint32, key string) (StagingObjectKey, error) {
	prefix := l.PartitionStagingPrefix(streamID, partition)
	relative, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return StagingObjectKey{}, fmt.Errorf("sink: staging key %q is outside prefix %q", key, prefix)
	}
	name, child, ok := strings.Cut(relative, "/")
	if !ok || child == "" {
		return StagingObjectKey{}, fmt.Errorf("sink: staging key %q has no provider object", key)
	}
	baseLSN, epoch, uuid, err := parseSegmentName(name, "")
	if err != nil {
		return StagingObjectKey{}, err
	}
	return StagingObjectKey{
		Key: key, BaseLSN: baseLSN, WriterEpoch: epoch, SegmentUUID: uuid, RelativeKey: child,
	}, nil
}

func (l Layout) root() string {
	if l.prefix == "" {
		return DefaultPrefix
	}
	return l.prefix
}

func (l Layout) partitionObjectPrefix(kind string, streamID string, partition uint32) string {
	streamID = keylayout.NormalizeStreamID(streamID)
	parts := []string{l.root(), kind, keylayout.Bucket(streamID, partition)}
	parts = appendStreamParts(parts, streamID)
	parts = append(parts, fmt.Sprintf("p%08d", partition))
	return strings.Join(parts, "/") + "/"
}

func segmentName(baseLSN, writerEpoch uint64, uuid [16]byte, suffix string) string {
	return fmt.Sprintf("seg-%020d-e%020d-%s%s", baseLSN, writerEpoch, hex.EncodeToString(uuid[:]), suffix)
}

func parseSegmentName(name, suffix string) (uint64, uint64, [16]byte, error) {
	const stemSize = len("seg-") + 20 + len("-e") + 20 + 1 + 32
	var uuid [16]byte
	if len(name) != stemSize+len(suffix) || !strings.HasSuffix(name, suffix) {
		return 0, 0, uuid, fmt.Errorf("sink: invalid segment object name %q", name)
	}
	stem := strings.TrimSuffix(name, suffix)
	if stem[:4] != "seg-" || stem[24:26] != "-e" || stem[46] != '-' {
		return 0, 0, uuid, fmt.Errorf("sink: invalid segment object name %q", name)
	}
	baseLSN, err := parseFixedUint(stem[4:24])
	if err != nil {
		return 0, 0, uuid, fmt.Errorf("sink: invalid segment base LSN in %q: %w", name, err)
	}
	epoch, err := parseFixedUint(stem[26:46])
	if err != nil || epoch == 0 {
		return 0, 0, uuid, fmt.Errorf("sink: invalid segment writer epoch in %q", name)
	}
	uuidText := stem[47:]
	decoded, err := hex.DecodeString(uuidText)
	if err != nil || len(decoded) != len(uuid) || uuidText != strings.ToLower(uuidText) {
		return 0, 0, uuid, fmt.Errorf("sink: invalid segment UUID in %q", name)
	}
	copy(uuid[:], decoded)
	if uuid == ([16]byte{}) {
		return 0, 0, uuid, fmt.Errorf("sink: empty segment UUID in %q", name)
	}
	return baseLSN, epoch, uuid, nil
}

func parseFixedUint(value string) (uint64, error) {
	if len(value) != 20 {
		return 0, fmt.Errorf("width=%d", len(value))
	}
	for i := range value {
		if value[i] < '0' || value[i] > '9' {
			return 0, fmt.Errorf("non-decimal digit")
		}
	}
	return strconv.ParseUint(value, 10, 64)
}

func appendStreamParts(parts []string, streamID string) []string {
	if streamID == "" {
		return parts
	}
	return append(parts, "streams", keylayout.StreamKey(streamID))
}
