package keylayout

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// SegmentObjectKey derives the final immutable segment object key. dataRoot
// must already be normalized by the owning component.
func SegmentObjectKey(dataRoot, streamID string, partition uint32, baseLSN, writerEpoch uint64, segmentUUID [16]byte) string {
	streamID = NormalizeStreamID(streamID)
	parts := []string{strings.Trim(dataRoot, "/"), "segments", Bucket(streamID, partition)}
	if streamID != "" {
		parts = append(parts, "streams", StreamKey(streamID))
	}
	parts = append(parts, fmt.Sprintf("p%08d", partition))
	return strings.Join(parts, "/") + fmt.Sprintf(
		"/seg-%020d-e%020d-%s.plseg",
		baseLSN, writerEpoch, hex.EncodeToString(segmentUUID[:]),
	)
}
