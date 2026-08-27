package bench

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand/v2"
	"path"
	"strings"

	"github.com/ankur-anand/unijord/internal/blobstore"
	"github.com/ankur-anand/unijord/partitionlog/keylayout"
	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

// TSBase makes synthetic record timestamps a function of LSN so resumed runs
// keep the catalog's non-decreasing timestamp invariant: ts = TSBase + LSN.
const TSBase = int64(1_700_000_000_000)

// NewID returns a random 16-byte identifier for writers and segments.
func NewID() [16]byte {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return id
}

// SyntheticSegment builds a SegmentRef that passes catalog validation and
// whose URI is the derived key the v2 catalog requires, without uploading any
// segment bytes. dataRoot must equal the catalog's SegmentRootPrefix.
func SyntheticSegment(dataRoot, streamID string, partition uint32, base uint64, recs int, epoch uint64, writer [16]byte) pmeta.SegmentRef {
	uuid := NewID()
	last := base + uint64(recs) - 1
	return pmeta.SegmentRef{
		URI:              keylayout.SegmentObjectKey(dataRoot, streamID, partition, base, epoch, uuid),
		StreamID:         streamID,
		Partition:        partition,
		WriterEpoch:      epoch,
		SegmentUUID:      uuid,
		WriterTag:        writer,
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   TSBase + int64(base),
		MaxTimestampMS:   TSBase + int64(last),
		RecordCount:      uint32(recs),
		BlockCount:       1,
		SizeBytes:        1 << 20,
		BlockIndexOffset: 1<<20 - 256,
		BlockIndexLength: 64,
		Codec:            segformat.CodecZstd,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      mrand.Uint64() | 1,
		TrailerHash:      mrand.Uint64() | 1,
	}
}

// CatalogInventory lists <prefix>/catalog/ and classifies objects. Keys are
// stable class names used in results: head, leaf, index-l01, index-l02, …,
// maintenance, other.
func CatalogInventory(ctx context.Context, backend blobstore.Store, prefix string) (map[string]InventoryClass, []blobstore.ObjectInfo, error) {
	classes := map[string]InventoryClass{}
	var objects []blobstore.ObjectInfo
	after := ""
	catalogPrefix := path.Join(prefix, "catalog") + "/"
	for {
		page, err := backend.List(ctx, blobstore.ListOptions{Prefix: catalogPrefix, AfterKey: after, Limit: blobstore.MaxListLimit})
		if err != nil {
			return nil, nil, err
		}
		for _, o := range page.Objects {
			objects = append(objects, o)
			name := path.Base(o.Key)
			class := "other"
			switch {
			case strings.HasPrefix(name, "head."):
				class = "head"
			case strings.HasPrefix(name, "leaf-"):
				class = "leaf"
			case strings.HasPrefix(name, "index-l") && len(name) >= 9:
				class = "index-" + name[6:9]
			case strings.Contains(o.Key, "/maintenance/"):
				class = "maintenance"
			}
			c := classes[class]
			c.Objects++
			c.Bytes += o.SizeBytes
			c.MaxBytes = max(c.MaxBytes, o.SizeBytes)
			classes[class] = c
		}
		if !page.HasMore {
			return classes, objects, nil
		}
		after = page.NextAfterKey
	}
}

// ExpectedSealedPages returns the number of sealed leaf and per-level index
// pages a write-once tree holds after `segments` publishes with no retention.
func ExpectedSealedPages(segments, leafLimit, indexLimit int) (leaves int, index []int) {
	leaves = segments / leafLimit
	n := leaves
	for {
		n = n / indexLimit
		if n == 0 {
			return leaves, index
		}
		index = append(index, n)
	}
}

// PageSizes returns the exact on-disk size of a full sealed leaf and index
// page under catformat v2.
func PageSizes(leafLimit, indexLimit int) (leafBytes, indexBytes int) {
	const header, trailer, leafEntry, indexEntry = 128, 32, 128, 80
	return header + leafLimit*leafEntry + trailer, header + indexLimit*indexEntry + trailer
}

// HeadBound is the maximum head.plc size for the given limits and open
// index levels.
func HeadBound(leafLimit, indexLimit, levels int) int {
	const header, lastSegment, section, leafEntry, indexEntry, trailer = 192, 128, 16, 128, 80, 32
	return header + lastSegment + leafLimit*leafEntry + levels*(section+indexLimit*indexEntry) + trailer
}

func describeParams(p Params) string {
	return fmt.Sprintf("segments=%d records=%d leaf=%d k=%d samples=%d", p.Segments, p.RecordsPerSegment, p.LeafLimit, p.IndexLimit, p.Samples)
}
