package segreader

import (
	"context"
	"fmt"
	"sort"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segblock"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

// Reader reads one immutable segment object. It is safe for concurrent Read or
// Scan calls after Open returns.
type Reader struct {
	store SegmentStore
	ref   pmeta.SegmentRef
	opts  Options

	preamble segformat.FilePreamble
	trailer  segformat.Trailer
	index    []segformat.BlockIndexEntry
}

func Open(ctx context.Context, store SegmentStore, ref pmeta.SegmentRef, opts Options) (*Reader, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: store is nil", ErrInvalidOptions)
	}
	normalized, err := normalizeOptions(opts)
	if err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidSegment, err)
	}
	if ref.SizeBytes < segformat.FilePreambleSize+segformat.TrailerSize {
		return nil, fmt.Errorf("%w: object too small: size=%d", ErrInvalidSegment, ref.SizeBytes)
	}

	trailerOff := ref.SizeBytes - segformat.TrailerSize
	trailerBytes, err := readAtExact(ctx, store, ref.URI, trailerOff, segformat.TrailerSize)
	if err != nil {
		return nil, err
	}
	trailer, err := segformat.ParseTrailer(trailerBytes, ref.SizeBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse trailer: %w", ErrCorruptData, err)
	}
	if err := validateRefTrailer(ref, trailer); err != nil {
		return nil, err
	}

	preambleBytes, err := readAtExact(ctx, store, ref.URI, 0, segformat.FilePreambleSize)
	if err != nil {
		return nil, err
	}
	preamble, err := segformat.ParseFilePreamble(preambleBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse file preamble: %w", ErrCorruptData, err)
	}
	if err := segformat.ValidatePreambleTrailer(preamble, trailer); err != nil {
		return nil, fmt.Errorf("%w: validate preamble/trailer: %w", ErrCorruptData, err)
	}

	indexBytes, err := readAtExact(ctx, store, ref.URI, trailer.BlockIndexOffset, uint64(trailer.BlockIndexLength))
	if err != nil {
		return nil, err
	}
	_, index, err := segformat.ParseBlockIndex(indexBytes, trailer.HashAlgo)
	if err != nil {
		return nil, fmt.Errorf("%w: parse block index: %w", ErrCorruptData, err)
	}
	if err := segformat.ValidateIndex(index, trailer); err != nil {
		return nil, fmt.Errorf("%w: validate block index: %w", ErrCorruptData, err)
	}

	if normalized.ValidateSegmentHash {
		indexEnd := trailer.BlockIndexOffset + uint64(trailer.BlockIndexLength)
		body, err := readAtExact(ctx, store, ref.URI, 0, indexEnd)
		if err != nil {
			return nil, err
		}
		got, err := segformat.HashBytes(trailer.HashAlgo, body)
		if err != nil {
			return nil, fmt.Errorf("%w: hash segment: %w", ErrCorruptData, err)
		}
		if got != trailer.SegmentHash {
			return nil, fmt.Errorf("%w: segment hash got=%x want=%x", ErrCorruptData, got, trailer.SegmentHash)
		}
	}

	return &Reader{
		store:    store,
		ref:      ref,
		opts:     normalized,
		preamble: preamble,
		trailer:  trailer,
		index:    append([]segformat.BlockIndexEntry(nil), index...),
	}, nil
}

func (r *Reader) Ref() pmeta.SegmentRef {
	return r.ref
}

func (r *Reader) Preamble() segformat.FilePreamble {
	return r.preamble
}

func (r *Reader) Trailer() segformat.Trailer {
	return r.trailer
}

func (r *Reader) BlockIndex() []segformat.BlockIndexEntry {
	return append([]segformat.BlockIndexEntry(nil), r.index...)
}

func (r *Reader) FindLSNByTimestamp(ctx context.Context, timestampMS int64) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if timestampMS > r.trailer.MaxTimestampMS {
		return 0, false, nil
	}
	if timestampMS <= r.trailer.MinTimestampMS {
		return r.trailer.BaseLSN, true, nil
	}

	block := sort.Search(len(r.index), func(i int) bool {
		return r.index[i].MaxTimestampMS >= timestampMS
	})
	if block >= len(r.index) {
		return 0, false, nil
	}
	for ; block < len(r.index); block++ {
		entry := r.index[block]
		if timestampMS <= entry.MinTimestampMS {
			return entry.BaseLSN, true, nil
		}
		if timestampMS > entry.MaxTimestampMS {
			continue
		}
		records, err := r.readBlock(ctx, block, entry.BaseLSN)
		if err != nil {
			return 0, false, err
		}
		for _, record := range records {
			if record.TimestampMS >= timestampMS {
				return record.LSN, true, nil
			}
		}
		return 0, false, fmt.Errorf("%w: no record >= timestamp_ms=%d in candidate block base_lsn=%d", ErrCorruptData, timestampMS, entry.BaseLSN)
	}
	return 0, false, nil
}

// Read returns up to limit records starting at fromLSN. A limit <= 0 reads all
// remaining records in this segment.
func (r *Reader) Read(ctx context.Context, fromLSN uint64, limit int) ([]Record, error) {
	scanner, err := r.Scan(ctx, fromLSN)
	if err != nil {
		return nil, err
	}
	defer scanner.Close()

	var out []Record
	for limit <= 0 || len(out) < limit {
		record, ok, err := scanner.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		out = append(out, record)
	}
	return out, nil
}

func (r *Reader) Scan(ctx context.Context, fromLSN uint64) (*Scanner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fromLSN > r.trailer.LastLSN {
		return &Scanner{closed: true}, nil
	}
	if fromLSN < r.trailer.BaseLSN {
		fromLSN = r.trailer.BaseLSN
	}
	block := sort.Search(len(r.index), func(i int) bool {
		return blockLastLSN(r.index[i]) >= fromLSN
	})
	if block >= len(r.index) {
		return &Scanner{closed: true}, nil
	}
	return &Scanner{
		r:         r,
		nextBlock: block,
		fromLSN:   fromLSN,
	}, nil
}

func (r *Reader) readBlock(ctx context.Context, idx int, fromLSN uint64) ([]Record, error) {
	entry := r.index[idx]
	length := uint64(segformat.BlockPreambleSize) + uint64(entry.StoredSize)
	if length > r.opts.MaxBlockBytes {
		return nil, fmt.Errorf("%w: block bytes=%d max=%d", ErrInvalidSegment, length, r.opts.MaxBlockBytes)
	}
	blockBytes, err := readAtExact(ctx, r.store, r.ref.URI, entry.BlockOffset, length)
	if err != nil {
		return nil, err
	}
	blockPreamble, err := segformat.ParseBlockPreamble(blockBytes[:segformat.BlockPreambleSize])
	if err != nil {
		return nil, fmt.Errorf("%w: parse block preamble: %w", ErrCorruptData, err)
	}
	if err := segformat.MatchBlockIndexEntry(blockPreamble, entry); err != nil {
		return nil, fmt.Errorf("%w: block/index mismatch: %w", ErrCorruptData, err)
	}
	raw, err := segblock.Open(r.trailer.Codec, r.trailer.HashAlgo, blockPreamble, blockBytes[segformat.BlockPreambleSize:])
	if err != nil {
		return nil, fmt.Errorf("%w: open block: %w", ErrCorruptData, err)
	}
	rawRecords, err := segformat.DecodeRawBlock(raw, blockPreamble)
	if err != nil {
		return nil, fmt.Errorf("%w: decode raw block: %w", ErrCorruptData, err)
	}
	records := make([]Record, 0, len(rawRecords))
	for _, rawRecord := range rawRecords {
		if rawRecord.LSN < fromLSN {
			continue
		}
		records = append(records, Record{
			Partition:   r.trailer.Partition,
			LSN:         rawRecord.LSN,
			TimestampMS: rawRecord.TimestampMS,
			Headers:     segformat.CloneHeaders(rawRecord.Headers),
			Value:       append([]byte(nil), rawRecord.Value...),
		})
	}
	return records, nil
}

func blockLastLSN(entry segformat.BlockIndexEntry) uint64 {
	return entry.BaseLSN + uint64(entry.RecordCount) - 1
}

func validateRefTrailer(ref pmeta.SegmentRef, trailer segformat.Trailer) error {
	if ref.Partition != trailer.Partition ||
		ref.SegmentUUID != trailer.SegmentUUID ||
		ref.WriterTag != trailer.WriterTag ||
		ref.BaseLSN != trailer.BaseLSN ||
		ref.LastLSN != trailer.LastLSN ||
		ref.MinTimestampMS != trailer.MinTimestampMS ||
		ref.MaxTimestampMS != trailer.MaxTimestampMS ||
		ref.RecordCount != trailer.RecordCount ||
		ref.BlockCount != trailer.BlockCount ||
		ref.SizeBytes != trailer.TotalSize ||
		ref.BlockIndexOffset != trailer.BlockIndexOffset ||
		ref.BlockIndexLength != trailer.BlockIndexLength ||
		ref.Codec != trailer.Codec ||
		ref.HashAlgo != trailer.HashAlgo ||
		ref.SegmentHash != trailer.SegmentHash ||
		ref.TrailerHash != trailer.TrailerHash {
		return fmt.Errorf("%w: segment ref does not match trailer", ErrInvalidSegment)
	}
	return nil
}
