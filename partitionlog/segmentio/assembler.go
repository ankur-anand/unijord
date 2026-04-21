package segmentio

import (
	"fmt"
	"hash/crc32"
)

// AssemblyPlan is the validated segment summary exposed to sinks before any
// part upload begins.
type AssemblyPlan struct {
	Metadata         Metadata
	TotalSize        uint64
	BlockCount       uint32
	BlockIndexOffset uint64
	BlockIndexLength uint32
}

type assembledBlock struct {
	Descriptor blockDescriptor
	Entry      BlockIndexEntry
}

type assembledSegment struct {
	Plan   AssemblyPlan
	Header Header
	Blocks []assembledBlock
	Footer Footer
}

func assembleSegment(partition uint32, codec Codec, descriptors []blockDescriptor) (assembledSegment, error) {
	if len(descriptors) == 0 {
		return assembledSegment{}, ErrEmptySegment
	}

	blocks := make([]assembledBlock, len(descriptors))
	entries := make([]BlockIndexEntry, len(descriptors))
	nextBlockOffset := uint64(HeaderSize)
	totalRecords := uint64(0)
	expectedSequence := uint32(0)
	prevMaxTimestamp := descriptors[0].MinTimestampMS

	for i, descriptor := range descriptors {
		if err := descriptor.validate(); err != nil {
			return assembledSegment{}, err
		}
		if descriptor.Sequence != expectedSequence {
			return assembledSegment{}, fmt.Errorf(
				"%w: block sequence=%d want=%d",
				ErrBlockIndexMismatch,
				descriptor.Sequence,
				expectedSequence,
			)
		}
		if descriptor.Codec != codec {
			return assembledSegment{}, fmt.Errorf(
				"%w: block codec=%s want=%s",
				ErrBlockIndexMismatch,
				descriptor.Codec,
				codec,
			)
		}
		if i > 0 {
			prev := descriptors[i-1]
			if descriptor.BaseLSN != prev.LastLSN+1 {
				return assembledSegment{}, fmt.Errorf(
					"%w: block %d base_lsn=%d want=%d",
					ErrBlockIndexMismatch,
					i,
					descriptor.BaseLSN,
					prev.LastLSN+1,
				)
			}
			if descriptor.MinTimestampMS < prevMaxTimestamp {
				return assembledSegment{}, fmt.Errorf("%w: block %d timestamp range regresses", ErrBlockIndexMismatch, i)
			}
		}

		entry := BlockIndexEntry{
			BlockOffset:    nextBlockOffset,
			StoredSize:     descriptor.StoredSize,
			RawSize:        descriptor.RawSize,
			BaseLSN:        descriptor.BaseLSN,
			RecordCount:    descriptor.RecordCount,
			CRC32C:         descriptor.CRC32C,
			MinTimestampMS: descriptor.MinTimestampMS,
			MaxTimestampMS: descriptor.MaxTimestampMS,
		}

		blocks[i] = assembledBlock{
			Descriptor: descriptor,
			Entry:      entry,
		}
		entries[i] = entry
		nextBlockOffset += uint64(descriptor.StoredSize)
		totalRecords += uint64(descriptor.RecordCount)
		expectedSequence++
		prevMaxTimestamp = descriptor.MaxTimestampMS
	}

	if totalRecords > maxUint32Value {
		return assembledSegment{}, fmt.Errorf("%w: segment record count too large=%d", ErrInvalidSegment, totalRecords)
	}

	header := Header{
		Partition:      partition,
		Codec:          codec,
		BaseLSN:        descriptors[0].BaseLSN,
		LastLSN:        descriptors[len(descriptors)-1].LastLSN,
		MinTimestampMS: descriptors[0].MinTimestampMS,
		MaxTimestampMS: descriptors[len(descriptors)-1].MaxTimestampMS,
		RecordCount:    uint32(totalRecords),
		BlockCount:     uint32(len(descriptors)),
	}
	if err := header.validate(); err != nil {
		return assembledSegment{}, err
	}

	blockIndexLength := uint64(len(entries)) * BlockIndexEntrySize
	if blockIndexLength > maxUint32Value {
		return assembledSegment{}, fmt.Errorf("%w: block index too large=%d", ErrInvalidSegment, blockIndexLength)
	}

	indexCRC := crc32.New(crc32cTable)
	indexBuf := make([]byte, BlockIndexEntrySize)
	for _, entry := range entries {
		entry.marshalTo(indexBuf)
		if _, err := indexCRC.Write(indexBuf); err != nil {
			return assembledSegment{}, fmt.Errorf("write block index crc: %w", err)
		}
	}

	footer := Footer{
		BlockIndexOffset: nextBlockOffset,
		BlockIndexLength: uint32(blockIndexLength),
		BlockIndexCRC:    indexCRC.Sum32(),
	}

	totalSize := footer.BlockIndexOffset + uint64(footer.BlockIndexLength) + FooterSize
	if totalSize > uint64(^uint(0)>>1) {
		return assembledSegment{}, fmt.Errorf("%w: total size too large=%d", ErrInvalidSegment, totalSize)
	}
	if err := footer.validate(int64(totalSize), header.BlockCount); err != nil {
		return assembledSegment{}, err
	}
	if err := validateBlockIndex(entries, header, footer.BlockIndexOffset); err != nil {
		return assembledSegment{}, err
	}

	return assembledSegment{
		Plan: AssemblyPlan{
			Metadata:         metadataFromHeader(header, int64(totalSize)),
			TotalSize:        totalSize,
			BlockCount:       header.BlockCount,
			BlockIndexOffset: footer.BlockIndexOffset,
			BlockIndexLength: footer.BlockIndexLength,
		},
		Header: header,
		Blocks: blocks,
		Footer: footer,
	}, nil
}
