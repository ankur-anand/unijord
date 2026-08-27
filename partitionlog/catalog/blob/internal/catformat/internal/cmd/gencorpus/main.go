package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ankur-anand/unijord/partitionlog/catalog/blob/internal/catformat"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
)

type fixture struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	File     string `json:"file"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
	PageID   string `json:"page_id,omitempty"`
	HashAlgo string `json:"hash_algorithm"`
}

type manifest struct {
	Format   string    `json:"format"`
	Version  uint16    `json:"version"`
	Fixtures []fixture `json:"fixtures"`
}

func main() {
	out := flag.String("out", "testdata/v1", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		panic(err)
	}

	var fixtures []fixture
	write := func(name, kind, hashAlgo string, encoded []byte, pageID [16]byte) {
		file := name + ".plc"
		if err := os.WriteFile(filepath.Join(*out, file), encoded, 0o644); err != nil {
			panic(err)
		}
		sum := sha256.Sum256(encoded)
		item := fixture{
			Name:     name,
			Kind:     kind,
			File:     file,
			Size:     len(encoded),
			SHA256:   hex.EncodeToString(sum[:]),
			HashAlgo: hashAlgo,
		}
		if pageID != ([16]byte{}) {
			item.PageID = hex.EncodeToString(pageID[:])
		}
		fixtures = append(fixtures, item)
	}

	empty, err := catformat.MarshalHead(emptyHead())
	must(err)
	write("empty-head-crc32c", "head", "crc32c", empty, [16]byte{})

	populated, err := catformat.MarshalHead(populatedHead())
	must(err)
	write("populated-head-xxh64", "head", "xxh64", populated, [16]byte{})

	fullyRetained, err := catformat.MarshalHead(fullyRetainedHead())
	must(err)
	write("fully-retained-head-xxh64", "head", "xxh64", fullyRetained, [16]byte{})

	leaf, leafID, err := catformat.MarshalLeafPage(leafPage())
	must(err)
	write("leaf-page-xxh64", "leaf_page", "xxh64", leaf, leafID)

	index, indexID, err := catformat.MarshalIndexPage(indexPage())
	must(err)
	write("index-page-crc32c", "index_page", "crc32c", index, indexID)

	encodedManifest, err := json.MarshalIndent(manifest{
		Format:   "catformat",
		Version:  catformat.Version,
		Fixtures: fixtures,
	}, "", "  ")
	must(err)
	encodedManifest = append(encodedManifest, '\n')
	must(os.WriteFile(filepath.Join(*out, "manifest.json"), encodedManifest, 0o644))
	fmt.Printf("wrote %d catformat v%d fixtures to %s\n", len(fixtures), catformat.Version, *out)
}

func emptyHead() catformat.Head {
	return catformat.Head{Header: catformat.HeadHeader{
		Partition:            7,
		HashAlgo:             segformat.HashCRC32C,
		SegmentLayoutVersion: catformat.SegmentLayoutV1,
		LeafLimit:            4,
		IndexLimit:           4,
		StreamKey:            filled32(0xa1),
		DataRootKey:          filled32(0xc1),
	}}
}

func populatedHead() catformat.Head {
	active := leafEntry(2, 3, 101, 104, 8, 0x22)
	return catformat.Head{
		Header: catformat.HeadHeader{
			Flags:                 catformat.FlagHasLastSegment,
			Partition:             7,
			HashAlgo:              segformat.HashXXH64,
			SegmentLayoutVersion:  catformat.SegmentLayoutV1,
			LeafLimit:             4,
			IndexLimit:            4,
			ActiveCount:           1,
			LevelCount:            1,
			NextLSN:               4,
			OldestLSN:             0,
			WriterEpoch:           8,
			Generation:            9,
			SegmentCount:          2,
			ReachableSegmentCount: 2,
			WriterID:              filled16(0xb1),
			StreamKey:             filled32(0xa1),
			DataRootKey:           filled32(0xc1),
		},
		LastSegment: active,
		Active:      []catformat.LeafEntry{active},
		Sections: []catformat.OpenIndexSection{{
			Level:   1,
			Entries: []catformat.IndexEntry{indexEntry(0, 0, 1, 100, 101, 7, 1, 0x31)},
		}},
	}
}

func fullyRetainedHead() catformat.Head {
	last := leafEntry(2, 3, 101, 104, 8, 0x22)
	return catformat.Head{
		Header: catformat.HeadHeader{
			Flags:                   catformat.FlagHasLastSegment,
			Partition:               7,
			HashAlgo:                segformat.HashXXH64,
			SegmentLayoutVersion:    catformat.SegmentLayoutV1,
			LeafLimit:               4,
			IndexLimit:              4,
			NextLSN:                 4,
			OldestLSN:               4,
			AppliedRetentionLSN:     4,
			AppliedRetentionVersion: 1,
			WriterEpoch:             8,
			Generation:              10,
			SegmentCount:            2,
			WriterID:                filled16(0xb1),
			StreamKey:               filled32(0xa1),
			DataRootKey:             filled32(0xc1),
		},
		LastSegment: last,
	}
}

func leafPage() catformat.LeafPage {
	return catformat.LeafPage{
		Header: catformat.PageHeader{
			Partition:      7,
			HashAlgo:       segformat.HashXXH64,
			EntryCount:     2,
			EntrySize:      catformat.LeafEntrySize,
			SeqLo:          0,
			SeqHi:          3,
			MinTimestampMS: 100,
			MaxTimestampMS: 104,
			Generation:     9,
			WriterEpoch:    8,
			StreamKey:      filled32(0xa1),
			SegmentCount:   2,
		},
		Entries: []catformat.LeafEntry{
			leafEntry(0, 1, 100, 101, 7, 0x11),
			leafEntry(2, 3, 101, 104, 8, 0x22),
		},
	}
}

func indexPage() catformat.IndexPage {
	return catformat.IndexPage{
		Header: catformat.PageHeader{
			Partition:      7,
			Level:          1,
			HashAlgo:       segformat.HashCRC32C,
			EntryCount:     2,
			EntrySize:      catformat.IndexEntrySize,
			SeqLo:          0,
			SeqHi:          8,
			MinTimestampMS: 100,
			MaxTimestampMS: 110,
			Generation:     9,
			WriterEpoch:    8,
			StreamKey:      filled32(0xa1),
			SegmentCount:   5,
		},
		Entries: []catformat.IndexEntry{
			indexEntry(0, 0, 3, 100, 104, 7, 2, 0x31),
			indexEntry(0, 4, 8, 104, 110, 8, 3, 0x41),
		},
	}
}

func leafEntry(base, last uint64, minTS, maxTS int64, epoch uint64, marker byte) catformat.LeafEntry {
	return catformat.LeafEntry{
		BaseLSN:          base,
		LastLSN:          last,
		MinTimestampMS:   minTS,
		MaxTimestampMS:   maxTS,
		RecordCount:      uint32(last - base + 1),
		BlockCount:       1,
		SizeBytes:        4096 + base,
		BlockIndexOffset: 2048,
		BlockIndexLength: 128,
		Codec:            segformat.CodecZstd,
		HashAlgo:         segformat.HashXXH64,
		SegmentHash:      0x0102030405060708 + base,
		TrailerHash:      0x1112131415161718 + base,
		WriterEpoch:      epoch,
		SegmentUUID:      filled16(marker),
		WriterTag:        filled16(byte(epoch)),
	}
}

func indexEntry(level uint8, lo, hi uint64, minTS, maxTS int64, generation, segments uint64, marker byte) catformat.IndexEntry {
	return catformat.IndexEntry{
		Level:          level,
		EntryCount:     uint32(segments),
		SeqLo:          lo,
		SeqHi:          hi,
		MinTimestampMS: minTS,
		MaxTimestampMS: maxTS,
		Generation:     generation,
		PageID:         filled16(marker),
		SegmentCount:   segments,
	}
}

func filled16(value byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = value
	}
	return out
}

func filled32(value byte) [32]byte {
	var out [32]byte
	for i := range out {
		out[i] = value
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
