package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

const corpusSchemaVersion = 1

type corpusManifest struct {
	SchemaVersion        int            `json:"schema_version"`
	SegmentFormatVersion uint16         `json:"segment_format_version"`
	Vectors              []corpusVector `json:"vectors"`
}

type corpusVector struct {
	Name             string         `json:"name"`
	File             string         `json:"file"`
	FileSHA256       string         `json:"file_sha256"`
	WriterByteStable bool           `json:"writer_byte_stable"`
	Writer           writerSpec     `json:"writer"`
	SegmentRef       segmentRefSpec `json:"segment_ref"`
	Blocks           []blockSpec    `json:"blocks"`
	Records          []recordSpec   `json:"records"`
}

type writerSpec struct {
	TargetBlockSize int    `json:"target_block_size"`
	CreatedUnixMS   int64  `json:"created_unix_ms"`
	SegmentUUID     string `json:"segment_uuid"`
	WriterTag       string `json:"writer_tag"`
}

type segmentRefSpec struct {
	URI              string   `json:"uri"`
	StreamID         string   `json:"stream_id"`
	Partition        uint32   `json:"partition"`
	WriterEpoch      string   `json:"writer_epoch"`
	SegmentUUID      string   `json:"segment_uuid"`
	WriterTag        string   `json:"writer_tag"`
	BaseLSN          string   `json:"base_lsn"`
	LastLSN          string   `json:"last_lsn"`
	MinTimestampMS   int64    `json:"min_timestamp_ms"`
	MaxTimestampMS   int64    `json:"max_timestamp_ms"`
	RecordCount      uint32   `json:"record_count"`
	BlockCount       uint32   `json:"block_count"`
	SizeBytes        string   `json:"size_bytes"`
	BlockIndexOffset string   `json:"block_index_offset"`
	BlockIndexLength uint32   `json:"block_index_length"`
	Codec            enumSpec `json:"codec"`
	HashAlgorithm    enumSpec `json:"hash_algorithm"`
	RecordFormat     enumSpec `json:"record_format"`
	SegmentHash      string   `json:"segment_hash"`
	TrailerHash      string   `json:"trailer_hash"`
}

type enumSpec struct {
	ID   uint16 `json:"id"`
	Name string `json:"name"`
}

type blockSpec struct {
	Offset         string `json:"offset"`
	StoredSize     uint32 `json:"stored_size"`
	RawSize        uint32 `json:"raw_size"`
	RecordCount    uint32 `json:"record_count"`
	BaseLSN        string `json:"base_lsn"`
	MinTimestampMS int64  `json:"min_timestamp_ms"`
	MaxTimestampMS int64  `json:"max_timestamp_ms"`
	BlockHash      string `json:"block_hash"`
}

type recordSpec struct {
	LSN         string       `json:"lsn"`
	TimestampMS int64        `json:"timestamp_ms"`
	Headers     []headerSpec `json:"headers"`
	ValueBase64 string       `json:"value_base64"`
}

type headerSpec struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
}

type vectorDefinition struct {
	name             string
	file             string
	streamID         string
	writerEpoch      uint64
	writerByteStable bool
	opts             segwriter.Options
	records          []segwriter.Record
}

func main() {
	out := flag.String("out", "", "output directory")
	flag.Parse()
	if *out == "" {
		fatalf("-out is required")
	}
	if segformat.Version != 2 {
		fatalf("generator is pinned to segment format version 2, current version is %d", segformat.Version)
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		fatalf("create output directory: %v", err)
	}

	manifest := corpusManifest{
		SchemaVersion:        corpusSchemaVersion,
		SegmentFormatVersion: segformat.Version,
	}
	for _, definition := range definitions() {
		vector, object, err := buildVector(definition)
		if err != nil {
			fatalf("build %s: %v", definition.name, err)
		}
		if err := os.WriteFile(filepath.Join(*out, definition.file), object, 0o644); err != nil {
			fatalf("write %s: %v", definition.file, err)
		}
		manifest.Vectors = append(manifest.Vectors, vector)
	}

	body, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		fatalf("marshal manifest: %v", err)
	}
	body = append(body, '\n')
	if err := os.WriteFile(filepath.Join(*out, "manifest.json"), body, 0o644); err != nil {
		fatalf("write manifest: %v", err)
	}
}

func definitions() []vectorDefinition {
	none := segwriter.DefaultOptions(7)
	none.Codec = segformat.CodecNone
	none.HashAlgo = segformat.HashCRC32C
	none.TargetBlockSize = 80
	none.PartSize = 128
	none.SealParallelism = 1
	none.BlockBufferCount = 3
	none.UploadParallelism = 1
	none.UploadQueueSize = 1
	none.SegmentUUID = id16(0x10)
	none.WriterTag = id16(0x40)
	none.CreatedUnixMS = 1_776_263_000_000

	zstd := segwriter.DefaultOptions(11)
	zstd.Codec = segformat.CodecZstd
	zstd.HashAlgo = segformat.HashXXH64
	zstd.TargetBlockSize = 320
	zstd.PartSize = 256
	zstd.SealParallelism = 1
	zstd.BlockBufferCount = 3
	zstd.UploadParallelism = 1
	zstd.UploadQueueSize = 1
	zstd.SegmentUUID = id16(0x70)
	zstd.WriterTag = id16(0xa0)
	zstd.CreatedUnixMS = 1_776_263_100_000

	return []vectorDefinition{
		{
			name:             "v2-none-crc32c",
			file:             "v2-none-crc32c.plseg",
			streamID:         "compatibility/v2",
			writerEpoch:      9,
			writerByteStable: true,
			opts:             none,
			records: []segwriter.Record{
				{LSN: 9_007_199_254_740_993, TimestampMS: 1_776_263_000_001, Value: []byte("alpha")},
				{
					LSN: 9_007_199_254_740_994, TimestampMS: 1_776_263_000_001,
					Headers: []segformat.Header{
						{Key: []byte("content-type"), Value: []byte("application/json")},
						{Key: []byte{0x00, 0x7f, 0xff}, Value: []byte{0x00, 0x01, 0xfe, 0xff}},
					},
					Value: []byte(`{"ok":true}`),
				},
				{
					LSN: 9_007_199_254_740_995, TimestampMS: 1_776_263_000_010,
					Headers: []segformat.Header{{Key: []byte("empty"), Value: nil}},
					Value:   nil,
				},
				{
					LSN: 9_007_199_254_740_996, TimestampMS: 1_776_263_000_011,
					Value: []byte{0x00, 0x01, 0x02, 0x7f, 0x80, 0xfe, 0xff},
				},
			},
		},
		{
			name:             "v2-zstd-xxh64",
			file:             "v2-zstd-xxh64.plseg",
			streamID:         "compatibility/v2",
			writerEpoch:      10,
			writerByteStable: false,
			opts:             zstd,
			records:          zstdRecords(),
		},
	}
}

func zstdRecords() []segwriter.Record {
	records := make([]segwriter.Record, 6)
	for i := range records {
		value := make([]byte, 180+i*17)
		for j := range value {
			value[j] = byte("ABCD"[(i+j)%4])
		}
		records[i] = segwriter.Record{
			LSN:         500 + uint64(i),
			TimestampMS: 1_776_263_100_001 + int64(i*3),
			Headers: []segformat.Header{
				{Key: []byte("kind"), Value: []byte("compressed")},
				{Key: []byte("ordinal"), Value: []byte(strconv.Itoa(i))},
			},
			Value: value,
		}
	}
	return records
}

func buildVector(def vectorDefinition) (corpusVector, []byte, error) {
	object, metadata, err := segwriter.Encode(context.Background(), def.records, def.opts)
	if err != nil {
		return corpusVector{}, nil, err
	}
	trailer, err := segformat.ParseTrailer(object[len(object)-segformat.TrailerSize:], uint64(len(object)))
	if err != nil {
		return corpusVector{}, nil, err
	}
	indexBytes := object[trailer.BlockIndexOffset : trailer.BlockIndexOffset+uint64(trailer.BlockIndexLength)]
	_, entries, err := segformat.ParseBlockIndex(indexBytes, trailer.HashAlgo)
	if err != nil {
		return corpusVector{}, nil, err
	}

	fileSum := sha256.Sum256(object)
	vector := corpusVector{
		Name:             def.name,
		File:             def.file,
		FileSHA256:       hex.EncodeToString(fileSum[:]),
		WriterByteStable: def.writerByteStable,
		Writer: writerSpec{
			TargetBlockSize: def.opts.TargetBlockSize,
			CreatedUnixMS:   def.opts.CreatedUnixMS,
			SegmentUUID:     hex.EncodeToString(def.opts.SegmentUUID[:]),
			WriterTag:       hex.EncodeToString(def.opts.WriterTag[:]),
		},
		SegmentRef: segmentRefSpec{
			URI:              "corpus://segformat/v2/" + def.file,
			StreamID:         def.streamID,
			Partition:        metadata.Partition,
			WriterEpoch:      decimal(def.writerEpoch),
			SegmentUUID:      hex.EncodeToString(metadata.SegmentUUID[:]),
			WriterTag:        hex.EncodeToString(def.opts.WriterTag[:]),
			BaseLSN:          decimal(metadata.BaseLSN),
			LastLSN:          decimal(metadata.LastLSN),
			MinTimestampMS:   metadata.MinTimestampMS,
			MaxTimestampMS:   metadata.MaxTimestampMS,
			RecordCount:      metadata.RecordCount,
			BlockCount:       metadata.BlockCount,
			SizeBytes:        decimal(metadata.SizeBytes),
			BlockIndexOffset: decimal(metadata.BlockIndexOffset),
			BlockIndexLength: metadata.BlockIndexLength,
			Codec:            enumSpec{ID: uint16(metadata.Codec), Name: metadata.Codec.String()},
			HashAlgorithm:    enumSpec{ID: uint16(metadata.HashAlgo), Name: metadata.HashAlgo.String()},
			RecordFormat: enumSpec{
				ID: uint16(trailer.RecordFormat), Name: trailer.RecordFormat.String(),
			},
			SegmentHash: hashHex(metadata.SegmentHash),
			TrailerHash: hashHex(metadata.TrailerHash),
		},
	}
	for _, entry := range entries {
		vector.Blocks = append(vector.Blocks, blockSpec{
			Offset:         decimal(entry.BlockOffset),
			StoredSize:     entry.StoredSize,
			RawSize:        entry.RawSize,
			RecordCount:    entry.RecordCount,
			BaseLSN:        decimal(entry.BaseLSN),
			MinTimestampMS: entry.MinTimestampMS,
			MaxTimestampMS: entry.MaxTimestampMS,
			BlockHash:      hashHex(entry.BlockHash),
		})
	}
	for _, record := range def.records {
		spec := recordSpec{
			LSN:         decimal(record.LSN),
			TimestampMS: record.TimestampMS,
			ValueBase64: base64.StdEncoding.EncodeToString(record.Value),
		}
		for _, header := range record.Headers {
			spec.Headers = append(spec.Headers, headerSpec{
				KeyBase64:   base64.StdEncoding.EncodeToString(header.Key),
				ValueBase64: base64.StdEncoding.EncodeToString(header.Value),
			})
		}
		if spec.Headers == nil {
			spec.Headers = []headerSpec{}
		}
		vector.Records = append(vector.Records, spec)
	}
	return vector, object, nil
}

func id16(start byte) [16]byte {
	var id [16]byte
	for i := range id {
		id[i] = start + byte(i)
	}
	return id
}

func decimal(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func hashHex(value uint64) string {
	return fmt.Sprintf("%016x", value)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gencorpus: "+format+"\n", args...)
	os.Exit(1)
}
