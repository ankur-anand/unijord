package segreader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ankur-anand/unijord/partitionlog/pmeta"
	"github.com/ankur-anand/unijord/partitionlog/segformat"
	"github.com/ankur-anand/unijord/partitionlog/segwriter"
)

const compatibilityCorpusDir = "../testdata/segformat/v2"

type compatibilityManifest struct {
	SchemaVersion        int                   `json:"schema_version"`
	SegmentFormatVersion uint16                `json:"segment_format_version"`
	Vectors              []compatibilityVector `json:"vectors"`
}

type compatibilityVector struct {
	Name             string                    `json:"name"`
	File             string                    `json:"file"`
	FileSHA256       string                    `json:"file_sha256"`
	WriterByteStable bool                      `json:"writer_byte_stable"`
	Writer           compatibilityWriterSpec   `json:"writer"`
	SegmentRef       compatibilitySegmentSpec  `json:"segment_ref"`
	Blocks           []compatibilityBlockSpec  `json:"blocks"`
	Records          []compatibilityRecordSpec `json:"records"`
}

type compatibilityWriterSpec struct {
	TargetBlockSize int    `json:"target_block_size"`
	CreatedUnixMS   int64  `json:"created_unix_ms"`
	SegmentUUID     string `json:"segment_uuid"`
	WriterTag       string `json:"writer_tag"`
}

type compatibilitySegmentSpec struct {
	URI              string                `json:"uri"`
	StreamID         string                `json:"stream_id"`
	Partition        uint32                `json:"partition"`
	WriterEpoch      string                `json:"writer_epoch"`
	SegmentUUID      string                `json:"segment_uuid"`
	WriterTag        string                `json:"writer_tag"`
	BaseLSN          string                `json:"base_lsn"`
	LastLSN          string                `json:"last_lsn"`
	MinTimestampMS   int64                 `json:"min_timestamp_ms"`
	MaxTimestampMS   int64                 `json:"max_timestamp_ms"`
	RecordCount      uint32                `json:"record_count"`
	BlockCount       uint32                `json:"block_count"`
	SizeBytes        string                `json:"size_bytes"`
	BlockIndexOffset string                `json:"block_index_offset"`
	BlockIndexLength uint32                `json:"block_index_length"`
	Codec            compatibilityEnumSpec `json:"codec"`
	HashAlgorithm    compatibilityEnumSpec `json:"hash_algorithm"`
	RecordFormat     compatibilityEnumSpec `json:"record_format"`
	SegmentHash      string                `json:"segment_hash"`
	TrailerHash      string                `json:"trailer_hash"`
}

type compatibilityEnumSpec struct {
	ID   uint16 `json:"id"`
	Name string `json:"name"`
}

type compatibilityBlockSpec struct {
	Offset         string `json:"offset"`
	StoredSize     uint32 `json:"stored_size"`
	RawSize        uint32 `json:"raw_size"`
	RecordCount    uint32 `json:"record_count"`
	BaseLSN        string `json:"base_lsn"`
	MinTimestampMS int64  `json:"min_timestamp_ms"`
	MaxTimestampMS int64  `json:"max_timestamp_ms"`
	BlockHash      string `json:"block_hash"`
}

type compatibilityRecordSpec struct {
	LSN         string                    `json:"lsn"`
	TimestampMS int64                     `json:"timestamp_ms"`
	Headers     []compatibilityHeaderSpec `json:"headers"`
	ValueBase64 string                    `json:"value_base64"`
}

type compatibilityHeaderSpec struct {
	KeyBase64   string `json:"key_base64"`
	ValueBase64 string `json:"value_base64"`
}

func TestSegmentCompatibilityCorpus(t *testing.T) {
	manifest := loadCompatibilityManifest(t)
	if manifest.SchemaVersion != 1 {
		t.Fatalf("schema_version = %d, want 1", manifest.SchemaVersion)
	}
	if manifest.SegmentFormatVersion != 2 {
		t.Fatalf("segment_format_version = %d, want 2", manifest.SegmentFormatVersion)
	}
	if len(manifest.Vectors) < 2 {
		t.Fatalf("vectors = %d, want at least 2", len(manifest.Vectors))
	}

	for _, vector := range manifest.Vectors {
		vector := vector
		t.Run(vector.Name, func(t *testing.T) {
			object := loadCompatibilityObject(t, vector)
			ref := compatibilityRef(t, vector.SegmentRef)
			store := newMemoryStore(map[string][]byte{ref.URI: object})
			reader, err := Open(context.Background(), store, ref, DefaultOptions())
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}

			assertCompatibilityMetadata(t, reader, vector)
			records, err := reader.Read(context.Background(), ref.BaseLSN, 0)
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			assertCompatibilityRecords(t, records, vector.Records)

			if vector.WriterByteStable {
				assertStableWriterBytes(t, vector, object)
			}
		})
	}
}

func loadCompatibilityManifest(t testing.TB) compatibilityManifest {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(compatibilityCorpusDir, "manifest.json"))
	if err != nil {
		t.Fatalf("read compatibility manifest: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var manifest compatibilityManifest
	if err := decoder.Decode(&manifest); err != nil {
		t.Fatalf("decode compatibility manifest: %v", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		t.Fatalf("compatibility manifest has trailing JSON: %v", err)
	}
	return manifest
}

func loadCompatibilityObject(t testing.TB, vector compatibilityVector) []byte {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(compatibilityCorpusDir, vector.File))
	if err != nil {
		t.Fatalf("read compatibility object %q: %v", vector.File, err)
	}
	want, err := hex.DecodeString(vector.FileSHA256)
	if err != nil || len(want) != sha256.Size {
		t.Fatalf("invalid file_sha256 %q", vector.FileSHA256)
	}
	got := sha256.Sum256(body)
	if !bytes.Equal(got[:], want) {
		t.Fatalf("sha256(%s) = %x, want %x", vector.File, got, want)
	}
	return body
}

func compatibilityRef(t testing.TB, spec compatibilitySegmentSpec) pmeta.SegmentRef {
	t.Helper()
	ref := pmeta.SegmentRef{
		URI:              spec.URI,
		StreamID:         spec.StreamID,
		Partition:        spec.Partition,
		WriterEpoch:      parseDecimal(t, "writer_epoch", spec.WriterEpoch),
		SegmentUUID:      parseID(t, "segment_uuid", spec.SegmentUUID),
		WriterTag:        parseID(t, "writer_tag", spec.WriterTag),
		BaseLSN:          parseDecimal(t, "base_lsn", spec.BaseLSN),
		LastLSN:          parseDecimal(t, "last_lsn", spec.LastLSN),
		MinTimestampMS:   spec.MinTimestampMS,
		MaxTimestampMS:   spec.MaxTimestampMS,
		RecordCount:      spec.RecordCount,
		BlockCount:       spec.BlockCount,
		SizeBytes:        parseDecimal(t, "size_bytes", spec.SizeBytes),
		BlockIndexOffset: parseDecimal(t, "block_index_offset", spec.BlockIndexOffset),
		BlockIndexLength: spec.BlockIndexLength,
		Codec:            segformat.Codec(spec.Codec.ID),
		HashAlgo:         segformat.HashAlgo(spec.HashAlgorithm.ID),
		SegmentHash:      parseHash(t, "segment_hash", spec.SegmentHash),
		TrailerHash:      parseHash(t, "trailer_hash", spec.TrailerHash),
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("SegmentRef.Validate() error = %v", err)
	}
	return ref
}

func assertCompatibilityMetadata(t testing.TB, reader *Reader, vector compatibilityVector) {
	t.Helper()
	ref := compatibilityRef(t, vector.SegmentRef)
	preamble := reader.Preamble()
	trailer := reader.Trailer()
	if preamble.Partition != ref.Partition || preamble.BaseLSN != ref.BaseLSN ||
		preamble.SegmentUUID != ref.SegmentUUID || preamble.WriterTag != ref.WriterTag ||
		preamble.Codec != ref.Codec || preamble.HashAlgo != ref.HashAlgo {
		t.Fatalf("preamble = %+v, does not match segment ref", preamble)
	}
	if trailer.CreatedUnixMS != vector.Writer.CreatedUnixMS {
		t.Fatalf("created_unix_ms = %d, want %d", trailer.CreatedUnixMS, vector.Writer.CreatedUnixMS)
	}
	if vector.SegmentRef.Codec.Name != ref.Codec.String() {
		t.Fatalf("codec name = %q, want %q", vector.SegmentRef.Codec.Name, ref.Codec.String())
	}
	if vector.SegmentRef.HashAlgorithm.Name != ref.HashAlgo.String() {
		t.Fatalf("hash algorithm name = %q, want %q", vector.SegmentRef.HashAlgorithm.Name, ref.HashAlgo.String())
	}
	if segformat.RecordFormat(vector.SegmentRef.RecordFormat.ID) != preamble.RecordFormat ||
		vector.SegmentRef.RecordFormat.Name != preamble.RecordFormat.String() ||
		trailer.RecordFormat != preamble.RecordFormat {
		t.Fatalf("record format = %+v, preamble=%s trailer=%s", vector.SegmentRef.RecordFormat, preamble.RecordFormat, trailer.RecordFormat)
	}

	entries := reader.BlockIndex()
	if len(entries) != len(vector.Blocks) {
		t.Fatalf("block index entries = %d, want %d", len(entries), len(vector.Blocks))
	}
	for i, spec := range vector.Blocks {
		entry := entries[i]
		if entry.BlockOffset != parseDecimal(t, "block offset", spec.Offset) ||
			entry.StoredSize != spec.StoredSize || entry.RawSize != spec.RawSize ||
			entry.RecordCount != spec.RecordCount ||
			entry.BaseLSN != parseDecimal(t, "block base_lsn", spec.BaseLSN) ||
			entry.MinTimestampMS != spec.MinTimestampMS || entry.MaxTimestampMS != spec.MaxTimestampMS ||
			entry.BlockHash != parseHash(t, "block_hash", spec.BlockHash) {
			t.Fatalf("block[%d] = %+v, want %+v", i, entry, spec)
		}
	}
}

func assertCompatibilityRecords(t testing.TB, got []Record, specs []compatibilityRecordSpec) {
	t.Helper()
	if len(got) != len(specs) {
		t.Fatalf("records = %d, want %d", len(got), len(specs))
	}
	for i, spec := range specs {
		wantValue := decodeBase64(t, "value_base64", spec.ValueBase64)
		if got[i].LSN != parseDecimal(t, "record lsn", spec.LSN) ||
			got[i].TimestampMS != spec.TimestampMS || !bytes.Equal(got[i].Value, wantValue) {
			t.Fatalf("record[%d] scalar fields do not match corpus", i)
		}
		if len(got[i].Headers) != len(spec.Headers) {
			t.Fatalf("record[%d] headers = %d, want %d", i, len(got[i].Headers), len(spec.Headers))
		}
		for j, header := range spec.Headers {
			wantKey := decodeBase64(t, "header key", header.KeyBase64)
			wantValue := decodeBase64(t, "header value", header.ValueBase64)
			if !bytes.Equal(got[i].Headers[j].Key, wantKey) || !bytes.Equal(got[i].Headers[j].Value, wantValue) {
				t.Fatalf("record[%d].headers[%d] does not match corpus", i, j)
			}
		}
	}
}

func assertStableWriterBytes(t testing.TB, vector compatibilityVector, want []byte) {
	t.Helper()
	ref := compatibilityRef(t, vector.SegmentRef)
	opts := segwriter.DefaultOptions(ref.Partition)
	opts.Codec = ref.Codec
	opts.HashAlgo = ref.HashAlgo
	opts.TargetBlockSize = vector.Writer.TargetBlockSize
	opts.PartSize = 128
	opts.SealParallelism = 1
	opts.BlockBufferCount = 3
	opts.UploadParallelism = 1
	opts.UploadQueueSize = 1
	opts.SegmentUUID = parseID(t, "writer segment_uuid", vector.Writer.SegmentUUID)
	opts.WriterTag = parseID(t, "writer writer_tag", vector.Writer.WriterTag)
	opts.CreatedUnixMS = vector.Writer.CreatedUnixMS

	records := make([]segwriter.Record, len(vector.Records))
	for i, spec := range vector.Records {
		records[i] = segwriter.Record{
			LSN:         parseDecimal(t, "writer record lsn", spec.LSN),
			TimestampMS: spec.TimestampMS,
			Value:       decodeBase64(t, "writer value", spec.ValueBase64),
		}
		for _, header := range spec.Headers {
			records[i].Headers = append(records[i].Headers, segformat.Header{
				Key:   decodeBase64(t, "writer header key", header.KeyBase64),
				Value: decodeBase64(t, "writer header value", header.ValueBase64),
			})
		}
	}
	got, _, err := segwriter.Encode(context.Background(), records, opts)
	if err != nil {
		t.Fatalf("segwriter.Encode() error = %v", err)
	}
	if !bytes.Equal(got, want) {
		gotHash := sha256.Sum256(got)
		wantHash := sha256.Sum256(want)
		t.Fatalf("writer bytes changed: sha256=%x, want %x; update the format version or justify regenerating the corpus", gotHash, wantHash)
	}
}

func parseDecimal(t testing.TB, field string, value string) uint64 {
	t.Helper()
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", field, value, err)
	}
	return parsed
}

func parseHash(t testing.TB, field string, value string) uint64 {
	t.Helper()
	if len(value) != 16 {
		t.Fatalf("%s=%q is not 16 hex characters", field, value)
	}
	parsed, err := strconv.ParseUint(value, 16, 64)
	if err != nil {
		t.Fatalf("parse %s=%q: %v", field, value, err)
	}
	return parsed
}

func parseID(t testing.TB, field string, value string) [16]byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 16 {
		t.Fatalf("%s=%q is not a 16-byte hex value", field, value)
	}
	var id [16]byte
	copy(id[:], decoded)
	return id
}

func decodeBase64(t testing.TB, field string, value string) []byte {
	t.Helper()
	decoded, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		t.Fatalf("decode %s=%q: %v", field, value, err)
	}
	return decoded
}

func compatibilityRefFromObject(object []byte) (pmeta.SegmentRef, bool) {
	if len(object) < segformat.FilePreambleSize+segformat.TrailerSize {
		return pmeta.SegmentRef{}, false
	}
	preamble, err := segformat.ParseFilePreamble(object[:segformat.FilePreambleSize])
	if err != nil {
		return pmeta.SegmentRef{}, false
	}
	trailer, err := segformat.ParseTrailer(object[len(object)-segformat.TrailerSize:], uint64(len(object)))
	if err != nil {
		return pmeta.SegmentRef{}, false
	}
	ref := pmeta.SegmentRef{
		URI:              "memory://compatibility-fuzz",
		Partition:        trailer.Partition,
		WriterEpoch:      1,
		SegmentUUID:      trailer.SegmentUUID,
		WriterTag:        trailer.WriterTag,
		BaseLSN:          trailer.BaseLSN,
		LastLSN:          trailer.LastLSN,
		MinTimestampMS:   trailer.MinTimestampMS,
		MaxTimestampMS:   trailer.MaxTimestampMS,
		RecordCount:      trailer.RecordCount,
		BlockCount:       trailer.BlockCount,
		SizeBytes:        trailer.TotalSize,
		BlockIndexOffset: trailer.BlockIndexOffset,
		BlockIndexLength: trailer.BlockIndexLength,
		Codec:            trailer.Codec,
		HashAlgo:         trailer.HashAlgo,
		SegmentHash:      trailer.SegmentHash,
		TrailerHash:      trailer.TrailerHash,
	}
	if preamble.Partition != ref.Partition || preamble.SegmentUUID != ref.SegmentUUID || preamble.WriterTag != ref.WriterTag {
		return pmeta.SegmentRef{}, false
	}
	if err := ref.Validate(); err != nil {
		return pmeta.SegmentRef{}, false
	}
	return ref, true
}

func compatibilityCorpusPath(name string) string {
	return filepath.Join(compatibilityCorpusDir, name)
}
