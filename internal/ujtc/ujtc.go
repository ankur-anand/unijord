// Package ujtc encodes and decodes immutable UJTC durability objects.
package ujtc

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/ankur-anand/unijord/internal/record"
	"github.com/cespare/xxhash/v2"
)

const (
	Version          uint16 = 1
	HeaderSize              = 128
	RecordHeaderSize        = 32
	HeaderEntrySize         = 8
	MaxObjectBytes   uint64 = 16 << 20
	MaxRecords              = 1 << 20
)

var (
	ErrInvalid   = errors.New("ujtc: invalid chunk")
	ErrNoRecords = errors.New("ujtc: no records")
)

var magic = [4]byte{'U', 'J', 'T', 'C'}

// Identity binds copied chunk bytes to the shard publication protocol that
// names them. WriterEpoch zero is reserved and rejected.
type Identity struct {
	NamespaceHash [32]byte
	Shard         uint32
	WriterEpoch   uint64
	Sequence      uint64
}

// Metadata describes authenticated fields derived from one complete UJTC
// object. ObjectHash is SHA-256 over the complete encoded object.
type Metadata struct {
	Identity
	RecordCount   uint32
	TimelineCount uint32
	BodyBytes     uint64
	BodyHash      uint64
	MinTimestamp  int64
	MaxTimestamp  int64
	ObjectHash    [32]byte
}

// Marshal deterministically encodes already-positioned records as one
// complete UJTC object. It does not publish or acknowledge the object.
func Marshal(identity Identity, records []record.Record) ([]byte, Metadata, error) {
	if identity.NamespaceHash == ([32]byte{}) {
		return nil, Metadata{}, fmt.Errorf("%w: zero namespace hash", ErrInvalid)
	}
	if identity.WriterEpoch == 0 {
		return nil, Metadata{}, fmt.Errorf("%w: zero writer epoch", ErrInvalid)
	}
	if len(records) == 0 {
		return nil, Metadata{}, ErrNoRecords
	}
	if len(records) > MaxRecords {
		return nil, Metadata{}, fmt.Errorf("%w: records=%d", ErrInvalid, len(records))
	}

	layout, err := measureRecords(records)
	if err != nil {
		return nil, Metadata{}, err
	}
	out := make([]byte, HeaderSize+layout.bodyBytes)
	offset := HeaderSize
	for _, item := range records {
		headersLength := encodedHeadersLength(item.Headers)
		total := RecordHeaderSize + len(item.TimelineKey) + headersLength + len(item.Value)
		header := out[offset : offset+RecordHeaderSize]
		binary.BigEndian.PutUint32(header[0:4], uint32(total))
		binary.BigEndian.PutUint16(header[4:6], uint16(len(item.TimelineKey)))
		binary.BigEndian.PutUint16(header[6:8], uint16(len(item.Headers)))
		binary.BigEndian.PutUint64(header[8:16], item.TimelineLSN)
		binary.BigEndian.PutUint64(header[16:24], uint64(item.TimestampMS))
		binary.BigEndian.PutUint32(header[24:28], uint32(headersLength))
		binary.BigEndian.PutUint32(header[28:32], uint32(len(item.Value)))
		offset += RecordHeaderSize
		offset += copy(out[offset:], item.TimelineKey)
		for _, applicationHeader := range item.Headers {
			entry := out[offset : offset+HeaderEntrySize]
			binary.BigEndian.PutUint16(entry[0:2], uint16(len(applicationHeader.Key)))
			binary.BigEndian.PutUint32(entry[4:8], uint32(len(applicationHeader.Value)))
			offset += HeaderEntrySize
			offset += copy(out[offset:], applicationHeader.Key)
			offset += copy(out[offset:], applicationHeader.Value)
		}
		offset += copy(out[offset:], item.Value)
	}
	if offset != len(out) {
		return nil, Metadata{}, fmt.Errorf("%w: measured bytes=%d encoded bytes=%d", ErrInvalid, len(out), offset)
	}

	body := out[HeaderSize:]
	bodyHash := xxhash.Sum64(body)
	copy(out[0:4], magic[:])
	binary.BigEndian.PutUint16(out[4:6], Version)
	binary.BigEndian.PutUint16(out[6:8], HeaderSize)
	binary.BigEndian.PutUint32(out[8:12], identity.Shard)
	binary.BigEndian.PutUint64(out[16:24], identity.WriterEpoch)
	binary.BigEndian.PutUint64(out[24:32], identity.Sequence)
	binary.BigEndian.PutUint32(out[32:36], uint32(len(records)))
	binary.BigEndian.PutUint32(out[36:40], layout.timelineCount)
	binary.BigEndian.PutUint64(out[40:48], uint64(layout.bodyBytes))
	binary.BigEndian.PutUint64(out[48:56], bodyHash)
	binary.BigEndian.PutUint64(out[56:64], uint64(layout.minTimestamp))
	binary.BigEndian.PutUint64(out[64:72], uint64(layout.maxTimestamp))
	binary.BigEndian.PutUint64(out[72:80], uint64(len(out)))
	copy(out[80:112], identity.NamespaceHash[:])
	binary.BigEndian.PutUint64(out[120:128], xxhash.Sum64(out[:120]))

	metadata := Metadata{
		Identity:      identity,
		RecordCount:   uint32(len(records)),
		TimelineCount: layout.timelineCount,
		BodyBytes:     uint64(layout.bodyBytes),
		BodyHash:      bodyHash,
		MinTimestamp:  layout.minTimestamp,
		MaxTimestamp:  layout.maxTimestamp,
		ObjectHash:    sha256.Sum256(out),
	}
	return out, metadata, nil
}

type marshalLayout struct {
	bodyBytes     int
	timelineCount uint32
	minTimestamp  int64
	maxTimestamp  int64
}

// measureRecords validates everything needed by the encoder before allocating
// the final object. This keeps invalid input from leaving a partially encoded
// large buffer and makes the following encoding pass allocation-free.
func measureRecords(records []record.Record) (marshalLayout, error) {
	states := make(map[string]timelineState)
	var layout marshalLayout
	for i, item := range records {
		if len(item.TimelineKey) == 0 || len(item.TimelineKey) > record.MaxTimelineKeyBytes {
			return marshalLayout{}, fmt.Errorf("%w: record=%d key bytes=%d", ErrInvalid, i, len(item.TimelineKey))
		}
		if item.TimelineLSN == math.MaxUint64 {
			return marshalLayout{}, fmt.Errorf("%w: record=%d reserved timeline LSN", ErrInvalid, i)
		}
		state, exists := states[string(item.TimelineKey)]
		if exists {
			if item.TimelineLSN != state.lsn+1 {
				return marshalLayout{}, fmt.Errorf("%w: key=%q lsn=%d want=%d", ErrInvalid, item.TimelineKey, item.TimelineLSN, state.lsn+1)
			}
			if item.TimestampMS < state.timestamp {
				return marshalLayout{}, fmt.Errorf("%w: key=%q timestamp=%d previous=%d", ErrInvalid, item.TimelineKey, item.TimestampMS, state.timestamp)
			}
		}
		states[string(item.TimelineKey)] = timelineState{lsn: item.TimelineLSN, timestamp: item.TimestampMS}
		if i == 0 || item.TimestampMS < layout.minTimestamp {
			layout.minTimestamp = item.TimestampMS
		}
		if i == 0 || item.TimestampMS > layout.maxTimestamp {
			layout.maxTimestamp = item.TimestampMS
		}

		headersLength, err := measureHeaders(item.Headers)
		if err != nil {
			return marshalLayout{}, fmt.Errorf("record=%d: %w", i, err)
		}
		if len(item.Value) > record.MaxRecordValueBytes {
			return marshalLayout{}, fmt.Errorf("%w: record=%d value bytes=%d", ErrInvalid, i, len(item.Value))
		}
		total := uint64(RecordHeaderSize) + uint64(len(item.TimelineKey)) + uint64(headersLength) + uint64(len(item.Value))
		if total > math.MaxUint32 || uint64(layout.bodyBytes)+total > MaxObjectBytes-HeaderSize {
			return marshalLayout{}, fmt.Errorf("%w: body exceeds %d bytes", ErrInvalid, MaxObjectBytes)
		}
		layout.bodyBytes += int(total)
	}
	layout.timelineCount = uint32(len(states))
	return layout, nil
}

// Unmarshal validates and decodes one complete UJTC object. The returned
// records own their key, header, and value bytes.
func Unmarshal(buf []byte) (Metadata, []record.Record, error) {
	if len(buf) < HeaderSize || !bytes.Equal(buf[0:4], magic[:]) {
		return Metadata{}, nil, fmt.Errorf("%w: header magic or size", ErrInvalid)
	}
	if binary.BigEndian.Uint16(buf[4:6]) != Version ||
		binary.BigEndian.Uint16(buf[6:8]) != HeaderSize ||
		!allZero(buf[12:16]) ||
		!allZero(buf[112:120]) ||
		binary.BigEndian.Uint64(buf[72:80]) != uint64(len(buf)) ||
		xxhash.Sum64(buf[:120]) != binary.BigEndian.Uint64(buf[120:128]) {
		return Metadata{}, nil, fmt.Errorf("%w: header metadata or hash", ErrInvalid)
	}
	bodyLength := binary.BigEndian.Uint64(buf[40:48])
	if bodyLength == 0 || bodyLength > MaxObjectBytes-HeaderSize || bodyLength != uint64(len(buf)-HeaderSize) {
		return Metadata{}, nil, fmt.Errorf("%w: body length=%d", ErrInvalid, bodyLength)
	}
	body := buf[HeaderSize:]
	bodyHash := binary.BigEndian.Uint64(buf[48:56])
	if xxhash.Sum64(body) != bodyHash {
		return Metadata{}, nil, fmt.Errorf("%w: body hash", ErrInvalid)
	}
	count := binary.BigEndian.Uint32(buf[32:36])
	// Every record needs a fixed header and at least one timeline-key byte.
	// Reject impossible counts before allocating the result slice so a small
	// malicious object cannot trigger a much larger allocation.
	if count == 0 || count > MaxRecords || uint64(count) > bodyLength/(RecordHeaderSize+1) {
		return Metadata{}, nil, fmt.Errorf("%w: record count=%d", ErrInvalid, count)
	}

	records := make([]record.Record, 0, count)
	offset := 0
	states := make(map[string]timelineState)
	for i := uint32(0); i < count; i++ {
		if len(body)-offset < RecordHeaderSize {
			return Metadata{}, nil, fmt.Errorf("%w: truncated record=%d", ErrInvalid, i)
		}
		header := body[offset : offset+RecordHeaderSize]
		total := binary.BigEndian.Uint32(header[0:4])
		keyLength := binary.BigEndian.Uint16(header[4:6])
		headerCount := binary.BigEndian.Uint16(header[6:8])
		headersLength := binary.BigEndian.Uint32(header[24:28])
		valueLength := binary.BigEndian.Uint32(header[28:32])
		want := uint64(RecordHeaderSize) + uint64(keyLength) + uint64(headersLength) + uint64(valueLength)
		if keyLength == 0 || keyLength > record.MaxTimelineKeyBytes ||
			valueLength > record.MaxRecordValueBytes || uint64(total) != want || want > uint64(len(body)-offset) {
			return Metadata{}, nil, fmt.Errorf("%w: record=%d lengths", ErrInvalid, i)
		}
		start := offset + RecordHeaderSize
		key := bytes.Clone(body[start : start+int(keyLength)])
		start += int(keyLength)
		headers, err := unmarshalHeaders(body[start:start+int(headersLength)], int(headerCount))
		if err != nil {
			return Metadata{}, nil, fmt.Errorf("record=%d: %w", i, err)
		}
		start += int(headersLength)
		item := record.Record{
			TimelineKey: key,
			TimelineLSN: binary.BigEndian.Uint64(header[8:16]),
			TimestampMS: int64(binary.BigEndian.Uint64(header[16:24])),
			Headers:     headers,
			Value:       bytes.Clone(body[start : start+int(valueLength)]),
		}
		if item.TimelineLSN == math.MaxUint64 {
			return Metadata{}, nil, fmt.Errorf("%w: reserved timeline LSN", ErrInvalid)
		}
		state, exists := states[string(key)]
		if exists && (item.TimelineLSN != state.lsn+1 || item.TimestampMS < state.timestamp) {
			return Metadata{}, nil, fmt.Errorf("%w: record=%d timeline order", ErrInvalid, i)
		}
		states[string(key)] = timelineState{lsn: item.TimelineLSN, timestamp: item.TimestampMS}
		records = append(records, item)
		offset += int(total)
	}
	if offset != len(body) {
		return Metadata{}, nil, fmt.Errorf("%w: trailing body bytes=%d", ErrInvalid, len(body)-offset)
	}

	var namespaceHash [32]byte
	copy(namespaceHash[:], buf[80:112])
	metadata := Metadata{
		Identity: Identity{
			NamespaceHash: namespaceHash,
			Shard:         binary.BigEndian.Uint32(buf[8:12]),
			WriterEpoch:   binary.BigEndian.Uint64(buf[16:24]),
			Sequence:      binary.BigEndian.Uint64(buf[24:32]),
		},
		RecordCount:   count,
		TimelineCount: binary.BigEndian.Uint32(buf[36:40]),
		BodyBytes:     bodyLength,
		BodyHash:      bodyHash,
		MinTimestamp:  int64(binary.BigEndian.Uint64(buf[56:64])),
		MaxTimestamp:  int64(binary.BigEndian.Uint64(buf[64:72])),
		ObjectHash:    sha256.Sum256(buf),
	}
	if metadata.NamespaceHash == ([32]byte{}) || metadata.WriterEpoch == 0 ||
		metadata.TimelineCount == 0 || metadata.TimelineCount != uint32(len(states)) {
		return Metadata{}, nil, fmt.Errorf("%w: namespace, writer epoch, or timeline count", ErrInvalid)
	}
	var actualMin, actualMax int64
	for i, item := range records {
		if i == 0 || item.TimestampMS < actualMin {
			actualMin = item.TimestampMS
		}
		if i == 0 || item.TimestampMS > actualMax {
			actualMax = item.TimestampMS
		}
	}
	if actualMin != metadata.MinTimestamp || actualMax != metadata.MaxTimestamp {
		return Metadata{}, nil, fmt.Errorf("%w: timestamp bounds", ErrInvalid)
	}
	return metadata, records, nil
}

type timelineState struct {
	lsn       uint64
	timestamp int64
}

func measureHeaders(headers []record.Header) (int, error) {
	if len(headers) > record.MaxHeaders {
		return 0, fmt.Errorf("%w: headers=%d", ErrInvalid, len(headers))
	}
	length := 0
	for _, header := range headers {
		if len(header.Key) > record.MaxHeaderKeyBytes || len(header.Value) > record.MaxHeaderValueBytes {
			return 0, fmt.Errorf("%w: header key=%d value=%d", ErrInvalid, len(header.Key), len(header.Value))
		}
		length += HeaderEntrySize + len(header.Key) + len(header.Value)
		if length > record.MaxHeaderBytes {
			return 0, fmt.Errorf("%w: header bytes=%d", ErrInvalid, length)
		}
	}
	return length, nil
}

func encodedHeadersLength(headers []record.Header) int {
	length := 0
	for _, header := range headers {
		length += HeaderEntrySize + len(header.Key) + len(header.Value)
	}
	return length
}

func unmarshalHeaders(buf []byte, count int) ([]record.Header, error) {
	if count > record.MaxHeaders || len(buf) > record.MaxHeaderBytes {
		return nil, fmt.Errorf("%w: header limits", ErrInvalid)
	}
	headers := make([]record.Header, 0, count)
	offset := 0
	for i := 0; i < count; i++ {
		if len(buf)-offset < HeaderEntrySize {
			return nil, fmt.Errorf("%w: truncated header=%d", ErrInvalid, i)
		}
		entry := buf[offset : offset+HeaderEntrySize]
		if !allZero(entry[2:4]) {
			return nil, fmt.Errorf("%w: header=%d reserved bytes", ErrInvalid, i)
		}
		keyLength := binary.BigEndian.Uint16(entry[0:2])
		valueLength := binary.BigEndian.Uint32(entry[4:8])
		offset += HeaderEntrySize
		if keyLength > record.MaxHeaderKeyBytes || valueLength > record.MaxHeaderValueBytes ||
			uint64(keyLength)+uint64(valueLength) > uint64(len(buf)-offset) {
			return nil, fmt.Errorf("%w: header=%d lengths", ErrInvalid, i)
		}
		headers = append(headers, record.Header{
			Key:   bytes.Clone(buf[offset : offset+int(keyLength)]),
			Value: bytes.Clone(buf[offset+int(keyLength) : offset+int(keyLength)+int(valueLength)]),
		})
		offset += int(keyLength) + int(valueLength)
	}
	if offset != len(buf) {
		return nil, fmt.Errorf("%w: trailing header bytes=%d", ErrInvalid, len(buf)-offset)
	}
	return headers, nil
}

func allZero(buf []byte) bool {
	for _, value := range buf {
		if value != 0 {
			return false
		}
	}
	return true
}
