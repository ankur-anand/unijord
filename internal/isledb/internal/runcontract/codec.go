// Package runcontract defines the E00 logical codecs. It contains no ingestion,
// storage, allocation-policy, or publication runtime. The normative contract is
// runfile/KAFKA_RUNFILE_INGESTION_DESIGN.md, section 32.
package runcontract

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	Events           = 1
	Heads            = 2
	Append           = 1
	Seal             = 2
	MaxTimelineBytes = 512
	MaxKeyBytes      = 1074
	MaxValueBytes    = 16 << 20
	MaxHeaders       = 4096
	MaxSequence      = uint64(1<<56 - 1)
	EventFixedBytes  = 38
	HeadBytes        = 32
)

var (
	ErrEncoding = errors.New("runcontract: invalid or noncanonical encoding")
	ErrLimit    = errors.New("runcontract: length or coordinate limit")
)

type Key struct {
	Kind      uint8
	Namespace [32]byte
	Shard     uint32
	Timeline  []byte
	LSN       uint64 // Events only; Heads requires zero.
}

// EncodeKey writes into caller-owned space; the returned slice aliases dst.
// Validation and the capacity check precede every write. dst must not overlap
// the input timeline; the later iterator owns a separate reusable key buffer.
func EncodeKey(dst []byte, k Key) ([]byte, error) {
	if k.Namespace == [32]byte{} || (k.Kind != Events && k.Kind != Heads) ||
		(k.Kind == Events && (k.LSN == 0 || k.LSN == math.MaxUint64)) ||
		(k.Kind == Heads && k.LSN != 0) {
		return nil, ErrEncoding
	}
	if len(k.Timeline) == 0 || len(k.Timeline) > MaxTimelineBytes {
		return nil, ErrLimit
	}
	n := 42 + len(k.Timeline)
	for _, b := range k.Timeline {
		if b == 0 {
			n++
		}
	}
	if k.Kind == Events {
		n += 8
	}
	if len(dst) < n {
		return nil, ErrLimit
	}
	b := dst[:n]
	copy(b, "UJ")
	b[2], b[3] = 1, k.Kind
	copy(b[4:36], k.Namespace[:])
	binary.BigEndian.PutUint32(b[36:40], k.Shard)
	p := 40
	for _, v := range k.Timeline {
		b[p] = v
		p++
		if v == 0 {
			b[p] = 255
			p++
		}
	}
	b[p], b[p+1] = 0, 0
	p += 2
	if k.Kind == Events {
		binary.BigEndian.PutUint64(b[p:], k.LSN)
	}
	return b, nil
}

// DecodeKey returns an owned timeline, bounded by MaxTimelineBytes.
func DecodeKey(b []byte) (Key, error) {
	var k Key
	if len(b) < 43 || len(b) > MaxKeyBytes || string(b[:2]) != "UJ" || b[2] != 1 || (b[3] != Events && b[3] != Heads) {
		return k, ErrEncoding
	}
	k.Kind = b[3]
	copy(k.Namespace[:], b[4:36])
	k.Shard = binary.BigEndian.Uint32(b[36:40])
	if k.Namespace == [32]byte{} {
		return Key{}, ErrEncoding
	}
	var timeline [MaxTimelineBytes]byte
	n, p := 0, 40
	for p < len(b) {
		v := b[p]
		p++
		if v == 0 {
			if p == len(b) {
				return Key{}, ErrEncoding
			}
			x := b[p]
			p++
			if x == 0 {
				if n == 0 {
					return Key{}, ErrEncoding
				}
				if k.Kind == Events {
					if len(b)-p != 8 {
						return Key{}, ErrEncoding
					}
					k.LSN = binary.BigEndian.Uint64(b[p:])
					if k.LSN == 0 || k.LSN == math.MaxUint64 {
						return Key{}, ErrEncoding
					}
				} else if p != len(b) {
					return Key{}, ErrEncoding
				}
				k.Timeline = append([]byte(nil), timeline[:n]...)
				return k, nil
			}
			if x != 255 {
				return Key{}, ErrEncoding
			}
		}
		if n == MaxTimelineBytes {
			return Key{}, ErrLimit
		}
		timeline[n] = v
		n++
	}
	return Key{}, ErrEncoding
}

type Header struct{ Key, Value []byte }

type Event struct {
	Kind             uint8
	TimestampPresent bool
	Timestamp        int64 // Exact source milliseconds, two's complement, no clamping.
	Offset           uint64
	LeaderEpoch      int32    // -1 means unavailable.
	Payload          []byte   // nil and empty are distinct.
	Headers          []Header // Ordered; duplicate and empty keys are preserved.
	Annotations      []byte   // Opaque source annotation bytes; nil and empty equivalent.
}

func validEvent(e Event) bool {
	return (e.Kind == Append || e.Kind == Seal) && (e.TimestampPresent || e.Timestamp == 0) && e.Offset < math.MaxInt64 && e.LeaderEpoch >= -1
}

// EventSize admits all variable lengths before the caller reserves its arena.
func EventSize(e Event) (int, error) {
	if !validEvent(e) {
		return 0, ErrEncoding
	}
	if len(e.Headers) > MaxHeaders {
		return 0, ErrLimit
	}
	n := EventFixedBytes
	add := func(v int) bool {
		if v > MaxValueBytes-n {
			return false
		}
		n += v
		return true
	}
	if !add(len(e.Payload)) || !add(len(e.Annotations)) {
		return 0, ErrLimit
	}
	for _, h := range e.Headers {
		if len(h.Key) > math.MaxUint16 || !add(6) || !add(len(h.Key)) || !add(len(h.Value)) {
			return 0, ErrLimit
		}
	}
	return n, nil
}

func nullableLength(b []byte) uint32 {
	if b == nil {
		return math.MaxUint32
	}
	return uint32(len(b))
}

// EncodeEvent supports the later direct borrowed-record-to-arena copy. No
// destination bytes are changed on invalid input or insufficient space. Source
// slices must not overlap dst (borrowed Kafka bytes and owned slot memory).
func EncodeEvent(dst []byte, e Event) ([]byte, error) {
	n, err := EventSize(e)
	if err != nil {
		return nil, err
	}
	if len(dst) < n {
		return nil, ErrLimit
	}
	b := dst[:n]
	copy(b, "UJEV")
	binary.BigEndian.PutUint16(b[4:6], 1)
	b[6], b[7] = e.Kind, 0
	if e.TimestampPresent {
		b[7] = 1
	}
	binary.BigEndian.PutUint64(b[8:16], uint64(e.Timestamp))
	binary.BigEndian.PutUint64(b[16:24], e.Offset)
	binary.BigEndian.PutUint32(b[24:28], uint32(e.LeaderEpoch))
	binary.BigEndian.PutUint32(b[28:32], nullableLength(e.Payload))
	binary.BigEndian.PutUint16(b[32:34], uint16(len(e.Headers)))
	binary.BigEndian.PutUint32(b[34:38], uint32(len(e.Annotations)))
	p := 38
	p += copy(b[p:], e.Payload)
	for _, h := range e.Headers {
		binary.BigEndian.PutUint16(b[p:p+2], uint16(len(h.Key)))
		binary.BigEndian.PutUint32(b[p+2:p+6], nullableLength(h.Value))
		p += 6
		p += copy(b[p:], h.Key)
		p += copy(b[p:], h.Value)
	}
	copy(b[p:], e.Annotations)
	return b, nil
}

// DecodeEvent borrows payload/header/annotation bytes. It validates the complete
// framing before allocating at most MaxHeaders descriptors, never payload bytes.
func DecodeEvent(b []byte) (Event, error) {
	if len(b) < EventFixedBytes || len(b) > MaxValueBytes || string(b[:4]) != "UJEV" || binary.BigEndian.Uint16(b[4:6]) != 1 || b[7] > 1 {
		return Event{}, ErrEncoding
	}
	e := Event{Kind: b[6], TimestampPresent: b[7] == 1, Timestamp: int64(binary.BigEndian.Uint64(b[8:16])), Offset: binary.BigEndian.Uint64(b[16:24]), LeaderEpoch: int32(binary.BigEndian.Uint32(b[24:28]))}
	if !validEvent(e) {
		return Event{}, ErrEncoding
	}
	count := int(binary.BigEndian.Uint16(b[32:34]))
	if count > MaxHeaders {
		return Event{}, ErrLimit
	}
	// A checked slice consumes at most the remaining input, before int conversion.
	take := func(p *int, n uint32, nullable bool) ([]byte, bool) {
		if nullable && n == math.MaxUint32 {
			return nil, true
		}
		if uint64(n) > uint64(len(b)-*p) {
			return nil, false
		}
		out := b[*p : *p+int(n) : *p+int(n)]
		*p += int(n)
		return out, true
	}
	p := EventFixedBytes
	var ok bool
	e.Payload, ok = take(&p, binary.BigEndian.Uint32(b[28:32]), true)
	if !ok {
		return Event{}, ErrEncoding
	}
	headerStart := p
	for i := 0; i < count; i++ {
		if len(b)-p < 6 {
			return Event{}, ErrEncoding
		}
		kl, vl := binary.BigEndian.Uint16(b[p:p+2]), binary.BigEndian.Uint32(b[p+2:p+6])
		p += 6
		if _, ok = take(&p, uint32(kl), false); !ok {
			return Event{}, ErrEncoding
		}
		if _, ok = take(&p, vl, true); !ok {
			return Event{}, ErrEncoding
		}
	}
	e.Annotations, ok = take(&p, binary.BigEndian.Uint32(b[34:38]), false)
	if !ok || p != len(b) {
		return Event{}, ErrEncoding
	}
	if count != 0 {
		e.Headers = make([]Header, count)
	}
	p = headerStart
	for i := range e.Headers {
		kl, vl := binary.BigEndian.Uint16(b[p:p+2]), binary.BigEndian.Uint32(b[p+2:p+6])
		p += 6
		e.Headers[i].Key, _ = take(&p, uint32(kl), false)
		e.Headers[i].Value, _ = take(&p, vl, true)
	}
	return e, nil
}

type Head struct {
	Sealed           bool
	TimestampPresent bool
	Timestamp        int64
	NextLSN          uint64
	LastOffset       uint64
}

func validHead(h Head) bool {
	return h.NextLSN >= 2 && h.LastOffset < math.MaxInt64 && (h.TimestampPresent || h.Timestamp == 0)
}

func EncodeHead(dst []byte, h Head) ([]byte, error) {
	if !validHead(h) {
		return nil, ErrEncoding
	}
	if len(dst) < HeadBytes {
		return nil, ErrLimit
	}
	b := dst[:HeadBytes]
	copy(b, "UJHD")
	binary.BigEndian.PutUint16(b[4:6], 1)
	b[6], b[7] = 0, 0
	if h.Sealed {
		b[6] = 1
	}
	if h.TimestampPresent {
		b[7] = 1
	}
	binary.BigEndian.PutUint64(b[8:16], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(b[16:24], h.NextLSN)
	binary.BigEndian.PutUint64(b[24:32], h.LastOffset)
	return b, nil
}

func DecodeHead(b []byte) (Head, error) {
	if len(b) != HeadBytes || string(b[:4]) != "UJHD" || binary.BigEndian.Uint16(b[4:6]) != 1 || b[6] > 1 || b[7] > 1 {
		return Head{}, ErrEncoding
	}
	h := Head{Sealed: b[6] == 1, TimestampPresent: b[7] == 1, Timestamp: int64(binary.BigEndian.Uint64(b[8:16])), NextLSN: binary.BigEndian.Uint64(b[16:24]), LastOffset: binary.BigEndian.Uint64(b[24:32])}
	if !validHead(h) {
		return Head{}, ErrEncoding
	}
	return h, nil
}

// SequenceRange is arithmetic only: record i gets base+i; a final head reuses
// its timeline's last event sequence. next=2^56 is an exhausted cursor.
func SequenceRange(base, count uint64) (hi, next uint64, err error) {
	if base == 0 || base > MaxSequence || count == 0 || count > math.MaxUint32 || count > MaxSequence-base+1 {
		return 0, 0, ErrLimit
	}
	return base + count - 1, base + count, nil
}

// AdvanceLSN never assigns MaxUint64, which is the exhausted next-LSN cursor.
func AdvanceLSN(next uint64) (lsn, resulting uint64, err error) {
	if next == 0 || next == math.MaxUint64 {
		return 0, 0, ErrLimit
	}
	return next, next + 1, nil
}
