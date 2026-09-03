# Unijord UJTC format

Binary format for one Unijord timeline chunk.

## 1. Encoding conventions

- All integers are big-endian.
- `u16`, `u32`, and `u64` are unsigned integers of the indicated width.
- `i64` is a two's-complement signed 64-bit integer.
- Timestamps are `i64` milliseconds since the Unix epoch.
- Byte strings are length-delimited and have no implicit character encoding.
- Reserved bytes MUST be zero. Readers MUST reject non-zero reserved bytes.
- `XXH64` is xxHash64 with seed `0`.
- `SHA-256` is the 32-byte SHA-256 digest defined by FIPS 180-4.
- `KiB` and `MiB` denote powers of two.

All length additions and conversions MUST be checked for integer overflow
before slicing, reading, or allocating memory.

## 2. Constants

| Name | Value |
| --- | ---: |
| `VERSION` | `1` |
| `HEADER_SIZE` | `128` |
| `RECORD_HEADER_SIZE` | `32` |
| `APPLICATION_HEADER_SIZE` | `8` |
| `MAX_OBJECT_SIZE` | `16 MiB` |
| `MAX_RECORD_COUNT` | `1,048,576` |
| `MAX_TIMELINE_KEY_SIZE` | `512` |
| `MAX_RECORD_VALUE_SIZE` | `4 MiB` |
| `MAX_APPLICATION_HEADER_COUNT` | `64` |
| `MAX_APPLICATION_HEADER_BYTES` | `256 KiB` |
| `MAX_APPLICATION_HEADER_KEY_SIZE` | `1,024` |
| `MAX_APPLICATION_HEADER_VALUE_SIZE` | `65,535` |
| `MAX_TIMELINE_LSN` | `2^64 - 2` |
| `RESERVED_TIMELINE_LSN` | `2^64 - 1` |

## 3. Object layout

```text
+----------------------------------+ offset 0
| Chunk header (128 B)       UJTC  |
+----------------------------------+ offset 128
| Record 0                         |
+----------------------------------+
| Record 1                         |
+----------------------------------+
| ...                              |
+----------------------------------+ object_size
```

The header and records are contiguous. There is no alignment padding, index,
or trailer. The records collectively form the body.

The complete object MUST satisfy:

```text
HEADER_SIZE + body_length = object_size
0 < object_size <= MAX_OBJECT_SIZE
```

## 4. Chunk header

Size: 128 bytes.

| Offset | Size | Field | Type | Required value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | ASCII `UJTC` |
| `4` | `2` | `version` | `u16` | `1` |
| `6` | `2` | `header_size` | `u16` | `128` |
| `8` | `4` | `shard` | `u32` | namespace-local shard |
| `12` | `4` | `reserved0` | bytes | all zero |
| `16` | `8` | `writer_epoch` | `u64` | greater than zero |
| `24` | `8` | `chunk_sequence` | `u64` | namespace/shard-local sequence |
| `32` | `4` | `record_count` | `u32` | number of body records |
| `36` | `4` | `timeline_count` | `u32` | number of distinct timeline keys |
| `40` | `8` | `body_length` | `u64` | body size in bytes |
| `48` | `8` | `body_xxh64` | `u64` | XXH64 of the complete body |
| `56` | `8` | `min_timestamp_ms` | `i64` | minimum record timestamp |
| `64` | `8` | `max_timestamp_ms` | `i64` | maximum record timestamp |
| `72` | `8` | `object_size` | `u64` | complete UJTC size |
| `80` | `32` | `namespace_hash` | bytes | section 5 |
| `112` | `8` | `reserved1` | bytes | all zero |
| `120` | `8` | `header_xxh64` | `u64` | XXH64 of header bytes `[0,120)` |

Header rules:

- `1 <= record_count <= MAX_RECORD_COUNT`;
- `1 <= timeline_count <= record_count`;
- `body_length > 0`;
- `min_timestamp_ms <= max_timestamp_ms`;
- `object_size == HEADER_SIZE + body_length`;
- `object_size` equals the actual number of object bytes;
- `XXH64(body) == body_xxh64`; and
- `XXH64(header[0:120]) == header_xxh64`.

## 5. Namespace and chunk identity

`namespace_hash` is:

```text
SHA-256(
    "unijord/timeline-index/namespace/v1\x00" ||
    u32_be(length(namespace_key)) ||
    namespace_key
)
```

`namespace_key` is the exact opaque namespace byte string. No Unicode,
case-folding, path, or whitespace normalization is applied. Its length is
measured in bytes. `namespace_hash` MUST NOT be 32 zero bytes.

The complete chunk identity is:

```text
(namespace_hash, shard, writer_epoch, chunk_sequence)
```

The exact namespace key is not stored in UJTC. A consumer opening UJTC through
an authenticated external reference MUST compare all four decoded identity
fields with that reference before accepting records.

The namespace digest contract is also specified independently in
[`../namespaceid/SPEC.md`](../namespaceid/SPEC.md).

## 6. Record encoding

Records occur in physical order immediately after the chunk header. Every
record begins with a 32-byte header.

| Offset | Size | Field | Type |
| ---: | ---: | --- | --- |
| `0` | `4` | `record_size` | `u32` |
| `4` | `2` | `timeline_key_length` | `u16` |
| `6` | `2` | `application_header_count` | `u16` |
| `8` | `8` | `timeline_lsn` | `u64` |
| `16` | `8` | `timestamp_ms` | `i64` |
| `24` | `4` | `application_headers_length` | `u32` |
| `28` | `4` | `value_length` | `u32` |

Variable regions immediately follow the record header:

```text
timeline_key | application_headers | value
```

The record MUST satisfy:

```text
record_size =
    RECORD_HEADER_SIZE +
    timeline_key_length +
    application_headers_length +
    value_length
```

Record rules:

- `1 <= timeline_key_length <= MAX_TIMELINE_KEY_SIZE`;
- `timeline_lsn <= MAX_TIMELINE_LSN`;
- `application_header_count <= MAX_APPLICATION_HEADER_COUNT`;
- `application_headers_length <= MAX_APPLICATION_HEADER_BYTES`;
- `value_length <= MAX_RECORD_VALUE_SIZE`;
- the complete record fits within the remaining body bytes; and
- every variable region has exactly its declared length.

Timeline keys and values are opaque byte strings. Timeline keys are compared
by exact byte equality.

## 7. Application-header encoding

`application_headers` is a concatenation of exactly
`application_header_count` entries. Each entry is:

| Offset | Size | Field | Type |
| ---: | ---: | --- | --- |
| `0` | `2` | `key_length` | `u16` |
| `2` | `2` | `reserved0` | bytes, zero |
| `4` | `4` | `value_length` | `u32` |
| `8` | `key_length` | `key` | bytes |
| varies | `value_length` | `value` | bytes |

Application-header rules:

- `key_length <= MAX_APPLICATION_HEADER_KEY_SIZE`;
- `value_length <= MAX_APPLICATION_HEADER_VALUE_SIZE`;
- every entry fits inside `application_headers_length`;
- decoding exactly `application_header_count` entries consumes exactly
  `application_headers_length` bytes; and
- `application_header_count == 0` if and only if
  `application_headers_length == 0`.

Empty keys and values are permitted. Duplicate keys are permitted. Entry order
is significant and MUST be preserved.

## 8. Timeline ordering

For every exact timeline key independently:

1. the first record in a chunk MAY have any `timeline_lsn` not greater than
   `MAX_TIMELINE_LSN`;
2. every later record for that key MUST have
   `timeline_lsn == previous_timeline_lsn + 1`; and
3. timestamps MUST be non-decreasing in physical encounter order.

Records for different timelines MAY be interleaved. No timestamp ordering is
required between different timelines. Continuity with records outside this
object is not asserted by UJTC.

After decoding the complete body:

- the number of exact distinct timeline keys MUST equal `timeline_count`;
- the minimum decoded timestamp MUST equal `min_timestamp_ms`;
- the maximum decoded timestamp MUST equal `max_timestamp_ms`;
- exactly `record_count` records MUST have been decoded; and
- no trailing body bytes may remain.

## 9. Canonical encoding

UJTC version 1 has one canonical encoding for a given chunk identity and
ordered record sequence:

- records retain their supplied order;
- application headers retain their supplied order;
- all lengths are exact;
- all reserved bytes are zero; and
- no padding or optional regions are present.

Two conforming encoders given the same identity and identical ordered records
MUST produce identical bytes.

## 10. Integrity and authentication boundary

`body_xxh64` and `header_xxh64` detect accidental corruption. XXH64 is not a
cryptographic authentication mechanism and does not establish object origin.

The SHA-256 of a complete UJTC object is:

```text
object_sha256 = SHA-256(object[0:object_size])
```

`object_sha256` is not stored inside UJTC. A publication protocol MAY carry it
in an authenticated external reference. When such a reference is used, the
consumer MUST verify the external object size, complete-object SHA-256, and
the identity tuple from section 5 before accepting records.

## 11. Decoder conformance

A conforming decoder MUST reject an object when any rule in sections 1–8 is
violated. In particular, it MUST reject:

- unknown magic, version, or header size;
- non-zero reserved bytes;
- zero namespace hash or writer epoch;
- mismatched object, body, record, or application-header lengths;
- count fields inconsistent with decoded contents;
- invalid timeline LSN or timestamp ordering;
- incorrect timestamp bounds; and
- incorrect XXH64 values.

A decoder MUST validate declared counts against the available body bytes
before using them to allocate memory. Malformed input MUST NOT cause unchecked
integer overflow, out-of-bounds access, or unbounded allocation.

## 12. Compatibility vector

Namespace key:

```text
ASCII "tenant-a"
namespace_hash = 3ff1ae2db4885a0ade2ecd7d6de3273370a6ac4d6e2f2f75643af361d1f288d7
```

Chunk identity:

```text
shard          = 17
writer_epoch   = 3
chunk_sequence = 9
```

Ordered records:

```text
1. key="run-a", timeline_lsn=0, timestamp_ms=-5,
   headers=[("kind", "open")], value="a0"
2. key="run-b", timeline_lsn=7, timestamp_ms=11,
   headers=[], value="b7"
3. key="run-a", timeline_lsn=1, timestamp_ms=12,
   headers=[], value="a1"
```

All quoted values above are ASCII byte strings. The canonical version-1
encoding has:

```text
object_size   = 261
object_sha256 = 057c84164871b5900b99bbd2acf0ea14ed03f532626b7ac23d5fa8e2b8757a57
```
