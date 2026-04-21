# segformat

Byte format for one immutable partition segment.

All integers are big-endian. Timestamps are signed 64-bit milliseconds since
the Unix epoch.

## Layout

```text
+-------------------------------+  offset 0
| File preamble (64 B)          |
+-------------------------------+
| Block preamble (64 B)         |
| Block 0 stored bytes          |
+-------------------------------+
| Block preamble (64 B)         |
| Block 1 stored bytes          |
+-------------------------------+
| ...                           |
+-------------------------------+
| Block preamble (64 B)         |
| Block N-1 stored bytes        |
+-------------------------------+
| Index preamble (64 B)         |
| Block index entries           |  N * 64 B
+-------------------------------+
| Trailer (192 B)               |  last 192 bytes
+-------------------------------+
```

Regions are contiguous. There is no padding between regions.

## Constants

| Name | Value |
| --- | ---: |
| `VERSION` | `2` |
| `FILE_PREAMBLE_SIZE` | `64` |
| `BLOCK_PREAMBLE_SIZE` | `64` |
| `INDEX_PREAMBLE_SIZE` | `64` |
| `BLOCK_INDEX_ENTRY_SIZE` | `64` |
| `TRAILER_SIZE` | `192` |
| `MAX_RAW_BLOCK_SIZE` | `16 MiB` |
| `MAX_STORED_BLOCK_SIZE` | `20 MiB` |
| `MAX_RECORD_VALUE_LEN` | `4 MiB` |
| `MAX_BLOCK_COUNT` | `67,108,862` |
| `MAX_RECORD_COUNT` | `2^32 - 1` |

`MAX_BLOCK_COUNT` is the largest block count whose index region fits in a
`u32 block_index_length`.

## Magic Values

| Region | Magic |
| --- | --- |
| File preamble | `PLSG` |
| Block preamble | `PLBK` |
| Index preamble | `PLIX` |
| Trailer | `PLFT` |

## Codecs

| Value | Name | Meaning |
| ---: | --- | --- |
| `0` | `none` | stored bytes equal raw bytes |
| `1` | `zstd` | one independent zstd frame per block |

For `none`, `stored_size == raw_size`.

For `zstd`, frames must not require a dictionary, and readers must verify the
decompressed size equals `raw_size`.

## Hash Algorithms

| Value | Name | Meaning |
| ---: | --- | --- |
| `0` | `crc32c` | CRC-32C Castagnoli polynomial `0x1EDC6F41`, zero-extended to `u64` |
| `1` | `xxh64` | XXH64, seed `0` |

The hash algorithm is segment-wide.

## File Preamble

Size: 64 bytes.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLSG` |
| `4` | `2` | `version` | `u16` | `2` |
| `6` | `2` | `preamble_len` | `u16` | `64` |
| `8` | `4` | `flags` | `u32` | `0` |
| `12` | `4` | `partition` | `u32` | partition id |
| `16` | `2` | `codec` | `u16` | codec table |
| `18` | `2` | `hash_algo` | `u16` | hash table |
| `20` | `2` | `record_format` | `u16` | `1` |
| `22` | `2` | `reserved0` | `u16` | `0` |
| `24` | `8` | `base_lsn` | `u64` | first LSN |
| `32` | `16` | `segment_uuid` | bytes | opaque id |
| `48` | `16` | `writer_tag` | bytes | opaque writer id |

The file preamble contains only fields known before block bytes are emitted.
Final counts and sizes are in the trailer.

## Block Preamble

Size: 64 bytes. One block preamble appears before each stored block payload.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLBK` |
| `4` | `2` | `preamble_len` | `u16` | `64` |
| `6` | `2` | `flags` | `u16` | `0` |
| `8` | `4` | `stored_size` | `u32` | payload bytes after preamble |
| `12` | `4` | `raw_size` | `u32` | bytes after decompression |
| `16` | `4` | `record_count` | `u32` | records in block |
| `20` | `4` | `reserved0` | `u32` | `0` |
| `24` | `8` | `base_lsn` | `u64` | first LSN in block |
| `32` | `8` | `min_timestamp_ms` | `i64` | min record timestamp |
| `40` | `8` | `max_timestamp_ms` | `i64` | max record timestamp |
| `48` | `8` | `block_hash` | `u64` | hash of stored bytes |
| `56` | `8` | `reserved1` | `u64` | `0` |

Rules:

- `stored_size > 0`
- `stored_size <= MAX_STORED_BLOCK_SIZE`
- `raw_size > 0`
- `raw_size <= MAX_RAW_BLOCK_SIZE`
- `record_count > 0`
- `min_timestamp_ms <= max_timestamp_ms`
- `hash(stored_bytes) == block_hash`

The writer must seal the block before writing the preamble, because
`stored_size` and `block_hash` are only known after compression.

## Raw Records

Record format `1`:

```text
record =>
  timestamp_ms: i64
  value_len:    u32
  value:        bytes[value_len]
```

LSN is implicit:

```text
lsn = block.base_lsn + record_index
```

Rules:

- decoded bytes consumed must equal `raw_size`
- decoded record count must equal `record_count`
- `value_len <= MAX_RECORD_VALUE_LEN`
- timestamps are non-decreasing within a block
- timestamps are inside `[min_timestamp_ms, max_timestamp_ms]`

## Index Preamble

Size: 64 bytes.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLIX` |
| `4` | `2` | `preamble_len` | `u16` | `64` |
| `6` | `2` | `flags` | `u16` | `0` |
| `8` | `4` | `entry_size` | `u32` | `64` |
| `12` | `4` | `entry_count` | `u32` | block count |
| `16` | `8` | `index_hash` | `u64` | hash of entry bytes |
| `24` | `40` | `reserved0` | bytes | all zero |

## Block Index Entry

Size: 64 bytes.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `8` | `block_offset` | `u64` | absolute offset of block preamble |
| `8` | `4` | `stored_size` | `u32` | same as block preamble |
| `12` | `4` | `raw_size` | `u32` | same as block preamble |
| `16` | `4` | `record_count` | `u32` | same as block preamble |
| `20` | `4` | `reserved0` | `u32` | `0` |
| `24` | `8` | `base_lsn` | `u64` | same as block preamble |
| `32` | `8` | `min_timestamp_ms` | `i64` | same as block preamble |
| `40` | `8` | `max_timestamp_ms` | `i64` | same as block preamble |
| `48` | `8` | `block_hash` | `u64` | same as block preamble |
| `56` | `8` | `reserved1` | `u64` | `0` |

Index rules:

- `entry_count == trailer.block_count`
- `index_hash == hash(all entry bytes)`
- entries are sorted by `base_lsn`
- adjacent LSN ranges are contiguous
- adjacent block byte ranges are contiguous:
  `entry[i].block_offset + 64 + entry[i].stored_size == entry[i+1].block_offset`
- block timestamp ranges are non-decreasing

## Trailer

Size: 192 bytes. The trailer is the last region in the object.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLFT` |
| `4` | `2` | `trailer_len` | `u16` | `192` |
| `6` | `2` | `version` | `u16` | `2` |
| `8` | `4` | `flags` | `u32` | `0` |
| `12` | `4` | `partition` | `u32` | partition id |
| `16` | `2` | `codec` | `u16` | same as preamble |
| `18` | `2` | `hash_algo` | `u16` | same as preamble |
| `20` | `2` | `record_format` | `u16` | same as preamble |
| `22` | `2` | `reserved0` | `u16` | `0` |
| `24` | `8` | `base_lsn` | `u64` | first segment LSN |
| `32` | `8` | `last_lsn` | `u64` | last segment LSN |
| `40` | `8` | `min_timestamp_ms` | `i64` | min segment timestamp |
| `48` | `8` | `max_timestamp_ms` | `i64` | max segment timestamp |
| `56` | `4` | `record_count` | `u32` | total records |
| `60` | `4` | `block_count` | `u32` | total blocks |
| `64` | `8` | `block_index_offset` | `u64` | absolute offset of index preamble |
| `72` | `4` | `block_index_length` | `u32` | index preamble + entries |
| `76` | `4` | `reserved1` | `u32` | `0` |
| `80` | `8` | `total_size` | `u64` | object size |
| `88` | `8` | `created_unix_ms` | `i64` | advisory |
| `96` | `16` | `segment_uuid` | bytes | same as preamble |
| `112` | `16` | `writer_tag` | bytes | same as preamble |
| `128` | `8` | `segment_hash` | `u64` | hash before trailer |
| `136` | `8` | `trailer_hash` | `u64` | hash of trailer with this field zeroed |
| `144` | `48` | `reserved2` | bytes | all zero |

Trailer rules:

- `last_lsn >= base_lsn`
- `record_count == last_lsn - base_lsn + 1`
- `block_count > 0`
- `record_count > 0`
- `min_timestamp_ms <= max_timestamp_ms`
- `block_index_length == 64 + block_count * 64`
- `block_index_offset + block_index_length + 192 == total_size`
- actual object size equals `total_size`
- `trailer_hash == hash(trailer bytes with [136,144) zeroed)`

`segment_hash` covers:

```text
bytes[0 : block_index_offset + block_index_length]
```

The trailer is not included in `segment_hash`.

## Validation

Reserved fields must be zero. Readers must reject non-zero reserved fields.

Validation can be partial:

- Trailer validation: checks the final 192 bytes and object size.
- Index validation: checks trailer plus index region.
- Block validation: checks one block preamble, stored bytes, and raw records.
- Full verification: checks the full object and `segment_hash`.

## Sequential Read

```text
read file preamble
loop:
  read 64-byte region preamble
  if magic == PLBK:
    read stored_size bytes
    verify block_hash
    decompress
    yield records
  if magic == PLIX:
    stop
```

A sequential reader does not need the trailer or index to yield records.

## Random Read

```text
read trailer
read index region
binary-search index by LSN or timestamp
read target block range
validate block
decode records
```

## Finalization

A segment object is finalized only if the final 192 bytes contain a valid
trailer and `total_size` matches the object size.

Partial uploads and truncated objects must be rejected.

## Quick Reference

```text
File preamble (64 B)
  0  PLSG                 32  segment_uuid[16]
  4  version        u16   48  writer_tag[16]
  6  preamble_len   u16
  8  flags          u32
 12  partition      u32
 16  codec          u16
 18  hash_algo      u16
 20  record_format  u16
 22  reserved       u16
 24  base_lsn       u64

Block preamble (64 B)
  0  PLBK                 32  min_timestamp_ms i64
  4  preamble_len   u16   40  max_timestamp_ms i64
  6  flags          u16   48  block_hash       u64
  8  stored_size    u32   56  reserved         u64
 12  raw_size       u32
 16  record_count   u32
 20  reserved       u32
 24  base_lsn       u64

Index preamble (64 B)
  0  PLIX                 16  index_hash       u64
  4  preamble_len   u16   24  reserved[40]
  6  flags          u16
  8  entry_size     u32
 12  entry_count    u32

Index entry (64 B)
  0  block_offset   u64   32  min_timestamp_ms i64
  8  stored_size    u32   40  max_timestamp_ms i64
 12  raw_size       u32   48  block_hash       u64
 16  record_count   u32   56  reserved         u64
 20  reserved       u32
 24  base_lsn       u64

Trailer (192 B)
  0  PLFT                 80  total_size       u64
  4  trailer_len    u16   88  created_unix_ms  i64
  6  version        u16   96  segment_uuid[16]
  8  flags          u32  112  writer_tag[16]
 12  partition      u32  128  segment_hash     u64
 16  codec          u16  136  trailer_hash     u64
 18  hash_algo      u16  144  reserved[48]
 20  record_format  u16
 22  reserved       u16
 24  base_lsn       u64
 32  last_lsn       u64
 40  min_ts_ms      i64
 48  max_ts_ms      i64
 56  record_count   u32
 60  block_count    u32
 64  index_offset   u64
 72  index_length   u32
 76  reserved       u32
```
