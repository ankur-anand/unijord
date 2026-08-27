# catformat

Byte format for the partitionlog blob catalog: one mutable **head** object and
immutable **leaf** and **index** page objects per stream partition.

All integers are big-endian. Signed integers use two's-complement
representation. Timestamps are signed 64-bit milliseconds since the Unix
epoch. Byte offsets are measured from the start of the containing object.
Reserved bytes are zero; readers reject an object containing a non-zero
reserved byte.

## 1. Objects

One stream partition owns:

```text
<catalog-prefix>/<bucket>/streams/<stream-key>/p<partition>/
    head.plc                                              mutable, CAS
    pages/l00/leaf-<seq_hi>-<seq_lo>-<generation>-<page_id>.plc     immutable
    pages/l01/index-l01-<seq_hi>-<seq_lo>-<generation>-<page_id>.plc immutable
    pages/l02/index-l02-…                                            immutable
    maintenance/retention.json                            retention protocol
    maintenance/gc/state.json                             lifecycle protocol
```

`<bucket>`, `<stream-key>`, and `p<partition>` are derived in §5.

| Object | Written | Rewritten | Deleted by |
| --- | --- | --- | --- |
| head | once at initialization | on every publish, retention, writer takeover, via compare-and-swap | never by normal lifecycle |
| leaf page | once, when the open leaf seals; once more per retention trim of the straddling leaf | never | ordered retention or reachability scrub |
| index page | once, when the open index page at its level seals; once more per retention trim | never | ordered retention or reachability scrub |

An open page never exists as an object. The
open leaf and the open index page of every level live inside the head. A page
object is created only at the moment it seals, and its bytes never change.

## 2. Constants

| Name | Value |
| --- | ---: |
| `VERSION` | `1` |
| `HEAD_HEADER_SIZE` | `192` |
| `PAGE_HEADER_SIZE` | `128` |
| `SECTION_HEADER_SIZE` | `16` |
| `TRAILER_SIZE` | `32` |
| `LEAF_ENTRY_SIZE` | `128` |
| `INDEX_ENTRY_SIZE` | `80` |
| `MAX_LEAF_ENTRIES` | `1024` |
| `MAX_INDEX_ENTRIES` | `1024` |
| `MAX_INDEX_LEVEL` | `63` |
| `MAX_OPEN_INDEX_LEVEL` | `64` |
| `MAX_RECORD_LSN` | `2^64 - 2` |
| `RESERVED_LSN` | `2^64 - 1` |
| `STREAM_KEY_SIZE` | `32` |
| `PAGE_ID_SIZE` | `16` |

`leaf_limit` and `index_limit` are **writer configuration**, recorded in the
head for validation, bounded by `MAX_LEAF_ENTRIES` and `MAX_INDEX_ENTRIES`.
Readers never need them to navigate; they follow entries.

Recommended defaults: `leaf_limit = 128`, `index_limit = 128`. With these, a
sealed leaf is 16 544 bytes and a sealed index page is 10 400 bytes; the head
never exceeds 192 + 128 + 128 × 128 + L × (16 + 128 × 80) bytes, i.e.
≈ 17 KB + 10.3 KB per index level.

## 3. Magic values

| Region | Magic |
| --- | --- |
| Head header | `PLCH` |
| Leaf page header | `PLCL` |
| Index page header | `PLCX` |
| Open-index section header (inside head) | `PLCS` |
| Trailer (head and pages) | `PLCT` |

## 4. Hash algorithms

| Value | Name | Meaning |
| ---: | --- | --- |
| `0` | `crc32c` | CRC-32C Castagnoli, zero-extended to `u64` |
| `1` | `xxh64` | XXH64, seed `0` |

The hash algorithm is per object, declared in the object's header, and applies
to every hash field in that object. Writers should use `xxh64`.

CRC32C uses the reflected Castagnoli polynomial `0x82F63B78` (normal form
`0x1EDC6F41`), initial value `0xFFFFFFFF`, reflected input and output, and
final XOR `0xFFFFFFFF`. The check value for ASCII `123456789` is
`0xE3069283`.

## 5. Identity fields

The **canonical stream ID** is valid UTF-8 and at most 512 bytes. Remove all
leading and trailing `/` bytes. The remaining value must be non-empty and
must not contain control characters, empty path segments, or `.` and `..`
path segments.

**`stream_key`** (32 bytes) is
`sha256(utf8(canonical_stream_id))`. Its object-key representation is 64
lowercase hexadecimal characters. It appears in every object so a reader can
reject a page or head that belongs to a different stream even if it was
placed under the wrong prefix.

**`bucket`** is a three-character, lowercase hexadecimal traffic shard:

```text
input = u32_be(len(utf8(canonical_stream_id)))
      + utf8(canonical_stream_id)
      + u32_be(partition)

bucket = lowercase_hex(crc32c(input) & 0x0fff), zero-padded to 3 characters
```

**`p<partition>`** is the literal `p` followed by the partition as eight
zero-padded decimal digits.

**`page_id`** (16 bytes) is the first 16 bytes of
`sha256(bytes[0 : trailer_offset])` of the page object — the header and all
entries, excluding the trailer. It is computed after the header is complete
and appears in the object key and in the parent's entry. It does not appear
inside the page itself. A reader must recompute it from the fetched bytes and
compare it with the entry that led there.

**`generation`** is the head's monotonic commit counter, copied into every
page sealed by that commit and into the page's object key. Two commits can
never produce the same page key. `generation == 2^64 - 1` is valid, but a
head at that value cannot be mutated again: a writer must return
`catalog.ErrGenerationExhausted` rather than wrap it to zero.

**`writer_epoch`** and **`writer_id`** form the writer fence.
`writer_epoch == 2^64 - 1` is valid, but another takeover must return
`catalog.ErrFenceExhausted` rather than wrap it to zero. A head has
`writer_epoch == 0` exactly when `writer_id` is all zero; no page or leaf
entry may carry epoch zero.

## 6. Trailer

Size: 32 bytes. Last region of every head and page object.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLCT` |
| `4` | `2` | `trailer_len` | `u16` | `32` |
| `6` | `2` | `reserved0` | `u16` | `0` |
| `8` | `8` | `body_hash` | `u64` | hash of `bytes[0 : trailer_offset]` |
| `16` | `8` | `trailer_hash` | `u64` | hash of trailer with `[16,24)` zeroed |
| `24` | `8` | `reserved1` | `u64` | `0` |

`trailer_offset = object_size - 32`. `body_hash` covers the header, every
entry, and (for the head) every section header. It is the integrity check for
whole-object reads. Range readers that fetch individual entries cannot verify
`body_hash`; see §13.

## 7. Leaf entry

Size: 128 bytes. One committed segment. The segment object key, stream ID,
and partition are derived as specified in §12.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `8` | `base_lsn` | `u64` | first LSN |
| `8` | `8` | `last_lsn` | `u64` | last LSN, inclusive |
| `16` | `8` | `min_timestamp_ms` | `i64` | |
| `24` | `8` | `max_timestamp_ms` | `i64` | |
| `32` | `4` | `record_count` | `u32` | `last_lsn - base_lsn + 1` |
| `36` | `4` | `block_count` | `u32` | `> 0` |
| `40` | `8` | `size_bytes` | `u64` | segment object size, `> 0` |
| `48` | `8` | `block_index_offset` | `u64` | from the segment trailer |
| `56` | `4` | `block_index_length` | `u32` | from the segment trailer, `> 0` |
| `60` | `2` | `codec` | `u16` | `0` = none; `1` = zstd |
| `62` | `2` | `hash_algo` | `u16` | §4; the segment's algorithm, not the page's |
| `64` | `8` | `segment_hash` | `u64` | from the segment trailer |
| `72` | `8` | `trailer_hash` | `u64` | from the segment trailer |
| `80` | `8` | `writer_epoch` | `u64` | fence epoch under which it was published, `> 0` |
| `88` | `16` | `segment_uuid` | bytes | non-zero |
| `104` | `16` | `writer_tag` | bytes | `writer_id` of the head that accepted the publish |
| `120` | `8` | `reserved0` | `u64` | `0` |

Rules:

- `base_lsn <= last_lsn <= MAX_RECORD_LSN`
- `record_count == last_lsn - base_lsn + 1` and `record_count > 0`
- `block_count > 0`, `size_bytes > 0`, `block_index_length > 0`
- `min_timestamp_ms <= max_timestamp_ms`
- `writer_epoch > 0`, `segment_uuid != 0`, `writer_tag != 0`
- `codec` and `hash_algo` are known values

`writer_epoch` and `writer_tag` record the fence that accepted the segment
**at publish time** (§11.1). They are *not* required to equal the current
head's fence: a leaf may contain segments published by several writer
incarnations, because a takeover does not seal the open leaf. Stored entries
are validated against the segment object instead: when a reader opens the
segment, the `writer_tag` and `segment_uuid` in the `.plseg` preamble and
trailer, and the `writer_epoch` encoded in the segment key, must equal the
entry's values.

## 8. Index entry

Size: 80 bytes. A reference to one sealed page one level down.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `1` | `level` | `u8` | level of the referenced page |
| `1` | `3` | `reserved0` | bytes | `0` |
| `4` | `4` | `entry_count` | `u32` | entries in the referenced page (direct children) |
| `8` | `8` | `seq_lo` | `u64` | first LSN covered |
| `16` | `8` | `seq_hi` | `u64` | last LSN covered, inclusive |
| `24` | `8` | `min_timestamp_ms` | `i64` | |
| `32` | `8` | `max_timestamp_ms` | `i64` | |
| `40` | `8` | `generation` | `u64` | generation that sealed the page |
| `48` | `16` | `page_id` | bytes | §5 |
| `64` | `8` | `segment_count` | `u64` | segments in the whole subtree below the referenced page |
| `72` | `8` | `reserved1` | `u64` | `0` |

`segment_count` is the subtree total, not the direct child count: for a
leaf reference it equals `entry_count`; for an index reference it equals the
sum of `segment_count` over that page's entries. It is what lets retention
(§11.3) and the head rule (§10.3) maintain `head.reachable_segment_count` without
descending into dropped subtrees.

Rules:

- `seq_lo <= seq_hi <= MAX_RECORD_LSN`
- `min_timestamp_ms <= max_timestamp_ms`
- `entry_count > 0`; `<= MAX_LEAF_ENTRIES` if `level == 0`, else `<= MAX_INDEX_ENTRIES`
- `generation > 0`
- `segment_count >= entry_count`; equality is required when `level == 0`
- `segment_count <= seq_hi - seq_lo + 1` (a segment holds at least one record)
- `page_id != 0`

## 9. Page header

Size: 128 bytes. Shared by leaf (`PLCL`) and index (`PLCX`) pages.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLCL` or `PLCX` |
| `4` | `2` | `version` | `u16` | `1` |
| `6` | `2` | `header_len` | `u16` | `128` |
| `8` | `4` | `flags` | `u32` | `0` |
| `12` | `4` | `partition` | `u32` | partition id |
| `16` | `1` | `level` | `u8` | `0` for leaf, `1..MAX_INDEX_LEVEL` for index |
| `17` | `1` | `reserved0` | `u8` | `0` |
| `18` | `2` | `hash_algo` | `u16` | §4, for this object's hashes |
| `20` | `4` | `entry_count` | `u32` | `> 0` |
| `24` | `4` | `entry_size` | `u32` | `128` for leaf, `80` for index |
| `28` | `4` | `reserved1` | `u32` | `0` |
| `32` | `8` | `seq_lo` | `u64` | `entries[0]` first LSN |
| `40` | `8` | `seq_hi` | `u64` | `entries[last]` last LSN |
| `48` | `8` | `min_timestamp_ms` | `i64` | `entries[0].min_timestamp_ms` |
| `56` | `8` | `max_timestamp_ms` | `i64` | `entries[last].max_timestamp_ms` |
| `64` | `8` | `generation` | `u64` | generation that sealed the page |
| `72` | `8` | `writer_epoch` | `u64` | fence epoch of that commit |
| `80` | `32` | `stream_key` | bytes | §5 |
| `112` | `8` | `segment_count` | `u64` | segments in this page's subtree |
| `120` | `8` | `reserved2` | `u64` | `0` |

Page layout:

```text
+-------------------------------+  offset 0
| Page header (128 B)           |
+-------------------------------+  offset 128
| entry[0]                      |  entry_size bytes each
| entry[1]                      |
| ...                           |
| entry[entry_count-1]          |
+-------------------------------+  trailer_offset = 128 + entry_count * entry_size
| Trailer (32 B)                |
+-------------------------------+
```

Rules:

- `object_size == 128 + entry_count * entry_size + 32`
- `generation > 0` and `writer_epoch > 0`
- leaf: `magic == PLCL`, `level == 0`, `entry_size == 128`,
  `entry_count <= MAX_LEAF_ENTRIES`, `segment_count == entry_count`
- index: `magic == PLCX`, `1 <= level <= MAX_INDEX_LEVEL`, `entry_size == 80`,
  `entry_count <= MAX_INDEX_ENTRIES`, every entry has
  `entry.level == level - 1`, and `segment_count == Σ entry.segment_count`
- index: every `entry.generation <= header.generation`
- `seq_lo`, `seq_hi`, `min_timestamp_ms`, `max_timestamp_ms` equal the values
  derived from the first and last entry
- entries are **contiguous and ordered**: for leaves
  `entry[i+1].base_lsn == entry[i].last_lsn + 1`; for index pages
  `entry[i+1].seq_lo == entry[i].seq_hi + 1`
- timestamps are **non-decreasing across entries**:
  `entry[i+1].min_timestamp_ms >= entry[i].max_timestamp_ms`
- leaf: `entry[i+1].writer_epoch >= entry[i].writer_epoch` (fences only move
  forward); the page header's `writer_epoch` is the epoch of the commit that
  sealed the page and is `>= entry[last].writer_epoch`
- the object key's `seq_hi`, `seq_lo`, `generation`, and `page_id` equal the
  header's values and the recomputed `page_id` (§5)
- `stream_key` and `partition` equal the reader's expected values
- `body_hash` and `trailer_hash` verify

A retention trim always rewrites the straddling page (§11.3), so contiguity
holds at every level.

## 10. Head

The head is one object, `head.plc`, replaced by compare-and-swap.

```text
+-------------------------------+  offset 0
| Head header (192 B)           |
+-------------------------------+  offset 192
| last_segment (128 B)          |  a leaf entry; all-zero when has_last_segment == 0
+-------------------------------+  offset 320
| active leaf entries           |  active_count * 128 B   (the OPEN leaf)
+-------------------------------+
| Section header (16 B)  level 1|
| index entries          level 1|  section.entry_count * 80 B   (the OPEN level-1 page)
+-------------------------------+
| Section header (16 B)  level 2|
| index entries          level 2|
+-------------------------------+
| ...  one section per level, ascending, exactly level_count sections
+-------------------------------+
| Trailer (32 B)                |
+-------------------------------+
```

### 10.1 Head header

Size: 192 bytes.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLCH` |
| `4` | `2` | `version` | `u16` | `1` |
| `6` | `2` | `header_len` | `u16` | `192` |
| `8` | `4` | `flags` | `u32` | bit 0: `has_last_segment`; others `0` |
| `12` | `4` | `partition` | `u32` | partition id |
| `16` | `2` | `hash_algo` | `u16` | §4 |
| `18` | `2` | `segment_layout_version` | `u16` | §12, currently `1` |
| `20` | `4` | `leaf_limit` | `u32` | writer's `leaf_limit`, `1..MAX_LEAF_ENTRIES` |
| `24` | `4` | `index_limit` | `u32` | writer's `index_limit`, `2..MAX_INDEX_ENTRIES` |
| `28` | `4` | `active_count` | `u32` | entries in the open leaf, `< leaf_limit` |
| `32` | `4` | `level_count` | `u32` | number of open-index sections, `<= MAX_OPEN_INDEX_LEVEL` |
| `36` | `4` | `reserved1` | `u32` | `0` |
| `40` | `8` | `next_lsn` | `u64` | LSN the next record receives |
| `48` | `8` | `oldest_lsn` | `u64` | retention floor |
| `56` | `8` | `applied_retention_lsn` | `u64` | last requested boundary applied |
| `64` | `8` | `applied_retention_version` | `u64` | policy version applied |
| `72` | `8` | `writer_epoch` | `u64` | fence; `0` only before the first writer |
| `80` | `8` | `generation` | `u64` | commit counter |
| `88` | `8` | `segment_count` | `u64` | lifetime committed segments; never reduced by retention |
| `96` | `16` | `writer_id` | bytes | fence; zero only before the first writer |
| `112` | `32` | `stream_key` | bytes | §5 |
| `144` | `32` | `data_root_key` | bytes | `sha256(utf8(data_root_prefix))`, §12 |
| `176` | `8` | `reachable_segment_count` | `u64` | segments reachable from the current roots |
| `184` | `8` | `reserved2` | bytes | `0` |

### 10.2 Section header

Size: 16 bytes. Precedes the open index entries for one level.

| Offset | Size | Field | Type | Value |
| ---: | ---: | --- | --- | --- |
| `0` | `4` | `magic` | bytes | `PLCS` |
| `4` | `1` | `level` | `u8` | `1..MAX_OPEN_INDEX_LEVEL`, strictly ascending across sections |
| `5` | `3` | `reserved0` | bytes | `0` |
| `8` | `4` | `entry_count` | `u32` | `0..index_limit-1` |
| `12` | `4` | `reserved1` | `u32` | `0` |

Section entries are index entries (§8), `80` bytes each. Sections appear for levels `1..level_count` with no gaps. A section may be
empty (`entry_count == 0`); that is the state immediately after the page at
that level sealed. Every entry in the level-`n` section has `entry.level == n - 1`.

### 10.3 Head rules

Let `roots` be the sequence formed by concatenating, in this order: the
level-`level_count` section entries, then level `level_count - 1`, …, then
level 1, then the active leaf entries. This is the reader's traversal order
(oldest history first).

- `object_size == 192 + 128 + active_count * 128 + Σ (16 + section.entry_count * 80) + 32`
- `next_lsn <= RESERVED_LSN`; `oldest_lsn <= next_lsn`; `applied_retention_lsn <= next_lsn`
- `applied_retention_version == 0` implies `applied_retention_lsn == 0`
- `generation` never decreases; any operation that would increment
  `generation == 2^64 - 1` fails with `catalog.ErrGenerationExhausted`
- `writer_epoch == 0` iff `writer_id` is all zero; any takeover that would
  increment `writer_epoch == 2^64 - 1` fails with
  `catalog.ErrFenceExhausted`
- if `has_last_segment`: `last_segment.writer_epoch <= writer_epoch` and
  `last_segment.last_lsn + 1 == next_lsn`. When `roots` is non-empty,
  `last_segment.last_lsn == roots[last].seq_hi`. When `active_count > 0`,
  `roots[last]` is a leaf entry and must equal `last_segment` exactly; when
  `active_count == 0`, `roots[last]` is an index entry and only the
  `seq_hi` relation is checked here — exact equality with the last leaf entry
  on the rightmost path is a tree-verification check (§15)
- if `has_last_segment` and `roots` is empty: `oldest_lsn == next_lsn`,
  `reachable_segment_count == 0`, and `active_count == 0`. This is a fully retained
  partition: the historical tip remains inline solely to preserve append
  timestamp and reconciliation state; it is not readable history
- if not `has_last_segment`: `oldest_lsn == next_lsn`, `segment_count == 0`,
  `reachable_segment_count == 0`, `active_count == 0`, every section empty
- `roots` is contiguous and ordered exactly as page entries are (§9), with
  `roots[0].seq_lo == oldest_lsn` and `roots[last].seq_hi + 1 == next_lsn`
  (for leaf entries read `base_lsn`/`last_lsn` as `seq_lo`/`seq_hi`)
- `reachable_segment_count <= segment_count` and
  `reachable_segment_count == Σ roots.segment_count`, counting each active
  leaf entry as `1`
- timestamps are non-decreasing across `roots`
- `active_count < leaf_limit`; every section `entry_count < index_limit`
- every root reference has `generation <= head.generation`; every active leaf
  has `writer_epoch <= head.writer_epoch`
- `stream_key`, `data_root_key`, and `partition` match the reader's expectation
- `segment_layout_version` is one the reader implements (§12); unknown values
  are rejected, not guessed
- `body_hash` and `trailer_hash` verify

The contiguity rule is the invariant that makes the tree a complete index of
`[oldest_lsn, next_lsn)`: there is exactly one root covering any LSN in that
range, and a reader that walks `roots` visits every retained segment once.

## 11. Writer algorithms

These are normative: an implementation may differ in code, not in the objects
it produces.

### 11.1 Publish

```text
publish(head, segment):
    require head.generation < 2^64 - 1
    require head.segment_layout_version == the sink layout version this writer minted segment under
    require segment.base_lsn == head.next_lsn
    require segment.writer_epoch == head.writer_epoch and segment.writer_tag == head.writer_id
    require has_last_segment == 0 or segment.min_timestamp_ms >= last_segment.max_timestamp_ms
    next = copy(head)
    next.generation += 1
    next.active.append(segment)
    next.next_lsn = segment.last_lsn + 1
    next.segment_count += 1
    next.reachable_segment_count += 1
    next.last_segment = segment; next.has_last_segment = 1
    if next.oldest_lsn == head.next_lsn and head.has_last_segment == 0:   # first ever segment
        next.oldest_lsn = segment.base_lsn
    if len(next.active) == leaf_limit:
        entry = write_leaf(next.generation, next.writer_epoch, next.active)   # PUT, once
        next.active = []
        push(next, 1, entry)
    CAS head.plc: expected token = token of `head`, body = encode(next)
```

`write_leaf` and `write_index` PUT the page object (§9) under its derived key
(§12) **before** the head CAS. A crash between the PUT and the CAS leaves an
orphan page; it is never reachable and is removed by the scrub.

```text
push(next, level, entry):
    require level <= MAX_OPEN_INDEX_LEVEL
    section = next.section(level)         # create empty section if level > level_count
    section.append(entry)
    if len(section) == index_limit:
        require level <= MAX_INDEX_LEVEL  # level 64 is the terminal open section
        parent = write_index(next.generation, next.writer_epoch, level, section)   # PUT, once
        section.clear()
        push(next, level + 1, parent)
```

Because `push` runs before the CAS, a single publish may PUT one leaf and up
to `level_count` index pages, then perform exactly one head CAS. The head CAS
is the commit point for all of them. Level 64 exists only as an open section
that can reference immutable level-63 pages. An append that would fill it
returns `catalog.ErrIndexFull`; an immutable level-64 page is never written.

### 11.2 Writer takeover

Load the head and require both `writer_epoch < 2^64 - 1` and
`generation < 2^64 - 1`; otherwise return `catalog.ErrFenceExhausted` or
`catalog.ErrGenerationExhausted`, respectively. Then increment both counters,
set `writer_id`, and CAS. No page objects change.

### 11.3 Retention

```text
apply_retention(head, target_lsn):
    segment = find(head, target_lsn)               # §13
    effective = segment.base_lsn                   # whole-segment granularity
    if effective <= head.oldest_lsn: no-op
    require head.generation < 2^64 - 1
    next = copy(head); next.generation += 1
    for each section, highest level first, and then active:
        drop entries with seq_hi < effective
        if the first remaining entry has seq_lo < effective:
            replace it with trim(entry, effective, next.generation)   # writes ONE new page per level on that path
    next.oldest_lsn = effective
    next.applied_retention_lsn = target_lsn
    next.applied_retention_version = request.policy_version
    next.reachable_segment_count = Σ segment_count over next's roots   # lifetime segment_count is unchanged
    CAS head.plc
```

`trim(entry, effective, generation)` loads the referenced page, drops leading
entries below `effective`, recursively trims its own first entry if that page
straddles, recomputes `segment_count` from the retained entries, and writes
the result as a **new page** with the new `generation` and a new `page_id`.
The original page becomes unreachable. This is the only copy-on-write
operation in the format and it occurs at most once per level per retention
application.

After the trim, the page-level contiguity rule (§9) and the head rule
`roots[0].seq_lo == oldest_lsn` (§10.3) both hold.

## 12. Derivations

Nothing derivable is stored. Readers reconstruct:

**Page object key** from an index entry (or from a head section entry) plus
the catalog prefix, stream, and partition:

```text
pages/l00/leaf-<seq_hi:020d>-<seq_lo:020d>-<generation:020d>-<page_id:32 lowercase hex>.plc
pages/l<level:02d>/index-l<level:02d>-<seq_hi:020d>-<seq_lo:020d>-<generation:020d>-<page_id:32 hex>.plc
```

`seq_hi` is the first sortable range field. Therefore, within one level,
lexicographic object-key order is ascending numerical `seq_hi` order.

**Segment object key** from a leaf entry plus the data root prefix, stream,
and partition, according to the grammar selected by the head's
`segment_layout_version`:

| `segment_layout_version` | Grammar |
| ---: | --- |
| `1` | `<data-root>/segments/<bucket>/streams/<stream-key>/p<partition:08d>/seg-<base_lsn:020d>-e<writer_epoch:020d>-<segment_uuid:32 lowercase hex>.plseg` |

Every writer and reader must implement this grammar byte-for-byte. If the
grammar changes, a new segment-layout version is allocated, and:

- a head has exactly one `segment_layout_version` for its whole history. A
  writer whose sink mints keys under a different version must refuse to open
  the partition (§11.1) — mixed grammars inside one partition are not
  representable, by design. Changing the grammar for a stream means a new
  partition or a new stream incarnation;
- a reader that does not implement the head's version must reject the head
  rather than derive keys under a grammar it assumes.

The page-key grammar (above) needs no separate version field: it is defined
by this specification and is covered by the head's `version`.

`stream_id` and `partition` on the reconstructed `SegmentRef` come from the
reader's configuration and are validated against the header's `stream_key`
and `partition`.

### 12.1 Reader contract: the catalog is not self-contained

The catalog deliberately stores hashes and derivation inputs instead of
complete segment keys and stream IDs. To use a partition, a reader **must be
supplied with**:

| Input | Verified against | Recoverable from the catalog? |
| --- | --- | --- |
| canonical stream ID | `stream_key` in every head and page | **No** — only its SHA-256 is stored |
| partition | `partition` in every head and page | yes (it is stored) |
| catalog prefix | implicit: it is where `head.plc` was found | n/a |
| data root prefix | `data_root_key` in the head | **No** — only its SHA-256 is stored |
| segment layout version | `segment_layout_version` in the head | yes (it is stored) |

The two non-recoverable inputs are hashed rather than stored because they are
operator-chosen strings whose values are not bounded by the fixed-width head
fields. The catalog detects a mismatch; it does not make those configuration
inputs recoverable. A deployment that requires bucket-only recovery must keep
the mapping outside this format.

Consequently, a catalog found in a bucket with no other information can
be **validated** (every hash, range, and count rule in this specification) and
**enumerated** (every segment's `base_lsn`, `writer_epoch`, `segment_uuid`,
sizes, and hashes), but its segments cannot be **opened** without the stream
ID and data root.

To turn a misconfigured data root into an immediate error instead of a
not-found on the first segment read, the head records
`data_root_key = sha256(utf8(data_root_prefix))`. Normalization removes all
leading and trailing `/` bytes; if the result is empty it becomes the literal
`partitionlog`. A reader must compare this field with the hash of its
configured data root when it loads the head and reject the head on mismatch.
The prefix itself is not stored because only equality is needed.

## 13. Lookup

```text
find(lsn):
    head = GET head.plc; validate
    if lsn < head.oldest_lsn or lsn >= head.next_lsn: not found
    for root in roots(head):                      # highest level first, then active
        if root.seq_lo <= lsn <= root.seq_hi:
            return descend(root, lsn)
    unreachable if head is valid

descend(entry, lsn):
    if entry.level == 0 and entry is a leaf ENTRY (from the head's active list): return it
    page = GET key(entry); validate against entry (page_id, ranges, level)
    i = first index with page.entries[i].seq_hi >= lsn   # binary search
    if page.level == 0: return page.entries[i]
    return descend(page.entries[i], lsn)
```

`lookup_timestamp(ts)` is the same walk with `max_timestamp_ms >= ts` as the
search key at every level; it relies on the non-decreasing timestamp rules.

Cost: one GET for the head, plus one GET per level from the root's level down
to a leaf. Roots in the head's active list cost no extra GET. With
`index_limit = 128`, a 10 000 000-segment partition answers in at most four
GETs (head, level-2 page, level-1 page, leaf), transferring at most about
90 KB and typically half that.

**Range reads.** Because entries are fixed-width, an implementation may probe
`bytes[128 + i * entry_size : +entry_size]` of a page with HTTP range
requests instead of fetching the whole page. Such a reader cannot verify
`body_hash`; it must verify at least that the page header's `stream_key`,
`partition`, `level`, `seq_lo`, `seq_hi`, and `entry_count` match the entry
that led there (one 128-byte range read) and should verify the leaf entry it
returns against the segment trailer when it opens the segment. Whole-page
reads are the recommended default; measurements show range probing costs more
round trips than it saves in bytes on every tested store.

## 14. Initialization

A reader loads `head.plc` and follows `.plc` page references. A missing head
means the partition is uninitialized.

```text
initialize(partition):
    create head.plc with compare-and-swap expected token ""
    if the compare-and-swap loses, validate and open the returned head.plc
```

The JSON maintenance objects `maintenance/retention.json` and
`maintenance/gc/state.json` are separate protocols and remain supported.

## 15. Validation

Reserved fields must be zero. Readers must reject non-zero reserved fields.

Validation can be partial:

- **Head validation:** header, sections, `roots` contiguity, hashes. Required
  before any use of the head.
- **Entry validation:** a page header and the specific entry returned, against
  the entry that referenced the page. Minimum for range readers.
- **Page validation:** full page including contiguity, timestamp order,
  `page_id` recomputation, hashes. Required for whole-page reads.
- **Tree verification:** every reachable page validated, the concatenation
  of all leaf entries across the tree equals `[oldest_lsn, next_lsn)` with
  `reachable_segment_count` entries, every index entry's `segment_count` equals the
  size of its subtree, and `last_segment` equals the final leaf entry on the
  rightmost path. For scrub, audit, and the compatibility corpus.

Any input that panics a parser, causes unbounded work, or is accepted while
violating a rule above is a format bug. Parsers must bound allocations by the
declared `entry_count` × `entry_size` and reject objects whose size does not
match before allocating.

## 16. Quick reference

```text
Head header (192 B)
  0  PLCH                       40  next_lsn                   u64
  4  version              u16   48  oldest_lsn                 u64
  6  header_len           u16   56  applied_retention_lsn      u64
  8  flags (bit0 has_last) u32  64  applied_retention_version  u64
 12  partition            u32   72  writer_epoch               u64
 16  hash_algo            u16   80  generation                 u64
 18  seg_layout_version   u16   88  segment_count              u64
 20  leaf_limit           u32   96  writer_id[16]
 24  index_limit          u32  112  stream_key[32]
 28  active_count         u32  144  data_root_key[32]
 32  level_count          u32  176  reachable_segment_count  u64
 36  reserved             u32  184  reserved[8]
then: last_segment (128 B leaf entry) | active leaf entries | sections | trailer

Section header (16 B)
  0  PLCS                        8  entry_count   u32
  4  level                u8    12  reserved      u32
  5  reserved[3]

Page header (128 B)
  0  PLCL | PLCX                32  seq_lo             u64
  4  version              u16   40  seq_hi             u64
  6  header_len           u16   48  min_timestamp_ms   i64
  8  flags                u32   56  max_timestamp_ms   i64
 12  partition            u32   64  generation         u64
 16  level                u8    72  writer_epoch       u64
 17  reserved             u8    80  stream_key[32]
 18  hash_algo            u16  112  segment_count      u64
                              120  reserved           u64
 20  entry_count          u32
 24  entry_size           u32
 28  reserved             u32

Leaf entry (128 B)
  0  base_lsn             u64   60  codec              u16
  8  last_lsn             u64   62  hash_algo          u16
 16  min_timestamp_ms     i64   64  segment_hash       u64
 24  max_timestamp_ms     i64   72  trailer_hash       u64
 32  record_count         u32   80  writer_epoch       u64
 36  block_count          u32   88  segment_uuid[16]
 40  size_bytes           u64  104  writer_tag[16]
 48  block_index_offset   u64  120  reserved           u64
 56  block_index_length   u32

Index entry (80 B)
  0  level                u8    24  min_timestamp_ms   i64
  1  reserved[3]                32  max_timestamp_ms   i64
  4  entry_count          u32   40  generation         u64
  8  seq_lo               u64   48  page_id[16]
 16  seq_hi               u64   64  segment_count      u64
                                72  reserved           u64

Trailer (32 B)
  0  PLCT                        8  body_hash          u64
  4  trailer_len          u16   16  trailer_hash       u64
  6  reserved             u16   24  reserved           u64
```
