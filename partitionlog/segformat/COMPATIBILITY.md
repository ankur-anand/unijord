# Segment Format Compatibility

The segment format is a durable storage contract. A segment written by an
older writer must remain readable after the Go implementation changes and by
independent readers written in other languages.

## Compatibility Corpus

Versioned fixtures live in:

```text
partitionlog/testdata/segformat/v2/
  manifest.json
  v2-none-crc32c.plseg
  v2-zstd-xxh64.plseg
```

The corpus covers:

- uncompressed and zstd blocks;
- CRC32C and XXH64 hashes;
- multiple blocks and block-index entries;
- duplicate and increasing timestamps;
- headers, empty values, binary header fields, and binary record values;
- LSNs above `2^53`, where JSON numeric values are not portable.

`manifest.json` is the language-neutral expected result. It records the
segment reference, preamble and trailer metadata, every block-index entry, and
every decoded record. Its encoding rules are:

- unsigned 64-bit decimal values use JSON strings;
- 64-bit hashes use exactly 16 lowercase hexadecimal characters;
- 16-byte identifiers use exactly 32 lowercase hexadecimal characters;
- record values and header bytes use standard padded RFC 4648 base64;
- codec, hash, and record-format enums include both their numeric wire value
  and canonical name.

Consumers must verify the fixture SHA-256 before using its expected metadata.
This distinguishes a corrupted fixture from a decoder failure.

## Go Compatibility Gate

Run:

```sh
make compatibility
```

`TestSegmentCompatibilityCorpus` performs four independent checks:

1. Verify each checked-in file against its SHA-256.
2. Open it through `segreader` using only the manifest's `SegmentRef`.
3. Compare preamble, trailer, block index, headers, values, LSNs, and
   timestamps with the manifest.
4. Re-encode vectors marked `writer_byte_stable` and compare the complete file
   byte for byte.

The uncompressed vector is byte-stable. The zstd vector is a decode contract,
not an encoder-output contract, because a compatible zstd library upgrade may
choose a different valid frame representation.

## Updating The Corpus

Generate the corpus explicitly with:

```sh
go generate ./partitionlog/segformat
```

Normal tests never run the generator. Do not regenerate fixtures merely to
make a compatibility failure disappear.

If an implementation change alters a byte-stable vector, either restore the
old encoding or introduce a new segment format version. Keep every released
version's corpus so current readers continue proving backward compatibility.

Corrections made before a format version is released must be reviewed as wire
format changes. The fixture diff, manifest diff, and reason for changing the
contract should be part of the same review.

## Cross-Language Readers

A reader in another language should run the same sequence:

1. Load `manifest.json` without coercing decimal strings through floating
   point.
2. Verify the `.plseg` SHA-256.
3. Parse the file according to `SPEC.md`.
4. Compare all segment, block, and record fields with the manifest.

Passing only the uncompressed fixture is not sufficient for full format
support. A complete reader must pass both current vectors.

## Sustained Fuzzing

Normal `go test` runs the fuzz seed corpus once. Sustained fuzzing is an
explicit release and scheduled-CI gate:

```sh
make fuzz FUZZTIME=5m
```

This runs every `segformat` parser target and the complete segment open/scan
target independently. Separate invocations preserve the exact failing target
and let Go retain discovered inputs in its fuzz cache.

`.github/workflows/partitionlog.yml` runs every target for ten minutes
each on a weekly schedule and on manual dispatch. Pull requests run the golden
compatibility gate and the ordinary seed corpus without paying the sustained
fuzzing cost.

Longer scheduled runs should use at least:

```sh
make fuzz FUZZTIME=30m
```

Any input that causes a panic, excessive unbounded work, or acceptance of an
invalid structure is a format bug. A minimized regression input should be
added to the permanent fuzz seeds before the fix is merged.
