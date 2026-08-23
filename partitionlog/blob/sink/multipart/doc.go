// Package multipart defines the object assembly contract used by partitionlog
// blob sinks.
//
// The package is deliberately storage-focused. It knows about object keys,
// parts, and receipts; it does not know about partitions, LSNs, segment files,
// catalogs, or retention.
//
// Commit requires at least one receipt, and receipt numbers must be contiguous
// starting at 1. PutPart retries are content-identified, Commit reconciles an
// already-created final object, and Cleanup only refers to staging work. Part
// SHA-256 values are logical retry identities, not provider checksum
// attestations unless a provider adapter explicitly supplies that guarantee.
package multipart
