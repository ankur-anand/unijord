// Package multipart defines the object assembly contract used by partitionlog
// blob sinks.
//
// The package is deliberately storage-focused. It knows about object keys,
// parts, and receipts; it does not know about partitions, LSNs, segment files,
// catalogs, or retention.
//
// Complete requires at least one receipt, and receipt numbers must be contiguous
// starting at 1. Provider implementations may add stricter limits.
package multipart
