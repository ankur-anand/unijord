// Package sink contains blob/object-backed segment sink implementations
// for partitionlog writers.
//
// It provides:
//   - Factory, which writes immutable segment objects through a multipart.Store
//   - object key layout helpers for segment and staging paths
//   - provider-specific multipart stores in subpackages such as s3, gcs, azure
//
// Durable catalog metadata for blob-backed deployments lives in
// partitionlog/catalog/blob, not in this package.
package sink
