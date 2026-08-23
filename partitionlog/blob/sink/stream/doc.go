// Package stream provides a provider-neutral, ordered streaming upload.
//
// The package turns an unordered, concurrently callable multipart.Session into
// a sequential byte stream. It owns part numbering, bounded buffering,
// backpressure, receipt ordering, and the upload lifecycle. Object-store
// implementations remain responsible only for uploading numbered parts,
// completing them in the supplied order, and aborting staging work.
package stream
