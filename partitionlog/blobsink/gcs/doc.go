// Package gcs implements the partitionlog blobsink multipart store using
// Google Cloud Storage temporary objects plus compose.
//
// GCS compose is the provider-native way to assemble independently uploaded
// objects into one final object:
// https://cloud.google.com/storage/docs/composing-objects
//
// GCS compose accepts at most 32 source objects per compose request. This store
// composes in levels for larger uploads.
package gcs
