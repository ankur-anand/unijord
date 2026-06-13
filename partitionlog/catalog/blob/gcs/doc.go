// Package gcs implements catalog/blob.Backend using Google Cloud Storage.
//
// Catalog pages are small JSON objects. The backend writes them with
// application/json content type and uses GCS object generation preconditions
// for immutable page creation and partition-head compare-and-swap.
package gcs
