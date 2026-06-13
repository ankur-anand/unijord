// Package azure implements catalog/blob.Backend using Azure Blob Storage.
//
// Catalog pages are small JSON objects. The backend writes them as block blobs
// with application/json content type and uses Azure conditional requests for
// immutable page creation and partition-head compare-and-swap.
package azure
