// Package azure implements the partitionlog blob/sink multipart store using
// Azure Blob Storage block blobs.
//
// Azure stages independent blocks with Put Block and publishes the final blob
// with Put Block List:
// https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
// https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list
package azure
