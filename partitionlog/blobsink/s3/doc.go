// Package s3 implements the partitionlog blobsink multipart store using
// Amazon S3 native multipart upload APIs.
//
// Production writers must configure segwriter.PartSize for S3's multipart
// constraints: every part except the final part must be at least 5 MiB, and an
// upload can contain at most 10,000 parts.
//
// Reference:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
package s3
