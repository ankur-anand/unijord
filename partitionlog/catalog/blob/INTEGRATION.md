# Blob Catalog Integration Tests

## MinIO

Start MinIO:

```sh
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Run the S3 catalog backend integration test:

```sh
go test ./partitionlog/catalog/blob/s3 \
  -run TestLiveMinIOBackendAndCatalog \
  -integration \
  -count=1
```

The test creates the bucket if needed and writes under a unique
`integration/minio/<timestamp>/...` prefix.

## Configuration

Defaults:

- `CATALOG_BLOB_MINIO_ENDPOINT=http://127.0.0.1:9000`
- `CATALOG_BLOB_MINIO_BUCKET=catalog-blob-it`
- `CATALOG_BLOB_MINIO_ACCESS_KEY` falls back to `MINIO_ROOT_USER`, then `minioadmin`
- `CATALOG_BLOB_MINIO_SECRET_KEY` falls back to `MINIO_ROOT_PASSWORD`, then `minioadmin`

The endpoint also falls back to `BLOB_SINK_MINIO_ENDPOINT` so local blob/sink and
blob catalog tests can share the same MinIO process.

## Azurite

Start Azurite:

```sh
docker run --rm -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite
```

Run the Azure catalog backend integration test:

```sh
go test ./partitionlog/catalog/blob/azure \
  -run TestLiveAzuriteBackendAndCatalog \
  -integration \
  -count=1
```

The test creates the container if needed and writes under a unique
`integration/azurite/<timestamp>/...` prefix.

## Azure Configuration

Defaults:

- `CATALOG_BLOB_AZURITE_CONTAINER=catalog-blob-it`
- `CATALOG_BLOB_AZURITE_CONNECTION_STRING` falls back to
  `BLOB_SINK_AZURITE_CONNECTION_STRING`, then the Azurite development account
  connection string for `http://127.0.0.1:10000/devstoreaccount1`.

## Fake GCS

The GCS catalog backend uses `github.com/fsouza/fake-gcs-server/fakestorage`
for local integration coverage. It runs in-process and does not require Docker.

Run the GCS catalog backend integration test:

```sh
go test ./partitionlog/catalog/blob/gcs \
  -run TestFakeGCSBackendAndCatalogIntegration \
  -integration \
  -count=1
```

The test creates an in-memory bucket and writes under a unique
`integration/fake-gcs/<timestamp>/...` prefix.
