# blobsink Integration Tests

Live integration tests are opt-in. Normal `go test ./partitionlog/blobsink/...`
uses in-process fakes only.

## Local Services

MinIO:

```sh
docker run --rm -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Azurite:

```sh
docker run --rm -p 10000:10000 -p 10001:10001 -p 10002:10002 \
  mcr.microsoft.com/azure-storage/azurite
```

## Running

```sh
go test ./partitionlog/blobsink/s3 ./partitionlog/blobsink/azure \
  -run Live -integration -count=1
```

The tests create buckets/containers if needed and write under a unique
`integration/<provider>/<timestamp>/...` prefix.

## Coverage

Each live backend validates:

- native multipart upload;
- out-of-order part upload with ordered completion;
- provider content type propagation;
- final object bytes and size;
- final object precondition failure when the key already exists;
- abort behavior;
- `blobsink.Factory -> segwriter` end-to-end segment upload;
- parsing the committed segment trailer from the uploaded object.

## Configuration

MinIO defaults:

- `BLOBSINK_MINIO_ENDPOINT=http://127.0.0.1:9000`
- `BLOBSINK_MINIO_BUCKET=blobsink-it`
- `BLOBSINK_MINIO_ACCESS_KEY` falls back to `MINIO_ROOT_USER`, then `minioadmin`
- `BLOBSINK_MINIO_SECRET_KEY` falls back to `MINIO_ROOT_PASSWORD`, then `minioadmin`

Azurite defaults:

- `BLOBSINK_AZURITE_CONTAINER=blobsink-it`
- `BLOBSINK_AZURITE_CONNECTION_STRING` defaults to the Azurite
  `devstoreaccount1` connection string on `127.0.0.1:10000`.
