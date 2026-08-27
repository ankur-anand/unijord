#!/usr/bin/env bash
# Start the emulator a plbench provider needs, or nothing for in-process and
# real providers. Mirrors the images and versions pinned in partitionlog.yml.
set -euo pipefail

provider="${1:?provider}"
case "$provider" in
  minio)
    docker run -d --name plbench-minio -p 9000:9000 \
      -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
      minio/minio:RELEASE.2025-09-07T16-13-09Z server /data
    for _ in $(seq 1 60); do
      curl -fsS http://127.0.0.1:9000/minio/health/live >/dev/null 2>&1 && exit 0
      sleep 1
    done
    docker logs plbench-minio; echo "minio did not become healthy"; exit 1
    ;;
  azurite)
    docker run -d --name plbench-azurite -p 10000:10000 \
      mcr.microsoft.com/azure-storage/azurite:3.35.0 azurite-blob --blobHost 0.0.0.0
    for _ in $(seq 1 60); do
      curl -fsS "http://127.0.0.1:10000/devstoreaccount1?comp=list" >/dev/null 2>&1 && exit 0
      # Azurite answers 403 on unauthenticated list once up; treat any HTTP answer as ready.
      if curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:10000/devstoreaccount1?comp=list" | grep -qE '^[0-9]{3}$'; then exit 0; fi
      sleep 1
    done
    docker logs plbench-azurite; echo "azurite did not become ready"; exit 1
    ;;
  fake-gcs|s3|azure|gcs)
    echo "no emulator to start for $provider"
    ;;
  *)
    echo "unknown provider $provider"; exit 1
    ;;
esac
