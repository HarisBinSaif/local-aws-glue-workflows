#!/bin/sh
# One-shot init: wait for MinIO, create the three buckets used by simple-etl,
# upload the sample input CSV.
set -eu

until /usr/bin/mc alias set local http://minio:9000 minio minio123 >/dev/null 2>&1; do
    echo "waiting for minio..."
    sleep 1
done

/usr/bin/mc mb --ignore-existing local/local-input
/usr/bin/mc mb --ignore-existing local/local-staging
/usr/bin/mc mb --ignore-existing local/local-output

/usr/bin/mc cp /seed/sample-input.csv local/local-input/sample-input.csv

echo "minio-init complete"
