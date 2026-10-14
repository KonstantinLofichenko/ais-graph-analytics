#!/bin/sh
# Upload only Parquet input, retaining relative paths; never write to the host.
set -eu
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
if ! find /input -type f -name '*.parquet' -print -quit | grep -q .; then
    echo 'No HAIS Parquet files found in the read-only /input mount' >&2
    exit 1
fi
find /input -type f -name '*.parquet' -exec sh -ec '
    for source do
        relative=${source#/input/}
        mc cp "$source" "local/ais-data/hais/source/$relative"
    done
' sh {} +
