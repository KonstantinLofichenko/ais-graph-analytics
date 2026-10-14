"""Replace the raw HAIS Iceberg table from the current MinIO Parquet prefix."""
import json
import os
from urllib.parse import urlsplit

import pyarrow.fs as pafs
import pyarrow.parquet as pq
from pyspark.sql import functions as F
from spark_session import create_spark
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.types import ByteType, ShortType


TABLE = "iceberg.hais_raw.ais_positions"


def inspect_source(source_uri, access_key, secret_key):
    """Inspect every footer without letting newer GeoParquet types hide columns."""
    uri = urlsplit(source_uri)
    if uri.scheme != "s3a" or not uri.netloc or uri.query or uri.fragment:
        raise ValueError("HAIS_SOURCE_URI must be an s3a://bucket/prefix/ URI")
    fs = pafs.S3FileSystem(
        access_key=access_key, secret_key=secret_key, region="us-east-1",
        endpoint_override="minio:9000", scheme="http",
    )
    prefix = uri.netloc + uri.path.rstrip("/")
    files = sorted(
        item.path for item in fs.get_file_info(pafs.FileSelector(prefix, recursive=True))
        if item.type == pafs.FileType.File and item.path.endswith(".parquet")
    )
    if not files:
        raise ValueError("No Parquet files in " + source_uri)
    schema = None
    expected_rows = 0
    for path in files:
        with fs.open_input_file(path) as stream:
            parquet = pq.ParquetFile(stream)
            current = parquet.schema_arrow
            expected_rows += parquet.metadata.num_rows
            if schema is None:
                schema = current
            elif not schema.equals(current, check_metadata=False):
                raise ValueError("Source schemas differ: " + path)
    # Geometry is exposed as binary by Arrow. Explicit Spark schema bypasses
    # the source's Spark 4.2 geometry(...) type, unsupported by this Spark runtime.
    return from_arrow_schema(schema), files, expected_rows


def normalize_for_iceberg(frame):
    """Iceberg has int, but no byte/short; widening preserves all values."""
    return frame.select(*[
        F.col(field.name).cast("int").alias(field.name)
        if isinstance(field.dataType, (ByteType, ShortType)) else F.col(field.name)
        for field in frame.schema.fields
    ])


def main():
    access_key = os.environ["MINIO_ROOT_USER"]
    secret_key = os.environ["MINIO_ROOT_PASSWORD"]
    source = os.environ.get("HAIS_SOURCE_URI", "s3a://ais-data/hais/source/")
    schema, files, footer_count = inspect_source(source, access_key, secret_key)
    print("Source URI:", source, flush=True)
    print("Parquet files:", len(files), "Footer rows:", footer_count, flush=True)
    spark = create_spark("HAIS raw Iceberg ingestion")
    try:
        # Explicit file list excludes unrelated objects and retains nested layout.
        frame = spark.read.schema(schema).parquet(*["s3a://" + path for path in files])
        frame.printSchema()
        frame.show(5, truncate=False)
        source_count = frame.count()
        if source_count != footer_count:
            raise RuntimeError(f"Spark count {source_count} != footer count {footer_count}")
        raw = normalize_for_iceberg(frame)
        raw.printSchema()
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.hais_raw "
                  "LOCATION 's3://warehouse/hais_raw/'")
        # Full atomic replacement, not append: reruns do not duplicate raw rows.
        raw.writeTo(TABLE).tableProperty("format-version", "2").createOrReplace()
        written_count = spark.table(TABLE).count()
        if written_count != source_count:
            raise RuntimeError(f"Iceberg count {written_count} != source count {source_count}")
        print("RAW_INGEST_RESULT=" + json.dumps({
            "source": source, "files": len(files), "source_count": source_count,
            "table": TABLE, "iceberg_count": written_count,
        }), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
