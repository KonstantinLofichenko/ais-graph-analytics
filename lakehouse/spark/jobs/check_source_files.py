"""Check the pre-uploaded MinIO input using the raw job's footer inspection."""
import json
import os
from ingest_hais_raw import inspect_source

source = os.environ.get("HAIS_SOURCE_URI", "s3a://ais-data/hais/source/")
_, files, rows = inspect_source(source, os.environ["MINIO_ROOT_USER"], os.environ["MINIO_ROOT_PASSWORD"])
print("SOURCE_RESULT=" + json.dumps({"source": source, "files": len(files), "footer_rows": rows}), flush=True)
