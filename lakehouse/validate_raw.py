"""Compare the successful Spark ingestion result with the shared Trino catalog."""
import argparse
import json
import os
import time
import urllib.request
from pathlib import Path


def query(sql):
    request = urllib.request.Request(
        os.environ.get('TRINO_URL', 'http://127.0.0.1:8090').rstrip('/') + '/v1/statement', data=sql.encode(),
        headers={'X-Trino-User': 'hais-validation'}, method='POST',
    )
    rows = []
    deadline = time.monotonic() + 300
    while True:
        with urllib.request.urlopen(request, timeout=30) as response:
            page = json.load(response)
        if 'error' in page:
            raise RuntimeError(page['error']['message'])
        rows.extend(page.get('data', []))
        if 'nextUri' not in page:
            return rows
        if time.monotonic() > deadline:
            raise TimeoutError('Trino validation exceeded 300 seconds')
        time.sleep(0.2)
        request = urllib.request.Request(page['nextUri'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('spark_log', type=Path)
    args = parser.parse_args()
    results = [json.loads(line.split('RAW_INGEST_RESULT=', 1)[1])
               for line in args.spark_log.read_text().splitlines()
               if line.startswith('RAW_INGEST_RESULT=')]
    if not results:
        raise RuntimeError('No successful ingestion result in Spark log')
    expected = results[-1]
    print(json.dumps(validate(expected), indent=2))
    print('Schema:', query('SHOW COLUMNS FROM iceberg.hais_raw.ais_positions'))
    print('Sample:', query('SELECT mmsi, date_time_utc, length(geometry) AS geometry_bytes '
                          'FROM iceberg.hais_raw.ais_positions LIMIT 5'))


def validate(expected):
    count = query('SELECT count(*) FROM iceberg.hais_raw.ais_positions')[0][0]
    if count <= 0:
        raise ValueError('Raw table is empty')
    if count != expected['source_count'] or count != expected['iceberg_count']:
        raise RuntimeError(f'Trino count {count} differs from Spark result {expected}')
    files = query('SELECT file_path FROM iceberg.hais_raw."ais_positions$files"')
    if not files or not all(row[0].startswith('s3://warehouse/') for row in files):
        raise RuntimeError('Raw table data is not in the existing Iceberg warehouse')
    columns = query('SHOW COLUMNS FROM iceberg.hais_raw.ais_positions')
    required = {'date_time_utc', 'mmsi', 'longitude', 'latitude', 'status',
                'course_over_ground', 'true_heading', 'speed_over_ground',
                'rate_of_turn', 'maneuvre', 'data_source', 'ais_class', 'msg_type', 'geometry'}
    missing = required - {row[0] for row in columns}
    if missing:
        raise ValueError('Raw columns missing: ' + ', '.join(sorted(missing)))
    return {'trino_count': count, 'spark_source_count': expected['source_count'],
            'source_files': expected['files'], 'warehouse_files': len(files),
            'counts_match': True}


if __name__ == '__main__':
    main()
