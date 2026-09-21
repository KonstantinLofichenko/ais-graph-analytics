"""Orchestrate source-faithful ClickHouse file() ingestion; never parse Parquet."""
from datetime import date, datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import re

from pipelines.port_visits.run import ClickHouse

LOGGER = logging.getLogger(__name__)
DIRECTORY = Path('/opt/ais/data/hais')
SCHEMA = """date_time_utc DateTime64(6, 'UTC'), mmsi UInt32,
longitude Float64, latitude Float64, status Nullable(Int32),
course_over_ground Nullable(Float64), true_heading Nullable(Int32),
speed_over_ground Nullable(Float64), rate_of_turn Nullable(Float64),
maneuvre Nullable(Int32), data_source Nullable(String), ais_class Nullable(String),
msg_type Nullable(Int32)"""
COLUMNS = (
    'date_time_utc, mmsi, longitude, latitude, status, course_over_ground, '
    'true_heading, speed_over_ground, rate_of_turn, maneuvre, data_source, ais_class, msg_type')
# Explicit schema avoids inference of the unsupported GeoParquet geometry type.
# Embed only our trusted schema constant; keep the file path parameterized.
FILE = "file({file_path:String}, 'Parquet', '" + SCHEMA.replace("\\", "\\\\").replace("'", "\\'") + "')"


class HaisClickHouse(ClickHouse):
    """Reuse existing connection/authentication, allowing a bulk query up to one hour."""
    def query(self, sql, params=None):
        response = self.session.post(
            self.url, params={'wait_end_of_query': '1', 'max_execution_time': '3300',
                              **(params or {})}, data=sql.encode(), timeout=(10, 3600))
        if not response.ok:
            raise RuntimeError(f'HAIS ClickHouse query failed: HTTP {response.status_code}:\n'
                               f'{response.text[:2000]}')
        return response.text


def parse_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('start_date/end_date must be YYYY-MM-DD dates')
    return date.fromisoformat(value)


def discover(start_date, end_date, directory=DIRECTORY):
    start, end = parse_date(start_date), parse_date(end_date)
    if start > end:
        raise ValueError('start_date must be <= end_date')
    files = []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        name = f'hais_{day.isoformat()}.snappy.parquet'
        path = Path(directory) / name
        if path.is_symlink() or not path.is_file():
            raise FileNotFoundError(f'Missing regular HAIS file: {path}')
        info = path.stat()
        files.append(dict(file_name=name, file_size=info.st_size,
                          source_date=day.isoformat(), mtime_ns=info.st_mtime_ns))
    return files


def scalar(ch, sql, params=None):
    return int(ch.query(sql + ' FORMAT TabSeparated', params).strip())


def audit(ch, item, started, status, row_count=None):
    row = {key: item[key] for key in ('file_name', 'file_size', 'source_date')}
    row.update(started_at=started, status=status, row_count=row_count,
               completed_at=None if status == 'running' else utc_now())
    ch.query('INSERT INTO raw.hais_ingestion_runs FORMAT JSONEachRow\n' + json.dumps(row))


def utc_now():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def load_file(item, directory=DIRECTORY, ch=None):
    own_client = ch is None
    ch = ch or HaisClickHouse()
    try:
        return _load_file(ch, item, Path(directory))
    finally:
        if own_client:
            ch.session.close()


def _load_file(ch, item, directory):
    # Reject paths outside the daily naming convention, including traversal/globs.
    day = parse_date(item['source_date'])
    if item['file_name'] != f'hais_{day.isoformat()}.snappy.parquet':
        raise ValueError('Invalid HAIS filename')
    path = directory / item['file_name']

    def unchanged():
        info = path.stat()
        if path.is_symlink() or (info.st_size, info.st_mtime_ns) != (item['file_size'], item['mtime_ns']):
            raise RuntimeError(f'HAIS file changed since discovery: {path.name}')

    unchanged()
    params = {'param_file_name': item['file_name'], 'param_file_size': str(item['file_size'])}
    identity = 'file_name = {file_name:String} AND file_size = {file_size:UInt64}'
    if scalar(ch, 'SELECT count() FROM raw.hais_ingestion_runs WHERE ' + identity +
              " AND status = 'success'", params):
        LOGGER.info('SKIP file=%s bytes=%s: successful ingestion exists',
                    item['file_name'], item['file_size'])
        return dict(item, status='skipped')
    if scalar(ch, 'SELECT count() FROM raw.hais_ingestion_runs WHERE ' + identity, params):
        raise RuntimeError('Unresolved HAIS attempt for ' + item['file_name'] +
                           '; reconcile raw rows and audit before retrying; refusing possible duplicates')
    started = utc_now()
    # Append status events with the same started_at; never replace earlier attempts.
    audit(ch, item, started, 'running')
    try:
        file_params = {'param_file_path': 'hais/' + item['file_name']}
        expected = scalar(ch, 'SELECT count() FROM ' + FILE, file_params)
        unchanged()
        ch.query(f'INSERT INTO raw.hais_positions ({COLUMNS}) SELECT {COLUMNS} FROM {FILE}', file_params)
        unchanged()
        loaded = expected
        audit(ch, item, started, 'success', loaded)
    except Exception:
        try:
            audit(ch, item, started, 'failed')
        except Exception:
            LOGGER.error('Could not append failed audit event; running event requires reconciliation')
        raise
    LOGGER.info('SUCCESS file=%s bytes=%s rows=%s', item['file_name'], item['file_size'], loaded)
    return dict(item, status='success', row_count=loaded)
