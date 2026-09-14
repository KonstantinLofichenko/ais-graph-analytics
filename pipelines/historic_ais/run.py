#!/usr/bin/env python3
"""Backfill one explicit BarentsWatch Historic AIS window into ClickHouse."""

import argparse
from datetime import datetime, timezone
import json
import os
from typing import Any, Iterable

import requests
from dotenv import load_dotenv

from client import HistoricAISClient


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamps must include a UTC offset')
    return parsed.astimezone(timezone.utc)


def load_settings() -> dict[str, Any]:
    load_dotenv(os.path.join(ROOT, '.env'))
    names = ('BW_AIS_CLIENT_ID', 'BW_AIS_CLIENT_SECRET', 'BW_TOKEN_URL',
             'BW_HISTORIC_BASE_URL', 'BW_HISTORIC_SCOPE', 'BW_HISTORIC_WINDOW_HOURS',
             'BW_HISTORIC_BATCH_SIZE', 'BW_HISTORIC_REQUEST_TIMEOUT_SECONDS',
             'CLICKHOUSE_URL', 'CLICKHOUSE_USER', 'CLICKHOUSE_PASSWORD',
             'BW_HISTORIC_POLYGON_JSON')
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        raise RuntimeError('Missing required environment variable(s): ' + ', '.join(missing))
    try:
        polygon = json.loads(os.environ['BW_HISTORIC_POLYGON_JSON'])
        window_hours = float(os.environ['BW_HISTORIC_WINDOW_HOURS'])
        batch_size = int(os.environ['BW_HISTORIC_BATCH_SIZE'])
        timeout = float(os.environ['BW_HISTORIC_REQUEST_TIMEOUT_SECONDS'])
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError('Invalid historical AIS environment configuration') from exc
    if not isinstance(polygon, dict) or polygon.get('type') != 'Polygon':
        raise RuntimeError('BW_HISTORIC_POLYGON_JSON must contain a Polygon object')
    if window_hours <= 0 or batch_size <= 0 or timeout <= 0:
        raise RuntimeError('Historical AIS numeric settings must be positive')
    return {
        'client_id': os.environ['BW_AIS_CLIENT_ID'], 'client_secret': os.environ['BW_AIS_CLIENT_SECRET'],
        'token_url': os.environ['BW_TOKEN_URL'], 'base_url': os.environ['BW_HISTORIC_BASE_URL'],
        'scope': os.environ['BW_HISTORIC_SCOPE'], 'polygon': polygon,
        'batch_size': batch_size, 'timeout': timeout, 'clickhouse_url': os.environ['CLICKHOUSE_URL'],
        'clickhouse_user': os.environ['CLICKHOUSE_USER'], 'clickhouse_password': os.environ['CLICKHOUSE_PASSWORD'],
    }


def normalize_record(mmsi: int, record: dict) -> dict:
    mapping = {'msgtime': 'msgtime', 'latitude': 'latitude', 'longitude': 'longitude',
               'speedOverGround': 'speed_over_ground', 'courseOverGround': 'course_over_ground',
               'trueHeading': 'true_heading', 'rateOfTurn': 'rate_of_turn',
               'navigationalStatus': 'navigational_status', 'shipType': 'ship_type',
               'name': 'name', 'stream': 'stream'}
    row = {'mmsi': mmsi, **{target: record.get(source) for source, target in mapping.items()}}
    if not row['msgtime'] or row['latitude'] is None or row['longitude'] is None:
        raise ValueError('AIS record is missing msgtime or coordinates')
    return row


def batches(rows: Iterable[dict], size: int) -> Iterable[list]:
    batch = []
    for row in rows:
        batch.append(row)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_rows(settings: dict, rows: list) -> None:
    body = '\n'.join(json.dumps(row, allow_nan=False) for row in rows) + '\n'
    response = requests.post(settings['clickhouse_url'],
                     params={'query': 'INSERT INTO raw.ais_positions FORMAT JSONEachRow',
                         'date_time_input_format': 'best_effort'},
                             auth=(settings['clickhouse_user'], settings['clickhouse_password']),
                             data=body.encode(), timeout=settings['timeout'])
    if not response.ok:
        raise RuntimeError(
            f'ClickHouse insert failed (HTTP {response.status_code}): '
            f'{response.text[:2000]}'
        )


def run(start: datetime, end: datetime) -> dict:
    settings = load_settings()
    client = HistoricAISClient(settings['token_url'], settings['base_url'], settings['client_id'],
                               settings['client_secret'], settings['scope'], settings['timeout'])
    positions_received = positions_inserted = tracks_requested = empty_tracks = 0
    failed_tracks = []
    pending = []
    try:
        mmsis = client.mmsis_in_area(settings['polygon'], start, end)
        for mmsi in mmsis:
            tracks_requested += 1
            try:
                records = client.tracks(mmsi, start, end)
            except RuntimeError:
                failed_tracks.append(mmsi)
                raise
            if not records:
                empty_tracks += 1
            for record in records:
                positions_received += 1
                try:
                    pending.append(normalize_record(mmsi, record))
                except ValueError:
                    continue
                if len(pending) >= settings['batch_size']:
                    insert_rows(settings, pending)
                    positions_inserted += len(pending)
                    pending = []
        if pending:
            insert_rows(settings, pending)
            positions_inserted += len(pending)
    finally:
        client.close()
    report = {'window_start': start.isoformat(), 'window_end': end.isoformat(), 'mmsi_count': len(mmsis),
              'tracks_requested': tracks_requested, 'positions_received': positions_received,
              'positions_inserted': positions_inserted, 'empty_tracks': empty_tracks,
              'failed_tracks': failed_tracks}
    print(json.dumps(report, indent=2))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    args = parser.parse_args()
    start, end = parse_timestamp(args.start), parse_timestamp(args.end)
    if start >= end:
        parser.error('--start must be earlier than --end')
    run(start, end)


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, ValueError) as exc:
        print(f'Historic AIS ingestion failed: {exc}')
        raise SystemExit(1) from None