"""Backfill missing logical visit dates from completed-run metadata; keep writers idle."""
import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import fcntl
import json

from dotenv import load_dotenv

from .run import ClickHouse, ROOT, timestamp


def plan_dates(rows, runs):
    """Fail before writes if metadata is missing or an existing date conflicts."""
    dates = {}
    for run in runs:
        identity = run['run_id']
        value = timestamp(run['window_start']).astimezone(timezone.utc).date().isoformat()
        if identity in dates and dates[identity] != value:
            raise RuntimeError(f'Conflicting run metadata for {identity}')
        dates[identity] = value
    pending = defaultdict(list)
    for row in rows:
        identity = row['run_id']
        if identity not in dates:
            raise RuntimeError(f'Missing completed-run metadata for {identity}; no date inferred')
        existing = row.get('activity_date')
        if existing is not None and existing != dates[identity]:
            raise RuntimeError(f'Existing activity_date conflicts with run metadata for {identity}')
        if existing is None:
            pending[identity].append(dict(row, activity_date=dates[identity]))
    return dict(pending)


def read_rows(ch, sql, params=None):
    return [json.loads(line) for line in ch.query(sql + ' FORMAT JSONEachRow', params).splitlines()]


def backfill(ch, *, apply=False):
    rows = read_rows(ch, 'SELECT * FROM analytics.port_visits FINAL ORDER BY run_id, visit_id')
    runs = read_rows(ch, "SELECT run_id, toString(window_start, 'UTC') AS window_start "
                        'FROM analytics.port_visit_runs FINAL')
    plan = plan_dates(rows, runs)
    total = sum(map(len, plan.values()))
    print(f'Visit activity_date backfill: runs={len(plan)} rows={total} apply={apply}')
    if not apply:
        return total
    for run_id, pending in sorted(plan.items()):
        params = {'param_run_id': run_id}
        latest = read_rows(ch, "SELECT toString(max(updated_at), 'UTC') AS latest "
                               'FROM analytics.port_visits WHERE run_id = {run_id:String}', params)[0]['latest']
        version = max(datetime.now(timezone.utc), timestamp(latest) + timedelta(microseconds=1))
        ch.insert('port_visits', [dict(row, updated_at=version) for row in pending],
                  batch_size=len(pending))
    after = read_rows(ch, 'SELECT * FROM analytics.port_visits FINAL ORDER BY run_id, visit_id')
    if plan_dates(after, runs):
        raise RuntimeError('Backfill left logical visits with missing activity_date')
    def unchanged(values):
        return [{k: v for k, v in row.items() if k not in ('activity_date', 'updated_at')}
                for row in values]
    if unchanged(rows) != unchanged(after):
        raise RuntimeError('Visit data changed during date backfill; keep all writers idle')
    print(f'Validated {len(after)} logical rows; visit payloads and deletion flags unchanged')
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Publish missing dates; default is read-only')
    args = parser.parse_args()
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    try:
        with (ROOT / 'pipelines/port_visits/.run.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            backfill(ch, apply=args.apply)
    finally:
        ch.session.close()


if __name__ == '__main__':
    main()
