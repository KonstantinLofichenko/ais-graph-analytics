"""Correct legacy window-end snapshot dates with writers idle; default is dry-run."""
import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from pipelines.port_visits.run import ClickHouse, ROOT, timestamp
from pipelines.port_visits.backfill_activity_dates import read_rows


def plan_dates(rows):
    pending = defaultdict(list)
    for row in rows:
        expected = timestamp(row['window_start']).astimezone(timezone.utc).date().isoformat()
        if row['snapshot_date'][:7] != expected[:7]:
            raise RuntimeError('Snapshot-date correction would cross monthly partitions; '
                               'a separately reviewed partition migration is required')
        if row['snapshot_date'] != expected:
            pending[row['run_id']].append(dict(row, snapshot_date=expected))
    return dict(pending)


def backfill(ch, *, apply=False):
    # Check every physical partition, not only the winners returned by FINAL.
    partitions = read_rows(ch, 'SELECT DISTINCT run_id, toString(window_start, \'UTC\') AS window_start, '
                              'toString(snapshot_date) AS snapshot_date FROM analytics.port_graph_metrics')
    plan_dates(partitions)
    rows = read_rows(ch, 'SELECT * FROM analytics.port_graph_metrics FINAL ORDER BY run_id, port_id')
    plan = plan_dates(rows)
    total = sum(map(len, plan.values()))
    print(f'Graph snapshot_date backfill: runs={len(plan)} rows={total} apply={apply}')
    if not apply:
        return total
    for run_id, pending in sorted(plan.items()):
        latest = read_rows(ch, "SELECT toString(max(exported_at), 'UTC') AS latest "
                               'FROM analytics.port_graph_metrics WHERE run_id = {run_id:String}',
                           {'param_run_id': run_id})[0]['latest']
        version = max(datetime.now(timezone.utc), timestamp(latest) + timedelta(microseconds=1))
        ch.insert('port_graph_metrics', [dict(row, exported_at=version) for row in pending],
                  batch_size=len(pending))
    after = read_rows(ch, 'SELECT * FROM analytics.port_graph_metrics FINAL ORDER BY run_id, port_id')
    if plan_dates(after):
        raise RuntimeError('Backfill left incorrect logical snapshot dates')
    def unchanged(values):
        return [{k: v for k, v in row.items() if k not in ('snapshot_date', 'exported_at')}
                for row in values]
    if unchanged(rows) != unchanged(after):
        raise RuntimeError('Graph data changed during date backfill; keep all writers idle')
    print(f'Validated {len(after)} logical rows; metrics, labels and deletion flags unchanged')
    return total


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true', help='Publish corrected dates; default is read-only')
    args = parser.parse_args()
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    try:
        backfill(ch, apply=args.apply)
    finally:
        ch.session.close()


if __name__ == '__main__':
    main()
