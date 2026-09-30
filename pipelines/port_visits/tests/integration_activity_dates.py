"""Opt-in date migration check in an isolated ClickHouse database; no Neo4j writes."""
from datetime import datetime, timedelta, timezone
import uuid

from dotenv import load_dotenv
from jinja2 import Environment, StrictUndefined

from pipelines.port_visits.run import ClickHouse, ROOT, publish_visits
from pipelines.port_visits.backfill_activity_dates import backfill


def main():
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    query = ch.query
    database = 'visit_date_test_' + uuid.uuid4().hex
    query('CREATE DATABASE ' + database)
    try:
        ch.query = lambda sql, params=None: query(sql.replace('analytics.', database + '.')
            .replace('DATABASE IF NOT EXISTS analytics', 'DATABASE IF NOT EXISTS ' + database), params)
        ch.initialize()
        start = datetime(2026, 9, 25, tzinfo=timezone.utc)
        row = dict(run_id='legacy-hash', visit_id='a', mmsi=123, port_id='P',
                   arrival_at=start - timedelta(days=1), last_observed_at=start,
                   departure_at=None, arrival_censored=1, end_reason='window_end',
                   observation_count=1, observed_stay_seconds=0, updated_at=start, is_deleted=0)
        ch.insert('port_visits', [row, dict(row, visit_id='b'), dict(row, visit_id='c', is_deleted=1)])
        ch.insert('port_visit_runs', [dict(run_id='legacy-hash', window_start=start,
            window_end=start + timedelta(days=1), dataset_hash='fixture', parameters='{}',
            source_rows=3, visit_count=2, completed_at=start)])
        # Exercise the idempotent upgrade statement too.
        migration = (ROOT / 'clickhouse/migrations/010_port_visits_activity_date.sql').read_text()
        ch.query('\n'.join(line for line in migration.splitlines() if not line.startswith('--')))
        assert backfill(ch) == 3
        assert ch.query('SELECT countIf(activity_date IS NULL) FROM analytics.port_visits FINAL').strip() == '3'
        assert backfill(ch, apply=True) == 3
        assert backfill(ch, apply=True) == 0
        sql = Environment(undefined=StrictUndefined).from_string(
            (ROOT / 'dbt/models/marts/current_port_visits.sql').read_text()).render(
            config=lambda **kw: '', source=lambda schema, table: database + '.' + table)
        ch.query('CREATE OR REPLACE VIEW analytics.current_port_visits AS ' + sql)
        assert ch.query("SELECT count(), countIf(activity_date = toDate('2026-09-25')) "
                        'FROM analytics.current_port_visits').strip() == '2\t2'
        publish_visits(ch, [row], 'legacy-hash', start, activity_date='2026-09-25')
        assert ch.query('SELECT count() FROM analytics.current_port_visits').strip() == '1'
        assert ch.query("SELECT count(), countIf(activity_date = toDate('2026-09-25')), sum(is_deleted) "
                        'FROM analytics.port_visits FINAL').strip() == '3\t3\t2'
        print('PASS: legacy dates use run metadata, rerun is a no-op, dates survive tombstones, current view propagates active dates')
    finally:
        query('DROP DATABASE ' + database)
        ch.session.close()


if __name__ == '__main__':
    main()
