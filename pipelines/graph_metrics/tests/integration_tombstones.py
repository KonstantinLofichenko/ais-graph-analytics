"""Opt-in ClickHouse fixture: temporary database, actual dbt view SQL, no Neo4j writes."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import uuid

from dotenv import load_dotenv
from jinja2 import Environment, StrictUndefined

from pipelines.graph_metrics.export import publish_metric_snapshot
from pipelines.graph_metrics.backfill_snapshot_dates import backfill
from pipelines.port_visits.backfill_activity_dates import read_rows
from pipelines.port_visits.run import ClickHouse, ROOT


def main():
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    query = ch.query
    database = 'graph_metric_test_' + uuid.uuid4().hex
    query('CREATE DATABASE ' + database)
    try:
        ch.query = lambda sql, params=None: query(sql.replace('analytics.', database + '.')
            .replace('DATABASE IF NOT EXISTS analytics', 'DATABASE IF NOT EXISTS ' + database), params)
        ch.initialize()
        for name in ('003_port_graph_metrics.sql', '006_graph_community_labels.sql',
                     '008_port_graph_metrics_tombstones.sql'):
            sql = (ROOT / 'clickhouse/migrations' / name).read_text()
            sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--'))
            for statement in sql.split(';'):
                if statement.strip():
                    ch.query(statement)
        ch.query('''CREATE TABLE analytics.countries (country_code String, country_name String,
                    iso3 String, numeric_code FixedString(3)) ENGINE = MergeTree ORDER BY country_code''')
        ch.query("INSERT INTO analytics.countries VALUES ('NO','Norway','NOR','578')")
        start = datetime(2026, 9, 25, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        ports = [dict(port_id=p, name=p, country='NO', latitude=60., longitude=5., radius_m=1500.)
                 for p in ('A', 'B', 'C')]
        ch.upsert_ports(ports, 'fixture', now)
        env = Environment(undefined=StrictUndefined)
        for name in ('port_graph_metrics_enriched', 'port_graph_communities'):
            sql = env.from_string((ROOT / 'dbt/models/marts' / (name + '.sql')).read_text()).render(
                config=lambda **kwargs: '', source=lambda schema, table: database + '.' + table,
                ref=lambda table: database + '.' + table)
            ch.query('CREATE VIEW analytics.' + name + ' AS ' + sql)
        metadata = dict(run_id='2026-09-25T00:00:00Z', window_start=start, window_end=start + timedelta(days=1))
        other = dict(run_id='2026-09-24T00:00:00Z', window_start=start-timedelta(days=1), window_end=start)
        def metrics(ids, meta):
            return [dict(meta, snapshot_date=meta['window_start'].date().isoformat(), port_id=p,
                         page_rank=.5, community_id=2 if p == 'C' else 1,
                         community_name=('South' if p == 'C' else 'North') if meta == metadata else None,
                         community_label=p if meta == metadata else None, exported_at=now) for p in ids]
        publish_metric_snapshot(ch, other, metrics(['A', 'C'], other), now, before_insert=lambda: None)
        for ids in (['A', 'B', 'C'], ['A', 'B'], ['A', 'B'], ['A'], [], [], ['A', 'C'], ['A', 'B', 'C']):
            publish_metric_snapshot(ch, metadata, metrics(ids, metadata), now, before_insert=lambda: None)
            rows = ch.query('''SELECT port_id FROM analytics.port_graph_metrics_enriched
                               WHERE run_id = {run_id:String} ORDER BY port_id FORMAT JSONEachRow''',
                            {'param_run_id': metadata['run_id']})
            assert [json.loads(line)['port_id'] for line in rows.splitlines()] == sorted(ids)
            rows = ch.query('''SELECT community_id, port_count FROM analytics.port_graph_communities
                               WHERE run_id = {run_id:String} ORDER BY community_id FORMAT JSONEachRow''',
                            {'param_run_id': metadata['run_id']})
            counts = {int(r['community_id']): int(r['port_count']) for r in map(json.loads, rows.splitlines())}
            expected = {}
            for port in ids:
                community = 2 if port == 'C' else 1
                expected[community] = expected.get(community, 0) + 1
            assert counts == expected, (counts, expected)
            for model in ('port_graph_metrics_enriched', 'port_graph_communities'):
                invalid = ch.query(
                    'SELECT count() FROM analytics.' + model + '''
                    WHERE activity_date != if(run_id = {run_id:String},
                        toDate('2026-09-25'), toDate('2026-09-24'))''',
                    {'param_run_id': metadata['run_id']}).strip()
                assert invalid == '0', (model, invalid)
        preserved = ch.query('''SELECT count(), countIf(is_deleted=0), countIf(community_name IS NULL),
                               countIf(community_label IS NULL)
                               FROM analytics.port_graph_metrics FINAL WHERE run_id = {run_id:String}''',
                             {'param_run_id': other['run_id']}).strip()
        assert preserved == '2\t2\t2\t2', preserved
        # Reproduce legacy window-end dates, including a deleted port, then correct
        # only date/version metadata. Keep old physical versions for retry coverage.
        publish_metric_snapshot(ch, metadata, metrics(['A', 'C'], metadata), now, before_insert=lambda: None)
        ch.query('SYSTEM STOP MERGES analytics.port_graph_metrics')
        old = read_rows(ch, 'SELECT * FROM analytics.port_graph_metrics FINAL')
        ch.insert('port_graph_metrics', [dict(row,
            snapshot_date=row['window_end'][:10], exported_at=now + timedelta(days=1)) for row in old])
        assert backfill(ch, apply=True) == 5
        assert backfill(ch, apply=True) == 0
        publish_metric_snapshot(ch, metadata, metrics(['A', 'C'], metadata), now, before_insert=lambda: None)
        assert ch.query('SELECT count() FROM analytics.port_graph_metrics_enriched').strip() == '4'
        print('PASS: 8 reconciliation transitions; enriched/community views exclude tombstones; other run and NULL labels preserved')
    finally:
        query('DROP DATABASE ' + database)
        ch.session.close()


if __name__ == '__main__':
    main()
