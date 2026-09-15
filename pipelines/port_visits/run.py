#!/usr/bin/env python3
"""Preview or publish a bounded port-visit snapshot. Run one writer at a time."""
import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import sys

import requests
from dotenv import load_dotenv
from neo4j import GraphDatabase

if __package__:
    from .detect import detect, digest, summaries, timestamp, validate_ports
else:
    from detect import detect, digest, summaries, timestamp, validate_ports

ROOT = Path(__file__).resolve().parents[2]
OWNER = 'port-visits-v1'


def date_string(value):
    return value.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


class ClickHouse:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = (os.getenv('CLICKHOUSE_USER', 'default'), os.environ['CLICKHOUSE_PASSWORD'])
        self.url = os.getenv('CLICKHOUSE_URL', 'http://localhost:8123')

    def query(self, sql, params=None):
        response = self.session.post(self.url, params={'wait_end_of_query': '1', **(params or {})},
                                     data=sql.encode(), timeout=(10, 120))
        if not response.ok:
            raise RuntimeError(f'ClickHouse query failed (HTTP {response.status_code}):\n{response.text[:2000]}')
        return response.text

    def insert(self, table, rows):
        for offset in range(0, len(rows), 1000):
            body = '\n'.join(json.dumps(row, default=date_string, allow_nan=False) for row in rows[offset:offset+1000])
            self.query('INSERT INTO analytics.' + table + ' FORMAT JSONEachRow\n' + body)

    def upsert_ports(self, ports, dataset_hash, now):
        # Include retained versions when backfilling legacy creation timestamps.
        existing = self.query('SELECT port_id, toString(min(created_at), \'UTC\') AS created_at '
                              'FROM analytics.ports GROUP BY port_id FORMAT JSONEachRow')
        created = {row['port_id']: timestamp(row['created_at'])
                   for row in (json.loads(line) for line in existing.splitlines() if line)}
        self.insert('ports', [dict(p, dataset_hash=dataset_hash, updated_at=now,
                                  created_at=created.get(p['port_id'], now)) for p in ports])

    def initialize(self):
        sql = (ROOT / 'clickhouse/init/02_port_visits.sql').read_text()
        sql = '\n'.join(line for line in sql.splitlines() if not line.lstrip().startswith('--'))
        for statement in sql.split(';'):
            if statement.strip():
                self.query(statement)

    def count_positions(self, start, end):
        # Same FINAL/window predicate as positions(); measured before loading any rows.
        sql = '''SELECT count() AS actual_rows FROM raw.ais_positions FINAL
                 WHERE msgtime >= {start:DateTime64(6, 'UTC')}
                   AND msgtime < {end:DateTime64(6, 'UTC')} FORMAT JSONEachRow'''
        data = self.query(sql, {'param_start': date_string(start), 'param_end': date_string(end)})
        return int(json.loads(data.splitlines()[0])['actual_rows'])

    def positions(self, start, end, max_rows):
        # Include outside positions so exits are detectable. Never truncate silently.
        sql = '''SELECT mmsi, toString(msgtime, 'UTC') AS msgtime, latitude, longitude,
                        speed_over_ground
                 FROM raw.ais_positions AS source FINAL
                 WHERE source.msgtime >= {start:DateTime64(6, 'UTC')}
                   AND source.msgtime < {end:DateTime64(6, 'UTC')}
                 ORDER BY mmsi, msgtime, latitude, longitude, speed_over_ground
                 LIMIT {limit:UInt64} FORMAT JSONEachRow'''
        data = self.query(sql, {'param_start': date_string(start), 'param_end': date_string(end),
                               'param_limit': str(max_rows+1)})
        rows = [json.loads(line) for line in data.splitlines() if line]
        if len(rows) > max_rows:
            raise RuntimeError('Input exceeds --max-rows; narrow the window or explicitly raise the limit')
        return rows


def enforce_row_limit(actual_rows, max_rows):
    # A hard safety ceiling, never auto-raised to actual_rows: catches unexpectedly wide windows.
    if actual_rows > max_rows:
        raise RuntimeError(f'Input row count {actual_rows:,} exceeds safety limit {max_rows:,}')


def batch_id(start, end):
    # Identity of an explicit window: version + normalized UTC bounds only, never wall-clock
    # or fetched rows, so retrying the same window always resolves to the same logical batch.
    return digest([OWNER, start.astimezone(timezone.utc).isoformat(), end.astimezone(timezone.utc).isoformat()])


def graph_snapshot(tx, ports, counts, run_id, start, end):
    # The managed summary is replaced atomically. No live Vessel properties are set.
    tx.run('''MATCH ()-[r:VISITED]->() WHERE r.managedBy = $owner DELETE r''', owner=OWNER).consume()
    tx.run('''UNWIND $ports AS row
              MERGE (p:Port {portId: row.port_id})
              SET p.createdAt=coalesce(p.createdAt, datetime()), p.updatedAt=datetime(),
                  p.name=row.name, p.country=row.country, p.latitude=row.latitude,
                  p.longitude=row.longitude, p.radiusM=row.radius_m,
                  p.referenceSource='NGA WPI via UN OCHA', p.datasetScope=$owner''',
           ports=ports, owner=OWNER).consume()
    for offset in range(0, len(counts), 1000):
        tx.run('''UNWIND $rows AS row
                  MERGE (v:Vessel {mmsi: row.mmsi})
                  WITH v, row MATCH (p:Port {portId: row.port_id})
                  CREATE (v)-[:VISITED {managedBy:$owner, visitCount:row.visit_count,
                         runId:$run, windowStart:datetime($start), windowEnd:datetime($end)}]->(p)''',
               rows=counts[offset:offset+1000], owner=OWNER, run=run_id,
               start=start.isoformat(), end=end.isoformat()).consume()
    # Metadata on ports also identifies empty snapshots without introducing an extra node type.
    tx.run('''MATCH (p:Port) WHERE p.datasetScope=$owner
              SET p.visitRunId=$run, p.visitWindowStart=datetime($start),
                  p.visitWindowEnd=datetime($end)''', owner=OWNER, run=run_id,
           start=start.isoformat(), end=end.isoformat()).consume()


def publish(ch, ports, visits, counts, run_id, start, end, parameters, stats):
    now = datetime.now(timezone.utc)
    dataset_hash = digest(ports)
    with GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                              auth=(os.getenv('NEO4J_USER', 'neo4j'), os.environ['NEO4J_PASSWORD'])) as driver:
        driver.verify_connectivity()
        with driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
            session.run('CREATE CONSTRAINT port_id_unique IF NOT EXISTS FOR (p:Port) REQUIRE p.portId IS UNIQUE').consume()
            session.run('CREATE CONSTRAINT vessel_mmsi_unique IF NOT EXISTS FOR (v:Vessel) REQUIRE v.mmsi IS UNIQUE').consume()
            # Persist visits before exposing graph results. Retrying the same inputs uses the same IDs.
            ch.upsert_ports(ports, dataset_hash, now)
            ch.insert('port_visits', [dict(v, run_id=run_id, updated_at=now) for v in visits])
            session.execute_write(graph_snapshot, ports, counts, run_id, start, end)
            ch.insert('port_visit_runs', [dict(run_id=run_id, window_start=start, window_end=end,
                      dataset_hash=dataset_hash, parameters=json.dumps(parameters, sort_keys=True),
                      source_rows=stats['source_rows'], visit_count=len(visits), completed_at=datetime.now(timezone.utc))])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', help='Inclusive UTC timestamp; default: end minus 30 days')
    parser.add_argument('--end', help='Exclusive UTC timestamp; default: current UTC time')
    parser.add_argument('--ports', type=Path, default=ROOT / 'data/ports/bergen.json')
    parser.add_argument('--min-stay-minutes', type=float, default=20)
    parser.add_argument('--max-gap-minutes', type=float, default=15)
    parser.add_argument('--max-speed-knots', type=float, default=3)
    parser.add_argument('--max-rows', type=int, default=500000)
    parser.add_argument('--max-rows-source', default='cli', help='Label for how --start/--end/--max-rows were resolved (logging only)')
    parser.add_argument('--apply', action='store_true', help='Write ClickHouse snapshots and refresh managed graph summaries')
    parser.add_argument('--init', action='store_true', help='Create additive ClickHouse objects; requires --apply')
    parser.add_argument('--report', type=Path, help='Write a JSON preview/report to this file')
    parser.add_argument('--result-json', type=Path,
                        help='Write small completed-run metadata for Airflow; requires --apply')
    args = parser.parse_args()
    if args.init and not args.apply:
        parser.error('--init requires --apply')
    if args.result_json and not args.apply:
        parser.error('--result-json requires --apply')
    end = timestamp(args.end) if args.end else datetime.now(timezone.utc)
    start = timestamp(args.start) if args.start else end-timedelta(days=30)
    if start >= end or args.max_rows < 1:
        parser.error('Require start < end and max-rows > 0')
    ports = sorted(json.loads(args.ports.read_text()), key=lambda p: p['port_id'])
    validate_ports(ports)
    parameters = dict(version=OWNER, min_stay=args.min_stay_minutes*60,
                      max_gap=args.max_gap_minutes*60, max_speed=args.max_speed_knots)
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    try:
        # Lock preview too: one local pipeline run at a time. No distributed scheduler yet.
        with (ROOT / 'pipelines/port_visits/.run.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            actual_rows = ch.count_positions(start, end)
            print(f'Port visit input: start={start.isoformat()} end={end.isoformat()} '
                  f'actual_rows={actual_rows} max_rows={args.max_rows} source={args.max_rows_source}')
            enforce_row_limit(actual_rows, args.max_rows)
            rows = ch.positions(start, end, args.max_rows)
            visits, stats = detect(rows, ports, **{k: v for k, v in parameters.items() if k != 'version'})
            counts = summaries(visits)
            run_id = batch_id(start, end)
            report = dict(run_id=run_id, mode='apply' if args.apply else 'preview',
                          window_start=start, window_end=end, ports=len(ports), parameters=parameters,
                          actual_rows=actual_rows, statistics=stats, relationships=len(counts),
                          visits=visits, summaries=counts)
            if args.apply:
                if args.init:
                    ch.initialize()
                publish(ch, ports, visits, counts, run_id, start, end, parameters, stats)
            if args.result_json:
                args.result_json.write_text(json.dumps(dict(
                    run_id=run_id, window_start=start.isoformat(), window_end=end.isoformat()))+'\n')
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(json.dumps(report, default=date_string, indent=2)+'\n')
            print(json.dumps({k: v for k, v in report.items() if k not in ('visits', 'summaries')}, default=date_string, indent=2))
    finally:
        ch.session.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Avoid credentials and server response bodies in default logs.
        detail = str(exc) if isinstance(exc, (RuntimeError, KeyError)) else 'verify configuration and database logs'
        print(f'Port visit batch failed ({type(exc).__name__}): {detail}', file=sys.stderr)
        raise SystemExit(1) from None
