"""Insert-only replacement semantics; databases are mocked, including FINAL reads."""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from pipelines.port_visits import run
from pipelines.port_connections.run import read_completed_visits

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
START = datetime(2026, 9, 25, tzinfo=timezone.utc)
RUN_ID = run.canonical_run_id(START)


def visit(identity):
    return dict(visit_id=identity, mmsi=258216000, port_id='WPI:23530',
                arrival_at=START, last_observed_at=START + timedelta(minutes=20),
                departure_at=None, arrival_censored=1, end_reason='data_gap',
                observation_count=3, observed_stay_seconds=1200.0)


class VersionedVisits:
    """Retain physical versions; emulate ReplacingMergeTree's latest logical rows."""
    def __init__(self):
        self.history = []
        self.calls = []
        self.batches = []
        self.completed = None

    def logical(self):
        latest = {}
        for row in self.history:
            key = (row['run_id'], row['visit_id'])
            if key not in latest or row['updated_at'] > latest[key]['updated_at']:
                latest[key] = row
        return list(latest.values())

    def active(self):
        return [r for r in self.logical() if not r['is_deleted']]

    def insert(self, table, rows, *, batch_size=1000):
        assert table == 'port_visits'
        self.batches.append((deepcopy(rows), batch_size))
        self.history.extend(deepcopy(rows))

    def query(self, sql, params):
        self.calls.append((sql, params))
        assert params['param_run_id'] == RUN_ID
        if 'port_visit_runs' in sql:
            return json.dumps(self.completed, default=run.date_string)
        if 'maxOrNull' in sql:
            return json.dumps({'latest': max((r['updated_at'] for r in self.history), default=None)},
                              default=run.date_string)
        assert 'FINAL' in sql and 'is_deleted = 0' in sql, sql
        if 'count()' in sql:
            return json.dumps({'active_visits': len(self.active())})
        return '\n'.join(json.dumps(r, default=run.date_string) for r in self.active())


class SnapshotPublishTests(unittest.TestCase):
    def setUp(self):
        self.ch = VersionedVisits()

    def publish(self, ids):
        rows = [visit(i) if isinstance(i, str) else i for i in ids]
        with redirect_stdout(io.StringIO()):
            run.publish_visits(self.ch, rows, RUN_ID, NOW, activity_date=START.date().isoformat())
        return {r['visit_id'] for r in self.ch.active()}

    def test_first_run_and_identical_retry_have_unique_active_rows(self):
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        self.assertEqual(len(self.ch.history), 4)
        self.assertEqual(len(self.ch.active()), 2)
        self.assertTrue(all(r['is_deleted'] == 0 for r in self.ch.logical()))

    def test_disappearing_visit_is_tombstoned_and_repeat_is_idempotent(self):
        self.publish(['A', 'B', 'C'])
        previous = deepcopy(self.ch.active()[-1])
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        tombstone = next(r for r in self.ch.logical() if r['visit_id'] == 'C')
        for key, value in previous.items():
            if key not in ('updated_at', 'is_deleted'):
                # JSON round trip formats timestamps but preserves instants/values.
                self.assertEqual(tombstone[key], run.date_string(value) if isinstance(value, datetime) else value)
        self.assertEqual(tombstone['is_deleted'], 1)
        self.assertGreater(tombstone['updated_at'], previous['updated_at'])
        rows, batch_size = self.ch.batches[-1]
        self.assertEqual(batch_size, 3)
        self.assertEqual({r['updated_at'] for r in rows}, {tombstone['updated_at']})
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        self.assertEqual(sum(r['is_deleted'] for r in self.ch.history), 1)

    def test_new_and_reappearing_visits_are_active(self):
        self.publish(['A', 'B'])
        self.publish(['A'])
        self.assertEqual(self.publish(['A', 'B', 'C']), {'A', 'B', 'C'})
        self.assertEqual(len(self.ch.logical()), 3)

    def test_changed_visit_with_same_id_replaces_values(self):
        self.publish(['A'])
        changed = dict(visit('A'), observation_count=7, end_reason='window_end')
        self.publish([changed])
        self.assertEqual(len(self.ch.active()), 1)
        self.assertEqual(self.ch.active()[0]['observation_count'], 7)
        self.assertEqual(self.ch.active()[0]['end_reason'], 'window_end')

    def test_multiple_obsolete_and_empty_snapshots(self):
        self.publish(['A', 'B', 'C'])
        self.assertEqual(self.publish([]), set())
        self.assertEqual(sum(r['is_deleted'] for r in self.ch.logical()), 3)
        self.assertEqual(self.publish([]), set())
        self.assertEqual(len(self.ch.history), 6)

    def test_clock_behind_existing_versions_still_writes_newer(self):
        self.ch.insert('port_visits', [dict(visit('A'), run_id=RUN_ID,
                       is_deleted=1, updated_at=NOW + timedelta(days=1))])
        self.publish(['A'])
        self.assertEqual(self.ch.active()[0]['updated_at'], NOW + timedelta(days=1, microseconds=1))

    def test_duplicate_calculated_ids_rejected_before_writes(self):
        with self.assertRaisesRegex(RuntimeError, 'duplicate visit IDs'):
            self.publish(['A', 'A'])
        self.assertEqual(self.ch.history, [])

    def test_connections_use_only_active_visits_and_keep_count_guard(self):
        self.publish(['A', 'B', 'C'])
        self.publish(['A', 'B'])
        end = START + timedelta(days=1)
        metadata = dict(run_id=RUN_ID, window_start=START.isoformat(), window_end=end.isoformat())
        self.ch.completed = dict(metadata, visit_count=2)
        rows = read_completed_visits(self.ch, metadata)
        self.assertEqual({v['visit_id'] for v in rows}, {'A', 'B'})
        self.ch.completed['visit_count'] = 3
        with self.assertRaisesRegex(RuntimeError, 'inconsistent'):
            read_completed_visits(self.ch, metadata)

    def test_dbt_and_bootstrap_views_filter_deleted_rows(self):
        for name in ('dbt/models/marts/current_port_visits.sql',
                     'dbt/models/marts/port_graph_metrics_enriched.sql',
                     'clickhouse/init/02_port_visits.sql',
                     'clickhouse/migrations/007_port_visits_tombstones.sql'):
            with self.subTest(name=name):
                sql = (run.ROOT / name).read_text().lower()
                self.assertIn('is_deleted = 0', sql)
                self.assertIn('final', sql)


class CompletionTests(unittest.TestCase):
    def test_publish_or_validation_failure_never_writes_completion_or_graph(self):
        for mode in ('insert_failure', 'count_mismatch', 'count_failure'):
            with self.subTest(mode=mode), patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused'}), \
                    patch.object(run.GraphDatabase, 'driver') as driver:
                ch = Mock()
                ch.query.side_effect = ['', '', '{"latest":null}',
                                        RuntimeError('count failed') if mode == 'count_failure'
                                        else '{"active_visits":99}']
                if mode == 'insert_failure':
                    ch.insert.side_effect = RuntimeError('insert failed')
                with self.assertRaises(RuntimeError):
                    run.publish(ch, [], [visit('A')], [], RUN_ID, 'hash', START,
                                START + timedelta(days=1), {}, {'source_rows': 3})
                self.assertTrue(all(c.args[0] != 'port_visit_runs' for c in ch.insert.call_args_list))
                session = driver.return_value.__enter__.return_value.session.return_value.__enter__.return_value
                session.execute_write.assert_not_called()

    def test_completed_count_written_only_after_validation_and_graph_success(self):
        for graph_failure in (False, True):
            with self.subTest(graph_failure=graph_failure), \
                    patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused'}), \
                    patch.object(run.GraphDatabase, 'driver') as driver, redirect_stdout(io.StringIO()):
                ch = Mock()
                events = []
                answers = iter(['', '', '{"latest":null}', '{"active_visits":2}'])
                def query(sql, *args):
                    events.append('validate' if 'active_visits' in sql else 'read')
                    return next(answers)
                ch.query.side_effect = query
                ch.insert.side_effect = lambda table, *a, **k: events.append(table)
                session = driver.return_value.__enter__.return_value.session.return_value.__enter__.return_value
                def graph(*args):
                    events.append('graph')
                    if graph_failure:
                        raise RuntimeError('graph failed')
                session.execute_write.side_effect = graph
                args = (ch, [], [visit('A'), visit('B')], [], RUN_ID, 'hash', START,
                        START + timedelta(days=1), {}, {'source_rows': 3})
                if graph_failure:
                    with self.assertRaisesRegex(RuntimeError, 'graph failed'):
                        run.publish(*args)
                    self.assertNotIn('port_visit_runs', events)
                else:
                    run.publish(*args)
                    self.assertEqual(events[-4:], ['port_visits', 'validate', 'graph', 'port_visit_runs'])
                    self.assertEqual(ch.insert.call_args.args[1][0]['visit_count'], 2)

    def test_snapshot_over_1000_rows_is_one_insert_request(self):
        ch = object.__new__(run.ClickHouse)
        ch.query = Mock()
        ch.insert('port_visits', [{'visit_id': str(i)} for i in range(1734)], batch_size=1734)
        ch.query.assert_called_once()
        self.assertEqual(len(ch.query.call_args.args[0].splitlines()), 1735)


if __name__ == '__main__':
    unittest.main()
