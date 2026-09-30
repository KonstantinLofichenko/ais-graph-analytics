"""Snapshot reconciliation regressions, with physical versions retained in memory."""
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock

from pipelines.graph_metrics.export import ExportValidationError, publish_metric_snapshot
from pipelines.port_visits.run import date_string

START = datetime(2026, 9, 25, tzinfo=timezone.utc)
NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
META = dict(run_id='2026-09-25T00:00:00Z', window_start=START, window_end=START + timedelta(days=1))


def metric(port, metadata=META):
    return dict(metadata, port_id=port, snapshot_date=metadata['window_start'].date().isoformat(),
                page_rank=0.5, community_id=1 if port != 'C' else 2,
                community_name='Community ' + port, community_label=port, exported_at=NOW)


class VersionedMetrics:
    def __init__(self):
        self.history = []
        self.batches = []
        self.queries = []

    def logical(self, run_id):
        latest = {}
        for row in self.history:
            if row['run_id'] == run_id:
                key = row['port_id']
                if key not in latest or row['exported_at'] > latest[key]['exported_at']:
                    latest[key] = row
        return list(latest.values())

    def active(self, run_id):
        return [r for r in self.logical(run_id) if not r.get('is_deleted', 0)]

    def insert(self, table, rows, *, batch_size=1000):
        assert table == 'port_graph_metrics'
        self.batches.append((deepcopy(rows), batch_size))
        self.history.extend(deepcopy(rows))

    def query(self, sql, params):
        self.queries.append(sql)
        rid = params['param_run_id']
        if 'maxOrNull' in sql:
            history = self.logical(rid) if 'FINAL' in sql else [r for r in self.history if r['run_id'] == rid]
            stamp = lambda v: date_string(v) if isinstance(v, datetime) else v
            windows = list({(stamp(r['window_start']), stamp(r['window_end']), r['snapshot_date']) for r in history})
            return json.dumps(dict(latest=max((r['exported_at'] for r in history), default=None),
                                   windows=windows), default=date_string)
        assert 'FINAL' in sql and 'is_deleted = 0' in sql
        return '\n'.join(json.dumps(row, default=date_string) for row in self.active(rid))


class MetricTombstoneTests(unittest.TestCase):
    def setUp(self):
        self.ch = VersionedMetrics()
        self.check = Mock()

    def publish(self, ports, metadata=META):
        rows = [metric(p, metadata) if isinstance(p, str) else p for p in ports]
        publish_metric_snapshot(self.ch, metadata, rows, NOW, before_insert=self.check)
        return {r['port_id'] for r in self.ch.active(metadata['run_id'])}

    def test_first_export_identical_and_repeated_reruns(self):
        for _ in range(3):
            self.assertEqual(self.publish(['A', 'B', 'C']), {'A', 'B', 'C'})
        self.assertEqual(len(self.ch.history), 9)
        self.assertEqual(len(self.ch.active(META['run_id'])), 3)
        self.assertEqual(self.check.call_count, 3)

    def test_one_disappearing_port_preserves_tombstone_values(self):
        self.publish(['A', 'B', 'C'])
        old = deepcopy(self.ch.logical(META['run_id'])[-1])
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        deleted = next(r for r in self.ch.logical(META['run_id']) if r['port_id'] == 'C')
        self.assertEqual(deleted['is_deleted'], 1)
        self.assertGreater(deleted['exported_at'], old['exported_at'])
        for key, value in old.items():
            if key not in ('exported_at', 'is_deleted'):
                self.assertEqual(deleted[key], date_string(value) if isinstance(value, datetime) else value)
        payload, batch_size = self.ch.batches[-1]
        self.assertEqual(len(payload), batch_size)
        self.assertEqual(len({r['exported_at'] for r in payload}), 1)
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        self.assertEqual(sum(r.get('is_deleted', 0) for r in self.ch.history), 1)

    def test_multiple_ports_disappear(self):
        self.publish(['A', 'B', 'C'])
        self.assertEqual(self.publish(['A']), {'A'})
        self.assertEqual(sum(r['is_deleted'] for r in self.ch.logical(META['run_id'])), 2)

    def test_nonempty_empty_empty_nonempty_transitions(self):
        self.publish(['A', 'B', 'C'])
        self.assertEqual(self.publish([]), set())
        self.assertEqual(sum(r['is_deleted'] for r in self.ch.logical(META['run_id'])), 3)
        self.assertEqual(self.publish([]), set())
        self.assertEqual(len(self.ch.history), 6)
        self.assertEqual(self.publish(['A', 'B']), {'A', 'B'})
        self.assertEqual(sum(r['is_deleted'] for r in self.ch.logical(META['run_id'])), 1)

    def test_first_empty_then_new_port(self):
        self.assertEqual(self.publish([]), set())
        self.assertEqual(self.publish(['D']), {'D'})
        self.assertEqual(self.publish(['D', 'E']), {'D', 'E'})

    def test_changed_scores_and_labels_replace_same_key(self):
        self.publish(['A'])
        changed = dict(metric('A'), page_rank=2.0, community_id=99,
                       community_name='New name', community_label='New label')
        self.publish([changed])
        row = self.ch.active(META['run_id'])[0]
        for key in ('page_rank', 'community_id', 'community_name', 'community_label'):
            self.assertEqual(row[key], changed[key])
        self.assertEqual(len(self.ch.logical(META['run_id'])), 1)

    def test_other_run_and_historical_null_labels_untouched(self):
        other = dict(META, run_id='2026-09-24T00:00:00Z', window_start=START-timedelta(days=1), window_end=START)
        self.publish([dict(metric('A', other), community_name=None, community_label=None)], other)
        before = deepcopy(self.ch.logical(other['run_id']))
        self.publish(['A', 'B'])
        self.publish(['B'])
        self.assertEqual(self.ch.logical(other['run_id']), before)
        self.assertEqual(self.ch.active(other['run_id'])[0]['community_name'], None)

    def test_clock_behind_tombstone_still_reactivates(self):
        row = dict(metric('A'), is_deleted=1, exported_at=NOW + timedelta(days=1))
        self.ch.history.append(row)
        self.publish(['A'])
        self.assertEqual(self.ch.active(META['run_id'])[0]['exported_at'], row['exported_at'] + timedelta(microseconds=1))

    def test_window_change_rejected_even_when_previous_port_deleted(self):
        self.publish(['A'])
        self.publish([])
        changed = dict(META, window_end=START + timedelta(days=10))
        before = deepcopy(self.ch.history)
        with self.assertRaisesRegex(ExportValidationError, 'different window'):
            self.publish(['A'], changed)
        self.assertEqual(self.ch.history, before)

    def test_equal_count_wrong_membership_and_duplicate_rows_fail(self):
        for result in ('{"port_id":"B"}', '{"port_id":"A"}\n{"port_id":"A"}'):
            ch = Mock()
            ch.query.side_effect = ['', '{"latest":null,"windows":[]}', result]
            with self.subTest(result=result), self.assertRaisesRegex(ExportValidationError, 'membership mismatch'):
                publish_metric_snapshot(ch, META, [metric('A')], NOW, before_insert=self.check)

    def test_insert_validation_and_source_check_errors_propagate(self):
        for phase in ('insert', 'validate', 'source'):
            ch = Mock()
            ch.query.side_effect = ['', '{"latest":null,"windows":[]}', RuntimeError('validation failed')]
            before = Mock()
            if phase == 'insert':
                ch.insert.side_effect = RuntimeError('insert failed')
            if phase == 'source':
                before.side_effect = RuntimeError('graph changed')
            with self.subTest(phase=phase), self.assertRaises(RuntimeError):
                publish_metric_snapshot(ch, META, [metric('A')], NOW, before_insert=before)
            if phase == 'source':
                ch.insert.assert_not_called()

    def test_duplicate_or_cross_run_rows_fail_before_writes(self):
        for rows in ([metric('A'), metric('A')], [dict(metric('A'), run_id='wrong')]):
            with self.subTest(rows=rows), self.assertRaises(ExportValidationError):
                self.publish(rows)
        self.assertEqual(self.ch.history, [])


if __name__ == '__main__':
    unittest.main()
