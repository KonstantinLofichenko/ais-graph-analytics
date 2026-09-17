from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock, Mock, call, patch
from zoneinfo import ZoneInfo

from neo4j.time import DateTime as Neo4jDateTime

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.graph_metrics.export import (
    ExportValidationError, OWNER, UINT64_MAX, build_export_rows, export_metrics,
    validate_snapshot_metadata,
)

METADATA = dict(run_id='2026-09-14T08:00:00Z', window_start='2026-09-14T08:00:00+00:00',
                window_end='2026-09-15T08:00:00+00:00')
PORTS = [dict(port_id='B', page_rank=0.75, community_id=2, visit_run_id=METADATA['run_id']),
         dict(port_id='A', page_rank=0.25, community_id=1, visit_run_id=METADATA['run_id'])]
EXPORTED_AT = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)


class SnapshotMetadataTests(unittest.TestCase):
    def test_reuses_run_id_and_normalizes_window_to_utc(self):
        metadata = dict(METADATA, window_start='2026-09-14T11:00:00+03:00',
                        window_end='2026-09-15T08:00:00Z')
        actual = validate_snapshot_metadata([metadata])
        self.assertEqual(actual['run_id'], METADATA['run_id'])
        self.assertEqual(actual['window_start'].isoformat(), METADATA['window_start'])
        self.assertEqual(actual['window_end'].isoformat(), METADATA['window_end'])

    def test_no_relationship_metadata_fails(self):
        with self.assertRaisesRegex(ExportValidationError, 'No managed CONNECTED_TO snapshot'):
            validate_snapshot_metadata([])

    def test_native_neo4j_named_zone_timestamps_normalize_to_utc(self):
        oslo = ZoneInfo('Europe/Oslo')
        metadata = dict(METADATA,
                        window_start=Neo4jDateTime.from_native(datetime(2026, 9, 14, 10, tzinfo=oslo)),
                        window_end=Neo4jDateTime.from_native(datetime(2026, 9, 15, 10, tzinfo=oslo)))
        actual = validate_snapshot_metadata([metadata])
        self.assertEqual(actual['window_start'].isoformat(), METADATA['window_start'])
        self.assertEqual(actual['window_end'].isoformat(), METADATA['window_end'])

    def test_python_datetimes_are_supported_and_native_naive_values_fail(self):
        metadata = dict(METADATA, window_start=datetime(2026, 9, 14, 8, tzinfo=timezone.utc))
        self.assertEqual(validate_snapshot_metadata([metadata])['window_start'], metadata['window_start'])
        for value in (datetime(2026, 9, 14, 8), Neo4jDateTime(2026, 9, 14, 8)):
            with self.subTest(value=value), self.assertRaisesRegex(ExportValidationError, 'timezone'):
                validate_snapshot_metadata([dict(METADATA, window_start=value)])

    def test_mixed_run_or_window_metadata_fails(self):
        for field, value in [('run_id', 'another-run'), ('window_start', '2026-09-13T08:00:00Z'),
                             ('window_end', '2026-09-16T08:00:00Z')]:
            with self.subTest(field=field), self.assertRaisesRegex(ExportValidationError, 'mixed run/window'):
                validate_snapshot_metadata([METADATA, dict(METADATA, **{field: value})])

    def test_missing_and_empty_metadata_fails(self):
        for field in METADATA:
            for value in (None, '', ' '):
                with self.subTest(field=field, value=value), self.assertRaises(ExportValidationError):
                    validate_snapshot_metadata([dict(METADATA, **{field: value})])
            missing = {key: value for key, value in METADATA.items() if key != field}
            with self.subTest(missing=field), self.assertRaises(ExportValidationError):
                validate_snapshot_metadata([missing])

    def test_naive_invalid_and_reversed_windows_fail(self):
        for field, value in [('window_start', '2026-09-14T08:00:00'),
                             ('window_end', '2026-09-15T08:00:00'),
                             ('window_start', 'invalid'),
                             ('window_end', METADATA['window_start']),
                             ('window_end', '2026-09-13T08:00:00Z')]:
            with self.subTest(field=field, value=value), self.assertRaises(ExportValidationError):
                validate_snapshot_metadata([dict(METADATA, **{field: value})])


class ExportRowTests(unittest.TestCase):
    def test_rows_have_exact_schema_are_sorted_and_share_one_export_timestamp(self):
        rows = build_export_rows([METADATA], PORTS, EXPORTED_AT)
        self.assertEqual([row['port_id'] for row in rows], ['A', 'B'])
        self.assertEqual(set(rows[0]), {'run_id', 'window_start', 'window_end', 'snapshot_date',
                                        'port_id', 'page_rank', 'community_id', 'exported_at'})
        self.assertEqual(rows[0]['page_rank'], 0.25)
        self.assertEqual(rows[1]['community_id'], 2)
        self.assertTrue(all(row['exported_at'] == EXPORTED_AT for row in rows))
        self.assertEqual(PORTS[0]['port_id'], 'B')  # Input is unchanged.

    def test_retry_on_another_date_keeps_run_port_keys_and_partition(self):
        later = datetime(2026, 10, 3, tzinfo=timezone.utc)
        first = build_export_rows([METADATA], PORTS, EXPORTED_AT)
        retry = build_export_rows([METADATA], PORTS, later)
        keys = lambda rows: [(r['run_id'], r['port_id'], r['snapshot_date']) for r in rows]
        self.assertEqual(keys(first), keys(retry))
        self.assertEqual(first[0]['snapshot_date'], '2026-09-15')
        self.assertNotEqual(first[0]['exported_at'], retry[0]['exported_at'])

    def test_partition_date_uses_utc_end_date_not_offset_date(self):
        metadata = dict(METADATA, window_end='2026-09-16T01:00:00+03:00')
        rows = build_export_rows([metadata], PORTS, EXPORTED_AT)
        self.assertEqual(rows[0]['snapshot_date'], '2026-09-15')
        self.assertEqual(rows[0]['window_end'].isoformat(), '2026-09-15T22:00:00+00:00')

    def test_zero_metrics_and_uint64_upper_bound_are_valid(self):
        ports = [dict(port_id='A', page_rank=0, community_id=0, visit_run_id=METADATA['run_id']),
                 dict(port_id='B', page_rank=2, community_id=UINT64_MAX,
                      visit_run_id=METADATA['run_id'])]
        rows = build_export_rows([METADATA], ports, EXPORTED_AT)
        self.assertEqual(rows[0]['page_rank'], 0.0)
        self.assertEqual(rows[1]['community_id'], UINT64_MAX)

    def test_missing_metrics_fail_instead_of_skipping_ports(self):
        for field in ('page_rank', 'community_id'):
            port = {key: value for key, value in PORTS[0].items() if key != field}
            with self.subTest(field=field), self.assertRaisesRegex(ExportValidationError, 'Port B:'):
                build_export_rows([METADATA], [PORTS[1], port], EXPORTED_AT)

    def test_invalid_page_rank_fails(self):
        for value in (None, True, '0.5', -0.1, float('nan'), float('inf'), -float('inf'), 10**400):
            with self.subTest(value=value), self.assertRaisesRegex(ExportValidationError, 'pageRank'):
                build_export_rows([METADATA], [dict(PORTS[0], page_rank=value)], EXPORTED_AT)

    def test_invalid_community_id_fails(self):
        for value in (None, True, '2', 2.0, -1, UINT64_MAX + 1):
            with self.subTest(value=value), self.assertRaisesRegex(ExportValidationError, 'communityId'):
                build_export_rows([METADATA], [dict(PORTS[0], community_id=value)], EXPORTED_AT)

    def test_empty_missing_and_duplicate_port_ids_fail(self):
        for ports in ([], [dict(PORTS[0], port_id=None)], [dict(PORTS[0], port_id=' ')],
                      [dict(PORTS[0], port_id='')], [PORTS[0], dict(PORTS[0])]):
            with self.subTest(ports=ports), self.assertRaises(ExportValidationError):
                build_export_rows([METADATA], ports, EXPORTED_AT)

    def test_missing_or_stale_visit_run_id_fails(self):
        missing = {key: value for key, value in PORTS[0].items() if key != 'visit_run_id'}
        for port in (missing, dict(PORTS[0], visit_run_id=None),
                     dict(PORTS[0], visit_run_id=''),
                     dict(PORTS[0], visit_run_id='2026-09-13T08:00:00Z')):
            with self.subTest(port=port), self.assertRaisesRegex(ExportValidationError, 'visitRunId'):
                build_export_rows([METADATA], [PORTS[1], port], EXPORTED_AT)


class ExportOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.driver = MagicMock()
        self.driver.__enter__.return_value = self.driver
        self.session = self.driver.session.return_value.__enter__.return_value
        self.tx = Mock()
        self.tx.run.return_value.single.return_value = dict(snapshots=[METADATA], ports=PORTS)
        self.session.execute_read.side_effect = lambda callback, *args: callback(self.tx, *args)
        self.ch = Mock()
        self.ch.query.return_value = '\n'.join(json.dumps({'port_id': port_id}) for port_id in ('A', 'B'))
        patches = [patch('pipelines.graph_metrics.export.GraphDatabase.driver', return_value=self.driver),
                   patch('pipelines.graph_metrics.export.ClickHouse', return_value=self.ch),
                   patch('pipelines.graph_metrics.export.load_dotenv'),
                   patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused-test-password'})]
        self.mocks = [self.enter_patch(patcher) for patcher in patches]

    def enter_patch(self, patcher):
        mocked = patcher.start()
        self.addCleanup(patcher.stop)
        return mocked

    def test_exports_only_supplied_active_endpoints_after_canonical_validation(self):
        # An unrelated canonical port is present; it must not join the export.
        self.ch.query.return_value += '\n' + json.dumps({'port_id': 'INACTIVE'})
        with self.assertLogs('pipelines.graph_metrics.export', level='INFO') as logs:
            summary = export_metrics()
        self.tx.run.assert_called_once()
        query = self.tx.run.call_args.args[0]
        self.assertIn('[r:CONNECTED_TO {managedBy: $owner}]', query)
        self.assertIn('collect(DISTINCT source) + collect(DISTINCT target)', query)
        self.assertIn('collect(DISTINCT port)', query)
        self.assertIn('collect(DISTINCT {run_id: r.runId', query)
        self.assertIn('visit_run_id: port.visitRunId', query)
        self.assertEqual(self.tx.run.call_args.kwargs, {'owner': OWNER})
        self.ch.query.assert_called_once_with('SELECT port_id FROM analytics.ports FINAL FORMAT JSONEachRow')
        table, rows = self.ch.insert.call_args.args
        self.assertEqual(table, 'port_graph_metrics')
        self.assertEqual([row['port_id'] for row in rows], ['A', 'B'])
        self.assertEqual({row['run_id'] for row in rows}, {METADATA['run_id']})
        self.assertEqual(len({row['exported_at'] for row in rows}), 1)
        self.assertEqual(summary, dict(METADATA, snapshot_date='2026-09-15', ports=2,
                                       communities=2, min_page_rank=0.25, max_page_rank=0.75))
        self.assertIn('run_id=' + METADATA['run_id'], logs.output[0])
        self.assertNotIn('unused-test-password', logs.output[0])
        self.ch.session.close.assert_called_once()
        self.driver.__exit__.assert_called_once()

    def test_legacy_hash_run_id_is_exported_unchanged(self):
        legacy_run_id = '0123456789abcdef' * 4
        self.tx.run.return_value.single.return_value = dict(
            snapshots=[dict(METADATA, run_id=legacy_run_id)],
            ports=[dict(port, visit_run_id=legacy_run_id) for port in PORTS])
        summary = export_metrics()
        rows = self.ch.insert.call_args.args[1]
        self.assertEqual({row['run_id'] for row in rows}, {legacy_run_id})
        self.assertEqual(summary['run_id'], legacy_run_id)

    def test_unknown_port_blocks_all_inserts_and_closes_clickhouse(self):
        self.ch.query.return_value = json.dumps({'port_id': 'A'})
        with self.assertRaisesRegex(ExportValidationError, 'missing from analytics.ports: B'):
            export_metrics()
        self.ch.insert.assert_not_called()
        self.ch.session.close.assert_called_once()

    def test_no_relationships_fails_without_opening_clickhouse(self):
        self.tx.run.return_value.single.return_value = None
        with self.assertRaisesRegex(ExportValidationError, 'No managed CONNECTED_TO snapshot'):
            export_metrics()
        self.mocks[1].assert_not_called()
        self.driver.__exit__.assert_called_once()

    def test_incomplete_metrics_fail_without_opening_clickhouse(self):
        self.tx.run.return_value.single.return_value = dict(
            snapshots=[METADATA], ports=[PORTS[0], dict(PORTS[1], community_id=None)])
        with self.assertRaisesRegex(ExportValidationError, 'Port A: communityId'):
            export_metrics()
        self.mocks[1].assert_not_called()
        self.ch.insert.assert_not_called()

    def test_missing_or_stale_visit_lineage_blocks_clickhouse_before_it_is_opened(self):
        for tag in (None, '2026-09-13T08:00:00Z'):
            with self.subTest(tag=tag):
                self.tx.run.return_value.single.return_value = dict(
                    snapshots=[METADATA], ports=[PORTS[0], dict(PORTS[1], visit_run_id=tag)])
                with self.assertRaisesRegex(ExportValidationError, 'Port A: visitRunId'):
                    export_metrics()
                self.mocks[1].assert_not_called()
                self.ch.insert.assert_not_called()

    def test_pinned_snapshot_is_checked_around_metrics_read_in_the_same_transaction(self):
        expected = dict(METADATA, relationships=1, movements=3, nodes=2, graph_state='same-graph')
        gds = ModuleType('pipelines.graph_metrics.gds')
        events = []
        gds._read_snapshot = Mock(side_effect=lambda tx: events.append('snapshot') or expected)
        gds._assert_snapshot = Mock()
        result = self.tx.run.return_value
        self.tx.run.side_effect = lambda *args, **kwargs: events.append('metrics') or result
        with patch.dict(sys.modules, {'pipelines.graph_metrics.gds': gds}):
            summary = export_metrics(expected)
        self.assertEqual(events, ['snapshot', 'metrics', 'snapshot'])
        self.assertEqual(gds._read_snapshot.call_args_list, [call(self.tx), call(self.tx)])
        self.assertEqual(gds._assert_snapshot.call_args_list,
                         [call(expected, expected), call(expected, expected)])
        self.session.execute_read.assert_called_once()
        self.assertIs(self.session.execute_read.call_args.args[1], expected)
        self.ch.insert.assert_called_once()
        self.assertEqual(summary['run_id'], expected['run_id'])

    def test_snapshot_change_before_or_during_metric_read_blocks_clickhouse(self):
        expected = dict(METADATA, relationships=1, movements=3, nodes=2, graph_state='original')
        changed = dict(expected, graph_state='different-graph-with-same-run-id')
        for successful_checks in (0, 1):
            with self.subTest(successful_checks=successful_checks):
                self.tx.run.reset_mock()
                gds = ModuleType('pipelines.graph_metrics.gds')
                gds._read_snapshot = Mock(side_effect=[expected] * successful_checks + [changed])
                gds._assert_snapshot = Mock(side_effect=[None] * successful_checks + [
                    ExportValidationError('Managed CONNECTED_TO snapshot changed')])
                with patch.dict(sys.modules, {'pipelines.graph_metrics.gds': gds}), \
                        self.assertRaisesRegex(ExportValidationError, 'snapshot changed'):
                    export_metrics(expected)
                self.assertEqual(gds._assert_snapshot.call_args, call(expected, changed))
                self.assertEqual(self.tx.run.call_count, successful_checks)
                self.mocks[1].assert_not_called()
                self.ch.insert.assert_not_called()

    def test_insert_error_still_closes_clickhouse(self):
        self.ch.insert.side_effect = RuntimeError('test insert failure')
        with self.assertRaisesRegex(RuntimeError, 'test insert failure'):
            export_metrics()
        self.ch.session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
