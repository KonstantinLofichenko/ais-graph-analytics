from datetime import datetime, timezone
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, Mock, patch
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
        from pipelines.graph_metrics import gds, export
        self.gds, self.export = gds, export
        self.record = dict(publications=[dict(METADATA, managed_by=OWNER, edge_count=1,
            generation=1, published_at='2026-09-16T00:00:00Z')], snapshots=[dict(METADATA)],
            connections=[dict(relationship_id='r1', source_id='a', target_id='b',
                source_port_id='A', target_port_id='B', source_is_port=True, target_is_port=True,
                source_visit_run_id=METADATA['run_id'], target_visit_run_id=METADATA['run_id'],
                movement_count=3)])
        self.ports = deepcopy(PORTS)
        self.driver = MagicMock()
        self.driver.__enter__.return_value = self.driver
        self.session = self.driver.session.return_value.__enter__.return_value
        self.tx = Mock()
        self.tx.run.side_effect = self.query
        self.session.execute_read.side_effect = lambda callback, *args: callback(self.tx, *args)
        self.ch = Mock()
        self.ch.query.return_value = '\n'.join(json.dumps({'port_id': p}) for p in ('A', 'B'))
        patches = [patch('pipelines.graph_metrics.export.GraphDatabase.driver', return_value=self.driver),
                   patch('pipelines.graph_metrics.export.ClickHouse', return_value=self.ch),
                   patch('pipelines.graph_metrics.export.load_dotenv'),
                   patch.dict(os.environ, {'NEO4J_PASSWORD': 'unused-test-password'})]
        self.mocks = []
        for patcher in patches:
            self.mocks.append(patcher.start())
            self.addCleanup(patcher.stop)

    def query(self, query, **params):
        if query == self.gds.SNAPSHOT_QUERY:
            record = self.record
        elif query == self.export.SNAPSHOT_QUERY:
            record = dict(ports=self.ports)
        else:
            self.fail('Unexpected query: ' + query)
        self.assertEqual(params, {'owner': OWNER})
        result = Mock()
        result.single.return_value = deepcopy(record)
        return result

    def empty(self):
        self.record['publications'][0]['edge_count'] = 0
        self.record['snapshots'] = []
        self.record['connections'] = []
        self.ports = []

    def test_nonempty_export_and_source_rechecks(self):
        self.ch.query.return_value += '\n' + json.dumps({'port_id': 'INACTIVE'})
        with self.assertLogs('pipelines.graph_metrics.export', level='INFO') as logs:
            summary = export_metrics()
        self.assertEqual([c.args[0] for c in self.tx.run.call_args_list], [
            self.gds.SNAPSHOT_QUERY, self.export.SNAPSHOT_QUERY,
            self.gds.SNAPSHOT_QUERY, self.gds.SNAPSHOT_QUERY])
        self.assertIn('collect(DISTINCT port)', self.export.SNAPSHOT_QUERY)
        self.ch.query.assert_called_once_with('SELECT port_id FROM analytics.ports FINAL FORMAT JSONEachRow')
        table, rows = self.ch.insert.call_args.args
        self.assertEqual(table, 'port_graph_metrics')
        self.assertEqual([r['port_id'] for r in rows], ['A', 'B'])
        self.assertEqual({r['run_id'] for r in rows}, {METADATA['run_id']})
        self.assertEqual(summary, dict(METADATA, snapshot_date='2026-09-15', ports=2,
                                      communities=2, min_page_rank=0.25, max_page_rank=0.75))
        self.assertIn('run_id=' + METADATA['run_id'], logs.output[0])
        self.ch.session.close.assert_called_once()

    def test_verified_empty_returns_summary_without_clickhouse(self):
        self.empty()
        snapshots, ports = self.export.read_graph_snapshot(self.tx)
        self.assertEqual(build_export_rows(snapshots, ports, EXPORTED_AT,
                                          verified_snapshot=snapshots[0]), [])
        summary = export_metrics(snapshots[0])
        self.assertEqual(summary, dict(METADATA, snapshot_date='2026-09-15', ports=0,
                                      communities=0, min_page_rank=None, max_page_rank=None))
        self.mocks[1].assert_not_called()
        self.ch.insert.assert_not_called()

    def test_empty_requires_verified_state_not_just_metadata(self):
        for verified in (None, dict(METADATA, nodes=0, relationships=0)):
            with self.assertRaises(ExportValidationError):
                build_export_rows([METADATA], [], EXPORTED_AT, verified_snapshot=verified)

    def test_missing_publication_and_unpublished_empty_fail(self):
        original = deepcopy(self.record)
        for empty in (False, True):
            self.record = deepcopy(original)
            if empty:
                self.empty()
            self.record['publications'] = []
            with self.assertRaisesRegex(ExportValidationError, 'ConnectionSnapshot'):
                export_metrics()
        self.mocks[1].assert_not_called()

    def test_malformed_publication_and_count_mismatch_fail(self):
        original = deepcopy(self.record)
        for field, value in [('managed_by', 'other'), ('run_id', ''), ('window_start', None),
                             ('window_end', METADATA['window_start']), ('edge_count', 0),
                             ('edge_count', 2), ('edge_count', True), ('edge_count', -1),
                             ('edge_count', 1.0), ('generation', True), ('generation', 0),
                             ('generation', None), ('published_at', None), ('published_at', 'bad')]:
            with self.subTest(field=field, value=value):
                self.record = deepcopy(original)
                self.record['publications'][0][field] = value
                with self.assertRaises(ExportValidationError):
                    export_metrics()
        self.mocks[1].assert_not_called()

    def test_bad_relationships_and_lineage_fail(self):
        original = deepcopy(self.record)
        for field, value in [('source_is_port', False), ('target_is_port', False),
                             ('movement_count', 0), ('movement_count', True),
                             ('source_visit_run_id', 'stale'), ('target_visit_run_id', None)]:
            self.record = deepcopy(original)
            self.record['connections'][0][field] = value
            with self.subTest(field=field), self.assertRaises(ExportValidationError):
                export_metrics()
        self.record = deepcopy(original)
        self.record['snapshots'][0]['run_id'] = 'mismatch'
        with self.assertRaises(ExportValidationError):
            export_metrics()
        self.mocks[1].assert_not_called()

    def test_nonempty_missing_or_invalid_metrics_fail(self):
        for ports in ([], PORTS[:1], [PORTS[0], dict(PORTS[1], community_id=None)],
                      [PORTS[0], dict(PORTS[1], visit_run_id='stale')]):
            self.ports = ports
            with self.subTest(ports=ports), self.assertRaises(ExportValidationError):
                export_metrics()
        self.mocks[1].assert_not_called()

    def test_generation_change_before_or_during_read_for_empty_and_nonempty(self):
        original = deepcopy(self.record)
        for empty in (False, True):
            for change_at in (1, 2, 3):
                self.record = deepcopy(original)
                self.ports = deepcopy(PORTS)
                if empty:
                    self.empty()
                expected = self.gds._read_snapshot(self.tx)
                reads = 0
                def query(sql, **params):
                    nonlocal reads
                    if sql == self.gds.SNAPSHOT_QUERY:
                        reads += 1
                        if reads == change_at:
                            self.record['publications'][0]['generation'] += 1
                    return self.query(sql, **params)
                self.tx.run.side_effect = query
                with self.subTest(empty=empty, change_at=change_at), self.assertRaisesRegex(
                        ExportValidationError, 'graph changed'):
                    export_metrics(expected)
                self.ch.insert.assert_not_called()
                self.tx.run.side_effect = self.query

    def test_unpinned_export_detects_publication_change_before_insert(self):
        for field, value in [('generation', 2), ('published_at', '2026-09-17T00:00:00Z')]:
            original = deepcopy(self.record)
            def canonical_query(sql):
                self.record['publications'][0][field] = value
                return '\n'.join(json.dumps({'port_id': p}) for p in ('A', 'B'))
            self.ch.query.side_effect = canonical_query
            with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                export_metrics()
            self.ch.insert.assert_not_called()
            self.record = original

    def test_unknown_port_and_insert_error_close_clickhouse(self):
        self.ch.query.return_value = json.dumps({'port_id': 'A'})
        with self.assertRaisesRegex(ExportValidationError, 'missing from analytics.ports: B'):
            export_metrics()
        self.ch.insert.assert_not_called()
        self.ch.session.close.assert_called_once()
        self.ch.session.close.reset_mock()
        self.ch.query.return_value += '\n' + json.dumps({'port_id': 'B'})
        self.ch.insert.side_effect = RuntimeError('insert failure')
        with self.assertRaisesRegex(RuntimeError, 'insert failure'):
            export_metrics()
        self.ch.session.close.assert_called_once()

    def test_legacy_run_id_preserved(self):
        legacy = '0123456789abcdef' * 4
        self.record['publications'][0]['run_id'] = legacy
        self.record['snapshots'][0]['run_id'] = legacy
        for row in self.record['connections']:
            row.update(source_visit_run_id=legacy, target_visit_run_id=legacy)
        self.ports = [dict(port, visit_run_id=legacy) for port in PORTS]
        self.assertEqual(export_metrics()['run_id'], legacy)
        self.assertEqual({r['run_id'] for r in self.ch.insert.call_args.args[1]}, {legacy})


if __name__ == '__main__':
    unittest.main()
