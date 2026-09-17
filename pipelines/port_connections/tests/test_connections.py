from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.port_connections.aggregate import aggregate_connections
from pipelines.port_connections.run import (
    OWNER, graph_snapshot, publish_port_connections, read_completed_visits, validate_metadata,
)

START = datetime(2026, 9, 14, 8, tzinfo=timezone.utc)
METADATA = dict(run_id='2026-09-14T08:00:00Z', window_start=START.isoformat(),
                window_end=(START + timedelta(days=1)).isoformat())


def visit(port, hour, mmsi=1, visit_id=None):
    return dict(port_id=port, mmsi=mmsi, arrival_at=(START + timedelta(hours=hour)).isoformat(),
                visit_id=visit_id or f'{mmsi}-{hour}-{port}')


def json_rows(rows):
    return '\n'.join(json.dumps(row) for row in rows)


def completed_run(count, **overrides):
    return dict(METADATA, visit_count=count, **overrides)


class AggregationTests(unittest.TestCase):
    def test_a_to_b(self):
        rows, stats = aggregate_connections([visit('A', 0), visit('B', 1)])
        self.assertEqual(rows, [dict(from_port_id='A', to_port_id='B', movement_count=1,
                                    vessel_count=1, first_seen=visit('A', 0)['arrival_at'],
                                    last_seen=visit('B', 1)['arrival_at'])])
        self.assertEqual(stats, dict(visits=2, movements=1, relationships=1, vessels=1))

    def test_a_to_b_to_c_has_only_adjacent_routes(self):
        rows, _ = aggregate_connections([visit('C', 2), visit('A', 0), visit('B', 1)])
        self.assertEqual([(r['from_port_id'], r['to_port_id']) for r in rows],
                         [('A', 'B'), ('B', 'C')])

    def test_same_port_has_no_connection(self):
        self.assertEqual(aggregate_connections([visit('A', 0), visit('A', 1)]),
                         ([], dict(visits=2, movements=0, relationships=0, vessels=0)))

    def test_same_port_visit_remains_the_source_of_the_next_movement(self):
        rows, _ = aggregate_connections([visit('A', 0), visit('A', 1), visit('B', 2)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['first_seen'], visit('A', 1)['arrival_at'])

    def test_a_to_b_to_a_preserves_direction(self):
        rows, _ = aggregate_connections([visit('A', 0), visit('B', 1), visit('A', 2)])
        self.assertEqual([(r['from_port_id'], r['to_port_id']) for r in rows],
                         [('A', 'B'), ('B', 'A')])

    def test_partition_by_mmsi(self):
        visits = [visit('A', 0, 1), visit('C', 1, 2), visit('B', 2, 1), visit('D', 3, 2)]
        rows, stats = aggregate_connections(visits)
        self.assertEqual([(r['from_port_id'], r['to_port_id']) for r in rows],
                         [('A', 'B'), ('C', 'D')])
        self.assertEqual(stats['vessels'], 2)

    def test_repeated_route_counts_movements_and_distinct_vessels(self):
        visits = [visit('A', 0, 1), visit('B', 1, 1), visit('A', 2, 1), visit('B', 3, 1),
                  visit('A', 4, 2), visit('B', 5, 2), visit('C', 6, 3)]
        rows, stats = aggregate_connections(visits)
        self.assertEqual(rows[0], dict(from_port_id='A', to_port_id='B', movement_count=3,
                                      vessel_count=2, first_seen=visits[0]['arrival_at'],
                                      last_seen=visits[5]['arrival_at']))
        self.assertEqual(stats, dict(visits=7, movements=4, relationships=2, vessels=2))

    def test_arrival_ties_use_visit_id_and_output_is_deterministic(self):
        visits = [visit('B', 0, visit_id='002'), visit('A', 0, visit_id='001'), visit('C', 1)]
        expected = aggregate_connections(visits)
        self.assertEqual(aggregate_connections(list(reversed(visits))), expected)
        self.assertEqual([(r['from_port_id'], r['to_port_id']) for r in expected[0]],
                         [('A', 'B'), ('B', 'C')])

    def test_arrival_order_uses_instants_and_seen_values_use_arrival_only(self):
        source = dict(visit('A', 0), arrival_at='2026-09-14T10:00:00+02:00',
                      departure_at='2026-09-14T08:30:00+00:00')
        destination = dict(visit('B', 1), departure_at='2026-09-14T12:00:00+00:00',
                           last_observed_at='2026-09-14T11:30:00+00:00')
        rows, _ = aggregate_connections([destination, source])
        self.assertEqual(rows[0]['first_seen'], START.isoformat())
        self.assertEqual(rows[0]['last_seen'], destination['arrival_at'])

    def test_empty_and_single_visit(self):
        for visits in ([], [visit('A', 0)]):
            with self.subTest(visits=visits):
                self.assertEqual(aggregate_connections(visits),
                                 ([], dict(visits=len(visits), movements=0,
                                           relationships=0, vessels=0)))


class CompletedRunTests(unittest.TestCase):
    def test_queries_use_exact_run_final_and_deterministic_order(self):
        visits = [visit('A', 0), visit('B', 1)]
        ch = Mock()
        ch.query.side_effect = [json_rows([completed_run(2)]), json_rows(visits)]
        self.assertEqual(read_completed_visits(ch, METADATA), visits)
        run_query, visit_query = ch.query.call_args_list
        self.assertIn('FROM analytics.port_visit_runs AS r FINAL', run_query.args[0])
        self.assertIn('run_id = {run_id:String}', run_query.args[0])
        self.assertIn("window_start = {window_start:DateTime64(6, 'UTC')}", run_query.args[0])
        self.assertIn("window_end = {window_end:DateTime64(6, 'UTC')}", run_query.args[0])
        self.assertEqual(run_query.args[1], dict(param_run_id=METADATA['run_id'],
                                                param_window_start='2026-09-14 08:00:00.000000',
                                                param_window_end='2026-09-15 08:00:00.000000'))
        self.assertIn('FROM analytics.port_visits FINAL', visit_query.args[0])
        self.assertIn('WHERE run_id = {run_id:String}', visit_query.args[0])
        self.assertIn('ORDER BY mmsi, arrival_at, visit_id', visit_query.args[0])
        self.assertEqual(visit_query.args[1], {'param_run_id': METADATA['run_id']})

    def test_incomplete_run_is_rejected_before_visits_are_read(self):
        ch = Mock()
        ch.query.return_value = ''
        with self.assertRaisesRegex(RuntimeError, 'No completed port-visit run'):
            read_completed_visits(ch, METADATA)
        ch.query.assert_called_once()

    def test_completed_run_identity_and_bounds_must_match(self):
        for field, value in [('run_id', 'other-run'),
                             ('window_start', '2026-09-14T07:00:00+00:00'),
                             ('window_end', '2026-09-15T07:00:00+00:00')]:
            with self.subTest(field=field):
                ch = Mock()
                ch.query.return_value = json_rows([dict(completed_run(0), **{field: value})])
                with self.assertRaisesRegex(RuntimeError, 'No completed port-visit run'):
                    read_completed_visits(ch, METADATA)
                ch.query.assert_called_once()

    def test_inconsistent_retried_snapshot_fails_instead_of_publishing_stale_visits(self):
        ch = Mock()
        ch.query.side_effect = [json_rows([completed_run(1)]),
                                json_rows([visit('A', 0), visit('B', 1)])]
        with self.assertRaisesRegex(RuntimeError, 'completed visit_count=1, FINAL visits=2'):
            read_completed_visits(ch, METADATA)

    def test_completed_empty_run_is_valid(self):
        ch = Mock()
        ch.query.side_effect = [json_rows([completed_run(0)]), '']
        self.assertEqual(read_completed_visits(ch, METADATA), [])


class FakeResult:
    def __init__(self, records=()):
        self.records = list(records)

    def __iter__(self):
        return iter(self.records)

    def consume(self):
        return None

    def single(self):
        return self.records[0]


def edge(source, destination, kind='CONNECTED_TO', owner=OWNER, source_label='Port',
         destination_label='Port', **properties):
    return dict(source=source, destination=destination, kind=kind, source_label=source_label,
                destination_label=destination_label, properties=dict(managedBy=owner, **properties))


class FakeGraph:
    """Small transactional graph model; query checks keep its supported semantics explicit."""
    def __init__(self, ports, edges=(), fail_on_write=None):
        self.ports = set(ports)
        self.edges = deepcopy(list(edges))
        self.calls = []
        self.fail_on_write = fail_on_write

    def execute_write(self, callback, *args):
        tx = FakeTransaction(self)
        callback(tx, *args)
        self.edges = tx.edges  # Commit only after the entire callback succeeds.


class FakeTransaction:
    def __init__(self, graph):
        self.graph = graph
        self.edges = deepcopy(graph.edges)
        self.writes = 0

    def run(self, query, **parameters):
        query = ' '.join(query.split())
        self.graph.calls.append((query, deepcopy(parameters)))
        if 'OPTIONAL MATCH' in query:
            assert 'OPTIONAL MATCH (p:Port {portId: port_id})' in query
            assert 'WHERE p IS NULL' in query
            return FakeResult([{'port_id': port_id} for port_id in parameters['port_ids']
                               if port_id not in self.graph.ports])
        if 'DELETE r' in query:
            assert 'MATCH (:Port)-[r:CONNECTED_TO]->(:Port)' in query
            assert 'WHERE r.managedBy = $owner DELETE r' in query
            self.edges = [e for e in self.edges
                          if not (e['kind'] == 'CONNECTED_TO'
                                  and e['source_label'] == e['destination_label'] == 'Port'
                                  and e['properties'].get('managedBy') == parameters['owner'])]
            return FakeResult()
        assert 'MATCH (a:Port {portId: row.from_port_id})' in query
        assert 'MATCH (b:Port {portId: row.to_port_id})' in query
        assert 'MERGE (a)-[r:CONNECTED_TO {managedBy: $owner}]->(b)' in query
        for assignment in ('r.movementCount = row.movement_count', 'r.vesselCount = row.vessel_count',
                           'r.firstSeen = datetime(row.first_seen)', 'r.lastSeen = datetime(row.last_seen)',
                           'r.runId = $run_id', 'r.windowStart = datetime($window_start)',
                           'r.windowEnd = datetime($window_end)'):
            assert assignment in query
        self.writes += 1
        if self.writes == self.graph.fail_on_write:
            return FakeResult([{'published': 0}])
        for row in parameters['rows']:
            properties = dict(movementCount=row['movement_count'], vesselCount=row['vessel_count'],
                              firstSeen=row['first_seen'], lastSeen=row['last_seen'],
                              runId=parameters['run_id'], windowStart=parameters['window_start'],
                              windowEnd=parameters['window_end'])
            existing = [e for e in self.edges
                        if e['source'] == row['from_port_id'] and e['destination'] == row['to_port_id']
                        and e['kind'] == 'CONNECTED_TO'
                        and e['properties'].get('managedBy') == parameters['owner']]
            if existing:
                existing[0]['properties'].update(properties)
            else:
                self.edges.append(edge(row['from_port_id'], row['to_port_id'],
                                       owner=parameters['owner'], **properties))
        return FakeResult([{'published': len(parameters['rows'])}])


class GraphSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.rows, _ = aggregate_connections([visit('A', 0), visit('B', 1)])
        self.unrelated = [edge('A', 'B', owner=None, note='manual route'),
                          edge('A', 'B', owner='some-other-publisher', note='keep'),
                          edge('vessel', 'A', kind='VISITED', source_label='Vessel'),
                          edge('A', 'vessel', destination_label='Vessel'),
                          edge('A', 'B', kind='OTHER')]

    def test_same_run_twice_is_logically_idempotent_and_preserves_manual_edges(self):
        graph = FakeGraph(['A', 'B', 'C'], [edge('A', 'C', runId='previous-run'), *self.unrelated])
        graph.execute_write(graph_snapshot, self.rows, METADATA)
        first = deepcopy(graph.edges)
        graph.execute_write(graph_snapshot, self.rows, METADATA)
        self.assertEqual(graph.edges, first)
        self.assertEqual(graph.edges[:-1], self.unrelated)
        self.assertEqual(graph.edges[-1], edge(
            'A', 'B', movementCount=1, vesselCount=1, firstSeen=self.rows[0]['first_seen'],
            lastSeen=self.rows[0]['last_seen'], runId=METADATA['run_id'],
            windowStart=METADATA['window_start'], windowEnd=METADATA['window_end']))

    def test_missing_port_is_reported_before_deletion(self):
        graph = FakeGraph(['A'], [edge('A', 'C', runId='previous-run'), *self.unrelated])
        before = deepcopy(graph.edges)
        with self.assertRaisesRegex(RuntimeError, r'Missing Neo4j Port nodes \(portId\): B'):
            graph.execute_write(graph_snapshot, self.rows, METADATA)
        self.assertEqual(graph.edges, before)
        self.assertEqual(len(graph.calls), 1)
        self.assertNotIn('DELETE', graph.calls[0][0])

    def test_empty_snapshot_clears_only_owned_connections(self):
        graph = FakeGraph(['A', 'B'], [edge('A', 'B'), *self.unrelated])
        graph.execute_write(graph_snapshot, [], METADATA)
        self.assertEqual(graph.edges, self.unrelated)
        self.assertEqual(len(graph.calls), 1)

    def test_later_batch_endpoint_failure_rolls_back_deletion_and_prior_writes(self):
        rows = [dict(self.rows[0], to_port_id=f'P{i:04}') for i in range(1001)]
        graph = FakeGraph(['A', 'B', *(row['to_port_id'] for row in rows)],
                          [edge('B', 'A', runId='previous-run'), *self.unrelated], fail_on_write=2)
        before = deepcopy(graph.edges)
        with self.assertRaisesRegex(RuntimeError, 'endpoint match failed: expected 1 relationships, published 0'):
            graph.execute_write(graph_snapshot, rows, METADATA)
        self.assertEqual(graph.edges, before)
        self.assertEqual(len([q for q, _ in graph.calls if 'MERGE' in q]), 2)


class EntrypointTests(unittest.TestCase):
    def test_canonical_and_legacy_run_ids_pass_through_query_graph_and_summary(self):
        for run_id in (METADATA['run_id'], '0123456789abcdef' * 4):
            with self.subTest(run_id=run_id):
                metadata = dict(METADATA, run_id=run_id)
                ch = Mock()
                ch.query.side_effect = [json_rows([dict(metadata, visit_count=2)]),
                                        json_rows([visit('A', 0), visit('B', 1)])]
                graph = FakeGraph(['A', 'B'])
                with patch('pipelines.port_connections.run.ClickHouse', return_value=ch), \
                        patch('pipelines.port_connections.run.load_dotenv'), \
                        patch('pipelines.port_connections.run.publish_connections',
                              side_effect=lambda rows, passed_metadata: graph.execute_write(
                                  graph_snapshot, rows, passed_metadata)):
                    summary = publish_port_connections(metadata)
                self.assertTrue(all(call.args[1]['param_run_id'] == run_id
                                    for call in ch.query.call_args_list))
                self.assertEqual(graph.edges[0]['properties']['runId'], run_id)
                self.assertEqual(summary['run_id'], run_id)

    def test_reuses_upstream_metadata_and_returns_only_small_summary(self):
        visits = [visit('A', 0), visit('B', 1)]
        ch = Mock()
        ch.query.side_effect = [json_rows([completed_run(2)]), json_rows(visits)]
        original = dict(METADATA)
        with patch('pipelines.port_connections.run.ClickHouse', return_value=ch), \
                patch('pipelines.port_connections.run.load_dotenv'), \
                patch('pipelines.port_connections.run.publish_connections') as publish, \
                self.assertLogs('pipelines.port_connections.run', level='INFO') as logs:
            summary = publish_port_connections(METADATA)
        rows, actual_metadata = publish.call_args.args
        self.assertEqual(actual_metadata, original)
        self.assertEqual(METADATA, original)
        self.assertEqual(rows, aggregate_connections(visits)[0])
        self.assertEqual(summary, dict(original, visits=2, movements=1, relationships=1, vessels=1))
        self.assertIn('run_id=' + original['run_id'], logs.output[0])
        self.assertIn('visits=2 movements=1 relationships=1 vessels=1', logs.output[0])
        ch.session.close.assert_called_once()

    def test_source_failure_closes_client_and_does_not_publish(self):
        ch = Mock()
        ch.query.return_value = ''
        with patch('pipelines.port_connections.run.ClickHouse', return_value=ch), \
                patch('pipelines.port_connections.run.load_dotenv'), \
                patch('pipelines.port_connections.run.publish_connections') as publish:
            with self.assertRaisesRegex(RuntimeError, 'No completed port-visit run'):
                publish_port_connections(METADATA)
        publish.assert_not_called()
        ch.session.close.assert_called_once()

    def test_metadata_rejects_missing_extra_and_nonstring_values(self):
        for metadata in (None, {}, dict(METADATA, visits=[]), dict(METADATA, run_id=42),
                         dict(METADATA, run_id=''), dict(METADATA, window_end=METADATA['window_start'])):
            with self.subTest(metadata=metadata), self.assertRaises(ValueError):
                validate_metadata(metadata)

    def test_metadata_rejects_naive_window_timestamps(self):
        for field in ('window_start', 'window_end'):
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'include a timezone'):
                validate_metadata(dict(METADATA, **{field: '2026-09-14T08:00:00'}))

    def test_metadata_preserves_equivalent_explicit_timezone_representation(self):
        metadata = dict(METADATA, window_start='2026-09-14T10:00:00+02:00',
                        window_end='2026-09-15T08:00:00Z')
        self.assertEqual(validate_metadata(metadata), metadata)


if __name__ == '__main__':
    unittest.main()
