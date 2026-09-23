"""Offline GDS orchestration tests using the real validation callbacks."""
from contextlib import contextmanager
from copy import deepcopy
import ast
import json
from pathlib import Path
import re
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from pipelines.graph_metrics import gds
from pipelines.graph_metrics.export import ExportValidationError


METADATA = dict(run_id='2026-09-15T08:00:00Z',
                window_start='2026-09-15T08:00:00.123456+00:00',
                window_end='2026-09-16T08:00:00.654321+00:00')
PAGERANK = dict(relationshipWeightProperty='movementCount', maxIterations=20,
                dampingFactor=0.85, concurrency=1)
LOUVAIN = dict(relationshipWeightProperty='movementCount', concurrency=1)


def connection(relationship_id, source, target, weight):
    return dict(relationship_id=relationship_id, source_id='node-' + source,
                target_id='node-' + target, source_port_id=source, target_port_id=target,
                source_is_port=True, target_is_port=True, movement_count=weight,
                source_visit_run_id=METADATA['run_id'], target_visit_run_id=METADATA['run_id'])


class GdsWorkflowTests(unittest.TestCase):
    def setUp(self):
        self.record = dict(publications=[dict(METADATA, managed_by=gds.OWNER,
                            edge_count=3, generation=1, published_at='2026-09-17T10:00:00Z')],
                           snapshots=[dict(METADATA)], connections=[
            connection('rel-1', 'A', 'B', 3), connection('rel-2', 'B', 'C', 7),
            connection('rel-3', 'C', 'A', 2),
        ])
        self.catalog = {
            gds.DIRECTED: dict(graphName=gds.DIRECTED, nodeCount=3, relationshipCount=3,
                               creationTime='2026-09-17T10:00:00+00:00'),
            gds.UNDIRECTED: dict(graphName=gds.UNDIRECTED, nodeCount=3, relationshipCount=6,
                                 creationTime='2026-09-17T10:00:01+00:00'),
        }
        self.ports = [
            dict(port_id='A', page_rank=0.2, community_id=10, visit_run_id=METADATA['run_id']),
            dict(port_id='B', page_rank=1.2, community_id=10, visit_run_id=METADATA['run_id']),
            dict(port_id='C', page_rank=0.6, community_id=20, visit_run_id=METADATA['run_id']),
        ]
        self.overrides = {}
        self.events = []
        self.session = Mock()
        self.session.run.side_effect = self.query
        self.session.execute_read.side_effect = lambda callback, *args: callback(self.session, *args)
        self.session.execute_write.side_effect = lambda callback, *args: callback(self.session, *args)

        @contextmanager
        def session():
            yield self.session

        session_patch = patch.object(gds, '_session', side_effect=session)
        session_patch.start()
        self.addCleanup(session_patch.stop)
        self.snapshot = gds.validate_graph_snapshot()
        self.state = dict(self.snapshot, projections={
            graph: dict(nodes=3, relationships=row['relationshipCount'], created_at=row['creationTime'])
            for graph, row in self.catalog.items()
        }, directed_relationships=3, communities=2, modularity=0.1)
        self.events.clear()
        self.session.reset_mock()

    def query(self, query, **params):
        self.events.append((query, deepcopy(params)))
        if query in self.overrides:
            override = self.overrides[query]
            if isinstance(override, Exception):
                raise override
            record = override(params) if callable(override) else override
        elif query == gds.SNAPSHOT_QUERY:
            record = self.record
        elif query in (gds.LIST_QUERY, gds.PROJECT_QUERY):
            record = self.catalog[params['graph']]
        elif query == gds.EXISTS_QUERY:
            record = dict(exists=False)
        elif query == gds.STALE_METRICS_QUERY:
            record = dict(stale_ports=len(self.ports))
        elif query == gds.PAGERANK_QUERY:
            record = dict(nodes=3, min_page_rank=0.2, max_page_rank=1.2)
        elif query == gds.LOUVAIN_QUERY:
            record = dict(nodes=3, communities=2)
        elif query == gds.LOUVAIN_STATS_QUERY:
            record = dict(communityCount=2, modularity=0.42, ranLevels=3)
        elif query == gds.PAGERANK_WRITE_QUERY:
            record = dict(nodePropertiesWritten=3, ranIterations=12, didConverge=True)
        elif query == gds.LOUVAIN_WRITE_QUERY:
            record = dict(nodePropertiesWritten=3, communityCount=2, modularity=0.64)
        elif query == gds.METRICS_QUERY:
            record = dict(ports=self.ports)
        elif query in (gds.CLEAR_QUERY, gds.DROP_QUERY):
            if query == gds.CLEAR_QUERY and not self.record['connections']:
                self.ports = []
            record = None  # drop(..., false) may yield no row when absent.
        else:
            self.fail('Unexpected Neo4j query: ' + query)
        result = Mock()
        result.single.return_value = deepcopy(record)
        return result

    def queries(self):
        return [query for query, _ in self.events]

    def params(self, query):
        return [params for statement, params in self.events if statement == query]

    def test_valid_snapshot_preserves_identity_precision_and_small_summary(self):
        actual = gds.validate_graph_snapshot()
        self.assertEqual({key: actual[key] for key in METADATA}, METADATA)
        self.assertEqual((actual['nodes'], actual['relationships'], actual['movements']), (3, 3, 12))
        self.assertEqual(self.params(gds.SNAPSHOT_QUERY), [{'owner': 'port-connections-v1'}])
        self.assertEqual(len(actual['graph_state']), 64)
        self.assertLess(len(json.dumps(actual)), 600)
        self.assertNotIn('connections', actual)
        self.assertNotIn('ports', actual)

    def test_missing_zero_and_mixed_snapshot_metadata_fail(self):
        original = deepcopy(self.record)
        cases = [dict(snapshots=[], connections=[]),
                 dict(snapshots=[dict(METADATA)], connections=[])]
        for field in METADATA:
            missing = {key: value for key, value in METADATA.items() if key != field}
            cases.append(dict(original, snapshots=[missing]))
            different = dict(METADATA, **{field: 'other-run' if field == 'run_id'
                                       else '2026-09-18T08:00:00Z'})
            cases.append(dict(original, snapshots=[dict(METADATA), different]))
        for record in cases:
            with self.subTest(record=record), self.assertRaises(ExportValidationError):
                self.record = record
                gds.validate_graph_snapshot()
        self.overrides[gds.SNAPSHOT_QUERY] = None
        with self.assertRaisesRegex(ExportValidationError, 'no result'):
            gds.validate_graph_snapshot()

    def test_non_port_endpoints_and_invalid_weights_fail_before_projection(self):
        original = deepcopy(self.record)
        cases = [('source_is_port', False), ('target_is_port', False)]
        cases.extend(('movement_count', value) for value in (None, True, 0, -1, 1.5, '3'))
        for key, value in cases:
            with self.subTest(key=key, value=value), self.assertRaises(ExportValidationError):
                self.record = deepcopy(original)
                self.record['connections'][0][key] = value
                gds.validate_graph_snapshot()
        self.assertNotIn(gds.PROJECT_QUERY, self.queries())

    def test_snapshot_fingerprint_is_stable_under_row_reordering(self):
        self.record['connections'].reverse()
        self.assertEqual(gds.validate_graph_snapshot(), self.snapshot)

    def test_snapshot_fingerprint_detects_replacement_and_equal_total_weight_swap(self):
        original = deepcopy(self.record)
        replaced = deepcopy(original)
        replaced['connections'][0]['relationship_id'] = 'replacement-rel'
        swapped = deepcopy(original)
        swapped['connections'][0]['movement_count'] = 7
        swapped['connections'][1]['movement_count'] = 3
        for changed in (replaced, swapped):
            with self.subTest(connections=changed['connections']):
                self.record = changed
                current = gds.validate_graph_snapshot()
                for key in ('run_id', 'nodes', 'relationships', 'movements'):
                    self.assertEqual(current[key], self.snapshot[key])
                self.assertNotEqual(current['graph_state'], self.snapshot['graph_state'])
                with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                    gds._assert_snapshot(self.snapshot, current)

    def test_recreation_drops_both_first_and_preserves_directed_and_undirected_counts(self):
        actual = gds.recreate_gds_projections(self.snapshot)
        queries = self.queries()
        self.assertEqual(self.params(gds.DROP_QUERY), [
            {'graph': 'ais-port-connections-directed'}, {'graph': 'ais-port-connections-undirected'},
        ])
        self.assertEqual(queries[:4], [gds.SNAPSHOT_QUERY, gds.DROP_QUERY,
                                      gds.DROP_QUERY, gds.PROJECT_QUERY])
        self.assertIn('gds.graph.drop($graph, false)', gds.DROP_QUERY)
        self.assertEqual(self.params(gds.PROJECT_QUERY), [
            dict(graph='ais-port-connections-directed', configuration={}),
            dict(graph='ais-port-connections-undirected',
                 configuration={'undirectedRelationshipTypes': ['CONNECTED_TO']}),
        ])
        self.assertEqual(actual['projections'], self.state['projections'])
        self.assertEqual(actual['directed_relationships'], 3)
        self.assertEqual(actual['graph_state'], self.snapshot['graph_state'])
        self.assertEqual(queries[-1], gds.SNAPSHOT_QUERY)

    def test_recreation_rejects_incorrect_projected_node_or_relationship_counts(self):
        for graph, key, count in ((gds.DIRECTED, 'nodeCount', 4),
                                  (gds.DIRECTED, 'relationshipCount', 2),
                                  (gds.UNDIRECTED, 'relationshipCount', 3)):
            with self.subTest(graph=graph, key=key):
                self.overrides[gds.PROJECT_QUERY] = lambda params: dict(
                    self.catalog[params['graph']], **({key: count} if params['graph'] == graph else {}))
                with self.assertRaisesRegex(ExportValidationError, 'Projected counts'):
                    gds.recreate_gds_projections(self.snapshot)

    def test_projection_recreation_between_stages_fails_before_algorithm(self):
        self.catalog[gds.DIRECTED]['creationTime'] = '2026-09-17T12:00:00+00:00'
        with self.assertRaisesRegex(ExportValidationError, 'projection changed'):
            gds.run_pagerank(self.state)
        self.assertNotIn(gds.PAGERANK_QUERY, self.queries())

    def test_stream_stats_and_write_settings_match_manual_02_through_05(self):
        preview = gds.run_louvain(gds.run_pagerank(self.state))
        written = gds.write_metrics_to_neo4j(preview)
        expected = {
            gds.PAGERANK_QUERY: ('pageRank', 'stream', gds.DIRECTED, PAGERANK),
            gds.LOUVAIN_QUERY: ('louvain', 'stream', gds.UNDIRECTED, LOUVAIN),
            gds.LOUVAIN_STATS_QUERY: ('louvain', 'stats', gds.UNDIRECTED, LOUVAIN),
            gds.PAGERANK_WRITE_QUERY: ('pageRank', 'write', gds.DIRECTED,
                                      dict(PAGERANK, writeProperty='pageRank')),
            gds.LOUVAIN_WRITE_QUERY: ('louvain', 'write', gds.UNDIRECTED,
                                     dict(LOUVAIN, writeProperty='communityId')),
        }
        manual = '\n'.join((Path(__file__).resolve().parents[3] / 'neo4j/gds' / filename).read_text()
                           for filename in ('02_pagerank.cypher', '03_louvain.cypher',
                                            '04_louvain_stats.cypher', '05_write_back.cypher'))
        manual_calls = {}
        for algorithm, mode, graph, body in re.findall(
                r"CALL gds\.(pageRank|louvain)\.(stream|stats|write)\('([^']+)',\s*\{(.*?)\}\)",
                manual, re.DOTALL):
            configuration = {key: ast.literal_eval(value.strip())
                             for key, value in re.findall(r'(\w+)\s*:\s*([^,]+)', body)}
            manual_calls[algorithm, mode] = dict(graph=graph, configuration=configuration)
        for query, (algorithm, mode, graph, configuration) in expected.items():
            with self.subTest(algorithm=algorithm, mode=mode):
                actual = self.params(query)
                self.assertEqual(actual, [dict(graph=graph, configuration=configuration)])
                self.assertEqual(actual[0], manual_calls[algorithm, mode])
        self.assertEqual(preview['modularity'], 0.42)
        self.assertEqual(written['modularity'], 0.64)
        self.assertEqual((preview['min_page_rank'], preview['max_page_rank']), (0.2, 1.2))
        self.assertNotIn('ports', written)

    def test_stream_node_count_mismatch_fails(self):
        for function, query in ((gds.run_pagerank, gds.PAGERANK_QUERY),
                                 (gds.run_louvain, gds.LOUVAIN_QUERY)):
            with self.subTest(query=query):
                self.overrides[query] = dict(nodes=2)
                with self.assertRaisesRegex(ExportValidationError, 'node count'):
                    function(self.state)

    def test_write_clears_only_metrics_and_preserves_visit_metadata(self):
        written = gds.write_metrics_to_neo4j(self.state)
        queries = self.queries()
        self.assertLess(queries.index(gds.CLEAR_QUERY), queries.index(gds.PAGERANK_WRITE_QUERY))
        self.assertLess(queries.index(gds.PAGERANK_WRITE_QUERY), queries.index(gds.LOUVAIN_WRITE_QUERY))
        self.assertEqual(queries[-1], gds.SNAPSHOT_QUERY)
        self.assertEqual(' '.join(gds.CLEAR_QUERY.split()),
                         'MATCH (port:Port) REMOVE port.pageRank, port.communityId')
        self.session.execute_write.assert_not_called()
        self.assertEqual(written['run_id'], METADATA['run_id'])
        self.assertEqual(written['modularity'], 0.64)

    def test_partial_algorithm_write_failure_propagates(self):
        for query in (gds.PAGERANK_WRITE_QUERY, gds.LOUVAIN_WRITE_QUERY):
            with self.subTest(query=query):
                self.events.clear()
                self.overrides.clear()
                failure = RuntimeError('algorithm write failed')
                self.overrides[query] = failure
                with self.assertRaises(RuntimeError) as raised:
                    gds.write_metrics_to_neo4j(self.state)
                self.assertIs(raised.exception, failure)
                self.assertIn(gds.CLEAR_QUERY, self.queries())

    def test_incomplete_algorithm_write_counts_fail(self):
        for query in (gds.PAGERANK_WRITE_QUERY, gds.LOUVAIN_WRITE_QUERY):
            with self.subTest(query=query):
                self.events.clear()
                self.overrides = {query: dict(nodePropertiesWritten=2)}
                with self.assertRaisesRegex(ExportValidationError, 'Written metric count'):
                    gds.write_metrics_to_neo4j(self.state)

    def test_graph_changed_before_write_or_export_blocks_both_destinations(self):
        self.record['connections'][0]['relationship_id'] = 'same-run-replacement'
        with patch.object(gds.metrics_export, 'export_metrics') as export:
            for operation in (gds.write_metrics_to_neo4j, gds.export_metrics_to_clickhouse):
                with self.subTest(operation=operation.__name__), \
                        self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                    operation(self.state)
            export.assert_not_called()
        self.assertNotIn(gds.CLEAR_QUERY, self.queries())
        self.assertNotIn(gds.PAGERANK_WRITE_QUERY, self.queries())

    def test_graph_changed_during_write_fails(self):
        def louvain_write(params):
            self.record['connections'][0]['relationship_id'] = 'replaced-during-write'
            return dict(nodePropertiesWritten=3, communityCount=2, modularity=0.64)
        self.overrides[gds.LOUVAIN_WRITE_QUERY] = louvain_write
        with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
            gds.write_metrics_to_neo4j(self.state)

    def test_missing_or_stale_endpoint_visit_run_id_blocks_workflow(self):
        original = deepcopy(self.record)
        for field in ('source_visit_run_id', 'target_visit_run_id'):
            for value in (None, '', 'other-run'):
                with self.subTest(field=field, value=value):
                    self.record = deepcopy(original)
                    self.record['connections'][0][field] = value
                    with self.assertRaisesRegex(ExportValidationError, 'visitRunId'):
                        gds.validate_graph_snapshot()
                    with patch.object(gds.metrics_export, 'export_metrics') as export:
                        with self.assertRaisesRegex(ExportValidationError, 'visitRunId'):
                            gds.export_metrics_to_clickhouse(self.state)
                        export.assert_not_called()

    def test_matching_ports_and_connections_cannot_replace_captured_dag_run(self):
        next_run = '2026-09-16T08:00:00Z'
        self.record['snapshots'][0]['run_id'] = next_run
        self.record['publications'][0]['run_id'] = next_run
        for row in self.record['connections']:
            row['source_visit_run_id'] = next_run
            row['target_visit_run_id'] = next_run
        self.assertEqual(gds.validate_graph_snapshot()['run_id'], next_run)
        with patch.object(gds.metrics_export, 'export_metrics') as export:
            with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                gds.export_metrics_to_clickhouse(self.state)
            export.assert_not_called()

    def test_invalid_metrics_or_visit_lineage_block_export(self):
        original = deepcopy(self.ports)
        cases = [('visit_run_id', None), ('visit_run_id', 'old-run'),
                 ('page_rank', None), ('page_rank', -0.1), ('page_rank', float('nan')),
                 ('community_id', None), ('community_id', True), ('community_id', -1),
                 ('port_id', ''), ('port_id', 'B')]
        with patch.object(gds.metrics_export, 'export_metrics') as export:
            for field, value in cases:
                with self.subTest(field=field, value=value):
                    self.ports = deepcopy(original)
                    self.ports[0][field] = value
                    with self.assertRaises(ExportValidationError):
                        gds.export_metrics_to_clickhouse(self.state)
            self.ports = deepcopy(original)
            self.ports[0].pop('visit_run_id')
            with self.assertRaisesRegex(ExportValidationError, 'visitRunId'):
                gds.export_metrics_to_clickhouse(self.state)
            export.assert_not_called()

    def test_metric_node_or_community_count_mismatch_fails(self):
        original = deepcopy(self.ports)
        for ports in (original[:-1], [dict(port, community_id=10) for port in original]):
            with self.subTest(ports=ports), self.assertRaisesRegex(ExportValidationError, 'count'):
                self.ports = ports
                gds.validate_metrics(self.state)

    def test_valid_metrics_refresh_actual_score_range_without_large_payload(self):
        state = dict(self.state, min_page_rank=99, max_page_rank=999)
        validated = gds.validate_metrics(state)
        self.assertEqual((validated['min_page_rank'], validated['max_page_rank']), (0.2, 1.2))
        self.assertEqual(validated['run_id'], METADATA['run_id'])
        self.assertNotIn('ports', validated)
        self.assertLess(len(json.dumps(validated)), 1200)

    def test_export_revalidates_and_pins_snapshot_then_reports_written_and_exported_statistics(self):
        written = gds.write_metrics_to_neo4j(gds.run_louvain(self.state))
        self.events.clear()
        exported = dict(METADATA, ports=3, communities=2, min_page_rank=0.3, max_page_rank=1.3)

        def export(*, expected_snapshot):
            self.assertIn(gds.METRICS_QUERY, self.queries())
            self.assertEqual(self.queries()[-1], gds.SNAPSHOT_QUERY)
            self.assertEqual(expected_snapshot['graph_state'], self.snapshot['graph_state'])
            self.assertEqual(expected_snapshot['modularity'], 0.64)
            self.assertEqual((expected_snapshot['min_page_rank'], expected_snapshot['max_page_rank']),
                             (0.2, 1.2))
            return exported

        with patch.object(gds.metrics_export, 'export_metrics', side_effect=export) as exporter:
            summary = gds.export_metrics_to_clickhouse(written)
        exporter.assert_called_once()
        self.assertEqual(summary, dict(METADATA, nodes=3, directed_relationships=3,
                                      communities=2, modularity=0.64, min_page_rank=0.3,
                                      max_page_rank=1.3, exported_ports=3))

    def empty_snapshot(self):
        self.record['connections'] = []
        self.record['snapshots'] = []
        self.record['publications'][0]['edge_count'] = 0
        return gds.validate_graph_snapshot()

    def test_verified_empty_workflow_skips_all_algorithms_and_clears_stale_metrics(self):
        snapshot = self.empty_snapshot()
        self.assertEqual((snapshot['relationships'], snapshot['movements'], snapshot['nodes']), (0, 0, 0))
        state = gds.recreate_gds_projections(snapshot)
        self.assertEqual(state['projections'], {})
        self.assertEqual(state['directed_relationships'], 0)
        state = gds.run_louvain(gds.run_pagerank(state))
        self.assertEqual((state['communities'], state['modularity'], state['ran_levels']), (0, None, 0))
        state = gds.write_metrics_to_neo4j(state)
        self.assertEqual(state['nodes_written'], 0)
        state = gds.validate_metrics(state)
        self.assertEqual((state['min_page_rank'], state['max_page_rank']), (None, None))
        self.assertEqual(self.ports, [])
        self.assertEqual(len(self.params(gds.DROP_QUERY)), 2)
        self.assertIn(gds.CLEAR_QUERY, self.queries())
        for query in (gds.PROJECT_QUERY, gds.PAGERANK_QUERY, gds.LOUVAIN_QUERY,
                      gds.LOUVAIN_STATS_QUERY, gds.PAGERANK_WRITE_QUERY,
                      gds.LOUVAIN_WRITE_QUERY, gds.METRICS_QUERY):
            self.assertNotIn(query, self.queries())

    def test_publication_missing_malformed_or_inconsistent_fails(self):
        original = deepcopy(self.record)
        for publications in ([], [original['publications'][0]] * 2):
            self.record = dict(original, publications=publications)
            with self.assertRaises(ExportValidationError):
                gds.validate_graph_snapshot()
        cases = [('managed_by', 'wrong'), ('run_id', ''), ('window_start', None),
                 ('window_end', METADATA['window_start']), ('published_at', None),
                 ('published_at', 'bad-date'), ('edge_count', 2), ('edge_count', 0),
                 ('edge_count', True), ('edge_count', -1), ('edge_count', 3.0),
                 ('generation', True), ('generation', 0), ('generation', 1.5),
                 ('generation', None), ('run_id', 'stale-run')]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                self.record = deepcopy(original)
                self.record['publications'][0][field] = value
                with self.assertRaises(ExportValidationError):
                    gds.validate_graph_snapshot()

    def test_generation_republication_detected_for_empty_and_nonempty(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                snapshot = self.empty_snapshot() if empty else gds.validate_graph_snapshot()
                self.record['publications'][0]['generation'] += 1
                with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                    gds.recreate_gds_projections(snapshot)

    def test_empty_republication_between_stages_and_during_clear_detected(self):
        state = gds.recreate_gds_projections(self.empty_snapshot())
        state = gds.run_louvain(gds.run_pagerank(state))
        self.record['publications'][0]['generation'] += 1
        for stage in (gds.run_pagerank, gds.run_louvain, gds.write_metrics_to_neo4j, gds.validate_metrics):
            with self.subTest(stage=stage.__name__), self.assertRaisesRegex(ExportValidationError, 'graph changed'):
                stage(state)
        self.record['publications'][0]['generation'] -= 1
        def clear(params):
            self.record['publications'][0]['generation'] += 1
        self.overrides[gds.CLEAR_QUERY] = clear
        with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
            gds.write_metrics_to_neo4j(state)

    def test_empty_rejects_unexpected_projection_and_stale_metrics(self):
        state = dict(gds.recreate_gds_projections(self.empty_snapshot()), communities=0)
        with self.assertRaisesRegex(ExportValidationError, 'Stale Port metrics'):
            gds.validate_metrics(state)
        self.overrides[gds.EXISTS_QUERY] = dict(exists=True)
        with self.assertRaisesRegex(ExportValidationError, 'Unexpected GDS projection'):
            gds.run_pagerank(state)

    def test_cleanup_handles_missing_graphs_and_attempts_both_after_failure(self):
        self.assertIsNone(gds.cleanup_gds_projections())
        self.assertEqual(self.params(gds.DROP_QUERY), [
            {'graph': gds.DIRECTED}, {'graph': gds.UNDIRECTED}])
        self.events.clear()
        failure = RuntimeError('first drop failed')

        def drop(params):
            if params['graph'] == gds.DIRECTED:
                raise failure
            return None

        self.overrides[gds.DROP_QUERY] = drop
        with self.assertRaises(RuntimeError) as raised:
            gds.cleanup_gds_projections()
        self.assertIs(raised.exception, failure)
        self.assertEqual(self.params(gds.DROP_QUERY), [
            {'graph': gds.DIRECTED}, {'graph': gds.UNDIRECTED}])


if __name__ == '__main__':
    unittest.main()
