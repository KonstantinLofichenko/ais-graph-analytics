"""Run the validated Port GDS workflow against one unchanged managed snapshot."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import logging
import os

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipelines.graph_metrics import export as metrics_export
from pipelines.graph_metrics.export import ExportValidationError, OWNER, validate_snapshot_metadata
from pipelines.port_visits.run import ROOT

LOGGER = logging.getLogger(__name__)
DIRECTED = 'ais-port-connections-directed'
UNDIRECTED = 'ais-port-connections-undirected'
PAGERANK_CONFIG = dict(relationshipWeightProperty='movementCount', maxIterations=20,
                       dampingFactor=0.85, concurrency=1)
LOUVAIN_CONFIG = dict(relationshipWeightProperty='movementCount', concurrency=1)

# Scan all owned relationships, including malformed non-Port endpoints, so none
# can be silently excluded from snapshot validation.
SNAPSHOT_QUERY = '''
    MATCH (source)-[r:CONNECTED_TO {managedBy: $owner}]->(target)
    RETURN collect(DISTINCT {run_id: r.runId, window_start: r.windowStart,
                             window_end: r.windowEnd}) AS snapshots,
           collect({relationship_id: elementId(r), source_id: elementId(source),
                    target_id: elementId(target), source_port_id: source.portId,
                    target_port_id: target.portId, source_is_port: source:Port,
                    source_visit_run_id: source.visitRunId, target_visit_run_id: target.visitRunId,
                    target_is_port: target:Port, movement_count: r.movementCount}) AS connections
'''

# Same Cypher aggregation projection as 01_project_graphs.cypher. An empty
# configuration preserves stored direction; Louvain retains both reciprocal weights.
PROJECT_QUERY = '''
    MATCH (source:Port)-[r:CONNECTED_TO {managedBy: 'port-connections-v1'}]->(target:Port)
    WITH gds.graph.project($graph, source, target,
      {sourceNodeLabels: ['Port'], targetNodeLabels: ['Port'],
       relationshipType: 'CONNECTED_TO', relationshipProperties: r { .movementCount }},
      $configuration) AS graph
    RETURN graph.graphName AS graphName, graph.nodeCount AS nodeCount,
           graph.relationshipCount AS relationshipCount
'''
LIST_QUERY = '''CALL gds.graph.list($graph)
    YIELD graphName, nodeCount, relationshipCount, creationTime
    RETURN graphName, nodeCount, relationshipCount, creationTime'''
DROP_QUERY = 'CALL gds.graph.drop($graph, false) YIELD graphName RETURN graphName'
PAGERANK_QUERY = '''CALL gds.pageRank.stream($graph, $configuration) YIELD nodeId, score
    RETURN count(*) AS nodes, min(score) AS min_page_rank, max(score) AS max_page_rank'''
LOUVAIN_QUERY = '''CALL gds.louvain.stream($graph, $configuration) YIELD nodeId, communityId
    RETURN count(*) AS nodes, count(DISTINCT communityId) AS communities'''
LOUVAIN_STATS_QUERY = '''CALL gds.louvain.stats($graph, $configuration)
    YIELD communityCount, modularity, ranLevels RETURN communityCount, modularity, ranLevels'''
CLEAR_QUERY = '''MATCH (port:Port)
    REMOVE port.pageRank, port.communityId'''
PAGERANK_WRITE_QUERY = '''CALL gds.pageRank.write($graph, $configuration)
    YIELD nodePropertiesWritten, ranIterations, didConverge
    RETURN nodePropertiesWritten, ranIterations, didConverge'''
LOUVAIN_WRITE_QUERY = '''CALL gds.louvain.write($graph, $configuration)
    YIELD nodePropertiesWritten, communityCount, modularity
    RETURN nodePropertiesWritten, communityCount, modularity'''
PROJECTED_PORTS = '''
    CALL gds.graph.relationships.stream($graph) YIELD sourceNodeId, targetNodeId
    UNWIND [sourceNodeId, targetNodeId] AS nodeId
    WITH DISTINCT gds.util.asNode(nodeId) AS port
'''
METRICS_QUERY = PROJECTED_PORTS + '''
    RETURN collect({port_id: port.portId, page_rank: port.pageRank,
                    community_id: port.communityId,
                    visit_run_id: port.visitRunId}) AS ports
'''


@contextmanager
def _session():
    load_dotenv(ROOT / '.env')
    with GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                              auth=(os.getenv('NEO4J_USER', 'neo4j'),
                                    os.environ['NEO4J_PASSWORD'])) as driver:
        driver.verify_connectivity()
        with driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
            yield session


def _single(result):
    record = result.single()
    if record is None:
        raise ExportValidationError('GDS returned no result; recreate projections and rerun the workflow')
    return record


def _read_snapshot(tx):
    record = _single(tx.run(SNAPSHOT_QUERY, owner=OWNER))
    metadata = validate_snapshot_metadata(record['snapshots'])
    connections = record['connections']
    if not connections:
        raise ExportValidationError('No managed CONNECTED_TO snapshot is available')
    nodes = set()
    for row in connections:
        if not row['source_is_port'] or not row['target_is_port']:
            raise ExportValidationError('Every managed CONNECTED_TO endpoint must be a Port')
        if any(row.get(key) != metadata['run_id']
               for key in ('source_visit_run_id', 'target_visit_run_id')):
            raise ExportValidationError('Every active Port visitRunId must match CONNECTED_TO run_id')
        weight = row['movement_count']
        if isinstance(weight, bool) or not isinstance(weight, int) or weight <= 0:
            raise ExportValidationError('Managed CONNECTED_TO movementCount must be a positive integer')
        nodes.update((row['source_id'], row['target_id']))
    # Transient equality check, never an ID or a stored property. Relationship
    # element IDs detect same-run replacements; endpoints/weights detect edits.
    state = json.dumps(sorted(connections, key=lambda row: row['relationship_id']),
                       sort_keys=True, separators=(',', ':'))
    return dict(run_id=metadata['run_id'], window_start=metadata['window_start'].isoformat(),
                window_end=metadata['window_end'].isoformat(), relationships=len(connections),
                movements=sum(row['movement_count'] for row in connections), nodes=len(nodes),
                graph_state=hashlib.sha256(state.encode()).hexdigest())


def _assert_snapshot(expected, current):
    keys = ('run_id', 'window_start', 'window_end', 'relationships', 'movements', 'nodes', 'graph_state')
    if any(expected[key] != current[key] for key in keys):
        raise ExportValidationError('Managed CONNECTED_TO graph changed during GDS calculation; '
                                    'rerun the entire workflow for the current snapshot')


def validate_graph_snapshot():
    with _session() as session:
        snapshot = session.execute_read(_read_snapshot)
    LOGGER.info('GDS snapshot: run_id=%s window=%s..%s relationships=%d movements=%d',
                snapshot['run_id'], snapshot['window_start'], snapshot['window_end'],
                snapshot['relationships'], snapshot['movements'])
    return snapshot


def _drop_projections(session):
    errors = []
    for graph in (DIRECTED, UNDIRECTED):
        try:
            session.run(DROP_QUERY, graph=graph).consume()
        except Exception as error:
            errors.append(error)
    if errors:
        raise errors[0]


def cleanup_gds_projections():
    """ALL_DONE cleanup; missing graphs are a successful no-op."""
    with _session() as session:
        _drop_projections(session)
    LOGGER.info('GDS projections dropped')


def _catalog_entry(session, graph):
    row = _single(session.run(LIST_QUERY, graph=graph))
    return dict(nodes=row['nodeCount'], relationships=row['relationshipCount'],
                created_at=str(row['creationTime']))


def _check_projections(session, state):
    for graph in (DIRECTED, UNDIRECTED):
        if _catalog_entry(session, graph) != state['projections'][graph]:
            raise ExportValidationError('GDS projection changed or was recreated; rerun the entire workflow')


def recreate_gds_projections(snapshot):
    with _session() as session:
        _assert_snapshot(snapshot, session.execute_read(_read_snapshot))
        _drop_projections(session)
        projections = {}
        for graph, configuration, multiplier in (
                (DIRECTED, {}, 1),
                (UNDIRECTED, {'undirectedRelationshipTypes': ['CONNECTED_TO']}, 2)):
            row = _single(session.run(PROJECT_QUERY, graph=graph, configuration=configuration))
            if (row['nodeCount'] != snapshot['nodes']
                    or row['relationshipCount'] != multiplier * snapshot['relationships']):
                raise ExportValidationError('Projected counts differ from the validated managed snapshot')
            projections[graph] = _catalog_entry(session, graph)
            if (projections[graph]['nodes'] != row['nodeCount']
                    or projections[graph]['relationships'] != row['relationshipCount']):
                raise ExportValidationError('GDS projection changed during recreation')
            LOGGER.info('GDS projection: graph=%s nodes=%d relationships=%d',
                        graph, row['nodeCount'], row['relationshipCount'])
        _assert_snapshot(snapshot, session.execute_read(_read_snapshot))
    return dict(snapshot, projections=projections,
                directed_relationships=projections[DIRECTED]['relationships'])


def run_pagerank(state):
    with _session() as session:
        _check_projections(session, state)
        row = _single(session.run(PAGERANK_QUERY, graph=DIRECTED, configuration=PAGERANK_CONFIG))
        if row['nodes'] != state['nodes']:
            raise ExportValidationError('PageRank node count differs from the projected active Ports')
    LOGGER.info('GDS PageRank: nodes=%d min_page_rank=%s max_page_rank=%s',
                row['nodes'], row['min_page_rank'], row['max_page_rank'])
    # As in the manual workflow, retain the projections, not per-node XCom data.
    # The write stage reruns the algorithm with the identical settings.
    return dict(state, min_page_rank=row['min_page_rank'], max_page_rank=row['max_page_rank'])


def run_louvain(state):
    with _session() as session:
        _check_projections(session, state)
        preview = _single(session.run(LOUVAIN_QUERY, graph=UNDIRECTED, configuration=LOUVAIN_CONFIG))
        if preview['nodes'] != state['nodes']:
            raise ExportValidationError('Louvain node count differs from the projected active Ports')
        stats = _single(session.run(LOUVAIN_STATS_QUERY, graph=UNDIRECTED, configuration=LOUVAIN_CONFIG))
    LOGGER.info('GDS Louvain: communityCount=%d modularity=%s ranLevels=%d',
                stats['communityCount'], stats['modularity'], stats['ranLevels'])
    return dict(state, communities=stats['communityCount'], modularity=stats['modularity'],
                ran_levels=stats['ranLevels'])


def write_metrics_to_neo4j(state):
    with _session() as session:
        _assert_snapshot(state, session.execute_read(_read_snapshot))
        _check_projections(session, state)
        # Separate commits match 05_write_back.cypher; visit metadata is read-only.
        session.run(CLEAR_QUERY).consume()
        pagerank = _single(session.run(PAGERANK_WRITE_QUERY, graph=DIRECTED,
                                      configuration=dict(PAGERANK_CONFIG, writeProperty='pageRank')))
        louvain = _single(session.run(LOUVAIN_WRITE_QUERY, graph=UNDIRECTED,
                                     configuration=dict(LOUVAIN_CONFIG, writeProperty='communityId')))
        if any(row['nodePropertiesWritten'] != state['nodes'] for row in (pagerank, louvain)):
            raise ExportValidationError('Written metric count differs from the projected active Ports')
        _check_projections(session, state)
        _assert_snapshot(state, session.execute_read(_read_snapshot))
    LOGGER.info('GDS write-back: run_id=%s nodes=%d ranIterations=%d didConverge=%s',
                state['run_id'], state['nodes'], pagerank['ranIterations'], pagerank['didConverge'])
    # Louvain stream, stats and write are separate executions, as in the manual
    # scripts. Report the final write's communities and modularity, not the preview.
    return dict(state, communities=louvain['communityCount'], modularity=louvain['modularity'])


def _validated_metrics(tx, state):
    _assert_snapshot(state, _read_snapshot(tx))
    _check_projections(tx, state)
    ports = _single(tx.run(METRICS_QUERY, graph=DIRECTED))['ports']
    rows = metrics_export.build_export_rows([state], ports, datetime.now(timezone.utc))
    if len(rows) != state['nodes']:
        raise ExportValidationError('Metric port count differs from the projected active Ports')
    if len({row['community_id'] for row in rows}) != state['communities']:
        raise ExportValidationError('Written community count differs from the current Port metrics')
    _assert_snapshot(state, _read_snapshot(tx))
    return rows


def validate_metrics(state):
    with _session() as session:
        rows = session.execute_read(_validated_metrics, state)
    return dict(state, min_page_rank=min(row['page_rank'] for row in rows),
                max_page_rank=max(row['page_rank'] for row in rows))


def export_metrics_to_clickhouse(state):
    # Revalidate in this task too, then pin the exporter's final Neo4j read to the
    # original snapshot. A graph swap between Airflow tasks must not export another run.
    state = validate_metrics(state)
    exported = metrics_export.export_metrics(expected_snapshot=state)
    summary = dict(run_id=exported['run_id'], window_start=exported['window_start'],
                   window_end=exported['window_end'], nodes=state['nodes'],
                   directed_relationships=state['directed_relationships'],
                   communities=exported['communities'], modularity=state['modularity'],
                   min_page_rank=exported['min_page_rank'], max_page_rank=exported['max_page_rank'],
                   exported_ports=exported['ports'])
    LOGGER.info('GDS metrics complete: run_id=%s window=%s..%s nodes=%d '
                'directed_relationships=%d communities=%d modularity=%s '
                'min_page_rank=%s max_page_rank=%s exported_ports=%d',
                summary['run_id'], summary['window_start'], summary['window_end'], summary['nodes'],
                summary['directed_relationships'], summary['communities'], summary['modularity'],
                summary['min_page_rank'], summary['max_page_rank'], summary['exported_ports'])
    return summary
