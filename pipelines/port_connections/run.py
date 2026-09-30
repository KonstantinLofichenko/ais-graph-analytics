"""Publish one completed port-visit run as the current managed connection snapshot."""
import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from neo4j import GraphDatabase

from pipelines.port_connections.aggregate import aggregate_connections
from pipelines.port_visits.detect import timestamp
from pipelines.port_visits.run import ClickHouse, ROOT, date_string

OWNER = 'port-connections-v1'
LOGGER = logging.getLogger(__name__)
METADATA_KEYS = {'run_id', 'window_start', 'window_end'}


def validate_metadata(metadata):
    """Accept only the small completed-run handoff; preserve its original identity."""
    if not isinstance(metadata, dict) or set(metadata) != METADATA_KEYS:
        raise ValueError('Port connections require only run_id, window_start and window_end metadata')
    if any(not isinstance(value, str) or not value.strip() for value in metadata.values()):
        raise ValueError('Port connection run metadata values must be nonempty strings')
    for field in ('window_start', 'window_end'):
        if datetime.fromisoformat(metadata[field].replace('Z', '+00:00')).tzinfo is None:
            raise ValueError('Port connection window metadata must include a timezone')
    if timestamp(metadata['window_start']) >= timestamp(metadata['window_end']):
        raise ValueError('Port connection run metadata requires window_start < window_end')
    return dict(metadata)


def read_completed_visits(ch, metadata):
    """Read FINAL rows for the supplied exact completed run, never the latest run."""
    metadata = validate_metadata(metadata)
    parameters = dict(param_run_id=metadata['run_id'],
                      param_window_start=date_string(timestamp(metadata['window_start'])),
                      param_window_end=date_string(timestamp(metadata['window_end'])))

    completed = ch.query('''
        SELECT
            r.run_id,
            toString(r.window_start, 'UTC') AS window_start,
            toString(r.window_end, 'UTC') AS window_end,
            r.visit_count
        FROM analytics.port_visit_runs AS r FINAL
        WHERE r.run_id = {run_id:String}
          AND r.window_start = {window_start:DateTime64(6, 'UTC')}
          AND r.window_end = {window_end:DateTime64(6, 'UTC')}
        FORMAT JSONEachRow''', parameters)

    runs = [json.loads(line) for line in completed.splitlines() if line.strip()]
    if (len(runs) != 1 or runs[0]['run_id'] != metadata['run_id']
            or timestamp(runs[0]['window_start']) != timestamp(metadata['window_start'])
            or timestamp(runs[0]['window_end']) != timestamp(metadata['window_end'])):
        raise RuntimeError('No completed port-visit run matches run_id and exact window: '
                           + metadata['run_id'])

    data = ch.query('''
        SELECT visit_id, mmsi, port_id, toString(arrival_at, 'UTC') AS arrival_at
        FROM analytics.port_visits FINAL
        WHERE run_id = {run_id:String} AND is_deleted = 0
        ORDER BY mmsi, arrival_at, visit_id
        FORMAT JSONEachRow''', {'param_run_id': metadata['run_id']})
    visits = [json.loads(line) for line in data.splitlines() if line.strip()]
    expected = int(runs[0]['visit_count'])
    if len(visits) != expected:
        raise RuntimeError(f'Port-visit run {metadata["run_id"]} is inconsistent: '
                           f'completed visit_count={expected}, FINAL visits={len(visits)}')
    return visits


def graph_snapshot(tx, connections, metadata):
    """Atomically replace owned edges and publish durable snapshot metadata."""
    # Lock this owner's snapshot before replacing edges. Failed transactions also
    # roll back this increment; successful republishes get a new generation.
    tx.run('''MERGE (s:ConnectionSnapshot {managedBy: $owner})
              SET s.generation = coalesce(s.generation, 0) + 1''', owner=OWNER).consume()
    port_ids = sorted({row[field] for row in connections
                       for field in ('from_port_id', 'to_port_id')})
    if port_ids:
        result = tx.run('''UNWIND $port_ids AS port_id
                           OPTIONAL MATCH (p:Port {portId: port_id})
                           WITH port_id, p WHERE p IS NULL
                           RETURN port_id ORDER BY port_id''', port_ids=port_ids)
        missing = [record['port_id'] for record in result]
        if missing:
            raise RuntimeError('Missing Neo4j Port nodes (portId): ' + ', '.join(missing))

    # Current snapshot, like VISITED. Manual edges and other relationship types survive.
    tx.run('''MATCH (:Port)-[r:CONNECTED_TO]->(:Port)
              WHERE r.managedBy = $owner DELETE r''', owner=OWNER).consume()
    for offset in range(0, len(connections), 1000):
        rows = connections[offset:offset+1000]
        result = tx.run('''UNWIND $rows AS row
            MATCH (a:Port {portId: row.from_port_id})
            MATCH (b:Port {portId: row.to_port_id})
            MERGE (a)-[r:CONNECTED_TO {managedBy: $owner}]->(b)
            SET r.movementCount = row.movement_count, r.vesselCount = row.vessel_count,
                r.firstSeen = datetime(row.first_seen), r.lastSeen = datetime(row.last_seen),
                r.runId = $run_id, r.windowStart = datetime($window_start),
                r.windowEnd = datetime($window_end)
            RETURN count(r) AS published''', rows=rows, owner=OWNER, **metadata)
        published = result.single()['published']
        if published != len(rows):
            # A disappeared/duplicate endpoint must roll back deletion and all batches.
            raise RuntimeError(f'Port connection endpoint match failed: expected {len(rows)} '
                               f'relationships, published {published}')

    # Do not restrict endpoint labels here: malformed owned edges must fail,
    # not disappear from the count or masquerade as a valid empty snapshot.
    final = tx.run('''MATCH (a)-[r:CONNECTED_TO]->(b)
        WHERE r.managedBy = $owner
        RETURN count(r) AS edge_count,
               sum(CASE WHEN a:Port AND b:Port THEN 0 ELSE 1 END) AS malformed''',
                   owner=OWNER).single()
    if final['malformed']:
        raise RuntimeError('Managed CONNECTED_TO relationships must have two Port endpoints')
    if final['edge_count'] != len(connections):
        raise RuntimeError(f'Port connection final edge count mismatch: expected {len(connections)}, '
                           f'actual {final["edge_count"]}')

    tx.run('''MATCH (s:ConnectionSnapshot {managedBy: $owner})
        SET s.runId = $run_id, s.windowStart = datetime($window_start),
            s.windowEnd = datetime($window_end), s.edgeCount = $edge_count,
            s.publishedAt = datetime()''',
           owner=OWNER, edge_count=final['edge_count'], **metadata).consume()


def publish_connections(connections, metadata):
    with GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                              auth=(os.getenv('NEO4J_USER', 'neo4j'),
                                    os.environ['NEO4J_PASSWORD'])) as driver:
        driver.verify_connectivity()
        with driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
            session.execute_write(graph_snapshot, connections, metadata)


def publish_port_connections(metadata):
    """Airflow entrypoint: reuse only the upstream run ID and exact window bounds."""
    metadata = validate_metadata(metadata)
    load_dotenv(ROOT / '.env')
    ch = ClickHouse()
    try:
        visits = read_completed_visits(ch, metadata)
    finally:
        ch.session.close()
    connections, stats = aggregate_connections(visits)
    publish_connections(connections, metadata)
    summary = dict(metadata, **stats)
    LOGGER.info('Port connections: run_id=%s visits=%d movements=%d relationships=%d '
                'vessels=%d window_start=%s window_end=%s',
                summary['run_id'], summary['visits'], summary['movements'],
                summary['relationships'], summary['vessels'],
                summary['window_start'], summary['window_end'])
    return summary
