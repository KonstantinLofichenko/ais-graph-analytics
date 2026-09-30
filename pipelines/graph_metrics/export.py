#!/usr/bin/env python3
"""Export existing Neo4j Port metrics for the current managed connection snapshot."""
from datetime import datetime, timedelta, timezone
import json
import logging
import math
import os
from pathlib import Path
import sys

if not __package__:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.time import DateTime as Neo4jDateTime

from pipelines.port_visits.run import ClickHouse, ROOT, date_string
from pipelines.port_visits.detect import timestamp

OWNER = 'port-connections-v1'
LOGGER = logging.getLogger(__name__)
UINT64_MAX = 2**64 - 1

PORT_PROPERTIES = '''{port_id: port.portId, port_name: port.name,
    page_rank: port.pageRank, community_id: port.communityId, visit_run_id: port.visitRunId,
    memberships: [(port)-[:MEMBER_OF]->(c) | {is_community: c:Community,
        community_id: c.community_id, run_id: c.run_id, community_name: c.community_name,
        community_label: c.community_label, port_count: c.port_count}]}'''

# Preserve node identity while collecting endpoints, including the empty case.
# The durable publication and all owned edges are validated around this read.
SNAPSHOT_QUERY = """
    WITH $owner AS owner
    CALL (owner) {
        MATCH (source)-[r:CONNECTED_TO {managedBy: owner}]->(target)
        RETURN collect(DISTINCT source) + collect(DISTINCT target) AS endpoints
    }
    UNWIND endpoints AS port
    WITH collect(DISTINCT port) AS ports
    RETURN [port IN ports | """ + PORT_PROPERTIES + """] AS ports
"""


class ExportValidationError(ValueError):
    """Invalid or incomplete snapshot data; nothing should be inserted."""


def _utc_datetime(value, field):
    try:
        if isinstance(value, Neo4jDateTime):
            result = value.to_native()
        elif isinstance(value, datetime):
            result = value
        else:
            result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (AttributeError, TypeError, ValueError):
        raise ExportValidationError(f'{field} must be an ISO timestamp with a timezone') from None
    if result.tzinfo is None or result.utcoffset() is None:
        raise ExportValidationError(f'{field} must include a timezone')
    return result.astimezone(timezone.utc)


def validate_snapshot_metadata(snapshots):
    """Require one complete identity from the current managed relationships."""
    if not snapshots:
        raise ExportValidationError('No managed CONNECTED_TO snapshot is available to export')
    if len(snapshots) != 1:
        raise ExportValidationError('Managed CONNECTED_TO relationships contain mixed run/window metadata')
    metadata = snapshots[0]
    run_id = metadata.get('run_id')
    if not isinstance(run_id, str) or not run_id.strip():
        raise ExportValidationError('Managed CONNECTED_TO snapshot requires a nonempty run_id')
    start = _utc_datetime(metadata.get('window_start'), 'window_start')
    end = _utc_datetime(metadata.get('window_end'), 'window_end')
    if start >= end:
        raise ExportValidationError('Managed CONNECTED_TO snapshot requires window_start < window_end')
    return dict(run_id=run_id, window_start=start, window_end=end)


def build_export_rows(snapshots, ports, exported_at, *, verified_snapshot=None):
    """Validate all active endpoint metrics before constructing a complete export."""
    metadata = validate_snapshot_metadata(snapshots)
    if not isinstance(exported_at, datetime) or exported_at.tzinfo is None:
        raise ExportValidationError('exported_at must be a timezone-aware datetime')
    exported_at = exported_at.astimezone(timezone.utc)
    if verified_snapshot is not None:
        if validate_snapshot_metadata([verified_snapshot]) != metadata:
            raise ExportValidationError('Verified snapshot identity differs from export metadata')
        if len(ports) != verified_snapshot['nodes']:
            raise ExportValidationError('Metric port count differs from verified active Ports')
    if not ports:
        # Only a state returned by the durable snapshot reader authorizes no rows.
        if (verified_snapshot is not None
                and type(verified_snapshot.get('relationships')) is int
                and verified_snapshot['relationships'] == 0
                and type(verified_snapshot.get('nodes')) is int
                and verified_snapshot['nodes'] == 0
                and type(verified_snapshot.get('generation')) is int
                and verified_snapshot['generation'] > 0
                and verified_snapshot.get('published_at')
                and verified_snapshot.get('graph_state')):
            return []
        raise ExportValidationError('Managed CONNECTED_TO snapshot has no active endpoint Ports')
    rows = []
    seen = set()
    for port in ports:
        port_id = port.get('port_id')
        if not isinstance(port_id, str) or not port_id.strip():
            raise ExportValidationError('Every active Port must have a nonempty portId')
        if port_id in seen:
            raise ExportValidationError(f'Duplicate active Port portId: {port_id}')
        seen.add(port_id)
        if port.get('visit_run_id') != metadata['run_id']:
            raise ExportValidationError(f'Port {port_id}: visitRunId must match current '
                                        f'CONNECTED_TO run_id {metadata["run_id"]}')
        page_rank = port.get('page_rank')
        if isinstance(page_rank, bool) or not isinstance(page_rank, (int, float)):
            raise ExportValidationError(f'Port {port_id}: pageRank must be a finite nonnegative number')
        try:
            page_rank = float(page_rank)
        except OverflowError:
            raise ExportValidationError(f'Port {port_id}: pageRank exceeds Float64 range') from None
        if not math.isfinite(page_rank) or page_rank < 0:
            raise ExportValidationError(f'Port {port_id}: pageRank must be a finite nonnegative number')
        community_id = port.get('community_id')
        if (isinstance(community_id, bool) or not isinstance(community_id, int)
                or not 0 <= community_id <= UINT64_MAX):
            raise ExportValidationError(f'Port {port_id}: communityId must be a UInt64 integer')
        rows.append(dict(metadata, snapshot_date=metadata['window_start'].date().isoformat(),
                         port_id=port_id, page_rank=page_rank, community_id=community_id,
                         exported_at=exported_at))
    return sorted(rows, key=lambda row: row['port_id'])


def _check_source_snapshot(tx, expected):
    # Reuse the durable publication contract without importing GDS at module load.
    from pipelines.graph_metrics.gds import _assert_snapshot, _read_snapshot
    current = _read_snapshot(tx)
    if expected is not None:
        _assert_snapshot(expected, current)
    return current


def read_graph_snapshot(tx, expected_snapshot=None):
    snapshot = _check_source_snapshot(tx, expected_snapshot)
    record = tx.run(SNAPSHOT_QUERY, owner=OWNER).single()
    if record is None:
        raise ExportValidationError('No metric endpoint result for the verified snapshot')
    _check_source_snapshot(tx, snapshot)
    return [snapshot], record['ports']


def validate_canonical_ports(ch, rows):
    data = ch.query('SELECT port_id FROM analytics.ports FINAL FORMAT JSONEachRow')
    known = {json.loads(line)['port_id'] for line in data.splitlines() if line.strip()}
    missing = sorted({row['port_id'] for row in rows} - known)
    if missing:
        raise ExportValidationError('Active Neo4j portId values missing from analytics.ports: '
                                    + ', '.join(missing))


def publish_metric_snapshot(ch, metadata, rows, exported_at, *, before_insert):
    """Reconcile one run's logical membership, including a validated empty export."""
    metadata = validate_snapshot_metadata([metadata])
    run_id = metadata['run_id']
    expected = {row['port_id'] for row in rows}
    if len(expected) != len(rows) or any(row['run_id'] != run_id for row in rows):
        raise ExportValidationError('Export rows require unique port IDs within the requested run')
    params = {'param_run_id': run_id}
    data = ch.query('''SELECT * FROM analytics.port_graph_metrics FINAL
                       WHERE run_id = {run_id:String} AND is_deleted = 0
                       FORMAT JSONEachRow''', params)
    previous = [json.loads(line) for line in data.splitlines() if line.strip()]
    # Read corrected logical dates after a metadata-only historical backfill.
    # FINAL retains each key's maximum version, including tombstones, so its
    # global maximum is also the watermark across physical versions.
    # The stored window must not change, particularly across monthly partitions.
    data = ch.query('''SELECT toString(maxOrNull(exported_at), 'UTC') AS latest,
                       groupUniqArray(tuple(toString(window_start, 'UTC'),
                           toString(window_end, 'UTC'), toString(snapshot_date))) AS windows
                       FROM analytics.port_graph_metrics FINAL WHERE run_id = {run_id:String}
                       FORMAT JSONEachRow''', params)
    history = json.loads(data)
    window = (date_string(metadata['window_start']), date_string(metadata['window_end']),
              metadata['window_start'].date().isoformat())
    if any(tuple(stored) != window for stored in history['windows']):
        raise ExportValidationError(f'Graph metric run {run_id} already has a different window/snapshot_date')
    exported_at = _utc_datetime(exported_at, 'exported_at')
    if history['latest'] is not None:
        exported_at = max(exported_at, timestamp(history['latest']) + timedelta(microseconds=1))
    active = [dict(row, is_deleted=0, exported_at=exported_at) for row in rows]
    tombstones = [dict(row, is_deleted=1, exported_at=exported_at)
                  for row in previous if row['port_id'] not in expected]
    payload = active + tombstones
    # Recheck Neo4j after the ClickHouse reads, immediately before publication.
    before_insert()
    ch.insert('port_graph_metrics', payload, batch_size=max(1, len(payload)))
    data = ch.query('''SELECT port_id FROM analytics.port_graph_metrics FINAL
                       WHERE run_id = {run_id:String} AND is_deleted = 0
                       FORMAT JSONEachRow''', params)
    actual = [json.loads(line)['port_id'] for line in data.splitlines() if line.strip()]
    if len(actual) != len(expected) or set(actual) != expected:
        raise ExportValidationError(f'Graph metric run {run_id} active membership mismatch: '
                                    f'expected={len(expected)} actual={len(actual)} '
                                    f'missing={sorted(expected - set(actual))} '
                                    f'unexpected={sorted(set(actual) - expected)}')
    LOGGER.info('Graph metric reconciliation: run_id=%s active_ports=%d tombstones=%d',
                run_id, len(actual), len(tombstones))


def export_metrics(expected_snapshot=None):
    """Read existing graph metrics and insert one validated, retry-stable snapshot."""
    load_dotenv(ROOT / '.env')
    with GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                              auth=(os.getenv('NEO4J_USER', 'neo4j'),
                                    os.environ['NEO4J_PASSWORD'])) as driver:
        driver.verify_connectivity()
        with driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
            snapshots, ports = session.execute_read(read_graph_snapshot, expected_snapshot)
            snapshot = snapshots[0]
            metadata = validate_snapshot_metadata(snapshots)
            exported_at = datetime.now(timezone.utc)
            rows = build_export_rows(snapshots, ports, exported_at,
                                     verified_snapshot=snapshot)
            from pipelines.graph_metrics.communities import validate_layer
            communities = session.execute_read(validate_layer, snapshot, ports)
            for row in rows:
                community = communities[row['community_id']]
                row.update(community_name=community['community_name'],
                           community_label=community['community_label'])
            ch = ClickHouse()
            try:
                if rows:
                    validate_canonical_ports(ch, rows)
                publish_metric_snapshot(ch, metadata, rows, exported_at,
                    before_insert=lambda: session.execute_read(_check_source_snapshot, snapshot))
            finally:
                ch.session.close()

    summary = dict(run_id=metadata['run_id'], window_start=metadata['window_start'].isoformat(),
                   window_end=metadata['window_end'].isoformat(),
                   snapshot_date=metadata['window_start'].date().isoformat(),
                   ports=len(rows), communities=len({row['community_id'] for row in rows}),
                   min_page_rank=min((row['page_rank'] for row in rows), default=None),
                   max_page_rank=max((row['page_rank'] for row in rows), default=None))
    LOGGER.info('Port graph metrics: run_id=%s window_start=%s window_end=%s ports=%d '
                'communities=%d min_page_rank=%s max_page_rank=%s',
                summary['run_id'], summary['window_start'], summary['window_end'],
                summary['ports'], summary['communities'], summary['min_page_rank'],
                summary['max_page_rank'])
    return summary


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    try:
        export_metrics()
    except Exception as exc:
        # Validation messages are ours; database exceptions may contain server details.
        detail = str(exc) if isinstance(exc, ExportValidationError) else 'verify configuration and database logs'
        LOGGER.error('Port graph metrics export failed (%s): %s', type(exc).__name__, detail)
        raise SystemExit(1) from None
