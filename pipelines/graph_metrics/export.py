#!/usr/bin/env python3
"""Export existing Neo4j Port metrics for the current managed connection snapshot."""
from datetime import datetime, timezone
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

from pipelines.port_visits.run import ClickHouse, ROOT

OWNER = 'port-connections-v1'
LOGGER = logging.getLogger(__name__)
UINT64_MAX = 2**64 - 1

# Metadata and endpoint membership come from one relationship scan. Collect nodes
# before projecting their properties so duplicate portId values remain detectable.
SNAPSHOT_QUERY = '''
    MATCH (source:Port)-[r:CONNECTED_TO {managedBy: $owner}]->(target:Port)
    WITH collect(DISTINCT {run_id: r.runId, window_start: r.windowStart,
                           window_end: r.windowEnd}) AS snapshots,
         collect(DISTINCT source) + collect(DISTINCT target) AS endpoints
    UNWIND endpoints AS port
    WITH snapshots, collect(DISTINCT port) AS ports
    RETURN snapshots,
           [port IN ports | {port_id: port.portId, page_rank: port.pageRank,
                             community_id: port.communityId}] AS ports
'''


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


def build_export_rows(snapshots, ports, exported_at):
    """Validate all active endpoint metrics before constructing a complete export."""
    metadata = validate_snapshot_metadata(snapshots)
    if not ports:
        raise ExportValidationError('Managed CONNECTED_TO snapshot has no active endpoint Ports')
    if not isinstance(exported_at, datetime) or exported_at.tzinfo is None:
        raise ExportValidationError('exported_at must be a timezone-aware datetime')
    exported_at = exported_at.astimezone(timezone.utc)
    rows = []
    seen = set()
    for port in ports:
        port_id = port.get('port_id')
        if not isinstance(port_id, str) or not port_id.strip():
            raise ExportValidationError('Every active Port must have a nonempty portId')
        if port_id in seen:
            raise ExportValidationError(f'Duplicate active Port portId: {port_id}')
        seen.add(port_id)
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
        rows.append(dict(metadata, snapshot_date=metadata['window_end'].date().isoformat(),
                         port_id=port_id, page_rank=page_rank, community_id=community_id,
                         exported_at=exported_at))
    return sorted(rows, key=lambda row: row['port_id'])


def read_graph_snapshot(tx):
    record = tx.run(SNAPSHOT_QUERY, owner=OWNER).single()
    if record is None:
        raise ExportValidationError('No managed CONNECTED_TO snapshot is available to export')
    return record['snapshots'], record['ports']


def validate_canonical_ports(ch, rows):
    data = ch.query('SELECT port_id FROM analytics.ports FINAL FORMAT JSONEachRow')
    known = {json.loads(line)['port_id'] for line in data.splitlines() if line.strip()}
    missing = sorted({row['port_id'] for row in rows} - known)
    if missing:
        raise ExportValidationError('Active Neo4j portId values missing from analytics.ports: '
                                    + ', '.join(missing))


def export_metrics():
    """Read existing graph metrics and insert one validated, retry-stable snapshot."""
    load_dotenv(ROOT / '.env')
    with GraphDatabase.driver(os.getenv('NEO4J_URI', 'bolt://localhost:7687'),
                              auth=(os.getenv('NEO4J_USER', 'neo4j'),
                                    os.environ['NEO4J_PASSWORD'])) as driver:
        driver.verify_connectivity()
        with driver.session(database=os.getenv('NEO4J_DATABASE', 'neo4j')) as session:
            snapshots, ports = session.execute_read(read_graph_snapshot)
    rows = build_export_rows(snapshots, ports, datetime.now(timezone.utc))
    ch = ClickHouse()
    try:
        validate_canonical_ports(ch, rows)
        ch.insert('port_graph_metrics', rows)
    finally:
        ch.session.close()

    first = rows[0]
    summary = dict(run_id=first['run_id'], window_start=first['window_start'].isoformat(),
                   window_end=first['window_end'].isoformat(), snapshot_date=first['snapshot_date'],
                   ports=len(rows), communities=len({row['community_id'] for row in rows}),
                   min_page_rank=min(row['page_rank'] for row in rows),
                   max_page_rank=max(row['page_rank'] for row in rows))
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
