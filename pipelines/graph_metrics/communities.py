"""Replace the current community layer from validated, already-written GDS metrics."""
from datetime import datetime, timezone
import logging

from pipelines.graph_metrics import export as metrics_export
from pipelines.graph_metrics.export import ExportValidationError, OWNER

LOGGER = logging.getLogger(__name__)
CONSTRAINT = '''CREATE CONSTRAINT community_id_unique IF NOT EXISTS
    FOR (c:Community) REQUIRE c.community_id IS UNIQUE'''
# The publisher takes a write lock on this same node before replacing connections.
# This dependent assignment locks it without advancing its publication generation.
LOCK_QUERY = '''MATCH (s:ConnectionSnapshot {managedBy: $owner})
    SET s.generation = s.generation RETURN s.generation AS generation'''
CLEAR_MEMBERSHIPS = 'MATCH ()-[r:MEMBER_OF]->() DELETE r'
CLEAR_COMMUNITIES = 'MATCH (c:Community) DETACH DELETE c'
WRITE_QUERY = '''UNWIND $communities AS row
    CREATE (c:Community)
    SET c.community_id=row.community_id, c.run_id=$run_id,
        c.community_name=row.community_name, c.community_label=row.community_label,
        c.port_count=row.port_count
    WITH c, row UNWIND row.port_ids AS port_id
    MATCH (p:Port {portId: port_id})
    MERGE (p)-[:MEMBER_OF]->(c)'''
COUNTS_QUERY = '''
    CALL () { MATCH (c:Community) RETURN count(c) AS communities }
    CALL () { MATCH ()-[r:MEMBER_OF]->() RETURN count(r) AS memberships }
    RETURN communities, memberships'''


def community_rows(run_id, ports):
    """Group validated active ports; portId breaks score ties independently of names."""
    groups = {}
    for port in sorted(ports, key=lambda p: (-p['page_rank'], p['port_id'])):
        groups.setdefault(port['community_id'], []).append(port)
    result = []
    for community_id, members in sorted(groups.items()):
        names = [p['port_name'].strip() if isinstance(p.get('port_name'), str)
                 and p['port_name'].strip() else p['port_id'] for p in members[:3]]
        result.append(dict(community_id=community_id, run_id=run_id,
                           community_name=names[0], community_label=' / '.join(names),
                           port_count=len(members), port_ids=[p['port_id'] for p in members]))
    return result


def validate_memberships(run_id, ports):
    """Require exactly one matching current membership, including deterministic labels."""
    expected = {c['community_id']: c for c in community_rows(run_id, ports)}
    for port in ports:
        memberships = port.get('memberships')
        if not isinstance(memberships, list) or len(memberships) != 1:
            raise ExportValidationError(f'Port {port["port_id"]}: exactly one MEMBER_OF is required')
        actual = memberships[0]
        community = expected[port['community_id']]
        if (not actual.get('is_community') or any(actual.get(k) != community[k] for k in
                ('community_id', 'run_id', 'community_name', 'community_label', 'port_count'))):
            raise ExportValidationError(f'Port {port["port_id"]}: stale or inconsistent Community metadata')
    return expected


def validate_layer(tx, snapshot, ports):
    communities = validate_memberships(snapshot['run_id'], ports)
    counts = tx.run(COUNTS_QUERY).single()
    if (counts is None or counts['communities'] != len(communities)
            or counts['memberships'] != len(ports)):
        raise ExportValidationError('Community layer counts differ from active Port memberships')
    return communities


def _replace(tx, expected):
    from pipelines.graph_metrics.gds import _assert_snapshot, _read_snapshot
    tx.run(LOCK_QUERY, owner=OWNER).consume()
    snapshot = _read_snapshot(tx)
    if expected is not None:
        _assert_snapshot(expected, snapshot)
    ports = tx.run(metrics_export.SNAPSHOT_QUERY, owner=OWNER).single()['ports']
    # Validate metrics and visit lineage before deleting anything. Old memberships
    # are intentionally ignored until the replacement is built in this transaction.
    metrics_export.build_export_rows([snapshot], ports, datetime.now(timezone.utc),
                                     verified_snapshot=snapshot)
    communities = community_rows(snapshot['run_id'], ports)
    if expected is not None and len(communities) != expected['communities']:
        raise ExportValidationError('Community count differs from GDS write-back')
    _assert_snapshot(snapshot, _read_snapshot(tx))
    tx.run(CLEAR_MEMBERSHIPS).consume()
    tx.run(CLEAR_COMMUNITIES).consume()
    tx.run(WRITE_QUERY, communities=communities, run_id=snapshot['run_id']).consume()
    written = tx.run(metrics_export.SNAPSHOT_QUERY, owner=OWNER).single()['ports']
    validate_layer(tx, snapshot, written)
    _assert_snapshot(snapshot, _read_snapshot(tx))
    return dict(run_id=snapshot['run_id'], communities=len(communities), memberships=len(ports))


def build_communities(expected_snapshot=None):
    """Atomic replacement, also usable after the manual GDS Cypher write-back."""
    from pipelines.graph_metrics.gds import _session
    with _session() as session:
        session.run(CONSTRAINT).consume()
        summary = session.execute_write(_replace, expected_snapshot)
    LOGGER.info('Community layer: run_id=%s communities=%d memberships=%d',
                summary['run_id'], summary['communities'], summary['memberships'])
    return summary


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    build_communities()
