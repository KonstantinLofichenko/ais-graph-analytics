"""Community naming and atomic replacement tests; no services required."""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

from pipelines.graph_metrics import communities as c, export, gds
from pipelines.graph_metrics.export import ExportValidationError

RUN = '2026-09-29T00:00:00Z'
SNAPSHOT = dict(run_id=RUN, window_start=RUN, window_end='2026-09-30T00:00:00Z',
                nodes=5, relationships=4, movements=8, generation=2,
                published_at=RUN, graph_state='test-state')


def ports():
    return [dict(port_id=pid, port_name=name, page_rank=score, community_id=cid,
                 visit_run_id=RUN, memberships=[]) for pid, name, score, cid in (
        ('D', 'FOURTH', .2, 6), ('C', 'FLORØ', .5, 6), ('B', 'MÅLØY', 1., 6),
        ('A', 'ÅLESUND', 2., 6), ('E', 'BERGEN', .3, 7))]


class CommunityTests(unittest.TestCase):
    def test_highest_score_and_top_three_names_and_small_community(self):
        groups = c.community_rows(RUN, ports())
        self.assertEqual(groups, [dict(community_id=6, run_id=RUN,
            community_name='ÅLESUND', community_label='ÅLESUND / MÅLØY / FLORØ',
            port_count=4, port_ids=['A', 'B', 'C', 'D']), dict(community_id=7, run_id=RUN,
            community_name='BERGEN', community_label='BERGEN', port_count=1, port_ids=['E'])])

    def test_ties_use_unique_port_key_and_missing_name_falls_back_to_key(self):
        rows = ports()[:3]
        for row in rows:
            row['page_rank'] = 1
        rows[2]['port_name'] = '  '
        expected = c.community_rows(RUN, rows)
        self.assertEqual(c.community_rows(RUN, rows[::-1]), expected)
        self.assertEqual(expected[0]['community_label'], 'B / FLORØ / FOURTH')

    def test_unique_constraint_is_on_community_id_only(self):
        self.assertIn('REQUIRE c.community_id IS UNIQUE', c.CONSTRAINT)
        self.assertNotIn('run_id', c.CONSTRAINT)
        ddl = Path('neo4j/cypher/01_constraints.cypher').read_text()
        self.assertIn(' '.join(c.CONSTRAINT.split()), ' '.join(ddl.split()))

    def test_invalid_memberships_and_stale_metadata_are_rejected(self):
        rows = ports()
        groups = {r['community_id']: r for r in c.community_rows(RUN, rows)}
        for p in rows:
            p['memberships'] = [dict(groups[p['community_id']], is_community=True)]
        c.validate_memberships(RUN, rows)
        for memberships in ([], rows[0]['memberships'] * 2, None):
            invalid = deepcopy(rows)
            invalid[0]['memberships'] = memberships
            with self.assertRaisesRegex(ExportValidationError, 'exactly one'):
                c.validate_memberships(RUN, invalid)
        for key, value in [('run_id', 'old'), ('community_id', 999), ('community_name', 'wrong'),
                           ('community_label', 'wrong'), ('port_count', 100), ('is_community', False)]:
            invalid = deepcopy(rows)
            invalid[0]['memberships'][0][key] = value
            with self.subTest(key=key), self.assertRaisesRegex(ExportValidationError, 'Community metadata'):
                c.validate_memberships(RUN, invalid)


class ReplacementTests(unittest.TestCase):
    def setUp(self):
        self.ports = ports()
        self.nodes = {999: dict(run_id='old')}
        self.events = []
        self.tx = Mock()
        self.tx.run.side_effect = self.query
        self.snapshot = dict(SNAPSHOT)
        self.patch = patch.object(gds, '_read_snapshot', side_effect=lambda tx: dict(self.snapshot))
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def query(self, query, **params):
        self.events.append(query)
        record = None
        if query == export.SNAPSHOT_QUERY:
            record = dict(ports=deepcopy(self.ports))
        elif query == c.CLEAR_MEMBERSHIPS:
            for p in self.ports:
                p['memberships'] = []
        elif query == c.CLEAR_COMMUNITIES:
            self.nodes = {}
        elif query == c.WRITE_QUERY:
            for group in params['communities']:
                self.assertNotIn(group['community_id'], self.nodes)
                self.nodes[group['community_id']] = deepcopy(group)
                for p in self.ports:
                    if p['port_id'] in group['port_ids']:
                        p['memberships'].append(dict(group, is_community=True))
        elif query == c.COUNTS_QUERY:
            record = dict(communities=len(self.nodes),
                          memberships=sum(len(p['memberships']) for p in self.ports))
        else:
            self.assertEqual(query, c.LOCK_QUERY)
        return Mock(single=Mock(return_value=record))

    def test_rerun_replaces_old_state_with_one_community_and_membership_per_key(self):
        expected = dict(SNAPSHOT, communities=2)
        first = c._replace(self.tx, expected)
        original = deepcopy(self.nodes)
        second = c._replace(self.tx, expected)
        self.assertEqual(first, second)
        self.assertEqual(self.nodes, original)
        self.assertEqual(set(self.nodes), {6, 7})
        self.assertTrue(all(len(p['memberships']) == 1 for p in self.ports))
        self.assertLess(self.events.index(c.CLEAR_MEMBERSHIPS), self.events.index(c.CLEAR_COMMUNITIES))
        self.assertLess(self.events.index(c.CLEAR_COMMUNITIES), self.events.index(c.WRITE_QUERY))
        self.assertEqual(self.events[0], c.LOCK_QUERY)

    def test_new_run_removes_previous_communities_including_reused_ids(self):
        c._replace(self.tx, None)
        self.snapshot.update(run_id='2026-09-30T00:00:00Z', window_start='2026-09-30T00:00:00Z',
                             window_end='2026-10-01T00:00:00Z')
        for p in self.ports:
            p.update(visit_run_id=self.snapshot['run_id'], community_id=6)
        c._replace(self.tx, None)
        self.assertEqual(set(self.nodes), {6})
        self.assertEqual(self.nodes[6]['run_id'], self.snapshot['run_id'])
        self.assertEqual(self.nodes[6]['port_count'], 5)

    def test_empty_publication_clears_old_layer(self):
        self.snapshot.update(nodes=0, relationships=0, movements=0)
        self.ports = []
        result = c._replace(self.tx, None)
        self.assertEqual(result, dict(run_id=RUN, communities=0, memberships=0))
        self.assertEqual(self.nodes, {})

    def test_bad_metrics_and_changed_snapshot_fail_before_cleanup(self):
        self.ports[0]['page_rank'] = None
        with self.assertRaises(ExportValidationError):
            c._replace(self.tx, None)
        self.assertNotIn(c.CLEAR_MEMBERSHIPS, self.events)
        self.ports = ports()
        with self.assertRaisesRegex(ExportValidationError, 'graph changed'):
            c._replace(self.tx, dict(SNAPSHOT, generation=1))
        self.assertNotIn(c.CLEAR_MEMBERSHIPS, self.events)

    def test_orphan_community_or_extra_membership_rejected(self):
        c._replace(self.tx, None)
        self.nodes[999] = dict(run_id='old')
        with self.assertRaisesRegex(ExportValidationError, 'counts'):
            c.validate_layer(self.tx, SNAPSHOT, self.ports)

    def test_builder_uses_one_managed_write_transaction(self):
        session = Mock()
        session.execute_write.return_value = dict(run_id=RUN, communities=2, memberships=5)
        with patch.object(gds, '_session') as connect:
            connect.return_value.__enter__.return_value = session
            c.build_communities(SNAPSHOT)
        session.run.assert_called_once_with(c.CONSTRAINT)
        session.execute_write.assert_called_once_with(c._replace, SNAPSHOT)


if __name__ == '__main__':
    unittest.main()
