from datetime import timedelta
import unittest

from pipelines.port_visits.backfill_activity_dates import plan_dates
from pipelines.port_visits.tests import test_snapshot_publish as snapshots
from pipelines.port_visits.tests.test_snapshot_publish import visit, START


class ActivityDatePublishTests(unittest.TestCase):
    def setUp(self):
        self.ch = snapshots.VersionedVisits()

    publish = snapshots.SnapshotPublishTests.publish

    def test_processing_date_is_independent_of_arrival_and_write_time(self):
        row = dict(visit('A'), arrival_at=START - timedelta(days=1))
        self.publish([row])
        self.assertEqual(self.ch.active()[0]['activity_date'], '2026-09-25')
        self.publish([row])
        self.assertEqual(self.ch.active()[0]['activity_date'], '2026-09-25')

    def test_tombstone_preserves_original_activity_date(self):
        self.publish(['A', 'B'])
        self.publish(['A'])
        tombstone = next(r for r in self.ch.logical() if r['visit_id'] == 'B')
        self.assertEqual(tombstone['activity_date'], '2026-09-25')
        self.assertEqual(tombstone['is_deleted'], 1)


class HistoricalActivityDateTests(unittest.TestCase):
    def test_metadata_supplies_date_for_hash_id_and_offset(self):
        rows = [dict(run_id='legacy-hash', visit_id='a', arrival_at='2020-01-01',
                     activity_date=None, is_deleted=deleted) for deleted in (0, 1)]
        plan = plan_dates(rows, [dict(run_id='legacy-hash', window_start='2026-09-25T01:00:00+03:00')])
        self.assertEqual([r['activity_date'] for r in plan['legacy-hash']], ['2026-09-24'] * 2)
        self.assertEqual([r['is_deleted'] for r in plan['legacy-hash']], [0, 1])
        self.assertIsNone(rows[0]['activity_date'])

    def test_correct_dates_make_rerun_a_noop(self):
        rows = [dict(run_id='r', activity_date='2026-09-25')]
        self.assertEqual(plan_dates(rows, [dict(run_id='r', window_start=START.isoformat())]), {})

    def test_missing_metadata_fails_without_guessing(self):
        with self.assertRaisesRegex(RuntimeError, 'Missing completed-run metadata'):
            plan_dates([dict(run_id='2026-09-25T00:00:00Z', activity_date=None)], [])

    def test_conflicting_date_fails(self):
        with self.assertRaisesRegex(RuntimeError, 'conflicts'):
            plan_dates([dict(run_id='r', activity_date='2026-09-26')],
                       [dict(run_id='r', window_start=START.isoformat())])
