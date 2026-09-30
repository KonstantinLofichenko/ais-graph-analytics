from datetime import timedelta
import unittest

from pipelines.graph_metrics.backfill_snapshot_dates import plan_dates
from pipelines.graph_metrics.tests import test_tombstones as snapshots


class SnapshotDateTests(unittest.TestCase):
    def test_backfill_preserves_tombstone_and_nullable_labels(self):
        row = dict(run_id='legacy-hash', window_start='2026-09-25T00:00:00Z',
                   snapshot_date='2026-09-26', is_deleted=1, community_name=None)
        result = plan_dates([row])['legacy-hash'][0]
        self.assertEqual(result, dict(row, snapshot_date='2026-09-25'))
        self.assertEqual(plan_dates([result]), {})

    def test_cross_partition_change_fails_before_publication(self):
        with self.assertRaisesRegex(RuntimeError, 'cross monthly partitions'):
            plan_dates([dict(run_id='r', window_start='2026-09-30T00:00:00Z', snapshot_date='2026-10-01')])

    def test_corrected_date_rerun_ignores_superseded_physical_date(self):
        ch = snapshots.VersionedMetrics()
        row = snapshots.metric('A')
        ch.history.extend([dict(row, snapshot_date='2026-09-26', is_deleted=0),
                           dict(row, is_deleted=0, exported_at=snapshots.NOW + timedelta(seconds=1))])
        snapshots.publish_metric_snapshot(ch, snapshots.META, [row], snapshots.NOW,
                                           before_insert=lambda: None)
        self.assertEqual(len(ch.active(snapshots.META['run_id'])), 1)
        self.assertEqual(ch.active(snapshots.META['run_id'])[0]['snapshot_date'], '2026-09-25')
