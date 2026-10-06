"""Installed PyFlink window/trigger checks; run inside the Flink image as well."""
import unittest
from pathlib import Path
import sys
from unittest.mock import MagicMock

if __file__ != '<stdin>':
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))

try:
    import pyflink
except ModuleNotFoundError:
    job = None
else:
    from pyflink.common import Time
    from pyflink.datastream.window import SlidingEventTimeWindows, TumblingEventTimeWindows, TriggerResult
    from pyflink.fn_execution.datastream.window.window_operator import WindowOperator
    import ais_vessel_features as job


@unittest.skipIf(job is None, 'Requires installed PyFlink; run in the Flink container with jobs on PYTHONPATH')
class InstalledWindowTests(unittest.TestCase):
    def test_boundary_alignment_slide_and_tumbling_equivalence(self):
        boundary = job.parse_msgtime('2026-10-06T18:10:00Z')
        for minutes in (5, 15, 30, 60):
            assigner = SlidingEventTimeWindows.of(Time.minutes(minutes), Time.minutes(5))
            before = assigner.assign_windows(None, boundary - 1, None)
            at = assigner.assign_windows(None, boundary, None)
            self.assertEqual(len(at), minutes // 5)
            self.assertTrue(all(w.start % 300000 == 0 and w.end % 300000 == 0 for w in at))
            ending = next(w for w in before if w.end == boundary)
            self.assertEqual(ending.start, boundary - minutes * 60000)
            self.assertTrue(all(w.start <= boundary < w.end for w in at))
            self.assertFalse(any(w.end == boundary for w in at))
            later = assigner.assign_windows(None, boundary + 300000, None)
            self.assertEqual([w.start + 300000 for w in at], [w.start for w in later])
        for timestamp in (boundary - 1, boundary, boundary + 1):
            self.assertEqual(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(5)).assign_windows(None, timestamp, None),
                             TumblingEventTimeWindows.of(Time.minutes(5)).assign_windows(None, timestamp, None))

    def test_larger_window_aggregation_out_of_order_and_nulls(self):
        end = job.parse_msgtime('2026-10-06T18:10:00Z')
        # Latest observation arrives first; each 5m pane includes a null speed.
        events = [{'event_timestamp_ms': end - minute * 60000, 'speed': None if minute % 5 == 0 else float(minute),
                   'ship_type': 60, 'navigation_status': minute} for minute in range(1, 61)]
        for minutes in (5, 15, 30, 60):
            assigner = SlidingEventTimeWindows.of(Time.minutes(minutes), Time.minutes(5))
            agg = job.VesselAggregate()
            acc = agg.create_accumulator()
            for event in events:
                if any(w.end == end for w in assigner.assign_windows(event, event['event_timestamp_ms'], None)):
                    acc = agg.add(event, acc)
            result = agg.get_result(acc)
            speeds = [float(i) for i in range(1, minutes + 1) if i % 5]
            self.assertEqual(result['position_count'], minutes)
            self.assertEqual(result['avg_speed'], sum(speeds) / len(speeds))
            self.assertEqual((result['min_speed'], result['max_speed']), (min(speeds), max(speeds)))
            self.assertEqual(result['last_observation']['navigation_status'], 1)

    def test_watermark_trigger_and_zero_allowed_lateness(self):
        assigner = SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5))
        window = next(w for w in assigner.assign_windows(None, 890000, None) if w.end == 900000)
        trigger = assigner.get_default_trigger(None)
        ctx = MagicMock()
        # An out-of-order event remains admissible while its window is open.
        ctx.get_current_watermark.return_value = 870000 - 30000 - 1
        self.assertEqual(trigger.on_element(None, 860000, window, ctx), TriggerResult.CONTINUE)
        ctx.register_event_time_timer.assert_called_once_with(899999)
        self.assertEqual(trigger.on_event_time(899999, window, ctx), TriggerResult.FIRE)
        operator = WindowOperator.__new__(WindowOperator)
        operator.window_assigner = assigner
        operator.allowed_lateness = 0
        operator.internal_timer_service = MagicMock()
        operator.internal_timer_service.current_watermark.return_value = 899998
        self.assertFalse(operator.is_window_late(window))
        operator.internal_timer_service.current_watermark.return_value = 899999
        self.assertTrue(operator.is_window_late(window))
        # The same late event still belongs to longer-lived overlapping windows.
        windows = assigner.assign_windows(None, 860000, None)
        self.assertTrue(any(not operator.is_window_late(w) for w in windows))


if __name__ == '__main__':
    unittest.main()
