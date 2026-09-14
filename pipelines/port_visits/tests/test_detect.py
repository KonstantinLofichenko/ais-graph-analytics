from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from detect import detect, summaries

PORT = dict(port_id='TEST:A', name='Test port', country='XX', latitude=60.0, longitude=5.0, radius_m=1500)
BASE = datetime(2026, 9, 1, tzinfo=timezone.utc)


def row(minute, inside=True, speed=0.5, mmsi=123):
    return dict(mmsi=mmsi, msgtime=(BASE+timedelta(minutes=minute)).isoformat(),
                latitude=60.0 if inside else 61.0, longitude=5.0, speed_over_ground=speed)


class DetectionTests(unittest.TestCase):
    def test_two_visits_preserved_and_summarized(self):
        rows = [row(0,False), row(1), row(11), row(21), row(22,False),
                row(30), row(40), row(50), row(51,False)]
        visits, _ = detect(rows, [PORT])
        self.assertEqual(len(visits),2)
        self.assertNotEqual(visits[0]['visit_id'], visits[1]['visit_id'])
        self.assertEqual(summaries(visits), [dict(mmsi=123,port_id='TEST:A',visit_count=2)])
        self.assertEqual(visits[0]['arrival_censored'],0)
        self.assertEqual(visits[0]['departure_at'],BASE+timedelta(minutes=22))

    def test_fast_pass_and_short_stay(self):
        self.assertEqual(detect([row(0,speed=10), row(10,speed=10), row(20,speed=10)], [PORT])[0],[])
        self.assertEqual(detect([row(0),row(5)], [PORT])[0],[])

    def test_gap_does_not_fabricate_dwell(self):
        self.assertEqual(detect([row(0),row(60)], [PORT])[0],[])

    def test_valid_stays_split_by_gap(self):
        visits,_=detect([row(t) for t in (0,10,20,60,70,80)], [PORT])
        self.assertEqual(len(visits),2)
        self.assertEqual(visits[0]['end_reason'],'data_gap')
        self.assertIsNone(visits[0]['departure_at'])
        self.assertEqual(visits[1]['arrival_censored'],1)

    def test_window_censoring(self):
        v=detect([row(t) for t in (0,10,20)], [PORT])[0][0]
        self.assertEqual(v['arrival_censored'],1)
        self.assertEqual(v['end_reason'],'window_end')
        self.assertIsNone(v['departure_at'])

    def test_duplicates_and_order(self):
        rows=[row(t) for t in (0,10,20)]
        first=detect(rows,[PORT])[0]
        second=detect(list(reversed(rows))+rows,[PORT])[0]
        self.assertEqual(first,second)
        self.assertEqual(first[0]['observation_count'],3)

    def test_conflicting_observations_break_stay(self):
        visits,stats=detect([row(0),row(10),row(10,False),row(20)], [PORT])
        self.assertEqual(visits,[])
        self.assertEqual(stats['conflicting_timestamps'],1)

    def test_unknown_speed_breaks_stay(self):
        self.assertEqual(detect([row(0),row(10,speed=None),row(20)],[PORT])[0],[])

    def test_overlap_assigns_once(self):
        other=dict(PORT,port_id='TEST:B')
        visits,_=detect([row(t) for t in (0,10,20)],[other,PORT])
        self.assertEqual(len(visits),1)
        self.assertEqual(visits[0]['port_id'],'TEST:A')

    def test_vessels_never_merged(self):
        visits,_=detect([row(0,mmsi=1),row(10,mmsi=2),row(20,mmsi=3)],[PORT])
        self.assertEqual(visits,[])

    def test_invalid_coordinates_and_thresholds(self):
        bad=dict(row(10),latitude=91)
        self.assertEqual(detect([bad],[PORT])[1]['invalid_rows'],1)
        with self.assertRaises(ValueError):
            detect([], [PORT], min_stay=float('nan'))

    def test_nanosecond_ais_timestamp(self):
        rows=[row(t) for t in (0,10,20)]
        for r in rows:
            r['msgtime']=r['msgtime'].replace('+00:00','.123456789Z')
        visits,stats=detect(rows,[PORT])
        self.assertEqual(len(visits),1)
        self.assertEqual(stats.get('invalid_rows',0),0)
