"""Spark tests for sentinel boundaries, precision, mappings and row preservation."""
import unittest
from datetime import datetime
from decimal import Decimal

from curate_hais import transform
from spark_session import create_spark

SCHEMA = """date_time_utc timestamp, mmsi int, longitude double, latitude double,
status int, course_over_ground decimal(6,2), true_heading int,
speed_over_ground decimal(6,2), rate_of_turn int, maneuvre int,
data_source string, ais_class string, msg_type int, geometry binary"""


class CuratedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("HAIS curated transformation tests")
        cls.spark.conf.set("spark.sql.ansi.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def frame(self, optional):
        rows = [(datetime(2026, 9, 1, 0, 0), 257000001, 5.1234567, 60.1234567,
                 15, cog, heading, speed, turn, 2, 'HAIS', 'A', 18, b'\x00\x01')
                for cog, heading, speed, turn in optional]
        return self.spark.createDataFrame(rows, SCHEMA)

    def test_sentinels_and_all_course_outliers_preserve_rows(self):
        rows = transform(self.frame([
            (Decimal(str(cog)), 511, Decimal('102.30'), -128)
            for cog in [360, 448, 471, 511, -1]
        ])).collect()
        self.assertEqual(len(rows), 5)
        for row in rows:
            for name in ['course_over_ground', 'true_heading', 'speed_over_ground', 'rate_of_turn']:
                self.assertIsNone(row[name])

    def test_valid_boundaries_mapping_precision_and_binary(self):
        rows = transform(self.frame([
            (Decimal('0'), 0, Decimal('0'), -127),
            (Decimal('359.99'), 512, Decimal('102.31'), 127),
        ])).collect()
        self.assertEqual(rows[1].course_over_ground, Decimal('359.99'))
        self.assertEqual(rows[1].true_heading, 512)  # No extra heading range rule.
        self.assertEqual(rows[1].speed_over_ground, Decimal('102.31'))
        self.assertEqual(rows[0].rate_of_turn, -127)
        self.assertEqual(rows[1].rate_of_turn, 127)
        self.assertEqual(rows[0].longitude, Decimal('5.123457'))
        self.assertEqual(rows[0].latitude, Decimal('60.123457'))
        self.assertEqual(rows[0].navigational_status, 15)
        self.assertEqual(rows[0].maneuver, 2)
        self.assertEqual(rows[0].message_type, 18)
        self.assertEqual(rows[0].geometry, b'\x00\x01')
        self.assertEqual(rows[0].date_time, datetime(2026, 9, 1))
        self.assertEqual(len(rows[0]), 14)

    def test_existing_optional_nulls_preserved(self):
        row = transform(self.frame([(None, None, None, None)])).first()
        self.assertTrue(all(row[n] is None for n in [
            'course_over_ground', 'true_heading', 'speed_over_ground', 'rate_of_turn']))


if __name__ == '__main__':
    unittest.main(verbosity=2)
