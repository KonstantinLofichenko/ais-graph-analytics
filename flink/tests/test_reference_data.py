"""The dbt seeds are the only reference-data source for derived streams."""
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'flink/jobs'))
from common import reference_data


class ReferenceDataTests(unittest.TestCase):
    def setUp(self):
        self.references = reference_data.ReferenceData(ROOT / 'dbt/seeds')

    def test_known_ship_type_and_category(self):
        self.assertEqual(self.references.ship(70), {'ship_type': 70, 'ship_type_name': 'Cargo ship', 'ship_category': 'Cargo'})
        self.assertEqual(self.references.ship(' 30 ')['ship_type_name'], 'Fishing')

    def test_unknown_null_and_boolean_ship_codes(self):
        for code in (None, 999, True):
            self.assertEqual(self.references.ship(code), {'ship_type': code, 'ship_type_name': None, 'ship_category': None})

    def test_navigation_and_defined_sentinels(self):
        self.assertEqual(self.references.navigation_name(5), 'Moored')
        self.assertEqual(self.references.navigation_name(15), 'Not defined')
        self.assertEqual(self.references.ship(0)['ship_type_name'], 'Not available')
        for code in (None, 999, True):
            self.assertIsNone(self.references.navigation_name(code))

    def test_invalid_reference_rows(self):
        for content in ('wrong,name\n1,A\n', 'code,name\n,A\n', 'code,name\n1,A\n 1 ,B\n',
                        'code,name\nabc,A\n', 'code,name\n1\n', 'code,name\n1,A,extra\n'):
            with self.subTest(content=content), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'seed.csv'
                path.write_text(content)
                with self.assertRaises(ValueError):
                    reference_data.load_reference(path, 'code', ('name',))

    def test_values_trimmed_and_empty_labels_null(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'seed.csv'
            path.write_text('code,name\n1, A \n2, \n')
            self.assertEqual(reference_data.load_reference(path, 'code', ('name',)), {'1': {'name':'A'}, '2': {'name':None}})

    def test_lookups_do_not_reopen_files(self):
        with patch.object(reference_data, 'load_reference', wraps=reference_data.load_reference) as load:
            refs = reference_data.ReferenceData(ROOT / 'dbt/seeds')
            self.assertEqual(load.call_count, 2)
            for _ in range(5):
                refs.ship(70)
                refs.navigation_name(5)
            self.assertEqual(load.call_count, 2)


if __name__ == '__main__':
    unittest.main()
