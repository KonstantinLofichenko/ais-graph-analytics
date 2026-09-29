"""Offline checks for the bounded vessel enrichment command."""
from datetime import date
import os
import subprocess
import sys
import unittest
from unittest.mock import Mock, patch

from pipelines.ai_enrichment import enrich_vessels as enrichment


class VesselEnrichmentTests(unittest.TestCase):
    def test_default_limit_and_environment_override(self):
        for configured, expected in ((None, '100'), ('250', '250')):
            with self.subTest(configured=configured):
                env = os.environ.copy()
                env.pop('AI_ENRICHMENT_LIMIT', None)
                env['PYTHON_DOTENV_DISABLED'] = '1'
                if configured is not None:
                    env['AI_ENRICHMENT_LIMIT'] = configured
                result = subprocess.run(
                    [sys.executable, '-c',
                     'from pipelines.ai_enrichment.enrich_vessels import DEFAULT_LIMIT; '
                     'print(DEFAULT_LIMIT)'],
                    env=env, check=True, capture_output=True, text=True,
                )
                self.assertEqual(result.stdout.strip(), expected)

    def test_limit_selects_new_rows_after_matching_hashes_are_skipped(self):
        vessels = [
            {'mmsi': 1, 'activity_date': date(2026, 9, 22), 'vessel_name': 'One', 'anomaly_rank': 1},
            {'mmsi': 2, 'activity_date': date(2026, 9, 22), 'vessel_name': 'Two', 'anomaly_rank': 2},
            {'mmsi': 3, 'activity_date': date(2026, 9, 22), 'vessel_name': 'Three', 'anomaly_rank': 3},
        ]
        existing = {(1, enrichment.calculate_input_hash(vessels[0]))}
        with patch.object(sys, 'argv', ['enrich_vessels', '--date', '2026-09-22',
                                        '--limit', '1']), \
             patch.object(enrichment, 'get_clickhouse_client', return_value=Mock()), \
             patch.object(enrichment, 'OpenAI', return_value=Mock()), \
             patch.object(enrichment, 'get_vessels', return_value=vessels), \
             patch.object(enrichment, 'get_existing_hashes', return_value=existing), \
             patch.object(enrichment, 'enrich_vessel', return_value=(Mock(), Mock())) as call, \
             patch.object(enrichment, 'insert_enrichment') as insert:
            enrichment.main()
        self.assertEqual(call.call_count, 1)
        self.assertEqual(call.call_args.kwargs['vessel'], vessels[1])
        self.assertEqual(insert.call_count, 1)
        self.assertEqual(insert.call_args.kwargs['vessel'], vessels[1])

    def test_nonpositive_limit_is_rejected(self):
        with patch.object(sys, 'argv', ['enrich_vessels', '--limit', '0']):
            with self.assertRaises(SystemExit) as error:
                enrichment.parse_args()
        self.assertEqual(error.exception.code, 2)


if __name__ == '__main__':
    unittest.main()
