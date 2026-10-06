"""Exercise the real seed lookups without a local PyFlink installation."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
CONFIG = {
    'KAFKA_BOOTSTRAP_SERVERS': 'broker.fixture:19092',
    'KAFKA_TOPIC': 'source.fixture',
    'FLINK_ENRICHED_TOPIC': 'enriched.fixture',
    'FLINK_REFERENCE_DIR': str(ROOT / 'dbt/seeds'),
    'FLINK_ENRICHMENT_GROUP_ID': 'enrichment.fixture',
}


class ReferenceEnrichmentTests(unittest.TestCase):
    def setUp(self):
        modules = {name: MagicMock() for name in (
            'pyflink', 'pyflink.common', 'pyflink.datastream',
            'pyflink.datastream.functions', 'pyflink.datastream.connectors',
            'pyflink.datastream.connectors.kafka')}
        modules['pyflink.datastream.functions'].MapFunction = type('MapFunction', (), {})
        spec = importlib.util.spec_from_file_location(
            'enrichment_test', ROOT / 'flink/jobs/ais_reference_enrichment.py')
        self.job = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.job)
        self.operator = self.job.ReferenceEnrichment(CONFIG['FLINK_REFERENCE_DIR'])
        self.operator.open(None)

    def enrich(self, **fields):
        return json.loads(self.operator.map(json.dumps(fields)))

    def test_known_ship_type_and_existing_category(self):
        enriched = self.enrich(shipType=70)
        self.assertEqual(enriched['ship_type_name'], 'Cargo ship')
        self.assertEqual(enriched['ship_category'], 'Cargo')

    def test_unknown_or_null_ship_type_preserves_value(self):
        for code in (999, None, True):
            with self.subTest(code=code):
                enriched = self.enrich(shipType=code)
                self.assertEqual(enriched['shipType'], code)
                self.assertIsNone(enriched['ship_type_name'])
                self.assertIsNone(enriched['ship_category'])

    def test_known_navigation_status(self):
        self.assertEqual(self.enrich(navigationalStatus=5)['navigation_status_name'], 'Moored')

    def test_unknown_null_and_missing_navigation_status(self):
        for fields in ({'navigationalStatus': 999}, {'navigationalStatus': None}, {}):
            enriched = self.enrich(**fields)
            self.assertIsNone(enriched['navigation_status_name'])
            for key, value in fields.items():
                self.assertEqual(enriched[key], value)

    def test_defined_sentinel_labels_are_preserved(self):
        enriched = self.enrich(shipType=0, navigationalStatus=15)
        self.assertEqual(enriched['ship_type_name'], 'Not available')
        self.assertEqual(enriched['ship_category'], 'Unknown')
        self.assertEqual(enriched['navigation_status_name'], 'Not defined')

    def test_preserves_every_original_field_without_mutation(self):
        event = {'mmsi': 123456789, 'shipType': 30, 'navigationalStatus': 7,
                 'msgtime': '2026-10-06T00:00:00Z', 'latitude': 60.1,
                 'name': None, 'extra': {'values': [1, 'two', None]}}
        before = json.dumps(event)
        enriched = self.job.enrich_event(
            event, self.operator.ship_types, self.operator.navigation_statuses)
        self.assertEqual(json.dumps(event), before)
        self.assertEqual({key: enriched[key] for key in event}, event)
        self.assertEqual(set(enriched) - set(event), {
            'ship_type_name', 'ship_category', 'navigation_status_name'})

    def test_malformed_json_raises_as_in_smoke_job(self):
        with self.assertRaises(json.JSONDecodeError):
            self.operator.map('{broken')
        with self.assertRaisesRegex(ValueError, 'must be an object'):
            self.operator.map('[]')

    def test_csvs_loaded_only_on_initialization(self):
        with patch.object(self.job, 'load_reference', wraps=self.job.load_reference) as load:
            operator = self.job.ReferenceEnrichment(CONFIG['FLINK_REFERENCE_DIR'])
            operator.open(None)
            self.assertEqual(load.call_count, 2)
            operator.map('{"shipType": 70}')
            operator.map('{"shipType": 30}')
            self.assertEqual(load.call_count, 2)

    def test_required_configuration_fails_before_submission(self):
        for name in CONFIG:
            for value in (None, '', '   '):
                env = dict(CONFIG)
                if value is None:
                    del env[name]
                else:
                    env[name] = value
                with self.subTest(name=name, value=value), patch.dict(os.environ, env, clear=True):
                    with self.assertRaisesRegex(SystemExit, name + ' is required'):
                        self.job.main()
        self.job.StreamExecutionEnvironment.get_execution_environment.assert_not_called()

    def test_configuration_and_separate_consumer_group(self):
        with patch.dict(os.environ, CONFIG, clear=True):
            self.job.main()
        source = self.job.KafkaSource.builder.return_value
        source.set_bootstrap_servers.assert_called_once_with(CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        topics = source.set_bootstrap_servers.return_value.set_topics
        topics.assert_called_once_with(CONFIG['KAFKA_TOPIC'])
        topics.return_value.set_group_id.assert_called_once_with(CONFIG['FLINK_ENRICHMENT_GROUP_ID'])
        self.job.KafkaRecordSerializationSchema.builder.return_value.set_topic.assert_called_once_with(
            CONFIG['FLINK_ENRICHED_TOPIC'])

    def test_rejects_feedback_loop(self):
        env = dict(CONFIG, FLINK_ENRICHED_TOPIC=CONFIG['KAFKA_TOPIC'])
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(SystemExit, 'must differ'):
                self.job.main()
        self.job.StreamExecutionEnvironment.get_execution_environment.assert_not_called()


if __name__ == '__main__':
    unittest.main()
