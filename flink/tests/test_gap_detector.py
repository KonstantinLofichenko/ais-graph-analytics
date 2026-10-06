"""Pure parsing/payload tests and a keyed-state/timer lifecycle test double."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "flink/jobs"))
from common.reference_data import ReferenceData
REFERENCE_DIR = str(ROOT / "dbt/seeds")
CONFIG = {'FLINK_REFERENCE_DIR': REFERENCE_DIR, 'KAFKA_BOOTSTRAP_SERVERS': 'broker.fixture:19092',
          'FLINK_GAP_SOURCE_TOPIC': 'source.fixture', 'FLINK_GAP_TOPIC': 'gaps.fixture',
          'FLINK_GAP_GROUP_ID': 'gap.fixture', 'FLINK_GAP_TIMEOUT_SECONDS': '600'}


class KeyedState:
    """Test-only stand-in for Flink's key-scoped ValueState."""
    def __init__(self):
        self.key = 1
        self.values = {}

    def value(self):
        return self.values.get(self.key)

    def update(self, value):
        self.values[self.key] = value

    def clear(self):
        self.values.pop(self.key, None)


class GapDetectorTests(unittest.TestCase):
    def setUp(self):
        modules = {name: MagicMock() for name in (
            'pyflink', 'pyflink.common', 'pyflink.datastream',
            'pyflink.datastream.functions', 'pyflink.datastream.state',
            'pyflink.datastream.connectors', 'pyflink.datastream.connectors.kafka')}
        modules['pyflink.datastream.functions'].KeyedProcessFunction = type('KeyedProcessFunction', (), {})
        spec = importlib.util.spec_from_file_location('gap_test', ROOT / 'flink/jobs/ais_gap_detector.py')
        self.job = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.job)
        self.references = ReferenceData(REFERENCE_DIR)

    def test_valid_mmsi(self):
        for value in (257039700, '257039700', ' 257039700 ', 1, 999999999):
            event = self.job.parse_event(json.dumps({'mmsi': value}))
            self.assertEqual(event['mmsi'], int(value))
            self.assertIsInstance(self.job.event_key(event), int)

    def test_missing_invalid_mmsi_and_malformed_json_are_dropped(self):
        messages = ['{}', 'null', '[]', '{broken']
        messages += [json.dumps({'mmsi': value}) for value in (
            None, True, False, '', 'None', '123abc', 0, -1, 1000000000, 257039700.0, {}, [])]
        with self.assertLogs(self.job.LOGGER, level='WARNING'):
            for message in messages:
                with self.subTest(message=message):
                    self.assertIsNone(self.job.parse_event(message))

    def test_timeout_validation(self):
        self.assertEqual(self.job.parse_timeout('600'), 600)
        for value in ('', '0', '-1', '1.5', 'NaN', 'abc', ' 60 ', str(2**63)):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'positive integer'):
                self.job.parse_timeout(value)

    def test_payload_and_original_timestamp_preservation(self):
        event = {'mmsi': 257039700, 'name': 'Vessel', 'msgtime': '2026-10-06T12:10:30+03:00',
                 'latitude': 65.9, 'longitude': 12.2}
        state = self.job.vessel_state(event, 1000, 60)
        self.assertEqual(state['timer_timestamp'], 61000)
        self.assertEqual(self.job.gap_payload(state, 62000, 60, self.references), {
            'event_type': 'AIS_GAP_DETECTED', 'mmsi': 257039700, 'name': 'Vessel',
            'last_event_msgtime': event['msgtime'], 'last_latitude': 65.9, 'last_longitude': 12.2,
            'gap_detected_at': '1970-01-01T00:01:02Z', 'gap_timeout_seconds': 60,
            'ship_type': None, 'ship_type_name': None, 'ship_category': None,
            'navigation_status': None, 'navigation_status_name': None})

    def test_missing_and_null_optional_fields(self):
        for event in ({'mmsi': 1}, {'mmsi': 1, 'name': None, 'latitude': None, 'longitude': None, 'msgtime': None}):
            state = self.job.vessel_state(event, 0, 60)
            payload = self.job.gap_payload(state, 60000, 60, self.references)
            for key in ('name', 'last_event_msgtime', 'last_latitude', 'last_longitude'):
                self.assertIsNone(payload[key])

    def process_fixture(self, timeout=600):
        state = KeyedState()
        runtime = MagicMock()
        runtime.get_state.return_value = state
        process = self.job.GapDetector(timeout, REFERENCE_DIR)
        process.open(runtime)
        context = MagicMock()
        return state, process, context, context.timer_service.return_value

    def test_detection_once_recovery_duration_and_new_timer(self):
        state, process, context, timers = self.process_fixture()
        timers.current_processing_time.return_value = 1000
        self.assertEqual(process.process_element({'mmsi': 1}, context), [])
        timers.register_processing_time_timer.assert_called_with(601000)
        timers.delete_processing_time_timer.assert_not_called()
        timers.current_processing_time.return_value = 2000
        previous_time = '2026-10-06T12:00:00+03:00'
        self.assertEqual(process.process_element(
            {'mmsi': 1, 'name': 'Before', 'msgtime': previous_time}, context), [])
        timers.delete_processing_time_timer.assert_called_once_with(601000)
        timers.register_processing_time_timer.assert_called_with(602000)
        self.assertEqual(list(process.on_timer(601000, context)), [])
        timers.current_processing_time.return_value = 603000
        detected = [json.loads(value) for value in process.on_timer(602000, context)]
        self.assertEqual(len(detected), 1)
        self.assertEqual(detected[0]['event_type'], 'AIS_GAP_DETECTED')
        retained = json.loads(state.value())
        self.assertEqual(retained['gap_detected_processing_time'], 603000)
        self.assertIsNone(retained['timer_timestamp'])
        self.assertEqual(retained['last_event_msgtime'], previous_time)
        for timestamp in (602000, 1202000, 1802000):
            self.assertEqual(list(process.on_timer(timestamp, context)), [])
        self.assertEqual(timers.register_processing_time_timer.call_count, 2)

        timers.current_processing_time.return_value = 700500
        # Source event time intentionally moves backwards: duration must use processing time.
        resumed_time = '2026-10-06T08:59:00Z'
        ended = [json.loads(value) for value in process.process_element(
            {'mmsi': 1, 'name': 'Resumed', 'msgtime': resumed_time}, context)]
        self.assertEqual(ended, [{
            'event_type': 'AIS_GAP_ENDED', 'mmsi': 1, 'name': 'Resumed',
            'previous_event_msgtime': previous_time, 'resumed_event_msgtime': resumed_time,
            'gap_detected_at': detected[0]['gap_detected_at'],
            'gap_ended_at': '1970-01-01T00:11:40Z', 'gap_duration_seconds': 698.5,
            'ship_type': None, 'ship_type_name': None, 'ship_category': None,
            'previous_navigation_status': None, 'previous_navigation_status_name': None,
            'resumed_navigation_status': None, 'resumed_navigation_status_name': None}])
        timers.register_processing_time_timer.assert_called_with(1300500)
        self.assertIsNone(json.loads(state.value())['gap_detected_processing_time'])
        self.assertEqual(json.loads(state.value())['last_event_msgtime'], resumed_time)
        # Recovery cancels no null timer and happens only once.
        timers.delete_processing_time_timer.assert_called_once_with(601000)
        self.assertEqual(list(process.on_timer(602000, context)), [])
        timers.current_processing_time.return_value = 701000
        self.assertEqual(process.process_element({'mmsi': 1}, context), [])
        timers.register_processing_time_timer.assert_called_with(1301000)
        timers.current_processing_time.return_value = 1301000
        self.assertEqual(len(list(process.on_timer(1301000, context))), 1)

    def test_recovery_with_missing_optional_fields_and_name_fallback(self):
        for name in (None, 'Earlier name'):
            with self.subTest(name=name):
                state, process, context, timers = self.process_fixture()
                timers.current_processing_time.return_value = 0
                process.process_element({'mmsi': 1, 'name': name}, context)
                timers.current_processing_time.return_value = 600000
                self.assertEqual(len(list(process.on_timer(600000, context))), 1)
                timers.current_processing_time.return_value = 900000
                event = json.loads(process.process_element({'mmsi': 1, 'name': None}, context)[0])
                self.assertEqual(event['name'], name)
                self.assertIsNone(event['previous_event_msgtime'])
                self.assertIsNone(event['resumed_event_msgtime'])
                self.assertEqual(event['gap_duration_seconds'], 900)

    def test_vessel_states_and_recoveries_are_isolated(self):
        state, process, context, timers = self.process_fixture()
        timers.current_processing_time.return_value = 1000
        for key in (1, 2):
            state.key = key
            self.assertEqual(process.process_element({'mmsi': key}, context), [])
        state.key = 1
        timers.current_processing_time.return_value = 601000
        self.assertEqual(len(list(process.on_timer(601000, context))), 1)
        first_gap_state = state.value()
        state.key = 2
        timers.current_processing_time.return_value = 602000
        self.assertEqual(process.process_element({'mmsi': 2}, context), [])
        second_monitoring_state = state.value()
        state.key = 1
        self.assertEqual(state.value(), first_gap_state)
        timers.current_processing_time.return_value = 603000
        self.assertEqual(json.loads(process.process_element({'mmsi': 1}, context)[0])['event_type'], 'AIS_GAP_ENDED')
        state.key = 2
        self.assertEqual(state.value(), second_monitoring_state)

    def test_detected_and_recovery_reference_semantics(self):
        state, process, context, timers = self.process_fixture()
        timers.current_processing_time.return_value = 0
        process.process_element({'mmsi': 1, 'shipType': 70, 'navigationalStatus': 5}, context)
        timers.current_processing_time.return_value = 600000
        detected = json.loads(list(process.on_timer(600000, context))[0])
        self.assertEqual((detected['ship_type'], detected['ship_type_name'], detected['ship_category']), (70, 'Cargo ship', 'Cargo'))
        self.assertEqual((detected['navigation_status'], detected['navigation_status_name']), (5, 'Moored'))
        timers.current_processing_time.return_value = 700000
        ended = json.loads(process.process_element({'mmsi': 1, 'shipType': 80, 'navigationalStatus': 0}, context)[0])
        self.assertEqual((ended['ship_type'], ended['ship_type_name']), (80, 'Tanker'))
        self.assertEqual((ended['previous_navigation_status'], ended['previous_navigation_status_name']), (5, 'Moored'))
        self.assertEqual((ended['resumed_navigation_status'], ended['resumed_navigation_status_name']), (0, 'Under way using engine'))
        self.assertNotIn('navigation_status_name', ended)
        self.assertEqual(ended['gap_duration_seconds'], 700)

    def test_unknown_reference_codes_and_single_load_in_operator(self):
        with patch.object(self.job, 'ReferenceData', wraps=ReferenceData) as load:
            state, process, context, timers = self.process_fixture()
            timers.current_processing_time.return_value = 0
            process.process_element({'mmsi': 1, 'shipType': 999, 'navigationalStatus': 999}, context)
            timers.current_processing_time.return_value = 600000
            detected = json.loads(list(process.on_timer(600000, context))[0])
            self.assertIsNone(detected['ship_type_name'])
            self.assertIsNone(detected['navigation_status_name'])
            timers.current_processing_time.return_value = 700000
            ended = json.loads(process.process_element({'mmsi': 1}, context)[0])
            self.assertIsNone(ended['previous_navigation_status_name'])
            self.assertIsNone(ended['resumed_navigation_status_name'])
            self.assertIsNone(ended['ship_type_name'])
            load.assert_called_once_with(REFERENCE_DIR)

    def test_required_configuration_and_separate_topics(self):
        for key in CONFIG:
            env = dict(CONFIG)
            del env[key]
            with patch.dict(os.environ, env, clear=True):
                with self.assertRaisesRegex(SystemExit, key + ' is required'):
                    self.job.main()
        with patch.dict(os.environ, dict(CONFIG, FLINK_GAP_TOPIC=CONFIG['FLINK_GAP_SOURCE_TOPIC']), clear=True):
            with self.assertRaisesRegex(SystemExit, 'must differ'):
                self.job.main()
        self.job.StreamExecutionEnvironment.get_execution_environment.assert_not_called()

    def test_kafka_configuration_is_environment_driven(self):
        with patch.dict(os.environ, CONFIG, clear=True):
            self.job.main()
        source = self.job.KafkaSource.builder.return_value
        source.set_bootstrap_servers.assert_called_once_with(CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        topics = source.set_bootstrap_servers.return_value.set_topics
        topics.assert_called_once_with(CONFIG['FLINK_GAP_SOURCE_TOPIC'])
        topics.return_value.set_group_id.assert_called_once_with(CONFIG['FLINK_GAP_GROUP_ID'])
        self.job.KafkaRecordSerializationSchema.builder.return_value.set_topic.assert_called_once_with(CONFIG['FLINK_GAP_TOPIC'])


if __name__ == '__main__':
    unittest.main()
