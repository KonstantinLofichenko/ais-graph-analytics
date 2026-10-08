"""Feature parsing and incremental aggregation without a local Flink cluster."""
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
          'FLINK_FEATURE_SOURCE_TOPIC': 'raw.fixture', 'FLINK_FEATURE_TOPIC': 'features.fixture',
          'FLINK_FEATURE_GROUP_ID': 'features.group.fixture', 'FLINK_FEATURE_WINDOWS_MINUTES': '5,15,30,60', 'FLINK_FEATURE_SLIDE_MINUTES': '5',
          'FLINK_FEATURE_WATERMARK_SECONDS': '30', 'FLINK_FEATURE_IDLE_SECONDS': '60'}


class VesselFeatureTests(unittest.TestCase):
    def setUp(self):
        modules = {name: MagicMock() for name in (
            'pyflink', 'pyflink.common', 'pyflink.common.watermark_strategy',
            'pyflink.datastream', 'pyflink.datastream.functions', 'pyflink.datastream.window',
            'pyflink.datastream.connectors', 'pyflink.datastream.connectors.base',
            'pyflink.datastream.checkpoint_config', 'pyflink.datastream.connectors.kafka')}
        for name in ('AggregateFunction', 'ProcessWindowFunction'):
            setattr(modules['pyflink.datastream.functions'], name, type(name, (), {}))
        modules['pyflink.common.watermark_strategy'].TimestampAssigner = type('TimestampAssigner', (), {})
        spec = importlib.util.spec_from_file_location('features_test', ROOT / 'flink/jobs/ais_vessel_features.py')
        self.job = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.job)
        self.job.configure_checkpoints = MagicMock()

    def event(self, **changes):
        return json.dumps({'mmsi': 257039700, 'msgtime': '2026-10-06T12:52:48+00:00', **changes})

    def aggregate(self, speeds):
        function = self.job.VesselAggregate()
        acc = function.create_accumulator()
        for speed in speeds:
            acc = function.add({'speed': speed, 'event_timestamp_ms': 0, 'ship_type': None, 'navigation_status': None}, acc)
        result = function.get_result(acc)
        result.pop('last_observation')
        return result

    def test_name_is_parsed_and_latest_non_null_follows_event_time(self):
        agg = self.job.VesselAggregate()
        acc = agg.create_accumulator()
        # Arrival order differs from event time; the latest observation has no name.
        observations = [
            ('2026-10-06T12:52:30Z', 'Latest named', 4),
            ('2026-10-06T12:52:10Z', 'Older arriving later', 2),
            ('2026-10-06T12:52:40Z', None, 6),
            ('2026-10-06T12:52:20Z', 'Middle', None),
        ]
        for msgtime, name, speed in observations:
            value = self.job.parse_event(self.event(msgtime=msgtime, name=name, speedOverGround=speed))
            self.assertEqual(value['name'], name)
            acc = agg.add(value, acc)
        result = agg.get_result(acc)
        self.assertEqual(result['name'], 'Latest named')
        self.assertEqual((result['position_count'], result['avg_speed'], result['min_speed'], result['max_speed']), (4, 4, 2, 6))

    def test_all_null_and_missing_names_emit_null(self):
        agg = self.job.VesselAggregate()
        acc = agg.create_accumulator()
        for event in (self.event(), self.event(name=None)):
            acc = agg.add(self.job.parse_event(event), acc)
        self.assertIsNone(agg.get_result(acc)['name'])

    def test_name_merge_is_independent_of_latest_reference_observation(self):
        agg = self.job.VesselAggregate()
        def pane(timestamp, name, ship_type):
            return agg.add({'event_timestamp_ms': timestamp, 'name': name, 'speed': None,
                            'ship_type': ship_type, 'navigation_status': None}, agg.create_accumulator())
        named = pane(20, 'Latest named', 70)
        newer_null = pane(30, None, 80)
        old = pane(10, 'Older', 60)
        for merged in (agg.merge(agg.merge(old, named), newer_null), agg.merge(newer_null, agg.merge(named, old))):
            result = agg.get_result(merged)
            self.assertEqual(result['name'], 'Latest named')
            self.assertEqual(result['last_observation']['ship_type'], 80)
        tied = pane(20, '', 70)
        self.assertEqual(agg.get_result(agg.merge(named, tied))['name'], '')
        self.assertEqual(agg.get_result(agg.add({'event_timestamp_ms': 20, 'name': 'Tie', 'speed': None,
                                               'ship_type': None, 'navigation_status': None}, named))['name'], 'Tie')

    def test_msgtime_offset_and_milliseconds(self):
        parse = self.job.parse_msgtime
        self.assertEqual(parse('1970-01-01T00:00:01.123456Z'), 1123)
        self.assertEqual(parse('2026-10-06T12:52:48+00:00'), parse('2026-10-06T15:52:48+03:00'))
        self.assertEqual(parse('2026-10-06T12:52:48Z'), parse('2026-10-06T12:52:48+00:00'))

    def test_invalid_timestamps_are_dropped(self):
        with self.assertLogs(self.job.LOGGER, level='WARNING'):
            for value in (None, '', 1791283338, '2026-10-06', '2026-10-06T12:52:48',
                          '2026-02-30T00:00:00Z', '2026-10-06T25:00:00Z', 'bad'):
                with self.subTest(value=value):
                    self.assertIsNone(self.job.parse_event(self.event(msgtime=value)))
            self.assertIsNone(self.job.parse_event('{"mmsi":257039700}'))

    def test_valid_mmsi_and_timestamp_assignment(self):
        for mmsi in (257039700, '257039700', ' 257039700 ', 1, 999999999):
            event = self.job.parse_event(self.event(mmsi=mmsi))
            self.assertEqual(self.job.event_key(event), int(mmsi))
            self.assertEqual(self.job.AisTimestampAssigner().extract_timestamp(event, -1), event['event_timestamp_ms'])

    def test_invalid_mmsi_and_json(self):
        with self.assertLogs(self.job.LOGGER, level='WARNING'):
            for mmsi in (None, '', True, 0, -1, 1000000000, 123.0, 'None', []):
                self.assertIsNone(self.job.parse_event(self.event(mmsi=mmsi)))
            for value in ('{}', '[]', 'null', '{broken'):
                self.assertIsNone(self.job.parse_event(value))

    def test_null_and_missing_speed_count_positions(self):
        events = [self.job.parse_event(self.event()), self.job.parse_event(self.event(speedOverGround=None)),
                  self.job.parse_event(self.event(speedOverGround=0))]
        self.assertEqual(self.aggregate([event['speed'] for event in events]),
                         {'name': None, 'position_count': 3, 'avg_speed': 0, 'min_speed': 0, 'max_speed': 0})

    def test_speed_aggregate(self):
        self.assertEqual(self.aggregate([2.0, None, 8.0, 5.0, 0.0]),
                         {'name': None, 'position_count': 5, 'avg_speed': 3.75, 'min_speed': 0.0, 'max_speed': 8.0})

    def test_all_null_speed_window(self):
        self.assertEqual(self.aggregate([None, None]),
                         {'name': None, 'position_count': 2, 'avg_speed': None, 'min_speed': None, 'max_speed': None})

    def test_invalid_speed_does_not_discard_position(self):
        with self.assertLogs(self.job.LOGGER, level='WARNING'):
            for value in ('fast', '3.2', True, float('inf'), float('nan'), []):
                event = self.job.parse_event(self.event(speedOverGround=value))
                self.assertIsNotNone(event)
                self.assertIsNone(event['speed'])
        self.assertEqual(self.job.parse_event(self.event(speedOverGround=102.3))['speed'], 102.3)

    def test_accumulator_merge_and_isolation(self):
        function = self.job.VesselAggregate()
        first = function.add({'speed': 4.0, 'event_timestamp_ms': 1, 'ship_type': None, 'navigation_status': None}, function.create_accumulator())
        other = function.add({'speed': None, 'event_timestamp_ms': 2, 'ship_type': None, 'navigation_status': None}, function.create_accumulator())
        merged = function.merge(first, other)
        self.assertEqual({k:v for k,v in function.get_result(merged).items() if k != 'last_observation'}, self.aggregate([4.0, None]))
        self.assertEqual({k:v for k,v in function.get_result(function.merge(other, other)).items() if k != 'last_observation'}, self.aggregate([None, None]))
        self.assertEqual(function.get_result(first)['position_count'], 1)
        self.assertEqual(function.get_result(other)['avg_speed'], None)

    def test_window_output(self):
        start = self.job.parse_msgtime('2026-10-06T12:50:00Z')
        context = MagicMock()
        context.window.return_value.start = start
        context.window.return_value.end = start + 300000
        aggregate = self.aggregate([2.0, None, 4.0])
        function = self.job.FeatureWindow(REFERENCE_DIR, 5)
        function.open(None)
        output = list(function.process(257039700, context, [{**aggregate, 'last_observation': {'ship_type': None, 'navigation_status': None}}]))
        self.assertEqual(len(output), 1)
        self.assertEqual(json.loads(output[0]), {'mmsi': 257039700, 'window_minutes': 5, 'window_start': '2026-10-06T12:50:00Z',
                         'window_end': '2026-10-06T12:55:00Z', **aggregate,
                         'ship_type': None, 'ship_type_name': None, 'ship_category': None,
                         'last_navigation_status': None, 'last_navigation_status_name': None})
        null_output = self.job.format_window(1, 5, start, start + 300000, self.aggregate([None]))
        self.assertIn('"avg_speed": null', null_output)

    def test_reference_attributes_follow_latest_event_time_not_arrival(self):
        aggregate = self.job.VesselAggregate()
        acc = aggregate.create_accumulator()
        values = [
            {'speed': 8.0, 'event_timestamp_ms': 20, 'ship_type': 80, 'navigation_status': 5},
            {'speed': 2.0, 'event_timestamp_ms': 10, 'ship_type': 70, 'navigation_status': 0},
        ]
        for value in values:
            acc = aggregate.add(value, acc)
        result = aggregate.get_result(acc)
        context = MagicMock()
        context.window.return_value.start = 0
        context.window.return_value.end = 300000
        with patch.object(self.job, 'ReferenceData', wraps=ReferenceData) as load:
            window = self.job.FeatureWindow(REFERENCE_DIR, 5)
            window.open(None)
            output = json.loads(list(window.process(1, context, [result]))[0])
            self.assertEqual(output['ship_type_name'], 'Tanker')
            self.assertEqual(output['ship_category'], 'Tanker')
            self.assertEqual(output['last_navigation_status_name'], 'Moored')
            self.assertEqual((output['position_count'], output['avg_speed'], output['min_speed'], output['max_speed']), (2, 5.0, 2.0, 8.0))
            self.assertNotIn('last_observation', output)
            # A later observation with missing codes replaces the earlier known values.
            acc = aggregate.add({'speed': None, 'event_timestamp_ms': 30, 'ship_type': None, 'navigation_status': None}, acc)
            output = json.loads(list(window.process(1, context, [aggregate.get_result(acc)]))[0])
            self.assertIsNone(output['ship_type_name'])
            self.assertIsNone(output['last_navigation_status_name'])
            self.assertEqual(output['avg_speed'], 5.0)
            load.assert_called_once_with(REFERENCE_DIR)

    def test_reference_merge_and_equal_timestamp_tie(self):
        aggregate = self.job.VesselAggregate()
        older = aggregate.add({'speed': 1, 'event_timestamp_ms': 10, 'ship_type': 70, 'navigation_status': 0}, aggregate.create_accumulator())
        newer = aggregate.add({'speed': 3, 'event_timestamp_ms': 20, 'ship_type': 80, 'navigation_status': 5}, aggregate.create_accumulator())
        for merged in (aggregate.merge(older, newer), aggregate.merge(newer, older)):
            self.assertEqual(aggregate.get_result(merged)['last_observation']['navigation_status'], 5)
            self.assertEqual(aggregate.get_result(merged)['avg_speed'], 2)
        tied = aggregate.add({'speed': None, 'event_timestamp_ms': 20, 'ship_type': 999, 'navigation_status': 999}, newer)
        self.assertEqual(aggregate.get_result(tied)['last_observation']['navigation_status'], 999)
        self.assertEqual(aggregate.get_result(aggregate.merge(newer, tied))['last_observation']['ship_type'], 999)

    def test_required_environment_and_numeric_settings(self):
        for key in CONFIG:
            env = dict(CONFIG)
            del env[key]
            with patch.dict(os.environ, env, clear=True), self.assertRaisesRegex(SystemExit, key):
                self.job.main()
        for key in ('FLINK_FEATURE_WATERMARK_SECONDS', 'FLINK_FEATURE_IDLE_SECONDS'):
            for value in ('-1', 'NaN', '0.5', str(2**63)):
                with patch.dict(os.environ, {**CONFIG, key: value}, clear=True), self.assertRaisesRegex(ValueError, key):
                    self.job.main()
        for key in ('FLINK_FEATURE_IDLE_SECONDS',):
            with patch.dict(os.environ, {**CONFIG, key: '0'}, clear=True), self.assertRaisesRegex(ValueError, key):
                self.job.main()
        self.job.StreamExecutionEnvironment.get_execution_environment.assert_not_called()

    def test_window_configuration(self):
        self.assertEqual(self.job.window_settings('5,15,30,60', '5'), ([5, 15, 30, 60], 5))
        self.assertEqual(self.job.window_settings(' 15 , 5 ', '5'), ([15, 5], 5))
        for value in ('', ' ', '5,', ',5', '5,,15', '0', '-5', '5,5', '5,05',
                      '1', '6', '5.0', '+5', 'five', str(2**63)):
            with self.subTest(windows=value), self.assertRaises(ValueError):
                self.job.window_settings(value, '5')
        for slide in ('', '0', '-5', '5.0', 'x', str(2**63)):
            with self.subTest(slide=slide), self.assertRaises(ValueError):
                self.job.window_settings('5,15,30,60', slide)

    def test_each_window_output_and_unchanged_calculations(self):
        for minutes in (5, 15, 30, 60):
            with self.subTest(minutes=minutes):
                output = json.loads(self.job.format_window(1, minutes, 0, minutes * 60000,
                                                         self.aggregate([2.0, None, 8.0, 5.0, 0.0])))
                self.assertEqual(output['window_minutes'], minutes)
                self.assertEqual(output['position_count'], 5)
                self.assertEqual((output['avg_speed'], output['min_speed'], output['max_speed']),
                                 (3.75, 0.0, 8.0))
                function = self.job.FeatureWindow(REFERENCE_DIR, minutes)
                function.open(None)
                context = MagicMock()
                context.window.return_value.start = 0
                context.window.return_value.end = minutes * 60000
                summary = {**self.aggregate([None]),
                           'last_observation': {'ship_type': 60, 'navigation_status': 5}}
                enriched = json.loads(next(function.process(1, context, [summary])))
                self.assertEqual(enriched['window_minutes'], minutes)
                self.assertEqual(enriched['ship_type_name'], 'Passenger ship')
                self.assertEqual(enriched['last_navigation_status_name'], 'Moored')
                self.assertIsNone(enriched['avg_speed'])

    def test_single_configured_window_needs_no_union(self):
        with patch.dict(os.environ, {**CONFIG, 'FLINK_FEATURE_WINDOWS_MINUTES': '12',
                                     'FLINK_FEATURE_SLIDE_MINUTES': '3'}, clear=True):
            self.job.main()
        self.assertEqual([call.args[0] for call in self.job.Time.minutes.call_args_list], [12, 3])
        env = self.job.StreamExecutionEnvironment.get_execution_environment.return_value
        stream = env.from_source.return_value.map.return_value.filter.return_value
        branch = stream.assign_timestamps_and_watermarks.return_value.key_by.return_value.window.return_value.aggregate.return_value
        branch.union.assert_not_called()
        branch.sink_to.assert_called_once()

    def test_configuration_wires_event_time_window_and_idleness(self):
        with patch.dict(os.environ, CONFIG, clear=True):
            self.job.main()
        self.assertEqual([call.args[0] for call in self.job.Time.minutes.call_args_list], [5, 5, 15, 5, 30, 5, 60, 5])
        self.assertEqual([call.args[0] for call in self.job.Duration.of_seconds.call_args_list], [30, 60])
        strategy = self.job.WatermarkStrategy.for_bounded_out_of_orderness.return_value
        strategy.with_idleness.assert_called_once()
        assigner = strategy.with_idleness.return_value.with_timestamp_assigner.call_args.args[0]
        self.assertIsInstance(assigner, self.job.AisTimestampAssigner)
        source = self.job.KafkaSource.builder.return_value.set_bootstrap_servers
        source.assert_called_once_with(CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        source.return_value.set_topics.assert_called_once_with(CONFIG['FLINK_FEATURE_SOURCE_TOPIC'])
        source.return_value.set_topics.return_value.set_group_id.assert_called_once_with(CONFIG['FLINK_FEATURE_GROUP_ID'])
        self.job.KafkaRecordSerializationSchema.builder.return_value.set_topic.assert_called_once_with(CONFIG['FLINK_FEATURE_TOPIC'])
        env = self.job.StreamExecutionEnvironment.get_execution_environment.return_value
        self.job.configure_checkpoints.assert_called_once_with(env, 'features')
        self.job.KafkaSink.builder.return_value.set_bootstrap_servers.return_value.set_delivery_guarantee.assert_called_once_with(self.job.DeliveryGuarantee.AT_LEAST_ONCE)
        stream = env.from_source.return_value.map.return_value.filter.return_value
        stream.assign_timestamps_and_watermarks.assert_called_once()
        window = stream.assign_timestamps_and_watermarks.return_value.key_by.return_value.window
        self.assertEqual(window.call_count, 4)
        window.assert_called_with(self.job.SlidingEventTimeWindows.of.return_value)
        branch = window.return_value.aggregate.return_value
        branch.union.assert_called_once_with(branch, branch, branch)
        branch.union.return_value.sink_to.assert_called_once()
        window.return_value.allowed_lateness.assert_not_called()
        args = window.return_value.aggregate.call_args.args
        self.assertIsInstance(args[0], self.job.VesselAggregate)
        self.assertIsInstance(args[1], self.job.FeatureWindow)

    def test_rejects_same_source_sink(self):
        with patch.dict(os.environ, {**CONFIG, 'FLINK_FEATURE_TOPIC': CONFIG['FLINK_FEATURE_SOURCE_TOPIC']}, clear=True):
            with self.assertRaisesRegex(SystemExit, 'must differ'):
                self.job.main()
        self.job.StreamExecutionEnvironment.get_execution_environment.assert_not_called()


if __name__ == '__main__':
    unittest.main()
