"""Kafka configuration tests without a Flink cluster or Kafka broker."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
CONFIG = {'KAFKA_BOOTSTRAP_SERVERS': 'broker.fixture:19092',
          'KAFKA_TOPIC': 'source.fixture', 'FLINK_SINK_TOPIC': 'sink.fixture'}


class FlinkConfigTests(unittest.TestCase):
    def setUp(self):
        modules = {name: MagicMock() for name in ('pyflink', 'pyflink.common',
                   'pyflink.datastream', 'pyflink.datastream.connectors',
                   'pyflink.datastream.connectors.kafka')}
        spec = importlib.util.spec_from_file_location('smoke_test', ROOT / 'flink/jobs/ais_kafka_smoke.py')
        self.job = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, modules):
            spec.loader.exec_module(self.job)

    def test_each_required_variable_fails_before_flink_execution(self):
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

    def test_source_sink_and_operator_use_supplied_configuration(self):
        with patch.dict(os.environ, CONFIG, clear=True):
            self.job.main()
        source = self.job.KafkaSource.builder.return_value
        source.set_bootstrap_servers.assert_called_once_with(CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        source.set_bootstrap_servers.return_value.set_topics.assert_called_once_with(CONFIG['KAFKA_TOPIC'])
        self.job.KafkaSink.builder.return_value.set_bootstrap_servers.assert_called_once_with(
            CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        self.job.KafkaRecordSerializationSchema.builder.return_value.set_topic.assert_called_once_with(
            CONFIG['FLINK_SINK_TOPIC'])
        env = self.job.StreamExecutionEnvironment.get_execution_environment.return_value
        self.assertEqual(env.from_source.call_args.args[2], CONFIG['KAFKA_TOPIC'])


class KafkaScriptTests(unittest.TestCase):
    def run_script(self, name, config, arguments=()):
        with tempfile.TemporaryDirectory() as directory:
            docker = Path(directory) / 'docker'
            output = Path(directory) / 'args'
            docker.write_text('#!/usr/bin/env python3\nimport json,os,sys\n'
                'if sys.argv[1] == "compose":\n'
                ' print(json.dumps({"services":{"ais-producer":{"environment":json.loads(os.environ["TEST_CONFIG"])}}}))\n'
                'else:\n'
                ' open(os.environ["TEST_ARGS"],"w").write(json.dumps(sys.argv[1:]))\n')
            docker.chmod(0o755)
            env = dict(os.environ, PATH=directory + os.pathsep + os.environ['PATH'],
                       TEST_CONFIG=json.dumps(config), TEST_ARGS=str(output))
            result = subprocess.run(['sh', str(ROOT / 'scripts' / name), *arguments], env=env,
                                    capture_output=True, text=True)
            return result, json.loads(output.read_text()) if output.exists() else None

    def test_topic_creation_uses_compose_configuration(self):
        result, args = self.run_script('create-kafka-topic.sh', CONFIG)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(args[args.index('--topic')+1], CONFIG['KAFKA_TOPIC'])
        self.assertEqual(args[args.index('--bootstrap-server')+1], CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        self.assertIn('--if-not-exists', args)
        self.assertEqual(args[args.index('--partitions')+1], '3')

    def test_explicit_topic_uses_same_creation_mechanism(self):
        result, args = self.run_script('create-kafka-topic.sh', CONFIG, ('enriched.fixture',))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(args[args.index('--topic')+1], 'enriched.fixture')
        self.assertIn('--if-not-exists', args)

    def test_invalid_topic_arguments_fail(self):
        for arguments in (('',), ('one', 'two')):
            result, args = self.run_script('create-kafka-topic.sh', CONFIG, arguments)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Usage:', result.stderr)
            self.assertIsNone(args)

    def test_explicit_topic_deletion_uses_shared_configuration(self):
        result, args = self.run_script('delete-kafka-topic.sh', CONFIG, ('retired.fixture',))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(args[args.index('--topic')+1], 'retired.fixture')
        self.assertEqual(args[args.index('--bootstrap-server')+1], CONFIG['KAFKA_BOOTSTRAP_SERVERS'])
        self.assertIn('--delete', args)
        self.assertIn('--if-exists', args)

    def test_topic_deletion_requires_explicit_non_source_topic(self):
        for arguments in ((), ('',), ('one', 'two'), (CONFIG['KAFKA_TOPIC'],)):
            result, args = self.run_script('delete-kafka-topic.sh', CONFIG, arguments)
            self.assertNotEqual(result.returncode, 0)
            self.assertIsNone(args)

    def test_missing_settings_fail_before_topic_command(self):
        for name in ('KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC'):
            config = dict(CONFIG)
            del config[name]
            result, args = self.run_script('create-kafka-topic.sh', config)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(name + ' is required', result.stderr)
            self.assertIsNone(args)


if __name__ == '__main__':
    unittest.main()
