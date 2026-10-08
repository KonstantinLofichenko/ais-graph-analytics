"""Validate defaults, storage isolation and installed checkpoint configuration."""
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs'))
from common.checkpointing import settings, configure_checkpoints


class CheckpointTests(unittest.TestCase):
    def test_defaults_and_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            defaults = settings()
            self.assertEqual(defaults['FLINK_CHECKPOINT_INTERVAL_SECONDS'], 60)
            self.assertEqual(defaults['FLINK_CHECKPOINT_TIMEOUT_SECONDS'], 120)
            self.assertEqual(defaults['FLINK_CHECKPOINT_MIN_PAUSE_SECONDS'], 10)
            self.assertEqual(defaults['FLINK_CHECKPOINT_DIR'], 'file:///opt/flink/checkpoints')
            self.assertEqual(defaults['FLINK_RESTART_ATTEMPTS'], 10)
            self.assertEqual(defaults['FLINK_RESTART_DELAY_SECONDS'], 10)
        with patch.dict(os.environ, {'FLINK_CHECKPOINT_INTERVAL_SECONDS': '15', 'FLINK_CHECKPOINT_DIR': 'file:///shared/state'}, clear=True):
            self.assertEqual(settings()['FLINK_CHECKPOINT_INTERVAL_SECONDS'], 15)
            self.assertEqual(settings()['FLINK_CHECKPOINT_DIR'], 'file:///shared/state')

    def test_invalid_settings_fail(self):
        for value in ('0', '-1', '1.2', ''):
            with patch.dict(os.environ, {'FLINK_CHECKPOINT_INTERVAL_SECONDS': value}, clear=True), self.assertRaises(ValueError):
                settings()
        for value in ('relative/path', 'file://remote/path', 's3://bucket/path'):
            with patch.dict(os.environ, {'FLINK_CHECKPOINT_DIR': value}, clear=True), self.assertRaises(ValueError):
                settings()

    def test_configuration_is_exactly_once_and_retained(self):
        common, stream, config = MagicMock(), MagicMock(), MagicMock()
        modules = {'pyflink.common': common, 'pyflink.datastream': stream, 'pyflink.datastream.checkpoint_config': config}
        env = MagicMock()
        with patch.dict(sys.modules, modules), patch.dict(os.environ, {}, clear=True):
            configure_checkpoints(env, 'gap')
        common.Configuration.return_value.set_string.assert_any_call('execution.checkpointing.dir', 'file:///opt/flink/checkpoints/gap')
        env.enable_checkpointing.assert_called_once_with(60000, stream.CheckpointingMode.EXACTLY_ONCE)
        checkpoints = env.get_checkpoint_config.return_value
        checkpoints.set_checkpoint_timeout.assert_called_once_with(120000)
        checkpoints.set_min_pause_between_checkpoints.assert_called_once_with(10000)
        checkpoints.set_max_concurrent_checkpoints.assert_called_once_with(1)
        checkpoints.set_externalized_checkpoint_retention.assert_called_once_with(config.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
        common.Configuration.return_value.set_string.assert_any_call('restart-strategy.type', 'fixed-delay')
        common.Configuration.return_value.set_string.assert_any_call('restart-strategy.fixed-delay.attempts', '10')
        common.Configuration.return_value.set_string.assert_any_call('restart-strategy.fixed-delay.delay', '10 s')
