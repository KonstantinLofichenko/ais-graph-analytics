"""Durable state configuration shared by the two AIS DataStream jobs."""
import os
from urllib.parse import urlparse


def settings():
    values = {}
    for name, default in (
        ('FLINK_CHECKPOINT_INTERVAL_SECONDS', 60),
        ('FLINK_CHECKPOINT_TIMEOUT_SECONDS', 120),
        ('FLINK_CHECKPOINT_MIN_PAUSE_SECONDS', 10),
        ('FLINK_RESTART_ATTEMPTS', 10),
        ('FLINK_RESTART_DELAY_SECONDS', 10),
    ):
        value = os.environ.get(name, str(default))
        if not value.isdecimal() or int(value) <= 0:
            raise ValueError(name + ' must be a positive integer')
        values[name] = int(value)
    directory = os.environ.get('FLINK_CHECKPOINT_DIR', 'file:///opt/flink/checkpoints')
    parsed = urlparse(directory)
    if parsed.scheme != 'file' or parsed.netloc or not parsed.path.startswith('/'):
        raise ValueError('FLINK_CHECKPOINT_DIR must be an absolute local file URI shared by all Flink containers')
    values['FLINK_CHECKPOINT_DIR'] = directory
    return values


def configure_checkpoints(env, job_key):
    # Lazy imports keep configuration validation usable without installed PyFlink.
    from pyflink.common import Configuration
    from pyflink.datastream import CheckpointingMode
    from pyflink.datastream.checkpoint_config import ExternalizedCheckpointRetention
    values = settings()
    configuration = Configuration()
    configuration.set_string('execution.checkpointing.storage', 'filesystem')
    configuration.set_string('execution.checkpointing.dir', values['FLINK_CHECKPOINT_DIR'].rstrip('/') + '/' + job_key)
    configuration.set_string('restart-strategy.type', 'fixed-delay')
    configuration.set_string('restart-strategy.fixed-delay.attempts', str(values['FLINK_RESTART_ATTEMPTS']))
    configuration.set_string('restart-strategy.fixed-delay.delay', str(values['FLINK_RESTART_DELAY_SECONDS']) + ' s')
    env.configure(configuration)
    env.enable_checkpointing(values['FLINK_CHECKPOINT_INTERVAL_SECONDS'] * 1000,
                             CheckpointingMode.EXACTLY_ONCE)
    checkpoints = env.get_checkpoint_config()
    checkpoints.set_checkpoint_timeout(values['FLINK_CHECKPOINT_TIMEOUT_SECONDS'] * 1000)
    checkpoints.set_min_pause_between_checkpoints(values['FLINK_CHECKPOINT_MIN_PAUSE_SECONDS'] * 1000)
    checkpoints.set_max_concurrent_checkpoints(1)
    checkpoints.set_externalized_checkpoint_retention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)


def with_uid(stream, uid):
    """Keep checkpoint operator identities stable across compatible submissions."""
    stream.uid(uid)
    return stream
