"""One-shot, serialized reconciliation of the two derived AIS jobs."""
import fcntl
import json
import logging
import os
from pathlib import Path
import socket
import struct
import subprocess
import time
import urllib.request
import urllib.parse
import uuid

LOG = logging.getLogger('flink-job-submitter')
TERMINAL = {'FAILED', 'CANCELED', 'FINISHED', 'SUSPENDED'}
JOBS = (
    ('gap', 'AIS vessel gap detector', 'ais_gap_detector.py', 'FLINK_GAP_SOURCE_TOPIC'),
    ('features', 'AIS vessel multi-window event-time features', 'ais_vessel_features.py', 'FLINK_FEATURE_SOURCE_TOPIC'),
)


def rest(path):
    with urllib.request.urlopen(os.environ.get('FLINK_REST_URL', 'http://flink-jobmanager:8081') + path, timeout=5) as response:
        return json.load(response)


def kafka_ready(brokers):
    # Kafka ApiVersions v0 handshake (same protocol-level readiness as broker
    # healthcheck), using only stdlib so the existing Flink image is sufficient.
    errors = []
    for broker in brokers.split(','):
        host, port = broker.strip().rsplit(':', 1)
        try:
            with socket.create_connection((host, int(port)), timeout=5) as connection:
                request = struct.pack('>hhih', 18, 0, 1, 0)
                connection.sendall(struct.pack('>i', len(request)) + request)
                def receive(size):
                    data = b''
                    while len(data) < size:
                        chunk = connection.recv(size - len(data))
                        if not chunk:
                            raise ConnectionError('Kafka closed connection')
                        data += chunk
                    return data
                length = struct.unpack('>i', receive(4))[0]
                if length < 6 or length > 1024 * 1024:
                    raise RuntimeError('Invalid Kafka response size')
                response = receive(length)
                correlation, error = struct.unpack('>ih', response[:6])
                if correlation != 1 or error != 0:
                    raise RuntimeError('Kafka ApiVersions handshake failed')
                return
        except (OSError, ValueError, RuntimeError) as error:
            errors.append(str(error))
    raise RuntimeError('No configured Kafka broker ready: ' + '; '.join(errors))


def legacy_matches(job, key, name, source):
    if job['name'] != name:
        return False
    nodes = rest('/jobs/' + job['jid'] + '/plan')['plan']['nodes']
    descriptions = [node['description'] for node in nodes]
    if not any('Source: ' + source + '<br/>' in d for d in descriptions):
        return False
    if key == 'gap':
        return len(nodes) == 2 and any('KEYED PROCESS' in d for d in descriptions)
    return any('VesselAggregate, FeatureWindow' in d for d in descriptions)


def resolve(jobs, registry, key, name, source):
    # Job ID is authoritative, including INITIALIZING/CREATED/RESTARTING states.
    active = [j for j in jobs if j['state'] not in TERMINAL]
    known = [j for j in active if j['jid'] == registry.get(key)]
    legacy = [j for j in active if j not in known and legacy_matches(j, key, name, source)]
    matches = known + legacy
    if len(matches) > 1:
        raise RuntimeError('Multiple active jobs for ' + key + '; refusing to submit')
    return matches[0] if matches else None


def save(path, registry):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(registry))
    temporary.replace(path)


def recovery_checkpoint(key):
    """Finalized externalized metadata only, isolated by logical job directory.

    Never fall back to a cold start when a selected snapshot cannot restore:
    the fixed pending Job ID and the snapshot remain available for a retry.
    """
    uri = os.environ.get('FLINK_CHECKPOINT_DIR', 'file:///opt/flink/checkpoints')
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme != 'file' or parsed.netloc or not parsed.path.startswith('/'):
        raise ValueError('Recovery requires an absolute shared local file URI')
    root = Path(urllib.parse.unquote(parsed.path))
    if not root.is_dir():
        raise RuntimeError('Checkpoint mount is missing: ' + str(root))
    candidates = []
    for metadata in (root / key).glob('*/chk-*/_metadata'):
        if metadata.is_file() and metadata.stat().st_size > 0:
            candidates.append(metadata)
    latest = max(candidates, key=lambda p: (p.stat().st_mtime_ns, p.parent.name), default=None)
    return latest.as_uri() if latest else None


def reconcile(path):
    registry = json.loads(path.read_text()) if path.exists() else {}
    deadline = time.monotonic() + int(os.environ.get('FLINK_SUBMIT_TIMEOUT_SECONDS', '300'))
    required = int(os.environ.get('FLINK_REQUIRED_TASKMANAGERS', '2'))
    announced = set()
    while time.monotonic() < deadline:
        try:
            overview = rest('/overview')
            if overview['taskmanagers'] < required:
                LOG.info('Waiting for TaskManagers: %s/%s', overview['taskmanagers'], required)
                time.sleep(2)
                continue
            kafka_ready(os.environ['KAFKA_BOOTSTRAP_SERVERS'])
            jobs = rest('/jobs/overview')['jobs']
        except Exception as error:
            LOG.info('Waiting for cluster/Kafka readiness: %s', error)
            time.sleep(2)
            continue
        running = 0
        for key, name, script, source_var in JOBS:
            job = resolve(jobs, registry, key, name, os.environ[source_var])
            if job:
                registry[key] = job['jid']
                save(path, registry)
                status = (key, job['jid'], job['state'])
                if status not in announced:
                    LOG.info('Detected %s Job ID %s: %s', key, job['jid'], job['state'])
                    announced.add(status)
                running += job['state'] == 'RUNNING'
                continue
            # Both jobs use parallelism=1 and the default shared slot group: one slot/job.
            if rest('/overview')['slots-available'] < 1:
                LOG.info('Waiting for a free task slot for %s', key)
                continue
            previous = registry.get(key)
            if not previous or any(j['jid'] == previous and j['state'] in TERMINAL for j in jobs):
                registry[key] = uuid.uuid4().hex
            # Persist before invoking the client. Retries reuse this fixed ID, so an
            # interrupted/ambiguous submission cannot create a second job.
            save(path, registry)
            LOG.info('Submitting %s (%s) Job ID %s', key, script, registry[key])
            command = ['/opt/flink/bin/flink', 'run', '-d',
                       '-m', urllib.parse.urlparse(os.environ.get('FLINK_REST_URL', 'http://flink-jobmanager:8081')).netloc,
                       '-D$internal.pipeline.job-id=' + registry[key],
                       '--pyFiles', '/opt/flink/jobs', '-py', '/opt/flink/jobs/' + script]
            checkpoint = recovery_checkpoint(key)
            if checkpoint:
                LOG.info('Restoring %s from retained checkpoint %s', key, checkpoint)
                command[3:3] = ['-s', checkpoint, '-claimMode', 'NO_CLAIM']
            else:
                LOG.warning('No completed checkpoint for %s; starting fresh at latest Kafka offsets', key)
            result = subprocess.run(command, timeout=max(1, deadline - time.monotonic()), check=False)
            LOG.info('Submission client for %s exited %s; confirming via REST', key, result.returncode)
            # Refresh REST before considering the next job/slot allocation.
            break
        if running == len(JOBS):
            LOG.info('Both AIS jobs confirmed RUNNING; submitter complete')
            return
        time.sleep(2)
    raise TimeoutError('Timed out before both AIS jobs were RUNNING')


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    directory = Path('/opt/flink/submitter-state')
    directory.mkdir(parents=True, exist_ok=True)
    # Shared volume serializes compose up, run, and concurrent retry invocations.
    with (directory / 'submit.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        reconcile(directory / 'jobs.json')


if __name__ == '__main__':
    main()
