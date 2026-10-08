"""Opt-in real recovery test: private Flink cluster/topics, production untouched.

Run: python3 flink/tests/integration_recovery.py
Uses the existing ais-flink image and Kafka broker; deletes only its private
containers/topics afterwards. Test-only 30s gap and 5s checkpoints shorten the
exercise; production window sizes/watermark/idleness stay unchanged.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]


def command(args, payload=None, check=True):
    result = subprocess.run(args, input=payload, text=True, capture_output=True, timeout=300)
    if check and result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return result.stdout + (result.stderr if 'recovery-submit' in args else '')


def wait_for(condition, label, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = condition()
            if value:
                print('PASS:', label, flush=True)
                return value
        except Exception as error:
            last = error
        time.sleep(1)
    raise AssertionError('Timed out: ' + label + ': ' + str(last))


def main():
    suffix = uuid.uuid4().hex[:12]
    project = 'ais-recovery-' + suffix
    topics = ['ais.test.recovery.' + suffix + '.' + kind for kind in ('positions', 'gaps', 'features')]
    configuration = json.loads(command(['docker', 'compose', '--profile', 'streaming', 'config', '--format', 'json']))
    env = configuration['services']['flink-jobmanager']['environment'].copy()
    reference_dir = env['FLINK_REFERENCE_DIR']
    env.update(FLINK_GAP_SOURCE_TOPIC=topics[0], FLINK_FEATURE_SOURCE_TOPIC=topics[0],
               FLINK_GAP_TOPIC=topics[1], FLINK_FEATURE_TOPIC=topics[2],
               FLINK_GAP_GROUP_ID=project + '-gap', FLINK_FEATURE_GROUP_ID=project + '-features',
               FLINK_GAP_TIMEOUT_SECONDS='30', FLINK_CHECKPOINT_INTERVAL_SECONDS='5',
               FLINK_CHECKPOINT_MIN_PAUSE_SECONDS='1', FLINK_REST_URL='http://recovery-jm:8081',
               FLINK_REQUIRED_TASKMANAGERS='1')
    kafka_base = ['docker', 'exec', '-i', 'ais-kafka', '/opt/kafka/bin/']
    def kafka(tool, *args, payload=None, check=True):
        return command(kafka_base[:-1] + [kafka_base[-1] + tool, '--bootstrap-server', 'ais-kafka:29092', *args], payload, check)
    with tempfile.TemporaryDirectory(prefix='ais-recovery-') as temporary:
        directory = Path(temporary)
        checkpoints, registry = directory / 'checkpoints', directory / 'registry'
        checkpoints.mkdir(mode=0o777)
        checkpoints.chmod(0o777)
        registry.mkdir()
        mounts = [str(ROOT / 'flink/jobs') + ':/opt/flink/jobs:ro',
                  str(ROOT / 'dbt/seeds') + ':' + reference_dir + ':ro',
                  str(checkpoints) + ':/opt/flink/checkpoints']
        common = dict(image='ais-flink:2.2.1', networks=['ais'], volumes=mounts)
        jm_env = dict(env, FLINK_PROPERTIES='jobmanager.rpc.address: recovery-jm\nparallelism.default: 1\n')
        tm_env = dict(env, FLINK_PROPERTIES='jobmanager.rpc.address: recovery-jm\ntaskmanager.numberOfTaskSlots: 2\nparallelism.default: 1\n')
        services = {
            'recovery-jm': dict(common, command='jobmanager', environment=jm_env),
            'recovery-tm': dict(common, command='taskmanager', environment=tm_env),
            'recovery-submit': dict(common, user='0:0', command=['python', '/opt/flink/scripts/submit_jobs.py'],
                                   environment=jm_env, volumes=mounts + [str(ROOT / 'flink/scripts') + ':/opt/flink/scripts:ro', str(registry) + ':/opt/flink/submitter-state']),
        }
        compose_file = directory / 'compose.json'
        compose_file.write_text(json.dumps(dict(services=services, networks={'ais': {'external': True, 'name': 'ais-network'}})))
        compose = ['docker', 'compose', '-p', project, '-f', str(compose_file)]
        jm = None
        def rest(path):
            return json.loads(command(['docker', 'exec', jm, 'python', '-c',
                                      "import urllib.request; print(urllib.request.urlopen('http://localhost:8081' + " + repr(path) + ",timeout=5).read().decode())"]))
        def running():
            jobs = rest('/jobs/overview')['jobs']
            return jobs if len(jobs) == 2 and all(j['state'] == 'RUNNING' for j in jobs) else None
        def snapshots():
            return {j['jid']: rest('/jobs/' + j['jid'] + '/checkpoints') for j in running() or []}
        def consume(topic):
            output = kafka('kafka-console-consumer.sh', '--topic', topic, '--from-beginning', '--timeout-ms', '5000', check=False)
            return [json.loads(line) for line in output.splitlines() if line.startswith('{')]
        def publish(mmsi, second, speed=2):
            payload = {'mmsi': mmsi, 'msgtime': '2026-10-08T00:00:%02dZ' % second,
                       'name': 'Recovery fixture', 'speedOverGround': speed}
            kafka('kafka-console-producer.sh', '--topic', topics[0], payload=json.dumps(payload) + '\n')
        try:
            for topic in topics:
                kafka('kafka-topics.sh', '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1')
            command(compose + ['up', '-d', 'recovery-jm', 'recovery-tm'])
            jm = command(compose + ['ps', '-q', 'recovery-jm']).strip()
            command(compose + ['run', '--rm', '--no-deps', 'recovery-submit'])
            initial = wait_for(running, 'two running fixture jobs')
            wait_for(lambda: (v if len(v := snapshots()) == 2 and all(x['latest']['completed'] for x in v.values()) else None), 'initial source checkpoints before fixtures')
            # Two observations in all four still-open windows. A becomes a detected gap.
            publish(999999981, 1, 2)
            publish(999999981, 2, 4)
            wait_for(lambda: any(p['mmsi'] == 999999981 and p['event_type'] == 'AIS_GAP_DETECTED' for p in consume(topics[1])), 'A gap detection')
            # B has ValueState plus a pending processing-time timer at the snapshot.
            publish(999999982, 3)
            sent = time.time() * 1000
            def ready_checkpoints():
                values = snapshots()
                return values if len(values) == 2 and all((v['latest']['completed'] or {}).get('trigger_timestamp', 0) > sent for v in values.values()) else None
            before = wait_for(ready_checkpoints, 'checkpoints containing detected A and pre-gap B')
            command(compose + ['stop', 'recovery-jm', 'recovery-tm'])
            # This arrives while Flink is absent: recovery must not start at latest.
            publish(999999981, 4, 6)
            command(compose + ['up', '-d', 'recovery-jm', 'recovery-tm'])
            logs = command(compose + ['run', '--rm', '--no-deps', 'recovery-submit'])
            after = wait_for(running, 'restored fixture jobs')
            assert {j['jid'] for j in after} == {j['jid'] for j in initial}
            recovered = snapshots()
            assert all(v['counts']['restored'] == 1 and v['latest']['restored'] for v in recovered.values()), recovered
            wait_for(lambda: any(p['mmsi'] == 999999982 and p['event_type'] == 'AIS_GAP_DETECTED' for p in consume(topics[1])), 'restored B processing-time timer')
            gaps = consume(topics[1])
            a = [p for p in gaps if p['mmsi'] == 999999981]
            assert [p['event_type'] for p in a].count('AIS_GAP_DETECTED') == 1, a
            assert [p['event_type'] for p in a].count('AIS_GAP_ENDED') == 1, a
            # Advance event time beyond all initial window ends using another key.
            kafka('kafka-console-producer.sh', '--topic', topics[0], payload=json.dumps({'mmsi': 999999983, 'msgtime': '2026-10-08T01:00:31Z'}) + '\n')
            def feature_results():
                rows = [p for p in consume(topics[2]) if p['mmsi'] == 999999981 and p['window_end'] == '2026-10-08T00:05:00Z']
                return rows if {p['window_minutes'] for p in rows} == {5, 15, 30, 60} else None
            features = wait_for(feature_results, 'four restored windows')
            assert all(p['position_count'] == 3 and p['avg_speed'] == 4 and p['min_speed'] == 2 and p['max_speed'] == 6 for p in features), features
            # Concurrent submitters share a lock; neither may add another copy.
            calls = [subprocess.Popen(compose + ['run', '--rm', '--no-deps', 'recovery-submit'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True) for _ in range(2)]
            for process in calls:
                output, _ = process.communicate(timeout=180)
                assert process.returncode == 0, output
            assert len(running()) == 2
            report = {'before': before, 'after': recovered, 'gap_events': gaps, 'feature_windows': features, 'submitter_logs': logs}
            report_path = Path(os.environ.get('FLINK_RECOVERY_TEST_REPORT', '/tmp/ais-flink-isolated-recovery.json'))
            report_path.write_text(json.dumps(report, indent=2))
            print('PASS: full private-cluster restart restores offsets, detected gap ValueState, pending timer and all four partial windows; no artificial detection or duplicate jobs')
            print('Evidence:', report_path)
        except Exception:
            evidence = {'logs': command(compose + ['logs', '--no-color'], check=False)}
            if jm:
                try:
                    evidence.update(checkpoints=snapshots(), gaps=consume(topics[1]), features=consume(topics[2]), positions=consume(topics[0]))
                except Exception as error:
                    evidence['inspection_error'] = str(error)
            Path('/tmp/ais-flink-isolated-recovery-failure.json').write_text(json.dumps(evidence, indent=2))
            raise
        finally:
            command(compose + ['down'], check=False)
            for topic in topics:
                kafka('kafka-topics.sh', '--delete', '--topic', topic, check=False)


if __name__ == '__main__':
    main()
