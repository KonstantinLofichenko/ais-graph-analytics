"""Explicit opt-in production Flink-only restart and checkpoint/ingestion checks.

Run from the repository root: python3 flink/scripts/verify_live_recovery.py
Recreates only the Flink JM, two TMs and submitter. Leaves other services alone.
Polls checkpoints and analytics ingestion, allowing the configured 600s gap timeout.
Detailed evidence is saved to /tmp/ais-flink-production-recovery.json.
"""
import json
from pathlib import Path
import subprocess
import time


def command(args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=300)
    if result.returncode:
        raise RuntimeError(result.stderr + result.stdout)
    return result.stdout


def rest(path):
    return json.loads(command(['docker', 'compose', 'exec', '-T', 'flink-jobmanager', 'python', '-c',
                              "import urllib.request; print(urllib.request.urlopen('http://localhost:8081' + " + repr(path) + ",timeout=5).read().decode())"]))


def wait_for(condition, label, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = condition()
            if value:
                print('PASS:', label, flush=True)
                return value
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError('Timed out: ' + label)


def running():
    jobs = rest('/jobs/overview')['jobs']
    active = [j for j in jobs if j['state'] not in {'CANCELED', 'FAILED', 'FINISHED', 'SUSPENDED'}]
    assert len(active) <= 2, active
    return active if len(active) == 2 and all(j['state'] == 'RUNNING' for j in active) else None


def checkpoints():
    jobs = running()
    return {j['jid']: rest('/jobs/' + j['jid'] + '/checkpoints') for j in jobs} if jobs else {}


def counts():
    query = 'SELECT count() AS features FROM analytics.ais_vessel_features FORMAT JSONEachRow; SELECT count() AS gaps FROM analytics.ais_vessel_gap_events FORMAT JSONEachRow;'
    output = command(['docker', 'compose', 'exec', '-T', 'clickhouse', 'sh', '-c',
                      'exec clickhouse-client --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" --multiquery --query "$1"', 'sh', query])
    return {k: int(v) for line in output.splitlines() for k, v in json.loads(line).items()}


def unrelated_start_times():
    services = ['ais-kafka', 'ais-clickhouse', 'ais-neo4j', 'ais-airflow', 'ais-metabase']
    return [json.loads(line) for line in command(['docker', 'inspect', '--format', '{{json .State.StartedAt}}', *services]).splitlines()]


def main():
    before = wait_for(lambda: (v if len(v := checkpoints()) == 2 and all(x['latest']['completed'] for x in v.values()) else None), 'both jobs have completed checkpoints')
    before_jobs = running()
    baseline = counts()
    other_services = unrelated_start_times()
    command(['docker', 'compose', '--profile', 'streaming', 'stop', 'flink-jobmanager', 'flink-taskmanager', 'flink-feature-taskmanager'])
    command(['docker', 'compose', '--profile', 'streaming', 'up', '-d', '--no-deps', '--force-recreate', 'flink-jobmanager', 'flink-taskmanager', 'flink-feature-taskmanager', 'flink-job-submitter'])
    after_jobs = wait_for(running, 'exactly two restored AIS jobs RUNNING')
    assert {j['jid'] for j in after_jobs} == {j['jid'] for j in before_jobs}
    after = checkpoints()
    assert all(x['counts']['restored'] == 1 and x['latest']['restored'] for x in after.values()), after
    print('PASS: both REST checkpoint histories report restoration', flush=True)
    command(['docker', 'compose', '--profile', 'streaming', 'run', '--rm', '--no-deps', 'flink-job-submitter'])
    assert {j['jid'] for j in running()} == set(before)
    def increasing_counts():
        current = counts()
        return current if all(current[k] > baseline[k] for k in baseline) else None
    final = wait_for(increasing_counts, 'both ClickHouse analytics counts increase', timeout=900)
    assert unrelated_start_times() == other_services
    output = {'before_jobs': before_jobs, 'after_jobs': after_jobs, 'before_checkpoints': before,
              'after_checkpoints': after, 'counts_before': baseline, 'counts_after': final,
              'unrelated_service_start_times': other_services,
              'submitter_logs': command(['docker', 'compose', 'logs', '--no-color', 'flink-job-submitter'])}
    path = Path('/tmp/ais-flink-production-recovery.json')
    path.write_text(json.dumps(output, indent=2))
    print('PASS: repeated submitter preserved IDs; other services were not restarted. Evidence:', path, flush=True)


if __name__ == '__main__':
    main()
