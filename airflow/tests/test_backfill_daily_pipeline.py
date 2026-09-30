"""Exercise the real shell script with fake Docker and sleep executables."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts/backfill_daily_pipeline.sh'
FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
root=Path(os.environ['BACKFILL_TEST_ROOT'])
config=json.loads((root/'config.json').read_text())
args=sys.argv[1:]
assert args[:4]==['exec','ais-airflow','airflow','dags'],args
args=args[4:]
event={'command':args}
command=args[0]
if command=='details':
    result=[dict(dag_id=args[1],is_paused=config.get('paused','False'),is_stale='False',
                 has_import_errors='False',max_active_runs=config.get('max_active_runs','1'))]
elif command=='list-runs':
    result=[{'run_id':'other'}] if config.get('busy') else []
elif command=='trigger':
    event['run_id']=args[args.index('--run-id')+1]
    event['conf']=json.loads(args[args.index('--conf')+1])
    result=[{'run_id':event['run_id']}]
elif command=='state':
    path=root/'polls.json'
    polls=json.loads(path.read_text()) if path.exists() else {}
    run_id=args[-1]
    index=polls.get(run_id,0)
    polls[run_id]=index+1
    path.write_text(json.dumps(polls))
    states=config.get('states',['success'])
    result=states[min(index,len(states)-1)]
    event.update(run_id=run_id,state=result)
else:
    raise AssertionError(args)
with (root/'events.jsonl').open('a') as log:
    log.write(json.dumps(event)+'\n')
if config.get('error_command')==command:
    sys.exit('simulated CLI error')
print((result + ', ' + json.dumps({'activity_date': '2026-09-28'}))
      if command == 'state' and config.get('state_with_conf', True)
      else result if command == 'state' else json.dumps(result))
'''
FAKE_SLEEP = r'''#!/usr/bin/env python3
import json,os,sys
from pathlib import Path
with (Path(os.environ['BACKFILL_TEST_ROOT'])/'events.jsonl').open('a') as log:
    log.write(json.dumps({'sleep':sys.argv[1:]})+'\n')
'''


class BackfillScriptTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        for name, source in [('docker', FAKE_DOCKER), ('sleep', FAKE_SLEEP)]:
            path = self.root / name
            path.write_text(source)
            path.chmod(0o755)
        self.env = dict(os.environ, PATH=str(self.root) + os.pathsep + os.environ['PATH'],
                        TMPDIR=str(self.root), BACKFILL_TEST_ROOT=str(self.root))

    def run_script(self, args, **config):
        (self.root / 'config.json').write_text(json.dumps(config))
        return subprocess.run(['bash', str(SCRIPT), *args], cwd=self.root, env=self.env,
                              capture_output=True, text=True, timeout=30)

    def events(self):
        path = self.root / 'events.jsonl'
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def triggers(self):
        return [e for e in self.events() if e.get('command', [''])[0] == 'trigger']

    def test_inclusive_range_waits_for_exact_run_success_before_next_day(self):
        result = self.run_script(['2026-09-28', '2026-09-29'], states=['queued', 'running', 'success'])
        self.assertEqual(result.returncode, 0, result.stderr)
        triggers = self.triggers()
        self.assertEqual([t['conf'] for t in triggers],
                         [{'activity_date': '2026-09-28'}, {'activity_date': '2026-09-29'}])
        self.assertNotEqual(triggers[0]['run_id'], triggers[1]['run_id'])
        events = self.events()
        for trigger in triggers:
            self.assertRegex(trigger['run_id'], r'^historical__2026-09-(28|29)__[0-9a-f]{32}$')
            states = [e['state'] for e in events if e.get('command', [''])[0] == 'state'
                      and e['run_id'] == trigger['run_id']]
            self.assertEqual(states, ['queued', 'running', 'success'])
        first_success = next(i for i, e in enumerate(events)
                             if e.get('state') == 'success' and e['run_id'] == triggers[0]['run_id'])
        self.assertLess(first_success, events.index(triggers[1]))
        self.assertEqual([e['sleep'] for e in events if 'sleep' in e], [['15']] * 4)
        self.assertIn('[2/2] Success: 2026-09-29', result.stdout)
        self.assertFalse((self.root / 'ais-daily-pipeline-backfill.lock').exists())

    def test_same_day_and_rerun_get_new_explicit_airflow_ids(self):
        for _ in range(2):
            result = self.run_script(['2024-02-29', '2024-02-29'], state_with_conf=False)
            self.assertEqual(result.returncode, 0, result.stderr)
        triggers = self.triggers()
        self.assertEqual(len(triggers), 2)
        self.assertNotEqual(triggers[0]['run_id'], triggers[1]['run_id'])
        self.assertEqual(triggers[0]['conf'], triggers[1]['conf'])
        self.assertTrue(all(e.get('command', [''])[0] in ('details', 'list-runs', 'trigger', 'state')
                            for e in self.events()))

    def test_invalid_input_never_calls_airflow(self):
        for args in ([], ['2026-09-28'], ['2026-09-28', '2026-09-29', 'extra'],
                     ['2026-09-29', '2026-09-28'], ['2026-9-28', '2026-09-29'],
                     ['2026-02-29', '2026-03-01'], ['2026-09-28T00:00:00Z', '2026-09-29'],
                     ['2026-09-28;echo bad', '2026-09-29'], ['2026-09-28\n', '2026-09-29']):
            with self.subTest(args=args):
                self.assertNotEqual(self.run_script(args).returncode, 0)
        self.assertEqual(self.events(), [])

    def test_failed_run_stops_without_triggering_next_date(self):
        result = self.run_script(['2026-09-28', '2026-09-29'], states=['running', 'failed'])
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(self.triggers()), 1)
        self.assertIn('Failed: 2026-09-28', result.stderr)

    def test_cli_errors_and_unknown_state_fail_closed_without_trigger_retry(self):
        for config in ({'error_command': 'trigger'}, {'error_command': 'state'}, {'states': ['None']}):
            with self.subTest(config=config):
                (self.root / 'events.jsonl').write_text('')
                result = self.run_script(['2026-09-28', '2026-09-29'], **config)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(len(self.triggers()), 1)
                self.assertIn('NOT cancelled', result.stderr)
                self.assertFalse((self.root / 'ais-daily-pipeline-backfill.lock').exists())

    def test_busy_paused_and_unsafe_concurrency_preflight_rejects(self):
        for config in ({'busy': True}, {'paused': 'True'}, {'max_active_runs': '2'},
                       {'error_command': 'details'}, {'error_command': 'list-runs'}):
            with self.subTest(config=config):
                result = self.run_script(['2026-09-28', '2026-09-29'], **config)
                self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.triggers(), [])

    def test_existing_local_lock_is_not_removed_or_ignored(self):
        lock = self.root / 'ais-daily-pipeline-backfill.lock'
        lock.mkdir()
        result = self.run_script(['2026-09-28', '2026-09-29'])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('backfill lock exists', result.stderr)
        self.assertTrue(lock.exists())
        self.assertEqual(self.events(), [])


if __name__ == '__main__':
    unittest.main()
