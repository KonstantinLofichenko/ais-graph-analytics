"""Identity and transition safeguards for automatic submissions."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('submit_jobs', Path(__file__).resolve().parents[1] / 'scripts/submit_jobs.py')
submitter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(submitter)


class SubmitterTests(unittest.TestCase):
    def test_known_id_is_recognized_during_transitions_without_name_lookup(self):
        for state in ('INITIALIZING', 'CREATED', 'RESTARTING', 'RUNNING', 'CANCELLING'):
            job = {'jid': 'known', 'name': 'renamed', 'state': state}
            with patch.object(submitter, 'rest', side_effect=AssertionError('unexpected plan lookup')):
                self.assertEqual(submitter.resolve([job], {'gap': 'known'}, 'gap', 'gap name', 'source'), job)

    def test_same_display_name_without_plan_match_is_not_adopted(self):
        job = {'jid': 'other', 'name': 'gap name', 'state': 'RUNNING'}
        with patch.object(submitter, 'rest', return_value={'plan': {'nodes': [{'description': 'Source: other<br/>'}]}}):
            self.assertIsNone(submitter.resolve([job], {}, 'gap', 'gap name', 'source'))

    def test_legacy_adoption_requires_source_and_operator_plan(self):
        job = {'jid': 'legacy', 'name': 'gap name', 'state': 'RUNNING'}
        plan = {'plan': {'nodes': [{'description': 'Source: source<br/>'}, {'description': 'KEYED PROCESS'}]}}
        with patch.object(submitter, 'rest', return_value=plan):
            self.assertEqual(submitter.resolve([job], {}, 'gap', 'gap name', 'source'), job)
            with self.assertRaisesRegex(RuntimeError, 'Multiple active'):
                submitter.resolve([job, dict(job, jid='duplicate')], {}, 'gap', 'gap name', 'source')

    def test_reconcile_waits_for_known_transition_even_with_no_free_slots(self):
        jobs = [
            {'jid': 'gap-id', 'name': 'renamed gap', 'state': 'INITIALIZING'},
            {'jid': 'feature-id', 'name': 'renamed features', 'state': 'RUNNING'},
        ]
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / 'jobs.json'
            submitter.save(registry, {'gap': 'gap-id', 'features': 'feature-id'})
            def ready(_):
                jobs[0]['state'] = 'RUNNING'
            with patch.dict(submitter.os.environ, {'FLINK_GAP_SOURCE_TOPIC': 'source', 'FLINK_FEATURE_SOURCE_TOPIC': 'source', 'KAFKA_BOOTSTRAP_SERVERS': 'broker:9092'}), patch.object(submitter, 'kafka_ready'), patch.object(submitter, 'rest', side_effect=lambda path: {'jobs': jobs} if path == '/jobs/overview' else {'taskmanagers': 2, 'slots-available': 0}), patch.object(submitter.time, 'sleep', side_effect=ready), patch.object(submitter.subprocess, 'run') as launch:
                submitter.reconcile(registry)
                launch.assert_not_called()

    def test_latest_completed_checkpoint_is_isolated_and_pending_ignored(self):
        import os
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old = root / 'gap' / 'old-id' / 'chk-1' / '_metadata'
            new = root / 'gap' / 'new-id' / 'chk-2' / '_metadata'
            for metadata in (old, new):
                metadata.parent.mkdir(parents=True)
                metadata.write_bytes(b'checkpoint')
            os.utime(old, (1, 1))
            pending = new.parent.parent / 'chk-3'
            pending.mkdir()
            (pending / '_metadata').touch()
            with patch.dict(submitter.os.environ, {'FLINK_CHECKPOINT_DIR': root.as_uri()}):
                self.assertEqual(submitter.recovery_checkpoint('gap'), new.as_uri())
                self.assertIsNone(submitter.recovery_checkpoint('features'))
            with patch.dict(submitter.os.environ, {'FLINK_CHECKPOINT_DIR': (root / 'missing').as_uri()}):
                with self.assertRaisesRegex(RuntimeError, 'mount is missing'):
                    submitter.recovery_checkpoint('gap')

    def test_terminal_job_is_not_treated_as_running(self):
        job = {'jid': 'known', 'name': 'gap name', 'state': 'FAILED'}
        self.assertIsNone(submitter.resolve([job], {'gap': 'known'}, 'gap', 'gap name', 'source'))

    def test_missing_job_is_restored_without_allowing_unmapped_state(self):
        from types import SimpleNamespace
        jobs = [{'jid': 'feature-id', 'name': 'renamed', 'state': 'RUNNING'}]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / 'jobs.json'
            metadata = root / 'gap' / 'gap-id' / 'chk-1' / '_metadata'
            metadata.parent.mkdir(parents=True)
            metadata.write_bytes(b'checkpoint')
            submitter.save(path, {'gap': 'gap-id', 'features': 'feature-id'})
            def launch(command, **kwargs):
                self.assertIn('-s', command)
                self.assertIn(metadata.as_uri(), command)
                self.assertIn('NO_CLAIM', command)
                self.assertNotIn('--allowNonRestoredState', command)
                jobs.append({'jid': 'gap-id', 'name': 'renamed', 'state': 'RUNNING'})
                return SimpleNamespace(returncode=0)
            env = {'FLINK_GAP_SOURCE_TOPIC': 'source', 'FLINK_FEATURE_SOURCE_TOPIC': 'source',
                   'KAFKA_BOOTSTRAP_SERVERS': 'broker:9092', 'FLINK_CHECKPOINT_DIR': root.as_uri()}
            with patch.dict(submitter.os.environ, env), patch.object(submitter, 'kafka_ready'), patch.object(submitter.time, 'sleep'), patch.object(submitter, 'rest', side_effect=lambda p: {'jobs': jobs} if p == '/jobs/overview' else {'taskmanagers': 2, 'slots-available': 1}), patch.object(submitter.subprocess, 'run', side_effect=launch) as client:
                submitter.reconcile(path)
                client.assert_called_once()

    def test_pending_id_is_persisted_before_submission_and_reused(self):
        jobs = []
        overview = {'taskmanagers': 2, 'slots-available': 2}
        with tempfile.TemporaryDirectory() as directory:
            registry = Path(directory) / 'jobs.json'
            def launch(command, **kwargs):
                import json
                job_id = json.loads(registry.read_text())['gap']
                self.assertIn('-D$internal.pipeline.job-id=' + job_id, command)
                self.assertIn('--pyFiles', command)
                jobs.append({'jid': job_id, 'name': 'renamed', 'state': 'INITIALIZING'})
                raise TimeoutError('simulate interrupted submission')
            with patch.dict(submitter.os.environ, {'FLINK_GAP_SOURCE_TOPIC': 'source', 'FLINK_FEATURE_SOURCE_TOPIC': 'source', 'KAFKA_BOOTSTRAP_SERVERS': 'broker:9092'}), patch.object(submitter, 'kafka_ready'), patch.object(submitter, 'rest', side_effect=lambda path: {'jobs': jobs} if path == '/jobs/overview' else overview), patch.object(submitter.subprocess, 'run', side_effect=launch), patch.object(submitter, 'recovery_checkpoint', return_value=None):
                with self.assertRaises(TimeoutError):
                    submitter.reconcile(registry)
                import json
                saved = json.loads(registry.read_text())
                self.assertEqual(submitter.resolve(jobs, saved, 'gap', 'gap name', 'source')['jid'], saved['gap'])


if __name__ == '__main__':
    unittest.main()
