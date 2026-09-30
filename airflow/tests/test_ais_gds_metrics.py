"""Exercise the real GDS DAG with a small offline TaskFlow harness."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import runpy
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


DAG_FILE = Path(__file__).resolve().parents[1] / 'dags' / 'ais_gds_metrics.py'
STAGES = (
    'validate_graph_snapshot', 'recreate_gds_projections', 'run_pagerank',
    'run_louvain', 'write_metrics_to_neo4j', 'build_communities', 'validate_metrics',
)
CLEANUP = 'cleanup_gds_projections'
COMPLETE = 'complete_gds_metrics'


class TaskCall:
    def __init__(self, function, options, args, kwargs):
        self.function = function
        self.options = options
        self.args = args
        self.kwargs = kwargs
        self.task_id = options.get('task_id', function.__name__)
        self.upstream = {
            value.task_id for value in (*args, *kwargs.values())
            if isinstance(value, TaskCall)
        }

    def __rshift__(self, downstream):
        downstream.upstream.add(self.task_id)
        return downstream


class GdsMetricsDagTests(unittest.TestCase):
    def setUp(self):
        self.dags = []
        self.calls = []

        def dag(**options):
            def decorate(function):
                def build():
                    self.dags.append(options)
                    function()
                return build
            return decorate

        def task(**options):
            def decorate(function):
                def bind(*args, **kwargs):
                    call = TaskCall(function, options, args, kwargs)
                    self.calls.append(call)
                    return call
                return bind
            return decorate

        sdk = ModuleType('airflow.sdk')
        sdk.dag = dag
        sdk.task = task
        sdk.TriggerRule = SimpleNamespace(ALL_DONE='all_done', ALL_SUCCESS='all_success')
        airflow = ModuleType('airflow')
        airflow.sdk = sdk
        gds = ModuleType('pipelines.graph_metrics.gds')
        self.operations = {}
        for name in (*STAGES, CLEANUP):
            operation = Mock(name=name, return_value={
                'run_id': '2026-09-15T08:00:00Z', 'completed_stage': name,
            })
            self.operations[name] = operation
            setattr(gds, name, operation)
        self.operations[CLEANUP].return_value = None
        modules_patch = patch.dict(sys.modules, {
            'airflow': airflow, 'airflow.sdk': sdk, 'pipelines.graph_metrics.gds': gds,
        })
        modules_patch.start()
        self.addCleanup(modules_patch.stop)
        path_patch = patch.object(sys, 'path', sys.path[:])
        path_patch.start()
        self.addCleanup(path_patch.stop)
        runpy.run_path(str(DAG_FILE))
        self.tasks = {call.task_id: call for call in self.calls}

    def execute_graph(self):
        """Model terminal all_success/all_done decisions, with no retry scheduler."""
        statuses, results = {}, {}
        pending = list(self.calls)
        while pending:
            ready = [call for call in pending if call.upstream <= statuses.keys()]
            self.assertTrue(ready, 'DAG has a cycle or an unknown dependency')
            for call in ready:
                pending.remove(call)
                rule = call.options.get('trigger_rule', 'all_success')
                self.assertIn(rule, ('all_success', 'all_done'))
                if rule == 'all_success' and any(statuses[name] != 'success'
                                                  for name in call.upstream):
                    statuses[call.task_id] = 'upstream_failed'
                    continue
                args = tuple(results[arg.task_id] if isinstance(arg, TaskCall) else arg
                             for arg in call.args)
                kwargs = {key: results[value.task_id] if isinstance(value, TaskCall) else value
                          for key, value in call.kwargs.items()}
                try:
                    results[call.task_id] = call.function(*args, **kwargs)
                except RuntimeError:
                    statuses[call.task_id] = 'failed'
                else:
                    statuses[call.task_id] = 'success'
        return statuses, results

    def test_manual_serial_dag_has_exact_sequential_stage_dependencies(self):
        self.assertEqual(len(self.dags), 1)
        dag = self.dags[0]
        self.assertEqual(dag['dag_id'], 'ais_gds_metrics')
        self.assertEqual(dag['start_date'], datetime(2026, 9, 1, tzinfo=timezone.utc))
        self.assertIsNone(dag['schedule'])
        self.assertFalse(dag['catchup'])
        self.assertEqual(dag['max_active_runs'], 1)
        self.assertEqual(dag['max_active_tasks'], 1)
        self.assertEqual(dag['default_args']['retries'], 2)
        self.assertEqual(dag['default_args']['retry_delay'], timedelta(minutes=1))
        self.assertEqual(len(self.calls), 9)
        self.assertNotIn('export_metrics_to_clickhouse', self.tasks)
        self.assertEqual(set(self.tasks), {*STAGES, CLEANUP, COMPLETE})
        for index, name in enumerate(STAGES):
            call = self.tasks[name]
            self.assertEqual(call.upstream, {STAGES[index - 1]} if index else set())
            self.assertEqual(call.args, (self.tasks[STAGES[index - 1]],) if index else ())
            self.assertEqual(call.kwargs, {})
            self.assertEqual(call.options.get('trigger_rule', 'all_success'), 'all_success')
            self.assertEqual(call.options['execution_timeout'], timedelta(minutes=15))
        for operation in self.operations.values():
            operation.assert_not_called()

    def test_cleanup_has_no_xcom_arguments_and_cannot_mask_pipeline_failure(self):
        cleanup = self.tasks[CLEANUP]
        self.assertEqual(cleanup.upstream, set(STAGES))
        self.assertEqual(cleanup.options['trigger_rule'], 'all_done')
        self.assertFalse(cleanup.options['do_xcom_push'])
        self.assertEqual(cleanup.args, ())
        self.assertEqual(cleanup.kwargs, {})
        completed = self.tasks[COMPLETE]
        self.assertEqual(completed.upstream, {STAGES[-1], CLEANUP})
        self.assertEqual(completed.options['trigger_rule'], 'all_success')
        self.assertFalse(completed.options['do_xcom_push'])
        downstream_ids = set().union(*(call.upstream for call in self.calls))
        self.assertEqual(set(self.tasks) - downstream_ids, {COMPLETE})

    def test_success_forwards_small_stage_state_to_exact_pipeline_functions(self):
        statuses, results = self.execute_graph()
        self.assertEqual(set(statuses.values()), {'success'})
        self.operations[STAGES[0]].assert_called_once_with()
        for index, name in enumerate(STAGES[1:], start=1):
            previous = results[STAGES[index - 1]]
            self.operations[name].assert_called_once_with(previous)
            self.assertIs(self.operations[name].call_args.args[0], previous)
            self.assertIs(results[name], self.operations[name].return_value)
        self.operations[CLEANUP].assert_called_once_with()
        self.assertIsNone(results[CLEANUP])
        self.assertIsNone(results[COMPLETE])

    def test_every_failed_stage_runs_cleanup_without_successful_terminal_task(self):
        for failed_stage in STAGES:
            with self.subTest(failed_stage=failed_stage):
                for operation in self.operations.values():
                    operation.reset_mock(side_effect=True)
                self.operations[failed_stage].side_effect = RuntimeError('stage failed')
                statuses, _ = self.execute_graph()
                self.assertEqual(statuses[failed_stage], 'failed')
                self.assertEqual(statuses[CLEANUP], 'success')
                self.assertEqual(statuses[COMPLETE], 'upstream_failed')
                self.operations[CLEANUP].assert_called_once_with()
                for later_stage in STAGES[STAGES.index(failed_stage) + 1:]:
                    self.operations[later_stage].assert_not_called()

    def test_cleanup_failure_prevents_successful_terminal_task(self):
        self.operations[CLEANUP].side_effect = RuntimeError('cleanup failed')
        statuses, _ = self.execute_graph()
        self.assertEqual(statuses[STAGES[-1]], 'success')
        self.assertEqual(statuses[CLEANUP], 'failed')
        self.assertEqual(statuses[COMPLETE], 'upstream_failed')

    def test_task_wrappers_propagate_original_failure(self):
        for name in (*STAGES, CLEANUP):
            with self.subTest(task=name):
                failure = RuntimeError('pipeline failure')
                self.operations[name].side_effect = failure
                args = () if name in (STAGES[0], CLEANUP) else ({'run_id': 'test-run'},)
                with self.assertRaises(RuntimeError) as raised:
                    self.tasks[name].function(*args)
                self.assertIs(raised.exception, failure)


if __name__ == '__main__':
    unittest.main()
