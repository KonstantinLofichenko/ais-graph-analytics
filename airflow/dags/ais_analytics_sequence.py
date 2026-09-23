"""Sequential deferred child-DAG coordination for Airflow 3; no AIS business logic."""
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from ais_port_visits_window import daily_windows

CHILDREN = ('ais_port_visits', 'ais_gds_metrics', 'ais_graph_metrics_export')


class DailyAnalyticsOperator(TriggerDagRunOperator):
    """Advance one child at a time, reconstructing progress from its completion event."""
    def __init__(self, **kwargs):
        super().__init__(trigger_dag_id=CHILDREN[0], wait_for_completion=True,
                         deferrable=True, allowed_states=['success'], failed_states=['failed'],
                         fail_when_dag_is_paused=True, **kwargs)

    @staticmethod
    def _windows(context):
        return daily_windows({**context.get('params', {}), **(context['dag_run'].conf or {})})

    @staticmethod
    def _child_run_id(context, step):
        # Airflow execution identifier only; never used as the analytical run_id.
        return f"{context['run_id']}__step_{step}"

    def _launch(self, context, windows, step):
        if step == len(windows) * len(CHILDREN):
            return {'completed_days': len(windows)}
        day, stage = divmod(step, len(CHILDREN))
        self.trigger_dag_id = CHILDREN[stage]
        self.trigger_run_id = self._child_run_id(context, step)
        # GDS/export consume the graph metadata produced by port visits.
        self.conf = windows[day] if stage == 0 else None
        self.log.info('Daily analytics %s -> %s: %s',
                      windows[day]['start'], windows[day]['end'], self.trigger_dag_id)
        return super().execute(context)

    def execute(self, context):
        return self._launch(context, self._windows(context), 0)

    def execute_complete(self, context, event):
        # Airflow reconstructs operators on resume: do not rely on mutated instance state.
        windows = self._windows(context)
        data = event[1]
        run_ids = data.get('run_ids', [])
        for step in range(len(windows) * len(CHILDREN)):
            expected = self._child_run_id(context, step)
            if run_ids == [expected] and data.get('dag_id') == CHILDREN[step % len(CHILDREN)]:
                if data.get(expected) != 'success':
                    raise AirflowException(f'Child {expected} did not succeed: {data.get(expected)}')
                return self._launch(context, windows, step + 1)
        raise AirflowException('Unexpected child completion event; refusing to advance')
