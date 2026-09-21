"""Manually coordinate the existing port-analysis DAGs in success order."""
from datetime import datetime, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(dag_id='ais_analytics_pipeline',
     start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1,
     render_template_as_native_obj=True,
     default_args={'retries': 0}, tags=['ais', 'ports', 'orchestration'])
def ais_analytics_pipeline():
    # Only port visits consumes start/end/max_rows. Without explicit start/end,
    # it resolves its usual rolling window from its own DAG run start time.
    visits = TriggerDagRunOperator(
        task_id='trigger_port_visits', trigger_dag_id='ais_port_visits',
        conf='{{ dag_run.conf or {} }}',
        wait_for_completion=True, deferrable=True,
        allowed_states=['success'], failed_states=['failed'],
    )
    # These DAGs read the window and canonical run_id from the managed graph
    # published by port visits; they do not accept window overrides in conf.
    gds = TriggerDagRunOperator(
        task_id='trigger_gds_metrics', trigger_dag_id='ais_gds_metrics',
        wait_for_completion=True, deferrable=True,
        allowed_states=['success'], failed_states=['failed'],
    )
    # GDS already exports; retain this requested standalone export stage too.
    export = TriggerDagRunOperator(
        task_id='trigger_graph_metrics_export', trigger_dag_id='ais_graph_metrics_export',
        wait_for_completion=True, deferrable=True,
        allowed_states=['success'], failed_states=['failed'],
    )
    visits >> gds >> export


ais_analytics_pipeline()
