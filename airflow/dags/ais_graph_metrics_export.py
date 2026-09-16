"""Manually export existing Neo4j port metrics for downstream analytics."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task


@dag(dag_id='ais_graph_metrics_export', start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)}, tags=['ais', 'ports'])
def ais_graph_metrics_export():
    @task(execution_timeout=timedelta(minutes=15), do_xcom_push=False)
    def export_port_metrics():
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.export import export_metrics
        export_metrics()

    export_port_metrics()


ais_graph_metrics_export()
