"""Manually compute, write, and validate Neo4j metrics for the current port graph."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import TriggerRule, dag, task


@dag(dag_id='ais_gds_metrics', start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)}, tags=['ais', 'ports', 'gds'])
def ais_gds_metrics():
    @task(execution_timeout=timedelta(minutes=15))
    def validate_graph_snapshot():
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import validate_graph_snapshot as validate
        return validate()

    @task(execution_timeout=timedelta(minutes=15))
    def recreate_gds_projections(snapshot):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import recreate_gds_projections as recreate
        return recreate(snapshot)

    @task(execution_timeout=timedelta(minutes=15))
    def run_pagerank(state):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import run_pagerank as calculate
        return calculate(state)

    @task(execution_timeout=timedelta(minutes=15))
    def run_louvain(state):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import run_louvain as calculate
        return calculate(state)

    @task(execution_timeout=timedelta(minutes=15))
    def write_metrics_to_neo4j(state):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import write_metrics_to_neo4j as write
        return write(state)

    @task(execution_timeout=timedelta(minutes=15))
    def validate_metrics(state):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import validate_metrics as validate
        return validate(state)

    @task(execution_timeout=timedelta(minutes=5), trigger_rule=TriggerRule.ALL_DONE,
          do_xcom_push=False)
    def cleanup_gds_projections():
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.graph_metrics.gds import cleanup_gds_projections as cleanup
        cleanup()

    @task(execution_timeout=timedelta(minutes=1), trigger_rule=TriggerRule.ALL_SUCCESS,
          do_xcom_push=False)
    def complete_gds_metrics():
        """Require successful validation and cleanup before marking the run successful."""

    snapshot = validate_graph_snapshot()
    projections = recreate_gds_projections(snapshot)
    pagerank = run_pagerank(projections)
    louvain = run_louvain(pagerank)
    written = write_metrics_to_neo4j(louvain)
    validated = validate_metrics(written)

    # No XCom arguments: cleanup must also run when a stage has no result.
    cleanup = cleanup_gds_projections()
    for stage in (snapshot, projections, pagerank, louvain, written, validated):
        stage >> cleanup

    # ALL_DONE cleanup must not hide a failed stage when Airflow checks leaf tasks.
    completed = complete_gds_metrics()
    validated >> completed
    cleanup >> completed


ais_gds_metrics()
