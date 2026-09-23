"""Manual port visits and consecutive-port connections workflow."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task, get_current_context


@dag(dag_id='ais_port_visits', start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)}, tags=['ais', 'ports'])
def ais_port_visits():
    @task(execution_timeout=timedelta(minutes=5))
    def download_ports():
        import sys
        sys.path.insert(0, '/opt/ais/pipelines/port_visits')
        from download_ports import download
        # Seven small reference records are passed by XCom, not a worker-local path.
        return download()

    @task(execution_timeout=timedelta(minutes=15))
    def run_port_visit_pipeline(ports):
        import json
        import os
        import subprocess
        import sys
        import tempfile
        from pathlib import Path

        sys.path.insert(0, '/opt/ais/airflow/dags')
        from ais_port_visits_window import resolve_window

        context = get_current_context()
        # Explicit start/end dates resolve to one UTC-midnight day;
        # the rolling default still uses dag_run.start_date so retries stay deterministic.
        start, end, max_rows, source = resolve_window(
            context['dag_run'].conf, context['dag_run'].start_date,
            os.environ.get('PORT_VISITS_WINDOW_HOURS'), os.environ.get('PORT_VISITS_MAX_ROWS'))
        print(f'Port visit window: start={start.isoformat()} end={end.isoformat()} '
              f'max_rows={max_rows} source={source}')
        with tempfile.TemporaryDirectory() as directory:
            reference = Path(directory) / 'ports.json'
            result = Path(directory) / 'result.json'
            reference.write_text(json.dumps(ports))
            subprocess.run([sys.executable, '/opt/ais/pipelines/port_visits/run.py',
                            '--ports', str(reference), '--start', start.isoformat(),
                            '--end', end.isoformat(), '--max-rows', str(max_rows),
                            '--max-rows-source', source, '--apply',
                            '--result-json', str(result)], check=True)
            # Only the successful batch's ID and window cross the task boundary.
            return json.loads(result.read_text())

    @task(execution_timeout=timedelta(minutes=15))
    def publish_port_connections(metadata):
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.port_connections.run import publish_port_connections as publish
        publish(metadata)

    publish_port_connections(run_port_visit_pipeline(download_ports()))


ais_port_visits()
