"""Two-task manual workflow. The live AIS producer remains independent."""
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
        import subprocess
        import sys
        import tempfile
        from pathlib import Path
        context = get_current_context()
        # Run start time is fixed across retries; use a trailing 30-day window.
        end = context['dag_run'].start_date
        start = end - timedelta(days=30)
        with tempfile.TemporaryDirectory() as directory:
            reference = Path(directory) / 'ports.json'
            reference.write_text(json.dumps(ports))
            subprocess.run([sys.executable, '/opt/ais/pipelines/port_visits/run.py',
                            '--ports', str(reference), '--start', start.isoformat(),
                            '--end', end.isoformat(), '--apply'], check=True)

    run_port_visit_pipeline(download_ports())


ais_port_visits()
