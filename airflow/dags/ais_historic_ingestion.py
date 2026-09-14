"""Manual BarentsWatch Historic AIS backfill workflow."""

from datetime import datetime, timedelta, timezone
import os
import subprocess
import sys

from airflow.sdk import dag, task, get_current_context


@dag(dag_id='ais_historic_ingestion', start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1,
     default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)}, tags=['ais', 'historic'])
def ais_historic_ingestion():
    @task(execution_timeout=timedelta(hours=2))
    def run_historic_ingestion():
        context = get_current_context()
        end = context['dag_run'].start_date
        start = end - timedelta(hours=float(os.environ['BW_HISTORIC_WINDOW_HOURS']))
        subprocess.run([sys.executable, '/opt/ais/pipelines/historic_ais/run.py',
                        '--start', start.isoformat(), '--end', end.isoformat()], check=True)

    run_historic_ingestion()


ais_historic_ingestion()