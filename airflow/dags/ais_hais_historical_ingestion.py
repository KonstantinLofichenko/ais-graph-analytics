"""Manually ingest an inclusive range of locally downloaded HAIS daily files."""
from datetime import datetime, timedelta, timezone
from airflow.sdk import Param, dag, task, get_current_context


@dag(dag_id='ais_hais_historical_ingestion',
     start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
     fail_fast=True, default_args={'retries': 0}, tags=['ais', 'hais', 'historic'],
     params={'start_date': Param(type='string', format='date'),
             'end_date': Param(type='string', format='date')})
def ais_hais_historical_ingestion():
    @task(execution_timeout=timedelta(minutes=5))
    def discover_files():
        import sys
        sys.path.insert(0, '/opt/ais')
        from pipelines.hais.ingest import discover
        context = get_current_context()
        params = {**context['params'], **(context['dag_run'].conf or {})}
        return discover(params.get('start_date'), params.get('end_date'))

    @task(execution_timeout=timedelta(hours=4),
          map_index_template="{{ task.op_kwargs['item']['source_date'] }}")
    def ingest_file(item):
        import sys
        sys.path.insert(0, '/opt/ais')
        from airflow.exceptions import AirflowSkipException
        from pipelines.hais.ingest import load_file
        result = load_file(item)
        if result['status'] == 'skipped':
            raise AirflowSkipException(f"Already loaded: {item['file_name']} ({item['file_size']} bytes)")
        return result

    ingest_file.expand(item=discover_files())


ais_hais_historical_ingestion()
