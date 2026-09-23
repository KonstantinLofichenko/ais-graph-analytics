"""Process an explicit date range as complete, sequential daily analytics chains."""
from datetime import datetime, timezone
from airflow.sdk import Param, dag

from ais_analytics_sequence import DailyAnalyticsOperator


@dag(dag_id='ais_analytics_pipeline',
     start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
     schedule=None, catchup=False, max_active_runs=1,
     default_args={'retries': 0}, tags=['ais', 'ports', 'orchestration'],
     params={'start': Param(type='string', format='date'),
             'end': Param(type='string', format='date'),
             'max_rows': Param(None, type=['null', 'integer'], minimum=1)})
def ais_analytics_pipeline():
    DailyAnalyticsOperator(task_id='daily_analytics')


ais_analytics_pipeline()
