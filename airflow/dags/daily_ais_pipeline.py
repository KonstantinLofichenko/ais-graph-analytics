"""Daily ClickHouse dbt models and vessel AI enrichment."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id='daily_ais_pipeline',
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['ais', 'dbt', 'ai'],
)
def daily_ais_pipeline():
    dbt_core = BashOperator(
        task_id='dbt_core',
        bash_command=(
            'dbt seed --project-dir /opt/ais/dbt --select '
            'ais_ship_types ais_navigational_status && '
            'dbt run --project-dir /opt/ais/dbt --select +vessels'
        ),
    )
    dbt_vessel_daily_features = BashOperator(
        task_id='dbt_vessel_daily_features',
        bash_command='dbt run --project-dir /opt/ais/dbt --select vessel_daily_features',
    )
    ai_enrichment = BashOperator(
        task_id='ai_enrichment',
        bash_command=(
            'python -m pipelines.ai_enrichment.enrich_vessels '
            '--date {{ (dag_run.logical_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}'
        ),
    )
    dbt_vessel_daily_enriched = BashOperator(
        task_id='dbt_vessel_daily_enriched',
        bash_command='dbt run --project-dir /opt/ais/dbt --select vessel_daily_enriched',
    )
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command=(
            'dbt test --project-dir /opt/ais/dbt --select '
            'vessels vessel_daily_features vessel_daily_enriched'
        ),
    )

    dbt_core >> dbt_vessel_daily_features >> ai_enrichment >> dbt_vessel_daily_enriched >> dbt_tests


daily_ais_pipeline()
