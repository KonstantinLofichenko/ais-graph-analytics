"""Daily ClickHouse dbt models and vessel AI enrichment."""
from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from daily_ais_window import resolve_activity_window


def ai_enrichment_command(context, jinja_env):
    # Pin AI to the same validated day as graph publication, even across midnight.
    window = context['ti'].xcom_pull(task_ids='resolve_activity_date')
    day = resolve_activity_window({'activity_date': window['activity_date']}, None)['activity_date']
    return f'python -m pipelines.ai_enrichment.enrich_vessels --date {day}'


def port_visit_conf(context, jinja_env):
    window = context['ti'].xcom_pull(task_ids='resolve_activity_date')
    return {key: value for key, value in window.items() if key != 'activity_date'}


@dag(
    dag_id='daily_ais_pipeline',
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule='0 2 * * *',
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['ais', 'dbt', 'ports', 'gds', 'ai'],
)
def daily_ais_pipeline():
    @task
    def resolve_activity_date():
        context = get_current_context()
        run = context['dag_run']
        return resolve_activity_window(run.conf, run.start_date)

    activity_date = resolve_activity_date()

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
    # Reuse the existing manual graph DAGs; wait for each successful completion.
    # Disable trigger-task retries to avoid accidentally replaying child runs.
    graph_tasks = []
    for child in ('ais_port_visits', 'ais_gds_metrics', 'ais_graph_metrics_export'):
        graph_tasks.append(TriggerDagRunOperator(
            task_id=child,
            trigger_dag_id=child,
            trigger_run_id='{{ run_id }}__' + child,
            conf=port_visit_conf if child == 'ais_port_visits' else None,
            wait_for_completion=True,
            deferrable=True,
            poke_interval=10,
            allowed_states=['success'],
            failed_states=['failed'],
            fail_when_dag_is_paused=True,
            retries=0,
        ))
    port_visits, gds, graph_export = graph_tasks
    dbt_graph_models = BashOperator(
        task_id='dbt_graph_models',
        bash_command='dbt run --project-dir /opt/ais/dbt --select current_port_visits',
    )
    dbt_vessel_daily_anomalies = BashOperator(
        task_id='dbt_vessel_daily_anomalies',
        bash_command='dbt run --project-dir /opt/ais/dbt --select vessel_daily_anomalies',
    )
    ai_enrichment = BashOperator(
        task_id='ai_enrichment',
        # The call limit remains controlled by Python/environment configuration.
        bash_command=ai_enrichment_command,
    )
    dbt_vessel_daily_enriched = BashOperator(
        task_id='dbt_vessel_daily_enriched',
        bash_command='dbt run --project-dir /opt/ais/dbt --select vessel_daily_enriched',
    )
    dbt_tests = BashOperator(
        task_id='dbt_tests',
        bash_command=(
            'dbt test --project-dir /opt/ais/dbt --select '
            'vessels vessel_daily_features current_port_visits vessel_daily_anomalies vessel_daily_enriched'
        ),
    )

    (
        activity_date >> dbt_core >> dbt_vessel_daily_features
        >> port_visits >> gds >> graph_export >> dbt_graph_models >> dbt_vessel_daily_anomalies
        >> ai_enrichment >> dbt_vessel_daily_enriched >> dbt_tests
    )


daily_ais_pipeline()
