"""Manual MinIO → Spark raw → validation → Spark curated → validation load."""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.python import PythonOperator

LOG = logging.getLogger(__name__)


def result_from_task(ti, task_id, marker):
    output = ti.xcom_pull(task_ids=task_id)
    lines = str(output or '').splitlines()
    candidates = [line[len(marker):] for line in lines if line.startswith(marker)]
    if not candidates:
        raise AirflowFailException(f'{task_id} did not return {marker}')
    return json.loads(candidates[-1])


def validate_raw_task(ti):
    from lakehouse.validate_raw import validate
    expected = result_from_task(ti, 'ingest_raw_iceberg', 'RAW_INGEST_RESULT=')
    LOG.info('Running existing raw Trino validator')
    result = validate(expected)
    LOG.info('Raw validation passed: %s', json.dumps(result))
    return result


def validate_curated_task():
    from lakehouse.validate_curated import validate
    LOG.info('Running existing curated Trino/REST validator')
    report, _, _ = validate()
    result = {key: report[key] for key in ['counts', 'required_null_rows',
                                         'normalized_nulls', 'partition_count', 'validation']}
    LOG.info('Curated validation passed: %s', json.dumps(result))
    return result


def final_summary_task(ti):
    source = result_from_task(ti, 'check_source_files', 'SOURCE_RESULT=')
    raw = ti.xcom_pull(task_ids='validate_raw')
    curated = ti.xcom_pull(task_ids='validate_curated')
    summary = {'source_files': source['files'], 'raw_rows': raw['trino_count'],
               'curated_rows': curated['counts']['curated'], 'validation': 'passed'}
    LOG.info('Historical HAIS lakehouse load succeeded: %s', json.dumps(summary))
    return summary


def spark_task(task_id, script, marker, retries=1):
    # The image contains the same scripts used by the manual Compose commands.
    # Capture verbose Spark output inside the disposable container. Publish only
    # the success summary; include the last 40 lines if the command fails.
    command = f'''set -euo pipefail
    echo 'Running spark-submit: {script}'
    if /opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 2g /opt/ais/jobs/{script} > /tmp/hais-job.log 2>&1; then
        grep '^{marker}' /tmp/hais-job.log | tail -n 1
    else
        echo 'Spark command failed; last 40 log lines:' >&2
        tail -n 40 /tmp/hais-job.log >&2
        exit 1
    fi'''
    return DockerOperator(
        task_id=task_id,
        image=os.environ.get('LAKEHOUSE_SPARK_IMAGE', 'ais-spark:3.5.7-iceberg-1.10.1'),
        docker_url='unix://var/run/docker.sock',
        network_mode=os.environ.get('LAKEHOUSE_DOCKER_NETWORK', 'ais-network'),
        entrypoint=['/bin/bash', '-c'], command=[command],
        environment={'HAIS_SOURCE_URI': os.environ.get('HAIS_SOURCE_URI', 's3a://ais-data/hais/source/')},
        private_environment={key: os.environ.get(key, '') for key in ['MINIO_ROOT_USER', 'MINIO_ROOT_PASSWORD']},
        mount_tmp_dir=False, auto_remove='force', force_pull=False,
        do_xcom_push=True, xcom_all=False, retries=retries,
        execution_timeout=timedelta(hours=1),
    )


with DAG(
    dag_id='ais_hais_lakehouse_load',
    description='Manual historical HAIS load from MinIO through raw and curated Iceberg tables',
    start_date=datetime(2026, 9, 1, tzinfo=timezone.utc),
    schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['historical', 'hais', 'lakehouse'],
) as dag:
    check_source_files = spark_task('check_source_files', 'check_source_files.py', 'SOURCE_RESULT=', retries=2)
    ingest_raw_iceberg = spark_task('ingest_raw_iceberg', 'ingest_hais_raw.py', 'RAW_INGEST_RESULT=')
    validate_raw = PythonOperator(task_id='validate_raw', python_callable=validate_raw_task, retries=0)
    curate_hais = spark_task('curate_hais', 'curate_hais.py', 'CURATED_RESULT=')
    validate_curated = PythonOperator(task_id='validate_curated', python_callable=validate_curated_task, retries=0)
    final_summary = PythonOperator(task_id='final_summary', python_callable=final_summary_task, retries=0)
    check_source_files >> ingest_raw_iceberg >> validate_raw >> curate_hais >> validate_curated >> final_summary
