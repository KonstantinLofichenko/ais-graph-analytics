FROM apache/airflow:3.3.1-python3.12
COPY --chown=airflow:root pipelines/port_visits/requirements.txt /tmp/ais-requirements.txt
COPY --chown=airflow:root pipelines/historic_ais/requirements.txt /tmp/historic-requirements.txt
COPY --chown=airflow:root pipelines/ai_enrichment/requirements.txt /tmp/ai-requirements.txt
COPY --chown=airflow:root dbt/requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.1" -r /tmp/ais-requirements.txt -r /tmp/historic-requirements.txt -r /tmp/ai-requirements.txt -r /tmp/dbt-requirements.txt
COPY --chown=airflow:root pipelines/port_visits /opt/ais/pipelines/port_visits
COPY --chown=airflow:root pipelines/port_connections /opt/ais/pipelines/port_connections
COPY --chown=airflow:root pipelines/graph_metrics /opt/ais/pipelines/graph_metrics
COPY --chown=airflow:root pipelines/historic_ais /opt/ais/pipelines/historic_ais
COPY --chown=airflow:root pipelines/hais /opt/ais/pipelines/hais
COPY --chown=airflow:root pipelines/ai_enrichment /opt/ais/pipelines/ai_enrichment
COPY --chown=airflow:root dbt/dbt_project.yml /opt/ais/dbt/dbt_project.yml
COPY --chown=airflow:root dbt/profiles.yml.example /opt/ais/dbt/profiles.yml
COPY --chown=airflow:root dbt/models /opt/ais/dbt/models
COPY --chown=airflow:root dbt/macros /opt/ais/dbt/macros
COPY --chown=airflow:root dbt/tests /opt/ais/dbt/tests
COPY --chown=airflow:root dbt/seeds /opt/ais/dbt/seeds
COPY --chown=airflow:root clickhouse/init/02_port_visits.sql /opt/ais/clickhouse/init/02_port_visits.sql
COPY --chown=airflow:root airflow/dags /opt/ais/airflow/dags
