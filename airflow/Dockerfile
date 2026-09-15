FROM apache/airflow:3.3.1-python3.12
COPY --chown=airflow:root pipelines/port_visits/requirements.txt /tmp/ais-requirements.txt
COPY --chown=airflow:root pipelines/historic_ais/requirements.txt /tmp/historic-requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.1" -r /tmp/ais-requirements.txt -r /tmp/historic-requirements.txt
COPY --chown=airflow:root pipelines/port_visits /opt/ais/pipelines/port_visits
COPY --chown=airflow:root pipelines/port_connections /opt/ais/pipelines/port_connections
COPY --chown=airflow:root pipelines/historic_ais /opt/ais/pipelines/historic_ais
COPY --chown=airflow:root clickhouse/init/02_port_visits.sql /opt/ais/clickhouse/init/02_port_visits.sql
COPY --chown=airflow:root airflow/dags /opt/ais/airflow/dags
