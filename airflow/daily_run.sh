docker compose exec airflow airflow dags trigger ais_port_visits \
  --conf '{
    "start": "2026-09-17T08:00:00+00:00",
    "end": "2026-09-18T08:00:00+00:00",
    "max_rows": 5000000
  }'


docker compose exec airflow airflow dags trigger ais_gds_metrics


docker compose exec airflow airflow dags trigger ais_graph_metrics_export


OR

docker compose exec -T airflow airflow dags trigger \
  ais_analytics_pipeline \
  --conf '{"start": "2026-09-21T08:00:00Z", "end":"2026-09-22T08:00:00Z", "max_rows": 5000000}'