# Historic AIS ingestion

This manual pipeline backfills a configured BarentsWatch Historic AIS polygon and explicit UTC window into ClickHouse `raw.ais_positions`.

```text
POST /v1/historic/mmsiinarea -> MMSIs
GET /v1/historic/tracks/{mmsi}/{fromDate}/{toDate} -> AIS records
                                -> raw.ais_positions (JSONEachRow)
```

The target uses `ReplacingMergeTree(ingested_at)` with logical key `(mmsi, msgtime)`. Historical results may overlap live Kafka ingestion, and retries may replay records; this is intentionally tolerated without SELECT-before-INSERT checks.

Required variables are documented in the root `.env.example`: BarentsWatch credentials and token URL, `BW_HISTORIC_BASE_URL`, `BW_HISTORIC_SCOPE`, `BW_HISTORIC_POLYGON_JSON`, request timeout, batch size, and ClickHouse connection settings. The current Bergen pilot polygon is the example default.

Manual local run:

```sh
python pipelines/historic_ais/run.py \
  --start 2026-09-01T00:00:00Z \
  --end 2026-09-02T00:00:00Z
```

The DAG `ais_historic_ingestion` is manual-only and computes a deterministic window from the Airflow run start time using `BW_HISTORIC_WINDOW_HOURS`. Trigger it with:

```sh
docker compose --profile batch up -d --build airflow
docker compose exec airflow airflow dags trigger ais_historic_ingestion
```

Check DAG imports, list the DAG and task, and inspect task logs:

```sh
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list | grep ais_historic_ingestion
docker compose exec airflow airflow tasks list ais_historic_ingestion
docker compose logs -f airflow
```

Validate the ClickHouse result after a successful run:

```sh
docker exec ais-clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT count() AS physical_rows, uniqExact((mmsi, msgtime)) AS logical_events FROM raw.ais_positions"

docker exec ais-clickhouse clickhouse-client \
  --user "$CLICKHOUSE_USER" \
  --password "$CLICKHOUSE_PASSWORD" \
  --query "SELECT min(msgtime), max(msgtime), count(), uniqExact(mmsi) FROM raw.ais_positions"
```