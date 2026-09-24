# dbt and HAIS → canonical AIS

Run the commands below from the repository root. This workflow uses host-side dbt
Core with Docker ClickHouse; it does not require Airbyte or country data.

## 1. Infrastructure and manually downloaded HAIS files

Use the [fresh-clone quickstart](../README.md#fresh-clone-quickstart) for the single
installation path: core services, bootstrap, local dbt setup, then HAIS ingestion.
Fresh initialization and bootstrap create the required raw/analytics tables; do
not run the legacy migration 001 on a fresh installation. The ClickHouse user
needs to create models in `analytics`, read sources, and insert into
`raw.ais_positions`.

HAIS ordering/downloading remains manual. See
[HAIS ingestion](../pipelines/hais/README.md) for file naming, permissions, audits,
and recovery. Wait for raw ingestion success before the canonical load. HAIS file
ingestion uses inclusive start and end dates; the dbt macro uses an exclusive end.

## 2. Install and configure dbt

Follow [quickstart step 11](../README.md#11-configure-dbt-from-the-profile-template)
for the Python 3.11 virtual environment, pinned dependencies, ignored local profile,
and environment credentials. The details below explain that configuration.

The profile name matches `dbt_project.yml`. Defaults are host `localhost`, HTTP
port `8123`, and database/schema `analytics`. Override `DBT_CLICKHOUSE_HOST` and
`DBT_CLICKHOUSE_PORT` if necessary; a client inside the Compose network would use
host `clickhouse`. This template targets local unencrypted HTTP, not a cloud setup.
See the [ClickHouse dbt configuration guide](https://clickhouse.com/docs/integrations/dbt/features-and-configurations).

The password environment variable uses dbt's secret prefix. Do not put credentials
in source-controlled files or enable shell tracing. dbt does **not** automatically
load the repository `.env`; supply the same credentials through the environment.
The local `dbt/profiles.yml` and virtual environment are ignored by Git. Reactivate
and re-export the environment when opening a new shell. There are no dbt package
dependencies, so `dbt deps` is not needed. Top-level Python dependencies are pinned;
transitive dependencies and the Python runtime are not fully locked.

All models currently use the profile's `analytics` schema; `staging/` and `marts/`
are model folders, not separate databases. The profile creates no new layer naming
scheme and does not change model logic.

## 3. Create the staging view

```sh
dbt run --project-dir dbt --select stg_hais_positions
```

`raw.hais_positions` is source-faithful: original HAIS fields, sentinel values, and
duplicates are preserved (geometry is omitted). The staging view maps HAIS fields,
selects one row per `(mmsi, date_time_utc)` using the existing ranking, and applies
the existing heading/course/turn-rate normalization. It leaves raw HAIS unchanged.
It is a view over all raw HAIS rows, not a materialized date-range load.

`raw.ais_positions` is the shared **canonical normalized table** used by downstream
vessel and port analysis. HAIS ship type/name are unavailable and remain NULL;
the existing staging model tags these records with `stream = 'hais'`. This workflow
does not promise normalization beyond the transformations present in that model.

## 4. Load a bounded range into canonical AIS

```sh
dbt run-operation load_hais_to_ais_positions --project-dir dbt \
  --args '{"start_date":"2026-09-01","end_date":"2026-09-17"}'
```

The macro uses **start inclusive, end exclusive**, at UTC midnight:
`[2026-09-01T00:00:00Z, 2026-09-17T00:00:00Z)`. This loads September 1–16.
For September 1 alone, use `start_date=2026-09-01`, `end_date=2026-09-02`.
Supply trusted, valid `YYYY-MM-DD` dates with start before end; the existing macro
interpolates arguments into SQL and does not implement strict date validation.

| Operation | Start | End | Included days |
| --- | --- | --- | --- |
| HAIS ingestion DAG | 2026-09-01 | 2026-09-16 | September 1–16 |
| dbt canonical-load macro | 2026-09-01 | 2026-09-17 | September 1–16 |

The macro performs an INSERT, not an incremental model or audit-controlled load.
Reruns can add physical row versions. `raw.ais_positions` uses
`ReplacingMergeTree(ingested_at)` with key `(mmsi, msgtime)`; use `FINAL` to read the
logical result. This can replace canonical records from overlapping live/REST
sources, including replacing richer name/ship-type values with HAIS NULLs. Review
overlapping source ranges before loading. The macro has no separate success ledger.

## 5. Downstream models and tests without countries

```sh
dbt build --project-dir dbt --select +vessels current_port_visits
```

This builds `stg_vessels`, `vessels`, and `current_port_visits` and runs the selected
models' existing tests (currently vessel MMSI uniqueness/non-null tests). No tests
are currently declared specifically for `stg_hais_positions` or
`current_port_visits`; build success alone is not a data-quality proof.
Port visits/GDS/export remain separate Airflow workflows. `current_port_visits`
can be empty until those port-visit inputs have been generated. dbt does not run
port detection or GDS, and the canonical-load macro is not automatically invoked
by `dbt build`.

## Optional country enrichment

Skip this section until `raw.countries` is populated by a separately configured
Airbyte REST Countries source. The repo does not reproduce that external source
and its credentials. The raw country table is required even though the enriched
model uses a LEFT JOIN. Do not run unqualified `dbt build` on a fresh clone without it.

```sh
dbt build --project-dir dbt --select +countries +port_graph_metrics_enriched
```

This builds/tests the country staging model, builds `analytics.countries`, and
creates `analytics.port_graph_metrics_enriched`. Ports, visits, and graph-metrics
source tables must also exist; populated graph metrics require the Airflow analytics
chain. The enriched view is the current dbt replacement for the removed SQL view
migration. `countries` is a dbt-built MergeTree table, not the old manually seeded
ReplacingMergeTree dimension. Neither removed migration should be applied.
