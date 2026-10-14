# dbt ClickHouse analytics

Run commands from the repository root. dbt builds the current ClickHouse vessel,
port, graph and AI analytics from shared AIS sources. Historical HAIS Parquet
loads and curated normalization are handled exclusively by the
[lakehouse pipeline](../lakehouse/README.md); there is no HAIS-to-ClickHouse loader.

## Configure

Follow [the project dbt setup](../README.md#4-configure-dbt) for the pinned
Python dependencies, ignored local profile and password environment variable.
The profile uses `analytics`, host `localhost`, HTTP port `8123`.
Container clients use `DBT_CLICKHOUSE_HOST=ais-clickhouse`. dbt does not load
`.env` automatically. Keep `dbt/profiles.yml` and credentials outside Git.
There are no dbt package dependencies; top-level Python dependencies are pinned.

```sh
source .venv-dbt/bin/activate
export DBT_PROFILES_DIR="$PWD/dbt"
dbt parse --project-dir dbt --profiles-dir dbt
dbt debug --project-dir dbt --profiles-dir dbt
dbt build --project-dir dbt --select +vessels current_port_visits
```

This builds `stg_vessels`, `vessels` and `current_port_visits`. Country-dependent
models require the optional Airbyte country source. dbt does not execute port
detection or GDS; the existing [daily Airflow master](../airflow/README.md#consolidated-daily-ais-pipeline)
coordinates those analytics. Shared canonical AIS history remains intact.

## Vessel features, anomalies, and AI enrichment

The normal daily path is:

```text
stg_vessels -> vessels -> vessel_daily_features
  -> vessel_daily_anomalies -> OpenAI enrichment history
  -> vessel_daily_enriched
```

`vessel_daily_features` derives one row per vessel and UTC `activity_date` from
canonical `raw.ais_positions`, including speed, stationary share, observation
coverage, and navigational-status measures. The model builds the historical
relation; the master does not artificially restrict it to the requested day.

`vessel_daily_anomalies` compares sufficiently observed vessel-days (at least
12 observation hours and 100 AIS points) with the same vessel's eligible days
from the preceding seven calendar days. Speed and stationary baselines each
require at least three usable days. A deviation is flagged against the larger
of a fixed floor or twice the baseline standard deviation. The model also
records navigation-status quality signals. Its `anomaly_score` sums relative
speed/stationary deviations and small status-signal increments; it is a
relative severity score, not a percentage or a 0–100 scale. Anomaly rows are
ranked within each `activity_date` by score (`anomaly_rank`).

The [AI task](../airflow/README.md#ai-behavior-and-recovery) selects only
`anomaly_rank <= 100`, strongest first, for the requested `activity_date`.
Matching model, `vessel_anomaly_v1` prompt version, and input hash are skipped
before `AI_ENRICHMENT_LIMIT` caps new calls. Successful responses are appended
to `analytics.vessel_ai_enrichment` with response IDs and token usage. The
deterministic dbt anomaly calculation does not depend on OpenAI; the later
`vessel_daily_enriched` mart joins only an exact current input-hash/model/prompt
match for current top-100 candidates. Older responses remain in history. The
newest response is selected only among matches to that exact key; a newer response
for different inputs cannot displace a valid older response. Unmatched AI fields
are NULL and `has_ai_enrichment = 0`. No-response, stale-response, and rank 101+
vessel-days still appear as feature rows without current AI content.

### AI freshness without OpenAI calls

Apply [migration 009](../clickhouse/migrations/009_vessel_ai_input_hashes.sql) on
existing installations (fresh bootstrap includes it). The new
`analytics.vessel_ai_input_hashes` table is a deterministic lookup, not response
history. `int_vessel_ai_candidates` owns the input projection and rounding used by
both Python and dbt. Its `input_values` representation includes field names, types
and exact hexadecimal value bytes, preserving NULLs and floating-point bits.
Python alone computes the existing `sha256(json.dumps(..., sort_keys=True,
default=str, separators=(",", ":")))` fingerprint. No historical hashes change.
`int_vessel_ai_current_inputs` joins the current input values to that lookup; it
never treats an old mapping for different values as current.

After rebuilding anomaly inputs, refresh mappings before building the presentation:

```sh
# Use the configured dbt and Python environments, or execute in ais-airflow.
dbt run --project-dir dbt --select int_vessel_ai_candidates
python -m pipelines.ai_enrichment.enrich_vessels --refresh-input-hashes-only
dbt run --project-dir dbt --select int_vessel_ai_current_inputs
dbt parse --project-dir dbt
dbt build --project-dir dbt --select vessel_daily_enriched
dbt test --project-dir dbt --select vessel_daily_enriched
```

The hash-only command covers all currently eligible dates, makes no OpenAI calls,
and never writes `vessel_ai_enrichment`. Normal AI execution also refreshes this
lookup before using its unchanged response cache and API-call limit. Airflow builds
the candidate view with anomalies and the current-input view before the final mart.
If inputs change without a lookup refresh, the new input values do not match and
presentation fails closed. Rebuild the materialized mart after source changes;
its freshness reflects its last successful build.

The `valid_current_ai` dbt test checks exact hashes, model/prompt, top-100 eligibility,
and NULL AI fields on unenriched rows. To see eligible days still lacking responses:

```sql
SELECT c.activity_date AS activity_date, count() AS needing_enrichment
FROM analytics.int_vessel_ai_current_inputs AS c
LEFT ANTI JOIN analytics.vessel_ai_enrichment AS h
    ON c.mmsi = h.mmsi AND c.activity_date = h.activity_date
   AND c.current_ai_input_hash = h.input_hash
   AND h.model = 'gpt-6-luna' AND h.prompt_version = 'vessel_anomaly_v1'
GROUP BY c.activity_date ORDER BY c.activity_date;
```

Use the configured model/prompt if changed. This query assumes the hash lookup
was refreshed; missing lookup entries must be refreshed before counting pending AI.

For a historical run, trigger `daily_ais_pipeline` with
`--conf '{"activity_date":"2026-09-27"}'` as shown in the Airflow guide. The
master pins port visits and AI selection to that UTC day while dbt rebuilds the
historical models. Run individual dbt commands here for setup, development,
or diagnosis rather than as a replacement for the master.

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

### Port analytics activity dates

`port_graph_metrics_enriched` and `port_graph_communities` expose `activity_date`
for date filtering. It is copied from the metric export's `snapshot_date`, which
uses the UTC **window-start date**. For example,
`run_id = 2026-09-29T00:00:00Z` has `activity_date = 2026-09-29`.
It is not derived from `run_id`, the export time, or the DAG execution date.

`current_port_visits.activity_date` comes directly from active
`port_visits.activity_date`. The visit writer supplies the UTC processing
window-start date; legacy dates use completed-run metadata, never arrival time.
Apply [migration 010 and its controlled backfill](../pipelines/port_visits/README.md#activity-date-upgrade)
before rebuilding the views.

`run_id` remains available for exact snapshot selection. Multiple runs can share
one calendar date; community uniqueness remains `(run_id, community_id)`.
Both views retain active metric rows (`FINAL` followed by `is_deleted = 0`), and
historical NULL community names/labels remain NULL.

```sh
dbt parse --project-dir dbt
dbt build --project-dir dbt --select current_port_visits port_graph_metrics_enriched port_graph_communities port_visit_activity_dates
dbt test --project-dir dbt --select current_port_visits port_graph_metrics_enriched port_graph_communities port_visit_activity_dates
```

Tests require non-null activity dates, a consistent snapshot date per run,
unchanged community keys, and community port totals equal to active metric rows.
