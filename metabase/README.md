# Metabase dashboard and transfer

## Norway Port Graph Analytics

The tracked export in [`exports/norway-port-graph-analytics/`](exports/norway-port-graph-analytics/)
contains four tabs:

| Tab | Main source and purpose |
| --- | --- |
| **Ports on Map** | Port map from `port_graph_metrics_enriched`, with Date, Location, and Community filters. |
| **Ports** | PageRank tables with human-readable Community names, vessels/visits by community, and a Communities list. Uses `port_graph_metrics_enriched` and `port_graph_communities`; country enrichment requires the separately populated `raw.countries` source. |
| **Vessels** | The `vessels` mart: totals, recent activity, categories, navigation status, last-known positions, and details. |
| **Anomalies & AI Insights** | `vessel_daily_anomalies` and `vessel_daily_enriched` for a selected UTC day. |

The **Anomalies & AI Insights** tab contains **Anomaly Vessel-Days**, **AI-Enriched Anomalies**,
**Speed + Stationary Anomalies**, **Average Anomaly Severity**, **Anomalies by
Type**, **Top 10 Anomalies**, and the **AI Insights** table. Its **Date** dashboard
filter selects `activity_date` across these cards. The AI-Enriched Anomalies KPI
counts distinct anomaly vessels with `has_ai_enrichment = 1`,
`ai_prompt_version = 'vessel_anomaly_v1'`, and `anomaly_rank <= 100`.
Average severity uses the dbt `anomaly_score`, which is not a percentage.

Metabase custom expressions intentionally translate stored technical values
for display: `speed_and_stationary` becomes **Speed + Stationary**;
`mostly_stationary`, `continuous_movement`, and `mixed_activity` become
**Mostly Stationary**, **Continuous Movement**, and **Mixed Activity**. Some
translations are repeated across cards to demonstrate presentation-layer
functions; the underlying ClickHouse values remain unchanged.

The current exported dashboard includes a fixed historical default for **Date**.
Review that value and source/filter mappings after import before
using the dashboard for a different run. The tracked export is a configuration
snapshot, not a provisioned database or a live screenshot.

**Date** also filters the Ports tab's Top/Bottom centrality tables, community
activity chart, and Communities list, plus the port map. For graph cards,
`activity_date = snapshot_date`: the exporter uses the UTC processing window-start
date. Date **2026-09-29** selects run **2026-09-29T00:00:00Z**. This aligns with
port visits and the existing anomalies activity-day convention. Historical graph
snapshot dates must be corrected with the documented backfill before using these
filters on an upgraded installation.
The current dashboard has no Run ID selector. `run_id` remains available in both
graph models for query-builder filters, drill-down, and exact-run troubleshooting.
Multiple runs may share a date; use `run_id` in those queries to distinguish them
rather than treating the date as a unique graph identifier.

Community names and labels come from the selected graph run. Historical NULL labels
remain NULL; they are never filled from current communities. The activity chart
groups by `community_name`, so unlabeled historical communities share a blank
group. The Communities list also includes `community_id`, keeping those rows distinct.

## JSON transfer

These scripts export one dashboard and every saved question or model it uses to
JSON, then recreate them through the Metabase API. They work with the optional
`analytics` Compose service after its initial setup. Export includes dashboard
tabs, layout, dashboard filters, card filter mappings, visualization settings,
additional chart series, and recursive saved-question dependencies. Query builder
references (`card__123`) and native SQL references (`{{#123-name}}`) are included.

The scripts use Python 3.9 or newer and `requests`:

```sh
python3 -m venv .venv-metabase
.venv-metabase/bin/python -m pip install -r metabase/requirements.txt
```

Set the URL and one authentication method in the shell environment. The scripts
accept `METABASE_API_KEY`, `METABASE_SESSION`, or `METABASE_USER` together with
`METABASE_PASSWORD`, in that order of preference. The older `MB_*` names also
work. The root `.env.example` uses `METABASE_*`; the scripts do not read `.env`
automatically. Keep credentials out of command arguments and exported JSON.

```sh
export METABASE_URL=http://localhost:3000
export METABASE_API_KEY='your local API key'
```

Find the dashboard ID in its Metabase URL. Choose a new or empty output directory:

```sh
.venv-metabase/bin/python metabase/export_metabase.py 12 \
  --output /tmp/ais-metabase-dashboard
```

The directory contains `dashboard.json`, one readable
`cards/<old-id>-<card-name>.json` per saved question/model, and `manifest.json`
with the complete dependency list and file names. The exporter retains the fields
needed for import and omits creator details, activity history, and hydrated copies
of saved cards. Export refuses a nonempty directory so old card files cannot enter
a later import. Review SQL, filters, and embedded values before sharing it.

The Norway dashboard snapshot is tracked at
[`metabase/exports/norway-port-graph-analytics`](exports/norway-port-graph-analytics).
The default root `metabase_export/` directory is a Git-ignored temporary output;
it can be removed after the reviewed snapshot is prepared. To refresh the tracked
snapshot, export to a fresh directory, review the JSON, and replace the tracked
files after checking for instance-specific or sensitive values.

Prepare the destination Metabase instance with its ClickHouse database and the
tables/views used by these questions. Check the local export without making API
calls, then import it into a destination collection (omit `--collection-id` for
the root collection):

```sh
.venv-metabase/bin/python metabase/import_metabase.py \
  /tmp/ais-metabase-dashboard --dry-run
.venv-metabase/bin/python metabase/import_metabase.py \
  /tmp/ais-metabase-dashboard --collection-id 4
```

Import creates dependencies first, rewrites saved-question IDs in queries, creates
the dashboard, then restores tabs, cards, series, and filter mappings. It writes
`import_id_map.json` into the export directory for inspection. Each import creates
new objects; rerunning it creates another copy. If the API fails partway through,
objects already created remain for manual review.

This JSON transfer assumes referenced database, table, and field IDs are valid on
the destination. It does not create database connections, collections, permissions,
or underlying ClickHouse data. For a different Metabase installation with different
source IDs, adapt those query/filter references before import or use Metabase's
instance serialization features where available. The dashboard-card bulk endpoint
used here requires Metabase 0.47 or newer; this project runs 0.63.17.x.

## Tests

The tests use fake API responses and require no running Metabase service:

```sh
.venv-metabase/bin/python -m unittest discover -s metabase/tests -p 'test_*.py' -v
```

They cover recursive dependencies, import ordering and ID remapping, layout and
filter restoration, dry-run validation, and refusal to reuse stale output files.
