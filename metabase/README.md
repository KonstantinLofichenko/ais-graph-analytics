# Metabase dashboard transfer

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

The reviewed Norway dashboard snapshot is tracked at
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
