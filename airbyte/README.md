# Optional Airbyte country enrichment

The single versioned configuration snapshot is in
[`exports/countries-to-clickhouse/`](exports/countries-to-clickhouse/):

- `countries-api-connector.yaml`: Connector Builder declarative manifest;
- `countries-api-source.json`: `Countries API` source configuration;
- `clickhouse-destination.json`: ClickHouse destination configuration;
- `countries-to-clickhouse-connection.json`: source-to-destination sync;
- `manifest.json`: provenance and required placeholders.

The source, destination, and connection were refreshed against the local Airbyte
public API on September 25, 2026. Secrets and deployment-specific IDs were
replaced with `REPLACE_WITH_*` placeholders. The Connector Builder YAML is the
September 24 export because the public API did not return its manifest; the
provenance is recorded in `manifest.json`. Resolve placeholders in ignored local
copies before making API requests.

## Source connector dependency

`Countries API` uses a custom Connector Builder definition based on:

```text
docker repository: airbyte/source-declarative-manifest
image tag:         7.28.2
custom:            true
```

The local source-definition UUID is intentionally not committed because it is
specific to that Airbyte installation. Import
`exports/countries-to-clickhouse/countries-api-connector.yaml` into Connector
Builder first, then put the new definition UUID in
your local copy of `countries-api-source.json`.

## Recreate the objects

1. Import `exports/countries-to-clickhouse/countries-api-connector.yaml` as the custom `Countries API` source definition.
2. Copy the source and destination JSON files to ignored `airbyte/*.local.json` files.
3. Replace the workspace, source-definition, API-key, and ClickHouse-password
   placeholders.
4. Create the destination and source through the Airbyte API or UI.
5. Copy their returned IDs into a local copy of `countries-to-clickhouse-connection.json`.
6. Create the connection and run a sync.
7. Confirm that `raw.countries` is populated before running the country-dependent
   dbt models.

The extracted connection selects only the `countries` stream, runs every 24
hours, uses `full_refresh_overwrite`, writes to the destination namespace, and
propagates new columns for non-breaking schema updates.

## Security

Never replace placeholders in the versioned export files. Keep local
files containing real credentials outside Git. Airbyte masks secret values in GET
responses, but every exported configuration should still be reviewed before it
is committed.

The snapshot files contain placeholders for the REST Countries API key and
ClickHouse password. The Connector Builder YAML references the key through
`config['api_key']`; it contains no key value. Do not replace placeholders in
the versioned files. Use ignored `airbyte/*.local.json` copies for local requests.
For a live API read, set `AIRBYTE_ACCESS_TOKEN` in the ignored root `.env`; the
token is used only in an Authorization header and is never written to this export.

## Validation

The repository test checks the snapshot file names, JSON structure, placeholders,
and absence of local credential values. It does not contact Airbyte:

```sh
python3 -m unittest discover -s airbyte/tests -p 'test_*.py' -v
```
