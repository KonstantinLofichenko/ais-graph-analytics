#!/bin/sh
# Lossless upgrade from legacy raw destinations. Entrypoint runs this before .sql.
set -eu

raw_db=${DERIVED_RAW_DATABASE:-raw}
analytics_db=${DERIVED_ANALYTICS_DATABASE:-analytics}
for database in "$raw_db" "$analytics_db"; do
    case "$database" in
        ''|[0-9]*|*[!a-zA-Z0-9_]*) echo 'Invalid derived database identifier' >&2; exit 1 ;;
    esac
done
[ "$raw_db" != "$analytics_db" ] || { echo 'Derived databases must differ' >&2; exit 1; }
case "${1:-}" in ''|--prepare-only) ;; *) echo 'Usage: 04_flink_derived.sh [--prepare-only]' >&2; exit 1 ;; esac
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
if [ "${1:-}" != --prepare-only ] && [ ! -r "$script_dir/04_flink_derived.sql" ]; then
    echo 'Cannot read derived schema SQL; migration not started' >&2
    exit 1
fi

query() {
    clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" \
        --query "$1"
}
exists() {
    [ "$(query "EXISTS TABLE $1")" = 1 ]
}
query "CREATE DATABASE IF NOT EXISTS $raw_db"
query "CREATE DATABASE IF NOT EXISTS $analytics_db"

# Preflight both tables before touching views: never merge ambiguous histories.
for table in ais_vessel_features ais_vessel_gap_events; do
    if exists "$raw_db.$table" && exists "$analytics_db.$table"; then
        echo "Both $raw_db.$table and $analytics_db.$table exist; refusing to merge or drop data" >&2
        exit 1
    fi
done

for table in ais_vessel_features ais_vessel_gap_events; do
    legacy=false
    missing_name=false
    wrong_target=false
    if exists "$raw_db.$table"; then legacy=true; fi
    if [ "$table" = ais_vessel_features ] && exists "$raw_db.${table}_kafka"; then
        if [ "$(query "SELECT count() FROM system.columns WHERE database='$raw_db' AND table='${table}_kafka' AND name='name'")" = 0 ]; then
            missing_name=true
        fi
    fi
    if exists "$raw_db.${table}_mv"; then
        if [ "$(query "SELECT position(create_table_query, 'TO $analytics_db.$table') > 0 FROM system.tables WHERE database='$raw_db' AND name='${table}_mv'")" = 0 ]; then
            wrong_target=true
        fi
    fi
    if [ "$legacy" = true ] || [ "$missing_name" = true ] || [ "$wrong_target" = true ]; then
        # Removing the view pauses its Kafka-to-table path. In-flight writes and
        # data parts retain the destination UUID across the Atomic rename.
        query "DROP VIEW IF EXISTS $raw_db.${table}_mv SYNC"
        if [ "$missing_name" = true ]; then
            # Recreate only this transport table to update its schema; preserve
            # its existing consumer group and committed offsets.
            query "DROP TABLE $raw_db.${table}_kafka SYNC"
        fi
        if [ "$legacy" = true ]; then
            query "RENAME TABLE $raw_db.$table TO $analytics_db.$table"
            echo "Moved $raw_db.$table -> $analytics_db.$table without copying rows"
        fi
    fi
done

if [ "${1:-}" != --prepare-only ]; then
    # Validated identifiers also make these replacements safe for isolated tests.
    sed -e "s/raw\./$raw_db./g" -e "s/analytics\./$analytics_db./g" \
        -e "s/DATABASE IF NOT EXISTS raw;/DATABASE IF NOT EXISTS $raw_db;/g" \
        -e "s/DATABASE IF NOT EXISTS analytics;/DATABASE IF NOT EXISTS $analytics_db;/g" \
        "$script_dir/04_flink_derived.sql" | \
        clickhouse-client --user "${CLICKHOUSE_USER:-default}" --password "${CLICKHOUSE_PASSWORD:-}" --multiquery
fi
