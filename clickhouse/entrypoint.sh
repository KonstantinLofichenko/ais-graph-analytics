#!/usr/bin/env bash
# Keep the upstream startup behavior, excluding the read-only HAIS bind mount
# from ownership changes on fresh data/user_files volumes.
set -euo pipefail

chown() {
    if [[ $# == 3 && $1 == -R ]]; then
        case "${3%/}" in
            /var/lib/clickhouse|/var/lib/clickhouse/user_files)
                find "${3%/}" \
                    -path /var/lib/clickhouse/user_files/hais -prune -o \
                    -exec chown -h "$2" {} +
                return
                ;;
        esac
    fi
    command chown "$@"
}
export -f chown

exec /entrypoint.sh "$@"
