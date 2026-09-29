#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "Usage: $0 ACTIVITY_DATE [MAX_ROWS] (one YYYY-MM-DD UTC day)" >&2
  exit 2
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."
command -v python3 >/dev/null || { echo "Error: python3 is required for argument validation." >&2; exit 1; }

conf=$(python3 - "$@" <<'PYTHON'
import json
import re
import sys

sys.path.insert(0, 'airflow/dags')
from daily_ais_window import resolve_activity_window

try:
    conf = {'activity_date': sys.argv[1]}
    if len(sys.argv) == 3:
        if not re.fullmatch(r'[0-9]+', sys.argv[2]):
            raise ValueError('max_rows must be a positive integer')
        conf['max_rows'] = int(sys.argv[2])
    resolve_activity_window(conf, None)
except ValueError as error:
    sys.exit(f'Error: {error}')
print(json.dumps(conf))
PYTHON
)

docker compose exec -T airflow airflow dags trigger daily_ais_pipeline --conf "$conf"
