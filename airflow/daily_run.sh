#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 START_DATE END_DATE [MAX_ROWS] (YYYY-MM-DD; start inclusive, end exclusive)" >&2
  exit 2
fi

cd "$(dirname "${BASH_SOURCE[0]}")/.."
command -v python3 >/dev/null || { echo "Error: python3 is required for argument validation." >&2; exit 1; }

conf=$(python3 - "$@" <<'PYTHON'
import json
import re
import sys

sys.path.insert(0, 'airflow/dags')
from ais_port_visits_window import parse_date

try:
    start = parse_date(sys.argv[1], 'start')
    end = parse_date(sys.argv[2], 'end')
    if start >= end:
        raise ValueError('start must be earlier than end')
    conf = {'start': sys.argv[1], 'end': sys.argv[2]}
    if len(sys.argv) == 4:
        if not re.fullmatch(r'[0-9]+', sys.argv[3]) or int(sys.argv[3]) <= 0:
            raise ValueError('max_rows must be a positive integer')
        conf['max_rows'] = int(sys.argv[3])
except ValueError as error:
    sys.exit(f'Error: {error}')
print(json.dumps(conf))
PYTHON
)

docker compose exec -T airflow airflow dags trigger ais_analytics_pipeline --conf "$conf"
