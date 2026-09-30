#!/usr/bin/env bash
# Inclusive activity-date range; coordinates only the existing daily master.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "Usage: $0 START_DATE END_DATE (YYYY-MM-DD; both dates inclusive)" >&2
  exit 2
fi
cd "$(dirname "${BASH_SOURCE[0]}")/.."
for tool in python3 docker; do
  command -v "$tool" >/dev/null || { echo "Error: $tool is required." >&2; exit 1; }
done

dates=$(python3 - "$@" <<'PYTHON'
from datetime import date, timedelta
import sys

sys.path.insert(0, 'airflow/dags')
from daily_ais_window import resolve_activity_window

try:
    for value in sys.argv[1:]:
        resolve_activity_window({'activity_date': value}, None)
    start, end = map(date.fromisoformat, sys.argv[1:])
    if start > end:
        raise ValueError('start date must not be after end date')
except (ValueError, OverflowError) as error:
    sys.exit(f'Error: invalid activity-date range: {error}')
for offset in range((end - start).days + 1):
    print((start + timedelta(days=offset)).isoformat())
PYTHON
)

# mkdir is an atomic, portable lock on macOS/Linux. Airflow's max_active_runs=1
# is the server-side safeguard against overlapping master runs from other clients.
lock_dir="${TMPDIR:-/tmp}/ais-daily-pipeline-backfill.lock"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "Error: backfill lock exists: $lock_dir" >&2
  echo "Check for another invocation. After a forced kill, verify runs are idle before removing this empty directory." >&2
  exit 1
fi
active_run_id=''
cleanup() {
  if [[ -n "$active_run_id" ]]; then
    echo "Stopped polling $active_run_id; the Airflow run was NOT cancelled. Check its state before retrying." >&2
  fi
  rmdir "$lock_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo 'Checking master and graph child DAG readiness...'
for dag in daily_ais_pipeline ais_port_visits ais_gds_metrics ais_graph_metrics_export; do
  docker exec ais-airflow airflow dags details "$dag" -o json |
    python3 -c '
import json, sys
rows = json.load(sys.stdin)
dag = sys.argv[1]
valid = isinstance(rows, list) and len(rows) == 1
if valid:
    row = rows[0]
    valid = row.get("dag_id") == dag and all(
        str(row.get(key)).lower() == "false"
        for key in ("is_paused", "is_stale", "has_import_errors")
    ) and (dag != "daily_ais_pipeline" or int(row.get("max_active_runs", 0)) == 1)
sys.exit(0 if valid else 1)
' "$dag" || { echo "Error: $dag is unavailable/paused/invalid, or master max_active_runs is not 1." >&2; exit 1; }
  for state in running queued; do
    docker exec ais-airflow airflow dags list-runs "$dag" --state "$state" -o json |
      python3 -c 'import json, sys; rows = json.load(sys.stdin); sys.exit(0 if isinstance(rows, list) and not rows else 1)' || {
        echo "Error: $dag has $state runs or its state could not be read. Wait for it to finish before backfilling." >&2
        exit 1
      }
  done
done

batch_id=$(python3 -c 'import uuid; print(uuid.uuid4().hex)')
total=$(printf '%s\n' "$dates" | wc -l | tr -d ' ')
index=0
while IFS= read -r activity_date; do
  index=$((index + 1))
  active_run_id="historical__${activity_date}__${batch_id}"
  echo "[$index/$total] Triggering $activity_date (run_id=$active_run_id)"
  # A CLI error is not retried automatically: the server might have accepted it.
  docker exec ais-airflow airflow dags trigger daily_ais_pipeline \
    --run-id "$active_run_id" --conf "{\"activity_date\":\"$activity_date\"}" -o json >/dev/null
  while true; do
    state=$(docker exec ais-airflow airflow dags state daily_ais_pipeline "$active_run_id")
    # Airflow appends ", <JSON conf>" when this run has configuration.
    state=${state%%,*}
    case "$state" in
      success)
        echo "[$index/$total] Success: $activity_date"
        active_run_id=''
        break
        ;;
      failed)
        echo "[$index/$total] Failed: $activity_date (run_id=$active_run_id); stopping." >&2
        active_run_id=''
        exit 1
        ;;
      queued|running)
        echo "[$index/$total] Waiting: $activity_date ($state; next check in 15s)"
        sleep 15
        ;;
      *)
        echo "Error: unexpected state '$state' for $active_run_id; stopping." >&2
        exit 1
        ;;
    esac
  done
done <<< "$dates"
echo "Completed $total daily runs successfully."
