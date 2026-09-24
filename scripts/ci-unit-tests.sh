#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Separate processes avoid collisions between pipeline modules named run.py.
# Only test_*.py is discovered; integration_check.py requires live databases.
for suite in \
  pipelines/port_visits/tests \
  pipelines/port_connections/tests \
  pipelines/graph_metrics/tests \
  pipelines/historic_ais/tests \
  pipelines/hais/tests \
  airflow/tests; do
  python -m unittest discover -s "$suite" -p 'test_*.py' -v
done
