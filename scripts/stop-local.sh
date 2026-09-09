#!/usr/bin/env bash
set -euo pipefail

docker compose down
brew services stop neo4j || true