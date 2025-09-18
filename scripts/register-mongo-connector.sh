#!/usr/bin/env bash
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:38083}"
CONFIG_PATH="${1:-$(cd "$(dirname "$0")" && pwd)/../debezium/mongodb-source.json}"

echo "Registering MongoDB source connector using ${CONFIG_PATH}" >&2

curl --fail --silent --show-error \
  -X POST \
  -H "Content-Type: application/json" \
  --data "@${CONFIG_PATH}" \
  "${CONNECT_URL}/connectors"
echo