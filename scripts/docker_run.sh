#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-mock}"
shift || true

cd "${ROOT}"
mkdir -p runtime

case "${MODE}" in
  mock)
    docker compose --profile mock up --build "$@"
    ;;
  real)
    MINITSDB_SOURCE="${MINITSDB_SOURCE:-third_party/mini-tsdb}"
    if [[ ! -f "${ROOT}/${MINITSDB_SOURCE}/CMakeLists.txt" ]]; then
      echo "mini-tsdb source not found at ${MINITSDB_SOURCE}." >&2
      echo "Copy mini-tsdb to ${MINITSDB_SOURCE} before running real mode." >&2
      exit 1
    fi
    MINITSDB_SOURCE="${MINITSDB_SOURCE}" docker compose --profile real up --build "$@"
    ;;
  *)
    echo "Usage: $0 [mock|real] [docker compose up args...]" >&2
    exit 1
    ;;
esac
