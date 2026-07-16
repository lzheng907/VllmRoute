#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-mock}"
JOBS="${JOBS:-4}"

cd "${ROOT}"

case "${MODE}" in
  mock)
    docker build \
      --target mock-runtime \
      --build-arg JOBS="${JOBS}" \
      -t "${VLLMROUTE_MOCK_IMAGE:-vllmroute:mock}" \
      .
    ;;
  real)
    MINITSDB_SOURCE="${MINITSDB_SOURCE:-third_party/mini-tsdb}"
    if [[ ! -f "${ROOT}/${MINITSDB_SOURCE}/CMakeLists.txt" ]]; then
      echo "mini-tsdb source not found at ${MINITSDB_SOURCE}." >&2
      echo "Copy mini-tsdb to ${MINITSDB_SOURCE} or set MINITSDB_SOURCE=/path/inside/build/context." >&2
      exit 1
    fi
    docker build \
      --target runtime \
      --build-arg JOBS="${JOBS}" \
      --build-arg MINITSDB_SOURCE="${MINITSDB_SOURCE}" \
      -t "${VLLMROUTE_IMAGE:-vllmroute:latest}" \
      .
    ;;
  *)
    echo "Usage: $0 [mock|real]" >&2
    exit 1
    ;;
esac
