#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-mock}"
cd "${ROOT}"

case "${MODE}" in
  mock)
    docker compose --profile mock run --rm --entrypoint /bin/bash mock
    ;;
  real)
    docker compose --profile real run --rm --entrypoint /bin/bash real
    ;;
  *)
    echo "Usage: $0 [mock|real]" >&2
    exit 1
    ;;
esac
