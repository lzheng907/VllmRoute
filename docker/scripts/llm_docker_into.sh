#!/usr/bin/env bash
set -euo pipefail
MODE="${1:-mock}"
CONTAINER="vllmroute-mock"
if [[ "${MODE}" == "real" ]]; then
  CONTAINER="vllmroute"
fi
exec docker exec -it "${CONTAINER}" /bin/bash
