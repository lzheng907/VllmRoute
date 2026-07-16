#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBS="${JOBS:-$(nproc)}"

build_cmake_dir() {
  local dir="$1"
  shift || true
  cmake -S "$ROOT/$dir" -B "$ROOT/$dir/build" "$@"
  cmake --build "$ROOT/$dir/build" -j "$JOBS"
}

build_cmake_dir infra-controller
cmake --install "$ROOT/infra-controller/build"

build_cmake_dir unit-manager
build_cmake_dir node/test

if [[ -n "${MINITSDB_ROOT:-}" || -n "${1:-}" ]]; then
  export MINITSDB_ROOT="${MINITSDB_ROOT:-$1}"
  build_cmake_dir node/vllm-client -DMINITSDB_ROOT="$MINITSDB_ROOT"
else
  echo "Skip node/vllm-client: set MINITSDB_ROOT or pass it as the first argument."
  echo "Example: MINITSDB_ROOT=/path/to/mini-tsdb ./scripts/build_all.sh"
fi
